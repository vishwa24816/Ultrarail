//! App state: N partitioned writer tasks, each owning one journal.
//! Handlers route by hash(scope,key,sender,receiver) and await a oneshot.
//! Each writer drains its queue per wake and pays ONE sync per batch (group commit).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::accounts::{Registry, Terminal};
use crate::delivery::Delivery;
use crate::dlq::{now_ms, Dlq, DlqEntry};
use crate::domain::{Money, PaymentTx};
use crate::events::EventLog;
use crate::journal::{Journal, JournalError, Staged};
use crate::metrics;
use crate::partition;
use crate::ws::{publish, BankEvent, PendingMap};

pub struct WriteCmd {
    pub scope: String,
    pub key: String,
    pub money: Money,
    pub debit_account: String,
    pub credit_account: String,
    pub bucket: u64,
    pub tx_id: String,
    pub reply: oneshot::Sender<CmdResult>,
}

/// (tx_id, lsn, replayed?) — replayed tells the client `replayed` vs `fresh`.
pub type CmdResult = Result<(String, u64, bool), String>;

#[derive(Clone)]
pub struct AppState {
    pub writers: Vec<mpsc::Sender<WriteCmd>>,
    pub dlq: Arc<Mutex<Dlq>>,
    pub bcast: broadcast::Sender<BankEvent>,
    pub pending: PendingMap,
    pub registry: Registry,
    pub failed_tx: broadcast::Sender<(String, Terminal)>,
    /// Durable txs by id for GET /payments/:id. Grows with traffic —
    /// ponytail: TTL sweep when it matters, map is bounded by journal anyway.
    pub store: Arc<Mutex<HashMap<String, StoredTx>>>,
    pub events: Vec<Arc<Mutex<EventLog>>>,
}

#[derive(Clone)]
pub struct StoredTx {
    pub tx: PaymentTx,
    pub lsn: u64,
    pub partition: usize,
}

impl AppState {
    pub fn partitions(&self) -> usize {
        self.writers.len()
    }
}

pub fn partition_count() -> usize {
    std::env::var("PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| (n.get() / 2).max(2)).unwrap_or(2))
        .max(1)
}

fn spawn_one(
    journal_path: PathBuf,
    events_path: PathBuf,
    id: usize,
    dlq: Arc<Mutex<Dlq>>,
    bcast: broadcast::Sender<BankEvent>,
    pending: PendingMap,
    store: Arc<Mutex<HashMap<String, StoredTx>>>,
    registry: Registry,
    failed_bcast: broadcast::Sender<(String, Terminal)>,
) -> Result<(mpsc::Sender<WriteCmd>, Arc<Mutex<EventLog>>), JournalError> {
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(1024);
    let (dtx, drx) = mpsc::channel::<PaymentTx>(1024);
    let frx = failed_bcast.subscribe();
    let mut journal = Journal::open(&journal_path)?;
    let events = Arc::new(Mutex::new(
        EventLog::open(&events_path).map_err(|e| JournalError::Codec(e.to_string()))?,
    ));
    tracing::info!("partition {id}: replayed {} records", journal.len());
    // Re-queue durable-but-unsettled txs into delivery on boot (crash recovery).
    let boot_txs: Vec<PaymentTx> = {
        let mut v = Vec::new();
        // ponytail: full journal scan per partition at boot only.
        for (tx_id, _) in journal.tx_list() {
            if let Ok(t) = journal.read_tx_public(&tx_id) {
                v.push(t);
            }
        }
        v
    };
    let ev2 = events.clone();
    let dlq2 = dlq.clone();
    let reg2 = registry.clone();
    tokio::spawn(async move {
        let rail = crate::delivery::SimRail::from_env();
        let rail_timeout = std::time::Duration::from_millis(
            std::env::var("RAIL_TIMEOUT_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(2000),
        );
        let reconcile_timeout_ms = std::env::var("RECONCILE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(300)
            * 1000;
        Delivery {
            inbox: drx,
            failed_rx: frx,
            events: ev2,
            dlq: dlq2,
            registry: reg2,
            rail,
            rail_timeout,
            reconcile_timeout_ms,
            partition: id,
        }
        .run()
        .await;
    });
    // Seed boot txs into delivery.
    {
        let dtx_boot = dtx.clone();
        tokio::spawn(async move {
            for t in boot_txs {
                let _ = dtx_boot.send(t).await;
            }
        });
    }
    let store2 = store.clone();
    tokio::spawn(async move {
        loop {
            // Block for the first cmd, then drain everything waiting (group commit batch).
            let first = match rx.recv().await {
                Some(c) => c,
                None => break,
            };
            let mut cmds = vec![first];
            while let Ok(c) = rx.try_recv() {
                cmds.push(c);
            }
            let t0 = Instant::now();
            journal.sweep_if_due();
            // Prepare each cmd (validation, in-memory). Failures reply immediately;
            // successes join one group-commit batch.
            let mut idx: Vec<usize> = Vec::new();
            let mut staged: Vec<Staged> = Vec::new();
            let mut early: Vec<(usize, String)> = Vec::new();
            for (i, c) in cmds.iter().enumerate() {
                match journal.prepare(
                    c.scope.clone(),
                    c.key.clone(),
                    c.money.clone(),
                    c.debit_account.clone(),
                    c.credit_account.clone(),
                    c.bucket,
                    c.tx_id.clone(),
                ) {
                    Ok(p) => {
                        if let Some(old) = p.expired_old_tx {
                            // Post-TTL retry: new payment, but leave an audit trail.
                            if let Ok(mut d) = dlq.lock() {
                                d.push(DlqEntry {
                                    reason: "key-expired-replayed".into(),
                                    scope: c.scope.clone(),
                                    key: c.key.clone(),
                                    tx_id: Some(old),
                                    detail: format!("expired key reused as fresh by partition {id}"),
                                    at_ms: now_ms(),
                                });
                            }
                        }
                        idx.push(i);
                        staged.push(p.staged);
                    }
                    Err(e) => early.push((i, e.to_string())),
                }
            }
            // Validation failures are operator-visible via the DLQ (best-effort).
            for (e_i, msg) in &early {
                let c = &cmds[*e_i];
                if let Ok(mut d) = dlq.lock() {
                    d.push(DlqEntry {
                        reason: "validation-failed".into(),
                        scope: c.scope.clone(),
                        key: c.key.clone(),
                        tx_id: None,
                        detail: msg.clone(),
                        at_ms: now_ms(),
                    });
                }
            }
            let results = journal.commit_batch(staged);
            metrics::observe_journal_sync(t0.elapsed().as_secs_f64() * 1000.0);
            let mut by_cmd: Vec<Option<CmdResult>> = (0..cmds.len()).map(|_| None).collect();
            for (e_i, msg) in early {
                metrics::count_rejected();
                by_cmd[e_i] = Some(Err(msg));
            }
            for (k, res) in idx.into_iter().zip(results.into_iter()) {
                by_cmd[k] = Some(match res {
                    Ok((tx, o)) => {
                        metrics::count_accepted();
                        if !o.replayed {
                            // Fresh durable tx: remember for status queries + hand to delivery.
                            if let Ok(mut s) = store2.lock() {
                                s.insert(tx.tx_id.clone(), StoredTx { tx: tx.clone(), lsn: o.lsn, partition: id });
                            }
                            let _ = dtx.send(tx.clone()).await;
                        }
                        // Publish to bank streams (fire-and-forget; lagging banks get dropped).
                        publish(
                            &bcast,
                            &pending,
                            &BankEvent {
                                tx_id: tx.tx_id.clone(),
                                status: "ACCEPTED_DURABLE",
                                amount: tx.money.amount,
                                price: tx.money.price,
                                quantity: tx.money.quantity,
                                currency: format!("{:?}", tx.money.currency),
                                debit_account: tx.entries.first().map(|e| e.account.clone()).unwrap_or_default(),
                                credit_account: tx.entries.get(1).map(|e| e.account.clone()).unwrap_or_default(),
                                lsn: o.lsn,
                                partition: id,
                            },
                            &cmds[k].debit_account,
                            &cmds[k].credit_account,
                        );
                        Ok((tx.tx_id, o.lsn, o.replayed))
                    }
                    Err(e) => {
                        metrics::count_rejected();
                        Err(e.to_string())
                    }
                });
            }
            for (cmd, res) in cmds.into_iter().zip(by_cmd.into_iter()) {
                let _ = cmd.reply.send(res.unwrap_or_else(|| Err("writer error".into())));
            }
        }
    });
    Ok((tx, events))
}

pub struct Writers {
    pub senders: Vec<mpsc::Sender<WriteCmd>>,
    pub dlq: Arc<Mutex<Dlq>>,
    pub bcast: broadcast::Sender<BankEvent>,
    pub pending: PendingMap,
    pub store: Arc<Mutex<HashMap<String, StoredTx>>>,
    pub events: Vec<Arc<Mutex<EventLog>>>,
    pub registry: Registry,
    pub failed_bcast: broadcast::Sender<(String, Terminal)>,
}

/// Open N partitions under `dir` as `journal-{i}.wal` plus a shared `dlq.wal`.
/// Fail-closed: any bad partition aborts boot.
pub fn spawn_writers(dir: PathBuf, n: usize) -> Result<Writers, JournalError> {
    let dlq = Arc::new(Mutex::new(Dlq::open(&dir.join("dlq.wal")).map_err(|e| JournalError::Codec(e.to_string()))?));
    let (bcast, _) = broadcast::channel(1024);
    let (failed_bcast, _) = broadcast::channel(1024);
    let pending = crate::ws::pending_map();
    let store = Arc::new(Mutex::new(HashMap::new()));
    let registry = Registry::default();
    let mut senders = Vec::with_capacity(n);
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let (s, ev) = spawn_one(
            dir.join(format!("journal-{i}.wal")),
            dir.join(format!("events-{i}.wal")),
            i,
            dlq.clone(),
            bcast.clone(),
            pending.clone(),
            store.clone(),
            registry.clone(),
            failed_bcast.clone(),
        )?;
        senders.push(s);
        events.push(ev);
    }
    let _ = partition::bucket; // router lives in api; keep import graph honest
    Ok(Writers { senders, dlq, bcast, pending, store, events, registry, failed_bcast })
}
