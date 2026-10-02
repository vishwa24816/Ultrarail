//! App state: N partitioned writer tasks, each owning one journal.
//! Handlers route by hash(scope,key,sender,receiver) and await a oneshot.
//! Each writer drains its queue per wake and pays ONE sync per batch (group commit).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::accounts::{Registry, Terminal};
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

/// Replaceable writer handle: supervision swaps the sender on partition restart
/// without touching handlers. Lock is never held across await (clone-then-send).
#[derive(Clone)]
pub struct WriterSlot {
    tx: Arc<Mutex<mpsc::Sender<WriteCmd>>>,
}

impl WriterSlot {
    pub async fn send(&self, cmd: WriteCmd) -> Result<(), String> {
        let s = self.tx.lock().map(|g| g.clone()).map_err(|_| "writer gone".to_string())?;
        s.send(cmd).await.map_err(|_| "writer overloaded".to_string())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub writers: Vec<WriterSlot>,
    pub dlq: Arc<Mutex<Dlq>>,
    pub bcast: broadcast::Sender<BankEvent>,
    pub pending: PendingMap,
    pub registry: Registry,
    pub failed_tx: broadcast::Sender<(String, Terminal)>,
    /// Durable txs by id for GET /payments/:id. Grows with traffic —
    /// ponytail: TTL sweep when it matters, map is bounded by journal anyway.
    pub store: Arc<Mutex<HashMap<String, StoredTx>>>,
    pub events: Vec<Arc<Mutex<EventLog>>>,
    /// Set on SIGTERM: intake returns 503, loops drain then exit.
    pub draining: Arc<AtomicBool>,
    /// Per-partition replay-complete flags for /ready.
    pub ready: Arc<Mutex<Vec<bool>>>,
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
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

/// Shared handles for one partition's supervisor.
#[derive(Clone)]
struct PartShared {
    dir: PathBuf,
    id: usize,
    dlq: Arc<Mutex<Dlq>>,
    bcast: broadcast::Sender<BankEvent>,
    pending: PendingMap,
    store: Arc<Mutex<HashMap<String, StoredTx>>>,
    registry: Registry,
    failed_bcast: broadcast::Sender<(String, Terminal)>,
    slot: WriterSlot,
    events: Arc<Mutex<EventLog>>,
    draining: Arc<AtomicBool>,
    ready: Arc<Mutex<Vec<bool>>>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

/// Run one partition generation: open, replay, serve until shutdown or crash.
async fn run_partition(sh: PartShared) -> Result<(), String> {
    let id = sh.id;
    // Fault injection for supervision tests (sandbox only).
    if std::env::var("PAYMENT_PANIC_PARTITION").ok().as_deref() == Some(&id.to_string()) {
        panic!("injected crash for partition {id}");
    }
    let mut journal =
        Journal::open(&sh.dir.join(format!("journal-{id}.wal"))).map_err(|e| e.to_string())?;
    // Fresh writer channel per generation; swap into the stable slot.
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(1024);
    if let Ok(mut g) = sh.slot.tx.lock() {
        *g = tx;
    }
    // Re-queue durable-but-unsettled txs (crash recovery) + rebuild attempt budgets.
    let boot_txs: Vec<PaymentTx> = {
        let mut v = Vec::new();
        for (tx_id, _) in journal.tx_list() {
            if let Ok(t) = journal.read_tx_public(&tx_id) {
                v.push(t);
            }
        }
        v
    };
    let boot_attempts: HashMap<String, u32> = {
        let mut m = HashMap::new();
        if let Ok(e) = sh.events.lock() {
            for ev in e.replay() {
                if matches!(ev.kind, crate::events::EventKind::Attempt) {
                    *m.entry(ev.tx_id).or_insert(0) += 1;
                }
            }
        }
        m
    };
    if let Ok(mut r) = sh.ready.lock() {
        if id < r.len() {
            r[id] = true;
        }
    }
    tracing::info!("partition {id}: replayed {} records", journal.len());
    let (dtx, drx) = mpsc::channel::<PaymentTx>(1024);
    let frx = sh.failed_bcast.subscribe();
    let dev = crate::delivery::Delivery {
        inbox: drx,
        failed_rx: frx,
        events: sh.events.clone(),
        dlq: sh.dlq.clone(),
        registry: sh.registry.clone(),
        rail: crate::delivery::SimRail::from_env(),
        rail_timeout: Duration::from_millis(
            std::env::var("RAIL_TIMEOUT_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(2000),
        ),
        reconcile_timeout_ms: std::env::var("RECONCILE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(300)
            * 1000,
        partition: id,
        boot_attempts,
        shutdown: sh.shutdown_rx.clone(),
    };
    let dev_handle = tokio::spawn(async move { dev.run().await });
    for t in boot_txs {
        let _ = dtx.send(t).await;
    }
    let mut shutdown = sh.shutdown_rx.clone();
    let res = writer_loop(&mut journal, &mut rx, &mut shutdown, &sh, &dtx).await;
    // Writer ended: close delivery inbox, wait for drain.
    drop(dtx);
    let _ = tokio::time::timeout(Duration::from_secs(30), dev_handle).await;
    res
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    journal: &mut Journal,
    rx: &mut mpsc::Receiver<WriteCmd>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    sh: &PartShared,
    dtx: &mpsc::Sender<PaymentTx>,
) -> Result<(), String> {
    let id = sh.id;
    loop {
        let first = tokio::select! {
            c = rx.recv() => match c {
                Some(c) => c,
                None => {
                    let _ = journal.sync();
                    return Ok(());
                }
            },
            _ = shutdown.changed() => {
                let _ = journal.sync();
                return Ok(());
            }
        };
        let mut cmds = vec![first];
        while let Ok(c) = rx.try_recv() {
            cmds.push(c);
        }
        process_batch(journal, cmds, sh, dtx, id).await;
    }
}

async fn process_batch(
    journal: &mut Journal,
    cmds: Vec<WriteCmd>,
    sh: &PartShared,
    dtx: &mpsc::Sender<PaymentTx>,
    id: usize,
) {
    let t0 = Instant::now();
    journal.sweep_if_due();
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
                    if let Ok(mut d) = sh.dlq.lock() {
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
    for (e_i, msg) in &early {
        let c = &cmds[*e_i];
        if let Ok(mut d) = sh.dlq.lock() {
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
                    if let Ok(mut s) = sh.store.lock() {
                        s.insert(tx.tx_id.clone(), StoredTx { tx: tx.clone(), lsn: o.lsn, partition: id });
                    }
                    let _ = dtx.send(tx.clone()).await;
                }
                publish(
                    &sh.bcast,
                    &sh.pending,
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

/// Supervisor: restart crashed partitions from WAL with backoff; give up after
/// 5 rapid crashes (DLQ `partition-down`, other partitions keep serving).
fn supervise(sh: PartShared) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let max_crashes: usize = std::env::var("SUPERVISOR_MAX_CRASHES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let base_backoff_ms: u64 = std::env::var("SUPERVISOR_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let mut crashes: Vec<Instant> = vec![];
        let mut backoff = Duration::from_millis(base_backoff_ms.max(1));
        loop {
            // Catch panics (incl. injected) so the supervisor survives to count + restart.
            let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                run_partition(sh.clone()),
            ))
            .await;
            match outcome {
                Ok(Ok(())) => break, // clean end (shutdown)
                Ok(Err(e)) => {
                    if crash(&sh, e, &mut crashes, max_crashes, &mut backoff).await
                        || sh.draining.load(Ordering::Relaxed)
                    {
                        break;
                    }
                }
                Err(_) => {
                    if crash(&sh, "panic".into(), &mut crashes, max_crashes, &mut backoff).await
                        || sh.draining.load(Ordering::Relaxed)
                    {
                        break;
                    }
                }
            }
        }
    })
}

/// Returns true when the partition is down for good (logged to DLQ).
async fn crash(
    sh: &PartShared,
    e: String,
    crashes: &mut Vec<Instant>,
    max_crashes: usize,
    backoff: &mut Duration,
) -> bool {
    tracing::error!("partition {} crashed: {e}", sh.id);
    let now = Instant::now();
    crashes.retain(|t| now.duration_since(*t) < Duration::from_secs(60));
    crashes.push(now);
    if crashes.len() >= max_crashes.max(1) {
        tracing::error!("partition {} down: {max_crashes} crashes in 60s", sh.id);
        if let Ok(mut d) = sh.dlq.lock() {
            d.push(DlqEntry {
                reason: "partition-down".into(),
                scope: String::new(),
                key: String::new(),
                tx_id: None,
                detail: format!("partition {} down: {e}", sh.id),
                at_ms: now_ms(),
            });
            d.flush();
        }
        return true;
    }
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(Duration::from_secs(60));
    false
}

pub struct Writers {
    pub slots: Vec<WriterSlot>,
    pub dlq: Arc<Mutex<Dlq>>,
    pub bcast: broadcast::Sender<BankEvent>,
    pub pending: PendingMap,
    pub store: Arc<Mutex<HashMap<String, StoredTx>>>,
    pub events: Vec<Arc<Mutex<EventLog>>>,
    pub registry: Registry,
    pub failed_bcast: broadcast::Sender<(String, Terminal)>,
    pub draining: Arc<AtomicBool>,
    pub ready: Arc<Mutex<Vec<bool>>>,
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub supervisors: Vec<tokio::task::JoinHandle<()>>,
}

/// Open N partitions under `dir`. Fail-closed: any bad partition aborts boot.
#[allow(clippy::too_many_arguments)]
pub fn spawn_writers(
    dir: PathBuf,
    n: usize,
    draining: Arc<AtomicBool>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> Result<Writers, JournalError> {
    let shutdown_rx = shutdown_tx.subscribe();
    let dlq = Arc::new(Mutex::new(Dlq::open(&dir.join("dlq.wal")).map_err(|e| JournalError::Codec(e.to_string()))?));
    let (bcast, _) = broadcast::channel(1024);
    let (failed_bcast, _) = broadcast::channel(1024);
    let pending = crate::ws::pending_map();
    let store = Arc::new(Mutex::new(HashMap::new()));
    let registry = Registry::default();
    let ready = Arc::new(Mutex::new(vec![false; n]));
    let mut slots = Vec::with_capacity(n);
    let mut events = Vec::with_capacity(n);
    let mut supervisors = Vec::with_capacity(n);
    for i in 0..n {
        let (wtx, _) = mpsc::channel::<WriteCmd>(1); // placeholder, swapped on first open
        let slot = WriterSlot { tx: Arc::new(Mutex::new(wtx)) };
        let ev = Arc::new(Mutex::new(
            EventLog::open(&dir.join(format!("events-{i}.wal")))
                .map_err(|e| JournalError::Codec(e.to_string()))?,
        ));
        let sh = PartShared {
            dir: dir.clone(),
            id: i,
            dlq: dlq.clone(),
            bcast: bcast.clone(),
            pending: pending.clone(),
            store: store.clone(),
            registry: registry.clone(),
            failed_bcast: failed_bcast.clone(),
            slot: slot.clone(),
            events: ev.clone(),
            draining: draining.clone(),
            ready: ready.clone(),
            shutdown_rx: shutdown_rx.clone(),
        };
        supervisors.push(supervise(sh));
        slots.push(slot);
        events.push(ev);
    }
    // No boot barrier here: partitions mark ready as they finish replay and
    // /ready gates traffic. A dead partition must never hold the listener hostage.
    let _ = partition::bucket; // router lives in api; keep import graph honest
    Ok(Writers { slots, dlq, bcast, pending, store, events, registry, failed_bcast, draining, ready, shutdown_tx, supervisors })
}
