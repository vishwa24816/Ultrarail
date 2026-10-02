//! App state: N partitioned writer tasks, each owning one journal.
//! Handlers route by hash(scope,key,sender,receiver) and await a oneshot.
//! Each writer drains its queue per wake and pays ONE sync per batch (group commit).

use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use crate::domain::Money;
use crate::journal::{Journal, JournalError, Staged};
use crate::metrics;
use crate::partition;

pub struct WriteCmd {
    pub scope: String,
    pub key: String,
    pub money: Money,
    pub debit_account: String,
    pub credit_account: String,
    pub bucket: u64,
    pub tx_id: String,
    pub reply: oneshot::Sender<Result<(String, u64), String>>,
}

#[derive(Clone)]
pub struct AppState {
    pub writers: Vec<mpsc::Sender<WriteCmd>>,
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

fn spawn_one(journal_path: PathBuf, id: usize) -> Result<mpsc::Sender<WriteCmd>, JournalError> {
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(1024);
    let mut journal = Journal::open(&journal_path)?;
    tracing::info!("partition {id}: replayed {} records", journal.len());
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
                    Ok(s) => {
                        idx.push(i);
                        staged.push(s);
                    }
                    Err(e) => early.push((i, e.to_string())),
                }
            }
            let results = journal.commit_batch(staged);
            metrics::observe_journal_sync(t0.elapsed().as_secs_f64() * 1000.0);
            let mut by_cmd: Vec<Option<Result<(String, u64), String>>> = (0..cmds.len()).map(|_| None).collect();
            for (e_i, msg) in early {
                metrics::count_rejected();
                by_cmd[e_i] = Some(Err(msg));
            }
            for (k, res) in idx.into_iter().zip(results.into_iter()) {
                by_cmd[k] = Some(match res {
                    Ok((tx, o)) => {
                        metrics::count_accepted();
                        Ok((tx.tx_id, o.lsn))
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
    Ok(tx)
}

/// Open N partitions under `dir` as `journal-{i}.wal`. Fail-closed: any bad partition aborts boot.
pub fn spawn_writers(dir: PathBuf, n: usize) -> Result<Vec<mpsc::Sender<WriteCmd>>, JournalError> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(spawn_one(dir.join(format!("journal-{i}.wal")), i)?);
    }
    let _ = partition::bucket; // router lives in api; keep import graph honest
    Ok(out)
}
