//! App state: single-writer journal task behind a bounded channel.
//! Handlers never block on fsync — they await a oneshot from the writer task.

use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use crate::domain::Money;
use crate::journal::{Journal, JournalError};
use crate::metrics;

pub struct WriteCmd {
    pub scope: String,
    pub key: String,
    pub money: Money,
    pub debit_account: String,
    pub credit_account: String,
    pub reply: oneshot::Sender<Result<(String, u64), String>>,
}

#[derive(Clone)]
pub struct AppState {
    pub tx: mpsc::Sender<WriteCmd>,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("journal: {0}")]
    Journal(#[from] JournalError),
    #[error("writer overloaded")]
    Overloaded,
    #[error("writer gone")]
    Gone,
}

pub fn spawn_writer(journal_path: PathBuf) -> Result<mpsc::Sender<WriteCmd>, JournalError> {
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(1024);
    let mut journal = Journal::open(&journal_path)?;
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let t0 = Instant::now();
            let out = journal.accept(cmd.scope, cmd.key, cmd.money, cmd.debit_account, cmd.credit_account);
            metrics::observe_journal_sync(t0.elapsed().as_secs_f64() * 1000.0);
            let reply = match out {
                Ok((tx, o)) => {
                    metrics::count_accepted();
                    Ok((tx.tx_id, o.lsn))
                }
                Err(e) => {
                    metrics::count_rejected();
                    Err(e.to_string())
                }
            };
            let _ = cmd.reply.send(reply);
        }
    });
    Ok(tx)
}
