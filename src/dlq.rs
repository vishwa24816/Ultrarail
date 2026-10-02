//! Dead-letter queue: append-only, best-effort, never blocks the ack path.
//! Reasons: `key-expired-replayed`, `validation-failed`, `bank-no-ack` (wave 4).

use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use wal_db::{Wal, WalError};

#[derive(Debug, Error)]
pub enum DlqError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("codec: {0}")]
    Codec(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub reason: String,
    pub scope: String,
    pub key: String,
    pub tx_id: Option<String>,
    pub detail: String,
    pub at_ms: u64,
}

pub struct Dlq {
    wal: Wal,
    last_sync: Instant,
}

impl Dlq {
    pub fn open(path: &Path) -> Result<Self, DlqError> {
        Ok(Self { wal: Wal::open(path)?, last_sync: Instant::now() })
    }

    /// Append without sync-before-ack; sync coalesced to at most every 5s.
    pub fn push(&mut self, e: DlqEntry) {
        let bytes = match serde_json::to_vec(&e) {
            Ok(b) => b,
            Err(_) => return,
        };
        if self.wal.append(&bytes).is_err() {
            return;
        }
        if self.last_sync.elapsed().as_secs() >= 5 {
            if self.wal.sync().is_ok() {
                self.last_sync = Instant::now();
            }
        }
    }

    /// Force durability now. Call after a sweep batch — one fsync per batch.
    pub fn flush(&mut self) {
        if self.wal.sync().is_ok() {
            self.last_sync = Instant::now();
        }
    }

    /// Unconditional final barrier (shutdown path).
    pub fn sync_all(&mut self) {
        let _ = self.wal.sync();
    }

    pub fn tail(&self, limit: usize) -> Result<Vec<DlqEntry>, DlqError> {
        let mut all = Vec::new();
        for entry in self.wal.iter()? {
            let entry = entry?;
            if let Ok(e) = serde_json::from_slice::<DlqEntry>(entry.data()) {
                all.push(e);
            }
        }
        let n = all.len();
        Ok(all.into_iter().skip(n.saturating_sub(limit.max(1))).collect())
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
