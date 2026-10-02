//! Append-only audit log: every state transition, dispute-grade.
//! `audit.wal` JSON lines with per-record crc framing inherited from wal-db.
//! Best-effort: audit failure never fails the payment (counted).

use std::path::Path;

use serde::{Deserialize, Serialize};
use wal_db::{Wal, WalError};

use crate::dlq::now_ms;
use crate::metrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at_ms: u64,
    pub tx_id: String,
    pub partition: usize,
    pub lsn: u64,
    pub from: String,
    pub to: String,
    pub actor: String,
    pub reason: String,
}

pub struct Audit {
    wal: Wal,
}

impl Audit {
    pub fn open(path: &Path) -> Result<Self, WalError> {
        Ok(Self { wal: Wal::open(path)? })
    }

    /// Best-effort append; failures counted, never propagated.
    pub fn record(&mut self, e: &AuditEntry) {
        match serde_json::to_vec(e) {
            Ok(b) => {
                if self.wal.append(&b).is_err() {
                    metrics::audit_dropped();
                }
            }
            Err(_) => metrics::audit_dropped(),
        }
    }

    pub fn sync(&mut self) {
        let _ = self.wal.sync();
    }

    pub fn for_tx(&self, tx_id: &str, limit: usize) -> Vec<AuditEntry> {
        let mut out = Vec::new();
        let iter = match self.wal.iter() {
            Ok(i) => i,
            Err(_) => return out,
        };
        for entry in iter.flatten() {
            if let Ok(e) = serde_json::from_slice::<AuditEntry>(entry.data()) {
                if e.tx_id == tx_id {
                    out.push(e);
                }
            }
        }
        let n = out.len();
        out.into_iter().skip(n.saturating_sub(limit.max(1))).collect()
    }
}

pub fn entry(
    tx_id: &str,
    partition: usize,
    lsn: u64,
    from: &str,
    to: &str,
    actor: &str,
    reason: &str,
) -> AuditEntry {
    AuditEntry {
        at_ms: now_ms(),
        tx_id: tx_id.into(),
        partition,
        lsn,
        from: from.into(),
        to: to.into(),
        actor: actor.into(),
        reason: reason.into(),
    }
}
