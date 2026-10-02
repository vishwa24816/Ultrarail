//! Per-partition event log: attempts, state changes, reconcile outcomes.
//! Separate `events-{i}.wal` per partition — never mixed into the tx journal,
//! so tx replay stays strict. Event replay is tolerant (skips garbage).

use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use wal_db::{Wal, WalError};

use crate::dlq::now_ms;

#[derive(Debug, Error)]
pub enum EventError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("codec: {0}")]
    Codec(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxEvent {
    pub tx_id: String,
    pub at_ms: u64,
    pub kind: EventKind,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Submitted,
    Attempt,
    Unknown,
    Reconciled,
    Settled,
    Failed,
}

pub struct EventLog {
    wal: Wal,
}

impl EventLog {
    pub fn open(path: &Path) -> Result<Self, EventError> {
        Ok(Self { wal: Wal::open(path)? })
    }

    /// Append + group sync with the caller (delivery syncs once per batch).
    pub fn append(&mut self, ev: &TxEvent) -> Result<(), EventError> {
        let bytes = serde_json::to_vec(ev).map_err(|e| EventError::Codec(e.to_string()))?;
        let _ = self.wal.append(&bytes)?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), EventError> {
        self.wal.sync()?;
        Ok(())
    }

    pub fn replay(&self) -> Vec<TxEvent> {
        let mut out = Vec::new();
        let iter = match self.wal.iter() {
            Ok(i) => i,
            Err(_) => return out,
        };
        for entry in iter.flatten() {
            if let Ok(ev) = serde_json::from_slice::<TxEvent>(entry.data()) {
                out.push(ev);
            }
        }
        out
    }
}

pub fn event(tx_id: &str, kind: EventKind, detail: impl Into<String>) -> TxEvent {
    TxEvent { tx_id: tx_id.into(), at_ms: now_ms(), kind, detail: detail.into() }
}
