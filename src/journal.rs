//! Journal: append-only WAL as source of truth.
//! Rule: ack (ACCEPTED_DURABLE) only after `sync()` returns. Replay rebuilds indexes.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use wal_db::{Wal, WalError};

use crate::domain::{LedgerEntry, Money, PaymentTx, TxStatus};

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("codec: {0}")]
    Codec(String),
    #[error("checksum mismatch at lsn {0}")]
    Checksum(u64),
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn checksum(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// Durable journal. Single writer only — share via mpsc, never Arc<Mutex>.
pub struct Journal {
    wal: Wal,
    tx_index: BTreeMap<String, u64>,   // tx_id -> lsn
    idem_index: BTreeMap<(String, String), String>, // (scope,key) -> tx_id
}

#[derive(Debug, Clone, Copy)]
pub struct AcceptOutcome {
    pub lsn: u64,
    pub durable: bool,
}

impl Journal {
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        let wal = Wal::open(path)?;
        let mut j = Self { wal, tx_index: BTreeMap::new(), idem_index: BTreeMap::new() };
        j.replay()?;
        Ok(j)
    }

    /// Idempotency lookup before doing any work.
    pub fn lookup(&self, scope: &str, key: &str) -> Option<&String> {
        self.idem_index.get(&(scope.to_string(), key.to_string()))
    }

    pub fn get(&self, tx_id: &str) -> Option<u64> {
        self.tx_index.get(tx_id).copied()
    }

    /// Build + validate + append + sync. Returns durable outcome — the ONLY ack path.
    pub fn accept(
        &mut self,
        scope: String,
        key: String,
        money: Money,
        debit_account: String,
        credit_account: String,
    ) -> Result<(PaymentTx, AcceptOutcome), JournalError> {
        if let Some(tx_id) = self.lookup(&scope, &key) {
            let lsn = self.tx_index[tx_id];
            let tx = self.read_tx(lsn)?;
            return Ok((tx, AcceptOutcome { lsn, durable: true }));
        }
        let tx_id = uuid::Uuid::now_v7().to_string();
        let entries = vec![
            LedgerEntry { account: debit_account, debit: money.amount, credit: 0 },
            LedgerEntry { account: credit_account, debit: 0, credit: money.amount },
        ];
        let mut tx = PaymentTx {
            tx_id,
            idempotency_scope: scope,
            idempotency_key: key,
            request_hash: 0,
            money,
            entries,
            status: TxStatus::Received,
            timestamp_ms: now_ms(),
            checksum: 0,
        };
        tx.validate().map_err(|e| JournalError::Codec(e.to_string()))?;
        tx.status = TxStatus::Validated;
        tx.request_hash = checksum(tx.tx_id.as_bytes()) as u64;
        let mut bytes = serde_json::to_vec(&tx).map_err(|e| JournalError::Codec(e.to_string()))?;
        let sum = checksum(&bytes);
        tx.checksum = sum;
        bytes = serde_json::to_vec(&tx).map_err(|e| JournalError::Codec(e.to_string()))?;
        let lsn: u64 = self.wal.append(&bytes)?.get();
        self.wal.sync()?; // <-- durability barrier; ack only after this returns
        tx.status = TxStatus::AcceptedDurable;
        self.tx_index.insert(tx.tx_id.clone(), lsn);
        self.idem_index.insert((tx.idempotency_scope.clone(), tx.idempotency_key.clone()), tx.tx_id.clone());
        Ok((tx, AcceptOutcome { lsn, durable: true }))
    }

    fn read_tx(&self, _lsn: u64) -> Result<PaymentTx, JournalError> {
        // ponytail: re-scan is O(n); per-tx offset cache when it matters
        for entry in self.wal.iter()? {
            let entry = entry?;
            let tx: PaymentTx =
                serde_json::from_slice(entry.data()).map_err(|e| JournalError::Codec(e.to_string()))?;
            let mut probe = tx.clone();
            let sum = probe.checksum;
            probe.checksum = 0;
            let bytes = serde_json::to_vec(&probe).map_err(|e| JournalError::Codec(e.to_string()))?;
            if checksum(&bytes) != sum {
                continue;
            }
            return Ok(tx);
        }
        Err(JournalError::Codec("tx not found".into()))
    }

    /// Replay all durable records; rebuild indexes. Stops at first corrupt record.
    fn replay(&mut self) -> Result<usize, JournalError> {
        let mut n = 0;
        let records: Vec<Vec<u8>> = {
            let mut v = Vec::new();
            for entry in self.wal.iter()? {
                let entry = entry?;
                v.push(entry.data().to_vec());
            }
            v
        };
        for (i, data) in records.into_iter().enumerate() {
            let tx: PaymentTx =
                serde_json::from_slice(&data).map_err(|e| JournalError::Codec(e.to_string()))?;
            let mut probe = tx.clone();
            let sum = probe.checksum;
            probe.checksum = 0;
            let bytes = serde_json::to_vec(&probe).map_err(|e| JournalError::Codec(e.to_string()))?;
            if checksum(&bytes) != sum {
                return Err(JournalError::Checksum(i as u64));
            }
            self.idem_index.insert(
                (tx.idempotency_scope.clone(), tx.idempotency_key.clone()),
                tx.tx_id.clone(),
            );
            self.tx_index.insert(tx.tx_id.clone(), i as u64);
            n += 1;
        }
        Ok(n)
    }

    pub fn len(&self) -> usize {
        self.tx_index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Currency;

    fn money() -> Money {
        Money { amount: 100, currency: Currency::USD }
    }

    #[test]
    fn accept_then_reopen_survives() {
        let dir = std::env::temp_dir().join(format!("journal-test-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("j.wal");
        let lsn = {
            let mut j = Journal::open(&path).unwrap();
            let (tx, out) = j
                .accept("payments".into(), "k1".into(), money(), "user:1".into(), "m:9".into())
                .unwrap();
            assert!(out.durable);
            assert_eq!(tx.status, TxStatus::AcceptedDurable);
            out.lsn
        };
        let _ = lsn;
        let j2 = Journal::open(&path).unwrap();
        assert_eq!(j2.len(), 1);
        assert!(j2.lookup("payments", "k1").is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retry_same_key_returns_original() {
        let dir = std::env::temp_dir().join(format!("journal-idem-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("j.wal");
        let mut j = Journal::open(&path).unwrap();
        let (t1, _) =
            j.accept("payments".into(), "dup".into(), money(), "user:1".into(), "m:9".into()).unwrap();
        let (t2, _) =
            j.accept("payments".into(), "dup".into(), money(), "user:1".into(), "m:9".into()).unwrap();
        assert_eq!(t1.tx_id, t2.tx_id);
        assert_eq!(j.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
