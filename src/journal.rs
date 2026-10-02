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

/// A validated tx waiting for its batch's sync.
pub enum Staged {
    Replay(PaymentTx),
    Fresh(PaymentTx, Vec<u8>),
}

enum StagedKind {
    Replay(PaymentTx),
    Fresh(usize), // index into pending
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

    /// Build + validate + serialize. No I/O — safe to call outside the writer.
    /// Returns the existing tx on idempotency hit (no new record).
    pub fn prepare(
        &mut self,
        scope: String,
        key: String,
        money: Money,
        debit_account: String,
        credit_account: String,
        bucket: u64,
        tx_id: String,
    ) -> Result<Staged, JournalError> {
        if let Some(existing) = self.lookup(&scope, &key) {
            let tx = self.read_tx(existing)?;
            return Ok(Staged::Replay(tx));
        }
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
        tx.checksum = checksum(&bytes);
        bytes = serde_json::to_vec(&tx).map_err(|e| JournalError::Codec(e.to_string()))?;
        let _ = bucket; // embedded in tx_id already; kept for future bucketed segments
        Ok(Staged::Fresh(tx, bytes))
    }

    /// Append a whole batch, then ONE sync for all of them (group commit).
    /// Ack-worthy only after this returns Ok.
    pub fn commit_batch(&mut self, batch: Vec<Staged>) -> Vec<Result<(PaymentTx, AcceptOutcome), JournalError>> {
        let mut seen: BTreeMap<(String, String), PaymentTx> = BTreeMap::new();
        let mut pending: Vec<(PaymentTx, Vec<u8>)> = Vec::new();
        // First pass: dedupe within the batch, append-serialize the rest.
        let mut kinds: Vec<StagedKind> = Vec::with_capacity(batch.len());
        for s in batch {
            match s {
                Staged::Replay(tx) => kinds.push(StagedKind::Replay(tx)),
                Staged::Fresh(tx, bytes) => {
                    let k = (tx.idempotency_scope.clone(), tx.idempotency_key.clone());
                    if let Some(first) = seen.get(&k) {
                        kinds.push(StagedKind::Replay(first.clone()));
                    } else {
                        seen.insert(k, tx.clone());
                        pending.push((tx, bytes));
                        kinds.push(StagedKind::Fresh(pending.len() - 1));
                    }
                }
            }
        }
        if pending.is_empty() {
            return kinds
                .into_iter()
                .map(|k| match k {
                    StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true })),
                    StagedKind::Fresh(_) => unreachable!(),
                })
                .collect();
        }
        let mut lsns: Vec<u64> = Vec::with_capacity(pending.len());
        for (_, bytes) in &pending {
            match self.wal.append(bytes) {
                Ok(lsn) => lsns.push(lsn.get()),
                Err(e) => {
                    let err = e.to_string();
                    return kinds
                        .into_iter()
                        .map(|k| match k {
                            StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true })),
                            StagedKind::Fresh(_) => Err(JournalError::Codec(err.clone())),
                        })
                        .collect();
                }
            }
        }
        if let Err(e) = self.wal.sync() {
            // <-- single durability barrier for the whole batch
            let err = e.to_string();
            return kinds
                .into_iter()
                .map(|k| match k {
                    StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true })),
                    StagedKind::Fresh(_) => Err(JournalError::Codec(err.clone())),
                })
                .collect();
        }
        let mut it = lsns.into_iter();
        for (tx, _) in &mut pending {
            let lsn = it.next().unwrap_or(0);
            tx.status = TxStatus::AcceptedDurable;
            self.tx_index.insert(tx.tx_id.clone(), lsn);
            self.idem_index.insert(
                (tx.idempotency_scope.clone(), tx.idempotency_key.clone()),
                tx.tx_id.clone(),
            );
        }
        kinds
            .into_iter()
            .map(|k| match k {
                StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true })),
                StagedKind::Fresh(i) => {
                    let (tx, _) = &pending[i];
                    Ok((tx.clone(), AcceptOutcome { lsn: self.tx_index[&tx.tx_id], durable: true }))
                }
            })
            .collect()
    }

    /// Legacy single-accept (prepare + solo commit). Kept for unit tests.
    pub fn accept(
        &mut self,
        scope: String,
        key: String,
        money: Money,
        debit_account: String,
        credit_account: String,
    ) -> Result<(PaymentTx, AcceptOutcome), JournalError> {
        let bucket = 0;
        let tx_id = uuid::Uuid::now_v7().to_string();
        let staged = self.prepare(scope, key, money, debit_account, credit_account, bucket, tx_id)?;
        let mut out = self.commit_batch(vec![staged]);
        out.pop().unwrap()
    }

    fn read_tx(&self, want: &str) -> Result<PaymentTx, JournalError> {
        for entry in self.wal.iter()? {
            let entry = entry?;
            let tx: PaymentTx =
                serde_json::from_slice(entry.data()).map_err(|e| JournalError::Codec(e.to_string()))?;
            if tx.tx_id != want {
                continue;
            }
            let mut probe = tx.clone();
            let sum = probe.checksum;
            probe.checksum = 0;
            let bytes = serde_json::to_vec(&probe).map_err(|e| JournalError::Codec(e.to_string()))?;
            if checksum(&bytes) != sum {
                return Err(JournalError::Checksum(0));
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
        Money { amount: 100, price: 100, quantity: 1, currency: Currency::USD }
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
