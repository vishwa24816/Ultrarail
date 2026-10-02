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
    tx_index: BTreeMap<String, u64>, // tx_id -> lsn
    idem_index: BTreeMap<(String, String), IdemRec>,
    ttl_ms: u64,
    last_sweep: std::time::Instant,
}

#[derive(Debug, Clone)]
struct IdemRec {
    tx_id: String,
    expires_ms: u64,
}

/// What prepare() found: a live key, an expired key (treated as new, logged to DLQ), or nothing.
pub struct Prepared {
    pub staged: Staged,
    /// Old tx_id when the key existed but expired — writer logs it to the DLQ.
    pub expired_old_tx: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct AcceptOutcome {
    pub lsn: u64,
    pub durable: bool,
    pub replayed: bool,
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
        let ttl_ms = std::env::var("IDEM_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(86400)
            * 1000;
        let wal = Wal::open(path)?;
        let mut j = Self {
            wal,
            tx_index: BTreeMap::new(),
            idem_index: BTreeMap::new(),
            ttl_ms,
            last_sweep: std::time::Instant::now(),
        };
        let counts = j.replay()?;
        tracing::info!("replayed {} records ({} expired dropped)", counts.0, counts.1);
        Ok(j)
    }

    /// Idempotency lookup. Expired keys behave as misses (caller treats as new).
    pub fn lookup(&self, scope: &str, key: &str) -> Option<&String> {
        self.idem_index.get(&(scope.to_string(), key.to_string())).and_then(|r| {
            if r.expires_ms > now_ms() {
                Some(&r.tx_id)
            } else {
                None
            }
        })
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
    ) -> Result<Prepared, JournalError> {
        let k = (scope.clone(), key.clone());
        if let Some(rec) = self.idem_index.get(&k).cloned() {
            if rec.expires_ms > now_ms() {
                let tx = self.read_tx(&rec.tx_id)?;
                return Ok(Prepared { staged: Staged::Replay(tx), expired_old_tx: None });
            }
            // Expired: fall through as a FRESH payment, but report the old id for the DLQ.
            let old = Some(rec.tx_id);
            return self.prepare_fresh(scope, key, money, debit_account, credit_account, tx_id, bucket).map(
                |staged| Prepared { staged, expired_old_tx: old },
            );
        }
        self.prepare_fresh(scope, key, money, debit_account, credit_account, tx_id, bucket)
            .map(|staged| Prepared { staged, expired_old_tx: None })
    }

    fn prepare_fresh(
        &mut self,
        scope: String,
        key: String,
        money: Money,
        debit_account: String,
        credit_account: String,
        tx_id: String,
        _bucket: u64,
    ) -> Result<Staged, JournalError> {
        let _ = self.ttl_ms;
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
                    StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true, replayed: true })),
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
                            StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true, replayed: true })),
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
                    StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true, replayed: true })),
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
                IdemRec { tx_id: tx.tx_id.clone(), expires_ms: tx.timestamp_ms.saturating_add(self.ttl_ms) },
            );
        }
        kinds
            .into_iter()
            .map(|k| match k {
                StagedKind::Replay(tx) => Ok((tx, AcceptOutcome { lsn: 0, durable: true, replayed: true })),
                StagedKind::Fresh(i) => {
                    let (tx, _) = &pending[i];
                    Ok((tx.clone(), AcceptOutcome { lsn: self.tx_index[&tx.tx_id], durable: true, replayed: false }))
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
        let p = self.prepare(scope, key, money, debit_account, credit_account, bucket, tx_id)?;
        let mut out = self.commit_batch(vec![p.staged]);
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

    /// Replay all durable records; rebuild indexes. Expired idempotency keys are
    /// dropped (counted). Returns (live, expired_dropped).
    fn replay(&mut self) -> Result<(usize, usize), JournalError> {
        let mut n = 0;
        let mut dropped = 0;
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
            self.tx_index.insert(tx.tx_id.clone(), i as u64);
            if tx.timestamp_ms.saturating_add(self.ttl_ms) > now_ms() {
                self.idem_index.insert(
                    (tx.idempotency_scope.clone(), tx.idempotency_key.clone()),
                    IdemRec { tx_id: tx.tx_id.clone(), expires_ms: tx.timestamp_ms.saturating_add(self.ttl_ms) },
                );
                n += 1;
            } else {
                dropped += 1;
            }
        }
        Ok((n, dropped))
    }

    /// Drop expired idempotency keys, at most once per 60s. Called per writer batch.
    pub fn sweep_if_due(&mut self) {
        if self.last_sweep.elapsed().as_secs() < 60 {
            return;
        }
        self.last_sweep = std::time::Instant::now();
        let now = now_ms();
        self.idem_index.retain(|_, r| r.expires_ms > now);
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
