//! Domain types: money, ledger entries, transaction lifecycle.
//! Invariant: postings balance per currency; amounts are i64 minor units (never float).

use serde::{Deserialize, Serialize};
use thiserror::Error;
use validator::Validate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    USD,
    EUR,
    GBP,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    /// Total in minor units (cents). Must equal price * quantity, and be > 0.
    pub amount: i64,
    /// Unit price in minor units. Part of the transaction value.
    pub price: i64,
    /// Number of units. Defaults to 1.
    pub quantity: u64,
    pub currency: Currency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub account: String,
    pub debit: i64,
    pub credit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TxStatus {
    Received,
    Validated,
    AcceptedDurable,
    Rejected,
    /// Submitted to rail, outcome unknown — reconcile before resubmitting.
    Unknown,
    /// Rail confirmed. Terminal.
    Settled,
    /// Terminal failure with reason code (INSUFFICIENT_FUNDS, RISKY_CLIENT,
    /// FROZEN_DEBIT, FROZEN_CREDIT, FROZEN_TOTAL). Never retried.
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentTx {
    pub tx_id: String, // UUIDv7
    pub idempotency_scope: String,
    pub idempotency_key: String,
    pub request_hash: u64,
    pub money: Money,
    pub entries: Vec<LedgerEntry>,
    pub status: TxStatus,
    pub timestamp_ms: u64,
    pub checksum: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("amount must be positive, got {0}")]
    BadAmount(i64),
    #[error("price must be positive, got {0}")]
    BadPrice(i64),
    #[error("amount {amount} != price {price} * quantity {quantity}")]
    PriceMismatch { amount: i64, price: i64, quantity: u64 },
    #[error("postings do not balance: debits={debits} credits={credits}")]
    Unbalanced { debits: i64, credits: i64 },
    #[error("entry amounts must be non-negative")]
    NegativeEntry,
    #[error("account name invalid: {0}")]
    BadAccount(String),
}

pub fn validate_account(name: &str) -> bool {
    // ponytail: allowlist charset+length instead of regex dep
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':')
}

impl PaymentTx {
    /// Enforce ledger invariants before anything touches the journal.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.money.amount <= 0 {
            return Err(DomainError::BadAmount(self.money.amount));
        }
        if self.money.price <= 0 {
            return Err(DomainError::BadPrice(self.money.price));
        }
        match self.money.price.checked_mul(self.money.quantity as i64) {
            Some(total) if total == self.money.amount => {}
            _ => {
                return Err(DomainError::PriceMismatch {
                    amount: self.money.amount,
                    price: self.money.price,
                    quantity: self.money.quantity,
                })
            }
        }
        let mut debits: i64 = 0;
        let mut credits: i64 = 0;
        for e in &self.entries {
            if e.debit < 0 || e.credit < 0 {
                return Err(DomainError::NegativeEntry);
            }
            if !validate_account(&e.account) {
                return Err(DomainError::BadAccount(e.account.clone()));
            }
            debits = debits.saturating_add(e.debit);
            credits = credits.saturating_add(e.credit);
        }
        if debits != credits || debits != self.money.amount {
            return Err(DomainError::Unbalanced { debits, credits });
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct CreatePayment {
    #[validate(length(min = 1, max = 64))]
    pub idempotency_scope: String,
    #[validate(length(min = 1, max = 128))]
    pub debit_account: String,
    #[validate(length(min = 1, max = 128))]
    pub credit_account: String,
    pub amount: i64,
    pub currency: Currency,
    /// Unit price in minor units. Defaults to `amount` (single-unit tx).
    pub price: Option<i64>,
    /// Number of units. Defaults to 1.
    pub quantity: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_tx() -> PaymentTx {
        PaymentTx {
            tx_id: "01900000-0000-7000-8000-000000000000".into(),
            idempotency_scope: "payments".into(),
            idempotency_key: "k1".into(),
            request_hash: 42,
            money: Money { amount: 100, price: 100, quantity: 1, currency: Currency::USD },
            entries: vec![
                LedgerEntry { account: "user:1".into(), debit: 100, credit: 0 },
                LedgerEntry { account: "merchant:9".into(), debit: 0, credit: 100 },
            ],
            status: TxStatus::Received,
            timestamp_ms: 1,
            checksum: 0,
        }
    }

    #[test]
    fn balanced_passes() {
        assert!(ok_tx().validate().is_ok());
    }

    #[test]
    fn unbalanced_rejected() {
        let mut tx = ok_tx();
        tx.entries[1].credit = 99;
        assert_eq!(
            tx.validate(),
            Err(DomainError::Unbalanced { debits: 100, credits: 99 })
        );
    }

    #[test]
    fn zero_amount_rejected() {
        let mut tx = ok_tx();
        tx.money.amount = 0;
        assert_eq!(tx.validate(), Err(DomainError::BadAmount(0)));
    }

    #[test]
    fn price_mismatch_rejected() {
        let mut tx = ok_tx();
        tx.money.price = 60; // 60*1 != 100
        assert_eq!(
            tx.validate(),
            Err(DomainError::PriceMismatch { amount: 100, price: 60, quantity: 1 })
        );
    }

    #[test]
    fn price_times_quantity_passes() {
        let mut tx = ok_tx();
        tx.money.price = 50;
        tx.money.quantity = 2;
        assert!(tx.validate().is_ok());
    }
}
