//! Matcher: rail confirmations become settlements exactly once.
//! Exact match on (rail_ref present + amount + currency + counterparty +
//! value_date). Anything less confident goes to the exception queue.
//! The per-partition settled-set lives in the writer task: check + record in
//! one turn, rebuilt from event replay on boot.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// A rail confirmation. All rail-side fields are optional: absent fields mean
/// a non-confident confirmation (old producers), which must never auto-settle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RailConfirm {
    pub rail_ref: Option<String>,
    pub amount: Option<i64>,
    pub currency: Option<String>,
    pub counterparty: Option<String>,
    pub value_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchResult {
    Confident,
    NearMiss(String),
}

pub fn match_confirm(
    tx_amount: i64,
    tx_currency: &str,
    tx_credit: &str,
    tx_day: u64,
    c: &RailConfirm,
) -> MatchResult {
    let missing = |f: &str| MatchResult::NearMiss(format!("missing rail field: {f}"));
    let rail_ref = match &c.rail_ref {
        Some(r) if !r.is_empty() => r,
        _ => return missing("rail_ref"),
    };
    let _ = rail_ref; // presence proves a real rail identifier; equality is N/A (ours is internal)
    macro_rules! eq {
        ($got:expr, $want:expr, $name:literal) => {
            match $got {
                Some(v) if v == $want => {},
                Some(_) => return MatchResult::NearMiss(format!("mismatch: {}", $name)),
                None => return missing($name),
            }
        };
    }
    eq!(c.amount, tx_amount, "amount");
    eq!(c.currency.as_deref(), tx_currency, "currency");
    eq!(c.counterparty.as_deref(), tx_credit, "counterparty");
    eq!(c.value_date.as_deref(), &tx_day.to_string(), "value_date");
    MatchResult::Confident
}

/// A settle request: delivery/bank asks the owning writer to settle.
/// The writer checks the settled-set, matches, then records — one turn.
#[derive(Debug, Clone)]
pub struct SettleReq {
    pub tx_id: String,
    pub confirm: RailConfirm,
}

/// Owned by the partition writer. Check + record happen in one turn: no race.
pub struct SettledSet {
    inner: HashSet<String>,
}

impl SettledSet {
    pub fn new() -> Self {
        Self { inner: HashSet::new() }
    }

    pub fn rebuild<'a>(tx_ids: impl Iterator<Item = &'a str>) -> Self {
        Self { inner: tx_ids.map(|s| s.to_string()).collect() }
    }

    /// True if this confirmation settles (first time). False = duplicate.
    pub fn settle(&mut self, tx_id: &str) -> bool {
        self.inner.insert(tx_id.to_string())
    }

    /// Release a guard insert when matching fails after the check.
    pub fn un_settle(&mut self, tx_id: &str) {
        self.inner.remove(tx_id);
    }

    pub fn contains(&self, tx_id: &str) -> bool {
        self.inner.contains(tx_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confirm() -> RailConfirm {
        RailConfirm {
            rail_ref: Some("rail-1".into()),
            amount: Some(100),
            currency: Some("USD".into()),
            counterparty: Some("m:9".into()),
            value_date: Some("43123".into()),
        }
    }

    #[test]
    fn exact_is_confident() {
        assert_eq!(match_confirm(100, "USD", "m:9", 43123, &confirm()), MatchResult::Confident);
    }

    #[test]
    fn missing_field_never_settles() {
        let mut c = confirm();
        c.rail_ref = None;
        assert!(matches!(match_confirm(100, "USD", "m:9", 43123, &c), MatchResult::NearMiss(_)));
    }

    #[test]
    fn amount_mismatch_is_near_miss() {
        let mut c = confirm();
        c.amount = Some(99);
        assert_eq!(
            match_confirm(100, "USD", "m:9", 43123, &c),
            MatchResult::NearMiss("mismatch: amount".into())
        );
    }

    #[test]
    fn settled_set_dedupes() {
        let mut s = SettledSet::new();
        assert!(s.settle("a"));
        assert!(!s.settle("a"));
    }
}
