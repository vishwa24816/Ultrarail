//! Bank account registry: balances + freeze/risk flags, fed by bank WS events.
//! Delivery consults this BEFORE submitting: frozen/risky/insufficient fail
//! terminally without touching the rail.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AcctState {
    pub balance: i64,
    pub debit_frozen: bool,
    pub credit_frozen: bool,
    pub totally_frozen: bool,
    pub risky: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    InsufficientFunds,
    RiskyClient,
    FrozenDebit,
    FrozenCredit,
    FrozenTotal,
}

impl Terminal {
    pub fn code(self) -> &'static str {
        match self {
            Terminal::InsufficientFunds => "INSUFFICIENT_FUNDS",
            Terminal::RiskyClient => "RISKY_CLIENT",
            Terminal::FrozenDebit => "FROZEN_DEBIT",
            Terminal::FrozenCredit => "FROZEN_CREDIT",
            Terminal::FrozenTotal => "FROZEN_TOTAL",
        }
    }

    pub fn message(self) -> String {
        format!("transaction failed for {}", self.code())
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, AcctState>>>,
}

impl Registry {
    pub fn set_balance(&self, account: &str, balance: i64) {
        if let Ok(mut m) = self.inner.lock() {
            m.entry(account.to_string()).or_default().balance = balance;
        }
    }

    /// flag: debit_frozen | credit_frozen | totally_frozen | risky
    pub fn set_flag(&self, account: &str, flag: &str, on: bool) {
        if let Ok(mut m) = self.inner.lock() {
            let a = m.entry(account.to_string()).or_default();
            match flag {
                "debit_frozen" => a.debit_frozen = on,
                "credit_frozen" => a.credit_frozen = on,
                "totally_frozen" => a.totally_frozen = on,
                "risky" => a.risky = on,
                _ => {}
            }
        }
    }

    /// Pre-submit gate. Returns the terminal reason, or None to proceed.
    pub fn check(&self, debit: &str, credit: &str, amount: i64) -> Option<Terminal> {
        let m = self.inner.lock().ok()?;
        let d = m.get(debit);
        let c = m.get(credit);
        if d.map(|a| a.totally_frozen).unwrap_or(false) || c.map(|a| a.totally_frozen).unwrap_or(false) {
            return Some(Terminal::FrozenTotal);
        }
        if d.map(|a| a.debit_frozen).unwrap_or(false) {
            return Some(Terminal::FrozenDebit);
        }
        if c.map(|a| a.credit_frozen).unwrap_or(false) {
            return Some(Terminal::FrozenCredit);
        }
        if d.map(|a| a.risky).unwrap_or(false) || c.map(|a| a.risky).unwrap_or(false) {
            return Some(Terminal::RiskyClient);
        }
        // Unknown accounts pass (banks create them); known debit must cover.
        if let Some(a) = d {
            if a.balance < amount {
                return Some(Terminal::InsufficientFunds);
            }
        }
        None
    }

    pub fn snapshot(&self, account: &str) -> AcctState {
        self.inner.lock().ok().and_then(|m| m.get(account).cloned()).unwrap_or_default()
    }
}
