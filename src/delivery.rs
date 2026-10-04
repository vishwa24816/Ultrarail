//! Delivery worker: one task per partition. Submits durable txs to the rail,
//! retries transients with capped backoff, parks timeouts as UNKNOWN and
//! reconciles before any resubmit. Terminal bank failures never retry.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::accounts::{Registry, Terminal};
use crate::dlq::{now_ms, Dlq, DlqEntry};
use crate::domain::PaymentTx;
use crate::events::{event, EventKind, EventLog};
use crate::metrics;

// ---- Rail adapter ----

pub enum SubmitOutcome {
    Acked(String),
    Transient(String),
    Timeout,
    Terminal(Terminal, String),
}

pub enum QueryOutcome {
    Settled,
    Failed(Terminal),
    StillUnknown,
}

pub trait Rail: Send + Sync {
    fn submit(&self, tx: &PaymentTx) -> impl std::future::Future<Output = SubmitOutcome> + Send;
    fn query(&self, tx_id: &str) -> impl std::future::Future<Output = QueryOutcome> + Send;
}

/// Simulated rail with env failure injection. Real adapter implements `Rail`.
pub struct SimRail {
    pub fail_rate: f64,
    pub timeout_rate: f64,
    pub latency_ms: u64,
    pub submits: Mutex<HashMap<String, u32>>,
}

impl SimRail {
    pub fn from_env() -> Self {
        let f = |k: &str, d: f64| std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d);
        let u = |k: &str, d: u64| std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d);
        Self {
            fail_rate: f("SIM_FAIL_RATE", 0.0),
            timeout_rate: f("SIM_TIMEOUT_RATE", 0.0),
            latency_ms: u("SIM_LATENCY_MS", 20),
            submits: Mutex::new(HashMap::new()),
        }
    }

    fn roll(&self) -> f64 {
        // ponytail: nanos-based roll, no rand dep
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0) % 1000) as f64
            / 1000.0
    }
}

impl Rail for SimRail {
    async fn submit(&self, tx: &PaymentTx) -> SubmitOutcome {
        if let Ok(mut m) = self.submits.lock() {
            *m.entry(tx.tx_id.clone()).or_insert(0) += 1;
        }
        tokio::time::sleep(Duration::from_millis(self.latency_ms)).await;
        if self.roll() < self.timeout_rate {
            // Simulate a hung rail: sleep past any caller timeout.
            tokio::time::sleep(Duration::from_secs(3600)).await;
            return SubmitOutcome::Timeout;
        }
        if self.roll() < self.fail_rate {
            return SubmitOutcome::Transient("sim transient".into());
        }
        SubmitOutcome::Acked(format!("sim-{}", tx.tx_id))
    }

    async fn query(&self, _tx_id: &str) -> QueryOutcome {
        // Simulated rail always knows: a submitted tx settled.
        QueryOutcome::Settled
    }
}

// ---- Worker ----

#[derive(Debug, Clone, PartialEq, Eq)]
enum DState {
    Queued,
    Unknown { since_ms: u64 },
    Settled,
    Failed(String),
}

struct Work {
    tx: PaymentTx,
    state: DState,
    attempts: u32,
    next_retry: Instant,
    inflight: bool,
}

pub struct Delivery {
    pub inbox: mpsc::Receiver<PaymentTx>,
    pub failed_rx: tokio::sync::broadcast::Receiver<(String, Terminal)>,
    pub settle_tx: mpsc::Sender<crate::matcher::SettleReq>,
    pub events: Arc<Mutex<EventLog>>,
    pub dlq: Arc<Mutex<Dlq>>,
    pub registry: Registry,
    pub rail: SimRail,
    pub rail_timeout: Duration,
    pub reconcile_timeout_ms: u64,
    pub partition: usize,
    /// Attempt counts rebuilt from the event log on restart (budget survives crashes).
    pub boot_attempts: HashMap<String, u32>,
    pub shutdown: tokio::sync::watch::Receiver<bool>,
    pub audit: Arc<Mutex<crate::audit::Audit>>,
}

fn backoff(attempt: u32) -> Duration {
    let ms = 100u64.saturating_mul(2u64.saturating_pow(attempt.min(8)));
    let capped = ms.min(30_000);
    let jitter = (now_ms() % 100) * capped / 100 / 2;
    Duration::from_millis(capped / 2 + jitter)
}

impl Delivery {
    pub async fn run(mut self) {
        let mut pending: HashMap<String, Work> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut reconcile_tick = tokio::time::interval(Duration::from_secs(5));
        let mut closed = false;
        loop {
            if closed && queue.is_empty() {
                self.sync_events();
                break;
            }
            tokio::select! {
                msg = self.inbox.recv(), if !closed => {
                    match msg {
                        Some(tx) => {
                            let id = tx.tx_id.clone();
                            if !pending.contains_key(&id) {
                                let attempts = self.boot_attempts.remove(&id).unwrap_or(0);
                                pending.insert(id.clone(), Work {
                                    tx, state: DState::Queued, attempts,
                                    next_retry: Instant::now(), inflight: false,
                                });
                                queue.push_back(id);
                            }
                        }
                        None => { closed = true; }
                    }
                }
                Ok((id, term)) = self.failed_rx.recv() => {
                    // Bank-declared terminal failure: act only on own txs, never retry.
                    metrics::delivery_failed(term.code());
                    if let Some(w) = pending.get_mut(&id) {
                        w.state = DState::Failed(term.code().into());
                        self.log(&id, EventKind::Failed, term.message(), "bank");
                    }
                    self.sync_events();
                }
                _ = reconcile_tick.tick() => {
                    self.reconcile(&mut pending).await;
                    self.sync_events();
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    self.pump(&mut pending, &mut queue).await;
                    self.sync_events();
                }
                _ = self.shutdown.changed() => {
                    // Drain: finish everything queued, sync, exit. Main enforces the deadline.
                    closed = true;
                }
            }
        }
    }

    /// Submit due txs. One submit per tx at a time (inflight flag) — no double-submit.
    async fn pump(&mut self, pending: &mut HashMap<String, Work>, queue: &mut VecDeque<String>) {
        let now = Instant::now();
        let mut due: Vec<String> = vec![];
        for id in queue.iter() {
            if let Some(w) = pending.get(id) {
                if !w.inflight && matches!(w.state, DState::Queued) && w.next_retry <= now {
                    due.push(id.clone());
                }
            }
            if due.len() >= 32 {
                break;
            }
        }
        for id in due {
            // Pre-submit gate: frozen / risky / insufficient fail terminally, no rail contact.
            let term = {
                let w = match pending.get(&id) {
                    Some(w) => w,
                    None => continue,
                };
                self.registry.check(
                    &w.tx.entries.first().map(|e| e.account.clone()).unwrap_or_default(),
                    &w.tx.entries.get(1).map(|e| e.account.clone()).unwrap_or_default(),
                    w.tx.money.amount,
                )
            };
                if let Some(t) = term {
                if let Some(w) = pending.get_mut(&id) {
                    w.state = DState::Failed(t.code().into());
                    self.log(&id, EventKind::Failed, t.message(), "system");
                }
                queue.retain(|x| x != &id);
                continue;
            }
            if let Some(w) = pending.get_mut(&id) {
                w.inflight = true;
                w.attempts += 1;
            }
            let tx = match pending.get(&id) {
                Some(w) => w.tx.clone(),
                None => continue,
            };
            let attempt_no = pending.get(&id).map(|w| w.attempts).unwrap_or(0);
            let res = tokio::time::timeout(self.rail_timeout, self.rail.submit(&tx)).await;
            match res {
                Ok(SubmitOutcome::Acked(rail_ref)) => {
                    metrics::delivery_settled();
                    // Settlement itself goes through the matcher's settled-set guard
                    // in the writer task (single owner, no race). Delivery only routes.
                    let tx_day = tx.timestamp_ms / 86_400_000;
                    let confirm = crate::matcher::RailConfirm {
                        rail_ref: Some(rail_ref),
                        amount: Some(tx.money.amount),
                        currency: Some(format!("{:?}", tx.money.currency)),
                        counterparty: tx.entries.get(1).map(|e| e.account.clone()),
                        value_date: Some(tx_day.to_string()),
                    };
                    let _ = self.settle_tx.send(crate::matcher::SettleReq { tx_id: id.clone(), confirm }).await;
                    self.log(&id, EventKind::Settled, format!("attempt {attempt_no} acked, routed to matcher"), "rail");
                    queue.retain(|x| x != &id);
                }
                Ok(SubmitOutcome::Transient(d)) => {
                    metrics::delivery_attempt("transient");
                    if let Some(w) = pending.get_mut(&id) {
                        let n = w.attempts;
                        w.inflight = false;
                        w.next_retry = Instant::now() + backoff(n);
                        if n >= 10 {
                            w.state = DState::Failed("RETRY_EXHAUSTED".into());
                            metrics::delivery_failed("RETRY_EXHAUSTED");
                            self.log(&id, EventKind::Failed, "retry budget exhausted", "system");
                            queue.retain(|x| x != &id);
                            continue;
                        }
                    }
                    self.log(&id, EventKind::Attempt, format!("attempt {attempt_no} transient: {d}"), "rail");
                }
                Ok(SubmitOutcome::Timeout) | Err(_) => {
                    metrics::delivery_unknown();
                    // Timeout after submit: UNKNOWN until reconcile says otherwise. Never resubmit blind.
                    if let Some(w) = pending.get_mut(&id) {
                        w.inflight = false;
                        w.state = DState::Unknown { since_ms: now_ms() };
                    }
                    self.log(&id, EventKind::Unknown, format!("attempt {attempt_no} timeout"), "system");
                }
                Ok(SubmitOutcome::Terminal(t, d)) => {
                    metrics::delivery_failed(t.code());
                    if let Some(w) = pending.get_mut(&id) {
                        w.state = DState::Failed(t.code().into());
                        w.inflight = false;
                    }
                    self.log(&id, EventKind::Failed, format!("{}: {d}", t.message()), "rail");
                    queue.retain(|x| x != &id);
                }
            }
        }
    }

    async fn reconcile(&mut self, pending: &mut HashMap<String, Work>) {
        // NOTE: a real network rail fans these queries out concurrently;
        // the sim answers instantly so sequential await is fine here.
        let unknowns: Vec<String> = pending
            .iter()
            .filter(|(_, w)| matches!(w.state, DState::Unknown { .. }))
            .map(|(k, _)| k.clone())
            .collect();
        for id in unknowns {
            let since = match pending.get(&id) {
                Some(w) => match w.state {
                    DState::Unknown { since_ms } => since_ms,
                    _ => continue,
                },
                None => continue,
            };
            let q = self.rail.query(&id).await;
            match q {
                QueryOutcome::Settled => {
                    if let Some(w) = pending.get_mut(&id) {
                        w.state = DState::Settled;
                    }
                    self.log(&id, EventKind::Reconciled, "rail confirmed settled", "rail");
                }
                QueryOutcome::Failed(t) => {
                    if let Some(w) = pending.get_mut(&id) {
                        w.state = DState::Failed(t.code().into());
                    }
                    self.log(&id, EventKind::Failed, t.message(), "rail");
                }
                QueryOutcome::StillUnknown => {
                    if now_ms().saturating_sub(since) > self.reconcile_timeout_ms {
                        if let Some(w) = pending.get_mut(&id) {
                            w.state = DState::Failed("UNKNOWN_UNRESOLVED".into());
                        }
                        self.log(&id, EventKind::Failed, "unknown past reconcile timeout", "system");
                        if let Ok(mut d) = self.dlq.lock() {
                            d.push(DlqEntry {
                                reason: "unknown-unresolved".into(),
                                scope: String::new(),
                                key: String::new(),
                                tx_id: Some(id.clone()),
                                detail: "reconcile timeout with rail".into(),
                                at_ms: now_ms(),
                            });
                            d.flush();
                        }
                    }
                }
            }
        }
    }

    fn log(&self, id: &str, kind: EventKind, detail: impl Into<String>, actor: &str) {
        let detail = detail.into();
        if let Ok(mut e) = self.events.lock() {
            let _ = e.append(&event(id, kind.clone(), detail.clone()));
        }
        let (from, to) = match kind {
            EventKind::Submitted => ("RECEIVED", "QUEUED"),
            EventKind::Attempt => ("QUEUED", "INFLIGHT"),
            EventKind::Unknown => ("INFLIGHT", "UNKNOWN"),
            EventKind::Reconciled => ("UNKNOWN", "SETTLED"),
            EventKind::Settled => ("QUEUED", "SETTLED"),
            EventKind::Failed => ("QUEUED", "FAILED"),
        };
        if let Ok(mut a) = self.audit.lock() {
            a.record(&crate::audit::entry(id, self.partition, 0, from, to, actor, &detail));
        }
    }

    fn sync_events(&self) {
        if let Ok(mut e) = self.events.lock() {
            let _ = e.sync();
        }
    }

    /// Snapshot for GET /payments/:id.
    pub fn snapshot(events: &Arc<Mutex<EventLog>>, tx_id: &str) -> Vec<crate::events::TxEvent> {
        events
            .lock()
            .ok()
            .map(|e| e.replay().into_iter().filter(|ev| ev.tx_id == tx_id).collect())
            .unwrap_or_default()
    }
}
