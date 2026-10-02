//! Metrics hooks: per-stage latency histograms + counters (exporter wired in Phase 5).

pub fn observe_validation(ms: f64) {
    metrics::histogram!("payment_validation_ms").record(ms);
}

pub fn observe_journal_sync(ms: f64) {
    metrics::histogram!("payment_journal_sync_ms").record(ms);
}

pub fn observe_total(ms: f64) {
    metrics::histogram!("payment_accept_total_ms").record(ms);
}

pub fn count_accepted() {
    metrics::counter!("payments_accepted_total").increment(1);
}

pub fn count_rejected() {
    metrics::counter!("payments_rejected_total").increment(1);
}
