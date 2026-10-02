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

pub fn delivery_attempt(kind: &str) {
    metrics::counter!("delivery_attempts_total", "kind" => kind.to_string()).increment(1);
}

pub fn delivery_settled() {
    metrics::counter!("delivery_settled_total").increment(1);
}

pub fn delivery_failed(reason: &str) {
    metrics::counter!("delivery_failed_total", "reason" => reason.to_string()).increment(1);
}

pub fn delivery_unknown() {
    metrics::counter!("delivery_unknown_total").increment(1);
}

pub fn audit_dropped() {
    metrics::counter!("audit_dropped_total").increment(1);
}

pub fn queue_depth(partition: usize, n: f64) {
    metrics::gauge!("writer_queue_depth", "partition" => partition.to_string()).set(n);
}
