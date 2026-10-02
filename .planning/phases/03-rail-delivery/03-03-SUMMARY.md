# Summary 03-03: Prometheus exporter + audit log

**Status:** complete

## What was built
- Exporter: `metrics-exporter-prometheus` on `PROM_ADDR` (default 127.0.0.1:9000); delivery counters by kind/reason, queue-depth gauges per partition. Label test proves no tx_id/key/account leaks into labels.
- `src/audit.rs`: `audit.wal` dispute-grade trail (tx, partition, lsn, from→to, actor, reason); hooks on accept/replay-hit + all delivery transitions; best-effort with `audit_dropped_total`; synced on shutdown. `GET /audit?tx_id=` operator view.
- Fixed along the way: supervisor-owned channels (requests survive restarts instead of 503ing on a dead placeholder); readiness-gated spawn helpers; TraceLayer failure-log silenced.

## Verification
- `cargo test`: 21/21 (incl. `audit_trail_covers_lifecycle_and_labels_stay_bounded`).
- Bench regression gate: re-run in Wave 4 commit (auth adds middleware cost).
