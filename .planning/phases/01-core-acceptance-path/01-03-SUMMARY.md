# Summary 01-03: Acceptance proof + metrics hooks

**Status:** complete

## What was built
- `tests/acceptance.rs`: 3 tests mapping 1:1 to Phase 1 success criteria (durable accept, unbalanced→422, invalid→REJECTED). Spawns the real binary on ephemeral ports.
- `src/metrics.rs`: `payment_validation_ms`, `payment_journal_sync_ms`, `payment_accept_total_ms` histograms + accepted/rejected counters (exporter wiring deferred to Phase 5).

## Verification
- `cargo test`: 8/8 pass (5 unit + 3 acceptance) in ~0.5s.
