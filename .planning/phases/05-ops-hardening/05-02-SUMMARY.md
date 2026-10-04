# Summary 05-02: Soak pilot + scripted ops checks

**Status:** complete

## What was built
- `scripts/soak.ps1`: matrix 100/500/2000 tx × 1/10/50 conc with mid-run kill-9, then full suite + all test-bank modes. Results committed in `SOAK.md`.
- `tests/ops.rs`: 4 gating checks (reconcile-UNKNOWN, DLQ sweep over 3 reasons, restart zero-loss, bank key rotation), each with PASS lines, non-zero exit on failure.

## Soak results (debug, Windows disk, 4 partitions)

| n | conc | tps | p50 | p99 |
|---|------|-----|-----|-----|
| 100 | 1/10/50 | 112/411/1573 | 8/19/25ms | 20/70/52ms |
| 500 | 1/10/50 | 101/486/1785 | 8/19/23ms | 28/50/62ms |
| 2000 | 1/10/50 | 60/82/63 | 8/22/472ms | 101/952/2596ms |

- Zero loss at every cell (all ok=N). The 2000/50 tail (p99 2.6s) is Little's Law on a single disk: ~500 queued per partition × 7ms fsync. Predictable degradation, not a cliff — release build + faster disk move it, not more writers.

## Verification
- `cargo test`: 36/36 (incl. 4 ops gates + 2 backup tests). `test-bank` all modes PASS.
