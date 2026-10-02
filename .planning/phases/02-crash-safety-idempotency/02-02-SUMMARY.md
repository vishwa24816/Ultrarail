# Summary 02-02: Per-partition recovery + kill-9 proof

**Status:** complete

## What was built
- Boot path (from 02-01) verified: `spawn_writers` replays each `journal-{i}.wal` independently, logs counts, aborts boot (`?`) on any corrupt partition — fail-closed.
- `tests/recovery.rs`: `kill9_loses_nothing_acked` (50 txs, kill, respawn, all 50 keys resolve to identical tx_ids) and `torn_tail_discarded` (hand-scribbled garbage tails on all 4 partitions, server boots, good records intact).

## Verification
- `cargo test --test recovery`: 2/2 pass. Logs show per-partition replay (11/14/8/14) after kill.
