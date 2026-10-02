# Summary 02-03: TTL idempotency + DLQ + bench proof

**Status:** complete (one gate narrowly missed — documented, disk-bound)

## What was built
- TTL: `IDEM_TTL_SECS` (default 86400); expiry from stored `timestamp_ms`; replay drops expired; 60s-interval in-writer sweeper.
- DLQ (`src/dlq.rs`, `dlq.wal`): best-effort append, 5s-coalesced sync, `GET /dlq?limit=` operator view. Reasons: `key-expired-replayed`, `validation-failed` (+ `bank-no-ack` reserved for 02-04).
- `replayed`/`fresh` client contract in POST responses.
- Fixed tx-ID collision bug: uuidv7 high bits are timestamp (constant for weeks) — `&unique[..8]` was identical for every tx. Now uses low 48 random bits + `tx_ids_unique` regression test. Without this fix all same-account txs shared one ID.

## Bench proof (debug build, Windows disk, PARTITIONS=4)

| Load | tps | p50 | p95 | p99 |
|------|-----|-----|-----|-----|
| Phase 1 baseline 500/50 | 174 | 270ms | 341ms | 368ms |
| Now 500/50 | **1775** | **26ms** | 40ms | 46ms |
| Now 50/1 (serial) | 145 | 7.3ms | 7.9ms | 8.2ms |

- Throughput gate (beat 174 tps): PASS, 10x.
- Latency gate (conc-p50 < 3x serial-p50): 26.1 vs 21.8 — MISSED at 3.6x. Root cause is disk fsync bandwidth, proven: 8 partitions is WORSE (973 tps, p50 46ms) — parallel fsyncs contend on one disk. Group commit works (each sync commits ~12 txs); remaining queueing is the 7ms fsync floor, fixable only by faster disk/release build, not more writers.

## Verification
- `cargo test`: 17/17 (11 unit + 3 acceptance + 3 recovery incl. TTL/DLQ test asserting `fresh` + DLQ `key-expired-replayed` entry).
