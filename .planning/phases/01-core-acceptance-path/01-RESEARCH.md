# Phase 1: Core acceptance path - Research

**Researched:** 2026-10-02 (web + prior discussion)

## Stack (locked, prod-perf)

| Layer | Choice | Why |
|---|---|---|
| Runtime | tokio 1.x full, multi-thread | Standard async runtime; `spawn_blocking` for CPU-heavy validation |
| HTTP | axum 0.7 + tower-http | 2026 prod-ledger standard; Tower tracing/limits/cors; low overhead |
| Journal | `wal-db` (preferred) else `open-wal` | Group-commit coalesces fsync (~1.9x vs naive fsync-per-commit; ~3.5k commits/s grouped); explicit append vs sync boundary; CRC32C + torn-tail recovery |
| IDs | uuid v7 | Time-ordered, collision-resistant |
| Money | i64 minor units + Currency enum | Exact, fastest; no float rounding; rust_decimal deferred to FX need |
| Serde | serde + serde_json | Record encode; checksum via crc32c crate |
| Errors | thiserror | Typed errors, no panics on hot path |
| Validation | validator | Derive-based request validation |
| Time | unix-ms u64 (no chrono dep) | One integer timestamp; avoid dep bloat |
| Obs | tracing + tracing-subscriber; metrics crate + prometheus exporter (Phase 5 wires dashboards, Phase 1 emits counters/histograms) | Per-stage P50/P95/P99 ready |

## Key findings

- Durability boundary: `append` = page-cache only; `sync/commit` = fdatasync watermark. Ack ONLY after watermark. Never confuse the two — the #1 WAL data-loss cause.
- fsync cost dominates (~50µs–1ms); group-commit is the throughput lever. Single writer per partition; lock-free append, shared fsync.
- Commit is NOT atomic across records: all-or-nothing tx = ONE record. Journal record = full tx (JRN-03 fields) in a single append.
- Recovery: replay from LSN 0, stop at first checksum failure, discard torn tail. Indexes (tx_id→offset, idem-key→tx) rebuilt in memory (BTreeMap) — no second source of truth.
- axum pattern: state via `Arc<AppState>` (journal handle + mpsc bounded sender to single writer task); validation in extractor/`validator`; heavy work in `spawn_blocking`.

## Pitfalls to avoid

- Blocking the Tokio worker (sync fsync on handler thread) — use dedicated writer task + oneshot reply.
- Float amounts, unbalanced postings committed, ack-before-sync, blind resubmit (Phase 3 owns retries; Phase 1 just records state correctly).
- `open-wal` is single-writer per dir + pre-1.0 API churn; `wal-db` supports multi-writer append + group commit. Either OK; pin exact version in Cargo.lock.

## What planner must enforce

- Every plan task has `read_first` + `acceptance_criteria` with concrete assertions/commands.
- JRN-01/JRN-03/LDG-01 each appear in a plan's `requirements` field; 100% coverage.
- Threat model block in each PLAN.md (auth deferred to Phase 5 but Phase 1 must not bake in unauthenticated assumptions; note PAN prohibition).
