# Phase 2: Crash safety + idempotency - Context

**Gathered:** 2026-10-02
**Status:** Ready for planning

## Phase Boundary

Refactor single-writer journal into a partitioned multi-writer pipeline; prove kill-9 recovery with zero loss of acked txs; enforce idempotency with TTL + dead-letter queue. (JRN-02, IDM-01, plus the concurrency refactor)

## Implementation Decisions

### Writer pipeline (partitioned multi-writer)
- **D-01:** Partitioned writers: N journal partitions (wal-db WAL each), each with own writer task + group-commit sync coalescing. Parallel fsyncs across partitions.
- **D-02:** Partition key = hash(time_bucket, sender, receiver) mod N. Same pair in the same window lands in the same journal (good for matching/netting later).
- **D-03:** Structured tx ID embeds routing data: `{timebucket}-{sender}-{receiver}-{unique}` (unique = uuid v7 short or LSN). Router parses or hashes the same fields — ID and partition key never disagree.
- **D-04:** Throughput scales with N until disk fsync bandwidth saturates; N configurable via env (default = num_cpus / 2). Correctness note: no global cross-partition ordering — sequence is per-partition LSN + wall-clock bucket.

### Recovery (per-partition index)
- **D-05:** Per-partition indexes, no global merge: each partition replays its own WAL on boot and rebuilds its own tx_id→lsn + (scope,key)→tx_id maps. Router consults the owning partition.
- **D-06:** Kill-9 test: all acked txs present in owning partitions after restart; uncommitted tails discarded per-partition.

### Idempotency (TTL + DLQ)
- **D-07:** Idempotency keys carry TTL (default 24h, configurable). Expired entries are swept lazily on access + by a background sweeper.
- **D-08:** Dead-letter queue: retries/expiries that can't resolve (e.g. key expired but client retries expecting original, or poison records failing validation on replay) go to a DLQ journal segment with reason + original payload, visible to operators. DLQ is append-only, never auto-retried.
- **D-09:** Same scope+key within TTL returns the original tx (no duplicate payment). After TTL, same key is treated as a new payment — document this contract in the API response/header.

### Bank connectivity (websockets, both sides)
- **D-10:** Two websocket planes on axum (`WebSocketUpgrade`, no new framework): client WS (`/ws/client`) for submit + receipts, bank WS (`/ws/bank`) for settlement events. HTTP POST stays as fallback.
- **D-11:** Acked delivery: rail streams `{tx_id, status, money, entries}` per event; bank replies `{tx_id, ack}`. Unacked past timeout → DLQ with reason `bank-no-ack` (feeds D-08).
- **D-12:** Dummy test bank = in-repo binary (`src/bin/test-bank.rs`) playing BOTH sides: connects as sender-bank and receiver-bank, validates receipts against submitted amounts, sends acks, asserts balance conservation. Production-level rehearsal harness, owned by Phase 2.
- **D-13:** Phase 3 handoff: real rail adapter replaces the dummy bank behind the same WS message protocol — protocol frozen here, transport reused there.

### Carried forward (locked in Phase 1, not re-asked)
- axum + Tokio, wal-db substrate, i64 minor units + Currency enum, UUIDv7 uniqueness core, `Idempotency-Key` header, JSON status envelope, sync-before-ack per partition.

### the agent's Discretion
- Exact N default, time-bucket width (minute vs hour), TTL sweep interval, DLQ segment rotation, tx-ID text encoding (readable vs compact).

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design
- `p.md` — state machine, journal format, idempotency rules, UNKNOWN handling
- `.planning/PROJECT.md` — core value (exactly-once, no silent loss)
- `.planning/REQUIREMENTS.md` — JRN-02, IDM-01 acceptance criteria
- `.planning/ROADMAP.md` — Phase 2 goal + success criteria
- `.planning/phases/01-core-acceptance-path/01-CONTEXT.md` — Phase 1 locked stack
- `src/journal.rs`, `src/app_state.rs` — current single-writer code being refactored

## Existing Code Insights

### Reusable Assets
- `src/domain.rs` — Money/LedgerEntry/validate() unchanged; tx-ID construction extends it.
- `src/journal.rs` — Journal becomes per-partition instance; accept()/replay() logic reused per partition.
- `src/api.rs` — handler flow unchanged; router computes partition key + structured ID before dispatch.
- `src/bin/bench.rs` — rerun at 500/50 to prove the queueing collapse is gone.
- `src/bin/test-bank.rs` (new, this phase) — dual-side bank rehearsal over both WS planes.

### Established Patterns
- Single-writer-per-journal (kept — now N of them); mpsc + oneshot dispatch (kept per partition); metrics hooks per stage (add partition label).

### Integration Points
- Phase 3 rail delivery consumes per-partition durable callbacks; Phase 4 matcher exploits same-pair-same-partition locality.

## Specific Ideas

- User phrasing: "partitioned writers where transaction ids also comes with data like time bucket, sender and receiver account id"; "TTL expiry with dead letter queue mechanism".

## Deferred Ideas

None — discussion stayed within phase scope.

---

*Phase: 2-Crash safety + idempotency*
*Context gathered: 2026-10-02*
