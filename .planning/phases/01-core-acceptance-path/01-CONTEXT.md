# Phase 1: Core acceptance path - Context

**Gathered:** 2026-10-02
**Status:** Ready for planning

## Phase Boundary

Durably accept payments to journal: API validates, appends a full journal record, fsyncs, then returns tx ID. Balanced minor-unit postings enforced pre-commit; invalid requests get REJECTED with no settlement entry. (JRN-01, JRN-03, LDG-01)

## Implementation Decisions

### Journal substrate
- **D-01:** Use a proven WAL crate (open-wal or wal-db) with group-commit + fdatasync — fastest to production, no hand-rolled recovery.
- **D-02:** Ack only after commit watermark durable; single writer per partition.

### API runtime
- **D-03:** axum + Tokio multi-thread runtime; Tower middleware (tracing, limits, cors). Bounded queues; CPU-heavy validation off async workers (spawn_blocking).

### Ledger types
- **D-04:** i64 integer minor units + explicit currency enum; no floats. Balance check pre-commit.
- **D-05:** Serde + crc32c checksums on records; thiserror for typed errors.

### API contract
- **D-06:** Standard: UUIDv7 tx IDs, `Idempotency-Key` header, JSON envelope `{ tx_id, status, ... }`; states RECEIVED → VALIDATED → ACCEPTED_DURABLE / REJECTED.

### Optimized stack (research-backed, prod performance)
- axum 0.7+, tokio full, tower-http, serde/serde_json, uuid v7, rust_decimal deferred (only if FX needs it), tracing + metrics (prometheus), validator, chrono (unix-ms timestamps).
- Perf: group-commit fsync is the lever (~1ms/commit single, ~3.5k commits/s grouped); keep hot path lock-free append, measure P99 per stage.

### the agent's Discretion
- Exact WAL crate pick (open-wal vs wal-db), segment sizing, index structure (BTreeMap rebuildable first), error body shape.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design
- `p.md` — state machine, journal record fields, durability boundary, rollout stages
- `.planning/PROJECT.md` — core value, constraints (no DB, sync-before-ack)
- `.planning/REQUIREMENTS.md` — JRN-01, JRN-03, LDG-01 acceptance criteria
- `.planning/ROADMAP.md` — Phase 1 goal + success criteria

## Existing Code Insights

### Reusable Assets
- None — greenfield, no existing code.

### Established Patterns
- None yet; Phase 1 sets them (journal-first, validate-before-commit).

### Integration Points
- Simulated rail adapter comes in Phase 3; Phase 1 exposes tx IDs + journal replay API for it.

## Specific Ideas

- Research refs: Atlas/ledger-rs prod stacks (axum+tokio+tracing), wal-db group-commit benchmarks (1.9x vs naive fsync-per-commit), open-wal durability model (commit = write + fdatasync watermark).

## Deferred Ideas

None — discussion stayed within phase scope.

---

*Phase: 1-Core acceptance path*
*Context gathered: 2026-10-02*
