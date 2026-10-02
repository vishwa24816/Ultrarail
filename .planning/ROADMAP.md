# Roadmap: Rust Payment-Rail Backend

**Project:** Rust Payment-Rail Backend
**Created:** 2026-10-02
**Granularity:** Coarse (5 phases)

| # | Phase | Goal | Requirements | Success Criteria |
|---|-------|------|--------------|------------------|
| 1 | Core acceptance path | Durably accept payments to journal | JRN-01, JRN-03, LDG-01 | 3 |
| 2 | Crash safety + idempotency | Restarts lose nothing, retries duplicate nothing | JRN-02, IDM-01 | 3 |
| 3 | Rail delivery | Deliver to rail safely with UNKNOWN handling | IDM-02, IDM-03 | 3 |
| 4 | Matching engine | Settle confident matches, queue the rest | LDG-02, LDG-03 | 3 |
| 5 | Ops hardening | Observable, recoverable sandbox pilot | OPS-01, OPS-02, OPS-03 | 3 |

### Phase 1: Core acceptance path
**Goal:** Durably accept payments to journal
**Mode:** mvp
**Success Criteria**:
1. API returns tx ID only after journal record synced to disk
2. Unbalanced postings / float amounts rejected before commit
3. Invalid requests rejected with REJECTED status (no journal settlement entry)

### Phase 2: Crash safety + idempotency
**Goal:** Restarts lose nothing, retries duplicate nothing
**Mode:** mvp
**Success Criteria**:
1. Kill -9 mid-load then restart: all acked txs present, indexes rebuilt by replay
2. Same idempotency key retried 10x creates exactly one payment
3. Different keys with same request hash handled per idempotency-scope rules

### Phase 3: Rail delivery
**Goal:** Deliver to rail safely with UNKNOWN handling
**Mode:** mvp
**Success Criteria**:
1. Transient rail failures retried with backoff+jitter, attempts logged on same tx
2. Timeout after submit → UNKNOWN, no blind resubmit; status query precedes resubmit
3. Simulated rail adapter exercises accept/timeout/duplicate scenarios

### Phase 4: Matching engine
**Goal:** Settle confident matches, queue the rest
**Mode:** mvp
**Success Criteria**:
1. Exact rail confirmations settle automatically
2. Ambiguous/unmatched records land in exception queue with audit trail
3. Double-settlement impossible: settled tx never re-settles on duplicate confirmation

### Phase 5: Ops hardening
**Goal:** Observable, recoverable sandbox pilot
**Mode:** mvp
**Success Criteria**:
1. Per-stage P50/P95/P99 + queue/retry/unknown/unmatched metrics visible
2. Sandbox pilot runs bounded load + recovery + duplicate scenarios green
3. AuthN/authZ + TLS on API; no PAN storage; access audit present
