# Requirements: Rust Payment-Rail Backend

**Defined:** 2026-10-02
**Core Value:** Durably accept a payment fast and settle it exactly once.

## v1 Requirements

### Journal & Acceptance

- [ ] **JRN-01**: User can submit payment via API and receive durable acceptance (tx ID) after journal sync
- [ ] **JRN-02**: Service recovers indexes by journal replay after crash/restart without loss
- [ ] **JRN-03**: Journal record contains sequence, tx ID, idempotency scope+key, request hash, amount/currency, ledger entries, state, timestamp, checksum

### Idempotency & Delivery

- [ ] **IDM-01**: Client can retry with same idempotency key and receive original tx result (no duplicate payment)
- [ ] **IDM-02**: Delivery worker retries only transient failures with capped backoff + jitter, recording each attempt as event on same tx
- [ ] **IDM-03**: Service marks post-submit timeout as UNKNOWN and reconciles (status query) before any resubmit

### Ledger & Matching

- [ ] **LDG-01**: Service enforces balanced double-entry postings in integer minor units with explicit currency (no floats)
- [ ] **LDG-02**: Matcher auto-settles confident matches on rail ref + amount + currency + counterparty + value date
- [ ] **LDG-03**: Ambiguous/unmatched records route to exception queue with audit trail

### Ops & Security

- [ ] **OPS-01**: Operator can view P50/P95/P99 per stage (validation, journal sync, rail RTT, matching) plus queue depth, retry age, unknowns, unmatched
- [ ] **OPS-02**: Operator can run restart-recovery and duplicate-request sandbox scenarios against simulated rail adapter
- [ ] **OPS-03**: API enforces authN/authZ, TLS, key management, and access audit; no cardholder data stored

## v2 Requirements

### Replication & Scale

- **REP-01**: Replicated durability (ack after N peers) with group-commit tuning
- **REP-02**: Partitioned writers / bounded-queue backpressure tuned by benchmark
- **OPS-04**: Reconciliation runbooks + alerts for UNKNOWN/unmatched aging

## Out of Scope

| Feature | Reason |
|---------|--------|
| Ms-level guaranteed settlement | External rail controls settlement time |
| Live rail traffic | Restricted pilot only after certification + runbooks |
| Disk B+ tree from day one | Simplest rebuildable index first; upgrade if measured bottleneck |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| JRN-01 | Phase 1 | Pending |
| JRN-03 | Phase 1 | Pending |
| LDG-01 | Phase 1 | Pending |
| JRN-02 | Phase 2 | Pending |
| IDM-01 | Phase 2 | Pending |
| IDM-02 | Phase 3 | Pending |
| IDM-03 | Phase 3 | Pending |
| LDG-02 | Phase 4 | Pending |
| LDG-03 | Phase 4 | Pending |
| OPS-01 | Phase 5 | Pending |
| OPS-02 | Phase 5 | Pending |
| OPS-03 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 12 total
- Mapped to phases: 12
- Unmapped: 0 ✓

---
*Requirements defined: 2026-10-02*
*Last updated: 2026-10-02 after initial definition*
