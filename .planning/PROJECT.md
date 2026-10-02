# Rust Payment-Rail Backend

## What This Is

Async Rust (Tokio) payment processing service with a journal-as-source-of-truth (no DB). Accepts payments with low P99 API-accept latency, persists durably before acknowledging, delivers to an external rail via adapter, and matches rail confirmations to settle. For operators running a production-shaped pilot with no silent loss or duplicate posting.

## Core Value

Durably accept a payment fast and settle it exactly once — no silent loss, no duplicate posting.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Durable journal acceptance path (RECEIVED → VALIDATED → ACCEPTED_DURABLE)
- [ ] Idempotent API (client key + server tx ID, safe retries)
- [ ] Rail delivery worker with adapter + UNKNOWN handling
- [ ] Double-entry ledger + payment matcher with exception queue
- [ ] Latency/ops metrics and recovery procedures

### Out of Scope

- Guaranteed ms-level final settlement — depends on external rail, not this service
- Full multi-zone replication in v1 — single-node durable sync first, replicate later
- Cardholder-data handling / PCI scope — avoid storing PANs; assess later if in scope

## Context

- Detailed design input in `p.md` (state machine, journal format, idempotency, ledger, rollout stages).
- Greenfield; no existing code. Simulated rail adapter for sandbox before live pilot.
- Journal: append-only segments + rebuildable ID and idempotency-key indexes (B+ tree or simpler ordered index — measure first).
- Amounts in integer minor units, explicit currency, no floats; balanced postings enforced before journal commit.

## Constraints

- **Tech**: Rust + Tokio; no database — journal files only
- **Correctness**: UNKNOWN on post-submit timeout, never blind resubmit; reconcile before resubmitting
- **Durability**: return ACCEPTED only after journal sync (fsync/group-commit); replication threshold TBD by deployment
- **Latency**: separate P99 targets for acceptance vs rail vs settlement, set after measuring

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Journal as source of truth, indexes rebuildable | No DB; replay recovers state | — Pending |
| Sync-before-ack durability boundary | Survive crash without silent loss | — Pending |
| Simulated rail first, live pilot last | Rail certification + recovery must precede live money | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-10-02 after initialization*
