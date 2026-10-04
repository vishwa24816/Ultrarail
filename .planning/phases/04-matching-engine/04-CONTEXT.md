# Phase 4: Matching engine - Context

**Gathered:** 2026-10-04
**Status:** Ready for planning

## Phase Boundary

Match rail confirmations to journal txs: auto-settle confident matches, route the rest to an exception queue with audit trail, and make double-settlement structurally impossible. (LDG-02, LDG-03)

## Implementation Decisions

### Double-settle guard (discussed + locked)
- **D-01:** Per-partition owned settled-set: each partition holds `settled: HashSet<tx_id>` in its writer task. A settle request first checks the set — duplicate confirmations stop there, zero journal writes, logged as `duplicate-confirmation` audit entry.
- **D-02:** The set rebuilds from event replay on boot (`Settled`/`Reconciled` events), so the guard survives restarts. No cross-partition coordination (a tx lives on exactly one partition by construction).
- **D-03:** Settle itself appends a settle event + audit entry; the set insert and event append happen in the same writer-task turn (single owner → no race, no lock).

### the agent's Discretion (bounded by roadmap success criteria)
- **Match rules:** exact match on rail ref + amount + currency + counterparty + value date auto-settles (LDG-02). Tolerance policy (amount epsilon, date window) is the planner's call — default to exact-only, flag near-misses to the exception queue.
- **Exceptions:** ambiguous/unmatched records land in the exception queue (DLQ-backed, new reason codes) with full tx + reason + audit trail. Operator actions (retry-as-new, write-off) are out of scope — queue + visibility only.
- **Matcher shape:** per-partition task fed by delivery settle events (matches the partition-owned architecture); hooks into existing `BankEvent`/delivery flow rather than a new service.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design
- `p.md` — matching fields (rail ref, amount, currency, counterparty, value date), exception queue
- `.planning/PROJECT.md` — core value (exactly-once)
- `.planning/REQUIREMENTS.md` — LDG-02, LDG-03 acceptance criteria
- `.planning/ROADMAP.md` — Phase 4 goal + success criteria
- `.planning/phases/02-crash-safety-idempotency/02-CONTEXT.md` — partitions, DLQ
- `.planning/phases/03-rail-delivery/03-CONTEXT.md` — delivery states, events, audit
- `src/delivery.rs`, `src/events.rs`, `src/dlq.rs`, `src/audit.rs` — code being extended

## Existing Code Insights

### Reusable Assets
- `EventLog` per partition — settle/reconcile events already flow here; settled-set rebuilds from it.
- `Dlq` + reasons — exception queue reuses it (`unmatched-confirmation`, `ambiguous-match`).
- `GET /payments/:id` — surface match state there.
- `test-bank` — extend with duplicate-confirmation scenario asserting single settlement.

### Established Patterns
- One owning task per partition; group-commit sync; per-partition replay; `replayed`/`fresh` contract.

### Integration Points
- Phase 5 ops reads match/unmatched metrics + exception queue depth.

## Specific Ideas

- User selected only the double-settle area; everything else follows roadmap defaults above.

## Deferred Ideas

- Operator retry/write-off actions on exceptions — future phase.
- Cross-partition netting — explicitly out of scope (partitioning decision stands).

---

*Phase: 4-Matching engine*
*Context gathered: 2026-10-04*
