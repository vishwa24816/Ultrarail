# Phase 5: Ops hardening - Context

**Gathered:** 2026-10-04
**Status:** Ready for planning

## Phase Boundary

Close out production readiness: a soak-tested sandbox pilot across volumes and concurrencies with every suite green, executable runbook checks for reconcile/exceptions/restart, and the two unowned gaps (journal backup/restore, key rotation) closed. (OPS-01, OPS-02, OPS-03)

## Implementation Decisions

### Pilot bar: soak test (discussed + locked)
- **D-01:** Soak, not just gates: sustained load across multiple volumes (100 / 500 / 2000 tx) and concurrencies (1 / 10 / 50), with a mid-run kill-9 + restart, ending with the FULL suite green (`cargo test`), `test-bank` (all modes), and bench within its gates.
- **D-02:** Soak harness extends `src/bin/bench.rs` (parameterized matrix) or a driver script — planner's call, but results must be a committed artifact (numbers in the SUMMARY, not console-only).

### Runbooks: scripted checks (discussed + locked)
- **D-03:** Executable checks, not prose: scripts (or a `test-bank --ops` mode) that verify each procedure end-to-end — reconcile an UNKNOWN, drain the DLQ paths, restart-and-recover, rotate a key without downtime.
- **D-04:** Each check prints PASS/FAIL per step and exits non-zero on failure so CI or an operator can gate on it.

### Leftover gaps: closed now (discussed + locked)
- **D-05:** Journal backup/restore: file-copy procedure for sealed segments + `meta.json` (WAL segments are immutable once rotated) plus a restore test (copy → fresh dir → boot → all txs queryable).
- **D-06:** Key rotation without downtime: dual-key acceptance window (old + new accepted, new issued) via `BANK_KEYS`/`CLIENT_KEYS` reload (SIGHUP or file-watch — planner picks, HUP is simpler), then old revoked. Tested: rotate mid-load, zero 401s for in-flight clients.

### Already done (not rebuilt)
- Exporter (`/metrics`), audit log, TLS + API keys, kill-9/duplicate/TTL/shutdown/supervision tests, per-stage latency hooks, `/health` vs `/ready`.

### the agent's Discretion
- Soak matrix exact sizes, script vs binary for checks, HUP vs file-watch for key reload, backup tooling shape (script + docs).

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design
- `p.md` — rollout stages, reconciliation procedures, runbooks
- `.planning/PROJECT.md` — core value, constraints
- `.planning/REQUIREMENTS.md` — OPS-01, OPS-02, OPS-03 acceptance criteria
- `.planning/ROADMAP.md` — Phase 5 goal + success criteria
- `.planning/phases/02-crash-safety-idempotency/02-CONTEXT.md` — DLQ, TTL
- `.planning/phases/03-rail-delivery/03-CONTEXT.md` — UNKNOWN/reconcile, auth, audit, exporter
- `src/bin/bench.rs`, `src/bin/test-bank.rs` — harnesses being extended

## Existing Code Insights

### Reusable Assets
- `bench.rs` (parameterized load), `test-bank` (all modes incl. `--terminal`, `--matching`), full `cargo test` suite, `/metrics`, `/dlq`, `/audit`, `GET /payments/:id`.
- `meta.json` partition guard — backup must include it.

### Established Patterns
- Proof-by-test for every reliability claim; committed numbers, not console-only.

### Integration Points
- None beyond this repo — Phase 5 closes the v1 milestone.

## Specific Ideas

- User wording: "soak test with multiple transactions and concurrencies and all suites".

## Deferred Ideas

- Multi-region replication (REP-01), partitioned-writer auto-tuning — v2.
- Full user-accounts auth model (key-based auth stands for v1).

---

*Phase: 5-Ops hardening*
*Context gathered: 2026-10-04*
