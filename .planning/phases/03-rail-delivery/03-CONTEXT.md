# Phase 3: Rail delivery - Context

**Gathered:** 2026-10-02
**Status:** Ready for planning

## Phase Boundary

Make the rail production-ready: deliver accepted txs through a supervised per-partition worker with bank-driven terminal failures, UNKNOWN + reconcile discipline, status queries, graceful shutdown, full observability, and auth/TLS pulled forward from Phase 5. (IDM-02, IDM-03, plus the production-hardening scope)

## Implementation Decisions

### Delivery worker (full, per-partition)
- **D-01:** Dedicated delivery worker per partition (not inline in the accept loop). Reads durable ACCEPTED txs, submits via rail adapter, records attempts as events on the same tx.
- **D-02:** Retries only transient failures, capped exponential backoff + jitter. Attempt log lives on the tx record.
- **D-03:** Post-submit timeout → UNKNOWN, never blind resubmit; reconcile (status query) before any resubmit. UNKNOWN ages into DLQ with reason `unknown-unresolved` after RECONCILE_TIMEOUT.
- **D-04:** `GET /payments/:id` status query (tx, attempts, state history). Reconcilers and clients read this; nothing else can answer "what happened to my payment".

### Bank-driven terminal failures (no retry)
- **D-05:** Terminal reason codes from the bank over WS: `INSUFFICIENT_FUNDS` (low balance), `RISKY_CLIENT`, `FROZEN_DEBIT`, `FROZEN_CREDIT`, `FROZEN_TOTAL`. Terminal = FAILED immediately, zero retries, response/message `transaction failed for {flag}`.
- **D-06:** Account registry in the rail: per bank account `{balance, debit_frozen, credit_frozen, totally_frozen, risky}` updated from bank WS events (`balance.update`, `account.flag`). Pre-submit check fails fast locally on frozen/risky/insufficient before touching the rail.
- **D-07:** Flags live on BOTH the bank account record and every transaction touching it (tx carries the flag snapshot that decided its fate — audit-proof).

### Crash hardening (supervised)
- **D-08:** Graceful shutdown: SIGTERM stops intake (503 with `Retry-After`), drains in-flight batches through sync, then exits. Tested, not just coded.
- **D-09:** Supervised writers: a panicking partition task restarts from its WAL (replay, not memory) while other partitions keep serving. Panic is logged with partition id + LSN watermark.

### Observability (full)
- **D-10:** Prometheus exporter (`/metrics`): per-stage P50/P95/P99, queue depth, retry age, unknowns, unmatched, DLQ depth, ack timeouts, terminal-failure counts by reason.
- **D-11:** Append-only audit log: every state transition (who/when/from clinet or bank, which partition, which LSN). Dispute-grade, no PII beyond account ids.

### Security now (pulled from Phase 5)
- **D-12:** TLS on all listeners (self-signed for sandbox, config-supplied certs); bank WS requires API key per bank; client endpoints require API key (user auth = key-based for now, accounts model later).
- **D-13:** Keys via env/config file with restrictive permissions warning at boot; unknown/expired key → 401, never a hint which half was wrong.

### Config + safety rails
- **D-14:** Validated config at boot: one struct, unknown keys rejected, `PARTITIONS` change without drain aborts boot with an explicit error (replaces the comment).
- **D-15:** Liveness vs readiness: `/health` = alive, `/ready` = journals replayed + workers running. Load balancers use `/ready`.

### Carried forward (locked, not re-asked)
- Partitioned wal-db journals + group commit; structured tx IDs (low random bits); TTL idempotency + DLQ; WS protocol shapes (extended with bank events below, not broken); i64/price/quantity money; sync-before-ack.

### the agent's Discretion
- Backoff constants, reconcile timeout, exact audit-log format, metrics bucket boundaries, key-rotation story (deferred).

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design
- `p.md` — rail adapter, UNKNOWN, reconciliation procedures
- `.planning/PROJECT.md` — core value, constraints
- `.planning/REQUIREMENTS.md` — IDM-02, IDM-03 acceptance criteria
- `.planning/ROADMAP.md` — Phase 3 goal + success criteria
- `.planning/phases/02-crash-safety-idempotency/02-CONTEXT.md` — D-01…D-13 (partitions, WS protocol, DLQ)
- `src/ws.rs`, `src/app_state.rs`, `src/journal.rs`, `src/dlq.rs` — code being extended

## Existing Code Insights

### Reusable Assets
- `submit()` in `src/ws.rs` — shared HTTP/WS path; delivery worker reuses partition routing.
- `Dlq` + `GET /dlq` — new reasons plug in (`unknown-unresolved`, `bank-lag-drop` exists).
- `BankEvent` broadcast — bank WS events extended with `balance.update` / `account.flag` messages.
- `test-bank` — extended with low-balance/frozen/risky scenarios asserting terminal FAILED.

### Established Patterns
- One owning task per partition; mpsc + oneshot; group-commit sync; per-partition replay; `replayed`/`fresh` contract.

### Integration Points
- Phase 4 matcher reads terminal FAILED + SETTLED states; account registry feeds its confidence rules.

## Specific Ideas

- User wording: "payment getting failed on low balance in bank account from bank websocket", "flags like risky client", "frozen bank account with debit credit and total freeze flags", "transaction fail without retry and throw message transaction failed for xyz flag".

## Deferred Ideas

- Key rotation automation — noted, not this phase.
- Multi-region replication (REP-01) — still v2.

---

*Phase: 3-Rail delivery*
*Context gathered: 2026-10-02*
