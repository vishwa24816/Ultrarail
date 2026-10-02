# Summary 03-01: Delivery worker + terminal failures + status query

**Status:** complete

## What was built
- `src/delivery.rs`: per-partition worker (outbox inbox, `inflight` flag — no double-submit), `Rail` trait + `SimRail` (env failure injection), capped backoff+jitter (100ms→30s, 10 tries), UNKNOWN on timeout with 5s reconcile ticks, `unknown-unresolved` DLQ past timeout.
- `src/accounts.rs`: registry (balance + debit/credit/total freeze + risky) fed by bank WS `balance.update`/`account.flag`; pre-submit gate fails terminally with `transaction failed for {CODE}`; flag snapshot on tx.
- `src/events.rs`: per-partition `events-{i}.wal` tolerant-replay event log (attempt/unknown/reconciled/settled/failed).
- `GET /payments/:id`: tx + partition/lsn + delivery state + full event history; 404 unknown.
- Bank `tx.failed{reason}` → terminal via broadcast fan-out.
- `test-bank --terminal`: FROZEN_TOTAL + INSUFFICIENT_FUNDS scenarios assert FAILED + exact message + zero rail attempts.
- `tests/delivery.rs`: timeout storm proves UNKNOWN → reconciled with exactly 1 unknown event (no resubmit).

## Verification
- `cargo test`: 18/18 (11 unit + 3 acceptance + 3 recovery + 1 delivery).
- Manual: SIM_TIMEOUT_RATE=1.0 run showed unknown → `rail confirmed settled`, single submit.
