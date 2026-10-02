# Summary 02-04: Bank websockets + dummy test bank

**Status:** complete

## What was built
- `src/ws.rs`: `/ws/client` (JSON submit → receipt on same socket, shares `submit()` path with HTTP) and `/ws/bank?bank_id=` (streams `BankEvent`s involving that bank prefix; `{tx_id, ack:true}` removes pending). Broadcast cap 1024, lagging banks dropped + `bank-lag-drop` DLQ note. Ack sweeper every 5s → `bank-no-ack` DLQ + per-batch `flush()` so DLQ survives crashes (found 9/10 entries lost to coalesced sync during testing).
- `src/bin/test-bank.rs`: connects as sender-bank + receiver-bank, submits N over client WS, validates receipts, acks everything, asserts conservation.
- `GET /dlq` operator view; protocol shapes frozen for Phase 3 reuse.

## Verification
- `test-bank 100`: PASS — 100/100 receipts, 100 events per bank side, debit == credit == 24900.
- `--no-ack` run: `bank-no-ack` entries appear in `/dlq` (proves timeout path).
- `cargo test`: 17/17 (11 unit + 3 acceptance + 3 recovery).
