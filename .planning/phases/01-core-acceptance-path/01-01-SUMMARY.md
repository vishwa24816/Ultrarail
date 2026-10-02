# Summary 01-01: Scaffold + domain + journal

**Status:** complete

## What was built
- Cargo workspace (`payment-rail`): tokio full, axum 0.7, tower-http, serde, uuid v7, validator, crc32fast, wal-db 1, tracing, metrics.
- `src/domain.rs`: Currency, Money (i64 minor units), LedgerEntry, TxStatus, PaymentTx + `validate()` (positive amount, balanced postings, account allowlist). `CreatePayment` with validator + deny_unknown_fields.
- `src/journal.rs`: `Journal` over wal-db `Wal` — `accept()` validates, appends one record, `sync()`s, then returns ACCEPTED_DURABLE; `replay()` rebuilds tx + idempotency indexes, fails closed on checksum mismatch.

## Commits
- Wave 1 implementation (pending commit with this summary).

## Verification
- `cargo test`: 5 unit tests pass (balanced/unbalanced/zero-amount, crash-reopen survival, duplicate-key idempotency).
