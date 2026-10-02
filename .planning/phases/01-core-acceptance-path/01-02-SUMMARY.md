# Summary 01-02: API + acceptance path

**Status:** complete

## What was built
- `src/app_state.rs`: single-writer journal task behind bounded mpsc(1024) + oneshot replies; overload → 503 path.
- `src/api.rs`: POST /payments (Idempotency-Key required → 400; validation fail → 422 REJECTED; valid → 201 ACCEPTED_DURABLE with UUIDv7 tx_id + lsn); GET /health; 64KB body limit + TraceLayer. Localhost-only bind noted (authN is Phase 5).
- `src/main.rs`: boots journal replay, writer task, axum serve on 127.0.0.1:3000.

## Verification
- Live probes via acceptance tests: 201 valid, 422 invalid, 400 missing key.
