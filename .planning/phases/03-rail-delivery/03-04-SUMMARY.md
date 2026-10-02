# Summary 03-04: TLS + API keys (bank and client)

**Status:** complete

## What was built
- `src/auth.rs`: `BANK_KEYS`/`CLIENT_KEYS` (`owner:secret`, secrets ≥32 chars), open-sandbox mode with loud WARN when unset, constant-time compare, full-table scan (no user enumeration), identical 401 bodies.
- Route split: client plane (/payments, /payments/:id, /ws/client, /dlq, /audit) vs bank plane (/ws/bank); `/health` + `/ready` stay open. Bank WS binds `bank_id` to key owner (cross-bank listen → 401).
- TLS via axum-server + rustls on `TLS_CERT`/`TLS_KEY`; plaintext only with loud WARN (or explicit `TLS_OFF`). Explicit ring provider install (tree also contains aws-lc).
- `tests/tls.rs`: plaintext dead on TLS port; missing/wrong/cross keys → identical 401s; client key → 201; WSS bank event + ack over verified fixture cert (`tests/certs/`, minted by `examples/gen_certs.rs` since system openssl is broken).
- `test-bank`: per-side keys (`BANK_KEY_USER`/`BANK_KEY_MERCHANT`, `CLIENT_KEY`), upgrade headers, sandbox-only insecure WSS.

## Verification
- `cargo test`: 23/23. `test-bank 100` with keys: PASS. Bench: 1980 tps (gate >1500 PASS, auth cost ~zero).
