# Phase 3: Rail delivery - Research

**Researched:** 2026-10-02 (codebase + prior bench data)

## What exists to build on

- `ws::submit()` — shared HTTP/WS accept path with partition routing; delivery reuses it.
- `BankEvent` broadcast + `PendingMap` ack tracking + 5s sweeper → DLQ — the delivery worker extends this pattern (submit → await rail ack → settle/fail/timeout).
- `Dlq` + `GET /dlq` — new reasons plug in.
- `test-bank` — extend with low-balance/frozen/risky scenarios.
- `metrics` hooks — exporter reads these; add terminal-failure counters + queue gauges.

## Approach per CONTEXT.md

| Decision | Mechanism |
|---|---|
| Worker | One delivery task per partition; drains ACCEPTED txs (in-memory outbox fed by writer post-sync), simulated rail adapter first (latency + failure injection via env), real adapter later behind same trait |
| Attempts | `Vec<Attempt{at_ms, kind, detail}>` on tx via journal event records; capped backoff 100ms→30s + jitter; only `transient` retried |
| UNKNOWN | Submit timeout → state UNKNOWN + reconcile query (simulated adapter answers); unresolved past RECONCILE_TIMEOUT → DLQ `unknown-unresolved` |
| Terminal | Bank WS `tx.failed{reason}` or pre-submit registry check → FAILED, zero retries, `transaction failed for {reason}` |
| Registry | `accounts: HashMap<account, {balance, debit_frozen, credit_frozen, totally_frozen, risky}>` in AppState (Mutex), updated by bank WS `balance.update`/`account.flag`; pre-submit gate |
| Status query | `GET /payments/:id` reads owning partition (route from embedded sender/receiver in tx_id, fallback scan) |
| Shutdown | `tokio::signal::ctrl_c` → stop intake (503), drain batches with sync, 10s deadline then exit |
| Supervision | `spawn_one` wrapped in supervisor loop: on panic/Err, log + reopen WAL + replay + resume; partitions independent |
| Metrics | `metrics-exporter-prometheus` on `/metrics` (separate port via PROM_ADDR); histograms already named |
| Audit | `audit.wal` append-only JSON lines per state transition (reuse Dlq-like WAL wrapper) |
| TLS/auth | `axum-server` with rustls or reverse-proxy note? Decision: rustls via `axum-server` crate, certs from `TLS_CERT`/`TLS_KEY`; API keys: `BANK_KEYS=user:secret,...`, `CLIENT_KEYS=...` in env, constant-time compare, 401 on miss |
| Config | Single `Config::from_env()` validated at boot; PARTITIONS change vs stored meta aborts (store N in `meta.json` next to journals) |

## Pitfalls

- Don't retry terminal failures (classify BEFORE backoff).
- Attempt records must not rewrite history — append-only events, never mutate the accepted record.
- Shutdown deadline must exceed one full group-commit sync or durability lies.
- API-key compare must be constant-time; 401 messages identical for bad vs missing.

## Planner must enforce

- IDM-02 in worker plan, IDM-03 in UNKNOWN/reconcile plan; every task read_first + acceptance_criteria; test-bank extension proves terminal failures; bench re-run guards throughput.
