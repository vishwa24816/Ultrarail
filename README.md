# Payment Rail — Rust Payment-Rail Backend

An async Rust (Tokio + Axum) payment processing service with a **journal as the
source of truth — no database**. It accepts payments with millisecond durable
acceptance, delivers them to a rail with exactly-once discipline, matches rail
confirmations, and survives crashes, restarts, and bank-side failures without
silent loss or duplicate posting.

The state machine
(`RECEIVED → VALIDATED → ACCEPTED_DURABLE → SUBMITTED → ACKNOWLEDGED → SETTLED`,
plus `REJECTED`, `UNKNOWN`, `RECONCILIATION_REQUIRED`), the journal-first
processing path, idempotent retries, double-entry ledger discipline, and the
staged rollout from simulated rail to production-shaped pilot.

## Accomplishments

Built over 5 phases (30 commits), verified by **36 automated tests**, a
dual-side bank rehearsal, and a committed soak matrix:

| Milestone | Result |
|---|---|
| Durable acceptance | Every `201` is fsync-backed; kill-9 loses zero acked txs |
| Throughput | **~1900 tx/s** at 50-way concurrency (was 174 before group commit — 10×) |
| Tail latency | p99 **~43 ms** at 500/50 (was 368 ms); serial floor ~7 ms (disk fsync) |
| Exactly-once | Duplicate-confirmation storms → 1 settlement; reconcile resubmits: 0 |
| Recovery | Kill-9, torn tails, graceful SIGTERM drain, partition supervision — all proven |
| Security | TLS + API keys both planes, uniform 401s, constant-time compare |
| Pilot | Soak 100→2000 tx × 1→50 conc with mid-run kill-9, all suites green (see `.planning/phases/05-ops-hardening/SOAK.md`) |

## Features and functionality

### Core payment path
- `POST /payments` — validate → idempotency check → journal sync → `201
  {tx_id, ACCEPTED_DURABLE}`. Invalid → `422 REJECTED`. No settlement entry.
- **Money done right:** integer minor units + explicit `price` × `quantity`
  (enforced `amount == price × quantity`), currency enum, balanced
  double-entry postings checked pre-commit. No floats anywhere.
- **Structured tx IDs** `{timebucket}-{sender}-{receiver}-{random}` that double
  as partition routing keys.

### Durability without a database
- Append-only `wal-db` journals, one per partition; ack only after the
  fdatasync watermark. Indexes (tx + idempotency) are rebuildable via replay.
- **Partitioned multi-writer** pipeline (hash of time-bucket + accounts),
  each with **group-commit** sync — one fsync per batch.
- **Idempotency with TTL** (default 24 h): same key returns the original;
  post-TTL retries become new payments flagged `fresh`, with the old id sent
  to the dead-letter queue. `replayed` / `fresh` contract on every receipt.

### Delivery and rail
- Per-partition delivery workers: transient-only retries (capped exponential
  backoff + jitter), per-tx inflight guards, attempt logs, `UNKNOWN` on
  timeout with **reconcile-before-resubmit** (proven: 1 submit, 0 resubmits).
- **Bank terminal failures never retry:** `INSUFFICIENT_FUNDS`,
  `RISKY_CLIENT`, `FROZEN_DEBIT/CREDIT/TOTAL` fail fast with
  `transaction failed for {flag}`. Account registry (balances + freeze/risk
  flags) fed live by bank websocket events, with pre-submit gating.
- `GET /payments/:id` — full tx + attempts + event history + match state.

### Matching engine
- Exact auto-settle on (rail ref + amount + currency + counterparty +
  value date); near-misses → exception queue with reasons and audit trail.
- **Double-settlement is structurally impossible:** partition-owned
  settled-set, check + record in one writer turn, rebuilt from event replay.

### Bank connectivity
- Two websocket planes: `/ws/client` (submit + receipts) and `/ws/bank`
  (settlement events, acks, balance/flag updates, confirmations, failures).
- `test-bank` rehearses **both sides** at production level: 100/100 dual-side
  validation, balance conservation, terminal scenarios, matching scenarios.

### Operations
- Graceful SIGTERM drain (503 + `Retry-After`, exit 0 only when synced);
  supervised partitions (crash → WAL-resume with backoff, give-up to DLQ).
- Prometheus `/metrics`, append-only audit log, DLQ viewer, `/health` vs
  `/ready`, validated boot config with partition-count guard.
- Backup/restore scripts (checksum manifests), zero-downtime key rotation
  (dual-key window via key files), sandbox soak harness.

## Quick start

```powershell
cargo build
$env:JOURNAL_DIR = "./data"; $env:TLS_OFF = "true"
.\target\debug\payment-rail.exe
```

```powershell
# Submit a payment
Invoke-RestMethod -Method Post http://127.0.0.1:3000/payments `
  -Headers @{"Idempotency-Key"="demo-1"} `
  -Body '{"idempotency_scope":"demo","debit_account":"user:1","credit_account":"merchant:9","amount":100,"currency":"USD"}' `
  -ContentType "application/json"

# Benchmark it
.\target\debug\bench.exe 500 50

# Rehearse both bank sides
.\target\debug\test-bank.exe 100
```

```powershell
cargo test          # full suite (36 tests)
powershell -NoProfile -File scripts/soak.ps1   # soak pilot
```

## Configuration (env)

| Var | Default | Purpose |
|---|---|---|
| `PARTITIONS` | cpus/2 | Journal partitions (changing needs a drain) |
| `JOURNAL_DIR` / `LISTEN_ADDR` / `PROM_ADDR` | `./data`, `:3000`, `:9000` | Paths and ports |
| `IDEM_TTL_SECS` | 86400 | Idempotency key lifetime |
| `BANK_KEYS` / `CLIENT_KEYS` (+ `*_FILE`) | unset (open) | `owner:secret` API keys |
| `TLS_CERT` / `TLS_KEY` / `TLS_OFF` | unset | TLS (rustls) |
| `SIM_FAIL_RATE` / `SIM_TIMEOUT_RATE` / `SIM_LATENCY_MS` | 0/0/20 | Simulated-rail fault injection |
| `RAIL_TIMEOUT_MS` / `RECONCILE_TIMEOUT_SECS` | 2000/300 | UNKNOWN discipline |

## Layout

```
src/            main, api, app_state, domain, journal, partition,
                delivery, matcher, accounts, events, dlq, audit,
                metrics, ws, auth, config
src/bin/        bench.rs (load harness) · test-bank.rs (dual-side rehearsal)
tests/          acceptance, recovery, delivery, matching, tls, backup, ops
scripts/        backup.ps1, restore.ps1, soak.ps1
.planning/      PROJECT, REQUIREMENTS, ROADMAP + per-phase CONTEXT/PLAN/SUMMARY
```

## What was deliberately deferred (v2)

Multi-region replication, partitioned-writer auto-tuning, operator
retry/write-off actions on exceptions, full user-accounts auth model
(key-based auth stands for v1). See `.planning/` for the complete decision
trail.
