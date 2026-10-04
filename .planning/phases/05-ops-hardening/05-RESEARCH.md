# Phase 5: Ops hardening - Research

**Researched:** 2026-10-04 (codebase; no external research needed)

## What exists to build on

- `bench.rs` (parameterized N/concurrency, percentile output) — soak matrix extends it.
- `test-bank` (all modes) — rotation test drives it with two key sets.
- `meta.json` partition guard — backup must include it; restore must pass the guard.
- `KeySets::from_env` — rotation needs reload; currently boot-only.
- Full `cargo test` suite (30 tests) + `/metrics` + `/dlq` + `/audit` + `GET /payments/:id`.

## Approach (locked by CONTEXT.md)

| Decision | Mechanism |
|---|---|
| Soak | Driver script (`scripts/soak.ps1` or extended bench) across 100/500/2000 tx × 1/10/50 conc, mid-run kill-9, ends all-suites + test-bank + bench green; numbers committed |
| Runbook checks | `test-bank --ops` mode (or scripts): reconcile-UNKNOWN, DLQ-path, restart-recover, rotate-key — each PASS/FAIL, non-zero exit on failure |
| Backup | Copy sealed `journal-*.wal` + `events-*.wal` + `meta.json` (+ `dlq.wal`, `audit.wal`); restore = copy to fresh dir, boot, all txs queryable |
| Rotation | Dual-key window: accept old+new during rotation; reload via SIGHUP handler (simplest) or restart-free file re-read; test rotates mid-load with zero 401s |

## Pitfalls

- SIGHUP on Windows: `tokio::signal::windows::ctrl_*` doesn't cover HUP. Cross-platform simplest: poll key-file mtime every 30s when `KEY_FILE` mode is used; env-var mode requires restart (documented). Decision: support `BANK_KEYS_FILE`/`CLIENT_KEYS_FILE` with mtime polling; env mode = restart-required.
- Backup of a LIVE journal copies a moving tail — procedure: backup only after graceful shutdown OR copy + replay-tolerant restore (wal-db truncates torn tails on open, so a live copy still replays cleanly; document both, test the shutdown variant).
- Soak must not depend on wall-clock midnight (day-bucket rotation mid-soak is fine — routing is by hash, buckets just rebalance).

## Planner must enforce

- OPS-01 (metrics evidence in pilot numbers), OPS-02 (soak + scripted checks green), OPS-03 (keys/TLS/audit already live; rotation test closes the loop).
