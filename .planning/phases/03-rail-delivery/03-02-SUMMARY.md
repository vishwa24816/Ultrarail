# Summary 03-02: Graceful shutdown + supervision + validated config

**Status:** complete

## What was built
- `src/config.rs`: single validated `Config::from_env()` (PAYMENT_* names + legacy fallbacks); unknown keys abort boot (caught its own test hook key once); `meta.json` partition-count guard aborts with drain instructions.
- Shutdown: SIGTERM/test-hook → draining flag (503 + Retry-After) → watch broadcast → writer final sync + delivery drain → supervisors join → exit 0 inside deadline, non-zero past it. Proven by `graceful_shutdown_drains_and_exits_clean` (late work refused, 5/5 pre-drain txs durable, exit 0).
- Supervision: per-partition supervisor catches panics (catch_unwind), reopens WAL + rebuilds attempt budgets from event replay, backs off, gives up after N rapid crashes with `partition-down` DLQ; others keep serving. Proven by `crashed_partition_gives_up_and_others_serve`.
- `/ready` (replayed + workers) vs `/health` (alive); spawn helpers wait on `/ready`; TraceLayer failure logging silenced so readiness polling doesn't spam.
- Writer handles are replaceable slots so restarts don't strand handlers.

## Verification
- `cargo test`: 20/20.
