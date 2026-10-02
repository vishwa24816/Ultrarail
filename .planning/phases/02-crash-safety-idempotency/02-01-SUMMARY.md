# Summary 02-01: Partitioned writers + structured IDs + group commit

**Status:** complete

## What was built
- `src/partition.rs`: minute `bucket()`, `route()` over (bucket,scope,key,sender,receiver), structured `tx_id()` `{bucket}-{sender}-{receiver}-{uuid8}`. Tests pin determinism + spread.
- `src/journal.rs`: split accept into `prepare()` (validate+serialize, no I/O) + `commit_batch()` (append all, ONE sync). In-batch duplicate keys reuse the first tx. Fixed `read_tx` to match by tx_id (was returning first record — latent double-payment bug). Legacy `accept()` kept for unit tests.
- `src/app_state.rs`: `spawn_writers(dir, n)` — N tasks, each drains its queue per wake and group-commits. `PARTITIONS` env, default cpus/2.
- `src/api.rs` + `main.rs`: router hashes scope+key before dispatch; `journal-{i}.wal` files; boot logs per-partition replay counts, fail-closed.
- `src/domain.rs`: `Money` now carries `price` + `quantity`; `amount == price*quantity` enforced (checked mul); API defaults price=amount, quantity=1.
- `tests/acceptance.rs`: fixed server-lifetime bug (return `Child` to test body; `mem::forget` leaked servers and locked the exe on Windows).

## Verification
- `cargo clean` + `cargo test`: 13/13 pass (10 unit + 3 acceptance).
