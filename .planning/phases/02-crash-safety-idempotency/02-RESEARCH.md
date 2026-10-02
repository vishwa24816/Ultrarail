# Phase 2: Crash safety + idempotency - Research

**Researched:** 2026-10-02 (prior bench data + wal-db API verified in Phase 1)

## Problem (measured)

- Serial durable cost ≈ 7ms/tx (debug, Windows fdatasync); single writer caps throughput at ~146–174 tps; conc 50 queues to p50 270ms.
- Fix must parallelize fsync, not just append: multi-writer append with per-commit sync keeps 1 fsync per tx.

## Approach (locked by CONTEXT.md)

| Decision | Mechanism |
|---|---|
| N partitions | N wal-db WALs (one dir each: `journal-{i}.wal`), each with own writer task; `PARTITIONS` env, default num_cpus/2 |
| Routing | `partition = hash(minute_bucket, sender, receiver) mod N`; tx ID = `{bucket}-{sender}-{receiver}-{uuidv7[..8]}` |
| Group commit | Writer task drains all pending cmds per wake, appends all, ONE `sync()` for the batch |
| Recovery | Per-partition replay on boot into per-partition maps; router consults owner |
| TTL | `expires_ms` on idem entries; lazy check on lookup + 60s sweeper; default 24h via `IDEM_TTL_SECS` |
| DLQ | `dlq.wal` append-only segments: poison replay records + post-TTL retry conflicts, with reason JSON |

## Pitfalls

- wal-db `Lsn::get()` is a byte offset, not a counter — never use as sequence; per-partition ordering only.
- `Wal` is `!Sync` (single-writer per instance) — one owning task per partition, handlers send via mpsc.
- Same scope+key must route to the same partition or duplicates escape — router hashes scope+key fields, NOT the generated tx_id.
- Clock skew on buckets: bucket from server time at accept, embedded in ID; replay trusts stored bucket.

## Planner must enforce

- JRN-02 in recovery plan, IDM-01 in TTL/DLQ plan; every task has read_first + acceptance_criteria; bench re-run proves p50 collapse.
