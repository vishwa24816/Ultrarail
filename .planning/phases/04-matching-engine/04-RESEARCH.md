# Phase 4: Matching engine - Research

**Researched:** 2026-10-04 (codebase; no external research needed — all inputs are in-repo)

## What exists to build on

- Delivery `Settled`/`Reconciled` events per partition (`events-{i}.wal`) — the matcher's input stream.
- `GET /payments/:id` — already merges tx + events; match state surfaces there.
- `Dlq` + `GET /dlq` — exception queue reuses it with `unmatched-confirmation` / `ambiguous-match` reasons.
- `test-bank` — both sides already ack; extend with duplicate confirmations + mismatched amounts.
- Per-partition writer task — the single owner where the settled-set lives (no lock, no race).

## Approach (locked by CONTEXT.md)

| Decision | Mechanism |
|---|---|
| Guard | `settled: HashSet<tx_id>` owned by partition writer; check-first, duplicates stop with `duplicate-confirmation` audit, zero journal writes |
| Rebuild | Replay `Settled`/`Reconciled` events on boot into the set |
| Settle | Exact match on (rail ref, amount, currency, counterparty, value date) → settle event + audit; insert + append in one writer turn |
| Near-miss | Anything else → exception queue with reason + full tx + audit; visibility only |

## Pitfalls

- Rail ref arrives via bank WS events: extend `BankEvent`/bank message protocol with `rail_ref`, `value_date`, `counterparty` — protocol addition must stay backward compatible (Phase 2/3 producers omit them → matcher treats missing fields as non-confident, never auto-settles).
- Settled-set is memory-only + event replay: a tx settled, then events WAL lost → double-settle possible. Accept (events WAL is fsynced per batch like the journal) and note it.
- Matcher must not slow the accept path: matching runs on delivery-settle events, never inline in POST.

## Planner must enforce

- LDG-02 in matcher plan, LDG-03 in exception plan; duplicate-confirmation test asserts single settlement + single settle event.
