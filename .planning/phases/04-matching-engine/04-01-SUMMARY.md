# Summary 04-01: Matcher core — exact settle, settled-set guard, exception queue

**Status:** complete

## What was built
- `src/matcher.rs`: `RailConfirm` (all-optional rail fields), exact `match_confirm` (rail_ref present + amount + currency + counterparty + value_date equal), `SettledSet` with `settle`/`un_settle`, unit tests.
- Writer-owned guard: `settle_one` runs check + match + record in one writer turn; duplicates → `duplicate-confirmation` audit only; near-miss → guard released + `ambiguous-match` DLQ + audit; unknown ids → single `unmatched-confirmation` DLQ.
- Delivery routes SimRail acks (`sim-{tx}` rail refs) through per-partition settle channels; bank `tx.confirmed` messages route via store lookup to the owning partition only (broadcast rejected — would DLQ-duplicate).
- `GET /payments/:id` gains `matched`; new `matched_total` / `duplicate_confirmations_total` / `exceptions_total{reason}` metrics.

## Verification
- `cargo test`: 27/27 (incl. 4 new matcher unit tests). Full pre-existing suite unaffected.
