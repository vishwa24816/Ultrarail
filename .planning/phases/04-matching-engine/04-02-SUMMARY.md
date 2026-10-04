# Summary 04-02: Matching proof — duplicate storm, mismatches, bench guard

**Status:** complete

## What was built
- `tests/matching.rs`: duplicate storm (20 concurrent confirms over 2 sockets → exactly 1 writer settlement + `duplicate-confirmation` audits); deterministic near-miss suite (SIM_FAIL_RATE=1.0 so bank confirms are the only match attempts → 2× `ambiguous-match` DLQ, 0 settlements); unknown-id confirm → exactly 1 `unmatched-confirmation`; kill-9 → rebuilt guard still single-settles.
- `test-bank --matching`: 50/50 matched exactly once, late mismatch stopped at guard.
- Bench: 1932 tps (gate >1500 PASS — matcher rides settle events, off the accept path).

## Bugs found by tests (fixed)
- Status store was memory-only: `GET /payments/:id` 404'd after restart. Boot now rebuilds it from journal replay.
- Test counted delivery-level + writer-level settle events together; writer-only counting is the true double-settle signal.

## Verification
- `cargo test --test matching`: 3/3. Full suite + bench gate green.
