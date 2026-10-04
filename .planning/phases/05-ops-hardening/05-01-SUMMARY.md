# Summary 05-01: Backup/restore + key rotation without downtime

**Status:** complete

## What was built
- Key-file mode (`BANK_KEYS_FILE`/`CLIENT_KEYS_FILE`, env names + legacy fallbacks): pairs (not map) so one name holds several secrets during rotation; 30s mtime reloader (`KEY_RELOAD_SECS` override for tests); key paths logged, never secrets.
- `scripts/backup.ps1` (drain-first, checksum manifest) + `scripts/restore.ps1` (verify-then-copy).
- `tests/backup.rs`: rotation (old→dual→new, zero in-window 401s, old revoked after) and backup round-trip (7 files, 10/10 txs queryable after wipe+restore).

## Bug found by test
- Same-name rotation keys overwrote each other in the HashMap (old secret lost on dual-write). Maps replaced with pair vecs.

## Verification
- `cargo test --test backup`: 2/2.
