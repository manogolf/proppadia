# MLB rolling-integrity independent-stage continuation V1

## Installed wrapper mutation

The daily wrapper is installed outside this repository at
`/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh`; there is no tracked
repository copy. This change captures `make mlb-check-rolling-integrity`'s
nonzero result, invokes the repository-owned continuation helper once, then
exits with the original integrity-check status. The existing wrapper EXIT trap
therefore still writes its summary and releases acquired locks.

- Pre-change installed-wrapper SHA-256: `3f642baa92a7350b6fae9beeaaba3c26f425b90a688b36795f5258f00be520d2`
- Post-change installed-wrapper SHA-256: `572ea9971bf43390dee30fd1f31b83191cd73f7e941910fcfc0e67c9362cbcb6`
- Repository helper: `bin/mlb_rolling_integrity_failure_containment.sh`
- Scope: on this failed gate, dependent stages are explicitly logged as skipped;
  established Full-game Totals and Totals prospective-shadow hooks each run
  once through their existing hooks/guards; the wrapper returns the original
  nonzero gate status. No threshold or data behavior changes.

## Rollback

If the installed wrapper still has the post-change hash above, replace its
rolling-integrity block with the prior direct command:

```zsh
# 1b) Verify rolling windows are populated + moving.
MLB_ROLLING_CHECK_DAYS="$MLB_ROLLING_CHECK_DAYS" \
MLB_ROLLING_CHECK_MIN_COVERAGE_PCT="$MLB_ROLLING_CHECK_MIN_COVERAGE_PCT" \
MLB_ROLLING_CHECK_MIN_COMPARABLE="$MLB_ROLLING_CHECK_MIN_COMPARABLE" \
make mlb-check-rolling-integrity
```

Do not overwrite the installed wrapper wholesale if its current hash differs;
review and reverse only this bounded block. The helper may be removed after
the installed wrapper no longer references it.

## Validation

Offline fake-hook regression coverage is in
`backend/mlb/tests/test_mlb_rolling_integrity_independent_continuation_v1.py`.
It proves exactly-once hook invocation, dependent-stage skips, original exit
status preservation despite hook failures, and lock release through an EXIT
trap. The test also checks installed-wrapper control-flow ordering. No
operational pipeline or database was run for this correction.
