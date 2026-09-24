# MLB stat-derived degraded-stage containment V1

Installed at `2026-09-24T10:09:57-07:00` after confirming that no MLB wrapper or stat-derived process was active.

This is operational containment, not a data correction. It does not change the legacy loader, database rows, exact-game foundation, schedules, models, predictions, or existing request eligibility.

## Behavior

The wrapper still invokes `make mlb-stat-derived-refresh` once with its original arguments. A deterministic stderr boundary identifies error evidence for that invocation. On success, execution continues through the original source with unchanged stage ordering and commands.

On nonzero exit, the wrapper preserves the original exit code and calls the containment coordinator. The coordinator durably claims and invokes only:

1. `bin/mlb_full_game_totals_daily_hook.sh` with the original date/run arguments;
2. `bin/mlb_totals_prospective_shadow_daily_hook.sh` with the original date/run/start/mode arguments.

Their existing claims, provider eligibility, and credit guards remain authoritative. Each is claimed before invocation and can run at most once for a run identity. All true dependents and unknown stages are recorded as `SKIPPED_UPSTREAM_STAT_DERIVED_UNAVAILABLE`. The wrapper then exits with the original stat-derived code, allowing the unchanged EXIT trap to release both wrapper locks and write the normal launchagent summary.

## Receipt

Path:

`artifacts/ops/mlb_stat_derived_degraded_containment_v1/<slate-date>/<run-identity>.json`

The receipt records run identity, slate/completed dates, wrapper start, requested stat date range and parameters, failure time, exact exit code, bounded error text and SHA-256 fingerprint, known-defect classification when matched, BvP state, every downstream classification/status, independent-stage durable claims/results, stale-output and checkpoint invariants, and the required overall classification.

Files and lock files are mode `0600`. Re-entry with the same identity reuses terminal claims and never repeats a claimed stage.

## Success-path comparison

- Stat command and arguments: unchanged.
- Subsequent normal stage source: unchanged.
- BvP command and exactly-once behavior: unchanged.
- Provider eligibility, claims, and credit logic: unchanged.
- LaunchAgent schedules: unchanged.
- Wrapper lock acquisition and EXIT trap: unchanged.
- Added success-path work: shell assignments, one deterministic stderr boundary, exit-code capture; no added subprocess.

## Required classifications

```text
STAT_DERIVED_FAILURE_VISIBILITY = VERIFIED
INDEPENDENT_STAGE_CONTINUATION = VERIFIED_AT_MOST_ONCE
DEPENDENT_STAGE_FAIL_CLOSED = VERIFIED
UNKNOWN_STAGE_POLICY = FAIL_CLOSED
OVERALL_WRAPPER_STATUS_INTEGRITY = VERIFIED_NONZERO_ORIGINAL_RC
LOCK_RELEASE_INTEGRITY = VERIFIED_UNCHANGED_EXIT_TRAP_AND_FIXTURE_LOCK_RELEASE
REQUEST_CREDIT_INTEGRITY = VERIFIED_EXISTING_HOOK_GUARDS_ONLY
BVP_EXACTLY_ONCE_INTEGRITY = VERIFIED_UNCHANGED_NOT_REINVOKED
LEGACY_DATA_EFFECT = NONE
EXACT_GAME_FOUNDATION_EFFECT = NONE
MONEYLINE_EFFECT = NONE
PREDICTION_QUALITY_EFFECT = NONE
MARKET_COMPARISON_EFFECT = NONE
HYPOTHETICAL_ROI_EFFECT = NONE
NATURAL_CONTAINMENT_VALIDATION = PENDING
STAT_DERIVED_RETRY = BLOCKED
EXACT_GAME_MIGRATION = NOT_APPLIED
```

The underlying defect remains visible as `POSTPONED_AS_FINAL_ADMISSION_PLUS_UNORDERED_DELETE_INSERT_CTE`.

## Validation

- Standard-library deterministic tests: 10 passed.
- Python compilation: passed.
- Installed wrapper `zsh -n`: passed.
- Offline contract validator: passed.
- `git diff --check`: passed before commit.
- Live API calls, database connections, pipeline runs, migrations, and manual retries: zero.

No natural MLB window occurred after the implementation commit during this task; operational validation therefore remains pending.
