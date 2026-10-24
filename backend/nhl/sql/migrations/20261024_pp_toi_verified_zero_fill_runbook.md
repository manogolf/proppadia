# PP-TOI verified-zero fill and 2026 backfill (P2.31)

Status: **applied 2026-10-24** (migration by the operator as `postgres`;
backfill of 9,472 rows committed through the fixer; receipts in
`artifacts/analysis/nhl/pp_toi_2026_backfill_preview/2026-10-24_pre_commit/`).
The full-role SQL fixture was not run; the function's exact body was later
exercised against the rollback-only test database (`NHL_PP_TOI_TEST_DSN`,
`ZeroFillFunctionDatabaseTests`), covering every guard. Never print
connection strings.

## Why

Since 2026-01 the source stores "no power-play time" as NULL: reconciliation
staging inserts `pp_toi_minutes = NULL`, the official boxscore no longer
carries per-player PP time, and `nhl.fill_pp_toi_minutes_from_shift_overlap`
fills only positive overlaps. Live HGB features then average only PP games
and score non-PP players from a blank.

## What changes

- `20261024_pp_toi_verified_zero_fill.sql` adds
  `nhl.fill_pp_toi_zero_from_shift_overlap(bigint)` (SECURITY DEFINER, fixed
  search path, execute granted only to `proppadia_nhl_ingest`). It writes
  0.00 into a NULL row only when the skater played, had no PP shot on goal,
  the game has manpower segments, the skater's de-duplicated shifts are for
  one team and within 60 seconds of TOI, and none overlaps an own-team PP
  window. Populated values are never changed. The existing positive function
  is untouched.
- `fill_pp_toi_minutes_for_date.py` calls the new function after the positive
  fill, per game, only when `to_regprocedure` finds it; until the migration is
  applied the daily run is unchanged. Its summary adds `verified_zero_fills`
  and `zero_fill_function_available`, admitted to the parent receipt.
- Readers: the SOG PP-role SQL uses `NULLIF(pp_toi_minutes, 0)` and is
  unaffected. HGB V1 already scored a blank as 0, and its PP response is flat
  above 0.8 minutes, so V1 predictions should move little; the feature becomes
  correct for any refit.

## 1. Disposable fixture (no production database)

In a fresh disposable PostgreSQL database, from the repository root, run in
order with `psql --no-psqlrc --set=ON_ERROR_STOP=1 --file`:

1. `backend/nhl/sql/tests/nhl_safe_promotion_fixture_setup.sql`
2. `backend/nhl/sql/migrations/20261017_safe_skater_source_promotion.sql`
3. `backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql`
4. `backend/nhl/sql/tests/nhl_safe_promotion_fixture_test.sql`
5. `backend/nhl/sql/tests/nhl_pp_toi_zero_fill_fixture_test.sql`
6. `backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill_rollback.sql`
7. `backend/nhl/sql/tests/nhl_safe_promotion_fixture_cleanup.sql` (after the
   20261017 rollback, as in that runbook)

Both fixture tests roll back their rows and raise on any failed guard. The
role-level fixture has not been run; the function logic is covered by
`backend/nhl/tests/test_pp_toi_verified_zero_fill.py` against the
rollback-only test database (`NHL_PP_TOI_TEST_DSN`).

## 2. Apply the migration (operator, approved administrative path)

Run `backend/nhl/sql/repairs/verify_nhl_pp_toi_verified_zero_fill.sql` first
(read-only; 2026-10-24 result: installed=false). Then either paste
`backend/nhl/sql/repairs/operator_sql_editor_pp_toi_verified_zero_fill.sql`
into the Supabase SQL editor as `postgres` (it is the migration body
unchanged, plus a session check and a receipt SELECT), or run
`psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql`
through the approved administrative path. Save the receipt, then run the
verify file again.

Verify:

```sql
SELECT to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NOT NULL AS installed,
       has_function_privilege('proppadia_nhl_daily_login', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') AS daily_can_execute,
       has_function_privilege('anon', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') AS anon_can_execute;
-- expect: true, true, false
```

From the next daily run, new slates get verified zeros automatically.

## 3. Backfill 2026-01-01 to 2026-10-23 (ingestion login)

Preview receipt (read-only, research login, 2026-10-24 18:09 UTC):
`artifacts/analysis/nhl/pp_toi_2026_backfill_preview/2026-10-24/`, from
`backend/nhl/sql/repairs/preview_nhl_pp_toi_2026_backfill.sql`.

- NULL rows in window: 11,575. Would fill 9,472: 7,371 ZERO and 2,101
  POSITIVE (mean 1.95 min; all on 2026-01-09..01-25, which the daily fixer
  never filled). Left NULL: 2,103 (no shift charts, no segments, PP SOG,
  shift/TOI mismatch, or missing TOI).
- 91 dates, 2026-01-09 to 2026-10-23, listed in
  `pp_toi_backfill_preview_by_date.csv`.
- SHA-256: rows `6332d783b5db4e4e82d7f2798615f7becdd7099f054df249edbbe0e8e3d28642`;
  by-date `ef11ea18e3204ec3090498a0759878c76e0055503a5e31f011f9d9a270145bb5`;
  summary `40e76515e64aa4b080beee8ba2b95eb1c7e61f11c5a3a5446616adbc363b598c`.

Re-run the preview immediately before the backfill and compare counts; new
slates may have been filled by then. First a dry run (no `--commit`; every
game rolls back), then the write, one date at a time through the existing
governed writer:

```bash
P=artifacts/analysis/nhl/pp_toi_2026_backfill_preview/2026-10-24/pp_toi_backfill_preview_by_date.csv
for d in $(awk -F, 'NR>1{print $1}' "$P"); do
  NHL_DAILY_RUN_ID="nhl_pp_toi_backfill_$d" ./bin/python backend/nhl/scripts/fill_pp_toi_minutes_for_date.py --date "$d"
done
```

Add `--commit` for the approved write. Keep the console output: each date's
`NHL_CHILD_SUMMARY_JSON` line records `filled_nulls` and `verified_zero_fills`.
A date that fails its invariant exits 2; its per-game commits stand and are
covered by the rollback below.

## 4. Verify after the backfill

- `preview_nhl_pp_toi_2026_backfill.sql` should return no rows for the
  window (any remainder is listed with its date).
- `verify_nhl_pp_toi_2026_null_scope.sql` should show only the left-NULL
  classes.
- Summed `filled_nulls` + `verified_zero_fills` should equal the preview's
  9,472, less anything filled by daily runs in between.

## Rollback

- Data: `artifacts/analysis/nhl/pp_toi_2026_backfill_preview/2026-10-24/rollback_nhl_pp_toi_2026_backfill.sql`
  (SHA-256 `e45e87b960a154fff7a27ebc5305627297aced27689d6b88ccb44bb0c6e6f5b8`)
  restores NULL, through `proppadia_nhl_source_correction`, only where a row
  still holds the exact value in the preview receipt. It reports the count
  before COMMIT. Regenerate it from the post-backfill state if the preview
  was refreshed.
- Function: `20261024_pp_toi_verified_zero_fill_rollback.sql` removes the
  entry point; the daily fixer then falls back to positive fill only.
