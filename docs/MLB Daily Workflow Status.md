# MLB Daily Workflow Status

Last updated: 2026-06-14

## Current Baseline

Patch 1B BvP lineage restoration is complete.

Patch 1C rolling context lineage restoration is complete.

The MLB daily workflow now preserves compact BvP context and compact d7/d15/d30 rolling production context through daily lineage health and Ops Brief visibility. Historical restoration has been applied to the approved 2026 regular-season windows where strict alignment checks passed.

## Patch 1B Summary

Restored fields:

- `bvp_plate_appearances`
- `bvp_at_bats`
- `bvp_hits`
- `bvp_total_bases`
- `bvp_avg`
- `bvp_slg`
- `bvp_payload_present`
- `bvp_source`

Historical write windows:

- `2026-04-01..2026-04-30`
- `2026-05-01..2026-05-31`
- `2026-06-01..2026-06-11`

March was intentionally not attempted.

Totals:

- files written: `233`
- BvP cells added: `83,415`
- unsafe_non_patch_changes: `0`
- final full-window post-write dry run ready_file_count: `0`
- final full-window projected BvP cells: `0`

Validation:

- row counts unchanged
- protected grade/result/price/pnl/upload-match fields unchanged
- final 8rain upload CSVs unchanged
- no DB-only reconstruction

## Daily Monitoring

BvP compact fields are included in daily feature-lineage health.

Strict artifacts fail if compact BvP columns disappear:

- slate output
- lane selector output
- ranking upload input
- Quick Card output

Upload diagnostics are advisory but visible. They now fill diagnostics-only BvP wrapper fields when direct compact BvP stats are present.

Ops Brief Freshness Audit and Source Health now show BvP lineage counts, payload rates, and warnings.

Accepted `2026-06-14` validation baseline:

- feature lineage health: `pass`
- pass: `6`
- warn: `0`
- fail: `0`
- final ranking and Quick Card upload CSV hashes unchanged

Accepted `2026-06-14` BvP payload rates:

- slate output: `90.0%`
- lane selector: `93.1%`
- ranking upload input: `90.2%`
- Quick Card output: `96.1%`
- ranking upload diagnostics: `90.2%`
- Quick Card upload diagnostics: `96.1%`

## Patch 1C Summary

Restored fields:

- `rolling_result_avg_7`
- `d7_hits`
- `d15_hits`
- `d30_hits`
- `d7_total_bases`
- `d15_total_bases`
- `d30_total_bases`
- `d7_hits_runs_rbis`
- `d15_hits_runs_rbis`
- `d30_hits_runs_rbis`
- `d7_strikeouts_batting`
- `d15_strikeouts_batting`
- `d30_strikeouts_batting`
- `d7_hits_allowed`
- `d15_hits_allowed`
- `d30_hits_allowed`

Deferred to possible Patch 1C.1:

- `d7/d15/d30_walks_allowed`
- `d7/d15/d30_earned_runs`

Daily preservation is implemented through:

- slate output
- lane selector output
- ranking upload input
- Quick Card output
- ranking upload diagnostics
- Quick Card upload diagnostics
- actual/source reconcile where applicable
- execution reconcile where applicable

Historical write windows:

- `2026-04-01..2026-04-30`
- `2026-05-01..2026-05-31`
- `2026-06-01..2026-06-11`

Totals:

- files written: `845`
- rolling context cells restored: `795,592`
- unsafe_non_patch_changes: `0`
- source_missing: `102`, skipped intentionally
- final 8rain upload files touched: `0`
- final full-window post-write dry run ready_file_count: `0`
- final full-window projected rolling cells: `0`

Validation:

- row counts unchanged
- protected grade/result/price/pnl/upload-match fields unchanged
- final 8rain upload CSVs unchanged
- no DB-only reconstruction
- final ranking and Quick Card upload CSV hashes unchanged

Final upload hashes:

- ranking: `36c555c94403e1967bf520c418e76ccb089e49e155cd552446822866df8fb058`
- Quick Card: `523d31f9faac631f3a48f57dc9b562e4e22d9283d8c09af04b40b1034b2b9097`

## Patch 1C 2026-06-14 Health Nuance

The default real current-slate `2026-06-14` artifacts were generated before Patch 1C daily preservation existed, so they fail the new Patch 1C feature-lineage health check.

Regenerated Patch 1C temp artifacts pass:

- pass: `6`
- warn: `0`
- fail: `0`

The real current-slate artifacts were intentionally not overwritten only to make the health check green. The next real daily run should naturally produce Patch 1C-compliant artifacts.

## Artifact Links

- Patch 1B status artifact: `artifacts/analysis/mlb/feature_lineage/patch_1b_bvp_lineage_restoration_status.md`
- Patch 1C status artifact: `artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_lineage_restoration_status.md`
- BvP production/alignment audit: `artifacts/analysis/mlb/feature_lineage/bvp_data_production_alignment_audit.md`
- Patch 1B recovery dry-run rollup: `artifacts/analysis/mlb/feature_lineage/bvp_lineage_recovery_window_dry_run_summary.md`
- Patch 1B final post-write dry run: `artifacts/analysis/mlb/feature_lineage/patch_1b_bvp_backfill_apr_jun_11_post_write_check_final/patch_1b_bvp_backfill_apr_jun_11_post_write_check_final_summary.md`
- Patch 1C schema validation: `artifacts/analysis/mlb/feature_lineage/feature_lineage_patch_1c_dry_run_schema_validation.md`
- Patch 1C April write report: `artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_dry_run/patch_1c_rolling_context_april_write_summary.md`
- Patch 1C May write report: `artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_dry_run/patch_1c_rolling_context_may_write_summary.md`
- Patch 1C June write report: `artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_dry_run/patch_1c_rolling_context_june_01_11_write_summary.md`
- Patch 1C final post-write dry run: `artifacts/analysis/mlb/feature_lineage/patch_1c_rolling_context_dry_run/patch_1c_rolling_context_apr_jun_11_post_write_check_summary.md`
- Daily feature-lineage health latest: `artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_latest.json`

## Guardrails

Patch 1B and Patch 1C did not change:

- model scoring
- selection logic
- thresholds
- upload row selection
- grading
- wager matching
- overlap logic
- final 8rain upload schema
