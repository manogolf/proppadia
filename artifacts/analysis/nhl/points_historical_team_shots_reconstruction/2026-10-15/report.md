# NHL Points historical team-shot reconstruction and evidence preservation

## Status

- Historical reconstruction: **HISTORICAL_FEATURE_DEFECT_MATERIAL**
- Future pregame evidence capture: **PREGAME_EVIDENCE_CAPTURE_DESIGNED_NOT_INSTALLED**

## Original frozen evidence

The exact validation source frame is `artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz` with SHA-256 `45605af7f472481af4d5a33a103b679ab4284db5d043f82842757297a3534892`. Its 2023 and 2024 training populations contain 47,221 and 43,265 player-games (90,486 total); 2025 is a separate 47,239-row holdout. The frozen HGB artifact SHA-256 is `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`; its ordered ten-feature contract and StandardScaler are preserved in `original_training_evidence_inventory.json`.

The original matrix and model are not modified. Official completed team-game boxscores provide source coverage for all historical target populations in this reconstruction; no database rebuild or additional provider call was needed.

## Training contamination

| Season | Rows | Changed | Changed % | Original mean | Official reconstruction mean | Mean absolute difference | Maximum difference |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2023 | 47221 | 9057 | 19.18% | 29.849 | 29.991 | 0.270 | 41.000 |
| 2024 | 43265 | 21490 | 49.67% | 28.012 | 27.988 | 0.355 | 27.000 |

Across training, 30547/90486 rows change (33.76%). Source-supported rows: 90486; unsupported rows: 0. No-history remains zero. Distribution by season maturity and history depth is in `training_contamination_summary.csv`.

The 2023 identity reconciliation is at two different grains. The original training frame has 44179 target-frame numeric ID values that differ from current NHL physical-team IDs; these are not counted as errors because the historical frame uses a stable alternate ID mapping for most rows. The prior raw-log audit separately checked 8043 historical source rows and found 2194 raw team-ID/official-side mismatches. Three raw ID season mappings (5, 14, 16) each point to two physical teams (3 edges spanning 4481 rows); 54 assignments are on the minority edge, but the retained mapping evidence does not establish which side produced the original training feature. There were 118 source-log rows with missing team IDs and 53 opening-week target player-games with nonzero original team history despite zero official prior games for their physical team. Stable aliases are separated in `2023_identity_discrepancy_resolution.csv` from the unresolved edges and missing sentinel.

## 2025 holdout

The corrected official reference has 47239 source-supported target rows and 65.77% exact row agreement with the original feature. Original and corrected feature means are 27.428 and 27.599. The 2025 per-team-game skater-sum reconciliation identifies 268 differing team-games: incomplete coverage 203, unexplained full-coverage totals 60, and unexplained excess player population 5. Of changed target features, 16170 include at least one prior game whose skater sum differs from the official team total; 0 changed rows have no such source-total difference identified. Per-game counts, missingness, team binding, and raw-vs-official totals are in `2025_skater_sum_official_total_discrepancies.csv`. For the 60 full-18-player groups where totals still differ, source revisions versus aggregation causes cannot be separated from retained artifacts; those cases remain unexplained.

## Frozen HGB sensitivity

The same frozen HGB and scaler were replayed with only `team_d10_sf_per_game` replaced. 2025 original versus corrected-feature results: expected Points mean 0.4591 vs 0.4601; count residual +0.0012 vs +0.0001; Brier 0.2067 vs 0.2068; log loss 0.6015 vs 0.6016. These are retrospective sensitivity metrics, not a prospective promotion result and do not establish model compatibility.

## Rebuild scope and decision

- Historical database rebuild: **not required** for this feature; retained official completed team-game source is sufficient.
- Historical feature reconstruction: rebuild only `team_d10_sf_per_game`; retain all original labels and nondefective feature columns unchanged.
- Training contamination: material. A corrected training matrix can be built by verified single-column replacement.
- Next modeling step: run one controlled date-blocked challenger with the corrected column, unchanged other inputs and labels, independently corrected 2025 holdout, corrected 2026 reference for diagnosis, and future prospective outcomes. Keep the frozen incumbent as an unpromoted control. Do not retrain as part of this study.

## 2026 evidence preservation

The actual production Phoenix path is `nhl.skater_game_logs_raw` → `backend/nhl/sql/export_points.sql` → `train_nhl_points_v2.csv` → `prepare_scoring_input` → `score_nhl_points_with_lineage.py` → `score_points_phoenix.py`. `export_nhl_points_hgb_features.py` and immutable `points_hgb_shadow` captures belong to the separate HGB research/shadow path. Existing HGB captures are not a substitute for Phoenix source-to-feature lineage.

An exact source-to-feature snapshot can be inserted at the `cli.py` daily export block immediately before scoring, using the same read-only repeatable-read PostgreSQL session as the feature export. The minimal patch proposal is: retain the exact production export bytes; capture target/game/team bindings; export the bounded raw player-game fields used by `team_logs`; retain grouped prior team-games, contributing IDs, source row/missingness counts, denominator, and computed D10; validate the evidence-derived value against each exported feature; then write checksummed create-only files under `artifacts/operational/nhl/points_feature_evidence/season=.../slate_date=.../run_id=...` and bind the manifest into the Points lane receipt. A separate later query is not sufficient to claim exact capture-time inputs. This was intentionally designed but not installed because it changes the shared exporter session and requires a real read-only database run to validate safely.

## Current source and limitations

The bounded read-only source snapshot for Sep. 29–Oct. 13, 2026 found 105 games, 210 team-game groups and 3,780 player-game rows, with no missing SOG, no duplicate player-game records, no home/away binding mismatches, and 18 rows per team-game. All 3,780 current rows have NULL `shot_attempts`; this confirms the current attempts defect remains. This current database state cannot prove the original Oct. 9–14 capture state. Shot-attempt repair is outside this task.

No production database writes, provider calls, paid credits, predictions, authority, or Funnel changes were made. See `summary.json` and the row-level datasets for complete counts and provenance.
