# Verified NHL Points PP-TOI Research Dataset

Research-only, offline counterfactual. No source acquisition, database write, model fit/score, or promotion occurred. The pending 32-row correction was not applied to canonical data; the retained counterfactual replay was used as-is.

Qualified feature rows: **21,599** of the immutable **90,486** frozen population.
Exact current-raw replay equality to the frozen PP-TOI feature: **21,519/21,599 (99.63%)**. The **326** baseline exceptions are separate; 80 are fully source-verified but explicitly flagged in the feature dataset, and 246 are excluded from it.
The verified counterfactual feature is independently recomputed from the retained contributor lineage for **100% of the 21,599 qualified rows**. This is corrected-value reproducibility; it is distinct from exact current-raw reproduction of the frozen baseline.

`points_hgb_verified_pp_toi_research_features.csv` retains the 21,599 qualified rows. `mean_pp_toi_last10` is replaced by the retained verified counterfactual value; the frozen value is retained as `mean_pp_toi_last10_frozen`. Other HGB V1 model features are copied unchanged. Rows with baseline compatibility exceptions are labeled and must not be silently included in exact-baseline comparisons.

`strict_prior_verified_contributor_lineage.csv` retains each source-verified contributor identity, original/reconstructed PP-TOI, reconciliation status, and PBP/boxscore/shiftchart hashes. It includes only strict-prior rows in the inclusive 120-day window and the latest ten for each target.

`baseline_exceptions_326.csv` preserves all 326 known current-raw replay versus frozen-feature mismatches. The 246 partially verified exceptions are not admitted to the feature dataset. `research_population_strata.csv` includes frozen-population and qualified-cohort counts for season, season-month, prior-window-game count, lifetime-game count, and points outcome strata.

The manifest records source and output SHA-256 values, row counts, feature order, qualification/reproduction counts, and invariants. Rebuild with `./bin/python backend/nhl/scripts/build_nhl_points_verified_pp_toi_research_dataset.py --out-dir artifacts/analysis/nhl/pp_toi_verified_feature_preparation_2026-10-16`.

This dataset is for controlled research only. It does not authorize model training, scoring, promotion, or canonical data changes.
