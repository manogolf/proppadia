# NHL Points HGB V2 direct repair

## Result

The existing HGB exporter now constructs `team_d10_sf_per_game` from official player-game SOG records bound to canonical physical team IDs, aggregates to team-game totals, and uses the latest up to ten strict-prior games in 120 days. It rejects missing official SOG, duplicate identities, invalid team binding, and missing source timestamps. The historical pre-export intermediate was not retained, so the first historical divergence cannot be localized beyond the captured feature boundary.

Independent official-boxscore validation: 1,800/1,800 rows exact; max difference 0.0; source gaps 0; corrected mean 25.911 (settled 25.972).

The historical database was not rebuilt. The 90,486-row training matrix and 47,239-row 2025 holdout retain exact identities and labels; all nine other model features are unchanged. Only `team_d10_sf_per_game` differs. See `corrected_historical_matrix_manifest.json`.

## 2025 out-of-time holdout

| Configuration | Expected | Realized | Residual | MAE | RMSE | Over 0.5 Brier | Over 0.5 log loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| V1_ORIGINAL_FEATURES | 0.4591 | 0.4602 | +0.0012 | 0.5372 | 0.6865 | 0.20674 | 0.60154 |
| V1_CORRECTED_TEAM_SHOTS | 0.4601 | 0.4602 | +0.0001 | 0.5376 | 0.6867 | 0.20677 | 0.60158 |
| V2_CORRECTED_TEAM_SHOTS | 0.4613 | 0.4602 | -0.0011 | 0.5405 | 0.6881 | 0.20746 | 0.60309 |

V2 is slightly worse than V1 with corrected inputs on this holdout: count MAE/RMSE and the 0.5 Brier/log loss all rise slightly. Full line metrics, date/game stability, early-season rows, and confidence bins are retained in the CSV outputs. No calibration or tuning was applied.

## 2026 retrospective (1,320 settled player-games)

| Configuration | Expected | Realized | Residual | MAE | RMSE | Over 0.5 Brier | Over 0.5 log loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| A_V1_CAPTURED_DEFECTIVE | 0.3168 | 0.4659 | +0.1491 | 0.5276 | 0.7227 | 0.22115 | 0.63474 |
| B_V1_CORRECTED_OFFICIAL_TEAM_SHOTS | 0.3753 | 0.4659 | +0.0906 | 0.5382 | 0.7110 | 0.21571 | 0.62116 |
| C_V2_CORRECTED_OFFICIAL_TEAM_SHOTS | 0.3702 | 0.4659 | +0.0957 | 0.5419 | 0.7136 | 0.21678 | 0.62353 |

V1 corrected inputs (B) improve over captured V1 (A); V2 corrected (C) does not add value over B: its expected points are slightly lower, its residual/MAE/RMSE and 0.5 Brier/log loss are slightly worse. On the full settled population, high-confidence Under false picks (P(Under) ≥80%) fall from 90/390 in A to 44/215 in B and 35/180 in C. Within the exact two-sided market cohort (806 settled rows), they are 35/116, 13/45, and 6/30. The fixed healthy negative-edge reference is small (148): its 0.5 Brier is 0.1981 for A, 0.2046 for B, 0.2043 for C, so the improvement in false Unders comes with slight deterioration there. Full settled, quote, edge ≥10/15/20%, market opposition, and history-depth cohorts are in `2026_known_region_comparisons.csv`.

## Operational decision

Feature correction is integrated in the existing HGB daily exporter. HGB remains `NHL_POINTS_COUNT_HGB_V1` at its established production role; V2 is saved and evaluated but not selected. Phoenix, all other lanes, the Points Funnel, model authority, and production DB are unchanged. V1 remains the deployed model and rollback reference. No additional daily orchestration or provider requests occurred.

Pregame evidence remains `INSTALLED_AWAITING_LIVE_VERIFICATION`; a real future daily execution is required before prospective validation. The 2026 comparisons are retrospective. The separate `shot_attempts` defect is unchanged.

## Reproducibility and validation

V2 refit twice using the same 90,486-row corrected training matrix and frozen HGB recipe: maximum prediction difference 0.0; prediction hash `32bf58500e27d777adc9060c20611eee5e3fc59245e49431b417a32d9c3f5dc2`. Artifact hashes are in `model_artifact_hashes.json`. Focused tests also cover the source/canonical-team history rules.

NHL_POINTS_HGB_V2_REPAIR_STATUS = FEATURE_REPAIRED_V2_NOT_SUPERIOR

NHL_POINTS_PREGAME_EVIDENCE_STATUS = INSTALLED_AWAITING_LIVE_VERIFICATION
