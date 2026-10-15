# NHL Points attempts feature lineage and causal replay

## Decision

The zero values originate from an upstream source gap: the read-only retained export contains 3,852 regular-season skater-game rows dated 2026-09-29 through 2026-10-14, and **all 3,852 have NULL `shot_attempts`**. The importer reads `shotAttempts`, or SOG plus `missedShots` when present, otherwise stores null. Provider payloads are not retained, so the exact absent source field cannot be identified. The HGB feature exporter explicitly applies `fillna(0.0)` before rolling rates; that is the first observed stage where null source data becomes zero.

The intended frozen feature is the arithmetic mean of `shot_attempts * 60 / toi_minutes` over up to ten player appearances in the prior 120 calendar days, excluding the target game date and any future games; only positive TOI contributes. No minimum-history threshold applies. The training-era HGB Python builder and 2025 holdout builder are separately identified in `feature_lineage_trace.csv`; Phoenix SQL feature construction is not used to claim the HGB lineage.

## Historical verification

The retained 2025 strict-prior feature frame has 47,239 rows; 46,008 comparable nonmissing values reproduce at tolerance 1e-12 with exact match rate 100.0%, maximum difference 3.55e-15, and history-count match rate 100.0%. This verifies the contract on retained 2025 logs, but 2023–24 training source rows are not retained here for a full training-row recomputation.

## 2026 raw availability and replay eligibility

The latest-per-date HGB captures contain 2,347 unique slate/game/player identities across 9,207 strict-prior player-game appearances. Of those, 9,206 have positive TOI and 0 have observed attempts. 393 rows across the capture dates can be bound as valid no-exposure zeros; among the 714 settled primary rows, only 3 are eligible, all with no prior positive-TOI exposure. No median or other imputation was used.

Baseline HGB replay parity: expected count max error 1.11e-16; Over max error 1.11e-16; Under max error 2.22e-16; selected-side probability max error 2.22e-16; selected-side mismatches 0. All pass 1e-12. The corrected-feature effect on the 714 settled rows is **not estimable** from three no-exposure cases; these cases do not test positive attempt rates. Fixed Under-confidence, displayed Edge, same/opposite market side, negative Edge, wrong-vs-correct and representative-case tables retain their full-population denominators and report the small eligible subset.

## Frozen-model sensitivity

On the retained 2025 holdout, setting attempts to zero while holding all other features fixed changes mean expected points by -0.0020 and mean Over probability by -0.0029. Brier, log loss, selected accuracy and fixed original Under-confidence bands are reported in `2025_zeroing_experiment.csv` and `2025_zeroing_confidence_bands.csv`. Frozen trees use attempts in 73 of 100 trees (119 split nodes; 8.5% of splits). The fixed-input response over historical training quantiles is in `partial_dependence_attempts.csv`; it is evaluated directly and may be non-monotonic. The retained 2025 zeroing effect is a small sensitivity result and does not explain the 2026 residual.

## Classification and smallest next repair

Feature support: **UPSTREAM_DATA_GAP**. Causal contribution: **INSUFFICIENT_EVIDENCE**. The 2026 residual cannot be causally assigned to attempts because actual strict-prior attempts are absent. The smallest justified repair proposal is to restore a validated upstream `shot_attempts` source feeding `nhl.skater_game_logs_raw`, then preserve the frozen 120-day strict-prior last-ten positive-TOI rate definition; distinguish missing from true zero during auditing. If that exact training definition is restored, the frozen HGB remains contract-compatible and retraining is not required. No repair was implemented.

Oct. 15 analysis is tagged `OUTCOME_BLIND_COUNTERFACTUAL`: 547 rows, 84 source-supported no-exposure rows replayed, and 463 excluded for missing source history. No outcomes were joined and no production prediction was overwritten.

No model retraining, calibration, market input, production feature change, Funnel change, authority update, provider call, paid credit, or DB mutation occurred. The raw export used a read-only query.


NHL_POINTS_ATTEMPTS_CAUSAL_STATUS = INSUFFICIENT_EVIDENCE
