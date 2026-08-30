# MLB Totals RAW frozen champion interaction/cancellation dissection v1

## Governed result

`STAGE1_REPRODUCTION = PASS`. All 231 feature pairs were diagnosed before joint grading; the required trio made 232 candidate relationships considered. Exactly 15 pair/group tests were frozen under manifest SHA `bfd7d1c1b27908831e83a0b2dcdef6276598ba06ba2e1669979304bc3514551c` and then evaluated without refitting or changing upstream state.

The manifest contains eight justified home/away concepts, four strict-prior-factor overlap tests, two high-correlation same-side prevention/bullpen tests, and the required count/confidence trio. Workload uncertainty was explicitly declined before joint grading because its contribution correlation was near zero, one term was neutral, and no pre-joint cancellation evidence existed.

## Structural findings

- Strongest redundancy diagnostic: `S2_14` (strict_prior_total_run_factor|park_history_depth), `MODERATE`, aggregate MAE interaction residual +0.011118.
- Strongest compensation diagnostic by absolute MAE interaction: `S2_11` (home_prevention|strict_prior_total_run_factor), `STRONG` / `FORECAST_AND_SCORE_LEVEL`.
- Strongest conditional diagnostic by absolute MAE interaction: `S2_15` (home_starter_prior_starts|away_starter_prior_starts|park_history_depth), `YES`.
- `COUNT_CONFIDENCE_DIRECT_ROLE = UNRESOLVED`.
- strict-prior factor relationship result: `REMAINS_COHERENT_ACROSS_ALL_TESTED_OVERLAPS`.
- Temporal interaction counts: `{"COMPENSATING": 1, "MIXED": 6, "REGIME_DEPENDENT": 4, "STABLE": 4}`.
- Stage-2 feature counts: `{"COMPENSATING_DEPENDENCE": 1, "CONDITIONAL_DEPENDENCE": 2, "POSSIBLE_FOUNDATION": 1, "POTENTIALLY_HARMFUL": 4, "REDUNDANT_DEPENDENCE": 1, "TEMPORALLY_UNSTABLE": 3, "UNIQUE_DEPENDENCE": 6, "UNRESOLVED": 2, "WEAK_OR_NEUTRAL": 2}`.

`CHAMPION_STRUCTURE_STAGE2 = FEW_DOMINANT_CONCEPTS`. `PLAUSIBLE_CORE_RANGE = 4–9 terms/concepts`. `STAGE3_REPLACEABILITY_JUSTIFIED`.

Full constituent, joint, interaction-residual, date-cluster bootstrap, Holm, concept, and structural-label evidence is retained in this package. These are frozen-model diagnostics, not causal decompositions or replaceability findings. No reduced model was built.
