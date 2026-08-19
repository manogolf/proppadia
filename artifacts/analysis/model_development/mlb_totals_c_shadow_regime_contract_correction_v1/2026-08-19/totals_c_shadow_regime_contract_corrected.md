# Totals C shadow regime contract — corrected v1

Task: `MLB_TOTALS_C_SHADOW_REGIME_CONTRACT_CORRECTION_V1`

Correction reason: `HUMAN_INTENT_CLARIFICATION_BEFORE_FIRST_FORMAL_REVIEW`

This addendum supersedes the original missing-metadata interpretation without rewriting the original contract at `artifacts/analysis/model_development/mlb_totals_count_confidence_only_live_shadow_launch_v1/2026-08-16/totals_c_shadow_regime_contract.md` (SHA-256 `6411632441a7bbd23926dd204ca2c9fe3762744f127dedfdbdb22dcd16c37d54`).

## Current default

`NORMAL_COMPETITIVE_REGIME` is the default during the current mid-August shadow period. Missing elimination, roster-turnover, lineup-churn, or replacement-player metadata is not affirmative evidence and does not trigger `LATE_SEASON_TRANSITION_WATCH`.

`LATE_SEASON_TRANSITION_WATCH` requires affirmative external/non-performance evidence that the competitive population may be changing materially. `LATE_SEASON_DISTINCT_REGIME` requires affirmative external/non-performance evidence that the environment has materially changed. No calendar date or C performance determines either state.

## Operational rendering

Daily operations report `C_REGIME = NORMAL` absent affirmative evidence. Deployment watches A–I remain separate health signals and do not themselves establish a late-season competitive regime.

## Weather and completion

Rain delays, weather interruptions, extra innings, and unusual but officially completed games neither create a late-season regime nor exclude a cluster. Postponed, suspended/incomplete, unresolved official completion, or unsafe canonical grading remains a grading/data issue.

## Evidence integrity

August 17 and 18 are completed primary-regime clusters; August 19 is a pending primary-regime cluster. August 17 remains the first prospective shadow date. Formal reviews remain at 8 and conditionally 12 completed primary-regime clusters. Predictions, contexts, outcomes, model identity, artifact, snapshot policy, bullpen freshness contract, comparators, and evaluation contracts are unchanged.

Immutable historical prediction payload labels are not rewritten; the append-only correction observation is the authoritative date-level regime label. Future scores use the corrected default.

`REPEATED_STRONG_TEAM=NOT_AN_ERROR_CONDITION`. A repeated Moneyline STRONG team warrants investigation only with an independent integrity signal.
