# Prospective integrity

`CURRENT_MODEL_PROSPECTIVE_INTEGRITY = PASS_WITH_LIMITATIONS`

- All 5,088 primary rows have prediction timestamps before first pitch, immutable original probabilities, and exact semantic ID/full artifact SHA binding.
- Primary duplicates: 0; timing-unresolved primary rows: 0; post-start primary admissions: 0.
- Outcomes attach after predictions and do not mutate probabilities. August 10-22 use canonical reconciliation sidecars; August 3-9 have no standalone sidecar files and retain the previously governed frozen original outcome attachment. This is the explicit outcome-source limitation.
- Prior adversarial limitation remains: exact feature vectors/code cutoffs exist, but contributing source-row timestamps are not available for every historical feature observation. No same-game outcome access is evidenced.
- PA-completeness and outcome-summary idempotency repairs affect canonical outcome attachment only, not original prediction identity or probability.

`OUTCOME_INTEGRITY = PASS_WITH_LIMITATIONS`
