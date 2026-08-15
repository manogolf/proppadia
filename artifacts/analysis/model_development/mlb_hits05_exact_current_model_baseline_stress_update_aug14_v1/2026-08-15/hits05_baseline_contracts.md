# Hits 0.5 leakage-safe baseline contracts

These definitions were frozen before the August 14 comparison and were not tuned to August outcomes.

## Baseline A — strict-prior population rate

`p_A = 0.575713564031321` for every evaluation row. This exactly reproduces the prior adversarial review's frozen pre-August-3 strict-family rate. It is estimated from 15,836 resolved original prediction rows for 537 players dated 2026-05-08 through 2026-08-02; all rows precede the first evaluated slate. Source: `artifacts/analysis/model_development/mlb_hits05_2026_season_to_date_evidence_v1/2026-08-14/hits05_season_primary_predictions.csv`.

## Baseline B — strict-prior hitter-shrunk rate

`p_B(i,d) = (hits_before_date(i,d) + 8 * p_A) / (resolved_games_before_date(i,d) + 8)`.

The eight pseudo-game rule is the unchanged governed formula in `artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_frozen_modeling_procedure.json`. Hitter histories begin from the same pre-August-3 source state as Baseline A and advance only after a completed historical date; same-date outcomes never enter that date's probabilities. A hitter with no prior resolved history receives `p_A`. There is no August tuning, market input, outcome leakage, differing evaluation denominator, or eligibility selector.

## Evaluation outcomes

Original exact-SHA strict-pregame predictions are immutable. August 10–14 use repaired canonical prospective outcome sidecars. August 3–9 retain the original frozen prospective outcome attachments that underlie the adversarial reference; no outcome was queried or reconstructed by this update.
