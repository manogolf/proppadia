# NHL Points model × market intersection map

## Scope and method

Prediction-level analysis of immutable 2026 HGB predictions, retained market attachments, and official participating outcomes. Finalized dates: Oct. 10–14. Oct. 10–13 are `DISCOVERY_CONTEXT`; Oct. 14 is `FIRST_PROSPECTIVE_CONTEXT`. Oct. 15 is pending inventory only. One prediction/consensus/outcome per `(slate_date, game_id, player_id, line)`, without provider multiplicity. Primary line is 0.5; 1.5 is separate; 2.5 is descriptive and sparse. Unresolved participation remains visible but is excluded from score denominators.

The full probability gap is HGB P(Over) minus normalized retained market P(Over). Production edge is the attached selected-side model probability minus the retained raw selected-side market implied probability. Selected-side gap is a third distinct quantity. Earlier canonical side-specific implied probabilities were normalized by their sum to derive no-vig P(Over).

## Where we stand

At 0.5 there are 714 settled rows. HGB and market agree on 507 (71.0%) and disagree on 207. Quadrants: both correct 330, both wrong 177, HGB correct/market wrong 86, market correct/HGB wrong 121. Directional disagreements favor HGB 86/207 (41.5%) to 121.

HGB is 416–298 (58.3%); market is 451–263 (63.2%). HGB Brier/log loss 0.2518/0.7012; market 0.2270/0.6477. HGB expected losses 213.3, observed losses 298, excess losses 84.7.

On same-side rows both are 330–177 (65.1%); HGB Brier/log loss 0.2344/0.6647, market 0.2232/0.6408. On opposite-side rows HGB is 86–121, market 121–86; Brier 0.2943 vs 0.2362. Market leads in both populations and more strongly when sides differ.

HGB P(Over)<50% contains 689 rows; HGB 398–291, market 433–256, Brier 0.2522 vs 0.2273. In 45–50% (N=32), HGB is 11–21 and market 23–9; realized Over rate 65.6%, mean HGB probability 47.5% and market probability 59.0%. In 40–45% (N=46), HGB 22–24 and market 28–18; HGB has slightly lower log loss (0.711 vs 0.736) but worse Brier and record. The <30% pool (N=381) is HGB 244–137 and market 256–125. No settled primary rows have HGB P(Over)>65%; empty upper bands remain visible.

All 207 crossings are HGB<50/market≥50: HGB wins 86 and market 121. Reverse crossings: zero. Within 5pp of 50%, both near: 10; HGB near/market decisive: 16; market near/HGB decisive: 136; neither near: 552. Same-side confidence groups and selected-confidence bands are reported separately; greater HGB confidence is not a general guarantee of lower proper-score loss.

## Complementarity and where to keep watching

The line-separated 2D matrix and rankings show where absolute, excess, and log-loss burdens collect. Both-wrong has 177 rows, Brier 0.526 HGB vs 0.391 market; both-correct has 330, Brier 0.078 vs 0.133. Local HGB wins and a small 40–45% log-loss advantage suggest information worth observing, but disagreement outcomes favor market 121–86 and market wins overall. This evidence does not yet establish durable complementarity. Continue prospective observation by date, particularly sub-50 and disagreement segments; treat 45–50% and long-price positive selected-side edges as questionable, not as filters.

Fixed 25/75, 50/50, and 75/25 blends are diagnostics only; no weights were tuned. Full signed probability-gap, selected-side-gap, production-edge, confidence, price, side, date, cumulative, and fixed-blend tables are available as separate CSVs. Oct. 13's larger contribution remains visible in date/cumulative tables. Oct. 14 has 56 settled 0.5 rows: HGB 36–20, market 33–23; it is a first prospective context, too small to settle the question.

Oct. 15 pending snapshot contains 210 quote-complete market matched prediction lines: 185 at 0.5, 23 at 1.5, and 2 at 2.5. At 0.5, same/opposite side counts are 130/55; HGB<50/market≥50 crossings: 55, reverse: 0. Rows without two-sided market quotes cannot support no-vig intersection metrics and are excluded from this count. All pending rows are ungraded.

## Package and guardrails

The package contains canonical settled/unresolved rows; quadrant, side agreement, same-side confidence, opposite-side divergence, full HGB/market probability bands, sub-/above-50 analyses, full 2D matrix/loss density, crossing/near-boundary, price, date/cumulative, proper-score/complementarity, fixed blends, loss/success rankings, pending inventory, lineage hashes, and backlog context.

No filter, threshold, production model, or Funnel V1 change. Provider calls: 0; paid credits: 0; production DB mutations: 0.
