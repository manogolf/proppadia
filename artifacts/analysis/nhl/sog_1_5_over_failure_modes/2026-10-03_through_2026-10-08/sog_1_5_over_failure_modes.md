# NHL SOG 1.5 Over failure modes

## Population and conclusion

Window: **2026-10-03 through 2026-10-08**. The analysis uses only settled official outcomes and selected authoritative production predictions for `poisson_baseline / baseline_v1`. It reconciles exactly to **566 settled Over calls**: 142 at 2 SOG, 199 at 3+ SOG, 139 losses at 1 SOG, and 86 losses at 0 SOG. There are no duplicate slate/game/player keys.

The descriptive result supports **`I_MORE_SAMPLE_REQUIRED`**. The zero-SOG group has mean lambda 2.190, one-SOG losses 2.242, successful Overs 2.506. The zero-vs-one lambda difference is -0.052 (player-cluster bootstrap 95% CI -0.197 to +0.086). 40/86 zero-SOG losses (46.5%) had actual TOI within 1.5 minutes of selected TOI, so exposure shortfall cannot explain most of that normal-TOI subset.

The production Poisson state implies 59.6 zeroes among 566 Over calls, compared with **86 observed** (difference +26.4; observed/expected 1.44x). The unconditional Over-call zero rate is 15.2% (player-cluster bootstrap 95% CI 12.2%–18.3%). Treat this as a diagnostic discrepancy, not a basis to fit another distribution.

## What separates the four outcome groups

`four_group_comparison.csv` reports n, mean, median, SD, and quartiles for lambda, Over/Under probabilities, selected rate/exposure, all recent windows, season TOI fallbacks, postgame actual TOI and shot rate, market differences, team environment and history fields. `zero_vs_outcome_group_effects.csv` adds standardized differences and player-cluster bootstrap intervals for zero-SOG cases versus one-SOG losses and both winning groups. `matched_zero_vs_win_comparison.csv` contains one-to-one matches where available: same position, within two calendar days, lambda within 0.30, selected TOI within 4 minutes, no replacement. It matched 86 of 86 zero-SOG cases; 0 remain explicitly unmatched. Pair-level differences and group-balance summaries are included.

Probability buckets and lambda quintiles are in `probability_lambda_bands.csv`; Poisson zero expectations by full population and lambda bucket are in `poisson_zero_diagnostic.csv`. Interpret confidence buckets alongside their counts.

## Exposure and shot-rate diagnostics

The normal-TOI criterion is `abs(actual TOI - selected TOI) <= 1.5 minutes`. Actual TOI is a postgame diagnostic, never a predictor. Shortfall bands and >3/>5 minute rates are in `toi_shortfall_analysis.csv`; the normal-TOI zero cases are in `normal_toi_zero_sog_cases.csv`. Realized SOG/60, actual-minus-selected TOI, realized-rate residuals, and team SOG share are diagnostic only.

`rate_exposure_decomposition.csv` reports lambda at production rate × selected TOI, production rate × actual TOI, realized rate × selected TOI, and the realized count. For zero-SOG losses, 66/86 still have production-rate × actual-exposure P(Over 1.5) above .50; this is a postgame decomposition, not causal identification.

Recent rate and TOI shapes use empirical quartiles of d5 minus d20; they are descriptive buckets rather than tuned filters. Results by group are in `rate_history_shape.csv` and `toi_history_shape.csv`. Repeat players require multiple calls before they appear in `repeated_player_failures.csv`; position/role and team/opponent/game views flag small cells. Team realized SOG and player share are postgame context. Official PP TOI was not available in the retained canonical outcome package.

The overall settled 1.5 zero-SOG rate was 27.8% (440/1,582), versus 15.2% for Over calls (86/566) and 34.8% for Under calls (354/1,016). Zero rates fell across lambda quintiles from 20.2% in Q1 (23/114) and 22.2% in Q2 (28/126) to 8.1% in Q5 (9/111), though every lambda quintile exceeded its Poisson zero expectation. By P(Over), zero rate was 21.2% in .50–.55 and 10.0% in .70+ (21/209).

TOI was within ±1 minute for 32/86 zero-SOG misses (37.2%), and within ±1.5 minutes for 40/86 (46.5%). More than 3 minutes below selected TOI occurred for 17/86 zeroes (19.8%), 21/139 one-SOG losses (15.1%), 21/142 two-SOG wins (14.8%), and 9/199 3+ wins (4.5%). The zero group’s mean actual-minus-selected TOI was -1.27 min, compared with -0.46 min for one-SOG losses.

Among the 86 cases, 8 players appeared more than once in the zero-SOG group, accounting for 17 zeroes; treat this as a monitor rather than a player-level conclusion. Defensemen had 24/116 zero outcomes (20.7%) versus forwards 62/450 (13.8%). The zero group averaged 25.0 team SOG versus 27.0 for one-SOG losses, 27.5 for 2-SOG wins, and 29.8 for 3+ wins; the window is short and this postgame signal is not a pregame rule.

`rate_history_shape.csv` shows 28/86 zeroes (32.6%) had strictly rising d5>d10>d20 rates; the analogous share was 47/139 (33.8%) for one-SOG losses and 100/341 (29.3%) among winning Overs. Decreasing recent TOI was present in 23/86 zeroes (26.7%) and 100/341 wins (29.3%), so no increasing concentration was evident.

Market-matched Over calls are partitioned by whether market P(Over) is within 5 points, the model is more bullish by over 5 points, or market is more bullish by over 5 points. Counts and 0/1/2/3+ outcome distributions are in `market_disagreement_analysis.csv`; no filter is proposed.

Shadow rows are restricted to exact common slate/game/player identities from each reconciliation-bound immutable prediction artifact. Per slate and arm, `shadow_zero_sog_flip_analysis.csv` reports availability, zero-loss flips to Under, one-SOG loss flips, and production winning Overs harmed by the same Under flip. C changed 33/78 available zero cases to Under, while also flipping 47/130 one-SOG losses and 49/332 production winning Overs. G changed 29/86 zero cases, 39/139 one-SOG losses, and 48/341 production wins. C/G effects must be read against harmed production wins; neither passes a no-harm test and no shadow is promoted.

## Hypotheses and scope

H1–H12 are adjudicated in `hypothesis_adjudication.csv`. The evidence does not validate a forward-testable zero-SOG regime, player defect, position rule, market filter, or shadow promotion. The right next step is prospective accumulation with this pregame diagnostic table and outcome labels. No predictions were reconstructed; selected prediction, feature, official outcome, and shadow artifact hashes were verified. Historical feature SHAs are posthoc exact-replay hashes, not receipt-certified runtime bytes.

No provider calls, paid credits, database reads or mutations, production changes, calibration, or alternate distributions were used.
