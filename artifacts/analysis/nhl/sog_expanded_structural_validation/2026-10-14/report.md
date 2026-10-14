# NHL SOG expanded structural validation

Offline fixed-rule study on Tier 1 exact-input overlap and Tier 2 retrospective strict-prior rows only. No Tier 3 scores are included. No threshold or weight was tuned.

## Population and validation

The structural population contains 13,143 Tier-1 and 18,305 Tier-2 player-games (31,448 total; 94,344 line rows). The prior package's nominal 13,389 + 26,084 population was reduced by Tier-1 reconstruction parity requirements, official-outcome availability, and required D10/D20/exposure fields. The disposition ledger records every exclusion. Remaining Tier 3 is excluded.

Tier-1 validation: 13,143 / 13,389 exact selected-input rows; selected-rate and selected-exposure differences are within 1e-12 for this overlap, all 39,429 threshold probabilities match within 1e-12 (max 8.88e-16), and derived lambda mismatches are 0 (max 2.66e-15). 246 Tier-1 rows fail exact reconstructed input parity and are excluded; overall Tier-1 selected-exposure mismatches are 204, all outside the evaluated overlap.

## Global results

See global count and line metrics by trust tier. The combined scorecard uses the fixed order D10 production → frozen divergence midpoint → fixed soft high-rate compression; D20 is diagnostic only.

| Variant | N | MAE | RMSE | Mean residual | Mean λ | Realized mean SOG |
|---|---:|---:|---:|---:|---:|---:|
| PRODUCTION | 31,448 | 1.0559 | 1.3753 | 0.0394 | 1.5423 | 1.5817 |
| DIVERGENCE_ONLY | 31,448 | 1.0419 | 1.3559 | 0.0382 | 1.5435 | 1.5817 |
| DIVERGENCE_PLUS_SOFT_HIGH_RATE | 31,448 | 1.0376 | 1.3513 | 0.0570 | 1.5247 | 1.5817 |
| FIXED_D20_DIAGNOSTIC | 31,448 | 1.0376 | 1.3498 | 0.0370 | 1.5447 | 1.5817 |

## Main interpretation

Divergence bias: **STRONGLY_CONFIRMED**. Independent high-D10 bias: **STRONGLY_CONFIRMED**. Midpoint correction: **STRONGLY_CONFIRMED**. Combined soft correction: **STRONGLY_CONFIRMED**.
Tier-1 / Tier-2 directional agreement: divergence gap same direction=True; near-agreement D10>9 residual same direction=True. MAE gain: midpoint 0.0140; additional soft compression 0.0043.
Tier 1 vs Tier 2: divergence residual gaps are -0.595 and -0.585; near-agreement D10>9 residuals are -0.337 and -0.245. Both tiers improve in count MAE (midpoint 0.0147/0.0135; incremental soft 0.0042/0.0044). Brier improves at all three lines in both tiers.
For combined D10>9 rows, production residual is -0.467; the independent near-agreement high-D10 residual is -0.280. Equal-player weighting shows divergence improvement for 575 vs 191 players; soft improvement for 184 vs 26. Top five players account for 4.2% of positive divergence gains and 28.0% of positive soft gains.
Across teams, divergence MAE improves on 32/32; high-D10 soft MAE improves on 31/32. These are general patterns, not a basis for team-specific adjustments.
High-TOI interaction: the high-D10 soft adjustment improves MAE in low, middle, and high TOI strata, with the highest-TOI D10>9 cohort improving from MAE 1.577 to 1.497; no aggregate high-exposure overcorrection is evident. PP strata use only the reconstructed prior-season PP/EV share proxy and are descriptive.
D5/D10/D20: D5 has the worst count MAE and RMSE in both trust tiers; D10 remains the production anchor; D20 has the best full-population raw count MAE/RMSE, while the targeted rules nearly match its MAE and retain D10 on near-agreement rows. Fixed D20 remains diagnostic only.
Date/week stability: combined divergence residual gap supports the hypothesized sign on 158/162 dates and 26/26 weeks; divergence MAE improves on 146/162 dates and 25/26 weeks. High-D10 production residual remains negative on 138/161 evaluable dates and all 25 evaluable weeks. Soft MAE improves on 122/161 dates and 24/25 weeks.
Combined divergence-plus-soft prospective shadow: **JUSTIFIED FOR SHADOW REVIEW**. Remaining Tier-3 decision support: **TIER3_OVERRIDE_NOT_NEEDED**; this is not authorization to use Tier 3.

## Month, stability, robustness and safety

The seven month-level records show divergence gaps from −0.519 to −0.664 and positive midpoint MAE gains from 0.0099 to 0.0156; soft MAE gains remain positive from 0.0265 to 0.0403. The Tier-2 March/April samples contain only 2/4 rows and are not interpreted independently. Combined count MAE improves in all seven months for both corrections; month-specific Brier/log-loss are in `month_level_correction_metrics.csv`.
Early (Oct–Dec), middle (Jan–Feb), and late (Mar–Apr) divergence gaps are −0.591, −0.565, and −0.599; midpoint MAE gains are 0.0134, 0.0136, and 0.0148; incremental soft gains are 0.0346, 0.0335, and 0.0441. Evidence does not suggest a late-season-only artifact.
Baseline safety tables show count MAE does not worsen for near-agreement, low/middle/high D10, low/middle/high lambda, or low/middle/high TOI aggregate groups. All three line-level Brier scores improve globally; inspect `baseline_safety.csv` for subgroup proper scores. PP usage is a strict-prior reconstructed share proxy; team subgroup figures carry an explicit low-support flag.

Detailed outputs: `divergence_metrics.csv`, `signed_divergence_bins.csv`, `signed_divergence_monotonicity.csv`, `high_rate_metrics.csv`, `near_agreement_high_rate_metrics.csv`, `overlap_high_rate_divergence_metrics.csv`, `month_level_structural_metrics.csv`, `month_level_correction_metrics.csv`, `early_middle_late_comparison.csv`, `tier1_vs_tier2_comparison.csv`, `date_week_stability_summary.csv`, `player_robustness_summary.csv`, `team_robustness.csv`, `toi_pp_interactions.csv`, `d5_d10_d20_context.csv`, `baseline_safety.csv`, `tier3_override_decision_support.json`, and `backlog_context.json`.

## Limits and backlog

Tier 2 is retrospective reconstruction, not original-pregame evidence. The expanded clean sample supports descriptive research only. This task did not modify production or existing shadows. Preserved backlog: prospective grading of the divergence shadow; continued fixed-blend collection; historical official-TOI binding; Puck Line deferred until tomorrow; Points cross-prop sanity/explanation audit; possible Tier-3 override; and prospective combined high-D10 shadow review if supported.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Pushed: NO.
