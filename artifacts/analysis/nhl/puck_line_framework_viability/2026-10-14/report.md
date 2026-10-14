# NHL Puck Line framework viability

## Scope and evidence

Read-only characterization of regular-season prospective captures from **2026-09-29 through 2026-10-13**. The final paired set contains **100 games across 14 dates**. September 29 is omitted because its retained prospective capture has no challenger prediction; the reconciliation dates after that are represented. Every scored row has a qualified prediction from both puck-line lanes and the Moneyline control timestamped before scheduled start, and an official final outcome. See `paired_games.csv`, `date_level_metrics.csv`, `cumulative_metrics.csv`, and `lineage_manifest.json` for exact identities and paths.

The repository calls `NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1` the puck-line **control/reference**. It does not formally call it a champion. The challenger is `NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2`. Both use the same frozen multinomial coefficient artifact (`2b72a59436ace35435b86c2dd1fa36afd3ee65fa73542db5093dd92571121d9a`) and six-feature contract. The challenger applies the `NHL_PUCK_LINE_SHOT_PRIOR_POLICY_V1` feature overlay; its retained implementation is [shot_prior.py](../../../../../backend/nhl/cross_market_shadow/shot_prior.py), SHA-256 `2685167120bdda0a9d45c6ae7e5f252fbaf6ca65dc9b4be076086b12b0e4b13c`. The frozen model parameters are [frozen_puck_line_control_v1.json](../../../../../backend/nhl/cross_market_shadow/frozen_puck_line_control_v1.json); scorer/grader is [core.py](../../../../../backend/nhl/cross_market_shadow/core.py); canonical grading labels are defined in [postgame_reconcile/core.py](../../../../../backend/nhl/postgame_reconcile/core.py). Historical fit/evaluation package: `artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/` (701-game fit; 1,312-game 2025 forward evaluation). Production-facing prospective contract is `DEFINED_NOT_ACTIVATED`; authority terminology remains shadow/reference.

The canonical label uses the official final home-minus-away score: absolute margin 1 is `ONE_GOAL_GAME`; margin at least 2 is the winning side's `*_BY_2_PLUS` class. The retained canonical class uses the final score including the decisive overtime/shootout goal. This window's evidence does not cleanly separate regulation, overtime, and shootout results, nor identify empty-net goals; no such split is inferred.

## Outcome difficulty and baselines

| Outcome | Games | Frequency |
|---|---:|---:|
| `AWAY_BY_2_PLUS` | 34 | 34% |
| `ONE_GOAL_GAME` | 34 | 34% |
| `HOME_BY_2_PLUS` | 32 | 32% |

There were 54 home wins and 46 away wins. The margin was exactly one goal in 34 games, exactly two in 22, and three or more in 44. Thus 66% finished with a two-plus goal margin; target classes are nearly balanced, so a high naive accuracy is not available from class imbalance.

| Benchmark | Result | Interpretation |
|---|---:|---|
| Full-window modal class (`ONE_GOAL_GAME`) | 34/100 (34.0%) | Descriptive only; it uses the full study window and is not a prospective baseline. |
| Expanding prior-date modal class | 30/97 (30.9%) | Strictly prior dates only; first date has no prior state and is omitted. |
| Expanding Moneyline-favored-side-conditioned class mode | 29/97 (29.9%) | Uses only prior-date class frequencies within the home/away favorite cell; first date omitted. |
| Moneyline favored winner direction | 60/100 (60.0%) | Winner-direction only; this does not predict the margin class. |

The expanding favorite-side class mode is a very small-history benchmark, not a fitted predictor. Moneyline probability-bin breakdowns are in `moneyline_probability_bins.csv`; counts are small above 0.70, so those tail rates are descriptive.

## Prospective model results

| Metric (100 paired games) | Reference | Challenger |
|---|---:|---:|
| Correct / accuracy | 36 / 36.0% | 35 / 35.0% |
| Balanced accuracy | 35.85% | 34.80% |
| Multiclass log loss (lower better) | 1.1107 | 1.1118 |
| Multiclass Brier sum (lower better) | 0.6764 | 0.6789 |

For proper scores, a no-skill uniform three-class forecast has log loss **1.0986** and Brier sum **0.6667**. Both model forecasts are worse than this reference. The reference's 36% accuracy is only two percentage points above the retrospective modal-class hit rate and its probabilities do not beat the uniform score.

Reference per-class recall: away-by-2+ **26.5%**, one-goal **52.9%**, home-by-2+ **28.1%**. Challenger: **11.8%**, **67.6%**, **25.0%**. Reference precision is 52.9%, 30.5%, and 37.5% respectively; challenger precision is 57.1%, 31.5%, and 40.0%. Full actual-row/predicted-column confusion matrices are in `class_confusion_metrics.csv`.

Mean predicted versus observed class probability was reference: away 29.2% vs 34%, one-goal 40.4% vs 34%, home 30.4% vs 32%; challenger: 27.8% vs 34%, one-goal 41.9% vs 34%, home 30.4% vs 32%. Both overstate one-goal frequency on average, and neither shows useful aggregate calibration evidence in this small sample. Class-level mean-probability diagnostics are in `class_probability_calibration.csv`; date stability and cumulative results are in the corresponding CSVs.

### Reference/challenger disagreements

They disagree on **21 games**. On those rows the reference was right and challenger wrong **8** times; challenger right and reference wrong **7** times; both wrong **6** times. Realized classes among disagreements were 7 away-by-2+, 8 one-goal, and 6 home-by-2+. The reference predicted away-by-2+ 12 times, home-by-2+ 6, and one-goal 3; the challenger predicted one-goal 17 times and each two-plus class twice. This is a material directional shift toward one-goal labels, without net improvement. Proper scores and hit rate both deteriorate slightly, so the shot-prior overlay is not a genuinely useful challenger on this evidence.

The key failure is mostly **margin-severity classification**, not simply choosing the wrong winner. For the 66 actual two-plus games, the reference made the correct class 18 times, undercalled the margin as one-goal 41 times, and picked the wrong winner direction 7 times. The challenger did so 12, 50, and 4 times respectively. For 34 one-goal outcomes, the reference predicted one-goal 18 times (13 overcalled with the right winner direction, 3 with wrong direction); challenger predicted one-goal 23 times (9 overcalled with the right direction, 2 with wrong direction). The challenger shifts errors from some outright direction mistakes into conservative one-goal predictions, but does not improve overall scoring.

## Moneyline and market comparison

The retained `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2` probability selected the correct winner **60%** of the time. By home-win-probability bins, the Moneyline direction hit rates were 57.9% for ≤0.50 (n=38), 47.1% for (0.50,0.60] (n=34), 75.0% for (0.60,0.70] (n=20), 83.3% for (0.70,0.80] (n=6), and 100% for (0.80,0.90] (n=2). Model three-class accuracy across those bins does not track these rates consistently; see the bin CSV. The evidence supports winner direction as an informative input, but does not show that either puck-line model adds dependable margin severity information beyond it.

Standard puck-line prices were available as paired, same-book pregame quotes on **73/100 games (73%)**, from 24 retained books (per-book paired coverage ranged from 28 to 65 games; see `market_provider_coverage.csv`). On this subset, mean game-level binary home -1.5-cover Brier was reference **0.2350**, challenger **0.2297**, and no-vig market consensus **0.2168**. Home -1.5 side accuracy was reference **68.5%**, challenger **64.4%**, and market **68.5%**. The retained market has the best proper score, though this remains a small, selected sample. The three-class target maps directly to the standard ±1.5 market: `HOME_BY_2_PLUS` is a home -1.5 cover; `AWAY_BY_2_PLUS` is an away -1.5 cover; and a one-goal game covers +1.5 for either selected side. The outcome is market-aligned, while the model has not learned it reliably.

Descriptive returns use **BetOnline (`betonlineag`)**, the latest qualified -1.5 quote before scheduled start, and one unit staked on each predicted directional two-plus class; one-goal predictions abstain. Reference: 38 priced bets (18 wins, 20 losses), **+6.13 units / +16.1%** per unit staked; challenger: 23 priced bets (12 wins, 11 losses), **+4.86 units / +21.1%**. Reference had 3 missing selected-side prices and 59 one-goal abstentions; challenger had 4 missing prices and 73 abstentions. This is a highly selective, small-sample descriptive return with no significance claim; it does not offset the weak all-game classification/probability results or establish an edge. Full counts and computation convention are in `roi.csv`.

## Diagnosis and direction

The 100-game paired run is useful characterization but remains a small prospective sample. It does not support a champion claim or stable calibration claim. The historical frozen reference's 2025 forward report already characterized superiority over fit-frequency controls as inconclusive; this live prospective slice is weaker still (36% accuracy against a 34% descriptive mode and 30.9% expanding mode, with worse proper scores than uniform class probabilities). The challenger worsens classification and proper scores, with no demonstrated incremental value from the shot prior.

The three-class task is coherent with the market, but the next research design should model a richer **goal-difference distribution** (potentially from a joint home/away score model or Skellam-style difference distribution) and derive `P(home margin ≥2)`, `P(away margin ≥2)`, and `P(|margin|=1)` coherently. Research should compare those probabilities directly with the internal classes and binary cover prices, with explicit OT/SO and late empty-net treatment only where retained evidence supports it. Do not fit or activate that replacement from this study.

Recommendation: **retire the current shot-prior challenger from claims of incremental value; retain the simple baseline only as a shadow/reference comparator; design a margin-distribution research track.** The current reference classification is **NO_EVIDENCE_OF_USEFUL_REFERENCE**; this does not mean its definition is a champion or that the framework is target-misaligned. No production authority or scoring behavior was changed. No formal puck-line champion exists in repository terminology.

## Validation and provenance

- Provider calls: **0**; paid credits: **0**; production database mutations: **0**.
- Utility: `backend/nhl/scripts/analyze_nhl_puck_line_framework_viability.py`.
- Focused test: `./bin/python -m unittest backend.nhl.tests.test_nhl_puck_line_framework_viability` — passed.
- Compilation: `./bin/python -m py_compile ...` — passed.
- `git diff --check` — passed.
- Package lineage: `lineage_manifest.json`; machine-readable findings: `summary.json`.
- ROI table: `roi.csv` (omitted with reason); market comparison: `market_comparison.json`; paired rows: `paired_games.csv`.
