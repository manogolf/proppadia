# NHL Puck Line goal-margin distribution research

Research-only, retained artifacts. Fit season: 2025 (1292 regular-season games). Prospective evaluation: 2026-09-29 through 2026-10-13 (100 games).

## Data and features

2025 final home/away scores and decision types: certified outcome spine and strict-prior control spine. 2026 targets and frozen reference/challenger predictions: framework-viability paired set. Features: `diff_std_goal_diff_pg`, `diff_r10_goal_diff_pg`, `diff_std_shot_diff_pg`, `diff_days_rest`, `home_back_to_back`, `away_back_to_back`; all are established strict-prior cross-market features. Moneyline probability is a pregame frozen V2 feature. No 2026 labels entered fitting.

## Candidate scores

| Candidate | N | Accuracy | Balanced accuracy | Log loss | Brier sum |
|---|---:|---:|---:|---:|---:|
| SIMPLE_GOAL_DISTRIBUTION_BASELINE | 100 | 0.3400 | 0.3333 | 1.1316 | 0.6900 |
| MONEYLINE_CONDITIONED_MARGIN_BASELINE | 100 | 0.3400 | 0.3358 | 1.1152 | 0.6781 |
| REGULARIZED_GOAL_POISSON | 100 | 0.3700 | 0.3646 | 1.1040 | 0.6740 |
| REFERENCE | 100 | 0.3600 | 0.3585 | 1.1107 | 0.6764 |
| CHALLENGER | 100 | 0.3500 | 0.3480 | 1.1118 | 0.6789 |
| UNIFORM | 100 | 0.3400 | 0.3333 | 1.0986 | 0.6667 |
| PRIOR_2025_BASE_RATE | 100 | 0.3400 | 0.3333 | 1.1184 | 0.6804 |

The regularized goal-Poisson model modestly improves on the frozen reference and challenger, but every new candidate has worse log loss and Brier score than uniform probabilities. That does not support a prospective shadow. Classification: `NO_EVIDENCE_DISTRIBUTIONAL_APPROACH_IMPROVES_PUCK_LINE`.

## Severity and benchmarks

The prior reference called 18/66 actual 2+ games at the right severity, undercalled 41 as one-goal, and chose the wrong winner 7 times. The prior challenger called 12/66 correctly, undercalled 50, and chose the wrong winner 4 times. Candidate-level counts are in `margin_severity_diagnostics.csv` and summarized in `summary.json`.

`case_examples.csv` lists evaluated examples with candidate probability at least 0.60 for 2+ or one-goal outcomes, plus direction-correct severity undercalls. An empty category means no game met that strength threshold.

## Market

`market_comparison.json` evaluates binary home -1.5 cover on games with retained consensus prices. It is not a three-class market comparison. Quote-level ROI was omitted because exact BetOnline selected-side prices and timestamps are not preserved in the paired artifact.


Classes are ordered away by 2+, one-goal final, home by 2+. Full final score includes decisive OT/SO goal. See CSVs for confusion, calibration by Moneyline bin, and per-game severities.

## Distribution diagnostics

```json
{
  "training_rows": 1292,
  "training_season": 2025,
  "home_goal_mean": 3.1996904024767803,
  "home_goal_variance": 3.0940975028357793,
  "home_overdispersion_variance_to_mean": 0.9669990260434995,
  "away_goal_mean": 3.0603715170278636,
  "away_goal_variance": 2.8484051291028867,
  "away_overdispersion_variance_to_mean": 0.9307383476987683,
  "home_away_goal_correlation": -0.11182362766714785,
  "observed_one_goal_rate": 0.4311145510835913,
  "independent_poisson_one_goal_rate": 0.4610565644184135,
  "observed_two_plus_rate": 0.5688854489164087,
  "independent_poisson_two_plus_rate": 0.5389434355815865,
  "observed_margin_frequency": {
    "-8": 1,
    "-7": 3,
    "-6": 4,
    "-5": 25,
    "-4": 64,
    "-3": 134,
    "-2": 113,
    "-1": 272,
    "1": 285,
    "2": 115,
    "3": 165,
    "4": 73,
    "5": 26,
    "6": 6,
    "7": 3,
    "8": 2,
    "9": 1
  },
  "outcome_decision_type_counts": {
    "REGULATION": 969,
    "OVERTIME": 206,
    "SHOOTOUT": 117
  },
  "empty_net_state_counts": {
    "UNRESOLVED_NOT_RECOVERABLE_FROM_LOCAL_CERTIFIED_INPUTS": 1292
  },
  "empty_net_analysis": "NOT_SUPPORTED_BY_RETAINED_EVIDENCE"
}
```

OT/SO analysis is in `ot_so_analysis.json`. Empty-net analysis: `EMPTY_NET_ANALYSIS = NOT_SUPPORTED_BY_RETAINED_EVIDENCE`; only 16 local PBP files were retained and the certified 2025 spine marks late empty-net attribution unresolved. Exact BetOnline quote-level ROI was not computed for new candidates because the retained paired artifact lacks exact selected-side prices and timestamps.

The 2025-to-2026 split is chronological and strict: candidate features are pregame fields from the existing strict-prior spine; only season 2025 was used for fitting. Reference/challenger/uniform/prior base-rate comparisons are in summary and candidate table.
