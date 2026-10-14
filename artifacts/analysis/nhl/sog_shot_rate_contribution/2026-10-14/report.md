# NHL SOG shot-rate contribution

## Populations and replay

```json
[
  {
    "population": "historical",
    "rows": 13389,
    "replayed": 13347,
    "unreplayable": 42,
    "matches_1e-9": 13347,
    "mismatches": 0,
    "max_abs_lambda_difference": 1.3322676295501878e-15
  },
  {
    "population": "prospective_oct08_13",
    "rows": 1608,
    "replayed": 1107,
    "unreplayable": 501,
    "matches_1e-9": 1107,
    "mismatches": 0,
    "max_abs_lambda_difference": 8.881784197001252e-16
  },
  {
    "population": "prospective_feature_bound_oct10_13",
    "rows": 1107,
    "replayed": 1107,
    "unreplayable": 0,
    "matches_1e-9": 1107,
    "mismatches": 0,
    "max_abs_lambda_difference": 8.881784197001252e-16
  }
]
```

Historical unreplayable rows: 20 lack a rate input and 22 lack selected exposure. The wider prospective denominator has 501 rows without detailed bound feature inputs. Counts and reasons: `unreplayable_reasons.csv`.

## Actual rate-source use

D10 controls all 13,369 historical rows with a usable rate and all 1,107 feature-bound prospective rows. Neither D20 nor D5 supplied production rate in these measured rows. In the wider 1,608 paired prospective denominator, 501 rows lack bound feature detail, so their source is unknown from retained evidence. See `rate_source_usage.csv`.

## History depth

Prospective feature-bound D10 has 10 prior games for 1,059/1,107 rows; 48 have fewer than 10 (1 row at 2; 8 at 3–4; 21 at 5–7; 18 at 8–9). Their combined MAE is 1.010 versus 1.041 at full 10; the sparse group has only 48 rows and no monotonic pattern across bins, so this does not show material degradation or support a gate. Historical certified rows lack an exact retained prior-game count; `num_event_last10` is not an appearance count. Upstream 5/10/20 rollups use up to the last 5/10/20 prior appearances with a positive aggregate TOI denominator; scorer imposes no minimum-history gate.

## D5/D10/D20 relationships

Across 13,369 historical common rows, D5/D10, D10/D20, and D5/D20 Pearson correlations are 0.862, 0.905, and 0.781; prospective values (n=1,107) are 0.864, 0.918, and 0.776. Mean absolute D5-D10 rate gap is about 1.13 SOG/60; D10-D20 is 0.82 historical and 0.79 prospective. D10-D20 differences persisted across adjacent prospective player captures (n=504, r=0.854); D5-D10 persistence was lower (r=0.704). See `d5_d10_d20_comparison.csv` and `player_rate_persistence.csv`.

## Fixed-window and D10 fallback ablations

On exact common rows, fixed D20 versus D10 reduced historical MAE from 1.035 to 1.016 (n=13,347) and prospective MAE from 1.039 to 1.030 (n=1,107). D5 increased those MAEs to 1.073 and 1.078. D20 also modestly improved threshold proper scores; D5 generally worsened them. These are fixed-window research comparisons, not production changes. See `rate_window_comparison.csv`, `rate_source_ablations.csv`, and `d10_ablation.csv`.

## Context contribution

Production scoring reads only D10/D20/D5 rates plus the independent TOI hierarchy. Attempts, season SOG rate, team/opponent shot context, and PP role are generated upstream but do not enter production lambda. Context-group descriptions are in `context_contribution_metrics.csv`; they are not causal contribution estimates.

## Rate tails and next weakness

Selected-rate tails show consistent regression-to-the-mean error: historical top decile n=1,337 has mean residual -0.54 SOG (top 5% n=669: -0.68); prospective top decile n=111 has -0.56 (top 5% n=56: -0.65). The bottom decile underpredicts by +0.36 historical and +0.25 prospective. Top-decile MAE is 1.42 historical and 1.52 prospective. This is the strongest independent candidate weakness, separate from the already-shadowed D10/D20 divergence. It supports a bounded correction study after more prospective accumulation; no correction is designed or applied here. See `rate_tail_metrics.csv`.

The prior exposure study's actual-TOI oracle reduced prospective MAE by about 0.030 SOG; fixed D20's current-sample rate-side difference is about 0.010 MAE. This descriptive scale comparison suggests tail calibration is the more promising next research direction, without causal attribution.

## Scorecards

See `feature_contribution_scorecard.csv` and `weakness_scorecard.csv`. D10/D20 divergence is the previously identified mechanism handled by the existing shadow, not a new weakness.

## Backlog

See `research_backlog_context.md`.
