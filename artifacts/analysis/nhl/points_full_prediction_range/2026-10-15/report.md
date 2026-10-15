# NHL Points full prediction-range map

## Scope

Corrected selected-side HGB Points 0.5 settled rows, Oct. 10–14, 2026. N=714; wins=416, losses=298. Oct. 15 is kept in `oct15_outcome_free.csv` (213 pending rows, with no outcome); unresolved rows are excluded.

## Full-range interpretation

Across all settled rows HGB expected 70.1% wins versus 58.3% realized, a 84.7 excess losses / 11.9% excess-loss rate. HGB Brier/log loss are 0.252/0.701; market selected-side values are 0.227/0.648. HGB-minus-market Brier and log loss are +0.025/+0.053, favoring market overall.

Negative Edge (<0): n=129, 96–33, realized 74.4% vs expected 68.6%, excess losses -7.6 (-5.9%). Positive Edge (>=0): n=585, 320–265, realized 54.7% vs expected 70.5%, excess losses +92.2 (+15.8%).

Near-zero regions are in `near_zero_regions.csv`; the narrow 0 to +1% range is noisy and under-supported (n=18). Fixed Edge and EV tables are exhaustive, with each partition reconciling to 714. Detailed rows and supporting tables are linked below.

## Methods and outputs

Calibration gap = mean selected-side HGB probability minus realized win rate. Excess losses = observed losses minus sum(1 − selected-side probability). Negative HGB-minus-market Brier/log loss favors HGB. Return tables assume one unit risked per row at that row's retained price; this is retrospective descriptive arithmetic, not executable ROI or a current quote record. Rankings and behavior classifications apply a descriptive support floor of 20 and do not define operational rules.

The package includes fixed Edge/EV bands, cumulative negative and positive Edge regions, near-zero regions, model selected-side probability, the established full HGB P(Over) bins, fixed no-vig selected-side market-probability bands, retained price bands, same/opposite agreement, Under/Over splits, full fixed interaction matrices, date stability, discovery/prospective comparison, rankings, descriptive return, chart-ready CSVs, and static charts.

- `edge_band_full_metrics.csv` and `ev_band_full_metrics.csv`
- `negative_edge_regions.csv`, `positive_edge_regions.csv`, `near_zero_regions.csv`
- `model_confidence_bands.csv`, `hgb_over_probability_bands.csv`, `market_confidence_bands.csv`, `price_bands.csv`
- `edge_price_matrix.csv`, `edge_model_confidence_matrix.csv`, `edge_market_agreement_matrix.csv`
- `under_edge_bands.csv`, `over_edge_bands.csv`, `date_stability.csv`, `discovery_vs_prospective.csv`
- `favorable_region_rankings.csv`, `failure_region_rankings.csv`, `descriptive_return_by_edge.csv`, `descriptive_return_by_price.csv`
- `edge_performance.png`, `model_confidence.png`

Funnel V1 reconciliation is retained in `summary.json` (658 inputs, 0 mismatches; unchanged). No filter, funnel, production, model, UI, DB, or provider change was made. Provider calls: 0; paid credits: 0; production DB mutations: 0.
