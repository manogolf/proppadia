# NHL Points corrected EV/Edge range surface

## Scope and contract

Read-only range presentation from the corrected selected-side artifacts. Source is `../canonical_rows.csv`; no predictions are rescored. Settled 0.5 population is 714 rows across Oct. 10–14, 416–298. Unresolved identities and Oct. 15 pending rows are excluded from metrics. For selected side S, probability, price, Edge and EV use that same side. The exact Edge and EV formula checks passed; fixed band boundaries were retained.

## Complete Edge surface

Calibration gap is expected minus realized win rate. A positive excess-loss rate means observed losses exceeded expected losses. HGB−market Brier below zero favors HGB descriptively; above zero favors the market.

| Edge band | N | W | L | Realized | Expected | Calibration gap | Expected losses | Observed losses | Excess losses | Excess-loss rate | HGB Brier | HGB log loss | Market Brier | Market log loss | HGB−market Brier | Mean price | Mean EV | Under | Over | Same | Opposite | Dates |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| < -15% | 12 | 9 | 3 | 75.0% | 53.5% | -21.5% | 5.584 | 3 | -2.584 | -21.5% | 0.239 | 0.670 | 0.206 | 0.606 | 0.033 | -284.583 | -0.267 | 1 | 11 | 12 | 0 | 2026-10-10|2026-10-12|2026-10-13|2026-10-14 |
| -15 to -10 | 21 | 16 | 5 | 76.2% | 63.2% | -13.0% | 7.731 | 5 | -2.731 | -13.0% | 0.196 | 0.578 | 0.177 | 0.532 | 0.019 | -390.333 | -0.161 | 10 | 11 | 21 | 0 | 2026-10-10|2026-10-12|2026-10-13|2026-10-14 |
| -10 to -5 | 29 | 19 | 10 | 65.5% | 70.1% | 4.5% | 8.684 | 10 | 1.316 | 4.5% | 0.244 | 0.687 | 0.247 | 0.697 | -0.003 | -413.948 | -0.091 | 26 | 3 | 29 | 0 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13 |
| -5 to -3 | 20 | 16 | 4 | 80.0% | 69.8% | -10.2% | 6.036 | 4 | -2.036 | -10.2% | 0.160 | 0.498 | 0.161 | 0.500 | -0.001 | -321.550 | -0.054 | 20 | 0 | 20 | 0 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| -3 to -1 | 28 | 21 | 7 | 75.0% | 71.9% | -3.1% | 7.872 | 7 | -0.872 | -3.1% | 0.190 | 0.567 | 0.190 | 0.565 | 0.000 | -321.321 | -0.025 | 28 | 0 | 27 | 1 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| -1 to 0 | 19 | 15 | 4 | 78.9% | 75.5% | -3.5% | 4.657 | 4 | -0.657 | -3.5% | 0.157 | 0.488 | 0.163 | 0.504 | -0.005 | -353.158 | -0.006 | 19 | 0 | 19 | 0 | 2026-10-10|2026-10-11|2026-10-13|2026-10-14 |
| 0 to +1 | 18 | 5 | 13 | 27.8% | 68.0% | 40.2% | 5.766 | 13 | 7.234 | 40.2% | 0.346 | 0.896 | 0.311 | 0.817 | 0.035 | -221.472 | 0.007 | 18 | 0 | 17 | 1 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +1 to +3 | 38 | 28 | 10 | 73.7% | 68.9% | -4.8% | 11.815 | 10 | -1.815 | -4.8% | 0.197 | 0.588 | 0.226 | 0.695 | -0.029 | -218.329 | 0.032 | 38 | 0 | 35 | 3 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +3 to +5 | 46 | 31 | 15 | 67.4% | 72.3% | 4.9% | 12.742 | 15 | 2.258 | 4.9% | 0.197 | 0.576 | 0.198 | 0.584 | -0.001 | -222.565 | 0.060 | 46 | 0 | 44 | 2 | 2026-10-10|2026-10-11|2026-10-13|2026-10-14 |
| +5 to +10 | 138 | 78 | 60 | 56.5% | 71.2% | 14.6% | 39.797 | 60 | 20.203 | 14.6% | 0.251 | 0.697 | 0.232 | 0.656 | 0.019 | -173.739 | 0.121 | 138 | 0 | 117 | 21 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +10 to +15 | 145 | 85 | 60 | 58.6% | 71.0% | 12.4% | 42.006 | 60 | 17.994 | 12.4% | 0.264 | 0.732 | 0.246 | 0.685 | 0.017 | -118.424 | 0.214 | 145 | 0 | 106 | 39 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +15 to +20 | 104 | 56 | 48 | 53.8% | 69.5% | 15.7% | 31.701 | 48 | 16.299 | 15.7% | 0.253 | 0.703 | 0.230 | 0.653 | 0.023 | -18.659 | 0.344 | 104 | 0 | 47 | 57 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +20%+ | 96 | 37 | 59 | 38.5% | 69.8% | 31.3% | 28.959 | 59 | 30.041 | 31.3% | 0.336 | 0.886 | 0.230 | 0.653 | 0.106 | 77.062 | 0.564 | 96 | 0 | 13 | 83 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |

Highest excess-loss rate: `0 to +1` (40.2%, n=18); lowest: `< -15%` (-21.5%, n=12). HGB has its lowest relative Brier in `+1 to +3` (HGB−market -0.0286); market has its largest Brier advantage in `+20%+` (HGB−market +0.1058).

Chart-ready source: `edge_band_performance_chart.csv`; charts: `edge_expected_vs_realized.png`, `edge_excess_loss_rate.png`, `edge_market_brier_delta.png`.

## Complete EV surface

| EV band | N | W | L | Realized | Expected | Calibration gap | Expected losses | Observed losses | Excess losses | Excess-loss rate | HGB Brier | HGB log loss | Market Brier | Market log loss | Mean Edge | Mean price | Under | Over | Same | Opposite | Dates |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| < -30% | 4 | 3 | 1 | 75.0% | 52.9% | -22.1% | 1.885 | 1 | -0.885 | -22.1% | 0.236 | 0.664 | 0.193 | 0.575 | -0.235 | -330.625 | 0 | 4 | 4 | 0 | 2026-10-10|2026-10-13 |
| -30 to -20 | 11 | 9 | 2 | 81.8% | 53.6% | -28.2% | 5.106 | 2 | -3.106 | -28.2% | 0.235 | 0.663 | 0.191 | 0.578 | -0.167 | -247.818 | 2 | 9 | 11 | 0 | 2026-10-10|2026-10-12|2026-10-13|2026-10-14 |
| -20 to -10 | 27 | 17 | 10 | 63.0% | 64.9% | 1.9% | 9.482 | 10 | 0.518 | 1.9% | 0.233 | 0.657 | 0.233 | 0.656 | -0.105 | -397.259 | 16 | 11 | 27 | 0 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| -10 to -5 | 34 | 26 | 8 | 76.5% | 70.9% | -5.6% | 9.898 | 8 | -1.898 | -5.6% | 0.194 | 0.577 | 0.192 | 0.574 | -0.054 | -387.294 | 33 | 1 | 34 | 0 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| -5 to 0 | 53 | 41 | 12 | 77.4% | 73.2% | -4.1% | 14.192 | 12 | -2.192 | -4.1% | 0.173 | 0.526 | 0.175 | 0.532 | -0.015 | -336.075 | 53 | 0 | 52 | 1 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| 0 to +5 | 60 | 36 | 24 | 60.0% | 69.7% | 9.7% | 18.176 | 24 | 5.824 | 9.7% | 0.243 | 0.683 | 0.250 | 0.726 | 0.017 | -230.425 | 60 | 0 | 57 | 3 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +5 to +10 | 83 | 55 | 28 | 66.3% | 72.8% | 6.5% | 22.613 | 28 | 5.387 | 6.5% | 0.211 | 0.609 | 0.209 | 0.608 | 0.050 | -216.783 | 83 | 0 | 79 | 4 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +10 to +20 | 163 | 87 | 76 | 53.4% | 72.1% | 18.7% | 45.473 | 76 | 30.527 | 18.7% | 0.275 | 0.752 | 0.242 | 0.677 | 0.094 | -167.739 | 163 | 0 | 142 | 21 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +20 to +30 | 105 | 69 | 36 | 65.7% | 72.4% | 6.7% | 28.996 | 36 | 7.004 | 6.7% | 0.235 | 0.670 | 0.239 | 0.668 | 0.142 | -116.267 | 105 | 0 | 77 | 28 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +30 to +40 | 65 | 36 | 29 | 55.4% | 68.3% | 12.9% | 20.611 | 29 | 8.389 | 12.9% | 0.256 | 0.709 | 0.244 | 0.682 | 0.178 | 10.585 | 65 | 0 | 19 | 46 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |
| +40%+ | 109 | 37 | 72 | 33.9% | 66.1% | 32.2% | 36.918 | 72 | 35.082 | 32.2% | 0.330 | 0.870 | 0.223 | 0.637 | 0.234 | 114.394 | 109 | 0 | 5 | 104 | 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14 |

Chart-ready source: `ev_band_performance_chart.csv`; chart: `ev_expected_vs_realized.png`.

## Side and market-agreement breakdown

Under and Over band tables are in `edge_by_under.csv` and `edge_by_over.csv`. Every fixed band is retained, including zero-count Over bands. Same/opposite W-L, N, and separate HGB/market Brier values are in `edge_by_market_agreement.csv`. Compact relationship table: `edge_band_relationship_summary.csv`.

### HGB Under

| Band | N | W-L | Realized | Expected | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|
| < -15% | 1 | 1–0 | 100.0% | 53.6% | -46.4% | 0.215 | 0.123 |
| -15 to -10 | 10 | 9–1 | 90.0% | 72.1% | -17.9% | 0.128 | 0.102 |
| -10 to -5 | 26 | 16–10 | 61.5% | 71.4% | 9.9% | 0.252 | 0.258 |
| -5 to -3 | 20 | 16–4 | 80.0% | 69.8% | -10.2% | 0.160 | 0.161 |
| -3 to -1 | 28 | 21–7 | 75.0% | 71.9% | -3.1% | 0.190 | 0.190 |
| -1 to 0 | 19 | 15–4 | 78.9% | 75.5% | -3.5% | 0.157 | 0.163 |
| 0 to +1 | 18 | 5–13 | 27.8% | 68.0% | 40.2% | 0.346 | 0.311 |
| +1 to +3 | 38 | 28–10 | 73.7% | 68.9% | -4.8% | 0.197 | 0.226 |
| +3 to +5 | 46 | 31–15 | 67.4% | 72.3% | 4.9% | 0.197 | 0.198 |
| +5 to +10 | 138 | 78–60 | 56.5% | 71.2% | 14.6% | 0.251 | 0.232 |
| +10 to +15 | 145 | 85–60 | 58.6% | 71.0% | 12.4% | 0.264 | 0.246 |
| +15 to +20 | 104 | 56–48 | 53.8% | 69.5% | 15.7% | 0.253 | 0.230 |
| +20%+ | 96 | 37–59 | 38.5% | 69.8% | 31.3% | 0.336 | 0.230 |

### HGB Over

| Band | N | W-L | Realized | Expected | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|
| < -15% | 11 | 8–3 | 72.7% | 53.5% | -19.3% | 0.241 | 0.213 |
| -15 to -10 | 11 | 7–4 | 63.6% | 55.1% | -8.5% | 0.258 | 0.245 |
| -10 to -5 | 3 | 3–0 | 100.0% | 58.3% | -41.7% | 0.174 | 0.150 |
| -5 to -3 | 0 | 0–0 | — | — | — | — | — |
| -3 to -1 | 0 | 0–0 | — | — | — | — | — |
| -1 to 0 | 0 | 0–0 | — | — | — | — | — |
| 0 to +1 | 0 | 0–0 | — | — | — | — | — |
| +1 to +3 | 0 | 0–0 | — | — | — | — | — |
| +3 to +5 | 0 | 0–0 | — | — | — | — | — |
| +5 to +10 | 0 | 0–0 | — | — | — | — | — |
| +10 to +15 | 0 | 0–0 | — | — | — | — | — |
| +15 to +20 | 0 | 0–0 | — | — | — | — | — |
| +20%+ | 0 | 0–0 | — | — | — | — | — |

### Same-side vs opposite-side

| Band | Same N | Same W-L | Same HGB Brier | Same market Brier | Opp N | Opp W-L | Opp HGB Brier | Opp market Brier |
|---|---|---|---|---|---|---|---|---|
| < -15% | 12 | 9–3 | 0.239 | 0.206 | 0 | 0–0 | — | — |
| -15 to -10 | 21 | 16–5 | 0.196 | 0.177 | 0 | 0–0 | — | — |
| -10 to -5 | 29 | 19–10 | 0.244 | 0.247 | 0 | 0–0 | — | — |
| -5 to -3 | 20 | 16–4 | 0.160 | 0.161 | 0 | 0–0 | — | — |
| -3 to -1 | 27 | 21–6 | 0.187 | 0.187 | 1 | 0–1 | 0.275 | 0.250 |
| -1 to 0 | 19 | 15–4 | 0.157 | 0.163 | 0 | 0–0 | — | — |
| 0 to +1 | 17 | 5–12 | 0.350 | 0.314 | 1 | 0–1 | 0.282 | 0.247 |
| +1 to +3 | 35 | 26–9 | 0.194 | 0.223 | 3 | 2–1 | 0.236 | 0.257 |
| +3 to +5 | 44 | 31–13 | 0.193 | 0.197 | 2 | 0–2 | 0.290 | 0.214 |
| +5 to +10 | 117 | 71–46 | 0.247 | 0.232 | 21 | 7–14 | 0.271 | 0.234 |
| +10 to +15 | 106 | 61–45 | 0.276 | 0.243 | 39 | 24–15 | 0.229 | 0.255 |
| +15 to +20 | 47 | 32–15 | 0.226 | 0.227 | 57 | 24–33 | 0.276 | 0.233 |
| +20%+ | 13 | 8–5 | 0.272 | 0.235 | 83 | 29–54 | 0.346 | 0.230 |

Same-side and opposite-side totals reconcile to 507 and 207; Under and Over total 689 and 25. No interpretation is assigned to sparse Over cells.

## Date stability

Each fixed Edge band by each settled date appears in `edge_band_date_metrics.csv`, including zero-count cells. Overall date counts: 2026-10-10: n=198, 122–76, realized 61.6%, expected 69.5%, excess +15.6; 2026-10-11: n=73, 37–36, realized 50.7%, expected 71.0%, excess +14.8; 2026-10-12: n=36, 21–15, realized 58.3%, expected 68.0%, excess +3.5; 2026-10-13: n=351, 200–151, realized 57.0%, expected 70.3%, excess +46.7; 2026-10-14: n=56, 36–20, realized 64.3%, expected 71.6%, excess +4.1. Oct. 13 contributes 351 of the 714 settled rows; inspect the date-level table alongside the aggregate before comparing ranges.

## Cumulative, near-zero, and sort views

Cumulative positive Edge rows (descriptive only):

| Region | N | W-L | Expected | Realized | Excess | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|---|
| Edge >= 0 | 585 | 320–265 | 70.5% | 54.7% | 92.213 | 15.8% | 0.264 | 0.234 |
| Edge >= +1% | 567 | 315–252 | 70.5% | 55.6% | 84.979 | 15.0% | 0.261 | 0.232 |
| Edge >= +3% | 529 | 287–242 | 70.7% | 54.3% | 86.794 | 16.4% | 0.266 | 0.232 |
| Edge >= +5% | 483 | 256–227 | 70.5% | 53.0% | 84.536 | 17.5% | 0.272 | 0.236 |
| Edge >= +10% | 345 | 178–167 | 70.2% | 51.6% | 64.334 | 18.6% | 0.281 | 0.237 |
| Edge >= +15% | 200 | 93–107 | 69.7% | 46.5% | 46.340 | 23.2% | 0.293 | 0.230 |
| Edge >= +20% | 96 | 37–59 | 69.8% | 38.5% | 30.041 | 31.3% | 0.336 | 0.230 |

Negative and near-zero regions:

| Region | N | W-L | Expected | Realized | Excess | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|---|
| Edge < 0 | 129 | 96–33 | 68.6% | 74.4% | -7.563 | -5.9% | 0.198 | 0.193 |
| Edge -5% to 0 | 67 | 52–15 | 72.3% | 77.6% | -3.564 | -5.3% | 0.172 | 0.173 |
| Edge -3% to 0 | 47 | 36–11 | 73.3% | 76.6% | -1.529 | -3.3% | 0.177 | 0.179 |
| Edge -1% to +1% | 37 | 20–17 | 71.8% | 54.1% | 6.577 | 17.8% | 0.249 | 0.235 |
| Edge 0 to +3% | 56 | 33–23 | 68.6% | 58.9% | 5.419 | 9.7% | 0.245 | 0.253 |
| Edge -3% to +3% | 103 | 69–34 | 70.8% | 67.0% | 3.891 | 3.8% | 0.214 | 0.219 |

Edge sort prefixes:

| Fraction | N | W-L | Realized | Expected | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|
| 0.100 | 72 | 29–43 | 40.3% | 70.4% | 30.2% | 0.340 | 0.235 |
| 0.200 | 143 | 61–82 | 42.7% | 69.9% | 27.2% | 0.312 | 0.230 |
| 0.250 | 179 | 81–98 | 45.3% | 69.7% | 24.5% | 0.297 | 0.228 |
| 0.500 | 357 | 184–173 | 51.5% | 70.4% | 18.8% | 0.280 | 0.237 |
| bottom_50% | 357 | 232–125 | 65.0% | 69.9% | 4.9% | 0.223 | 0.217 |

EV sort prefixes:

| Fraction | N | W-L | Realized | Expected | Excess rate | HGB Brier | Market Brier |
|---|---|---|---|---|---|---|---|
| 0.100 | 72 | 24–48 | 33.3% | 66.4% | 33.0% | 0.338 | 0.221 |
| 0.200 | 143 | 53–90 | 37.1% | 66.5% | 29.4% | 0.319 | 0.227 |
| 0.250 | 179 | 74–105 | 41.3% | 67.1% | 25.7% | 0.305 | 0.231 |
| 0.500 | 357 | 183–174 | 51.3% | 69.5% | 18.2% | 0.279 | 0.237 |
| bottom_50% | 357 | 233–124 | 65.3% | 70.7% | 5.5% | 0.225 | 0.217 |

Top-set overlap is in `ranking_overlap.csv`. Both preserve top 10%, 20%, 25%, and 50%, plus bottom 50%. No threshold is proposed.

## Direct descriptive answers

1. The complete W-L progression is shown in the Edge table from `< -15%` through `+20%+`.
2. Expected/realized separation appears per band in that table and in the unsmoothed chart; no band boundary is selected.
3. Highest and lowest excess-loss rates are visible with N in both range tables.
4. HGB beats market Brier where HGB−market Brier is negative; the converse is positive. This is descriptive, not causal.
5. Accuracy is not monotonic across the ordered Edge ranges. The Under-only pattern closely follows the full sample because positive Edge bands are all Under; Over has only 25 rows, all in negative bands.
6. In the +20%+ band, 83/96 rows are opposite-side (29–54); same-side rows are 8–5. Directional disagreement therefore accounts for most rows and losses in that highest Edge band. Date slices expose concentration, including Oct. 13.
7. Edge and EV descending prefixes show the outcomes they prioritize; `ranking_overlap.csv` reports top-set overlap.
8. A descriptive transition is visible from +5% Edge upward, where the broad bands remain below expected accuracy and the +20%+ band widens the gap. The 0 to +1% bin is also poor but has only 18 rows. These patterns may be inspected later; neither is a threshold or filter decision.

This package surfaces corrected evidence only. It creates no model thesis, filter, Funnel stage, UI change, production change, or SOG comparison. Provider calls: 0; paid credits: 0; production DB mutations: 0.
