# NHL Points displayed EV / Edge reliability surface — corrected

## Correction and source trace

The package from commit `90bf6a3b49ce28057f57d217444cd480951e859a` is superseded. Its root cause was unconditionally reading `p_over` and `price_over` while evaluating model-selected outcomes. The prior claim that positive Edge occurred in only two rows is invalid: it counted the Over-oriented tail, not the selected-side Edge population.

The retained export contains both sides (`p_over/p_under`, `price_over/price_under`, `fair_over/fair_under`, `edge_over/edge_under`). Funnel V1 selects OVER when model P(over) is at least 0.5 and UNDER otherwise; its selected production Edge is selected model probability minus raw American implied probability at the corresponding side price. The supplied six examples reproduce this side-normalized formula.

Repository trace (`source_trace.json`): the React board fetches `points_with_market.csv`, turns `p_over` into the line probability and puts `p_over_mkt` plus `price_over` in its market map. `bestEdgeCandidate/topPlayableEdgeRows` calculate `p_over - marketProbability`; the adapter labels the row `Over`, and does not switch to Under. The static table is also Over-oriented. Thus the correction fixtures establish the intended acceptance contract, while checked-in UI source does not implement that contract today. This study records that discrepancy and computes the requested corrected selected-side analysis; no UI source was changed.

## Population and reproduction

Historical finalized intersection: 1284 rows (714 settled 0.5 rows; 555 settled 1.5; 13 settled 2.5). At 0.5: 416–298, accuracy 58.3%, mean selected probability 70.1%, realized rate 58.3%; Edge positive/negative/zero: 585/129/0. Brier/log loss: model 0.2518/0.7012; no-vig market 0.2270/0.6477.

All six supplied Cowan, Rielly, Power, Nylander, Matthews and Quinn arithmetic fixtures pass to the displayed 0.1 percentage point. Cowan Under: model probability 66.9967%, market -169, fair -203, Edge +4.2%, EV +6.6%. Historical selected-side reproduction: 1284 rows; maximum Edge error 0; maximum EV error 0; fair-price mismatches 0; orientation mismatches 0.

## Funnel V1 reconciliation

On the exact 658-row raw canonical 0.5 discovery population, corrected Edge < 0 matches frozen Funnel V1 negative-edge classification in 658/658 rows: 120 negative and 538 nonnegative; mismatches 0. Funnel V1 files and code were not modified. This reconciles 658 → 538 exactly.

## Corrected reliability surface

| Selected-side Edge | N | W–L | Accuracy | Excess losses |
|---|---:|---:|---:|---:|
| < -15% | 12 | 9–3 | 75.0% | -2.6 |
| -15 to -10 | 21 | 16–5 | 76.2% | -2.7 |
| -10 to -5 | 34 | 24–10 | 70.6% | +0.8 |
| -5 to -3 | 32 | 28–4 | 87.5% | -3.0 |
| -3 to -1 | 72 | 59–13 | 81.9% | +1.9 |
| -1 to 0 | 53 | 46–7 | 86.8% | +0.3 |
| 0 to +1 | 73 | 57–16 | 78.1% | +7.7 |
| +1 to +3 | 141 | 118–23 | 83.7% | +6.1 |
| +3 to +5 | 117 | 97–20 | 82.9% | +3.5 |
| +5 to +10 | 268 | 186–82 | 69.4% | +33.3 |
| +10 to +15 | 212 | 134–78 | 63.2% | +30.9 |
| +15 to +20 | 136 | 78–58 | 57.4% | +22.9 |
| +20%+ | 111 | 45–66 | 40.5% | +35.5 |

| Selected-side EV | N | W–L | Accuracy | Excess losses |
|---|---:|---:|---:|---:|
| < -30% | 4 | 3–1 | 75.0% | -0.9 |
| -30 to -20 | 11 | 9–2 | 81.8% | -3.1 |
| -20 to -10 | 27 | 17–10 | 63.0% | +0.5 |
| -10 to -5 | 39 | 31–8 | 79.5% | -2.5 |
| -5 to 0 | 143 | 122–21 | 85.3% | +0.6 |
| 0 to +5 | 272 | 227–45 | 83.5% | +16.8 |
| +5 to +10 | 197 | 154–43 | 78.2% | +12.9 |
| +10 to +20 | 265 | 163–102 | 61.5% | +48.8 |
| +20 to +30 | 139 | 92–47 | 66.2% | +14.5 |
| +30 to +40 | 72 | 41–31 | 56.9% | +9.7 |
| +40%+ | 113 | 38–75 | 33.6% | +37.5 |

Edge and EV are strongly related transforms of selected probability and price: overall Pearson/Spearman 0.945/0.991; 0.5 line 0.955/0.989. `EV = Edge / q`, where q is raw implied probability, so price alters ranking. Both surfaces are non-monotonic in realized accuracy, and high positive bands show poor calibration: Edge ≥20% is 96 rows; EV ≥20% is 279 rows. No threshold is supported.

Under: 689 rows, 398–291, accuracy 57.8%; Over: 25 rows, 18–7, accuracy 72.0%. Same-side market/model agreement: 507 rows, 330–177; opposite-side: 207 rows, 86–121. These are descriptive, not promotion evidence.

Price interaction appears in `price_band_metrics.csv`: accuracy declines from 80.5% at odds ≤-200 (n=816) to 17.6% at +176 or longer (n=17), with substantial confounding by selected probability and side. Top 10% Edge sort: 29–43; top 10% EV sort: 24–48. At top 20%, Edge 61–82 and EV 53–90; neither ranking improves outcomes. Top 10% Edge/EV overlap and Jaccard are in `ranking_overlap.csv`.

Oct. 15 is pending: 213 quote-complete rows, 200 Under and 13 Over, no grading. All line/date, band, price, agreement and sort tables remain descriptive. The prior study’s Over-only edge/EV reliability, positive-tail scarcity, edge/EV relationship, and conclusions about where green helps are invalid; the selected-side outcomes and frozen Funnel V1 counts survive, but must be interpreted with the corrected columns. Funnel V1 is unchanged.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. No operational rule or cutoff established.
