# NHL Points provider-level near-zero price neighborhood

**Manual-search classification:** `NO_MEANINGFUL_PROVIDER_LEVEL_SAMPLE`  
**Retained-evidence availability:** `RARE_NEIGHBORHOOD`  
**Provider-level large positive edge:** `MODEL_OVERCONFIDENCE_REGION`

## Evidence and sample

Exact provider quotes were recoverable for the bound observations on Oct. 10–14. The sample has 19283 retained provider quote records across 18 books; one duplicate exact quote key is flagged, and 1060 records outside the bound pregame window are excluded. There are 14893 eligible quote rows on Oct. 10–14 and 4390 on Oct. 15. The Oct. 10–14 outcome-bearing denominator is 1174 settled prediction rows; quote rows are not independent outcomes. Oct. 15 has 4388 eligible quote rows in pending inventory only and contributes no performance data.

The exact primary combination is sparse: production edge `[-3%, 0%)` with a selected-side provider price of +100 or better occurs 0 times in finalized evidence. Provider quote edge `[-3%, 0%)` plus +100 or better occurs 0 times; `[-1%, 0%)` plus +100 or better occurs 0 times. Provider edge `[0%, +1%)` plus +100 occurs 0 times, and `[0%, +3%)` plus +100 occurs 1 times. The retained evidence therefore does not provide a meaningful settled sample for the manual thesis.

For production edge below zero, `negative_edge_plus_money_availability.csv` gives prediction-unique counts at +100, +110, and +125 for all negative (132 predictions), `[-3%,0%)` (48), and `[-1%,0%)` (20) groups. Each of those counts is zero at all three price thresholds. `price_dispersion.csv` reports each prediction’s worst, median, and best bound selected-side quote and implied-probability spread. Among 870 finalized predictions with a selected-side quote, the attachment median is at least 10 American price points worse than an observed provider quote in 570 rows and at least 2 implied-probability points worse in 472 rows.

## Model and market results

Production edge is retained unchanged as selected-side HGB probability minus raw implied probability from the separately aggregated median price. It reproduces 918 rows with maximum absolute difference 2.22e-16. Provider quote edge is separate: selected-side HGB probability minus raw implied probability from that exact provider’s American price. Same-provider no-vig values exist only when both sides are present for the exact player/game/line/provider.

The aggregated-median negative near-zero subset has 47 settled rows, HGB 36-11, Brier 0.1766 vs market 0.1787, and log loss 0.5353 vs 0.5405. This is the historical near-zero baseline, not the requested plus-money population.

Exact BetOnline coverage is 572 settled predictions overall and 18 in negative near-zero production edge. In that BetOnline subset, HGB Brier advantage is -0.0021, descriptive net return is -0.022 units per unit risk, with no provider substitution. Best observed selected-side price covers 851 settled predictions; its negative near-zero production-edge subset covers 46 and has 0.042 descriptive units per unit risk. Best observed does not mean guaranteed accessible or executable.

The one settled `[0%,+3%)` plus-money provider-edge case went 0–1; HGB Brier was 0.230 vs same-provider no-vig 0.216, and the market was closer on that row. The provider-edge `[-3%,0%)` plus-money and `[-1%,0%)` plus-money groups had no settled rows. Near-even favorite comparisons are also thin: `[-3%,0%)` with -105 through -101 has 3 settled rows, while `[0%,+3%)` has 5; broader -120 through -106 groups have 3 and 11 settled rows respectively. These small samples do not establish HGB/market complementarity. Date separation and same-side/opposite-side results are in their dedicated tables. The prediction-level provider-edge control at +10% or above has HGB excess losses 77.0, Brier 0.324 vs paired same-provider no-vig 0.237, supporting `MODEL_OVERCONFIDENCE_REGION`. This is descriptive evidence of overconfidence; it does not establish market crowding or causality.

## Oct. 15 and decision

Oct. 15 has 55 prediction identities with a selected-side +100-or-better quote, 1 production-negative-near-zero plus-money cases, 0 provider-negative-near-zero plus-money cases, 165 exact BetOnline selected-side quotes, and 269 best-observed instances. They remain `PENDING_OUTCOME`.

Discovery (Oct. 10–13) had 660 settled aggregated-median comparison rows; Oct. 14 prospective context had 56 settled rows, HGB 36–20, with median-price Brier 0.240 vs market 0.234 and best-observed Brier 0.240 vs its provider-price benchmark 0.231. No plus-money near-zero cases on Oct. 14 were settled. The actual manual near-zero/plus-money thesis received a fair quote-recoverability and availability test, but not a meaningful performance test: exact provider quotes, same-run predictions, and official outcomes were available, while the relevant intersection was too rare. No frozen prospective provider-price experiment is justified by this sample. Funnel V1 is unchanged; no rule was created. Provider calls: 0; paid credits: 0; production DB mutations: 0.

See `provider_coverage.csv`, `providers_observed.csv`, `provider_inventory.csv`, `provider_quotes.csv`, `prediction_neighborhood_rows.csv`, `neighborhood_performance.csv`, `provider_edge_band_prediction_metrics.csv`, `policy_performance.csv`, `date_neighborhood_performance.csv`, and `oct15_pending_summary.json` for the full evidence.
