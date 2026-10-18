# NHL 2026 Moneyline favorites and pricing analysis

**Scope:** retained regular-season predictions dated September 29–October 17, 2026. The frozen all-candidate export is the input population; source ledgers bind model probabilities and pregame quotes. This is descriptive research, not an execution record or betting recommendation.

## Population and model comparison

There are 124 games with both model identities: 120 have settled official outcomes and four October 17 games remain unresolved. Predictive metrics use the 120 settled games. Both models have the same game denominator. The V3 and V2 probabilities were joined from the retained per-slate Moneyline ledgers; V2 agrees with the exported probability to floating-point precision. V3 reproduces the previous 89-game study probabilities to floating-point precision.

| Probability source | Games | Accuracy | Brier | Log loss | Mean predicted home win | Observed home win |
|---|---:|---:|---:|---:|---:|---:|
| Shot-finishing V3 | 120 | 66.7% | 0.2241 | 0.6404 | 55.6% | 55.0% |
| Schedule-logit V2 | 120 | 60.0% | 0.2252 | 0.6398 | 53.1% | 55.0% |
| BetOnline no-vig market | 120 | 65.0% | 0.2262 | 0.6437 | 53.9% | 55.0% |

V3 has the highest accuracy and lowest Brier score; its log loss is slightly above V2. The differences are small on this sample. In the 89-game historical overlap, the old report's model probability metrics reproduce exactly: V3 accuracy 66.3%, Brier 0.2225, log loss 0.6372; V2 57.3%, 0.2288, 0.6467. The full set adds 31 settled games from October 13–16. They improve aggregate V2 accuracy and results, so the old 89-game V2 loss should not be generalized to the full window.

Overall calibration is close in the aggregate: V3 averages 55.6% against 55.0% home wins; V2 53.1%; market 53.9%. The bins are uneven: V3's 40–50% bin is 13/34 home wins (38.2%), its 50–60% bin 20/31 (64.5%), and its 70–80% bin 15/18 (83.3%). These bins and the 15-game post-cutover segment are too small for a drift claim.

The authority changed at `2026-10-14T15:27:11Z`. There are 105 settled predictions before that cutover and 15 after it. The candidate file records historical roles (V3 challenger/V2 reference before cutover; V3 production/V2 shadow afterward) and also preserves the frozen export's current role labels. Post-cutover metrics are descriptive only: V3 is 11–4, accuracy 73.3%, Brier 0.1985, log loss 0.5844; V2 is 10–5, 66.7%, 0.2017, 0.5918. Fifteen games are far too few to establish a performance or calibration change.

## Market favorites and disagreement

Strict same-book, two-sided BetOnline prices are available for all 120 settled games and pass `source update <= quote observation <= prediction < scheduled start`. Two pick'em games are excluded from favorite/underdog splits. BetOnline's market favorite won 77 of 118 games (65.3%). Home favorites were 51/78 (65.4%); away favorites were 26/40 (65.0%).

| BetOnline favorite no-vig probability | Games | Favorite wins | Favorite win rate | V3 accuracy | V2 accuracy |
|---|---:|---:|---:|---:|---:|
| 50–55% | 46 | 28 | 60.9% | 67.4% | 56.5% |
| 55–60% | 33 | 19 | 57.6% | 54.5% | 48.5% |
| 60–65% | 19 | 13 | 68.4% | 68.4% | 63.2% |
| 65–70% | 11 | 9 | 81.8% | 81.8% | 81.8% |
| 70–80% | 9 | 8 | 88.9% | 88.9% | 88.9% |

V3 and V2 chose different teams in 22 settled games; V3 was correct in 15 and V2 in 7. V3 selected the market underdog in 22 priced games, winning 12 (54.5%); the average absolute model-to-market probability gap in those games was 8.7 percentage points. V3's accuracy by absolute probability gap was 73.3% for 0–2.5pp (n=30), 64.0% for 2.5–5pp (n=25), 67.5% for 5–10pp (n=40), and 60.0% above 10pp (n=25). These broad bins do not show a monotonic advantage as disagreement grows.

By price side, V3 picked the market favorite 96 times (67 correct; 69.8%) and the underdog 22 times (12 correct; 54.5%). V2 picked the favorite 94 times (62 correct; 66.0%) and the underdog 24 times (9 correct; 37.5%). The underdog subgroup is small and exploratory.

## One-unit hypothetical price results

Each wager risks one unit at the quoted American price, with winnings calculated from decimal odds. Each game contributes at most one wager per model and sportsbook. BetOnline is the predeclared primary book; no best-price book selection is used.

| BetOnline policy | Games | Wins–losses | Net units | ROI |
|---|---:|---:|---:|---:|
| V3 probability-selected side | 120 | 80–40 | +16.81 | +14.0% |
| V2 probability-selected side | 120 | 72–48 | +1.66 | +1.4% |
| Market favorite | 118 | 77–41 | +9.92 | +8.4% |
| Market underdog | 118 | 41–77 | −24.14 | −20.5% |

The original 89-game BetOnline study reported V3 +13.3% ROI, V2 −4.3%, and market favorites +1.5%. Replaying the same 89 games from the export-referenced source ledgers gives V3 +13.33% and V2 −4.29%, essentially the same result; the small V3 difference is about 0.01 percentage point. Full-window ROI is V3 +14.0%, V2 +1.4%. The 31 added settled games materially change V2's combined result. These are retrospective one-unit simulations, not executed wagers, and do not include limits, stake availability, fees, or slippage.

Alternative books are evaluated separately on their own retained prices, never pooled as independent games. On the fixed 120-game coverage set, V3 ROI was +13.1% at DraftKings, +12.8% at FanDuel, +12.9% at BetMGM, +13.0% at Bovada, and +13.3% at William Hill US. V2 was +0.6%, +0.5%, +0.5%, +0.5%, and +0.8%, respectively. Coverage varies for other books: CourtSide has 112 settled games, Rebet 99, BetOpenly 58, and the listed exchange-style sources 64–68. These gaps can bias cross-book comparisons; no alternate book is selected because it performed best.

## Threshold sensitivity

The following fixed threshold grid is retrospective and overlapping; it is not a selected operating rule.

| V3 BetOnline filter | Games | Net units | ROI |
|---|---:|---:|---:|
| Edge ≥ 0pp | 83 | +13.62 | +16.4% |
| Edge ≥ 2.5pp | 70 | +9.95 | +14.2% |
| Edge ≥ 5pp | 55 | +7.61 | +13.8% |
| Edge ≥ 10pp | 24 | +1.60 | +6.7% |
| EV ≥ 0% | 75 | +10.14 | +13.5% |
| EV ≥ 2.5% | 65 | +12.06 | +18.5% |
| EV ≥ 5% | 56 | +8.46 | +15.1% |
| American price ≥ −200 | 99 | +12.49 | +12.6% |
| American price ≥ −150 | 67 | +11.19 | +16.7% |

V2 threshold ROI is much less stable: edge ≥5pp gives 51 games and +0.4%, while edge ≥10pp gives 27 and +12.3%. That jump is a small-sample warning against choosing thresholds from this same outcome set.

## Evidence limits and next analysis

- The full window adds observations but remains a short early-season sample. There is no statistical evidence here that V3's subgroup ROI is durable or that calibration has drifted.
- The post-cutover 15-game segment is too small to separate a role change from ordinary game variance. Continue prospective paired V3/V2 grading using each immutable model identity and the same predeclared BetOnline price policy.
- The original frozen export had V3 probability fields blank on 105/124 rows, and `selected_side` matched the retained-ledger probability side on only 60/124. The corrected full-window export now binds all 248 V3/V2 rows to immutable model prediction sources, supplies complete home/away probabilities and consistent selections, and retains both quote sides. The 120-game probability and BetOnline results in this report did not change: the prior analysis had already recovered those same retained-ledger probabilities and source-bound prices. No metric was inferred from the faulty candidate fields.
- Follow-up priorities: (1) continue the same paired, chronological, BetOnline-only evaluation until at least a full season or materially larger settled sample; (2) evaluate market favorite-price bands and V3 market-underdog picks prospectively with bands frozen before outcomes; (3) audit quote coverage and price timestamp alignment across books, especially sparsely covered books, without selecting an alternate book after observing ROI.

## Files and reproducibility

- Unfiltered one-row-per-model/game candidates: [`moneyline_candidates_unfiltered.csv`](../../artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17/moneyline_candidates_unfiltered.csv)
- Unfiltered model-side sportsbook quotes, joined by `candidate_key`: [`moneyline_model_side_quotes_unfiltered.csv`](../../artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17/moneyline_model_side_quotes_unfiltered.csv)
- Full metrics, calibration bins, price-band tables, threshold grids, lineage hashes, and checks: [`analysis_summary.json`](../../artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17/analysis_summary.json)
- Output checksums and row counts: [`OUTPUT_MANIFEST.json`](../../artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17/OUTPUT_MANIFEST.json)
- Rebuild command: `./bin/python backend/nhl/scripts/analyze_nhl_moneyline_favorites_pricing_2026.py`
- Frozen input manifest SHA-256: `10a0e68ac04903f13b6769650690e364129b50fac9c01cc17a2bd5c840d8f194`.

The candidate file has 248 rows (124 games × two model identities); the quote sidecar has 5,358 qualified rows and keeps books separate. The frozen export had 5,196 Moneyline quote rows. The derived sidecar rebinds the exact retained two-sided ledger price to each probability-derived model pick, recovering the price for the correct side when the export's `selected_side` does not match that pick. Its `(candidate_key, sportsbook)` key is unique. Candidate rows include the corrected probability-derived pick, outcome status, historical/export role labels, source and ledger paths, and hashes. Quote rows include sportsbook, American/decimal price, no-vig implied probability, model probability for the quoted side, edge, one-unit EV, quote/update timestamps, qualification status, and source ledger hash.

The corrected raw all-candidate package separately contains 248 Moneyline candidate rows and 10,392 chronology-qualified Moneyline quote rows: 5,196 HOME and 5,196 AWAY. `selected_side` continues to record each model's choice; `quote_side` identifies the outcome priced in each row. The export manifest records 248/248 source-backed probabilities, side agreement, and prediction source path/hash validation.
