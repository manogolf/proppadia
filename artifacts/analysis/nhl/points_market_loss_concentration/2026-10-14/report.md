# NHL Points model × market loss-concentration audit

## Scope and evidence

Strict retained HGB prediction + final outcome + passing market attachment dates: 2026-10-10, 2026-10-11, 2026-10-12, 2026-10-13. Primary 0.5 intersection: **658** rows; all supported lines: **1191** rows.

The audit uses immutable daily `points_predictions.csv` rows, a matching market attachment hash and passing attachment integrity record, plus participating final reconciled outcomes at exact game/player/line keys. Oct. 1–9 Phoenix attachments are inventoried but excluded. Oct. 14 has no final grade yet.

## Primary 0.5 results

| N | Model wins-losses | Model accuracy | Model Brier | Model log loss | Market Brier | Market log loss | Expected losses | Observed losses | Excess losses |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 658 | 380-278 | 0.578 | 0.2528 | 0.7032 | 0.2268 | 0.6481 | 197.5 | 278 | +80.5 |

## Interpretation and study limits

Across four independent slates, the model is materially overconfident at 0.5. The 658-row primary set has 80.5 more losses than summed model expectation, and the attached market consensus has lower Brier and log loss. This is descriptive across a short early-season window, not a validated failure rule. The largest absolute loss count occurs at 0.5 because that line supplies 658 rows; lines 1.5 have 524 market rows and 2.5 only 9, so latter lines are secondary and the 2.5 result is not decision-grade.

By date, 0.5 excess losses are positive on all four dates, though sizes vary. Discovery/validation is not credible with four dates; all avoidance candidates remain untested, excluded rows = 0, wins sacrificed = 0, retained population = 658/658, ROI change unavailable. Ex post high-log-loss ranking is descriptive only and not an avoidable rule.

Retained `price_over`/`price_under` values are attachment-level prices with aggregate source-book lists, not provider-specific quote history. Provider selection and per-provider quote timestamps are absent, so ROI and BetOnline-only ROI are withheld. Consensus probabilities remain suitable for proper-score comparison.

Manual-review recommendation: do not discontinue NHL.com checks yet. The study has not tested manual variables inside a repeatable failure regime because the needed captured context is absent for these HGB rows and there are too few dates. A read-only daily market/sanity summary is justified to replace repeated ad hoc browsing over time; it must not filter picks. SOG/Saves reuse is straightforward after exact binding to their own outcomes; Moneyline/Puck Line need their own settlement and market definitions.

Backlog preserved: first live SOG structural-correction capture; prospective SOG shadow grades; fixed-blend collection; historical official-TOI binding; Puck Line deferred; Tier3 override not currently needed.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production model/authority/filters: unchanged.

Files in `artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14/`.
