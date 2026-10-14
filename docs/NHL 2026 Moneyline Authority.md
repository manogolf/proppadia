# NHL 2026 Moneyline authority

Effective prospectively from the cutover recorded in
`backend/nhl/cross_market_shadow/moneyline_authority_v1.json`:

- **Production/champion:** `NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3`
- **Shadow/challenger:** `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2`

The decision follows the 89 paired settled games in
`artifacts/analysis/nhl/moneyline_prospective_performance/2026-10-13/`.
V3 led the comparison on the retained accuracy, Brier, log-loss, AUC, and
disagreement-game measures. This is a prospective authority decision, not a
claim of permanent superiority or durable edge. The fitted models, features,
probability calculations, calibration, and price-source conventions are
unchanged. Historical rows and their original roles remain unchanged.

`./bin/python -m backend.nhl.cli daily --with-odds` requests full-game `h2h`
with its existing player-prop markets, retains the single parent odds
observation, and passes those retained prices into the existing cross-market
capture. The daily receipt records the authority and links the production and
shadow artifacts. V3 capture failure blocks the Moneyline lane; V2 is never
used as a production fallback.

The immutable outputs are `moneyline_production_predictions.csv` for V3 and
`moneyline_schedule_logit_v2_shadow_predictions.csv` for V2. The original
`v2_immutable_predictions.csv` and V3 model-identity artifact remain available
for identity verification and paired grading. Postgame grading retains both
streams and their paired comparison; the 8Rain Moneyline rows and prospective
production summaries use V3. Pre-cutover captures continue to use their
historical V2 production role.

Adding `h2h` to the parent request increases the upper-bound event-market
budget from 6 to 7 markets per region: 21 credits per paid event across the
current three regions, with a 336-credit ceiling at the 16-game request cap.
There is still one odds acquisition in the daily path; the Moneyline capture
reuses that immutable observation.
