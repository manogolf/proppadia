# September 19 NHL preseason catch-up

## Result

`SEPTEMBER_19_PRESEASON_CATCHUP_PRESERVED_WITH_PARTIAL_MARKET_COVERAGE`

The 410-of-410 missing season-2026 5v5 TOI population is the expected opening-day cold start: retained data contained no season-2026 game before September 19 and no completed season-2026 game. No contracted preseason/prior-season SOG TOI fallback was found, so SOG prediction remained fail-closed.

The morning boundary was corrected so this SOG-only prerequisite blocks only SOG. A deterministic fixture now proves that Mainline, Points, Saves, grading, and market-capture readiness remain independent while `SOG_MORNING_PREREQUISITES_READY=false`.

## Preserved observations

At 13:45:50.949 PT the catch-up claimed and made one frozen `us,us2` h2h+spreads request. It completed at 13:45:51.164 PT and charged 4 credits. Create-only raw evidence and separate h2h and spreads receipts were written. The provider returned 33 NHL events dated September 29 through October 10, not today's seven preseason games; therefore today's h2h and puck-line market counts are zero.

The frozen bulk request form for SOG, Points, and Saves was rejected with HTTP 422. Each family has one durable failed claim, no raw payload, and no automatic retry. The successful request confirms 4 credits; the conservative maximum exposure including the rejected attempts is 12 credits.

## Predictions

- Mainline: 7 Moneyline and 7 puck-line shadow predictions. All six opening-season inputs were fit-median imputed, every row is `PRESEASON_REHEARSAL_EXCLUDED`, and no wager was produced.
- SOG: 0 predictions; `BLOCKED_SEASON_2026_5V5_TOI_UNAVAILABLE`.
- Points: 455 player-game inputs, 1,365 frozen predictions, all 455 ladders coherent. Market count 0.
- Saves: 42 goalie-game inputs, 546 conditional-on-start frozen predictions, 0 market-selected starters. Market count 0.

The seven canonical games were DAL at STL, VGK at LAK, WPG at EDM, CHI at MIN, VAN at SEA, MTL at TOR, and TOR at MTL. First puck was 16:00 PT. Mainline evidence was captured 2:14:08.835 before first puck; the attempted four-family sequence ended 2:13:12.202 before first puck.

## Boundary

A later final-pregame Mainline capture remains useful only if the provider begins listing today's events. The three player-prop families must not be retried automatically under the current frozen bulk-endpoint binding. No model, coefficient, threshold, candidate, publication, wagering, schedule, or credential behavior changed.

Operational evidence is preserved under `artifacts/operational/nhl/preseason_catchup/2026-09-19/`; this report records only hashes, counts, timestamps, and governed classifications.
