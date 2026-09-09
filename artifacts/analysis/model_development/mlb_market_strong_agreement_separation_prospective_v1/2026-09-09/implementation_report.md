# MLB market-strong agreement-separation prospective test v1

## Frozen status

The test was frozen on 2026-09-09 before its first eligible game date, 2026-09-10. Its fixed horizon is the remainder of calendar 2026. The prior 56–20 cohort supplies context and the planning allocation only; it contributes zero rows and zero outcomes to this prospective ledger.

The current classification is **`SEPARATION_EVIDENCE_INSUFFICIENT`**, with report status **`INTERIM_DESCRIPTIVE`** and horizon status **`NOT_STARTED`**. This is expected: the initial ledger contains no post-freeze games or outcomes, and no decision is permitted before 2027-01-01 or before every minimum-evidence gate is met.

## Locked comparison

Pinnacle is the non-outcome-selected reference market. A market side is strong only when its two-sided no-vig h2h probability is strictly greater than 0.60 at the historical snapshot at or before the immutable designated daily prediction timestamp. The frozen model is `MLB_GAME_PYTHAGOREAN_LOG5_V1` at hash `804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6`; it is strong only when its selected-side probability is strictly greater than 0.60.

The primary comparison is all eligible market-strong/model-agreement outcomes versus all eligible market-strong outcomes without agreement. The latter combines model-not-strong and model-strong-opposite games for the primary test while preserving those subclasses separately. Missing model, missing reference price, absent book, one-sided market, stale/post-start quote, and identity mismatch states remain visible in the risk and price ledgers and are never silently discarded.

The fixed bookmaker set is Pinnacle, BetOnline, DraftKings, FanDuel, BetMGM, BetRivers, Fanatics, Bovada, MyBookie.ag, and William Hill US. Results remain book-specific. There is no best-price composite and no retrospective bookmaker selection.

## Evidence and decisions

The lower-bound design target is 732 eligible resolved games: at least 438 agreement games, 294 without agreement, 20 resolved date clusters, and 100 blocked-date out-of-time scored games. This is the two-proportion planning requirement for a declared 10 percentage-point lift from a 60.8% baseline at two-sided alpha 0.05 and 80% power, before date-cluster inflation. If the 2026 season ends below the target, the study emits an interim descriptive report and does not manufacture earlier rows or force a terminal decision.

Reports include group win rate, market and model Brier score and log loss, each fixed book's flat-risk return and paid break-even excess, direct agreement-minus-without-agreement economic contrasts, date-clustered intervals, and expanding blocked-date market-only versus market-plus-agreement scores and coefficients. Bookmaker cells reuse a game's single outcome and do not increase the effective sample size.

The only terminal labels are:

- `MODEL_AGREEMENT_INCREMENTAL_VALUE_SUPPORTED`
- `MARKET_STRENGTH_EXPLAINS_COHORT`
- `MODEL_AGREEMENT_INCREMENTAL_VALUE_NOT_SUPPORTED`
- `SEPARATION_EVIDENCE_INSUFFICIENT`

No production, scheduler, public-prediction, model, threshold, publication, or wagering change is part of this test.

## Acquisition status

No Odds API request was made while implementing or freezing this package, and no credit authorization is inferred from the earlier 76-game acquisition. A future acquisition requires a separate positive total-credit ceiling supplied explicitly to the client. Cost is predeclared as 10 credits per game date for one MLB h2h historical request containing the ten fixed books.

The client writes secret-free request parameters, redacts `apiKey` values from every exception and ledger field, retains quota headers, preserves each redacted raw response before parsing, refuses a repeated successful request, refuses a retry after an unknown or positive charge, and can reconcile a preserved successful response without another request.
