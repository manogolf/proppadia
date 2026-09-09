# MLB agreement-separation live-timing review v3

## Finding

The 10-credit historical endpoint is not justified as the routine acquisition path for this prospective study. The current sport-odds endpoint can return the frozen ten books as one bookmaker group, and h2h costs one credit per group. The least expensive valid design is to expand and move the existing three-market Pinnacle request: its gross cost remains three credits per date and its incremental study cost is zero.

The installed LaunchAgent includes 05:30 local time. The wrapper calls the immutable moneyline lifecycle before the full-game market hook, but roster/stat refresh, predictions-wide, Hits 0.5 research scoring, slate generation, and artifact preservation intervene. On 2026-09-09 the model timestamp was `2026-09-09T12:32:39.520656Z`, all 15 rows committed at `2026-09-09T12:32:42.569548Z`, and the current Pinnacle request began at `2026-09-09T12:51:33.355487Z`—a 1130.786-second delay. The order is safe, but the delay is not “immediate” under the new five-minute ceiling.

The existing call is `GET /v4/sports/baseball_mlb/odds` with `bookmakers=pinnacle`, no `regions`, and `markets=h2h,totals,spreads`. Its recorded `x-requests-last` is 3. The complete raw response is written before HTTP status handling. The parser deliberately retains only Pinnacle rows for its existing downstream tables, so adding nine books to the raw request does not make those books model inputs or change existing Pinnacle attachments. The sample preserved 14 Pinnacle h2h updates; all were pregame and no later than response receipt.

## Acquisition comparison through September 27

| Option | Incremental credits, no failures | Gross designated-request credits | Incremental credits if every date needs historical recovery |
|---|---:|---:|---:|
| EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST | 0 | 54 | 180 |
| ADD_SEPARATE_LIVE_TEN_BOOK_H2H_REQUEST | 18 | 18 | 198 |
| HISTORICAL_SNAPSHOT_FOR_EVERY_DATE | 180 | 180 | 180 |

The 54-credit gross figure for the expanded option is the already-existing 18 × 3-credit request, not new study usage. A separate one-market live request costs 18 additional credits. Historical acquisition on every date costs 180 additional credits. If separate live attempts were charged and every date also required fallback, the combined incremental ceiling would be 198.

## Timing amendment and recovery

Version 3 replaces only the designated price timing term. The primary price is now the first qualifying live capture initiated after durable prediction commit and within five minutes, with no intervening workflow stage. The ten-book raw payload cannot enter prediction generation because the database commit is the barrier and parsing/state construction happens afterward.

Every attempt must preserve the complete body, redacted parameters, request/receipt times, and quota headers. Retry live only after a demonstrably uncharged failure and only inside the five-minute window. A charged or uncertain failure cannot be replaced by a later live price. It may be recovered with the historical endpoint at the already-fixed first-attempt timestamp—or at commit plus five minutes if the request never started. Recovery is automatic for request-level failure, never driven by prices or outcomes, and is separately labeled. Individual absent or one-sided books in a successful response are classified rather than backfilled.

This rule preserves prospective status because prediction, target time, and recovery rule are frozen before outcomes. Historical recovery is operationally less exact and must remain a separately reported sensitivity. No API call or scheduler change was made.
