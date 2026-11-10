# NHL weekly model evaluation — week ending 2026-10-31

Schedule: automatic weekly build is on; next report due 2026-11-08 (week ending 2026-11-07).

Week: 2026-10-25 to 2026-10-31 (7 slates with production rows). Season to date: SOG (production and shadows) from 2026-10-08, Points HGB as production from 2026-10-10, Moneyline, Puck Line and Saves from 2026-10-01. Governed grades only. Two-sided lines from 2026-10-10: only picks whose exact line had the same book or prediction market quoting both Over and Under before the prediction (`backend/nhl/two_sided_lines.py`).

## Where every model stands (W-L)

| Lane | Model | Week | Win % | Season | Win % |
|---|---|---:|---:|---:|---:|
| moneyline | NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3 | 29-18 | 61.7% | 131-71 | 64.9% |
| moneyline | NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2 | 30-17 | 63.8% | 124-78 | 61.4% |
| points | production: HGB | 4335-711 | 85.9% | 14147-2386 | 85.6% |
| points | shadow: Phoenix (same players) | 2397-2649 | 47.5% | 6675-8673 | 43.5% |
| puck_line | NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2 | 20-27 | 42.6% | 82-120 | 40.6% |
| puck_line | NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1 | 21-26 | 44.7% | 85-117 | 42.1% |
| saves | production | 869-353 | 71.1% | 3585-1667 | 68.3% |
| sog | production | 4008-1038 | 79.4% | 14277-3972 | 78.2% |
| sog | shadow: A_PRIOR_SEASON_CARRY_FORWARD | 3731-997 | 78.9% | 13275-3696 | 78.2% |
| sog | shadow: B_PRIOR_SEASON_RECENCY_WEIGHTED | 3700-1028 | 78.3% | 13161-3810 | 77.5% |
| sog | shadow: C_MULTISEASON_SHRUNK_PLAYER | 3749-979 | 79.3% | 13333-3638 | 78.6% |
| sog | shadow: D_PLAYER_ROLE_HIERARCHICAL | 3986-1054 | 79.1% | 14196-3927 | 78.3% |
| sog | shadow: F_CURRENT_PRESEASON_UPDATE | 3744-1116 | 77.0% | 13493-4093 | 76.7% |
| sog | shadow: G_COLD_START_TO_CURRENT_SEASON_BLEND | 3968-1072 | 78.7% | 14184-3939 | 78.3% |
| sog | shadow: Poisson formula (latest pregame) | 3692-1036 | 78.1% | 3692-1036 | 78.1% |
| sog | shadow: SHRUNK_180 (latest pregame) | 3756-972 | 79.4% | 3756-972 | 79.4% |

## Probability quality (production SOG and Points HGB)

Brier and log loss on P(over); lower is better. Mean pick probability vs actual win rate shows calibration.

| Lane | Window | n | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|
| sog | week | 5046 | 4008-1038 | 79.4% | 79.9% | 0.1409 | 0.4349 |
| sog | season | 18249 | 14277-3972 | 78.2% | 79.9% | 0.1492 | 0.4629 |
| points | week | 5046 | 4335-711 | 85.9% | 86.2% | 0.1024 | 0.3245 |
| points | season | 16533 | 14147-2386 | 85.6% | 86.9% | 0.1020 | 0.3239 |

## Two-sided lines (production SOG and Points HGB)

The same measures on picks whose line had a two-sided quote: the level where the model meets the market. Share = two-sided picks out of all settled picks on slates with quote evidence.

| Lane | Window | n | Share | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| sog | week | 1547 | 30.7% | 998-549 | 64.5% | 67.5% | 0.2131 | 0.6139 |
| sog | season | 3959 | 23.6% | 2555-1404 | 64.5% | 67.9% | 0.2175 | 0.6249 |
| points | week | 1986 | 39.4% | 1492-494 | 75.1% | 76.3% | 0.1764 | 0.5344 |
| points | season | 5859 | 35.4% | 4355-1504 | 74.3% | 78.1% | 0.1754 | 0.5322 |

## By line and side (week / season)

| Lane | Line | Side | Week W-L | Week two-sided | Season W-L | Season win % | Season two-sided | Two-sided win % |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| points | 0.5 | OVER | 205-99 | 170-80 | 483-299 | 61.8% | 368-204 | 64.3% |
| points | 0.5 | UNDER | 974-404 | 490-258 | 3258-1471 | 68.9% | 1478-886 | 62.5% |
| points | 1.5 | UNDER | 1517-165 | 636-130 | 5006-505 | 90.8% | 1905-351 | 84.4% |
| points | 2.5 | UNDER | 1639-43 | 196-26 | 5400-111 | 98.0% | 604-63 | 90.6% |
| sog | 1.5 | OVER | 357-200 | 225-149 | 1296-829 | 61.0% | 581-399 | 59.3% |
| sog | 1.5 | UNDER | 786-339 | 192-144 | 2710-1248 | 68.5% | 542-367 | 59.6% |
| sog | 2.5 | OVER | 72-63 | 51-47 | 286-288 | 49.8% | 145-155 | 48.3% |
| sog | 2.5 | UNDER | 1262-285 | 248-139 | 4500-1009 | 81.7% | 668-313 | 68.1% |
| sog | 3.5 | OVER | 6-3 | 5-1 | 37-47 | 44.0% | 19-17 | 52.8% |
| sog | 3.5 | UNDER | 1525-148 | 277-69 | 5448-551 | 90.8% | 600-153 | 79.7% |

## Model vs market by gap (two-sided lines, season to date)

Gap = model probability on its side minus the market's no-vig probability on that side (median across books). Lower log loss is better; the closer of the two shows who forecast better. `sog formula` is the Poisson formula: production until 2026-10-25, the `sog_poisson_formula_shadow` lane after (latest pregame capture per player-game).

| Lane | Gap | Side | Week W-L | Season W-L | Win % | Model | Market | Log loss model | Log loss market |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| points | model 10+ below | OVER | 5-1 | 25-6 | 80.6% | 55.8% | 70.3% | 0.6139 | 0.5077 |
| points | model 10+ below | UNDER | 7-3 | 43-16 | 72.9% | 58.8% | 71.6% | 0.6403 | 0.6059 |
| points | within 10 | OVER | 154-76 | 317-169 | 65.2% | 58.6% | 58.6% | 0.6530 | 0.6575 |
| points | within 10 | UNDER | 1232-352 | 3169-840 | 79.0% | 80.8% | 77.6% | 0.4660 | 0.4571 |
| points | 10-15 above | OVER | 10-3 | 20-16 | 55.6% | 59.5% | 47.5% | 0.6652 | 0.6739 |
| points | 10-15 above | UNDER | 63-43 | 468-212 | 68.8% | 81.1% | 68.9% | 0.6229 | 0.5652 |
| points | over 15 above | OVER | 1-0 | 6-13 | 31.6% | 56.3% | 37.3% | 0.6961 | 0.5477 |
| points | over 15 above | UNDER | 20-16 | 307-232 | 57.0% | 76.7% | 56.3% | 0.7700 | 0.6422 |
| sog | model 10+ below | OVER | 1-0 | 12-4 | 75.0% | 53.8% | 67.4% | 0.6648 | 0.5759 |
| sog | model 10+ below | UNDER | 3-2 | 75-20 | 78.9% | 59.7% | 73.7% | 0.5961 | 0.5221 |
| sog | within 10 | OVER | 223-178 | 509-369 | 58.0% | 61.5% | 58.8% | 0.6553 | 0.6501 |
| sog | within 10 | UNDER | 643-312 | 1297-599 | 68.4% | 68.4% | 66.1% | 0.5917 | 0.5910 |
| sog | 10-15 above | OVER | 27-9 | 110-94 | 53.9% | 67.5% | 55.1% | 0.7052 | 0.6624 |
| sog | 10-15 above | UNDER | 58-30 | 210-123 | 63.1% | 74.3% | 62.1% | 0.6491 | 0.6183 |
| sog | over 15 above | OVER | 30-10 | 114-104 | 52.3% | 70.9% | 50.1% | 0.7438 | 0.6662 |
| sog | over 15 above | UNDER | 13-8 | 228-91 | 71.5% | 77.5% | 56.1% | 0.5877 | 0.6146 |
| sog formula | model 10+ below | OVER | 9-2 | 20-6 | 76.9% | 53.6% | 67.4% | 0.6545 | 0.5570 |
| sog formula | model 10+ below | UNDER | 39-15 | 111-33 | 77.1% | 59.9% | 74.2% | 0.6081 | 0.5436 |
| sog formula | within 10 | OVER | 161-130 | 447-321 | 58.2% | 61.4% | 58.6% | 0.6577 | 0.6497 |
| sog formula | within 10 | UNDER | 432-179 | 1086-466 | 70.0% | 68.5% | 66.9% | 0.5829 | 0.5765 |
| sog formula | 10-15 above | OVER | 67-54 | 150-139 | 51.9% | 66.7% | 54.2% | 0.7111 | 0.6627 |
| sog formula | 10-15 above | UNDER | 107-58 | 259-151 | 63.2% | 75.3% | 62.9% | 0.6525 | 0.6129 |
| sog formula | over 15 above | OVER | 66-58 | 150-152 | 49.7% | 70.7% | 49.9% | 0.7783 | 0.6828 |
| sog formula | over 15 above | UNDER | 92-78 | 307-161 | 65.6% | 77.9% | 56.5% | 0.6701 | 0.6347 |

## Areas for improvement (season, segments with at least 40 picks)

Gap = actual win rate minus the model's average confidence in its pick. Negative means overconfident.

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 2.5 | OVER | .70-1.00 | 86 | 62.8% | 76.8% | -14.0 pts |
| sog | 2.5 | OVER | .60-.70 | 149 | 53.7% | 65.1% | -11.4 pts |
| sog | 2.5 | OVER | .55-.60 | 165 | 46.7% | 57.6% | -10.9 pts |
| sog | 2.5 | OVER | .50-.55 | 174 | 43.1% | 51.8% | -8.7 pts |
| points | 0.5 | OVER | .70-1.00 | 48 | 64.6% | 72.8% | -8.2 pts |
| sog | 1.5 | OVER | .70-1.00 | 785 | 72.2% | 79.1% | -6.9 pts |
| sog | 3.5 | UNDER | .45-.50 | 47 | 59.6% | 52.8% | +6.7 pts |
| sog | 3.5 | UNDER | .30-.40 | 272 | 72.4% | 65.4% | +7.0 pts |
| sog | 3.5 | UNDER | .40-.45 | 69 | 69.6% | 57.3% | +12.3 pts |

Two-sided lines only (at least 40 picks):

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 2.5 | OVER | .60-.70 | 80 | 51.2% | 64.8% | -13.6 pts |
| sog | 2.5 | OVER | .50-.55 | 94 | 40.4% | 51.8% | -11.4 pts |
| sog | 2.5 | OVER | .55-.60 | 90 | 47.8% | 57.7% | -9.9 pts |
| sog | 1.5 | OVER | .70-1.00 | 315 | 68.9% | 78.3% | -9.4 pts |
| sog | 1.5 | OVER | .50-.55 | 209 | 44.5% | 52.3% | -7.8 pts |
| sog | 1.5 | UNDER | .40-.45 | 260 | 50.0% | 57.5% | -7.5 pts |
| points | 0.5 | OVER | .50-.55 | 221 | 58.8% | 52.4% | +6.4 pts |
| points | 0.5 | OVER | .55-.60 | 146 | 65.8% | 57.5% | +8.2 pts |
| sog | 3.5 | UNDER | .40-.45 | 42 | 69.0% | 57.1% | +11.9 pts |

## Improvement log

New this week: IMP-013, IMP-014, IMP-015, IMP-016, IMP-017, IMP-018, IMP-019, IMP-020, IMP-021, IMP-022, IMP-023, IMP-024, IMP-025, IMP-026, IMP-027, IMP-028, IMP-029, IMP-030. Still flagged: IMP-001, IMP-002, IMP-003, IMP-005, IMP-004, IMP-006, IMP-010, IMP-009, IMP-011, IMP-012. Open or in test: 30. Full log: `docs/nhl/model_improvement_log.md`.

## Data health

Latest player performance pulse: `NHL PULSE 2026-10-25: PASS | 4 lanes verified | new transitions 11554 | invalid evidence 0 | invalid identities 0 | hash mismatches 0 | missing files 0`

## Not yet covered

- Closing-line value.
- Moneyline and Puck Line probability quality (W-L only above).
