# NHL weekly model evaluation — week ending 2026-11-14

Schedule: automatic weekly build is on; next report due 2026-11-22 (week ending 2026-11-21).

Week: 2026-11-08 to 2026-11-14 (7 slates with production rows). W-L table season to date: SOG (production and shadows) from 2026-10-08, Points HGB as production from 2026-10-10, Moneyline, Puck Line and Saves from 2026-10-01. Every other table covers the current production model only: SOG HGB from 2026-10-25, Points (current model identity) from 2026-11-01; retired models appear only in the gap table, labelled. Governed grades only. Two-sided lines from 2026-10-10: only picks whose exact line had the same book or prediction market quoting both Over and Under before the prediction (`backend/nhl/two_sided_lines.py`).

## Where every model stands (W-L)

| Lane | Model | Week | Win % | Season | Win % |
|---|---|---:|---:|---:|---:|
| moneyline | NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3 | 24-25 | 49.0% | 187-114 | 62.1% |
| moneyline | NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2 | 28-21 | 57.1% | 181-120 | 60.1% |
| points | production: HGB | 4515-723 | 86.2% | 23233-3908 | 85.6% |
| points | shadow: Phoenix (same players) | 2332-2906 | 44.5% | 11492-14464 | 44.3% |
| puck_line | NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2 | 18-31 | 36.7% | 124-177 | 41.2% |
| puck_line | NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1 | 18-31 | 36.7% | 127-174 | 42.2% |
| saves | production | 871-403 | 68.4% | 5364-2449 | 68.7% |
| sog | production | 4139-1099 | 79.0% | 22674-6183 | 78.6% |
| sog | shadow: A_PRIOR_SEASON_CARRY_FORWARD | 3831-1065 | 78.2% | 20987-5860 | 78.2% |
| sog | shadow: B_PRIOR_SEASON_RECENCY_WEIGHTED | 3794-1102 | 77.5% | 20824-6023 | 77.6% |
| sog | shadow: C_MULTISEASON_SHRUNK_PLAYER | 3838-1058 | 78.4% | 21054-5793 | 78.4% |
| sog | shadow: D_PLAYER_ROLE_HIERARCHICAL | 4083-1152 | 78.0% | 22461-6267 | 78.2% |
| sog | shadow: F_CURRENT_PRESEASON_UPDATE | 3891-1164 | 77.0% | 21346-6491 | 76.7% |
| sog | shadow: G_COLD_START_TO_CURRENT_SEASON_BLEND | 4124-1111 | 78.8% | 22537-6191 | 78.4% |
| sog | shadow: Poisson formula (latest pregame) | 4092-1146 | 78.1% | 11994-3342 | 78.2% |
| sog | shadow: SHRUNK_180 (latest pregame) | 4151-1087 | 79.2% | 12158-3178 | 79.3% |

## Probability quality (production SOG and Points HGB)

Brier and log loss on P(over); lower is better. Mean pick probability vs actual win rate shows calibration.

| Lane | Window | n | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|
| sog | week | 5238 | 4139-1099 | 79.0% | 79.7% | 0.1442 | 0.4425 |
| sog | season | 15654 | 12405-3249 | 79.2% | 79.8% | 0.1428 | 0.4390 |
| points | week | 5238 | 4515-723 | 86.2% | 85.9% | 0.0964 | 0.3094 |
| points | season | 10608 | 9086-1522 | 85.7% | 86.0% | 0.1006 | 0.3194 |

## Two-sided lines (production SOG and Points HGB)

The same measures on picks whose line had a two-sided quote: the level where the model meets the market. Share = two-sided picks out of all settled picks on slates with quote evidence.

| Lane | Window | n | Share | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| sog | week | 1945 | 37.1% | 1306-639 | 67.1% | 68.3% | 0.2064 | 0.5991 |
| sog | season | 5019 | 32.1% | 3320-1699 | 66.1% | 67.6% | 0.2082 | 0.6021 |
| points | week | 2391 | 45.6% | 1812-579 | 75.8% | 75.8% | 0.1603 | 0.4870 |
| points | season | 4656 | 43.9% | 3488-1168 | 74.9% | 75.9% | 0.1672 | 0.5062 |

## By line and side (week / season)

| Lane | Line | Side | Week W-L | Week two-sided | Season W-L | Season win % | Season two-sided | Two-sided win % |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| points | 0.5 | OVER | 189-161 | 188-156 | 419-290 | 59.1% | 394-271 | 59.2% |
| points | 0.5 | UNDER | 1014-382 | 552-281 | 2017-810 | 71.3% | 1069-570 | 65.2% |
| points | 1.5 | UNDER | 1599-147 | 776-126 | 3189-347 | 90.2% | 1479-282 | 84.0% |
| points | 2.5 | UNDER | 1713-33 | 296-16 | 3461-75 | 97.9% | 546-45 | 92.4% |
| sog | 1.5 | OVER | 391-223 | 320-188 | 1128-646 | 63.6% | 762-489 | 60.9% |
| sog | 1.5 | UNDER | 778-354 | 181-133 | 2381-1063 | 69.1% | 561-412 | 57.7% |
| sog | 2.5 | OVER | 74-43 | 66-38 | 228-154 | 59.7% | 172-119 | 59.1% |
| sog | 2.5 | UNDER | 1324-305 | 375-178 | 3936-900 | 81.4% | 909-441 | 67.3% |
| sog | 3.5 | OVER | 3-3 | 3-2 | 13-8 | 61.9% | 12-5 | 70.6% |
| sog | 3.5 | UNDER | 1569-171 | 361-100 | 4719-478 | 90.8% | 904-233 | 79.5% |

## Model vs market by gap (two-sided lines, season to date)

Gap = model probability on its side minus the market's no-vig probability on that side (median across books). Lower log loss is better; the closer of the two shows who forecast better. `sog formula` is the Poisson formula: production until 2026-10-25, the `sog_poisson_formula_shadow` lane after (latest pregame capture per player-game). `points retired models` is production Points before the current model; for history only.

| Lane | Gap | Side | Week W-L | Season W-L | Win % | Model | Market | Log loss model | Log loss market |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| points | model 10+ below | OVER | 1-0 | 3-0 | 100.0% | 51.8% | 63.6% | 0.6574 | 0.4534 |
| points | model 10+ below | UNDER | 2-1 | 6-3 | 66.7% | 55.4% | 66.8% | 0.6731 | 0.6394 |
| points | within 10 | OVER | 185-155 | 384-270 | 58.7% | 58.9% | 58.6% | 0.6654 | 0.6479 |
| points | within 10 | UNDER | 1577-395 | 2985-828 | 78.3% | 78.9% | 76.3% | 0.4715 | 0.4701 |
| points | 10-15 above | OVER | 2-1 | 7-1 | 87.5% | 72.3% | 61.5% | 0.5028 | 0.5937 |
| points | 10-15 above | UNDER | 37-23 | 92-59 | 60.9% | 74.7% | 62.9% | 0.6632 | 0.6124 |
| points | over 15 above | UNDER | 8-4 | 11-7 | 61.1% | 77.7% | 60.0% | 0.6544 | 0.6080 |
| points retired models | model 10+ below | OVER | 0-0 | 25-6 | 80.6% | 55.8% | 70.3% | 0.6139 | 0.5077 |
| points retired models | model 10+ below | UNDER | 0-0 | 43-16 | 72.9% | 58.8% | 71.6% | 0.6403 | 0.6059 |
| points retired models | within 10 | OVER | 0-0 | 317-169 | 65.2% | 58.6% | 58.6% | 0.6530 | 0.6575 |
| points retired models | within 10 | UNDER | 0-0 | 3169-840 | 79.0% | 80.8% | 77.6% | 0.4660 | 0.4571 |
| points retired models | 10-15 above | OVER | 0-0 | 20-16 | 55.6% | 59.5% | 47.5% | 0.6652 | 0.6739 |
| points retired models | 10-15 above | UNDER | 0-0 | 468-212 | 68.8% | 81.1% | 68.9% | 0.6229 | 0.5652 |
| points retired models | over 15 above | OVER | 0-0 | 6-13 | 31.6% | 56.3% | 37.3% | 0.6961 | 0.5477 |
| points retired models | over 15 above | UNDER | 0-0 | 307-232 | 57.0% | 76.7% | 56.3% | 0.7700 | 0.6422 |
| sog | model 10+ below | OVER | 2-1 | 3-1 | 75.0% | 52.5% | 64.2% | 0.6632 | 0.5706 |
| sog | model 10+ below | UNDER | 3-1 | 10-3 | 76.9% | 60.7% | 72.4% | 0.5434 | 0.4935 |
| sog | within 10 | OVER | 353-210 | 821-557 | 59.6% | 61.9% | 59.4% | 0.6450 | 0.6441 |
| sog | within 10 | UNDER | 853-381 | 2169-981 | 68.9% | 69.4% | 66.8% | 0.5838 | 0.5855 |
| sog | 10-15 above | OVER | 15-13 | 60-35 | 63.2% | 68.6% | 56.6% | 0.6253 | 0.6289 |
| sog | 10-15 above | UNDER | 46-23 | 151-82 | 64.8% | 71.5% | 59.8% | 0.6143 | 0.6110 |
| sog | over 15 above | OVER | 19-4 | 62-20 | 75.6% | 76.9% | 56.2% | 0.4802 | 0.5960 |
| sog | over 15 above | UNDER | 15-6 | 44-20 | 68.8% | 75.5% | 56.5% | 0.6695 | 0.6457 |
| sog formula | model 10+ below | OVER | 9-2 | 37-12 | 75.5% | 55.1% | 68.5% | 0.6489 | 0.5689 |
| sog formula | model 10+ below | UNDER | 55-17 | 195-57 | 77.4% | 60.4% | 74.7% | 0.5984 | 0.5409 |
| sog formula | within 10 | OVER | 241-175 | 878-626 | 58.4% | 61.4% | 58.6% | 0.6556 | 0.6510 |
| sog formula | within 10 | UNDER | 608-233 | 2166-891 | 70.9% | 69.5% | 67.8% | 0.5714 | 0.5653 |
| sog formula | 10-15 above | OVER | 96-64 | 306-254 | 54.6% | 66.8% | 54.5% | 0.6682 | 0.6378 |
| sog formula | 10-15 above | UNDER | 101-66 | 468-261 | 64.2% | 75.2% | 62.9% | 0.6585 | 0.6174 |
| sog formula | over 15 above | OVER | 72-60 | 272-257 | 51.4% | 71.0% | 50.4% | 0.7547 | 0.6685 |
| sog formula | over 15 above | UNDER | 80-66 | 476-275 | 63.4% | 78.0% | 56.8% | 0.6997 | 0.6387 |

## Areas for improvement (season, segments with at least 40 picks)

Gap = actual win rate minus the model's average confidence in its pick. Negative means overconfident.

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 1.5 | OVER | .50-.55 | 333 | 45.3% | 52.5% | -7.2 pts |
| sog | 2.5 | OVER | .50-.55 | 116 | 48.3% | 52.4% | -4.1 pts |
| sog | 1.5 | OVER | .55-.60 | 291 | 53.6% | 57.7% | -4.0 pts |
| points | 0.5 | UNDER | .40-.45 | 298 | 54.0% | 57.7% | -3.6 pts |
| points | 0.5 | UNDER | .45-.50 | 184 | 49.5% | 52.7% | -3.2 pts |
| points | 0.5 | OVER | .50-.55 | 251 | 50.2% | 52.6% | -2.4 pts |
| sog | 2.5 | OVER | .60-.70 | 140 | 67.1% | 64.0% | +3.1 pts |
| sog | 3.5 | UNDER | .40-.45 | 71 | 62.0% | 58.0% | +4.0 pts |
| sog | 1.5 | UNDER | .45-.50 | 375 | 56.5% | 52.5% | +4.1 pts |

Two-sided lines only (at least 40 picks):

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 1.5 | OVER | .50-.55 | 270 | 43.3% | 52.5% | -9.1 pts |
| sog | 2.5 | OVER | .50-.55 | 97 | 46.4% | 52.4% | -6.0 pts |
| points | 0.5 | UNDER | .45-.50 | 173 | 47.4% | 52.7% | -5.3 pts |
| sog | 1.5 | UNDER | .40-.45 | 277 | 52.7% | 57.4% | -4.7 pts |
| sog | 2.5 | UNDER | .00-.30 | 616 | 74.5% | 78.4% | -3.9 pts |
| points | 0.5 | UNDER | .40-.45 | 283 | 54.1% | 57.7% | -3.6 pts |
| sog | 1.5 | UNDER | .45-.50 | 303 | 55.1% | 52.4% | +2.7 pts |
| sog | 2.5 | UNDER | .45-.50 | 122 | 54.9% | 52.2% | +2.8 pts |
| sog | 2.5 | OVER | .60-.70 | 96 | 68.8% | 63.7% | +5.1 pts |

## Improvement log

New this week: IMP-034, IMP-035, IMP-036, IMP-037, IMP-038. Still flagged: none. Open or in test: 5. Full log: `docs/nhl/model_improvement_log.md`.

## Data health

Latest player performance pulse: `NHL PULSE 2026-11-14: PASS | 4 lanes verified | new transitions 26933 | invalid evidence 0 | invalid identities 0 | hash mismatches 0 | missing files 0`

## Not yet covered

- Closing-line value.
- Moneyline and Puck Line probability quality (W-L only above).
