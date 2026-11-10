# NHL weekly model evaluation — week ending 2026-11-07

Schedule: automatic weekly build is on; next report due 2026-11-15 (week ending 2026-11-14).

Week: 2026-11-01 to 2026-11-07 (7 slates with production rows). Season to date: SOG (production and shadows) from 2026-10-08, Points HGB as production from 2026-10-10, Moneyline, Puck Line and Saves from 2026-10-01. Governed grades only. Two-sided lines from 2026-10-10: only picks whose exact line had the same book or prediction market quoting both Over and Under before the prediction (`backend/nhl/two_sided_lines.py`).

## Where every model stands (W-L)

| Lane | Model | Week | Win % | Season | Win % |
|---|---|---:|---:|---:|---:|
| moneyline | NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3 | 32-18 | 64.0% | 163-89 | 64.7% |
| moneyline | NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2 | 29-21 | 58.0% | 153-99 | 60.7% |
| points | production: HGB | 4571-799 | 85.1% | 18718-3185 | 85.5% |
| points | shadow: Phoenix (same players) | 2485-2885 | 46.3% | 9160-11558 | 44.2% |
| puck_line | NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2 | 24-26 | 48.0% | 106-146 | 42.1% |
| puck_line | NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1 | 24-26 | 48.0% | 109-143 | 43.3% |
| saves | production | 908-379 | 70.6% | 4493-2046 | 68.7% |
| sog | production | 4258-1112 | 79.3% | 18535-5084 | 78.5% |
| sog | shadow: A_PRIOR_SEASON_CARRY_FORWARD | 3881-1099 | 77.9% | 17156-4795 | 78.2% |
| sog | shadow: B_PRIOR_SEASON_RECENCY_WEIGHTED | 3869-1111 | 77.7% | 17030-4921 | 77.6% |
| sog | shadow: C_MULTISEASON_SHRUNK_PLAYER | 3883-1097 | 78.0% | 17216-4735 | 78.4% |
| sog | shadow: D_PLAYER_ROLE_HIERARCHICAL | 4182-1188 | 77.9% | 18378-5115 | 78.2% |
| sog | shadow: F_CURRENT_PRESEASON_UPDATE | 3962-1234 | 76.3% | 17455-5327 | 76.6% |
| sog | shadow: G_COLD_START_TO_CURRENT_SEASON_BLEND | 4229-1141 | 78.8% | 18413-5080 | 78.4% |
| sog | shadow: Poisson formula (latest pregame) | 4210-1160 | 78.4% | 7902-2196 | 78.3% |
| sog | shadow: SHRUNK_180 (latest pregame) | 4251-1119 | 79.2% | 8007-2091 | 79.3% |

## Probability quality (production SOG and Points HGB)

Brier and log loss on P(over); lower is better. Mean pick probability vs actual win rate shows calibration.

| Lane | Window | n | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|
| sog | week | 5370 | 4258-1112 | 79.3% | 79.8% | 0.1431 | 0.4394 |
| sog | season | 23619 | 18535-5084 | 78.5% | 79.9% | 0.1478 | 0.4575 |
| points | week | 5370 | 4571-799 | 85.1% | 86.1% | 0.1047 | 0.3292 |
| points | season | 21903 | 18718-3185 | 85.5% | 86.7% | 0.1027 | 0.3252 |

## Two-sided lines (production SOG and Points HGB)

The same measures on picks whose line had a two-sided quote: the level where the model meets the market. Share = two-sided picks out of all settled picks on slates with quote evidence.

| Lane | Window | n | Share | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| sog | week | 1527 | 28.4% | 1016-511 | 66.5% | 66.8% | 0.2056 | 0.5941 |
| sog | season | 5486 | 24.8% | 3571-1915 | 65.1% | 67.6% | 0.2142 | 0.6164 |
| points | week | 2265 | 42.2% | 1676-589 | 74.0% | 75.9% | 0.1745 | 0.5265 |
| points | season | 8124 | 37.1% | 6031-2093 | 74.2% | 77.5% | 0.1751 | 0.5306 |

## By line and side (week / season)

| Lane | Line | Side | Week W-L | Week two-sided | Season W-L | Season win % | Season two-sided | Two-sided win % |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| points | 0.5 | OVER | 230-129 | 206-115 | 713-428 | 62.5% | 574-319 | 64.3% |
| points | 0.5 | UNDER | 1003-428 | 517-289 | 4261-1899 | 69.2% | 1995-1175 | 62.9% |
| points | 1.5 | UNDER | 1590-200 | 703-156 | 6596-705 | 90.3% | 2608-507 | 83.7% |
| points | 2.5 | UNDER | 1748-42 | 250-29 | 7148-153 | 97.9% | 854-92 | 90.3% |
| sog | 1.5 | OVER | 380-223 | 217-152 | 1676-1052 | 61.4% | 798-551 | 59.2% |
| sog | 1.5 | UNDER | 817-370 | 188-135 | 3527-1618 | 68.6% | 730-502 | 59.3% |
| sog | 2.5 | OVER | 82-48 | 55-34 | 368-336 | 52.3% | 200-189 | 51.4% |
| sog | 2.5 | UNDER | 1350-310 | 286-124 | 5850-1319 | 81.6% | 954-437 | 68.6% |
| sog | 3.5 | OVER | 4-2 | 4-2 | 41-49 | 45.6% | 23-19 | 54.8% |
| sog | 3.5 | UNDER | 1625-159 | 266-64 | 7073-710 | 90.9% | 866-217 | 80.0% |

## Model vs market by gap (two-sided lines, season to date)

Gap = model probability on its side minus the market's no-vig probability on that side (median across books). Lower log loss is better; the closer of the two shows who forecast better. `sog formula` is the Poisson formula: production until 2026-10-25, the `sog_poisson_formula_shadow` lane after (latest pregame capture per player-game).

| Lane | Gap | Side | Week W-L | Season W-L | Win % | Model | Market | Log loss model | Log loss market |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| points | model 10+ below | OVER | 2-0 | 27-6 | 81.8% | 55.5% | 69.8% | 0.6174 | 0.5055 |
| points | model 10+ below | UNDER | 4-2 | 47-18 | 72.3% | 58.7% | 71.4% | 0.6431 | 0.6095 |
| points | within 10 | OVER | 199-115 | 516-284 | 64.5% | 58.8% | 58.7% | 0.6491 | 0.6443 |
| points | within 10 | UNDER | 1408-433 | 4577-1273 | 78.2% | 80.2% | 77.1% | 0.4763 | 0.4673 |
| points | 10-15 above | OVER | 5-0 | 25-16 | 61.0% | 60.7% | 48.8% | 0.6290 | 0.6579 |
| points | 10-15 above | UNDER | 55-36 | 523-248 | 67.8% | 80.3% | 68.2% | 0.6274 | 0.5705 |
| points | over 15 above | OVER | 0-0 | 6-13 | 31.6% | 56.3% | 37.3% | 0.6961 | 0.5477 |
| points | over 15 above | UNDER | 3-3 | 310-235 | 56.9% | 76.8% | 56.3% | 0.7711 | 0.6427 |
| sog | model 10+ below | OVER | 0-0 | 12-4 | 75.0% | 53.8% | 67.4% | 0.6648 | 0.5759 |
| sog | model 10+ below | UNDER | 4-0 | 79-20 | 79.8% | 59.8% | 73.7% | 0.5913 | 0.5137 |
| sog | within 10 | OVER | 245-169 | 754-538 | 58.4% | 61.4% | 58.8% | 0.6530 | 0.6484 |
| sog | within 10 | UNDER | 673-288 | 1970-887 | 69.0% | 68.4% | 66.0% | 0.5830 | 0.5833 |
| sog | 10-15 above | OVER | 18-13 | 128-107 | 54.5% | 67.3% | 55.0% | 0.7063 | 0.6659 |
| sog | 10-15 above | UNDER | 47-29 | 257-152 | 62.8% | 74.0% | 61.9% | 0.6458 | 0.6168 |
| sog | over 15 above | OVER | 13-6 | 127-110 | 53.6% | 71.5% | 50.8% | 0.7281 | 0.6590 |
| sog | over 15 above | UNDER | 16-6 | 244-97 | 71.6% | 77.4% | 56.2% | 0.5868 | 0.6165 |
| sog formula | model 10+ below | OVER | 8-4 | 28-10 | 73.7% | 54.2% | 67.7% | 0.6564 | 0.5846 |
| sog formula | model 10+ below | UNDER | 29-7 | 140-40 | 77.8% | 60.1% | 74.4% | 0.6000 | 0.5332 |
| sog formula | within 10 | OVER | 190-130 | 637-451 | 58.5% | 61.2% | 58.3% | 0.6555 | 0.6516 |
| sog formula | within 10 | UNDER | 472-192 | 1558-658 | 70.3% | 68.9% | 67.1% | 0.5736 | 0.5684 |
| sog formula | 10-15 above | OVER | 60-51 | 210-190 | 52.5% | 66.0% | 53.6% | 0.6906 | 0.6521 |
| sog formula | 10-15 above | UNDER | 108-44 | 367-195 | 65.3% | 74.8% | 62.5% | 0.6307 | 0.6057 |
| sog formula | over 15 above | OVER | 50-45 | 200-197 | 50.4% | 71.0% | 50.4% | 0.7700 | 0.6772 |
| sog formula | over 15 above | UNDER | 89-48 | 396-209 | 65.5% | 77.9% | 56.7% | 0.6669 | 0.6326 |

## Areas for improvement (season, segments with at least 40 picks)

Gap = actual win rate minus the model's average confidence in its pick. Negative means overconfident.

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 2.5 | OVER | .70-1.00 | 99 | 63.6% | 76.1% | -12.4 pts |
| sog | 2.5 | OVER | .55-.60 | 200 | 49.0% | 57.6% | -8.6 pts |
| sog | 2.5 | OVER | .60-.70 | 190 | 57.4% | 64.9% | -7.6 pts |
| sog | 3.5 | OVER | .50-.55 | 42 | 45.2% | 52.0% | -6.7 pts |
| sog | 2.5 | OVER | .50-.55 | 215 | 45.6% | 51.9% | -6.3 pts |
| sog | 1.5 | OVER | .70-1.00 | 979 | 73.4% | 78.9% | -5.5 pts |
| sog | 3.5 | UNDER | .45-.50 | 64 | 59.4% | 52.8% | +6.6 pts |
| points | 0.5 | OVER | .60-.70 | 330 | 70.6% | 63.8% | +6.8 pts |
| sog | 3.5 | UNDER | .40-.45 | 90 | 64.4% | 57.4% | +7.0 pts |

Two-sided lines only (at least 40 picks):

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 2.5 | OVER | .70-1.00 | 45 | 64.4% | 75.6% | -11.2 pts |
| sog | 2.5 | OVER | .60-.70 | 101 | 56.4% | 64.8% | -8.3 pts |
| sog | 2.5 | OVER | .50-.55 | 127 | 44.1% | 51.9% | -7.8 pts |
| sog | 2.5 | OVER | .55-.60 | 116 | 50.0% | 57.6% | -7.6 pts |
| sog | 1.5 | OVER | .50-.55 | 292 | 45.2% | 52.3% | -7.1 pts |
| sog | 1.5 | OVER | .70-1.00 | 399 | 71.2% | 78.2% | -7.0 pts |
| points | 0.5 | OVER | .60-.70 | 262 | 70.2% | 63.8% | +6.5 pts |
| points | 0.5 | OVER | .55-.60 | 233 | 64.4% | 57.5% | +6.9 pts |
| sog | 3.5 | UNDER | .45-.50 | 43 | 60.5% | 52.7% | +7.8 pts |

## Improvement log

New this week: IMP-031, IMP-032, IMP-033. Still flagged: IMP-001, IMP-003, IMP-002, IMP-005, IMP-004, IMP-014, IMP-009, IMP-011, IMP-016, IMP-015, IMP-012, IMP-017, IMP-018, IMP-019, IMP-021, IMP-020, IMP-024, IMP-023, IMP-022, IMP-026, IMP-029. Open or in test: 33. Full log: `docs/nhl/model_improvement_log.md`.

## Data health

Latest player performance pulse: `NHL PULSE 2026-11-07: PASS | 4 lanes verified | new transitions 21588 | invalid evidence 0 | invalid identities 0 | hash mismatches 0 | missing files 0` (revision created 2026-11-08 after the build; the build showed the stale 2026-10-25 pulse)

## Not yet covered

- Closing-line value.
- Moneyline and Puck Line probability quality (W-L only above).
