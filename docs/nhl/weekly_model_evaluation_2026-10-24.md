# NHL weekly model evaluation — week ending 2026-10-24

Week: 2026-10-18 to 2026-10-24 (7 slates with production rows). Season to date: SOG (production and shadows) from 2026-10-08, Points HGB as production from 2026-10-10, Moneyline, Puck Line and Saves from 2026-10-01. Governed grades only.

## Where every model stands (W-L)

| Lane | Model | Week | Win % | Season | Win % |
|---|---|---:|---:|---:|---:|
| moneyline | NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3 | 26-17 | 60.5% | 102-53 | 65.8% |
| moneyline | NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2 | 0-0 | — | 59-38 | 60.8% |
| points | production: HGB | 4122-711 | 85.3% | 9812-1675 | 85.4% |
| points | shadow: Phoenix (same players) | 2188-2645 | 45.3% | 4278-6024 | 41.5% |
| puck_line | NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2 | 20-23 | 46.5% | 62-93 | 40.0% |
| puck_line | NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1 | 20-23 | 46.5% | 64-91 | 41.3% |
| saves | production | 751-367 | 67.2% | 2716-1314 | 67.4% |
| sog | production | 3749-1084 | 77.6% | 10269-2934 | 77.8% |
| sog | shadow: A_PRIOR_SEASON_CARRY_FORWARD | 3340-950 | 77.9% | 9544-2699 | 78.0% |
| sog | shadow: B_PRIOR_SEASON_RECENCY_WEIGHTED | 3310-980 | 77.2% | 9461-2782 | 77.3% |
| sog | shadow: C_MULTISEASON_SHRUNK_PLAYER | 3352-938 | 78.1% | 9584-2659 | 78.3% |
| sog | shadow: D_PLAYER_ROLE_HIERARCHICAL | 3606-1011 | 78.1% | 10210-2873 | 78.0% |
| sog | shadow: F_CURRENT_PRESEASON_UPDATE | 3422-1057 | 76.4% | 9749-2977 | 76.6% |
| sog | shadow: G_COLD_START_TO_CURRENT_SEASON_BLEND | 3592-1025 | 77.8% | 10216-2867 | 78.1% |

## Probability quality (production SOG and Points HGB)

Brier and log loss on P(over); lower is better. Mean pick probability vs actual win rate shows calibration.

| Lane | Window | n | W-L | Win % | Mean pick prob | Brier | Log loss |
|---|---|---:|---:|---:|---:|---:|---:|
| sog | week | 4833 | 3749-1084 | 77.6% | 79.9% | 0.1533 | 0.4782 |
| sog | season | 13203 | 10269-2934 | 77.8% | 80.0% | 0.1524 | 0.4735 |
| points | week | 4833 | 4122-711 | 85.3% | 85.5% | 0.1015 | 0.3206 |
| points | season | 11487 | 9812-1675 | 85.4% | 87.2% | 0.1018 | 0.3236 |

## By line and side (week / season)

| Lane | Line | Side | Week W-L | Season W-L | Season win % |
|---|---:|---|---:|---:|---:|
| points | 0.5 | OVER | 183-141 | 278-200 | 58.2% |
| points | 0.5 | UNDER | 889-398 | 2284-1067 | 68.2% |
| points | 1.5 | UNDER | 1469-142 | 3489-340 | 91.1% |
| points | 2.5 | UNDER | 1581-30 | 3761-68 | 98.2% |
| sog | 1.5 | OVER | 365-234 | 939-629 | 59.9% |
| sog | 1.5 | UNDER | 676-336 | 1924-909 | 67.9% |
| sog | 2.5 | OVER | 94-80 | 214-225 | 48.7% |
| sog | 2.5 | UNDER | 1171-266 | 3238-724 | 81.7% |
| sog | 3.5 | OVER | 11-16 | 31-44 | 41.3% |
| sog | 3.5 | UNDER | 1432-152 | 3923-403 | 90.7% |

## Areas for improvement (season, segments with at least 40 picks)

Gap = actual win rate minus the model's average confidence in its pick. Negative means overconfident.

| Lane | Line | Side | Band | n | Win % | Predicted | Gap |
|---|---:|---|---|---:|---:|---:|---:|
| sog | 2.5 | OVER | .70-1.00 | 75 | 62.7% | 77.3% | -14.6 pts |
| sog | 2.5 | OVER | .60-.70 | 103 | 51.5% | 65.6% | -14.1 pts |
| sog | 2.5 | OVER | .55-.60 | 137 | 44.5% | 57.7% | -13.1 pts |
| sog | 1.5 | OVER | .70-1.00 | 596 | 70.3% | 79.4% | -9.1 pts |
| sog | 2.5 | OVER | .50-.55 | 124 | 42.7% | 51.6% | -8.8 pts |
| points | 0.5 | UNDER | .30-.40 | 837 | 57.1% | 65.5% | -8.4 pts |
| sog | 2.5 | UNDER | .45-.50 | 153 | 59.5% | 53.2% | +6.3 pts |
| sog | 3.5 | UNDER | .30-.40 | 205 | 73.2% | 65.3% | +7.9 pts |
| sog | 3.5 | UNDER | .40-.45 | 48 | 66.7% | 57.0% | +9.7 pts |

## Data health

Latest player performance pulse: `NHL PULSE 2026-10-25: PASS | 4 lanes verified | new transitions 11554 | invalid evidence 0 | invalid identities 0 | hash mismatches 0 | missing files 0`

## Not yet covered

- Model vs market (two-sided lines) and closing-line value.
- Moneyline and Puck Line probability quality (W-L only above).
