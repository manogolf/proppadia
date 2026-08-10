# NHL moneyline strict-prior team and goalie feature spine

## Result

The frozen 2,798-game population is preserved exactly. A team-only goals/shots spine was constructed with season-to-date, prior-5, and prior-10 windows, always shifted before the target game and reset by canonical season. Season `2023` chronology is exact. Season `2024` team ordering is bounded because 82 Utah games lack dates; no value was filled from full-season data.

Team source grain is exactly 5,596 team-games with no duplicates or many-to-many joins. Early-history nulls remain visible. Schedule/rest is exact only while a team's date chain is complete; 1235 game rows are date-blocked after accounting for missing dates and downstream rest uncertainty. No schedule helper was trusted or used; season grouping follows the certified parent.

Goalie logs support actual max-TOI goalie oracle identity for 1,400 season `2023` games and 1,316 season `2024` games. They contain no certified projected or confirmed pregame starter history, and actual starter/performance is excluded from model inputs. Roster history covers only 3 and 9 games respectively, with no governed injury or scratch source.

## Leakage and readiness

All constructed team statistics use only lower game IDs within the same canonical season and are shifted before rolling. Final score, winner, goalie participation, and other postgame fields are evaluation/oracle-only. No feature was selected, optimized, or fitted.

Team-only baseline research is `READY_WITH_BOUNDED_LIMITS`; team+schedule is `BLOCKED_BY_DATE_GAPS`; team+goalie is `BLOCKED_BY_GOALIE_TIMING`. The evidence selects one next task: bounded remediation and certification of the 82 missing season `2024` Utah game dates. Baseline fitting, training, odds, ROI, deployment, and restart remain unauthorized.
