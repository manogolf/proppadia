# NHL mainline historical feasibility and repository inventory

## Decision

Proppadia can construct a trustworthy **outcome-first** game population for canonical seasons `2023` and `2024`, but it cannot yet construct a historically price-certified or model-replayable mainline population. The inspected odds archive has 153 daily files, 907 event wrappers, 855 events with book data, and zero mainline markets. Its only market keys are player shots on goal, alternate player shots on goal, goalie saves, and player points.

## Outcomes and identity

`nhl.games` contains 1,400 games for season `2023`, 1,398 for season `2024`, and 1,312 for season `2025`, with 4,110 distinct IDs and no duplicate IDs or home/away identity conflicts. It does not contain scores. Separate season `2023` and `2024` team summary tables provide exactly two distinct team rows and goal counts for every game. Season `2025` has 900 rows marked final in `nhl.games`, but no populated canonical team-game score table, so it is not outcome-certified here. Regulation, shootout, and overtime settlement remain separate and must not be inferred from full-game goals.

## Prices and markets

No moneyline, regulation moneyline, puck line, game total, team total, or first-period quote survived in the inspected archive. Wrapper capture timestamps are real but apply only to player props; they cannot certify absent mainline prices. There is no derived consensus or closing/opening distinction for mainlines.

## Models and features

No surviving game-level win, score, goal-difference, total, puck-line, Elo, Skellam, or mainline Poisson baseline was identified. The reproducible SOG Poisson formula and other player-prop artifacts are not mainline systems. Team goal/shot rolling summaries create bounded season `2023`/`2024` feature potential, but their strict-prior definitions still need certification. Goalie history is largely actual/postgame: 1,400 games in season `2023`, 1,316 in season `2024`, and 359 in season `2025`, with zero rows shown written before scheduled start. Lineup continuity is weak: only 3 games in season `2023`, 9 in season `2024`; season `2025` has broad game coverage but only 469 individual roster rows timestamped before start.

## Feasibility ranking

Full-game moneyline ranks first because its season `2023`/`2024` identity and outcome grain can now be certified without assuming prices or a model. Puck line, game total, and team total have definable neutral outcomes but remain blocked by absent price history. Regulation moneyline is additionally blocked by regulation-score certification. First-period markets lack enough repository evidence.

## SOG comparison and boundary

The SOG lane is structurally further along: Level 4 probability reproduction, a fixed control, and a characterized feature platform. Mainline has only an outcome-spine opportunity, no baseline, and no prices. This does not imply either market is easier or more profitable, and it does not displace the SOG lane.

## Recommendation

Unlock exactly one bounded task: season `2023` and season `2024` full-game moneyline population and outcome certification. Odds acquisition/backfill, model training, challenger fitting, feature selection, ROI analysis, wagers, promotion, and production restart remain unauthorized.
