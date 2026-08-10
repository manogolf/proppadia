# NHL full-game moneyline population and outcome certification

## Result

The fixed outcome spine is certified at 2,798 games: 1,400 for canonical season `2023` and 1,398 for canonical season `2024`. Every game has unique identity, aligned home/away teams, a decisive reconstructed final score, and one neutral HOME_WIN or AWAY_WIN target. There are no exclusions. Eighty-two Utah rows in season `2024` have null `nhl.games.game_date`; the gap is retained visibly and does not alter canonical `season + game_id` identity.

## Authority correction and hierarchy

`nhl.games` is authoritative for season, game ID, date, and home/away identity. The season team-summary tables are authoritative only for two-team grain: exactly two distinct, correctly aligned team rows exist for every game. Their `num_event_goal_for` is zero on both rows for every game and therefore is **not** score authority, correcting the feasibility package's preliminary interpretation.

Final scores are reconstructed from the raw season shot-stage fields: score before each event plus the event's goal indicator. For shootouts, the score remains tied in shot events, so one goal is added to the winner identified by the stable game-level `homeTeamWon` flag. Score state overrides that flag for one visible conflict, game `2024020002`, where four goal events establish NJD 3–1 BUF while the flag incorrectly says away win.

## Outcomes

Season `2023`: 752 home wins, 648 away wins, 205 overtime decisions, and 83 shootout decisions. Season `2024`: 790 home wins, 608 away wins, 213 overtime decisions, and 78 shootout decisions. Rates are descriptive only.

Raw periods distinguish regulation from extra time; tied extra-time score plus winner flag identifies shootouts. All remaining extra-time decisions are overtime. No final tie remains after neutral settlement.

## Contract and boundary

Natural grain is one row per game; identity is `canonical_season + game_id`; ordering is season then game ID. Final goals, winner, differential, total, and decision type are outcome-only. Team strength, rest, goalie, and lineup concepts are not constructed here and require strict-prior timing certification.

This certification unlocks exactly one bounded strict-prior team/goalie feature-spine construction task on these 2,798 games. Prices, odds acquisition, baseline creation, training, fitting, ROI, production changes, and restart remain blocked or unauthorized.
