# Normal completed-game outcome pipeline trace

`insert_mlb_stat_derived.py` selects StatsAPI schedule games whose `detailedState` is exactly `Final` (and optionally an accepted in-season `gameType`). For each final `gamePk`, it fetches `/api/v1.1/game/{game_pk}/feed/live` and `/api/v1/game/{game_pk}/boxscore`.

It iterates both teams' boxscore player maps, keys players by the official person ID and game by `gamePk`, and writes `mlb.player_stats` with primary key `(player_id, game_id)`. The upsert updates game/team/position and batting/pitching statistics on conflict. Stored batting fields include AB, hits, total bases, RBI, runs, strikeouts, walks, singles, doubles, triples, home runs and stolen bases; PA and its components are filled by the separate completed-slate PA refresh.

Players are written when the official boxscore provides a nonempty batting stats object or the player is a pitcher. Bench players who never appeared and have no batting/pitching stats are not written. Zero-PA substitutes can receive rows when the official boxscore provides a batting stats object. A missing row is therefore not sufficient evidence of nonappearance.

The normal path attempts to record every official appearance represented by batting or pitching stats, but the August 2 complete-game comparison found missing appearance rows (see `normal_game_participant_coverage.csv`). It does not record every rostered nonparticipant. Dates may be skipped when `mlb_api` derived rows already exist, while the wrapper's completed-slate gate tests date-level presence rather than source correction freshness. Therefore an existing local row can be stale relative to a later official correction; explicit `MLB_STAT_SKIP_EXISTING_DATES=0` reprocessing is required to refresh it.

No `mlb.player_stats` row was modified by this audit.
