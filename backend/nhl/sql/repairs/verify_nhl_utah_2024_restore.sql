-- Read-only check for NHL Backlog P2.57 (2024-25 Utah games restore). Run before and after
-- the editor blocks. Expected before: 82 games, 0 dated, 0 skater rows, 0 goalie rows,
-- player 8484910 absent. Expected after the games block: 82 dated. Expected after the
-- player/logs block: 2952 skater rows, 172 goalie rows, 0 off-team rows, 0 PP above TOI,
-- player 8484910 present.
WITH utah AS (
  SELECT game_id, game_date, start_time_utc, home_team_id, away_team_id
    FROM nhl.games
   WHERE season = 2024 AND game_type = 2 AND 'UTA' IN (home_team_code, away_team_code)
)
SELECT
  (SELECT count(*) FROM utah) AS utah_games,
  (SELECT count(*) FROM utah WHERE game_date IS NOT NULL AND start_time_utc IS NOT NULL) AS utah_games_dated,
  (SELECT count(*) FROM nhl.skater_game_logs_raw WHERE game_id IN (SELECT game_id FROM utah)) AS skater_rows,
  (SELECT count(*) FROM nhl.goalie_game_logs_raw WHERE game_id IN (SELECT game_id FROM utah)) AS goalie_rows,
  (SELECT count(*) FROM nhl.skater_game_logs_raw l JOIN utah u USING (game_id)
    WHERE l.team_id NOT IN (u.home_team_id, u.away_team_id)) AS skater_rows_off_team,
  (SELECT count(*) FROM nhl.skater_game_logs_raw l JOIN utah u USING (game_id)
    WHERE l.pp_toi_minutes > l.toi_minutes) AS skater_rows_pp_above_toi,
  (SELECT count(*) FROM nhl.skater_game_logs_raw l JOIN utah u USING (game_id)
    WHERE l.pp_toi_minutes IS NULL) AS skater_rows_pp_null,
  (SELECT count(*) FROM nhl.players WHERE player_id = 8484910) AS player_8484910_present;
