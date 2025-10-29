-- backend/nhl/sql/export_points.sql
-- Exports today's skater rows with lightweight features for training/serving NHL Points models.
-- Input: :CURRENT_DATE (ET)
-- Output: CSV via \copy TO STDOUT WITH CSV HEADER from the CLI.

WITH g AS (
  SELECT game_id, game_date, home_team_id, away_team_id
  FROM nhl.games
WHERE game_date = CURRENT_DATE
),
rs AS (
  SELECT r.game_id, r.team_id, r.player_id
  FROM nhl.roster_status r
  WHERE r.game_id IN (SELECT game_id FROM g)
),
base AS (
  SELECT
    rs.player_id,
    rs.game_id,
    rs.team_id,
    CASE WHEN rs.team_id = g.home_team_id THEN g.away_team_id
         WHEN rs.team_id = g.away_team_id THEN g.home_team_id
         ELSE NULL END AS opponent_id,
    (rs.team_id = g.home_team_id) AS is_home,
    g.game_date
  FROM rs
  JOIN g ON g.game_id = rs.game_id
)
SELECT
  b.player_id,
  b.game_id,
  b.team_id,
  b.opponent_id,
  b.is_home::int AS is_home,
  b.game_date,
  /* rolling form (exclude current game) */
  (SELECT AVG(x.points) FROM (
     SELECT COALESCE(l2.points, COALESCE(l2.goals,0)+COALESCE(l2.assists,0))::float AS points
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 5
  ) x)                                    AS d5_points_avg,
  (SELECT AVG(x.points) FROM (
     SELECT COALESCE(l2.points, COALESCE(l2.goals,0)+COALESCE(l2.assists,0))::float AS points
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 10
  ) x)                                    AS d10_points_avg,
  (SELECT AVG(x.sog) FROM (
     SELECT COALESCE(l2.shots_on_goal,0)::float AS sog
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 10
  ) x)                                    AS d10_sog_avg,
  (SELECT AVG(x.attempts) FROM (
     SELECT COALESCE(l2.shot_attempts,0)::float AS attempts
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 10
  ) x)                                    AS d10_attempts_avg,
  (SELECT AVG(x.toi_min) FROM (
     SELECT NULLIF(l2.toi_minutes,0)::float AS toi_min
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 10
  ) x)                                    AS d10_toi_min_avg,
  (SELECT AVG(x.pp_min) FROM (
     SELECT NULLIF(l2.pp_toi_minutes,0)::float AS pp_min
     FROM nhl.skater_game_logs_raw l2
     WHERE l2.player_id = b.player_id AND l2.game_date < b.game_date
     ORDER BY l2.game_date DESC, l2.game_id DESC
     LIMIT 10
  ) x)                                    AS d10_pp_min_avg
FROM base b
ORDER BY b.player_id, b.game_id;
