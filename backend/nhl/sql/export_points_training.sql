-- Export historical training set (last 3 years) with label `points` and rich rolling features.

WITH base AS (
  SELECT
    l.player_id,
    l.game_id,
    g.game_date,
    l.team_id,
    CASE
      WHEN l.team_id = g.home_team_id THEN g.away_team_id
      WHEN l.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    (l.team_id = g.home_team_id) AS is_home
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date >= CURRENT_DATE - INTERVAL '3 years'
),
cur AS (
  SELECT
    l.player_id,
    l.game_id,
    l.game_date,
    COALESCE(l.points, COALESCE(l.goals,0)+COALESCE(l.assists,0))::int AS points,
    COALESCE(l.shots_on_goal,0)::int AS sog,
    COALESCE(l.shot_attempts,0)::int AS attempts,
    NULLIF(l.toi_minutes,0)::float AS toi_min,
    NULLIF(l.pp_toi_minutes,0)::float AS pp_min
  FROM nhl.skater_game_logs_raw l
  WHERE l.game_date >= CURRENT_DATE - INTERVAL '3 years'
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
  ) x)                                    AS d10_pp_min_avg,
  /* training label (current game’s points) */
  COALESCE(c.points,0)                    AS points
FROM base b
LEFT JOIN cur c
  ON c.player_id = b.player_id
 AND c.game_id   = b.game_id
WHERE c.points IS NOT NULL
ORDER BY b.game_date, b.player_id, b.game_id;
