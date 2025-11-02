\pset format csv
\pset footer off
\pset tuples_only on

-- Export historical training set (last 3 years) with label y_points and rolling features.
COPY (
WITH logs AS (
  SELECT
    l.player_id,
    l.game_id,
    l.team_id,
    (l.team_id = g.home_team_id) AS is_home,
    CASE
      WHEN l.team_id = g.home_team_id THEN g.away_team_id
      WHEN l.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    g.game_date,
    COALESCE(l.points, COALESCE(l.goals,0)+COALESCE(l.assists,0))::int AS points,
    COALESCE(l.shots_on_goal,0)::float AS sog,
    COALESCE(l.shot_attempts,0)::float AS attempts,
    NULLIF(l.toi_minutes,0)::float AS toi_min,
    NULLIF(l.pp_toi_minutes,0)::float AS pp_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date >= CURRENT_DATE - INTERVAL '3 years'
),
roll AS (
  SELECT
    player_id, game_id, team_id, opponent_id, is_home, game_date, points,
    AVG(points::float)   OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING)  AS d5_points_avg,
    AVG(points::float)   OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)  AS d10_points_avg,
    AVG(sog)             OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)  AS d10_sog_avg,
    AVG(attempts)        OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)  AS d10_attempts_avg,
    AVG(toi_min)         OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)  AS d10_toi_min_avg,
    AVG(pp_min)          OVER (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)  AS d10_pp_min_avg
  FROM logs
),
team_pp AS (
  SELECT DISTINCT
    team_id,
    game_id,
    game_date,
    AVG(pp_min) OVER (PARTITION BY team_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_team_pp_min
  FROM logs
),
team_goals AS (
  -- goals_for per (game_id, team_id) from whichever table has it
  SELECT
    g.game_id,
    g.game_date,
    t.team_id,
    COALESCE(
      (SELECT SUM(sp.goals)::int
         FROM nhl.skater_points_raw sp
        WHERE sp.game_id = g.game_id AND sp.team_id = t.team_id),
      (SELECT SUM(COALESCE(sgl.goals,0))::int
         FROM nhl.skater_game_logs_raw sgl
        WHERE sgl.game_id = g.game_id AND sgl.team_id = t.team_id),
      0
    ) AS goals_for
  FROM nhl.games g
  JOIN LATERAL (VALUES (g.home_team_id),(g.away_team_id)) AS t(team_id) ON TRUE
),
opp_allowed AS (
  -- opponent goals-for in the same game → rolling “goals allowed” for my team
  SELECT
    my.team_id,
    my.game_id,
    my.game_date,
    (AVG(opp.goals_for) OVER (
       PARTITION BY my.team_id
       ORDER BY my.game_date, my.game_id
       ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
     ))::float AS opp_d5_goals_allowed_avg
  FROM team_goals my
  JOIN team_goals opp
    ON opp.game_id = my.game_id
   AND opp.team_id <> my.team_id
)
SELECT
  r.player_id,
  r.game_id,
  r.team_id,
  r.opponent_id,
  (r.is_home)::int AS is_home,
  r.game_date,
  COALESCE(r.d5_points_avg,       0.0) AS d5_points_avg,
  COALESCE(r.d10_points_avg,      0.0) AS d10_points_avg,
  COALESCE(r.d10_sog_avg,         0.0) AS d10_sog_avg,
  COALESCE(r.d10_attempts_avg,    0.0) AS d10_attempts_avg,
  COALESCE(r.d10_toi_min_avg,     0.0) AS d10_toi_min_avg,
  r.d10_pp_min_avg                         AS d10_pp_min_avg,        -- keep NULL if no PP history
  COALESCE(tp.d10_team_pp_min,     0.0) AS d10_team_pp_min,
  COALESCE(oa.opp_d5_goals_allowed_avg, 0.0) AS opp_d5_goals_allowed_avg,
  COALESCE(r.points, 0)              AS y_points
FROM roll r
LEFT JOIN team_pp     tp ON tp.team_id = r.team_id AND tp.game_id = r.game_id
LEFT JOIN opp_allowed oa ON oa.team_id = r.team_id AND oa.game_id = r.game_id
WHERE r.points IS NOT NULL
ORDER BY r.game_date, r.player_id, r.game_id
) TO STDOUT WITH CSV HEADER;
