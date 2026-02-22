-- Rich, self-contained features for points (train/infer). Relies only on skater_game_logs_raw + games/roster_status.
-- Emits one row per (player_id, game_id) on a given :slate_date when filtered, or whole history when not.

WITH base AS (
  SELECT
    rs.player_id,
    rs.game_id,
    g.game_date,
    rs.team_id,
    CASE WHEN rs.team_id = g.home_team_id THEN g.away_team_id
         WHEN rs.team_id = g.away_team_id THEN g.home_team_id
         ELSE NULL END AS opponent_id,
    (rs.team_id = g.home_team_id) AS is_home
  FROM nhl.roster_status rs
  JOIN nhl.games g USING (game_id)
),
hist AS (
  SELECT
    l.player_id, l.game_id, l.game_date,
    COALESCE(l.points, COALESCE(l.goals,0)+COALESCE(l.assists,0))::int AS points,
    COALESCE(l.shots_on_goal,0)::int AS sog,
    COALESCE(l.shot_attempts,0)::int AS attempts,
    NULLIF(l.toi_minutes,0)::float AS toi_min,
    NULLIF(l.pp_toi_minutes,0)::float AS pp_min
  FROM nhl.skater_game_logs_raw l
),
feat AS (
  SELECT
    b.player_id, b.game_id, b.team_id, b.opponent_id, b.is_home, b.game_date,
    -- prior N rolling (exclude current)
    AVG(h.points)::float    FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lifetime_points_avg, -- broad prior
    AVG(h.points)::float    FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING)  AS d5_points_avg,
    AVG(h.points)::float    FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 10 PRECEDING) AS d10_points_avg,
    AVG(h.sog)::float       FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 10 PRECEDING) AS d10_sog_avg,
    AVG(h.attempts)::float  FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 10 PRECEDING) AS d10_attempts_avg,
    AVG(h.toi_min)::float   FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 10 PRECEDING) AS d10_toi_min_avg,
    AVG(h.pp_min)::float    FILTER (WHERE h.game_date < b.game_date ORDER BY h.game_date DESC ROWS BETWEEN 1 PRECEDING AND 10 PRECEDING) AS d10_pp_min_avg
  FROM base b
  LEFT JOIN hist h ON h.player_id = b.player_id
  GROUP BY b.player_id, b.game_id, b.team_id, b.opponent_id, b.is_home, b.game_date
)
SELECT
  player_id, game_id, team_id, opponent_id, is_home::int AS is_home, game_date,
  COALESCE(d5_points_avg, 0.0)    AS d5_points_avg,
  COALESCE(d10_points_avg, 0.0)   AS d10_points_avg,
  COALESCE(d10_sog_avg, 0.0)      AS d10_sog_avg,
  COALESCE(d10_attempts_avg, 0.0) AS d10_attempts_avg,
  COALESCE(d10_toi_min_avg, 0.0)  AS d10_toi_min_avg,
  COALESCE(d10_pp_min_avg, 0.0)   AS d10_pp_min_avg,
  -- training label when available
  (SELECT COALESCE(points,0) FROM nhl.skater_game_logs_raw x
    WHERE x.player_id=feat.player_id AND x.game_id=feat.game_id) AS points
FROM feat
ORDER BY player_id, game_id;
