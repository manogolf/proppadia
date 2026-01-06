-- Step 1b: compute + fill player PP role into
-- nhl.training_features_nhl_sog_enriched_pregame_v2_denali_cols.role_pp_share
-- backend/nhl/sql/fill_sog_role_pp_share_for_slate.sql
-- Definition (robust + simple):
--   role_pp_share = avg over last 10 games of:
--       (player_pp_toi_minutes / team_sum_pp_toi_minutes_in_that_game)
-- where team_sum_pp_toi_minutes_in_that_game = SUM(pp_toi_minutes) across skaters for that (game_id, team_id).
--
-- Usage:
--   psql ... -v slate_date='2025-12-27' -f backend/nhl/sql/fill_sog_role_pp_share_for_slate.sql

\set ON_ERROR_STOP on

WITH
-- team total PP TOI per game (in player-minutes; i.e., roughly 5 * team PP minutes)
team_pp AS (
  SELECT
    l.game_id,
    l.team_id,
    SUM(COALESCE(l.pp_toi_minutes, 0))::numeric AS team_sum_pp_toi_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date < :'slate_date'::date
  GROUP BY 1,2
),

-- player PP share per past game (only where team total > 0)
player_pp_share_by_game AS (
  SELECT
    l.player_id,
    l.team_id,
    g.game_date,
    (COALESCE(l.pp_toi_minutes, 0)::numeric / NULLIF(tp.team_sum_pp_toi_min, 0))::numeric AS pp_share_game
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  JOIN team_pp tp
    ON tp.game_id = l.game_id
   AND tp.team_id = l.team_id
  WHERE g.game_date < :'slate_date'::date
    AND tp.team_sum_pp_toi_min > 0
),

-- rolling avg of last 10 valid PP-share games per player/team
pp_role AS (
  SELECT
    s.player_id,
    s.team_id,
    AVG(s.pp_share_game)::numeric AS role_pp_share,
    COUNT(*) AS n_games_used
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (PARTITION BY p.player_id, p.team_id ORDER BY p.game_date DESC) AS rn
    FROM player_pp_share_by_game p
  ) s
  WHERE s.rn <= 10
  GROUP BY 1,2
)

UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2_denali_cols tgt
SET role_pp_share = pr.role_pp_share
FROM pp_role pr
WHERE tgt.game_date = :'slate_date'::date
  AND tgt.player_id = pr.player_id
  AND tgt.team_id   = pr.team_id
  AND pr.n_games_used >= 3;  -- avoid noisy roles for brand-new histories
