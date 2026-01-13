-- ==========================================================
-- This is the only PP role fill file called by cli.py.
-- ==========================================================

-- backend/nhl/sql/fill_sog_pp_role_for_slate.sql
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date='2025-12-23' -f backend/nhl/sql/fill_sog_pp_role_for_slate.sql

\set ON_ERROR_STOP on

\set pregame_table nhl.training_features_nhl_sog_enriched_pregame_v2

ALTER TABLE :pregame_table
  ADD COLUMN IF NOT EXISTS pp_role_share_final numeric,
  ADD COLUMN IF NOT EXISTS pp_role_source text;

WITH
p AS (
  SELECT player_id, game_id, team_id, game_date, role_pp_share
  FROM :pregame_table
  WHERE game_date = :'slate_date'
),

-- ✅ FIX: pp_toi_minutes (not pp_toi_min)
player_pp AS (
  SELECT
    l.player_id,
    l.game_id,
    l.team_id,
    NULLIF(l.pp_toi_minutes, 0)::numeric AS pp_toi_min
  FROM nhl.skater_game_logs_raw l
  WHERE l.game_date = :'slate_date'
),

team_pp AS (
  SELECT
    l.game_id,
    l.team_id,
    SUM(NULLIF(l.pp_toi_minutes, 0))::numeric AS team_pp_toi_min
  FROM nhl.skater_game_logs_raw l
  WHERE l.game_date = :'slate_date'
  GROUP BY 1,2
),

u AS (
  SELECT
    p.player_id,
    p.game_id,
    p.team_id,
    CASE
      WHEN tp.team_pp_toi_min IS NULL OR tp.team_pp_toi_min = 0 THEN NULL
      WHEN pp.pp_toi_min IS NULL THEN NULL
      ELSE (pp.pp_toi_min / tp.team_pp_toi_min)
    END AS unit_pp_share
  FROM p
  LEFT JOIN player_pp pp
    ON pp.player_id = p.player_id
   AND pp.game_id   = p.game_id
   AND pp.team_id   = p.team_id
  LEFT JOIN team_pp tp
    ON tp.game_id = p.game_id
   AND tp.team_id = p.team_id
)

UPDATE :pregame_table t
SET
  pp_role_share_final = COALESCE(u.unit_pp_share, t.role_pp_share, 0),
  pp_role_source      = CASE
    WHEN u.unit_pp_share IS NOT NULL THEN 'unit'
    WHEN t.role_pp_share IS NOT NULL THEN 'role'
    ELSE 'zero'
  END
FROM u
WHERE t.game_date  = :'slate_date'
  AND t.player_id  = u.player_id
  AND t.game_id    = u.game_id
  AND t.team_id    = u.team_id;

SELECT
  game_date,
  COUNT(*)                                               AS rows_total,
  COUNT(*) FILTER (WHERE pp_role_share_final IS NOT NULL) AS rows_with_final,
  COUNT(*) FILTER (WHERE pp_role_share_final IS NULL)     AS rows_missing_final,
  COUNT(*) FILTER (WHERE pp_role_source = 'unit')         AS n_unit,
  COUNT(*) FILTER (WHERE pp_role_source = 'role')         AS n_role,
  COUNT(*) FILTER (WHERE pp_role_source = 'zero')         AS n_zero
FROM :pregame_table
WHERE game_date = :'slate_date'
GROUP BY 1
ORDER BY 1;
