\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Usage:
-- psql --no-psqlrc -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD \
--   -f backend/nhl/sql/seed_goalie_features_for_slate.sql "$SUPABASE_DB_URL"

WITH params AS (SELECT :'slate_date'::date AS d),

-- Goalies from roster_status for the slate date
roster_g AS (
  SELECT
    r.player_id,
    r.team_id,
    r.game_id,
    r.game_date::date AS game_date
  FROM nhl.roster_status r, params p
  WHERE r.game_date::date = p.d
    AND r.position = 'G'           -- adjust if your schema differs
),

-- Derive opponent_id and is_home from nhl.games
slate_goalies AS (
  SELECT DISTINCT
    rg.player_id,
    rg.team_id,
    CASE
      WHEN rg.team_id = g.home_team_id THEN g.away_team_id
      ELSE g.home_team_id
    END AS opponent_id,
    (rg.team_id = g.home_team_id) AS is_home,
    rg.game_id,
    rg.game_date
  FROM roster_g rg
  JOIN nhl.games g ON g.game_id = rg.game_id
),

-- Latest **pre-slate** rolling features per goalie
feat_roll AS (
  SELECT
    s.player_id,
    fr.d5_shots_faced_per60,
    fr.d5_saves_per60,
    fr.d10_shots_faced_per60,
    fr.d10_saves_per60,
    fr.d10_save_pct,
    fr.d20_saves_per60
  FROM slate_goalies s
  LEFT JOIN LATERAL (
    SELECT
      m.d5_shots_faced_per60,
      m.d5_saves_per60,
      m.d10_shots_faced_per60,
      m.d10_saves_per60,
      m.d10_save_pct,
      -- present if your MV has it; otherwise will be NULL
      m.d20_saves_per60
    FROM nhl.goalie_roll_feats_m m, params p
    WHERE m.player_id = s.player_id
      AND m.game_date < p.d
    ORDER BY m.game_date DESC, m.game_id DESC
    LIMIT 1
  ) fr ON TRUE
),

-- Team environment (pace) on the slate date
team_env AS (
  SELECT
    t.team_id,
    t.game_date::date AS game_date,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.pace_index
  FROM nhl.tf_team_roll10 t, params p
  WHERE t.game_date::date = p.d
)

INSERT INTO nhl.training_features_goalie_saves_v2 AS tgt
  (player_id, game_id, team_id, opponent_id, is_home, game_date,
   d5_shots_faced_per60, d5_saves_per60,
   d10_shots_faced_per60, d10_saves_per60, d10_save_pct,
   d20_saves_per60,
   team_d10_sf_per_game, opp_d10_sf_allowed_per_game, pace_index)
SELECT
  s.player_id,
  s.game_id,
  s.team_id,
  s.opponent_id,
  s.is_home,
  s.game_date,
  f.d5_shots_faced_per60,
  f.d5_saves_per60,
  f.d10_shots_faced_per60,
  f.d10_saves_per60,
  f.d10_save_pct,
  f.d20_saves_per60,
  e.team_d10_sf_per_game,
  e.opp_d10_sf_allowed_per_game,
  e.pace_index
FROM slate_goalies s
LEFT JOIN feat_roll f ON f.player_id = s.player_id
LEFT JOIN team_env  e ON e.team_id   = s.team_id AND e.game_date = s.game_date
ON CONFLICT (player_id, game_id) DO UPDATE
SET d5_shots_faced_per60        = COALESCE(EXCLUDED.d5_shots_faced_per60,        tgt.d5_shots_faced_per60),
    d5_saves_per60              = COALESCE(EXCLUDED.d5_saves_per60,              tgt.d5_saves_per60),
    d10_shots_faced_per60       = COALESCE(EXCLUDED.d10_shots_faced_per60,       tgt.d10_shots_faced_per60),
    d10_saves_per60             = COALESCE(EXCLUDED.d10_saves_per60,             tgt.d10_saves_per60),
    d10_save_pct                = COALESCE(EXCLUDED.d10_save_pct,                tgt.d10_save_pct),
    d20_saves_per60             = COALESCE(EXCLUDED.d20_saves_per60,             tgt.d20_saves_per60),
    team_d10_sf_per_game        = COALESCE(EXCLUDED.team_d10_sf_per_game,        tgt.team_d10_sf_per_game),
    opp_d10_sf_allowed_per_game = COALESCE(EXCLUDED.opp_d10_sf_allowed_per_game, tgt.opp_d10_sf_allowed_per_game),
    pace_index                  = COALESCE(EXCLUDED.pace_index,                  tgt.pace_index);
