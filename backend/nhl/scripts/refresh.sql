-- scripts/refresh.sql
-- Daily refresh: fill SOG context, goalie cadence & season %, refresh ready MVs, and log an audit.

-- Bump timeouts for long rollups/refreshes (Supabase default ~2m)

SET statement_timeout = '10min';
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = '5min';

BEGIN;

-------------------------------------------------------------------------------
-- A0) Team roll-10 from goalie logs (preferred) with fallback to tf_team_game_sog
-------------------------------------------------------------------------------

-- Detect goalie shots column for A0
SELECT column_name AS shotscol
FROM information_schema.columns
WHERE table_schema='nhl' AND table_name='v_goalie_game_logs_played'
  AND column_name IN ('shots_faced','shots_against','sa','shots')
ORDER BY CASE column_name WHEN 'shots_faced' THEN 1 WHEN 'shots_against' THEN 2 WHEN 'sa' THEN 3 ELSE 4 END
LIMIT 1;
\gset

\if :{?shotscol}
DROP MATERIALIZED VIEW IF EXISTS nhl.team_roll10_m;
CREATE MATERIALIZED VIEW nhl.team_roll10_m AS
WITH g AS (
  SELECT
    gl.game_id,
    gl.game_date::date AS game_date,
    gl.team_id,
    gl.opponent_id,
    COALESCE(gl.:shotscol,0)::numeric AS shots_faced
  FROM nhl.v_goalie_game_logs_played gl
),
team_sa AS (  -- shots ALLOWED by team (own goalies faced)
  SELECT team_id, game_id, game_date, SUM(shots_faced) AS sa_per_game
  FROM g GROUP BY 1,2,3
),
team_sf AS (  -- shots FOR by team (opponent goalies faced)
  SELECT opponent_id AS team_id, game_id, game_date, SUM(shots_faced) AS sf_per_game
  FROM g GROUP BY 1,2,3
),
base AS (
  SELECT
    COALESCE(sa.team_id, sf.team_id)     AS team_id,
    COALESCE(sa.game_id, sf.game_id)     AS game_id,
    COALESCE(sa.game_date, sf.game_date) AS game_date,
    sa.sa_per_game,
    sf.sf_per_game
  FROM team_sa sa
  FULL JOIN team_sf sf USING (team_id, game_id, game_date)
)
SELECT
  team_id,
  game_id,
  game_date,
  AVG(sf_per_game) OVER (PARTITION BY team_id ORDER BY game_date, game_id
                         ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_d10_sf_per_game,
  AVG(sa_per_game) OVER (PARTITION BY team_id ORDER BY game_date, game_id
                         ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_d10_sf_allowed_per_game
FROM base
ORDER BY team_id, game_date, game_id;

CREATE UNIQUE INDEX IF NOT EXISTS team_roll10_m_uniq
  ON nhl.team_roll10_m (team_id, game_date, game_id);

REFRESH MATERIALIZED VIEW nhl.team_roll10_m;

\else
-- Fallback: use tf_team_game_sog if goalie logs not available (staler, but safe)
DROP MATERIALIZED VIEW IF EXISTS nhl.team_roll10_m;
CREATE MATERIALIZED VIEW nhl.team_roll10_m AS
WITH games AS (
  SELECT
    t.team_id,
    t.opponent_id,
    t.game_id,
    t.game_date::date AS game_date,
    t.team_sog::numeric AS team_sog,
    o.team_sog::numeric AS opp_sog_in_this_game
  FROM nhl.tf_team_game_sog t
  JOIN nhl.tf_team_game_sog o
    ON o.game_id=t.game_id AND o.team_id=t.opponent_id AND o.opponent_id=t.team_id
),
roll AS (
  SELECT
    team_id,
    game_id,
    game_date,
    CASE WHEN COUNT(team_sog) OVER w10 > 0
         THEN AVG(team_sog) OVER w10 END AS team_d10_sf_per_game,
    CASE WHEN COUNT(opp_sog_in_this_game) OVER w10 > 0
         THEN AVG(opp_sog_in_this_game) OVER w10 END AS opp_d10_sf_allowed_per_game
  FROM games
  WINDOW w10 AS (
    PARTITION BY team_id
    ORDER BY game_date, game_id
    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
  )
)
SELECT team_id, game_id, game_date, team_d10_sf_per_game, opp_d10_sf_allowed_per_game
FROM roll
ORDER BY team_id, game_date, game_id;

CREATE UNIQUE INDEX IF NOT EXISTS team_roll10_m_uniq
  ON nhl.team_roll10_m (team_id, game_date, game_id);

REFRESH MATERIALIZED VIEW nhl.team_roll10_m;
\endif

-- Upsert roll-10 into consumer table + pace_index
INSERT INTO nhl.tf_team_roll10 AS t
  (team_id, game_date, team_d10_sf_per_game, opp_d10_sf_allowed_per_game)
SELECT team_id, game_date, team_d10_sf_per_game, opp_d10_sf_allowed_per_game
FROM nhl.team_roll10_m
ON CONFLICT (team_id, game_date) DO UPDATE
SET team_d10_sf_per_game        = EXCLUDED.team_d10_sf_per_game,
    opp_d10_sf_allowed_per_game = EXCLUDED.opp_d10_sf_allowed_per_game;

UPDATE nhl.tf_team_roll10 t
SET pace_index = CASE
  WHEN t.team_d10_sf_per_game IS NULL OR t.opp_d10_sf_allowed_per_game IS NULL THEN NULL
  ELSE sqrt(t.team_d10_sf_per_game * t.opp_d10_sf_allowed_per_game) END
WHERE t.pace_index IS DISTINCT FROM CASE
  WHEN t.team_d10_sf_per_game IS NULL OR t.opp_d10_sf_allowed_per_game IS NULL THEN NULL
  ELSE sqrt(t.team_d10_sf_per_game * t.opp_d10_sf_allowed_per_game) END;

-------------------------------------------------------------------------------
-- A1) Goalie rolling features (d5/d10 per-60 + d10_save_pct), leakage-safe
-------------------------------------------------------------------------------

-- Detect goalie cols for A1
SELECT column_name AS g_shotscol
FROM information_schema.columns
WHERE table_schema='nhl' AND table_name='v_goalie_game_logs_played'
  AND column_name IN ('shots_faced','shots_against','sa','shots')
ORDER BY CASE column_name WHEN 'shots_faced' THEN 1 WHEN 'shots_against' THEN 2 WHEN 'sa' THEN 3 ELSE 4 END
LIMIT 1;
\gset

SELECT column_name AS g_savescol
FROM information_schema.columns
WHERE table_schema='nhl' AND table_name='v_goalie_game_logs_played'
  AND column_name IN ('saves','sv')
ORDER BY CASE column_name WHEN 'saves' THEN 1 ELSE 2 END
LIMIT 1;
\gset

SELECT column_name AS g_toicol
FROM information_schema.columns
WHERE table_schema='nhl' AND table_name='v_goalie_game_logs_played'
  AND column_name IN ('toi_minutes','time_on_ice_minutes','toi','time_on_ice','minutes','min_played')
ORDER BY 1
LIMIT 1;
\gset

\if :{?g_shotscol}
\if :{?g_savescol}

DROP MATERIALIZED VIEW IF EXISTS nhl.goalie_roll_feats_m;

\if :{?g_toicol}
CREATE MATERIALIZED VIEW nhl.goalie_roll_feats_m AS
WITH gl AS (
  SELECT
    gl.player_id, gl.game_id, gl.team_id, gl.opponent_id, gl.is_home,
    gl.game_date::date AS game_date,
    COALESCE(gl.:g_shotscol,0)::numeric AS sf,
    COALESCE(gl.:g_savescol,0)::numeric AS sv,
    COALESCE(NULLIF(gl.:g_toicol,0),60)::numeric AS toi
  FROM nhl.v_goalie_game_logs_played gl
),
roll AS (
  SELECT
    player_id, game_id, team_id, opponent_id, is_home, game_date,
    SUM(sf)  OVER w5  AS sf_d5,
    SUM(sv)  OVER w5  AS sv_d5,
    SUM(toi) OVER w5  AS toi_d5,
    SUM(sf)  OVER w10 AS sf_d10,
    SUM(sv)  OVER w10 AS sv_d10,
    SUM(toi) OVER w10 AS toi_d10
  FROM gl
  WINDOW
    w5  AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING),
    w10 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)
)
SELECT
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  CASE WHEN toi_d5  > 0 THEN 60*sf_d5 /toi_d5  END AS d5_shots_faced_per60,
  CASE WHEN toi_d5  > 0 THEN 60*sv_d5 /toi_d5  END AS d5_saves_per60,
  CASE WHEN toi_d10 > 0 THEN 60*sf_d10/toi_d10 END AS d10_shots_faced_per60,
  CASE WHEN toi_d10 > 0 THEN 60*sv_d10/toi_d10 END AS d10_saves_per60,
  CASE WHEN sf_d10  > 0 THEN    sv_d10/sf_d10 END AS d10_save_pct
FROM roll
ORDER BY game_date, player_id, game_id;
\else
CREATE MATERIALIZED VIEW nhl.goalie_roll_feats_m AS
WITH gl AS (
  SELECT
    gl.player_id, gl.game_id, gl.team_id, gl.opponent_id, gl.is_home,
    gl.game_date::date AS game_date,
    COALESCE(gl.:g_shotscol,0)::numeric AS sf,
    COALESCE(gl.:g_savescol,0)::numeric AS sv,
    60::numeric AS toi
  FROM nhl.v_goalie_game_logs_played gl
),
roll AS (
  SELECT
    player_id, game_id, team_id, opponent_id, is_home, game_date,
    SUM(sf)  OVER w5  AS sf_d5,
    SUM(sv)  OVER w5  AS sv_d5,
    SUM(toi) OVER w5  AS toi_d5,
    SUM(sf)  OVER w10 AS sf_d10,
    SUM(sv)  OVER w10 AS sv_d10,
    SUM(toi) OVER w10 AS toi_d10
  FROM gl
  WINDOW
    w5  AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING),
    w10 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)
)
SELECT
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  CASE WHEN toi_d5  > 0 THEN 60*sf_d5 /toi_d5  END AS d5_shots_faced_per60,
  CASE WHEN toi_d5  > 0 THEN 60*sv_d5 /toi_d5  END AS d5_saves_per60,
  CASE WHEN toi_d10 > 0 THEN 60*sf_d10/toi_d10 END AS d10_shots_faced_per60,
  CASE WHEN toi_d10 > 0 THEN 60*sv_d10/toi_d10 END AS d10_saves_per60,
  CASE WHEN sf_d10  > 0 THEN    sv_d10/sf_d10 END AS d10_save_pct
FROM roll
ORDER BY game_date, player_id, game_id;
\endif

CREATE UNIQUE INDEX IF NOT EXISTS goalie_roll_feats_m_uniq
  ON nhl.goalie_roll_feats_m (player_id, game_id);

REFRESH MATERIALIZED VIEW nhl.goalie_roll_feats_m;

INSERT INTO nhl.training_features_goalie_saves_v2 AS t
  (player_id, game_id, team_id, opponent_id, is_home, game_date,
   d5_shots_faced_per60, d5_saves_per60, d10_shots_faced_per60, d10_saves_per60, d10_save_pct)
SELECT
  m.player_id, m.game_id, m.team_id, m.opponent_id, m.is_home, m.game_date,
  m.d5_shots_faced_per60, m.d5_saves_per60, m.d10_shots_faced_per60, m.d10_saves_per60, m.d10_save_pct
FROM nhl.goalie_roll_feats_m m
WHERE m.game_date >= current_date - interval '60 days'
ON CONFLICT (player_id, game_id) DO UPDATE
SET d5_shots_faced_per60  = EXCLUDED.d5_shots_faced_per60,
    d5_saves_per60        = EXCLUDED.d5_saves_per60,
    d10_shots_faced_per60 = EXCLUDED.d10_shots_faced_per60,
    d10_saves_per60       = EXCLUDED.d10_saves_per60,
    d10_save_pct          = EXCLUDED.d10_save_pct;

\else
\echo 'ERROR[A1]: missing saves column on nhl.v_goalie_game_logs_played (tried saves/sv)'
\endif
\else
\echo 'ERROR[A1]: missing shots column on nhl.v_goalie_game_logs_played (tried shots_faced/shots_against/sa/shots)'
\endif

-------------------------------------------------------------------------------
-- B) Goalie rest_days / b2b from previous appearance
-------------------------------------------------------------------------------
WITH appearances AS (
  SELECT
    g.player_id,
    g.game_id,
    gm.game_date::date AS game_date,
    LAG(gm.game_date::date) OVER (
      PARTITION BY g.player_id
      ORDER BY gm.game_date, g.game_id
    ) AS prev_game_date
  FROM nhl.v_goalie_game_logs_played g
  JOIN nhl.games gm ON gm.game_id = g.game_id
)
,derived AS (
  SELECT
    player_id,
    game_id,
    CASE WHEN prev_game_date IS NULL THEN NULL
         ELSE GREATEST(0, (game_date - prev_game_date))::int END AS rest_days_goalie,
    CASE WHEN prev_game_date IS NULL THEN NULL
         ELSE ((game_date - prev_game_date) = 1) END AS b2b_flag_goalie
  FROM appearances
)
UPDATE nhl.training_features_goalie_saves_v2 t
SET
  rest_days = COALESCE(t.rest_days, d.rest_days_goalie),
  b2b_flag  = COALESCE(t.b2b_flag,  d.b2b_flag_goalie)
FROM derived d
WHERE t.player_id = d.player_id
  AND t.game_id   = d.game_id
  AND (t.rest_days IS NULL OR t.b2b_flag IS NULL);

-------------------------------------------------------------------------------
-- C) season_save_pct = prev 2 seasons + current season-to-date (pre-game)
-------------------------------------------------------------------------------
WITH base AS (
  SELECT
    g.player_id,
    g.game_id,
    g.game_date::date AS game_date,
    (EXTRACT(YEAR FROM g.game_date)::int
     - CASE WHEN EXTRACT(MONTH FROM g.game_date) < 9 THEN 1 ELSE 0 END
    )::int AS season_start_year,
    COALESCE(g.saves,0)::numeric       AS saves,
    COALESCE(g.shots_faced,0)::numeric AS shots_faced
  FROM nhl.v_goalie_game_logs_played g
),
season_totals AS (
  SELECT player_id, season_start_year,
         SUM(saves) AS season_saves,
         SUM(shots_faced) AS season_shots
  FROM base
  GROUP BY player_id, season_start_year
),
stod AS (
  SELECT
    b.player_id, b.game_id,
    SUM(b.saves)       OVER w_stod AS stod_saves,
    SUM(b.shots_faced) OVER w_stod AS stod_shots,
    b.season_start_year AS curr_season
  FROM base b
  WINDOW w_stod AS (
    PARTITION BY b.player_id, b.season_start_year
    ORDER BY b.game_date, b.game_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  )
),
assembled AS (
  SELECT
    s.player_id, s.game_id,
    COALESCE(s.stod_saves, 0) + COALESCE(p1.season_saves, 0) + COALESCE(p2.season_saves, 0) AS num_saves,
    COALESCE(s.stod_shots, 0) + COALESCE(p1.season_shots, 0) + COALESCE(p2.season_shots, 0) AS den_shots
  FROM stod s
  LEFT JOIN season_totals p1 ON p1.player_id = s.player_id AND p1.season_start_year = s.curr_season - 1
  LEFT JOIN season_totals p2 ON p2.player_id = s.player_id AND p2.season_start_year = s.curr_season - 2
)
UPDATE nhl.training_features_goalie_saves_v2 t
SET season_save_pct = CASE WHEN a.den_shots > 0 THEN a.num_saves / a.den_shots ELSE NULL END
FROM assembled a
WHERE t.player_id = a.player_id AND t.game_id = a.game_id
  AND (t.season_save_pct IS DISTINCT FROM CASE WHEN a.den_shots > 0 THEN a.num_saves / a.den_shots ELSE NULL END);

-------------------------------------------------------------------------------
-- D) Refresh ready MVs
-------------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW nhl.training_features_nhl_sog_v2_ready;
REFRESH MATERIALIZED VIEW nhl.training_features_goalie_saves_v2_ready;

-------------------------------------------------------------------------------
-- E) Data-quality snapshot
-------------------------------------------------------------------------------
WITH sog AS (
  SELECT
    COUNT(*)::bigint AS rows_total,
    COUNT(d10_sog_per60)::bigint AS d10_sog_per60_nn,
    COUNT(attempts_d10_per60)::bigint AS attempts_d10_per60_nn,
    COUNT(team_d10_sf_per_game)::bigint AS team_d10_sf_pg_nn,
    COUNT(opp_d10_sf_allowed_per_game)::bigint AS opp_d10_sf_allowed_pg_nn,
    COUNT(pace_index)::bigint AS pace_index_nn,
    COUNT(role_pp_share)::bigint AS role_pp_share_nn,
    COUNT(rest_days)::bigint AS rest_days_nn,
    COUNT(b2b_flag)::bigint AS b2b_flag_nn,
    COUNT(opp_d10_sf_per60)::bigint AS opp_d10_sf_per60_nn,
    COUNT(team_d10_sa_per60)::bigint AS team_d10_sa_per60_nn,
    COUNT(pace_matchup_index)::bigint AS pace_matchup_index_nn
  FROM nhl.training_features_nhl_sog_v2_ready
),
goal AS (
  SELECT
    COUNT(*)::bigint AS rows_total,
    COUNT(d10_shots_faced_per60)::bigint AS d10_sf60_nn,
    COUNT(d10_save_pct)::bigint AS d10_sv_nn,
    COUNT(team_d10_sf_per_game)::bigint AS team_d10_sf_pg_nn,
    COUNT(opp_d10_sf_allowed_per_game)::bigint AS opp_d10_sf_allowed_pg_nn,
    COUNT(pace_index)::bigint AS pace_idx_nn,
    COUNT(rest_days)::bigint AS rest_days_nn,
    COUNT(b2b_flag)::bigint AS b2b_flag_nn,
    COUNT(d5_saves_per60)::bigint AS d5_sv60_nn,
    COUNT(d10_saves_per60)::bigint AS d10_sv60_nn,
    COUNT(d5_shots_faced_per60)::bigint AS d5_sf60_nn,
    COUNT(season_save_pct)::bigint AS season_sv_nn
  FROM nhl.training_features_goalie_saves_v2_ready
)
INSERT INTO nhl.data_quality_audit (audit_date, check_name, level, result)
SELECT CURRENT_DATE, 'sog_ready_coverage', 'info',
  jsonb_build_object(
    'rows_total', s.rows_total,
    'd10_sog_per60_nn', s.d10_sog_per60_nn,
    'attempts_d10_per60_nn', s.attempts_d10_per60_nn,
    'team_d10_sf_per_game_nn', s.team_d10_sf_pg_nn,
    'opp_d10_sf_allowed_per_game_nn', s.opp_d10_sf_allowed_pg_nn,
    'pace_index_nn', s.pace_index_nn,
    'role_pp_share_nn', s.role_pp_share_nn,
    'rest_days_nn', s.rest_days_nn,
    'b2b_flag_nn', s.b2b_flag_nn,
    'opp_d10_sf_per60_nn', s.opp_d10_sf_per60_nn,
    'team_d10_sa_per60_nn', s.team_d10_sa_per60_nn,
    'pace_matchup_index_nn', s.pace_matchup_index_nn
  )
FROM sog s
UNION ALL
SELECT CURRENT_DATE, 'goalie_ready_coverage', 'info',
  jsonb_build_object(
    'rows_total', g.rows_total,
    'd10_shots_faced_per60_nn', g.d10_sf60_nn,
    'd10_save_pct_nn', g.d10_sv_nn,
    'team_d10_sf_per_game_nn', g.team_d10_sf_pg_nn,
    'opp_d10_sf_allowed_per_game_nn', g.opp_d10_sf_allowed_pg_nn,
    'pace_index_nn', g.pace_idx_nn,
    'rest_days_nn', g.rest_days_nn,
    'b2b_flag_nn', g.b2b_flag_nn,
    'd5_saves_per60_nn', g.d5_sv60_nn,
    'd10_saves_per60_nn', g.d10_sv60_nn,
    'd5_shots_faced_per60_nn', g.d5_sf60_nn,
    'season_save_pct_nn', g.season_sv_nn
  )
FROM goal g
ON CONFLICT (check_name, audit_date) DO UPDATE
SET result = EXCLUDED.result, level = EXCLUDED.level;

COMMIT;

