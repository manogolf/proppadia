-- scripts/refresh.sql
-- Daily refresh: fill SOG context, goalie cadence & season %, refresh ready MVs, and log an audit.

-- Bump timeouts for long rollups/refreshes (Supabase default ~2m)
SET statement_timeout = '10min';
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = '5min';

BEGIN;


BEGIN;

------------------------------------------------------------------------------
-- A) SOG context from tf_team_game_sog (prior-10, leakage-safe)
------------------------------------------------------------------------------
WITH games AS (
  SELECT
    t.team_id,
    t.opponent_id,
    t.game_id,
    t.game_date::date AS game_date,
    t.team_sog,
    o.team_sog AS opp_sog_in_this_game
  FROM nhl.tf_team_game_sog t
  JOIN nhl.tf_team_game_sog o
    ON o.game_id = t.game_id
   AND o.team_id = t.opponent_id
   AND o.opponent_id = t.team_id
),
roll AS (
  SELECT
    team_id,
    game_id,
    game_date,
    CASE WHEN COUNT(team_sog) OVER w10 > 0
         THEN AVG(team_sog) OVER w10 ELSE NULL END AS team_d10_sf_per60_calc,
    CASE WHEN COUNT(opp_sog_in_this_game) OVER w10 > 0
         THEN AVG(opp_sog_in_this_game) OVER w10 ELSE NULL END AS team_d10_sa_per60_calc
  FROM games
  WINDOW w10 AS (
    PARTITION BY team_id
    ORDER BY game_date, game_id
    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
  )
),
opp_side AS (
  SELECT
    a.team_id,
    a.game_id,
    a.team_d10_sf_per60_calc,
    a.team_d10_sa_per60_calc,
    b.team_d10_sf_per60_calc AS opp_d10_sf_per60_calc
  FROM roll a
  JOIN (
    SELECT r.*, g.opponent_id
    FROM roll r
    JOIN (SELECT DISTINCT game_id, team_id, opponent_id FROM games) g
      ON g.game_id = r.game_id AND g.team_id = r.team_id
  ) b
    ON b.game_id = a.game_id
   AND b.opponent_id = a.team_id
),
final_ctx AS (
  SELECT
    team_id,
    game_id,
    opp_d10_sf_per60_calc  AS opp_d10_sf_per60,
    team_d10_sa_per60_calc AS team_d10_sa_per60,
    CASE
      WHEN team_d10_sf_per60_calc IS NULL OR opp_d10_sf_per60_calc IS NULL
        THEN NULL
      ELSE sqrt(team_d10_sf_per60_calc * opp_d10_sf_per60_calc)
    END AS pace_matchup_index
  FROM opp_side
)
UPDATE nhl.training_features_nhl_sog_v2 t
SET
  opp_d10_sf_per60   = f.opp_d10_sf_per60,
  team_d10_sa_per60  = f.team_d10_sa_per60,
  pace_matchup_index = f.pace_matchup_index
FROM final_ctx f
WHERE t.game_id = f.game_id
  AND t.team_id = f.team_id;

------------------------------------------------------------------------------
-- B) Goalie rest_days / b2b_flag from goalie’s own previous appearance
------------------------------------------------------------------------------
WITH appearances AS (
  SELECT
    g.player_id,
    g.game_id,
    gm.game_date::date AS game_date,
    LAG(gm.game_date::date) OVER (
      PARTITION BY g.player_id
      ORDER BY gm.game_date, g.game_id
    ) AS prev_game_date
  FROM nhl.goalie_game_logs_raw g
  JOIN nhl.games gm ON gm.game_id = g.game_id
),
derived AS (
  SELECT
    player_id,
    game_id,
    CASE
      WHEN prev_game_date IS NULL THEN NULL
      ELSE GREATEST(0, (game_date - prev_game_date))::int
    END AS rest_days_goalie,
    CASE
      WHEN prev_game_date IS NULL THEN NULL
      ELSE ((game_date - prev_game_date) = 1)
    END AS b2b_flag_goalie
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

------------------------------------------------------------------------------
-- C) Goalie season_save_pct = previous 2 seasons + current season-to-date (pre-game)
------------------------------------------------------------------------------
WITH base AS (
  SELECT
    g.player_id,
    g.game_id,
    g.game_date::date AS game_date,
    (EXTRACT(YEAR FROM g.game_date)::int
     - CASE WHEN EXTRACT(MONTH FROM g.game_date) < 9 THEN 1 ELSE 0 END
    )::int AS season_start_year,
    g.saves::numeric       AS saves,
    g.shots_faced::numeric AS shots_faced
  FROM nhl.goalie_game_logs_raw g
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
    b.player_id,
    b.game_id,
    SUM(b.saves) OVER w_stod AS stod_saves,
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
    s.player_id,
    s.game_id,
    COALESCE(s.stod_saves, 0) +
      COALESCE(p1.season_saves, 0) +
      COALESCE(p2.season_saves, 0) AS num_saves,
    COALESCE(s.stod_shots, 0) +
      COALESCE(p1.season_shots, 0) +
      COALESCE(p2.season_shots, 0) AS den_shots
  FROM stod s
  LEFT JOIN season_totals p1
         ON p1.player_id = s.player_id AND p1.season_start_year = s.curr_season - 1
  LEFT JOIN season_totals p2
         ON p2.player_id = s.player_id AND p2.season_start_year = s.curr_season - 2
)
UPDATE nhl.training_features_goalie_saves_v2 t
SET season_save_pct =
  CASE WHEN a.den_shots > 0 THEN a.num_saves / a.den_shots ELSE NULL END
FROM assembled a
WHERE t.player_id = a.player_id
  AND t.game_id   = a.game_id
  AND (t.season_save_pct IS DISTINCT FROM
       CASE WHEN a.den_shots > 0 THEN a.num_saves / a.den_shots ELSE NULL END);

------------------------------------------------------------------------------
-- D) Refresh ready MVs (so exports see latest values)
------------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW nhl.training_features_nhl_sog_v2_ready;
REFRESH MATERIALIZED VIEW nhl.training_features_goalie_saves_v2_ready;

------------------------------------------------------------------------------
-- E) Data-quality snapshot (fixed aliases)
------------------------------------------------------------------------------
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
SELECT
  CURRENT_DATE,
  'sog_ready_coverage',
  'info',
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
SELECT
  CURRENT_DATE,
  'goalie_ready_coverage',
  'info',
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
ON CONFLICT (check_name, audit_date)
DO UPDATE SET result = EXCLUDED.result, level = EXCLUDED.level;

COMMIT;
