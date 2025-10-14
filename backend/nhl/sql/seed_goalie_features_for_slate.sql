\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Expect: psql -v slate_date=YYYY-MM-DD

-- ---------- one-time safety: dedupe table & ensure unique index ----------
DO $$
BEGIN
  DELETE FROM nhl.training_features_goalie_saves_v2 t
  USING nhl.training_features_goalie_saves_v2 z
  WHERE t.player_id = z.player_id
    AND t.game_id   = z.game_id
    AND t.ctid < z.ctid;

  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='nhl'
      AND indexname='uq_training_features_goalie_saves_v2_player_game'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_training_features_goalie_saves_v2_player_game
             ON nhl.training_features_goalie_saves_v2 (player_id, game_id)';
  END IF;
END $$;

-- Ensure start_prob column exists
ALTER TABLE nhl.training_features_goalie_saves_v2
  ADD COLUMN IF NOT EXISTS start_prob numeric;

-- ---------- discover goalie-log column names ----------
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
  AND column_name IN ('toi_minutes','time_on_ice_minutes','minutes','toi','time_on_ice','min_played')
ORDER BY 1
LIMIT 1;
\gset

WITH params AS (
  SELECT :'slate_date'::date AS d
),
-- games on the slate
g AS (
  SELECT game_id, home_team_id, away_team_id, game_date::date AS game_date
  FROM nhl.games
  WHERE game_date = (SELECT d FROM params)
),
-- slate goalies (roster_status × players.position = 'G')
roster_g AS (
  SELECT DISTINCT
    r.player_id,
    r.team_id,
    r.game_id,
    gm.home_team_id,
    gm.away_team_id,
    gm.game_date::date AS game_date
  FROM nhl.roster_status r
  JOIN nhl.players p USING (player_id)
  JOIN g gm USING (game_id)
  WHERE COALESCE(p.position,'') = 'G'
),
cand AS (
  SELECT
    r.player_id,
    r.game_id,
    CASE WHEN r.team_id = r.home_team_id THEN r.home_team_id ELSE r.away_team_id END AS team_id,
    CASE WHEN r.team_id = r.home_team_id THEN r.away_team_id ELSE r.home_team_id END AS opponent_id,
    (r.team_id = r.home_team_id) AS is_home,
    r.game_date
  FROM roster_g r
),
-- normalize prior goalie logs (strictly before slate date)
prior_raw AS (
  SELECT
    gl.player_id,
    gl.game_id,
    gl.game_date::date AS gd,
    COALESCE(gl.:g_shotscol,0)::numeric AS sf,
    COALESCE(gl.:g_savescol,0)::numeric AS sv,
    COALESCE(NULLIF(gl.:g_toicol,0),60)::numeric AS toi
  FROM nhl.v_goalie_game_logs_played gl
),
-- rolling windows (5/10/20) per candidate goalie, using appearances before the slate date
roll AS (
  SELECT
    x.player_id, x.game_id,
    SUM(CASE WHEN rn <= 5  THEN sf  END) AS sf5,
    SUM(CASE WHEN rn <= 5  THEN sv  END) AS sv5,
    SUM(CASE WHEN rn <= 5  THEN toi END) AS toi5,
    SUM(CASE WHEN rn <= 10 THEN sf  END) AS sf10,
    SUM(CASE WHEN rn <= 10 THEN sv  END) AS sv10,
    SUM(CASE WHEN rn <= 10 THEN toi END) AS toi10,
    SUM(CASE WHEN rn <= 20 THEN sv  END) AS sv20,
    SUM(CASE WHEN rn <= 20 THEN toi END) AS toi20
  FROM (
    SELECT
      c.player_id, c.game_id,
      pr.sf, pr.sv, pr.toi,
      ROW_NUMBER() OVER (
        PARTITION BY c.player_id, c.game_id
        ORDER BY pr.gd DESC, pr.game_id DESC
      ) AS rn
    FROM cand c
    JOIN prior_raw pr
      ON pr.player_id = c.player_id
     AND pr.gd < c.game_date
  ) x
  GROUP BY 1,2
),
prev AS (
  -- Previous appearance date and minutes (for rest_days/b2b & starter heuristic)
  SELECT
    c.player_id, c.game_id,
    (
      SELECT pr.gd
      FROM prior_raw pr
      WHERE pr.player_id = c.player_id AND pr.gd < c.game_date
      ORDER BY pr.gd DESC, pr.game_id DESC
      LIMIT 1
    ) AS prev_game_date,
    (
      SELECT pr.toi
      FROM prior_raw pr
      WHERE pr.player_id = c.player_id AND pr.gd < c.game_date
      ORDER BY pr.gd DESC, pr.game_id DESC
      LIMIT 1
    ) AS prev_toi_minutes
  FROM cand c
),
-- team context (latest <= slate date) for both team & opponent
teamctx AS (
  SELECT
    c.player_id, c.game_id,
    t.team_d10_sf_per_game                                AS team_d10_sf_per_game,
    o.opp_d10_sf_allowed_per_game                         AS opp_d10_sf_allowed_per_game,
    CASE
      WHEN t.team_d10_sf_per_game IS NOT NULL AND o.opp_d10_sf_allowed_per_game IS NOT NULL
      THEN sqrt(t.team_d10_sf_per_game * o.opp_d10_sf_allowed_per_game)
      ELSE NULL
    END AS pace_index,
    o.team_d10_sf_per_game                                AS opp_d10_sf_per60,
    t.opp_d10_sf_allowed_per_game                         AS team_d10_sa_per60,
    CASE
      WHEN t.team_d10_sf_per_game IS NOT NULL AND o.team_d10_sf_per_game IS NOT NULL
      THEN sqrt(t.team_d10_sf_per_game * o.team_d10_sf_per_game)
      ELSE NULL
    END AS pace_matchup_index
  FROM cand c
  LEFT JOIN LATERAL (
    SELECT team_d10_sf_per_game, opp_d10_sf_allowed_per_game
    FROM nhl.tf_team_roll10 tt
    WHERE tt.team_id = c.team_id AND tt.game_date < c.game_date
    ORDER BY tt.game_date DESC
    LIMIT 1
  ) t ON TRUE
  LEFT JOIN LATERAL (
    SELECT team_d10_sf_per_game, opp_d10_sf_allowed_per_game
    FROM nhl.tf_team_roll10 tt
    WHERE tt.team_id = c.opponent_id AND tt.game_date < c.game_date
    ORDER BY tt.game_date DESC
    LIMIT 1
  ) o ON TRUE
),
-- assemble rows
seed AS (
  SELECT
    c.player_id, c.game_id, c.team_id, c.opponent_id, c.is_home, c.game_date,
    CASE WHEN r.toi10 > 0 THEN 60*r.sf10 / r.toi10 END AS d10_shots_faced_per60,
    CASE WHEN r.sf10  > 0 THEN     r.sv10 / r.sf10  END AS d10_save_pct,
    tc.team_d10_sf_per_game,
    tc.opp_d10_sf_allowed_per_game,
    tc.pace_index,
    CASE WHEN p.prev_game_date IS NULL THEN NULL ELSE GREATEST(0, (c.game_date - p.prev_game_date))::int END AS rest_days,
    CASE WHEN p.prev_game_date IS NULL THEN NULL ELSE ((c.game_date - p.prev_game_date) = 1) END AS b2b_flag,
    CASE WHEN r.toi5  > 0 THEN 60*r.sv5  / r.toi5  END AS d5_saves_per60,
    CASE WHEN r.toi10 > 0 THEN 60*r.sv10 / r.toi10 END AS d10_saves_per60,
    CASE WHEN r.toi5  > 0 THEN 60*r.sf5  / r.toi5  END AS d5_shots_faced_per60,
    NULL::numeric AS season_save_pct,  -- filled later by refresh C)
    tc.opp_d10_sf_per60,
    tc.team_d10_sa_per60,
    tc.pace_matchup_index,
    CASE WHEN r.toi20 > 0 THEN 60*r.sv20 / r.toi20 END AS d20_saves_per60,
    -- carry prev fields so final SELECT can compute start_prob safely
    p.prev_game_date,
    p.prev_toi_minutes
  FROM cand c
  LEFT JOIN roll    r  USING (player_id, game_id)
  LEFT JOIN prev    p  USING (player_id, game_id)
  LEFT JOIN teamctx tc USING (player_id, game_id)
  WHERE r.player_id IS NOT NULL -- only keep true goalies with prior logs
),
-- keep exactly one row per (player_id, game_id)
seed_one AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY player_id, game_id ORDER BY player_id, game_id) AS rn
    FROM seed s
  ) x
  WHERE rn = 1
)
INSERT INTO nhl.training_features_goalie_saves_v2 AS t (
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  d10_shots_faced_per60, d10_save_pct,
  team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
  pace_index, rest_days, b2b_flag,
  d5_saves_per60, d10_saves_per60, d5_shots_faced_per60, season_save_pct,
  opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index,
  d20_saves_per60,
  start_prob
)
SELECT
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  d10_shots_faced_per60, d10_save_pct,
  team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
  pace_index, rest_days, b2b_flag,
  d5_saves_per60, d10_saves_per60, d5_shots_faced_per60, season_save_pct,
  opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index,
  d20_saves_per60,
  CASE
    WHEN prev_game_date IS NULL THEN 0.55
    WHEN (game_date - prev_game_date) = 1 AND COALESCE(prev_toi_minutes,0) >= 40 THEN 0.35
    WHEN (game_date - prev_game_date) = 1 AND COALESCE(prev_toi_minutes,0) < 40 THEN 0.65
    ELSE 0.55
  END AS start_prob
FROM seed_one
ON CONFLICT (player_id, game_id) DO UPDATE
SET d10_shots_faced_per60       = COALESCE(EXCLUDED.d10_shots_faced_per60,       t.d10_shots_faced_per60),
    d10_save_pct                = COALESCE(EXCLUDED.d10_save_pct,                t.d10_save_pct),
    team_d10_sf_per_game        = COALESCE(EXCLUDED.team_d10_sf_per_game,        t.team_d10_sf_per_game),
    opp_d10_sf_allowed_per_game = COALESCE(EXCLUDED.opp_d10_sf_allowed_per_game, t.opp_d10_sf_allowed_per_game),
    pace_index                  = COALESCE(EXCLUDED.pace_index,                  t.pace_index),
    rest_days                   = COALESCE(EXCLUDED.rest_days,                   t.rest_days),
    b2b_flag                    = COALESCE(EXCLUDED.b2b_flag,                    t.b2b_flag),
    d5_saves_per60              = COALESCE(EXCLUDED.d5_saves_per60,              t.d5_saves_per60),
    d10_saves_per60             = COALESCE(EXCLUDED.d10_saves_per60,             t.d10_saves_per60),
    d5_shots_faced_per60        = COALESCE(EXCLUDED.d5_shots_faced_per60,        t.d5_shots_faced_per60),
    season_save_pct             = COALESCE(EXCLUDED.season_save_pct,             t.season_save_pct),
    opp_d10_sf_per60            = COALESCE(EXCLUDED.opp_d10_sf_per60,            t.opp_d10_sf_per60),
    team_d10_sa_per60           = COALESCE(EXCLUDED.team_d10_sa_per60,           t.team_d10_sa_per60),
    pace_matchup_index          = COALESCE(EXCLUDED.pace_matchup_index,          t.pace_matchup_index),
    d20_saves_per60             = COALESCE(EXCLUDED.d20_saves_per60,             t.d20_saves_per60),
    start_prob                  = COALESCE(EXCLUDED.start_prob,                  t.start_prob);

\echo 'seed_goalie_features_for_slate: upserted rows for slate_date=' :'slate_date'
SELECT COUNT(*) FROM nhl.training_features_goalie_saves_v2 WHERE game_date = :'slate_date'::date;
