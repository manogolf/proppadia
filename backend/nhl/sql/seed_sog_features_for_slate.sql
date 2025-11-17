\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Expect: psql -v slate_date=YYYY-MM-DD

-- ---------- one-time safety: dedupe existing table & ensure unique index ----------
DO $$
BEGIN
  -- Only run this maintenance if the legacy training table exists
  IF to_regclass('nhl.training_features_nhl_sog_v2') IS NOT NULL THEN

    -- remove accidental dupes so we can have a clean unique index
    DELETE FROM nhl.training_features_nhl_sog_v2 t
    USING nhl.training_features_nhl_sog_v2 z
    WHERE t.player_id = z.player_id
      AND t.game_id   = z.game_id
      AND t.ctid < z.ctid;

    -- ensure unique index on (player_id, game_id)
    IF NOT EXISTS (
      SELECT 1
      FROM pg_indexes
      WHERE schemaname = 'nhl'
        AND indexname = 'uq_training_features_nhl_sog_v2_player_game'
    ) THEN
      EXECUTE '
        CREATE UNIQUE INDEX uq_training_features_nhl_sog_v2_player_game
        ON nhl.training_features_nhl_sog_v2 (player_id, game_id)
      ';
    END IF;

  END IF;
END $$;

-- ---------- ensure feature columns exist (we are ADDING features, not deleting them) ----------
ALTER TABLE nhl.training_features_nhl_sog_v2
  ADD COLUMN IF NOT EXISTS attempts_d10_per60   numeric,
  ADD COLUMN IF NOT EXISTS pace_index           numeric,
  ADD COLUMN IF NOT EXISTS opp_d10_sf_per60     numeric,
  ADD COLUMN IF NOT EXISTS team_d10_sa_per60    numeric;

WITH params AS (
  -- IMPORTANT: this uses a psql variable :slate_date
  SELECT :'slate_date'::date AS d
),

-- games for the slate date
g AS (
  SELECT game_id, home_team_id, away_team_id, game_date::date AS game_date
  FROM nhl.games
  WHERE game_date = (SELECT d FROM params)
),

-- roster rows for those games (DISTINCT in case roster_status has multiple rows)
r AS (
  SELECT DISTINCT r.player_id, r.team_id, r.game_id
  FROM nhl.roster_status r
  JOIN g USING (game_id)
),

-- add opponent / is_home / game_date
enrich AS (
  SELECT
    r.player_id,
    r.game_id,
    r.team_id,
    CASE WHEN r.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_id,
    (r.team_id = g.home_team_id) AS is_home,
    g.game_date
  FROM r
  JOIN g USING (game_id)
),

-- keep skaters only (exclude goalies) — DISTINCT just in case players join fans out
skaters AS (
  SELECT DISTINCT e.*
  FROM enrich e
  JOIN nhl.players p ON p.player_id = e.player_id
  WHERE COALESCE(p.position,'') <> 'G'
),

-- latest per-player feature row (strictly before slate_date)
last_feat AS (
  SELECT DISTINCT ON (t.player_id)
    t.player_id,
    t.d5_sog_per60,
    t.d10_sog_per60,
    t.d20_sog_per60,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.role_pp_share,
    t.rest_days,             -- prior (we recompute below)
    t.b2b_flag,              -- prior (we recompute below)

    -- Placeholder: attempts_d10_per60 lives in this table now.
    -- For existing history this will be NULL until we wire up a backfill / upstream view.
    t.attempts_d10_per60,

    -- Placeholders for new features; these columns exist now and can be populated later.
    t.pace_index,
    t.opp_d10_sf_per60,
    t.team_d10_sa_per60,

    t.pace_matchup_index,
    t.game_date AS prev_game_date
  FROM nhl.training_features_nhl_sog_v2 t
  JOIN skaters s ON s.player_id = t.player_id
  WHERE t.game_date < (SELECT d FROM params)
  ORDER BY t.player_id, t.game_date DESC, t.game_id DESC
),

-- latest team roll-10 for each skater's team (as of <= slate_date)
team_roll AS (
  SELECT DISTINCT ON (t.team_id)
    t.team_id,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game
  FROM nhl.tf_team_roll10 t
  ORDER BY t.team_id, t.game_id DESC
),

-- assemble seed rows
seed AS (
  SELECT
    s.player_id,
    s.game_id,
    s.team_id,
    s.opponent_id,
    s.is_home,
    (SELECT d FROM params) AS game_date,
    NULL::int AS shots_on_goal,
    lf.d5_sog_per60,
    lf.d10_sog_per60,
    lf.d20_sog_per60,
    COALESCE(tr.team_d10_sf_per_game, lf.team_d10_sf_per_game)        AS team_d10_sf_per_game,
    COALESCE(tr.opp_d10_sf_allowed_per_game, lf.opp_d10_sf_allowed_per_game)
                                                                      AS opp_d10_sf_allowed_per_game,
    lf.role_pp_share,
    CASE WHEN lf.prev_game_date IS NOT NULL
         THEN GREATEST(0, ((SELECT d FROM params) - lf.prev_game_date))::int END AS rest_days,
    CASE WHEN lf.prev_game_date IS NOT NULL
         THEN ((SELECT d FROM params) - lf.prev_game_date = 1) END      AS b2b_flag,
    lf.attempts_d10_per60,
    lf.pace_index,
    lf.opp_d10_sf_per60,
    lf.team_d10_sa_per60,
    lf.pace_matchup_index
  FROM skaters s
  LEFT JOIN last_feat lf ON lf.player_id = s.player_id
  LEFT JOIN team_roll tr ON tr.team_id = s.team_id
),

-- *** de-dup source rows: keep exactly one row per (player_id, game_id) ***
seed_one AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY player_id, game_id ORDER BY player_id, game_id) AS rn
    FROM seed s
  ) x
  WHERE rn = 1
)

INSERT INTO nhl.training_features_nhl_sog_v2 AS t (
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  shots_on_goal,
  d5_sog_per60, d10_sog_per60, d20_sog_per60,
  team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
  role_pp_share, rest_days, b2b_flag,
  attempts_d10_per60,
  pace_index, opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index
)
SELECT
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  shots_on_goal,
  d5_sog_per60, d10_sog_per60, d20_sog_per60,
  team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
  role_pp_share, rest_days, b2b_flag,
  attempts_d10_per60,
  pace_index, opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index
FROM seed_one
ON CONFLICT (player_id, game_id) DO UPDATE
SET shots_on_goal                = EXCLUDED.shots_on_goal,
    d5_sog_per60                 = EXCLUDED.d5_sog_per60,
    d10_sog_per60                = EXCLUDED.d10_sog_per60,
    d20_sog_per60                = EXCLUDED.d20_sog_per60,
    team_d10_sf_per_game         = EXCLUDED.team_d10_sf_per_game,
    opp_d10_sf_allowed_per_game  = EXCLUDED.opp_d10_sf_allowed_per_game,
    role_pp_share                = EXCLUDED.role_pp_share,
    rest_days                    = EXCLUDED.rest_days,
    b2b_flag                     = EXCLUDED.b2b_flag,
    attempts_d10_per60           = EXCLUDED.attempts_d10_per60,
    pace_index                   = EXCLUDED.pace_index,
    opp_d10_sf_per60             = EXCLUDED.opp_d10_sf_per60,
    team_d10_sa_per60            = EXCLUDED.team_d10_sa_per60,
    pace_matchup_index           = EXCLUDED.pace_matchup_index;

\echo 'seed_sog_features_for_slate: upserted rows for slate_date=' :'slate_date'
SELECT COUNT(*) FROM nhl.training_features_nhl_sog_v2 WHERE game_date = :'slate_date'::date;
