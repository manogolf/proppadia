\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Allow this seed to finish even with lots of historical rows
SET statement_timeout = 0;

-- Expect: psql -v slate_date=YYYY-MM-DD

-- ---------- one-time safety: dedupe existing table & ensure unique index ----------
DO $$
BEGIN
  IF to_regclass('nhl.training_features_sog_denali') IS NULL THEN
    RAISE EXCEPTION 'Table nhl.training_features_sog_denali does not exist';
  END IF;

  -- remove accidental dupes so we can have a clean unique index
  DELETE FROM nhl.training_features_sog_denali t
  USING nhl.training_features_sog_denali z
  WHERE t.player_id = z.player_id
    AND t.game_id   = z.game_id
    AND t.ctid < z.ctid;

  -- ensure unique index on (player_id, game_id)
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'nhl'
      AND indexname = 'uq_training_features_sog_denali_player_game'
  ) THEN
    EXECUTE '
      CREATE UNIQUE INDEX uq_training_features_sog_denali_player_game
      ON nhl.training_features_sog_denali (player_id, game_id)
    ';
  END IF;
END $$;

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

-- opponent / is_home / game_date for each roster player
enrich AS (
  SELECT
    r.player_id,
    r.game_id,
    r.team_id,
    CASE
      WHEN r.team_id = g.home_team_id THEN g.away_team_id
      ELSE g.home_team_id
    END AS opponent_id,
    (r.team_id = g.home_team_id) AS is_home,
    g.game_date
  FROM r
  JOIN g USING (game_id)
),

-- keep skaters only (exclude goalies)
skaters AS (
  SELECT DISTINCT e.*
  FROM enrich e
  JOIN nhl.players p ON p.player_id = e.player_id
  WHERE COALESCE(p.position,'') <> 'G'
),

/*
  ✅ Authoritative previous-appearance date:
  Use skater logs + games, not training_features_sog_denali, so rest_days/b2b
  don’t break when Denali features are stale or missing recent games.
*/
last_game AS (
  SELECT DISTINCT ON (l.player_id)
    l.player_id,
    g2.game_date::date AS prev_game_date
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g2
    ON g2.game_id = l.game_id
  JOIN skaters s
    ON s.player_id = l.player_id
  WHERE g2.game_date::date < (SELECT d FROM params)
  ORDER BY l.player_id, g2.game_date DESC, l.game_id DESC
),

-- latest per-player feature row (strictly before slate_date) from existing Denali table
-- NOTE: Rolling per60 fields are intentionally NOT used for seeding to avoid stub contamination.
last_feat AS (
  SELECT DISTINCT ON (t.player_id)
    t.player_id,

    -- pace
    t.pace_index,
    t.pace_matchup_index,

    -- team / opp environment
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.opp_d10_sf_per60,
    t.team_d10_sa_per60,
    t.opp_d10_sa_per60,

    -- role / PP
    t.role_pp_share,

    -- season / szn TOI & shifts
    t.szn_toi_per_game_5on5,
    t.szn_toi_per_game_pp,
    t.szn_toi_per_game_pk,
    t.szn_shifts_per_game_5on5,
    t.szn_shifts_per_game_pp,
    t.szn_shifts_per_game_pk,

    t.season_5on5_icetime_per_game,
    t.season_5on4_icetime_per_game,
    t.season_4on5_icetime_per_game,
    t.season_5on5_shifts_per_game,
    t.season_5on4_shifts_per_game,
    t.season_4on5_shifts_per_game,

    -- team top-line shares
    t.team_szn_5on5_top_line_xgf_share,
    t.team_5v5_top_line_icetime_share,
    t.team_5v5_top_line_shotattempts_share,

    -- last-10 / last-5 counts
    t.last10_team_sog_share,
    t.team_num_sog_last10,
    t.team_num_event_last10,

    t.num_sog_last5,
    t.num_sog_last10,
    t.num_sog_szn_to_date,

    t.num_event_last5,
    t.num_event_last10,
    t.num_event_szn_to_date,

    t.hot_last5_flag
  FROM nhl.training_features_sog_denali t
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

-- assemble seed rows for the slate
seed AS (
  SELECT
    s.player_id,
    s.game_id,
    s.team_id,
    s.opponent_id,
    s.is_home,
    (SELECT d FROM params) AS game_date,

    -- label: unfilled at seed time
    NULL::numeric AS shots_on_goal,

    -- IMPORTANT POLICY: do NOT seed rolling per60 from prior feature tables.
    -- These are filled by the later "Refresh SOG rollups" step from raw logs.
    NULL::numeric AS d5_sog_per60,
    NULL::numeric AS d10_sog_per60,
    NULL::numeric AS d20_sog_per60,
    NULL::numeric AS attempts_d10_per60,

    -- ✅ rest / schedule (computed from last_game.prev_game_date)
    CASE
      WHEN lg.prev_game_date IS NOT NULL
        THEN GREATEST(0, ((SELECT d FROM params) - lg.prev_game_date))::int
      ELSE NULL
    END AS rest_days,
    CASE
      WHEN lg.prev_game_date IS NOT NULL
        THEN (((SELECT d FROM params) - lg.prev_game_date) = 1)
      ELSE NULL
    END AS b2b_flag,

    -- pace
    lf.pace_matchup_index,

    -- team / opp environment (two refreshed from team_roll)
    COALESCE(tr.team_d10_sf_per_game,        lf.team_d10_sf_per_game)        AS team_d10_sf_per_game,
    COALESCE(tr.opp_d10_sf_allowed_per_game, lf.opp_d10_sf_allowed_per_game) AS opp_d10_sf_allowed_per_game,

    -- role (raw; pp_role_final computed later)
    lf.role_pp_share,

    -- last-10 / last-5 team counts (from Denali)
    lf.last10_team_sog_share,
    lf.team_num_sog_last10,
    lf.team_num_event_last10,

    -- player SOG/event rolling counts (from Denali)
    lf.num_sog_last5,
    lf.num_sog_last10,
    lf.num_sog_szn_to_date,

    lf.num_event_last5,
    lf.num_event_last10,
    lf.num_event_szn_to_date,

    lf.hot_last5_flag
  FROM skaters s
  LEFT JOIN last_feat lf ON lf.player_id = s.player_id
  LEFT JOIN last_game lg ON lg.player_id = s.player_id
  LEFT JOIN team_roll tr ON tr.team_id = s.team_id
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

INSERT INTO nhl.training_features_nhl_sog_enriched_pregame_v2 AS t (
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,
  season,
  shots_on_goal,

  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,

  rest_days,
  b2b_flag,

  pace_matchup_index,

  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,

  role_pp_share,

  last10_team_sog_share,
  team_num_shotwasongoal_for_last10,
  team_num_event_shot_for_last10,

  num_shotwasongoal_last5,
  num_shotwasongoal_last10,
  num_shotwasongoal_season_to_date,

  num_event_shot_last5,
  num_event_shot_last10,
  num_event_shot_season_to_date,

  hot_last5_flag
)
SELECT
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,
  (game_id / 1000000)::int AS season,
  shots_on_goal,

  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,

  rest_days,
  b2b_flag,

  pace_matchup_index,

  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,

  role_pp_share,

  last10_team_sog_share,
  team_num_sog_last10           AS team_num_shotwasongoal_for_last10,
  team_num_event_last10         AS team_num_event_shot_for_last10,

  num_sog_last5                 AS num_shotwasongoal_last5,
  num_sog_last10                AS num_shotwasongoal_last10,
  num_sog_szn_to_date           AS num_shotwasongoal_season_to_date,

  num_event_last5               AS num_event_shot_last5,
  num_event_last10              AS num_event_shot_last10,
  num_event_szn_to_date         AS num_event_shot_season_to_date,

  hot_last5_flag
FROM seed_one
ON CONFLICT (player_id, game_id) DO UPDATE
SET team_id                           = EXCLUDED.team_id,
    opponent_id                       = EXCLUDED.opponent_id,
    is_home                           = EXCLUDED.is_home,
    game_date                         = EXCLUDED.game_date,
    season                            = EXCLUDED.season,
    shots_on_goal                     = EXCLUDED.shots_on_goal,

    -- IMPORTANT: wipe any stubby rollups on conflict too; rollups are filled by the refresh step.
    d5_sog_per60                      = EXCLUDED.d5_sog_per60,
    d10_sog_per60                     = EXCLUDED.d10_sog_per60,
    d20_sog_per60                     = EXCLUDED.d20_sog_per60,
    attempts_d10_per60                = EXCLUDED.attempts_d10_per60,

    rest_days                         = EXCLUDED.rest_days,
    b2b_flag                          = EXCLUDED.b2b_flag,
    pace_matchup_index                = EXCLUDED.pace_matchup_index,
    team_d10_sf_per_game              = EXCLUDED.team_d10_sf_per_game,
    opp_d10_sf_allowed_per_game       = EXCLUDED.opp_d10_sf_allowed_per_game,
    role_pp_share                     = EXCLUDED.role_pp_share,
    last10_team_sog_share             = EXCLUDED.last10_team_sog_share,
    team_num_shotwasongoal_for_last10 = EXCLUDED.team_num_shotwasongoal_for_last10,
    team_num_event_shot_for_last10    = EXCLUDED.team_num_event_shot_for_last10,
    num_shotwasongoal_last5           = EXCLUDED.num_shotwasongoal_last5,
    num_shotwasongoal_last10          = EXCLUDED.num_shotwasongoal_last10,
    num_shotwasongoal_season_to_date  = EXCLUDED.num_shotwasongoal_season_to_date,
    num_event_shot_last5              = EXCLUDED.num_event_shot_last5,
    num_event_shot_last10             = EXCLUDED.num_event_shot_last10,
    num_event_shot_season_to_date     = EXCLUDED.num_event_shot_season_to_date,
    hot_last5_flag                    = EXCLUDED.hot_last5_flag;

\echo 'seed_sog_features_for_slate: upserted rows for slate_date=' :'slate_date'
SELECT COUNT(*)
FROM nhl.training_features_nhl_sog_enriched_pregame_v2
WHERE game_date = :'slate_date'::date;
