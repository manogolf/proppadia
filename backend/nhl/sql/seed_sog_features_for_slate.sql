\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Allow this heavy seed to finish even with lots of historical rows
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
    CASE WHEN r.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_id,
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

-- past REGULAR-SEASON logs for these skaters, strictly before slate_date
-- regular season identified by game_id pattern '____02%%%%' (e.g., 2023020182)
logs AS (
  SELECT
    l.player_id,
    l.game_id,
    l.game_date::date AS game_date,
    COALESCE(l.shots_on_goal,  0)::int    AS shots_on_goal,
    COALESCE(l.shot_attempts,  0)::int    AS shot_attempts,
    COALESCE(l.toi_minutes,    0)::numeric AS toi_minutes,
    COALESCE(l.pp_toi_minutes, 0)::numeric AS pp_toi_minutes
  FROM nhl.skater_game_logs_raw l
  JOIN skaters s ON s.player_id = l.player_id
  JOIN nhl.games gg ON gg.game_id = l.game_id
  WHERE l.game_date < (SELECT d FROM params)
    AND substring(gg.game_id::text, 5, 2) = '02'  -- '02' = regular season
),

-- per-game SOG/attempts per 60 + rolling windows over regular-season logs
logs_roll AS (
  SELECT
    player_id,
    game_id,
    game_date,
    shots_on_goal,
    shot_attempts,
    toi_minutes,
    pp_toi_minutes,

    CASE WHEN toi_minutes > 0
         THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
         ELSE NULL END AS sog_per60,
    CASE WHEN toi_minutes > 0
         THEN (shot_attempts::double precision / toi_minutes::double precision) * 60.0
         ELSE NULL END AS attempts_per60,

    -- d5 / d10 / d20 SOG per 60 (exclude current row)
    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS d5_sog_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS d10_sog_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS d20_sog_per60,

    -- attempts d10 per 60 (exclude current row)
    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shot_attempts::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS attempts_d10_per60,

    -- last 5 / 10 SOG counts (exclude current row)
    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS num_sog_last5,

    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS num_sog_last10,

    -- last 5 / 10 game counts (events)
    COUNT(*) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS num_event_last5,

    COUNT(*) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS num_event_last10
  FROM logs
),

-- NHL season key: flips on July 1 (same convention as your saves script)
season_key AS (
  SELECT
    lr.player_id,
    lr.game_id,
    lr.game_date,
    CASE
      WHEN EXTRACT(MONTH FROM lr.game_date) >= 7
        THEN EXTRACT(YEAR FROM lr.game_date) + 1
      ELSE EXTRACT(YEAR FROM lr.game_date)
    END AS nhl_season_key,
    lr.shots_on_goal,
    lr.shot_attempts
  FROM logs_roll lr
),

-- season-to-date SOG & event counts (within each NHL season, up to slate_date)
season_agg AS (
  SELECT
    player_id,
    nhl_season_key,
    SUM(shots_on_goal) AS num_sog_szn_to_date,
    COUNT(*)           AS num_event_szn_to_date
  FROM season_key
  GROUP BY player_id, nhl_season_key
),

-- latest regular-season snapshot per player (before slate_date),
-- carrying rolling windows + season aggregates
log_snap AS (
  SELECT DISTINCT ON (lr.player_id)
    lr.player_id,
    lr.game_date AS prev_game_date,
    lr.d5_sog_per60,
    lr.d10_sog_per60,
    lr.d20_sog_per60,
    lr.attempts_d10_per60,
    lr.num_sog_last5,
    lr.num_sog_last10,
    lr.num_event_last5,
    lr.num_event_last10,
    sa.num_sog_szn_to_date,
    sa.num_event_szn_to_date
  FROM logs_roll lr
  LEFT JOIN season_key sk
    ON sk.player_id = lr.player_id
   AND sk.game_id   = lr.game_id
  LEFT JOIN season_agg sa
    ON sa.player_id      = sk.player_id
   AND sa.nhl_season_key = sk.nhl_season_key
  ORDER BY lr.player_id, lr.game_date DESC, lr.game_id DESC
),

-- latest per-player feature row (strictly before slate_date) from existing table
-- used to fill non-log features and as fallback for rolling fields
last_feat AS (
  SELECT DISTINCT ON (t.player_id)
    t.player_id,

    -- core rates / volume
    t.d5_sog_per60,
    t.d10_sog_per60,
    t.d20_sog_per60,
    t.attempts_d10_per60,

    -- rest / schedule
    t.rest_days,
    t.b2b_flag,

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

    t.hot_last5_flag,

    t.game_date AS prev_game_date
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

    -- core rates (prefer regular-season log windows, fallback to existing features)
    COALESCE(ls.d5_sog_per60,  lf.d5_sog_per60)  AS d5_sog_per60,
    COALESCE(ls.d10_sog_per60, lf.d10_sog_per60) AS d10_sog_per60,
    COALESCE(ls.d20_sog_per60, lf.d20_sog_per60) AS d20_sog_per60,
    COALESCE(ls.attempts_d10_per60, lf.attempts_d10_per60) AS attempts_d10_per60,

    -- rest / schedule (recomputed from regular-season prev_game_date if available)
    CASE
      WHEN ls.prev_game_date IS NOT NULL
        THEN GREATEST(0, ((SELECT d FROM params) - ls.prev_game_date))::int
      WHEN lf.prev_game_date IS NOT NULL
        THEN GREATEST(0, ((SELECT d FROM params) - lf.prev_game_date))::int
      ELSE NULL
    END AS rest_days,
    CASE
      WHEN ls.prev_game_date IS NOT NULL
        THEN ((SELECT d FROM params) - ls.prev_game_date = 1)
      WHEN lf.prev_game_date IS NOT NULL
        THEN ((SELECT d FROM params) - lf.prev_game_date = 1)
      ELSE NULL
    END AS b2b_flag,

    -- pace
    lf.pace_index,
    lf.pace_matchup_index,

    -- team / opp environment (two refreshed from team_roll)
    COALESCE(tr.team_d10_sf_per_game,        lf.team_d10_sf_per_game)        AS team_d10_sf_per_game,
    COALESCE(tr.opp_d10_sf_allowed_per_game, lf.opp_d10_sf_allowed_per_game) AS opp_d10_sf_allowed_per_game,
    lf.opp_d10_sf_per60,
    lf.team_d10_sa_per60,
    lf.opp_d10_sa_per60,

    -- role
    lf.role_pp_share,

    -- season / szn TOI & shifts
    lf.szn_toi_per_game_5on5,
    lf.szn_toi_per_game_pp,
    lf.szn_toi_per_game_pk,
    lf.szn_shifts_per_game_5on5,
    lf.szn_shifts_per_game_pp,
    lf.szn_shifts_per_game_pk,

    lf.season_5on5_icetime_per_game,
    lf.season_5on4_icetime_per_game,
    lf.season_4on5_icetime_per_game,
    lf.season_5on5_shifts_per_game,
    lf.season_5on4_shifts_per_game,
    lf.season_4on5_shifts_per_game,

    -- team top-line shares
    lf.team_szn_5on5_top_line_xgf_share,
    lf.team_5v5_top_line_icetime_share,
    lf.team_5v5_top_line_shotattempts_share,

    -- last-10 / last-5 team counts (unchanged – still from team context)
    lf.last10_team_sog_share,
    lf.team_num_sog_last10,
    lf.team_num_event_last10,

    -- player SOG/event rolling (prefer regular-season logs, fallback to last_feat)
    COALESCE(ls.num_sog_last5,  lf.num_sog_last5)  AS num_sog_last5,
    COALESCE(ls.num_sog_last10, lf.num_sog_last10) AS num_sog_last10,
    COALESCE(ls.num_sog_szn_to_date, lf.num_sog_szn_to_date) AS num_sog_szn_to_date,

    COALESCE(ls.num_event_last5,  lf.num_event_last5)  AS num_event_last5,
    COALESCE(ls.num_event_last10, lf.num_event_last10) AS num_event_last10,
    COALESCE(ls.num_event_szn_to_date, lf.num_event_szn_to_date) AS num_event_szn_to_date,

    lf.hot_last5_flag
  FROM skaters s
  LEFT JOIN last_feat lf ON lf.player_id = s.player_id
  LEFT JOIN team_roll tr ON tr.team_id = s.team_id
  LEFT JOIN log_snap ls ON ls.player_id = s.player_id
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

INSERT INTO nhl.training_features_sog_denali AS t (
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,

  shots_on_goal,

  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,

  rest_days,
  b2b_flag,

  pace_index,
  pace_matchup_index,

  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,
  opp_d10_sf_per60,
  team_d10_sa_per60,
  opp_d10_sa_per60,

  role_pp_share,

  szn_toi_per_game_5on5,
  szn_toi_per_game_pp,
  szn_toi_per_game_pk,
  szn_shifts_per_game_5on5,
  szn_shifts_per_game_pp,
  szn_shifts_per_game_pk,

  season_5on5_icetime_per_game,
  season_5on4_icetime_per_game,
  season_4on5_icetime_per_game,
  season_5on5_shifts_per_game,
  season_5on4_shifts_per_game,
  season_4on5_shifts_per_game,

  team_szn_5on5_top_line_xgf_share,
  team_5v5_top_line_icetime_share,
  team_5v5_top_line_shotattempts_share,

  last10_team_sog_share,
  team_num_sog_last10,
  team_num_event_last10,

  num_sog_last5,
  num_sog_last10,
  num_sog_szn_to_date,

  num_event_last5,
  num_event_last10,
  num_event_szn_to_date,

  hot_last5_flag
)
SELECT
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,

  shots_on_goal,

  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,

  rest_days,
  b2b_flag,

  pace_index,
  pace_matchup_index,

  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,
  opp_d10_sf_per60,
  team_d10_sa_per60,
  opp_d10_sa_per60,

  role_pp_share,

  szn_toi_per_game_5on5,
  szn_toi_per_game_pp,
  szn_toi_per_game_pk,
  szn_shifts_per_game_5on5,
  szn_shifts_per_game_pp,
  szn_shifts_per_game_pk,

  season_5on5_icetime_per_game,
  season_5on4_icetime_per_game,
  season_4on5_icetime_per_game,
  season_5on5_shifts_per_game,
  season_5on4_shifts_per_game,
  season_4on5_shifts_per_game,

  team_szn_5on5_top_line_xgf_share,
  team_5v5_top_line_icetime_share,
  team_5v5_top_line_shotattempts_share,

  last10_team_sog_share,
  team_num_sog_last10,
  team_num_event_last10,

  num_sog_last5,
  num_sog_last10,
  num_sog_szn_to_date,

  num_event_last5,
  num_event_last10,
  num_event_szn_to_date,

  hot_last5_flag
FROM seed_one
ON CONFLICT (player_id, game_id) DO UPDATE
SET team_id                         = EXCLUDED.team_id,
    opponent_id                     = EXCLUDED.opponent_id,
    is_home                         = EXCLUDED.is_home,
    game_date                       = EXCLUDED.game_date,
    shots_on_goal                   = EXCLUDED.shots_on_goal,
    d5_sog_per60                    = EXCLUDED.d5_sog_per60,
    d10_sog_per60                   = EXCLUDED.d10_sog_per60,
    d20_sog_per60                   = EXCLUDED.d20_sog_per60,
    attempts_d10_per60              = EXCLUDED.attempts_d10_per60,
    rest_days                       = EXCLUDED.rest_days,
    b2b_flag                        = EXCLUDED.b2b_flag,
    pace_index                      = EXCLUDED.pace_index,
    pace_matchup_index              = EXCLUDED.pace_matchup_index,
    team_d10_sf_per_game            = EXCLUDED.team_d10_sf_per_game,
    opp_d10_sf_allowed_per_game     = EXCLUDED.opp_d10_sf_allowed_per_game,
    opp_d10_sf_per60                = EXCLUDED.opp_d10_sf_per60,
    team_d10_sa_per60               = EXCLUDED.team_d10_sa_per60,
    opp_d10_sa_per60                = EXCLUDED.opp_d10_sa_per60,
    role_pp_share                   = EXCLUDED.role_pp_share,
    szn_toi_per_game_5on5           = EXCLUDED.szn_toi_per_game_5on5,
    szn_toi_per_game_pp             = EXCLUDED.szn_toi_per_game_pp,
    szn_toi_per_game_pk             = EXCLUDED.szn_toi_per_game_pk,
    szn_shifts_per_game_5on5        = EXCLUDED.szn_shifts_per_game_5on5,
    szn_shifts_per_game_pp          = EXCLUDED.szn_shifts_per_game_pp,
    szn_shifts_per_game_pk          = EXCLUDED.szn_shifts_per_game_pk,
    season_5on5_icetime_per_game    = EXCLUDED.season_5on5_icetime_per_game,
    season_5on4_icetime_per_game    = EXCLUDED.season_5on4_icetime_per_game,
    season_4on5_icetime_per_game    = EXCLUDED.season_4on5_icetime_per_game,
    season_5on5_shifts_per_game     = EXCLUDED.season_5on5_shifts_per_game,
    season_5on4_shifts_per_game     = EXCLUDED.season_5on4_shifts_per_game,
    season_4on5_shifts_per_game     = EXCLUDED.season_4on5_shifts_per_game,
    team_szn_5on5_top_line_xgf_share= EXCLUDED.team_szn_5on5_top_line_xgf_share,
    team_5v5_top_line_icetime_share = EXCLUDED.team_5v5_top_line_icetime_share,
    team_5v5_top_line_shotattempts_share = EXCLUDED.team_5v5_top_line_shotattempts_share,
    last10_team_sog_share           = EXCLUDED.last10_team_sog_share,
    team_num_sog_last10             = EXCLUDED.team_num_sog_last10,
    team_num_event_last10           = EXCLUDED.team_num_event_last10,
    num_sog_last5                   = EXCLUDED.num_sog_last5,
    num_sog_last10                  = EXCLUDED.num_sog_last10,
    num_sog_szn_to_date             = EXCLUDED.num_sog_szn_to_date,
    num_event_last5                 = EXCLUDED.num_event_last5,
    num_event_last10                = EXCLUDED.num_event_last10,
    num_event_szn_to_date           = EXCLUDED.num_event_szn_to_date,
    hot_last5_flag                  = EXCLUDED.hot_last5_flag;

\echo 'seed_sog_features_for_slate: upserted rows for slate_date=' :'slate_date'
SELECT COUNT(*) FROM nhl.training_features_sog_denali WHERE game_date = :'slate_date'::date;
