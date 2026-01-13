-- ============================================================
-- backend/nhl/sql/fill_sog_pairings_for_slate.sql
-- Fill slate-day rolling pairing (teammate overlap) features onto the SOG pregame table.
--
-- Run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD -f backend/nhl/sql/fill_sog_pairings_for_slate.sql
--
-- Notes:
--   - d0 (same-game pairings) requires same-day shiftcharts; SKIP in pregame.
--   - d10/d20 are computed from prior games in the same season using:
--       nhl.shift_teammate_overlap_features_game + nhl.games
-- ============================================================

\set ON_ERROR_STOP on
SET statement_timeout = 0;

-- ============================================================
-- 0) Ensure required columns exist (idempotent)
-- ============================================================
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  -- d0 (same-game pairings) [pregame: intentionally not filled]
  ADD COLUMN IF NOT EXISTS d0_top_mate_player_id              bigint  NULL,
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_sec            int     NULL,
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_share          numeric NULL,
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_avg          numeric NULL,
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_std          numeric NULL,

  -- rolling pairings (d10)
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_games_in_window                int NULL,

  -- rolling pairings (d20)
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_games_in_window                int NULL,

  -- missingness-aware features
  ADD COLUMN IF NOT EXISTS d10_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket            smallint NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket            smallint NULL,

  -- bookkeeping
  ADD COLUMN IF NOT EXISTS pairings_source                    text NULL,
  ADD COLUMN IF NOT EXISTS pairings_updated_at                timestamptz NULL;

-- ============================================================
-- 1) Compute D10/D20 rollups for slate players from prior games (same season)
-- ============================================================
WITH
params AS (
  SELECT (:'slate_date')::date AS slate_date
),

slate_rows AS (
  SELECT DISTINCT
    t.game_id::bigint   AS slate_game_id,
    t.player_id::bigint AS player_id,
    t.game_date::date   AS slate_date,
    g.season::int       AS season
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN nhl.games g
    ON g.game_id = t.game_id
  JOIN params ON TRUE
  WHERE t.game_date::date = params.slate_date
    AND t.player_id IS NOT NULL
),

prior_games AS (
  SELECT
    g.game_id::bigint   AS game_id,
    g.game_date::date   AS game_date,
    g.season::int       AS season
  FROM nhl.games g
  JOIN params ON TRUE
  JOIN (SELECT DISTINCT season FROM slate_rows) ss
    ON ss.season = g.season
  WHERE g.game_date::date < params.slate_date
),

per_game AS (
  -- Per-player per-prior-game pairing features (game_date comes from nhl.games)
  SELECT
    f.player_id::bigint AS player_id,
    pg.game_id::bigint  AS game_id,
    pg.game_date::date  AS game_date,
    pg.season::int      AS season,

    COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
    f.top_mate_overlap_share::double precision   AS top_mate_overlap_share,
    f.top3_mates_overlap_share::double precision AS top3_mates_overlap_share
  FROM nhl.shift_teammate_overlap_features_game f
  JOIN prior_games pg
    ON pg.game_id = f.game_id
  WHERE f.player_id IS NOT NULL
),

ranked AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (PARTITION BY p.player_id ORDER BY p.game_date DESC, p.game_id DESC) AS rn
  FROM per_game p
),

roll_d10 AS (
  SELECT
    player_id,
    season,
    COUNT(*)::int AS games_in_window,
    COUNT(*) FILTER (WHERE shiftcharts_available)::int AS shiftcharts_games,

    AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_avg,
    STDDEV_POP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_std,

    AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_avg,
    STDDEV_POP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_std
  FROM ranked
  WHERE rn <= 10
  GROUP BY 1,2
),

roll_d20 AS (
  SELECT
    player_id,
    season,
    COUNT(*)::int AS games_in_window,
    COUNT(*) FILTER (WHERE shiftcharts_available)::int AS shiftcharts_games,

    AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_avg,
    STDDEV_POP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_std,

    AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_avg,
    STDDEV_POP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_std
  FROM ranked
  WHERE rn <= 20
  GROUP BY 1,2
),

final AS (
  SELECT
    s.slate_game_id AS game_id,
    s.player_id,
    s.slate_date,
    s.season,

    -- D10
    r10.games_in_window                    AS d10_games_in_window,
    r10.shiftcharts_games                  AS d10_shiftcharts_games,
    (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) AS d10_shiftcharts_coverage_rate,
    r10.top_mate_overlap_share_avg         AS d10_top_mate_overlap_share_avg,
    r10.top_mate_overlap_share_std         AS d10_top_mate_overlap_share_std,
    r10.top3_mates_overlap_share_avg       AS d10_top3_mates_overlap_share_avg,
    r10.top3_mates_overlap_share_std       AS d10_top3_mates_overlap_share_std,

    -- D20
    r20.games_in_window                    AS d20_games_in_window,
    r20.shiftcharts_games                  AS d20_shiftcharts_games,
    (r20.shiftcharts_games::double precision / NULLIF(r20.games_in_window::double precision, 0)) AS d20_shiftcharts_coverage_rate,
    r20.top_mate_overlap_share_avg         AS d20_top_mate_overlap_share_avg,
    r20.top_mate_overlap_share_std         AS d20_top_mate_overlap_share_std,
    r20.top3_mates_overlap_share_avg       AS d20_top3_mates_overlap_share_avg,
    r20.top3_mates_overlap_share_std       AS d20_top3_mates_overlap_share_std

  FROM slate_rows s
  LEFT JOIN roll_d10 r10
    ON r10.player_id = s.player_id
   AND r10.season    = s.season
  LEFT JOIN roll_d20 r20
    ON r20.player_id = s.player_id
   AND r20.season    = s.season
)

-- ============================================================
-- 2) Apply D10/D20 updates to slate rows (base table)
-- ============================================================
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  -- D10
  d10_games_in_window             = f.d10_games_in_window,
  d10_shiftcharts_games           = f.d10_shiftcharts_games,
  d10_shiftcharts_coverage_rate   = f.d10_shiftcharts_coverage_rate,
  d10_top_mate_overlap_share_avg  = f.d10_top_mate_overlap_share_avg,
  d10_top_mate_overlap_share_std  = f.d10_top_mate_overlap_share_std,
  d10_top3_mates_overlap_share_avg= f.d10_top3_mates_overlap_share_avg,
  d10_top3_mates_overlap_share_std= f.d10_top3_mates_overlap_share_std,

  -- D20
  d20_games_in_window             = f.d20_games_in_window,
  d20_shiftcharts_games           = f.d20_shiftcharts_games,
  d20_shiftcharts_coverage_rate   = f.d20_shiftcharts_coverage_rate,
  d20_top_mate_overlap_share_avg  = f.d20_top_mate_overlap_share_avg,
  d20_top_mate_overlap_share_std  = f.d20_top_mate_overlap_share_std,
  d20_top3_mates_overlap_share_avg= f.d20_top3_mates_overlap_share_avg,
  d20_top3_mates_overlap_share_std= f.d20_top3_mates_overlap_share_std,

  pairings_source                 = concat_ws(
                                     ';',
                                     nullif(t.pairings_source, ''),
                                     'pairings_features_game_d10d20'
                                   ),
  pairings_updated_at             = now()
FROM final f
WHERE t.game_date::date = (:'slate_date')::date
  AND t.game_id::bigint = f.game_id
  AND t.player_id::bigint = f.player_id;

-- ============================================================
-- 3) Missingness-aware features (compute AFTER rolling updates)
-- ============================================================
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_pairings_available = (t.d10_shiftcharts_games IS NOT NULL AND t.d10_shiftcharts_games > 0),
  d20_pairings_available = (t.d20_shiftcharts_games IS NOT NULL AND t.d20_shiftcharts_games > 0),

  d10_pairings_cov_bucket = CASE
    WHEN t.d10_shiftcharts_coverage_rate IS NULL THEN 0
    WHEN t.d10_shiftcharts_coverage_rate < 0.33 THEN 1
    WHEN t.d10_shiftcharts_coverage_rate < 0.66 THEN 2
    ELSE 3
  END,

  d20_pairings_cov_bucket = CASE
    WHEN t.d20_shiftcharts_coverage_rate IS NULL THEN 0
    WHEN t.d20_shiftcharts_coverage_rate < 0.33 THEN 1
    WHEN t.d20_shiftcharts_coverage_rate < 0.66 THEN 2
    ELSE 3
  END
WHERE t.game_date::date = (:'slate_date')::date;

-- ============================================================
-- 4) Quick coverage summary
-- ============================================================
WITH base AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(*) FILTER (WHERE d0_top_mate_overlap_share IS NOT NULL) AS n_with_d0,
    COUNT(*) FILTER (WHERE d10_shiftcharts_games IS NOT NULL AND d10_shiftcharts_games > 0) AS n_with_d10,
    COUNT(*) FILTER (WHERE d20_shiftcharts_games IS NOT NULL AND d20_shiftcharts_games > 0) AS n_with_d20
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date::date = (:'slate_date')::date
)
SELECT
  (:'slate_date')::date AS game_date,
  n_rows,
  n_with_d0,
  (n_rows - n_with_d0)  AS n_missing_d0,
  n_with_d10,
  (n_rows - n_with_d10) AS n_missing_d10,
  n_with_d20,
  (n_rows - n_with_d20) AS n_missing_d20
FROM base;
