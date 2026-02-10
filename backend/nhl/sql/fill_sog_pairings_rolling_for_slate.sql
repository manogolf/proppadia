-- ============================================================
-- backend/nhl/sql/fill_sog_pairings_rolling_for_slate.sql
-- Rolling pairings for slate day (d10/d20) computed from
-- nhl.shift_teammate_overlap_features_game + nhl.games
--
-- This replaces older dependencies on non-existent
-- nhl.shift_teammate_overlap_features_rolling_d10/d20 objects.
--
-- Run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD -f backend/nhl/sql/fill_sog_pairings_rolling_for_slate.sql
-- ============================================================

\set ON_ERROR_STOP on
SET statement_timeout = 0;

-- Ensure columns exist (idempotent)
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_games_in_window                int NULL,

  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_games_in_window                int NULL,

  ADD COLUMN IF NOT EXISTS d10_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket            smallint NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket            smallint NULL,

  ADD COLUMN IF NOT EXISTS pairings_source                    text NULL,
  ADD COLUMN IF NOT EXISTS pairings_updated_at                timestamptz NULL;

WITH
params AS (
  SELECT (:'slate_date')::date AS slate_date
),

slate_rows AS (
  SELECT DISTINCT
    t.game_id::bigint   AS slate_game_id,
    t.player_id::bigint AS player_id,
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
  SELECT
    l.player_id::bigint AS player_id,
    pg.game_id::bigint  AS game_id,
    pg.game_date::date  AS game_date,
    pg.season::int      AS season,

    COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
    f.top_mate_overlap_share::double precision   AS top_mate_overlap_share,
    f.top3_mates_overlap_share::double precision AS top3_mates_overlap_share
  FROM prior_games pg
  JOIN nhl.skater_game_logs_raw l
    ON l.game_id = pg.game_id
  LEFT JOIN nhl.shift_teammate_overlap_features_game f
    ON f.game_id = pg.game_id
   AND f.player_id::bigint = l.player_id::bigint
  WHERE l.player_id IS NOT NULL
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
    s.season,

    r10.games_in_window                    AS d10_games_in_window,
    r10.shiftcharts_games                  AS d10_shiftcharts_games,
    (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) AS d10_shiftcharts_coverage_rate,
    r10.top_mate_overlap_share_avg         AS d10_top_mate_overlap_share_avg,
    r10.top_mate_overlap_share_std         AS d10_top_mate_overlap_share_std,
    r10.top3_mates_overlap_share_avg       AS d10_top3_mates_overlap_share_avg,
    r10.top3_mates_overlap_share_std       AS d10_top3_mates_overlap_share_std,

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

UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_games_in_window              = f.d10_games_in_window,
  d10_shiftcharts_games            = f.d10_shiftcharts_games,
  d10_shiftcharts_coverage_rate    = f.d10_shiftcharts_coverage_rate,
  d10_top_mate_overlap_share_avg   = f.d10_top_mate_overlap_share_avg,
  d10_top_mate_overlap_share_std   = f.d10_top_mate_overlap_share_std,
  d10_top3_mates_overlap_share_avg = f.d10_top3_mates_overlap_share_avg,
  d10_top3_mates_overlap_share_std = f.d10_top3_mates_overlap_share_std,

  d20_games_in_window              = f.d20_games_in_window,
  d20_shiftcharts_games            = f.d20_shiftcharts_games,
  d20_shiftcharts_coverage_rate    = f.d20_shiftcharts_coverage_rate,
  d20_top_mate_overlap_share_avg   = f.d20_top_mate_overlap_share_avg,
  d20_top_mate_overlap_share_std   = f.d20_top_mate_overlap_share_std,
  d20_top3_mates_overlap_share_avg = f.d20_top3_mates_overlap_share_avg,
  d20_top3_mates_overlap_share_std = f.d20_top3_mates_overlap_share_std,

pairings_source = CASE
  WHEN t.pairings_source IS NULL OR t.pairings_source = '' THEN 'pairings_features_game_d10d20'
  WHEN t.pairings_source LIKE '%pairings_features_game_d10d20%' THEN t.pairings_source
  ELSE t.pairings_source || ';pairings_features_game_d10d20'
END,
  pairings_updated_at              = now()
FROM final f
WHERE t.game_date::date = (:'slate_date')::date
  AND t.game_id::bigint = f.game_id
  AND t.player_id::bigint = f.player_id;

-- Missingness-aware post-pass (idempotent + source-stamped)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_pairings_available = (t.d10_shiftcharts_games IS NOT NULL AND t.d10_shiftcharts_games > 0),
  d20_pairings_available = (t.d20_shiftcharts_games IS NOT NULL AND t.d20_shiftcharts_games > 0),

  -- keep your numeric buckets 0..3
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
  END,

  -- IMPORTANT: idempotent suffix append (prevents repeated token spam)
  pairings_source = CASE
    WHEN COALESCE(t.pairings_source, '') = '' THEN 'pairings_features_game_d10d20'
    WHEN t.pairings_source LIKE '%pairings_features_game_d10d20%' THEN t.pairings_source
    ELSE t.pairings_source || ';' || 'pairings_features_game_d10d20'
  END,
  pairings_updated_at = NOW()
WHERE t.game_date::date = (:'slate_date')::date;