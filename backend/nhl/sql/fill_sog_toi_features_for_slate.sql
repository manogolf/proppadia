-- ============================================================
-- fill_sog_toi_features_for_slate.sql
-- Adds TOI-based exposure  stability  trend features (NO PP/EV/SH splits)
-- Target: nhl.training_features_nhl_sog_enriched_pregame_v2 (rows for :slate_date)
-- Designed for psql execution (called from backend/nhl/cli.py via run_psql_file)
-- Usage:
--   psql "$SUPABASE_DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 \
--     -v "slate_date=2025-12-23" \
--     -f backend/nhl/sql/fill_sog_toi_features_for_slate.sql
-- ============================================================

BEGIN;

-- 1) Ensure columns exist (idempotent)
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d3_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d5_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d20_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_min_sd  numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_cv      numeric,
  ADD COLUMN IF NOT EXISTS toi_trend_3v10  numeric;

-- 2) Compute TOI features from realized history (exclude slate day itself)
WITH params AS (
  SELECT (:'slate_date')::date AS slate_date
),
slate AS (
  SELECT
    t.player_id::bigint AS player_id,
    t.season::int       AS season
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN params p ON TRUE
  WHERE t.game_date = p.slate_date
),
hist AS (
  SELECT
    l.player_id::bigint AS player_id,
    g.season::int       AS season,
    g.game_date::date   AS game_date,
    g.game_id::bigint   AS game_id,
    NULLIF(l.toi_minutes, 0)::numeric AS toi_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  JOIN params p ON TRUE
  WHERE g.game_date < p.slate_date
),
hist_ranked AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (
      PARTITION BY h.player_id, h.season
      ORDER BY h.game_date DESC, h.game_id DESC
    ) AS rn_desc
  FROM hist h
  JOIN slate s
    ON s.player_id = h.player_id
   AND s.season    = h.season
  WHERE h.toi_min IS NOT NULL
),
aggs AS (
  SELECT
    player_id,
    season,
    AVG(CASE WHEN rn_desc <= 3  THEN toi_min END) AS d3_toi_min_avg,
    AVG(CASE WHEN rn_desc <= 5  THEN toi_min END) AS d5_toi_min_avg,
    AVG(CASE WHEN rn_desc <= 10 THEN toi_min END) AS d10_toi_min_avg,
    AVG(CASE WHEN rn_desc <= 20 THEN toi_min END) AS d20_toi_min_avg,
    STDDEV_SAMP(CASE WHEN rn_desc <= 10 THEN toi_min END) AS d10_toi_min_sd,
    (AVG(CASE WHEN rn_desc <= 3  THEN toi_min END)
     - AVG(CASE WHEN rn_desc <= 10 THEN toi_min END)) AS toi_trend_3v10
  FROM hist_ranked
  GROUP BY 1,2
),
final AS (
  SELECT
    a.*,
    CASE
      WHEN a.d10_toi_min_avg IS NULL OR a.d10_toi_min_avg = 0 THEN NULL
      WHEN a.d10_toi_min_sd  IS NULL THEN NULL
      ELSE a.d10_toi_min_sd / a.d10_toi_min_avg
    END AS d10_toi_cv
  FROM aggs a
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d3_toi_min_avg = f.d3_toi_min_avg,
  d5_toi_min_avg = f.d5_toi_min_avg,
  d10_toi_min_avg = f.d10_toi_min_avg,
  d20_toi_min_avg = f.d20_toi_min_avg,
  d10_toi_min_sd  = f.d10_toi_min_sd,
  d10_toi_cv      = f.d10_toi_cv,
  toi_trend_3v10  = f.toi_trend_3v10
FROM params p, final f
WHERE t.game_date = p.slate_date
  AND f.player_id = t.player_id::bigint
  AND f.season    = t.season::int;

COMMIT;

-- Optional quick check (run manually if you want):
-- SELECT
--   COUNT(*) AS rows_slate,
--   COUNT(d10_toi_min_avg) AS nn_d10_toi,
--   COUNT(d10_toi_min_sd)  AS nn_sd10,
--   COUNT(d10_toi_cv)      AS nn_cv10,
--   MIN(d10_toi_min_avg)   AS min_d10_toi,
--   MAX(d10_toi_min_avg)   AS max_d10_toi
-- FROM nhl.training_features_nhl_sog_enriched_pregame_v2
-- WHERE game_date = (:'slate_date')::date;
