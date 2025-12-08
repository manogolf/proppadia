-- backend/nhl/sql/rebuild_sog_denali_from_logs.sql
--
-- Recompute SOG rolling features in nhl.training_features_sog_denali
-- directly from nhl.skater_game_logs_raw + nhl.games.
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
--     -v start_date=2025-10-01 \
--     -v end_date=2025-12-31 \
--     -f backend/nhl/sql/rebuild_sog_denali_from_logs.sql
--
-- This updates (for rows whose game_date is in [start_date, end_date]):
--   - d5_sog_per60, d10_sog_per60, d20_sog_per60
--   - num_sog_last5, num_sog_last10, num_sog_szn_to_date
--   - num_event_last5, num_event_last10, num_event_szn_to_date

\set QUIET on
\set ON_ERROR_STOP on
\pset tuples_only on
\pset pager off

WITH params AS (
  SELECT
    :'start_date'::date AS start_date,
    :'end_date'::date   AS end_date
),

-- Base logs: one row per skater/game with SOG, attempts, TOI, and season/game_date.
logs AS (
  SELECT
    g.season,
    l.game_id,
    l.player_id,
    COALESCE(l.game_date, g.game_date::date) AS game_date,

    -- We assume these are already joined to team/opponent/is_home elsewhere;
    -- for rolling SOG we only need SOG/attempts/TOI.
    COALESCE(l.shots_on_goal, 0)::numeric AS shots_on_goal,
    COALESCE(l.shot_attempts, 0)::numeric AS shot_attempts,
    NULLIF(l.toi_minutes, 0)::numeric     AS toi_minutes
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  -- Optional: restrict to regular season only if game_type is available.
  -- WHERE g.game_type = 2
),

-- Ordered per-player history across ALL seasons.
ordered AS (
  SELECT
    logs.*,

    -- Rolling sums of SOG over previous games (not including current row).
    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS sog_last5,

    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS sog_last10,

    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS sog_last20,

    -- Rolling sums of "event shots" (attempts) over previous games.
    SUM(shot_attempts) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS ev_last5,

    SUM(shot_attempts) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS ev_last10,

    -- Season-to-date sums up to, but not including, the current game.
    SUM(shots_on_goal) OVER (
      PARTITION BY player_id, season
      ORDER BY game_date, game_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS sog_szn_to_date,

    SUM(shot_attempts) OVER (
      PARTITION BY player_id, season
      ORDER BY game_date, game_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS ev_szn_to_date,

    -- Rolling sums of TOI (minutes) for per-60 rates.
    SUM(COALESCE(toi_minutes, 0)) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS toi_last5,

    SUM(COALESCE(toi_minutes, 0)) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS toi_last10,

    SUM(COALESCE(toi_minutes, 0)) OVER (
      PARTITION BY player_id
      ORDER BY season, game_date, game_id
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS toi_last20

  FROM logs
),

calc AS (
  SELECT
    o.season,
    o.game_id,
    o.player_id,
    o.game_date,

    -- Per-60 rates from prior N games.
    CASE
      WHEN o.toi_last5 > 0
        THEN 60.0 * o.sog_last5 / o.toi_last5
      ELSE NULL
    END AS d5_sog_per60,

    CASE
      WHEN o.toi_last10 > 0
        THEN 60.0 * o.sog_last10 / o.toi_last10
      ELSE NULL
    END AS d10_sog_per60,

    CASE
      WHEN o.toi_last20 > 0
        THEN 60.0 * o.sog_last20 / o.toi_last20
      ELSE NULL
    END AS d20_sog_per60,

    -- Rolling counts (use sums as integers; COALESCE to 0 for early games).
    COALESCE(o.sog_last5,       0)::int AS num_sog_last5,
    COALESCE(o.sog_last10,      0)::int AS num_sog_last10,
    COALESCE(o.sog_szn_to_date, 0)::int AS num_sog_szn_to_date,

    COALESCE(o.ev_last5,        0)::int AS num_event_last5,
    COALESCE(o.ev_last10,       0)::int AS num_event_last10,
    COALESCE(o.ev_szn_to_date,  0)::int AS num_event_szn_to_date

  FROM ordered o
),

-- Restrict to the range we actually want to fix in the feature table.
target_rows AS (
  SELECT c.*
  FROM calc c
  JOIN params p
    ON c.game_date BETWEEN p.start_date AND p.end_date
)

UPDATE nhl.training_features_sog_denali t
SET
  d5_sog_per60          = tr.d5_sog_per60,
  d10_sog_per60         = tr.d10_sog_per60,
  d20_sog_per60         = tr.d20_sog_per60,
  num_sog_last5         = tr.num_sog_last5,
  num_sog_last10        = tr.num_sog_last10,
  num_sog_szn_to_date   = tr.num_sog_szn_to_date,
  num_event_last5       = tr.num_event_last5,
  num_event_last10      = tr.num_event_last10,
  num_event_szn_to_date = tr.num_event_szn_to_date
FROM target_rows tr
WHERE t.player_id = tr.player_id
  AND t.game_id   = tr.game_id;

\echo 'rebuild_sog_denali_from_logs: updated rows in nhl.training_features_sog_denali for range ['
\echo :'start_date'
\echo ','
\echo :'end_date'
\echo ']'
