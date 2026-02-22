-- ============================================================
-- backend/nhl/sql/create_shift_teammate_overlap_rolling_features.sql
--
-- Purpose:
--   Build pregame-safe rolling teammate-overlap features from shiftcharts-derived
--   per-game overlap metrics, using ONLY prior games for each player.
--
-- Requires:
--   - nhl.games (game_id, game_date)
--   - nhl.skater_game_logs_raw (player_id, game_id)  -- spine of player-games
--   - nhl.shift_teammate_overlap_features_game (
--       player_id, game_id,
--       shiftcharts_available, top_mate_overlap_share, top3_mates_overlap_share
--     )
-- ============================================================

\set ON_ERROR_STOP on
SET statement_timeout = 0;

CREATE SCHEMA IF NOT EXISTS nhl;

-- ----------------------------
-- D10
-- ----------------------------
DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_rolling_d10;

CREATE VIEW nhl.shift_teammate_overlap_features_rolling_d10 AS
WITH spine AS (
  SELECT DISTINCT
    s.player_id::bigint AS player_id,
    s.game_id::bigint   AS game_id
  FROM nhl.skater_game_logs_raw s
  WHERE s.player_id IS NOT NULL
    AND s.game_id   IS NOT NULL
),
base AS (
  SELECT
    sp.player_id,
    sp.game_id,
    g.game_date::date AS game_date,

    -- availability + values (NULL when no shiftcharts for that game)
    COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
    f.top_mate_overlap_share,
    f.top3_mates_overlap_share
  FROM spine sp
  JOIN nhl.games g
    ON g.game_id = sp.game_id
  LEFT JOIN nhl.shift_teammate_overlap_features_game f
    ON f.player_id::bigint = sp.player_id
   AND f.game_id::bigint   = sp.game_id
)
SELECT
  player_id,
  game_id,
  game_date,
  shiftcharts_available,

  -- rolling d10 over PRIOR games only
  AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev10
    AS d10_top_mate_overlap_share_avg,

  STDDEV_SAMP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev10
    AS d10_top_mate_overlap_share_std,

  AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev10
    AS d10_top3_mates_overlap_share_avg,

  STDDEV_SAMP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev10
    AS d10_top3_mates_overlap_share_std,

  COUNT(*) OVER w_prev10
    AS d10_games_in_window,

  COUNT(*) FILTER (WHERE shiftcharts_available) OVER w_prev10
    AS d10_shiftcharts_games,

  (COUNT(*) FILTER (WHERE shiftcharts_available) OVER w_prev10)::double precision
    / NULLIF((COUNT(*) OVER w_prev10)::double precision, 0.0)
    AS d10_shiftcharts_coverage_rate

FROM base
WINDOW w_prev10 AS (
  PARTITION BY player_id
  ORDER BY game_date, game_id
  ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
);

-- ----------------------------
-- D20
-- ----------------------------
DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_rolling_d20;

CREATE VIEW nhl.shift_teammate_overlap_features_rolling_d20 AS
WITH spine AS (
  SELECT DISTINCT
    s.player_id::bigint AS player_id,
    s.game_id::bigint   AS game_id
  FROM nhl.skater_game_logs_raw s
  WHERE s.player_id IS NOT NULL
    AND s.game_id   IS NOT NULL
),
base AS (
  SELECT
    sp.player_id,
    sp.game_id,
    g.game_date::date AS game_date,

    COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
    f.top_mate_overlap_share,
    f.top3_mates_overlap_share
  FROM spine sp
  JOIN nhl.games g
    ON g.game_id = sp.game_id
  LEFT JOIN nhl.shift_teammate_overlap_features_game f
    ON f.player_id::bigint = sp.player_id
   AND f.game_id::bigint   = sp.game_id
)
SELECT
  player_id,
  game_id,
  game_date,
  shiftcharts_available,

  -- rolling d20 over PRIOR games only
  AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev20
    AS d20_top_mate_overlap_share_avg,

  STDDEV_SAMP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev20
    AS d20_top_mate_overlap_share_std,

  AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev20
    AS d20_top3_mates_overlap_share_avg,

  STDDEV_SAMP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available) OVER w_prev20
    AS d20_top3_mates_overlap_share_std,

  COUNT(*) OVER w_prev20
    AS d20_games_in_window,

  COUNT(*) FILTER (WHERE shiftcharts_available) OVER w_prev20
    AS d20_shiftcharts_games,

  (COUNT(*) FILTER (WHERE shiftcharts_available) OVER w_prev20)::double precision
    / NULLIF((COUNT(*) OVER w_prev20)::double precision, 0.0)
    AS d20_shiftcharts_coverage_rate

FROM base
WINDOW w_prev20 AS (
  PARTITION BY player_id
  ORDER BY game_date, game_id
  ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
);
