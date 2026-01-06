-- backend/nhl/sql/create_shift_teammate_overlap_rolling_features.sql
--
-- RUN:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/sql/create_shift_teammate_overlap_rolling_features.sql
--
-- Purpose:
--   Build pregame-safe rolling teammate-overlap features from shiftcharts-derived
--   per-game overlap metrics, using ONLY prior games for each player.
--
-- Requires:
--   - nhl.games (game_id, game_date)
--   - nhl.skater_game_logs_raw (player_id, game_id)  -- the “spine” of player-games
--   - nhl.shift_teammate_overlap_features_game (player_id, game_id, shiftcharts_available,
--       top_mate_overlap_share, top3_mates_overlap_share)

CREATE SCHEMA IF NOT EXISTS nhl;

DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_rolling_d10;

CREATE VIEW nhl.shift_teammate_overlap_features_rolling_d10 AS
WITH base AS (
  SELECT
    s.player_id,
    s.game_id,
    g.game_date,

    -- same-game availability + values (may be NULL if no shiftcharts for that game)
    TRUE AS shiftcharts_available,
    f.top_mate_overlap_share,
    f.top3_mates_overlap_share
  FROM nhl.skater_game_logs_raw s
  JOIN nhl.games g
    ON g.game_id = s.game_id
  LEFT JOIN nhl.shift_teammate_overlap_features_game f
    ON f.player_id = s.player_id
   AND f.game_id   = s.game_id
)
SELECT
  player_id,
  game_id,
  game_date,

  -- keep same-game flag around (can be useful for debugging / training)
  COALESCE(shiftcharts_available, false) AS shiftcharts_available,

  -- ---------- Rolling (D10) using ONLY PRIOR GAMES ----------
  -- Window frame: last 10 rows (games) strictly before the current game.
  -- Missingness-aware:
  --   - averages/stddevs use FILTER (WHERE shiftcharts_available) so they return NULL
  --     if none of the prior games in-window have shiftcharts.
  --   - counts tell you how much coverage you actually had.
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

  (COUNT(*) FILTER (WHERE shiftcharts_available) OVER w_prev10)::float
    / NULLIF((COUNT(*) OVER w_prev10)::float, 0.0)
    AS d10_shiftcharts_coverage_rate

FROM base
WINDOW w_prev10 AS (
  PARTITION BY player_id
  ORDER BY game_date, game_id
  ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
);

-- ============================================================
-- VERIFICATIONS (copy/paste after running if you want)
-- ============================================================

-- 1) View exists + rowcount sanity
-- SELECT COUNT(*) AS rows FROM nhl.shift_teammate_overlap_features_rolling_d10;

-- 2) Spot-check: a game WITH shiftcharts available should still have rolling values
--    based only on prior games (not current). Pick a known-covered game_id, e.g. 2025020580.
-- SELECT *
-- FROM nhl.shift_teammate_overlap_features_rolling_d10
-- WHERE game_id = 2025020580
-- ORDER BY d10_shiftcharts_games DESC NULLS LAST
-- LIMIT 20;

-- 3) Spot-check: a game with NO shiftcharts should have shiftcharts_available=false
--    and the rolling features may still be non-NULL if prior games had coverage.
-- SELECT *
-- FROM nhl.shift_teammate_overlap_features_rolling_d10
-- WHERE game_id = 2025020057
-- ORDER BY d10_shiftcharts_games DESC NULLS LAST
-- LIMIT 20;

-- 4) Coverage distribution (how often we have usable rolling overlap features)
-- SELECT
--   AVG((d10_top_mate_overlap_share_avg IS NOT NULL)::int) AS pct_with_d10_top_mate_avg,
--   AVG(COALESCE(d10_shiftcharts_coverage_rate,0))         AS avg_d10_coverage_rate
-- FROM nhl.shift_teammate_overlap_features_rolling_d10;

-- 5) Confirm “pregame-safe”: current game never contributes (by construction).
--    Quick check: when a player has ONLY 1 game so far, window is empty => NULL rolling.
-- SELECT *
-- FROM nhl.shift_teammate_overlap_features_rolling_d10
-- WHERE d10_games_in_window = 0
-- LIMIT 20;
