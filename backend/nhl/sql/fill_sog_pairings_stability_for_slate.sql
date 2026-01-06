-- ============================================================
-- Fill rolling "top-mate stability" features for a slate
--
-- Source of mate IDs: nhl.shiftcharts_pairings_game
--
-- Writes:
--   d10_top_mate_repeat_rate
--   d20_top_mate_repeat_rate
--
-- Run (psql):
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD \
--     -f backend/nhl/sql/fill_sog_pairings_stability_for_slate.sql
-- ============================================================

\set ON_ERROR_STOP on

WITH base AS (
  SELECT
    g.game_date,
    p.player_id,
    p.game_id,
    p.top_mate_player_id
  FROM nhl.shiftcharts_pairings_game p
  JOIN nhl.games g USING (game_id)
  WHERE p.top_mate_player_id IS NOT NULL
),
prev AS (
  SELECT
    b.*,
    LAG(b.top_mate_player_id) OVER (
      PARTITION BY b.player_id
      ORDER BY b.game_date, b.game_id
    ) AS prev_top_mate_player_id
  FROM base b
),
roll AS (
  SELECT
    p.player_id,
    p.game_id,
    AVG(
      CASE
        WHEN p.prev_top_mate_player_id IS NULL THEN NULL
        WHEN p.top_mate_player_id = p.prev_top_mate_player_id THEN 1.0
        ELSE 0.0
      END
    ) OVER (
      PARTITION BY p.player_id
      ORDER BY p.game_date, p.game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS d10_repeat_rate,
    AVG(
      CASE
        WHEN p.prev_top_mate_player_id IS NULL THEN NULL
        WHEN p.top_mate_player_id = p.prev_top_mate_player_id THEN 1.0
        ELSE 0.0
      END
    ) OVER (
      PARTITION BY p.player_id
      ORDER BY p.game_date, p.game_id
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS d20_repeat_rate
  FROM prev p
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_top_mate_repeat_rate = r.d10_repeat_rate,
  d20_top_mate_repeat_rate = r.d20_repeat_rate
FROM roll r
WHERE t.game_date = DATE :'slate_date'
  AND r.game_id   = t.game_id
  AND r.player_id = t.player_id;
