-- ============================================================
-- FILE: backend/nhl/sql/create_shift_teammate_overlap_features_missingness_aware.sql
--
-- PURPOSE:
--   Replace the rollup view with a missingness-aware version:
--     - shiftcharts_available boolean
--     - overlap features NULL when shiftcharts missing for player/game
--
-- HOW TO RUN:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/sql/create_shift_teammate_overlap_features_missingness_aware.sql
-- ============================================================

\set ON_ERROR_STOP on

DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_game;

CREATE VIEW nhl.shift_teammate_overlap_features_game AS
WITH
-- Base set of player-games you care about (aligns with your logs table)
base_players AS (
  SELECT
    s.game_id,
    s.player_id::bigint AS player_id,
    ROUND(COALESCE(s.toi_minutes,0) * 60.0)::int AS toi_sec
  FROM nhl.skater_game_logs_raw s
),

-- Does shiftcharts exist for this specific player-game?
sc_present AS (
  SELECT DISTINCT
    r.game_id,
    r.player_id::bigint AS player_id
  FROM nhl.shiftcharts_raw r
  WHERE r.start_sec IS NOT NULL
    AND r.end_sec IS NOT NULL
    AND r.end_sec > r.start_sec
    AND COALESCE(r.duration_sec, (r.end_sec - r.start_sec)) < 600  -- drop obvious goalie-long segments
),

-- Rank overlaps for top1/top3 rollups (already computed in the overlap table)
ranked AS (
  SELECT
    e.game_id,
    e.player_id::bigint AS player_id,
    e.mate_id::bigint   AS mate_id,
    e.overlap_sec,
    ROW_NUMBER() OVER (
      PARTITION BY e.game_id, e.player_id
      ORDER BY e.overlap_sec DESC, e.mate_id
    ) AS rn
  FROM nhl.shift_teammate_overlap_game e
),

agg AS (
  SELECT
    game_id,
    player_id,
    MAX(overlap_sec) FILTER (WHERE rn = 1)   AS top_mate_overlap_sec,
    SUM(overlap_sec) FILTER (WHERE rn <= 3)  AS top3_mates_overlap_sec
  FROM ranked
  GROUP BY 1,2
)

SELECT
  bp.game_id,
  bp.player_id,
  bp.toi_sec,

  -- missingness flag
  (sp.player_id IS NOT NULL) AS shiftcharts_available,

  -- If no shiftcharts for that player-game, leave NULLs (not zeros)
  CASE WHEN sp.player_id IS NOT NULL THEN COALESCE(a.top_mate_overlap_sec, 0) ELSE NULL END AS top_mate_overlap_sec,
  CASE WHEN sp.player_id IS NOT NULL THEN COALESCE(a.top3_mates_overlap_sec, 0) ELSE NULL END AS top3_mates_overlap_sec,

  CASE
    WHEN sp.player_id IS NULL THEN NULL
    WHEN COALESCE(bp.toi_sec,0) > 0 THEN (COALESCE(a.top_mate_overlap_sec,0)::double precision / bp.toi_sec)
    ELSE NULL
  END AS top_mate_overlap_share,

  CASE
    WHEN sp.player_id IS NULL THEN NULL
    WHEN COALESCE(bp.toi_sec,0) > 0 THEN (COALESCE(a.top3_mates_overlap_sec,0)::double precision / bp.toi_sec)
    ELSE NULL
  END AS top3_mates_overlap_share

FROM base_players bp
LEFT JOIN sc_present sp
  ON sp.game_id = bp.game_id AND sp.player_id = bp.player_id
LEFT JOIN agg a
  ON a.game_id = bp.game_id AND a.player_id = bp.player_id;
