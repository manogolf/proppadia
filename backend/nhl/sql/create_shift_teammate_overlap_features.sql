-- ============================================================
-- FILE: nhl/sql/create_shift_teammate_overlap_features.sql
-- PURPOSE:
--   Compute teammate overlap seconds from shiftcharts_raw and
--   roll up into per-(player_id, game_id) features:
--     - top_mate_overlap_sec
--     - top_mate_overlap_share
--     - top3_mates_overlap_share
--
-- NOTES:
--   - Uses shift interval overlaps (start_sec/end_sec).
--   - Filters out obvious goalie "20:00" shifts via duration_sec >= 600.
--   - Uses skater_game_logs_raw.toi_minutes as denominator (more reliable).
--
-- HOW TO RUN:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/sql/create_shift_teammate_overlap_features.sql
-- ============================================================

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS nhl;

-- 1) Edge table: teammate overlap seconds per game
DROP TABLE IF EXISTS nhl.shift_teammate_overlap_game CASCADE;

CREATE TABLE nhl.shift_teammate_overlap_game (
  game_id      bigint NOT NULL,
  team_abbrev  text   NOT NULL,
  player_id    bigint NOT NULL,
  mate_id      bigint NOT NULL,
  overlap_sec  integer NOT NULL,
  PRIMARY KEY (game_id, player_id, mate_id)
);

-- Helpful indexes for joins/rollups
CREATE INDEX IF NOT EXISTS idx_shift_teammate_overlap_game_player
  ON nhl.shift_teammate_overlap_game (player_id, game_id);

CREATE INDEX IF NOT EXISTS idx_shift_teammate_overlap_game_game
  ON nhl.shift_teammate_overlap_game (game_id);

WITH base AS (
  SELECT
    r.game_id,
    r.team_abbrev,
    r.player_id,
    r.start_sec,
    r.end_sec
  FROM nhl.shiftcharts_raw r
  WHERE r.start_sec IS NOT NULL
    AND r.end_sec   IS NOT NULL
    AND r.end_sec > r.start_sec
    -- Drop obvious goalie "full period" or long blocks (goalies often show 20:00)
    AND COALESCE(r.duration_sec, (r.end_sec - r.start_sec)) < 600
),
pairs AS (
  SELECT
    a.game_id,
    a.team_abbrev,
    a.player_id,
    b.player_id AS mate_id,
    GREATEST(LEAST(a.end_sec, b.end_sec) - GREATEST(a.start_sec, b.start_sec), 0) AS ov
  FROM base a
  JOIN base b
    ON b.game_id     = a.game_id
   AND b.team_abbrev = a.team_abbrev
   AND b.player_id   > a.player_id   -- avoid double counting
   AND b.start_sec   < a.end_sec
   AND b.end_sec     > a.start_sec
),
summed AS (
  SELECT
    game_id,
    team_abbrev,
    player_id,
    mate_id,
    SUM(ov)::int AS overlap_sec
  FROM pairs
  WHERE ov > 0
  GROUP BY 1,2,3,4
),
-- Expand to both directions so rollups are simple
both_dirs AS (
  SELECT game_id, team_abbrev, player_id, mate_id, overlap_sec FROM summed
  UNION ALL
  SELECT game_id, team_abbrev, mate_id AS player_id, player_id AS mate_id, overlap_sec FROM summed
)
INSERT INTO nhl.shift_teammate_overlap_game (game_id, team_abbrev, player_id, mate_id, overlap_sec)
SELECT game_id, team_abbrev, player_id, mate_id, overlap_sec
FROM both_dirs;

-- 2) Rollup view: top mate overlap + top3 overlap, normalized by TOI seconds
DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_game;

CREATE VIEW nhl.shift_teammate_overlap_features_game AS
WITH toi AS (
  SELECT
    s.game_id,
    s.player_id::bigint AS player_id,
    ROUND(COALESCE(s.toi_minutes,0) * 60.0)::int AS toi_sec
  FROM nhl.skater_game_logs_raw s
),
ranked AS (
  SELECT
    e.game_id,
    e.player_id,
    e.mate_id,
    e.overlap_sec,
    ROW_NUMBER() OVER (PARTITION BY e.game_id, e.player_id ORDER BY e.overlap_sec DESC, e.mate_id) AS rn
  FROM nhl.shift_teammate_overlap_game e
),
agg AS (
  SELECT
    game_id,
    player_id,
    MAX(overlap_sec) FILTER (WHERE rn = 1) AS top_mate_overlap_sec,
    SUM(overlap_sec) FILTER (WHERE rn <= 3) AS top3_mates_overlap_sec
  FROM ranked
  GROUP BY 1,2
)
SELECT
  a.game_id,
  a.player_id,
  t.toi_sec,
  COALESCE(a.top_mate_overlap_sec, 0) AS top_mate_overlap_sec,
  COALESCE(a.top3_mates_overlap_sec, 0) AS top3_mates_overlap_sec,
  CASE WHEN COALESCE(t.toi_sec,0) > 0
       THEN (COALESCE(a.top_mate_overlap_sec,0)::double precision / t.toi_sec)
       ELSE NULL
  END AS top_mate_overlap_share,
  CASE WHEN COALESCE(t.toi_sec,0) > 0
       THEN (COALESCE(a.top3_mates_overlap_sec,0)::double precision / t.toi_sec)
       ELSE NULL
  END AS top3_mates_overlap_share
FROM agg a
LEFT JOIN toi t
  ON t.game_id = a.game_id AND t.player_id = a.player_id;
