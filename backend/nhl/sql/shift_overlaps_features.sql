/* =============================================================================
   FILE: backend/nhl/sql/shift_overlaps_features.sql

   PURPOSE:
     Build the first shift-overlap feature set from nhl.shiftcharts_raw:
       - per game/team pair overlaps (seconds)
       - per player-game rollups:
           top_mate_overlap_sec
           top_mate_overlap_pct
           total_overlap_sec
           mate_count

   HOW TO RUN (exact command):
     psql --no-psqlrc -v ON_ERROR_STOP=1 "$SUPABASE_DB_URL" \
       -v start_date="'2025-10-07'" \
       -v end_date="'2025-12-27'" \
       -f backend/nhl/sql/shift_overlaps_features.sql

   NOTES:
     - This assumes nhl.shiftcharts_raw is populated (via your shiftcharts ingester).
     - We restrict to skaters by joining nhl.skater_game_logs_raw for (game_id, player_id),
       which excludes goalies that appear in shiftcharts.
============================================================================= */

\set ON_ERROR_STOP on

-- -------------------------
-- 0) Tables to store results
-- -------------------------

CREATE TABLE IF NOT EXISTS nhl.shift_overlaps_game (
  game_id        bigint NOT NULL,
  team_id        bigint NOT NULL,
  player_id      bigint NOT NULL,
  mate_player_id bigint NOT NULL,
  overlap_sec    integer NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, team_id, player_id, mate_player_id)
);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_game_player
  ON nhl.shift_overlaps_game (player_id, game_id);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_game_mate
  ON nhl.shift_overlaps_game (mate_player_id, game_id);


CREATE TABLE IF NOT EXISTS nhl.shift_overlap_features_game (
  game_id               bigint NOT NULL,
  team_id               bigint NOT NULL,
  player_id             bigint NOT NULL,

  total_shift_sec       integer NOT NULL,
  mate_count            integer NOT NULL,

  total_overlap_sec     integer NOT NULL,
  top_mate_player_id    bigint,
  top_mate_overlap_sec  integer NOT NULL,
  top_mate_overlap_pct  double precision NOT NULL,

  updated_at            timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_shift_overlap_features_game_game
  ON nhl.shift_overlap_features_game (game_id);

-- --------------------------------------------
-- 1) Compute overlaps for games in date window
-- --------------------------------------------

WITH params AS (
  SELECT
    DATE :start_date AS start_date,
    DATE :end_date   AS end_date
),
games AS (
  SELECT g.game_id
  FROM nhl.games g
  JOIN params p ON TRUE
  WHERE g.game_date BETWEEN p.start_date AND p.end_date
),
-- Skaters only: restrict shift rows to players that exist in skater_game_logs_raw.
-- This avoids goalies (who often have a full 20:00 "shift" row).
skaters AS (
  SELECT DISTINCT l.game_id::bigint, l.player_id::bigint
  FROM nhl.skater_game_logs_raw l
  JOIN games gg ON gg.game_id = l.game_id
),
shifts AS (
  SELECT
    r.game_id::bigint,
    r.team_id::bigint,
    r.player_id::bigint,
    r.start_sec::integer,
    r.end_sec::integer
  FROM nhl.shiftcharts_raw r
  JOIN games gg ON gg.game_id = r.game_id
  JOIN skaters s ON s.game_id = r.game_id AND s.player_id = r.player_id
  WHERE r.start_sec IS NOT NULL
    AND r.end_sec   IS NOT NULL
    AND r.end_sec > r.start_sec
),
-- pairwise overlap per (game, team, player, mate)
pair_overlaps AS (
  SELECT
    a.game_id,
    a.team_id,
    a.player_id,
    b.player_id AS mate_player_id,
    SUM(
      GREATEST(
        0,
        LEAST(a.end_sec, b.end_sec) - GREATEST(a.start_sec, b.start_sec)
      )
    )::integer AS overlap_sec
  FROM shifts a
  JOIN shifts b
    ON b.game_id  = a.game_id
   AND b.team_id  = a.team_id
   AND b.player_id > a.player_id            -- avoid duplicates & self
   AND b.start_sec < a.end_sec
   AND b.end_sec   > a.start_sec
  GROUP BY 1,2,3,4
),
-- make it directed: (player -> mate) and (mate -> player)
pair_overlaps_directed AS (
  SELECT game_id, team_id, player_id, mate_player_id, overlap_sec FROM pair_overlaps
  UNION ALL
  SELECT game_id, team_id, mate_player_id AS player_id, player_id AS mate_player_id, overlap_sec FROM pair_overlaps
)
INSERT INTO nhl.shift_overlaps_game (game_id, team_id, player_id, mate_player_id, overlap_sec, updated_at)
SELECT
  game_id, team_id, player_id, mate_player_id, overlap_sec, now()
FROM pair_overlaps_directed
WHERE overlap_sec > 0
ON CONFLICT (game_id, team_id, player_id, mate_player_id) DO UPDATE
SET overlap_sec = EXCLUDED.overlap_sec,
    updated_at  = now();

-- -----------------------------------------
-- 2) Rollup: first real feature per skater
-- -----------------------------------------

WITH params AS (
  SELECT
    DATE :start_date AS start_date,
    DATE :end_date   AS end_date
),
games AS (
  SELECT g.game_id, g.game_date
  FROM nhl.games g
  JOIN params p ON TRUE
  WHERE g.game_date BETWEEN p.start_date AND p.end_date
),
skaters AS (
  SELECT DISTINCT l.game_id::bigint, l.player_id::bigint, l.team_id::bigint
  FROM nhl.skater_game_logs_raw l
  JOIN games gg ON gg.game_id = l.game_id
),
player_shift_sec AS (
  SELECT
    r.game_id::bigint,
    r.team_id::bigint,
    r.player_id::bigint,
    SUM(r.duration_sec)::integer AS total_shift_sec
  FROM nhl.shiftcharts_raw r
  JOIN games gg ON gg.game_id = r.game_id
  JOIN skaters s ON s.game_id = r.game_id AND s.player_id = r.player_id
  WHERE r.duration_sec IS NOT NULL
    AND r.duration_sec > 0
  GROUP BY 1,2,3
),
overlaps AS (
  SELECT
    o.game_id,
    o.team_id,
    o.player_id,
    o.mate_player_id,
    o.overlap_sec
  FROM nhl.shift_overlaps_game o
  JOIN games gg ON gg.game_id = o.game_id
),
ranked_mates AS (
  SELECT
    o.*,
    ROW_NUMBER() OVER (
      PARTITION BY o.game_id, o.team_id, o.player_id
      ORDER BY o.overlap_sec DESC, o.mate_player_id
    ) AS rn
  FROM overlaps o
),
rollup AS (
  SELECT
    s.game_id,
    s.team_id,
    s.player_id,

    COALESCE(ps.total_shift_sec, 0) AS total_shift_sec,
    COUNT(DISTINCT o.mate_player_id)::integer AS mate_count,
    COALESCE(SUM(o.overlap_sec), 0)::integer  AS total_overlap_sec,

    MAX(CASE WHEN rm.rn = 1 THEN rm.mate_player_id END) AS top_mate_player_id,
    COALESCE(MAX(CASE WHEN rm.rn = 1 THEN rm.overlap_sec END), 0)::integer AS top_mate_overlap_sec
  FROM skaters s
  LEFT JOIN player_shift_sec ps
    ON ps.game_id = s.game_id AND ps.team_id = s.team_id AND ps.player_id = s.player_id
  LEFT JOIN overlaps o
    ON o.game_id = s.game_id AND o.team_id = s.team_id AND o.player_id = s.player_id
  LEFT JOIN ranked_mates rm
    ON rm.game_id = s.game_id AND rm.team_id = s.team_id AND rm.player_id = s.player_id
  GROUP BY 1,2,3,4
),
final AS (
  SELECT
    r.*,
    CASE
      WHEN r.total_shift_sec > 0
        THEN (r.top_mate_overlap_sec::double precision / r.total_shift_sec::double precision)
      ELSE 0.0
    END AS top_mate_overlap_pct
  FROM rollup r
)
INSERT INTO nhl.shift_overlap_features_game
  (game_id, team_id, player_id,
   total_shift_sec, mate_count, total_overlap_sec,
   top_mate_player_id, top_mate_overlap_sec, top_mate_overlap_pct,
   updated_at)
SELECT
  game_id, team_id, player_id,
  total_shift_sec, mate_count, total_overlap_sec,
  top_mate_player_id, top_mate_overlap_sec, top_mate_overlap_pct,
  now()
FROM final
ON CONFLICT (player_id, game_id) DO UPDATE
SET team_id              = EXCLUDED.team_id,
    total_shift_sec      = EXCLUDED.total_shift_sec,
    mate_count           = EXCLUDED.mate_count,
    total_overlap_sec    = EXCLUDED.total_overlap_sec,
    top_mate_player_id   = EXCLUDED.top_mate_player_id,
    top_mate_overlap_sec = EXCLUDED.top_mate_overlap_sec,
    top_mate_overlap_pct = EXCLUDED.top_mate_overlap_pct,
    updated_at           = now();

-- Optional sanity check (prints counts; safe under ON_ERROR_STOP)
-- SELECT COUNT(*) AS overlaps_rows FROM nhl.shift_overlaps_game;
-- SELECT COUNT(*) AS features_rows FROM nhl.shift_overlap_features_game;
