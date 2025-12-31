/* ============================================================
   FILE: backend/nhl/sql/create_shift_overlaps_and_rollups.sql

   PURPOSE:
     1) Normalize nhl.shiftcharts_raw into time intervals
     2) Build per-game overlap seconds between player pairs
     3) Roll up per-player per-game “linemate / matchup” features

   ASSUMPTIONS (based on your probe output):
     - nhl.shiftcharts_raw columns include:
         gameId, period, playerId, teamId, startTime, endTime, duration, typeCode
     - typeCode=517 appears to be goalie rows (20:00 “shift”). We exclude those.
     - Times are "MM:SS" strings.
     - We scope to regulation periods 1..3 by default (OT can be added later).

   OUTPUT TABLES:
     - nhl.shift_overlaps_pairs: pair overlap seconds by game (A,B)
     - nhl.shift_overlaps_player: per-player aggregate overlap seconds split teammate/opponent
     - nhl.shift_features_game: final per (game_id, player_id) features for modeling

   ============================================================ */

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS nhl;

-- ----------------------------
-- 1) Pair overlaps table
-- ----------------------------
CREATE TABLE IF NOT EXISTS nhl.shift_overlaps_pairs (
  game_id        bigint NOT NULL,
  player_id_a    bigint NOT NULL,
  team_id_a      bigint NOT NULL,
  player_id_b    bigint NOT NULL,
  team_id_b      bigint NOT NULL,
  same_team      boolean NOT NULL,
  overlap_sec    integer NOT NULL,
  PRIMARY KEY (game_id, player_id_a, player_id_b)
);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_pairs_game
  ON nhl.shift_overlaps_pairs (game_id);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_pairs_a
  ON nhl.shift_overlaps_pairs (player_id_a, game_id);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_pairs_b
  ON nhl.shift_overlaps_pairs (player_id_b, game_id);

COMMENT ON TABLE nhl.shift_overlaps_pairs IS
  'Per-game overlap seconds between player pairs derived from nhl.shiftcharts_raw (skaters only).';

-- ----------------------------
-- 2) Player overlap aggregates
-- ----------------------------
CREATE TABLE IF NOT EXISTS nhl.shift_overlaps_player (
  game_id             bigint NOT NULL,
  player_id           bigint NOT NULL,
  team_id             bigint NOT NULL,
  teammate_overlap_sec integer NOT NULL,
  opponent_overlap_sec integer NOT NULL,
  PRIMARY KEY (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_player_game
  ON nhl.shift_overlaps_player (game_id);

CREATE INDEX IF NOT EXISTS idx_shift_overlaps_player_player
  ON nhl.shift_overlaps_player (player_id, game_id);

COMMENT ON TABLE nhl.shift_overlaps_player IS
  'Per-game overlap totals for each player split into teammate vs opponent overlap seconds.';

-- ----------------------------
-- 3) Final rollup features
-- ----------------------------
CREATE TABLE IF NOT EXISTS nhl.shift_features_game (
  game_id bigint NOT NULL,
  game_date date NOT NULL,
  player_id bigint NOT NULL,
  team_id bigint NOT NULL,

  -- overlap totals
  teammate_overlap_sec integer NOT NULL,
  opponent_overlap_sec integer NOT NULL,

  -- linemate concentration / stability proxies
  top1_linemate_overlap_share double precision NOT NULL,
  top3_linemate_overlap_share double precision NOT NULL,
  linemate_entropy double precision NOT NULL,

  -- matchup concentration proxy
  top1_opponent_overlap_share double precision NOT NULL,
  top3_opponent_overlap_share double precision NOT NULL,
  opponent_entropy double precision NOT NULL,

  PRIMARY KEY (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_shift_features_game_date
  ON nhl.shift_features_game (game_date);

CREATE INDEX IF NOT EXISTS idx_shift_features_game_player
  ON nhl.shift_features_game (player_id, game_date);

COMMENT ON TABLE nhl.shift_features_game IS
  'Per (game_id, player_id) shift-based linemate/matchup rollup features (derived from shiftcharts).';

-- ============================================================
-- REFRESH PROCEDURE
-- ============================================================

CREATE OR REPLACE PROCEDURE nhl.refresh_shift_overlaps_and_features(
  p_start_date date,
  p_end_date   date
)
LANGUAGE plpgsql
AS $$
BEGIN
  /*
    Rebuild overlaps + features for games with nhl.games.game_date in [p_start_date, p_end_date].
    This is “replacement”, not supplementation: we delete rows for those games and recompute from raw.
  */

  -- --- target games in window ---
  WITH games_in_range AS (
    SELECT g.game_id, g.game_date
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  )
  -- delete downstream first
  DELETE FROM nhl.shift_features_game f
  USING games_in_range gr
  WHERE f.game_id = gr.game_id;

  WITH games_in_range AS (
    SELECT g.game_id
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  )
  DELETE FROM nhl.shift_overlaps_player p
  USING games_in_range gr
  WHERE p.game_id = gr.game_id;

  WITH games_in_range AS (
    SELECT g.game_id
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  )
  DELETE FROM nhl.shift_overlaps_pairs s
  USING games_in_range gr
  WHERE s.game_id = gr.game_id;

  -- ----------------------------------------------------------
  -- Build normalized skater shifts (REG only; OT can be added)
  -- ----------------------------------------------------------
  WITH games_in_range AS (
    SELECT g.game_id, g.game_date
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  ),
  raw AS (
    SELECT
      r."gameId"::bigint   AS game_id,
      r."period"::int     AS period,
      r."playerId"::bigint AS player_id,
      r."teamId"::bigint  AS team_id,
      r."startTime"::text AS start_time,
      r."endTime"::text   AS end_time,
      r."duration"::text  AS duration,
      r."typeCode"::int   AS type_code
    FROM nhl.shiftcharts_raw r
    JOIN games_in_range gr ON gr.game_id = r."gameId"::bigint
    WHERE r."playerId" IS NOT NULL
      AND r."teamId"   IS NOT NULL
      AND r."period"   IS NOT NULL
      AND r."startTime" IS NOT NULL
      AND r."endTime"   IS NOT NULL
      AND COALESCE(r."typeCode"::int, 0) <> 517          -- exclude goalies (based on your sample)
      AND r."period"::int BETWEEN 1 AND 3                -- REG only
  ),
  norm AS (
    SELECT
      game_id,
      period,
      player_id,
      team_id,

      -- convert "MM:SS" to seconds into period
      (split_part(start_time, ':', 1)::int * 60 + split_part(start_time, ':', 2)::int) AS start_in_per_sec,
      (split_part(end_time,   ':', 1)::int * 60 + split_part(end_time,   ':', 2)::int) AS end_in_per_sec,

      -- duration seconds (fallback if end < start)
      (split_part(duration,   ':', 1)::int * 60 + split_part(duration,   ':', 2)::int) AS dur_sec
    FROM raw
    WHERE start_time ~ '^\d{2}:\d{2}$'
      AND end_time   ~ '^\d{2}:\d{2}$'
      AND duration   ~ '^\d{2}:\d{2}$'
  ),
  shifts AS (
    SELECT
      game_id,
      period,
      player_id,
      team_id,
      -- absolute seconds since game start (REG baseline)
      ((period - 1) * 1200 + start_in_per_sec) AS start_sec,
      ((period - 1) * 1200 +
        CASE
          WHEN end_in_per_sec >= start_in_per_sec THEN end_in_per_sec
          ELSE (start_in_per_sec + GREATEST(dur_sec, 0))
        END
      ) AS end_sec
    FROM norm
    WHERE dur_sec >= 0
  ),
  pairs AS (
    SELECT
      a.game_id,
      a.player_id AS player_id_a,
      a.team_id   AS team_id_a,
      b.player_id AS player_id_b,
      b.team_id   AS team_id_b,
      (a.team_id = b.team_id) AS same_team,
      (LEAST(a.end_sec, b.end_sec) - GREATEST(a.start_sec, b.start_sec))::int AS overlap_sec
    FROM shifts a
    JOIN shifts b
      ON b.game_id = a.game_id
     AND b.player_id > a.player_id
     AND a.start_sec < b.end_sec
     AND b.start_sec < a.end_sec
  ),
  pairs_agg AS (
    SELECT
      game_id, player_id_a, team_id_a, player_id_b, team_id_b, same_team,
      SUM(GREATEST(overlap_sec, 0))::int AS overlap_sec
    FROM pairs
    WHERE overlap_sec > 0
    GROUP BY 1,2,3,4,5,6
  )
  INSERT INTO nhl.shift_overlaps_pairs (
    game_id, player_id_a, team_id_a, player_id_b, team_id_b, same_team, overlap_sec
  )
  SELECT game_id, player_id_a, team_id_a, player_id_b, team_id_b, same_team, overlap_sec
  FROM pairs_agg;

  -- ----------------------------------------------------------
  -- Build per-player totals (teammate vs opponent)
  -- ----------------------------------------------------------
  WITH games_in_range AS (
    SELECT g.game_id
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  ),
  exploded AS (
    -- represent each pair twice so each player gets their own perspective
    SELECT
      s.game_id,
      s.player_id_a AS player_id,
      s.team_id_a   AS team_id,
      s.same_team,
      s.overlap_sec
    FROM nhl.shift_overlaps_pairs s
    JOIN games_in_range gr ON gr.game_id = s.game_id
    UNION ALL
    SELECT
      s.game_id,
      s.player_id_b AS player_id,
      s.team_id_b   AS team_id,
      s.same_team,
      s.overlap_sec
    FROM nhl.shift_overlaps_pairs s
    JOIN games_in_range gr ON gr.game_id = s.game_id
  ),
  agg AS (
    SELECT
      game_id,
      player_id,
      team_id,
      COALESCE(SUM(CASE WHEN same_team THEN overlap_sec ELSE 0 END), 0)::int AS teammate_overlap_sec,
      COALESCE(SUM(CASE WHEN NOT same_team THEN overlap_sec ELSE 0 END), 0)::int AS opponent_overlap_sec
    FROM exploded
    GROUP BY 1,2,3
  )
  INSERT INTO nhl.shift_overlaps_player (
    game_id, player_id, team_id, teammate_overlap_sec, opponent_overlap_sec
  )
  SELECT game_id, player_id, team_id, teammate_overlap_sec, opponent_overlap_sec
  FROM agg;

  -- ----------------------------------------------------------
  -- Rollup features per (game_id, player_id)
  -- ----------------------------------------------------------
  WITH games_in_range AS (
    SELECT g.game_id, g.game_date
    FROM nhl.games g
    WHERE g.game_date BETWEEN p_start_date AND p_end_date
  ),
  exploded AS (
    -- perspective per player for each "other" overlap
    SELECT
      s.game_id,
      gr.game_date,
      s.player_id_a AS player_id,
      s.team_id_a   AS team_id,
      s.player_id_b AS other_player_id,
      s.team_id_b   AS other_team_id,
      s.same_team,
      s.overlap_sec
    FROM nhl.shift_overlaps_pairs s
    JOIN games_in_range gr ON gr.game_id = s.game_id
    UNION ALL
    SELECT
      s.game_id,
      gr.game_date,
      s.player_id_b AS player_id,
      s.team_id_b   AS team_id,
      s.player_id_a AS other_player_id,
      s.team_id_a   AS other_team_id,
      s.same_team,
      s.overlap_sec
    FROM nhl.shift_overlaps_pairs s
    JOIN games_in_range gr ON gr.game_id = s.game_id
  ),
  totals AS (
    SELECT
      p.game_id,
      gr.game_date,
      p.player_id,
      p.team_id,
      p.teammate_overlap_sec,
      p.opponent_overlap_sec
    FROM nhl.shift_overlaps_player p
    JOIN games_in_range gr ON gr.game_id = p.game_id
  ),
  ranked_linemates AS (
    SELECT
      e.game_id, e.player_id,
      e.overlap_sec,
      ROW_NUMBER() OVER (PARTITION BY e.game_id, e.player_id ORDER BY e.overlap_sec DESC) AS rn,
      SUM(e.overlap_sec) OVER (PARTITION BY e.game_id, e.player_id)::double precision AS sum_sec
    FROM exploded e
    WHERE e.same_team
  ),
  ranked_opponents AS (
    SELECT
      e.game_id, e.player_id,
      e.overlap_sec,
      ROW_NUMBER() OVER (PARTITION BY e.game_id, e.player_id ORDER BY e.overlap_sec DESC) AS rn,
      SUM(e.overlap_sec) OVER (PARTITION BY e.game_id, e.player_id)::double precision AS sum_sec
    FROM exploded e
    WHERE NOT e.same_team
  ),
  linemate_shares AS (
    SELECT
      game_id,
      player_id,
      COALESCE(MAX(CASE WHEN rn = 1 THEN overlap_sec / NULLIF(sum_sec, 0) END), 0.0) AS top1_share,
      COALESCE(SUM(CASE WHEN rn <= 3 THEN overlap_sec / NULLIF(sum_sec, 0) ELSE 0 END), 0.0) AS top3_share,
      -- entropy over shares (more stable line => lower entropy)
      COALESCE(SUM(
        CASE
          WHEN sum_sec > 0 AND overlap_sec > 0 THEN
            - (overlap_sec / sum_sec) * LN(overlap_sec / sum_sec)
          ELSE 0
        END
      ), 0.0) AS entropy
    FROM ranked_linemates
    GROUP BY 1,2
  ),
  opponent_shares AS (
    SELECT
      game_id,
      player_id,
      COALESCE(MAX(CASE WHEN rn = 1 THEN overlap_sec / NULLIF(sum_sec, 0) END), 0.0) AS top1_share,
      COALESCE(SUM(CASE WHEN rn <= 3 THEN overlap_sec / NULLIF(sum_sec, 0) ELSE 0 END), 0.0) AS top3_share,
      COALESCE(SUM(
        CASE
          WHEN sum_sec > 0 AND overlap_sec > 0 THEN
            - (overlap_sec / sum_sec) * LN(overlap_sec / sum_sec)
          ELSE 0
        END
      ), 0.0) AS entropy
    FROM ranked_opponents
    GROUP BY 1,2
  )
  INSERT INTO nhl.shift_features_game (
    game_id, game_date, player_id, team_id,
    teammate_overlap_sec, opponent_overlap_sec,
    top1_linemate_overlap_share, top3_linemate_overlap_share, linemate_entropy,
    top1_opponent_overlap_share, top3_opponent_overlap_share, opponent_entropy
  )
  SELECT
    t.game_id,
    t.game_date,
    t.player_id,
    t.team_id,
    t.teammate_overlap_sec,
    t.opponent_overlap_sec,
    COALESCE(ls.top1_share, 0.0),
    COALESCE(ls.top3_share, 0.0),
    COALESCE(ls.entropy, 0.0),
    COALESCE(os.top1_share, 0.0),
    COALESCE(os.top3_share, 0.0),
    COALESCE(os.entropy, 0.0)
  FROM totals t
  LEFT JOIN linemate_shares ls
    ON ls.game_id = t.game_id AND ls.player_id = t.player_id
  LEFT JOIN opponent_shares os
    ON os.game_id = t.game_id AND os.player_id = t.player_id;

END;
$$;

-- ============================================================
-- HOW TO RUN (example):
--   CALL nhl.refresh_shift_overlaps_and_features('2025-10-07', '2025-12-27');
-- ============================================================
