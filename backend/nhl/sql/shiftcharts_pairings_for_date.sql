-- ============================================================
-- backend/nhl/sql/shiftcharts_pairings_for_date.sql
-- Build per-game teammate overlap (“pairings”) metrics from shiftcharts.
--
-- Run (example):
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v game_date=2026-01-02 -f backend/nhl/sql/shiftcharts_pairings_for_date.sql
--
-- Key fix vs naive overlap:
--   Uses UNIONed coverage via range_agg() (multirange) per (game,team,player,period),
--   then intersects multiranges so overlap_sec can never exceed toi_sec.
-- ============================================================

\set ON_ERROR_STOP on

-- Accept either -v game_date=YYYY-MM-DD or -v slate_date=YYYY-MM-DD
\if :{?game_date}
  \set run_date :'game_date'
\elif :{?slate_date}
  \set run_date :'slate_date'
\else
  \echo 'ERROR: must provide -v game_date=YYYY-MM-DD (or slate_date)'
  \quit 1
\endif


-- ----------------------------
-- 0) Tables + indexes (idempotent)
-- ----------------------------
CREATE TABLE IF NOT EXISTS nhl.shiftcharts_shifts (
  game_id      bigint  NOT NULL,
  shift_id     bigint  NOT NULL,
  player_id    bigint  NOT NULL,
  team_id      int     NOT NULL,
  period       int     NOT NULL,

  start_time   text    NULL,
  end_time     text    NULL,
  duration     text    NULL,

  start_sec    int     NOT NULL,
  end_sec      int     NOT NULL,
  dur_sec      int     NOT NULL,

  ingested_at  timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, shift_id)
);

CREATE INDEX IF NOT EXISTS idx_shiftcharts_shifts_game_team
  ON nhl.shiftcharts_shifts (game_id, team_id);

CREATE INDEX IF NOT EXISTS idx_shiftcharts_shifts_game_player
  ON nhl.shiftcharts_shifts (game_id, player_id);

CREATE TABLE IF NOT EXISTS nhl.shiftcharts_pairings_game (
  game_id                    bigint NOT NULL,
  player_id                  bigint NOT NULL,
  team_id                    int    NOT NULL,

  toi_sec                    int    NOT NULL,

  top_mate_player_id         bigint NULL,
  top_mate_overlap_sec       int    NOT NULL DEFAULT 0,
  top_mate_overlap_share     numeric NULL,

  top3_overlap_share_avg     numeric NULL,
  top3_overlap_share_std     numeric NULL,

  computed_at                timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_shiftcharts_pairings_game_team
  ON nhl.shiftcharts_pairings_game (game_id, team_id);

-- ----------------------------
-- 1) Normalize shifts for :game_date into nhl.shiftcharts_shifts
-- ----------------------------
WITH games AS (
  SELECT g.game_id::bigint AS game_id
  FROM nhl.games g
  WHERE g.game_date::date = (:'run_date')::date

),
raw_src AS (
  SELECT
    r.game_id::bigint                                 AS game_id,
    r.shift_id::bigint                                AS shift_id,
    COALESCE(r.player_id, NULLIF((r.raw->>'playerId')::bigint, 0)) AS player_id,
    COALESCE(r.team_id,   NULLIF((r.raw->>'teamId')::int, 0))      AS team_id,
    COALESCE(r.period,    NULLIF((r.raw->>'period')::int, 0))      AS period,
    COALESCE(r.start_time, r.raw->>'startTime')       AS start_time,
    COALESCE(r.end_time,   r.raw->>'endTime')         AS end_time,
    COALESCE(r.duration,   r.raw->>'duration')        AS duration,
    r.ingested_at                                     AS ingested_at
  FROM nhl.shiftcharts_raw r
  JOIN games g USING (game_id)
),
parsed AS (
  SELECT
    game_id,
    shift_id,
    player_id,
    team_id,
    period,
    start_time,
    end_time,
    duration,
    ingested_at,

    CASE
      WHEN start_time ~ '^[0-9]{1,2}:[0-9]{2}$'
        THEN (split_part(start_time, ':', 1)::int * 60) + split_part(start_time, ':', 2)::int
      ELSE NULL
    END AS start_sec_raw,

    CASE
      WHEN end_time ~ '^[0-9]{1,2}:[0-9]{2}$'
        THEN (split_part(end_time, ':', 1)::int * 60) + split_part(end_time, ':', 2)::int
      ELSE NULL
    END AS end_sec_raw,

    CASE
      WHEN duration ~ '^[0-9]{1,2}:[0-9]{2}$'
        THEN (split_part(duration, ':', 1)::int * 60) + split_part(duration, ':', 2)::int
      ELSE NULL
    END AS dur_sec_raw
  FROM raw_src
  WHERE player_id IS NOT NULL
    AND team_id IS NOT NULL
    AND period IS NOT NULL
)
INSERT INTO nhl.shiftcharts_shifts
  (game_id, shift_id, player_id, team_id, period, start_time, end_time, duration,
   start_sec, end_sec, dur_sec, ingested_at)
SELECT
  game_id,
  shift_id,
  player_id,
  team_id,
  period,
  start_time,
  end_time,
  duration,

  COALESCE(start_sec_raw, 0) AS start_sec,
  COALESCE(
    end_sec_raw,
    CASE
      WHEN start_sec_raw IS NOT NULL AND dur_sec_raw IS NOT NULL THEN start_sec_raw + dur_sec_raw
      ELSE 0
    END
  ) AS end_sec,

  COALESCE(
    dur_sec_raw,
    CASE
      WHEN start_sec_raw IS NOT NULL AND end_sec_raw IS NOT NULL THEN GREATEST(0, end_sec_raw - start_sec_raw)
      ELSE 0
    END
  ) AS dur_sec,

  ingested_at
FROM parsed
WHERE COALESCE(start_sec_raw, 0) >= 0
  AND (
    COALESCE(
      end_sec_raw,
      CASE
        WHEN start_sec_raw IS NOT NULL AND dur_sec_raw IS NOT NULL THEN start_sec_raw + dur_sec_raw
        ELSE 0
      END
    )
  ) >= COALESCE(start_sec_raw, 0)
  AND NOT (
    COALESCE(start_sec_raw, 0) = 0
    AND COALESCE(
      end_sec_raw,
      CASE
        WHEN start_sec_raw IS NOT NULL AND dur_sec_raw IS NOT NULL THEN start_sec_raw + dur_sec_raw
        ELSE 0
      END
    ) = 1200
  )
ON CONFLICT (game_id, shift_id) DO UPDATE SET
  player_id   = EXCLUDED.player_id,
  team_id     = EXCLUDED.team_id,
  period      = EXCLUDED.period,
  start_time  = EXCLUDED.start_time,
  end_time    = EXCLUDED.end_time,
  duration    = EXCLUDED.duration,
  start_sec   = EXCLUDED.start_sec,
  end_sec     = EXCLUDED.end_sec,
  dur_sec     = EXCLUDED.dur_sec,
  ingested_at = now();

-- ----------------------------
-- 2) Compute per-game overlap metrics (UNIONED coverage; no >100% bug)
-- ----------------------------
WITH games AS (
  SELECT g.game_id::bigint AS game_id
  FROM nhl.games g
  WHERE g.game_date::date = (:'run_date')::date

),
shifts AS (
  SELECT
    s.game_id,
    s.team_id,
    s.player_id,
    s.period,
    s.start_sec,
    s.end_sec
  FROM nhl.shiftcharts_shifts s
  JOIN games g USING (game_id)
  WHERE s.dur_sec > 0
    AND s.end_sec > s.start_sec
),
-- Unioned coverage per player per period (multirange)
player_period_union AS (
  SELECT
    game_id,
    team_id,
    player_id,
    period,
    range_agg(int4range(start_sec, end_sec, '[)')) AS mr
  FROM shifts
  GROUP BY 1,2,3,4
),
-- TOI computed from unioned coverage (so it matches the overlap math)
player_toi AS (
  SELECT
    game_id,
    team_id,
    player_id,
    SUM(seg_len)::int AS toi_sec
  FROM (
    SELECT
      u.game_id,
      u.team_id,
      u.player_id,
      (upper(r) - lower(r)) AS seg_len
    FROM player_period_union u
    CROSS JOIN LATERAL unnest(u.mr) AS r
  ) x
  GROUP BY 1,2,3
),
-- Pair overlaps computed from intersection of unioned multiranges per period
pair_overlaps_period AS (
  SELECT
    a.game_id,
    a.team_id,
    a.player_id AS player_id,
    b.player_id AS mate_player_id,
    a.period,
    COALESCE(SUM(upper(r) - lower(r)), 0)::int AS overlap_sec
  FROM player_period_union a
  JOIN player_period_union b
    ON b.game_id   = a.game_id
   AND b.team_id   = a.team_id
   AND b.period    = a.period
   AND b.player_id <> a.player_id
  CROSS JOIN LATERAL unnest(a.mr * b.mr) AS r
  GROUP BY 1,2,3,4,5
),
pair_overlaps AS (
  SELECT
    game_id,
    team_id,
    player_id,
    mate_player_id,
    SUM(overlap_sec)::int AS overlap_sec
  FROM pair_overlaps_period
  GROUP BY 1,2,3,4
),
pair_shares AS (
  SELECT
    p.game_id,
    p.team_id,
    p.player_id,
    p.mate_player_id,
    p.overlap_sec,
    t.toi_sec,
    CASE WHEN t.toi_sec > 0 THEN (p.overlap_sec::numeric / t.toi_sec::numeric) ELSE NULL END AS overlap_share
  FROM pair_overlaps p
  JOIN player_toi t
    ON t.game_id   = p.game_id
   AND t.team_id   = p.team_id
   AND t.player_id = p.player_id
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, team_id, player_id
      ORDER BY overlap_sec DESC, mate_player_id
    ) AS rn
  FROM pair_shares
),
top_mate AS (
  SELECT
    game_id,
    player_id,
    team_id,
    mate_player_id AS top_mate_player_id,
    overlap_sec    AS top_mate_overlap_sec,
    overlap_share  AS top_mate_overlap_share
  FROM ranked
  WHERE rn = 1
),
top3_stats AS (
  SELECT
    game_id,
    player_id,
    team_id,
    AVG(overlap_share)        AS top3_overlap_share_avg,
    STDDEV_POP(overlap_share) AS top3_overlap_share_std
  FROM ranked
  WHERE rn <= 3
  GROUP BY 1,2,3
),
final AS (
  SELECT
    t.game_id,
    t.player_id,
    t.team_id,
    t.toi_sec,

    m.top_mate_player_id,
    COALESCE(m.top_mate_overlap_sec, 0)  AS top_mate_overlap_sec,
    m.top_mate_overlap_share,

    s.top3_overlap_share_avg,
    s.top3_overlap_share_std
  FROM player_toi t
  LEFT JOIN top_mate   m ON m.game_id=t.game_id AND m.team_id=t.team_id AND m.player_id=t.player_id
  LEFT JOIN top3_stats s ON s.game_id=t.game_id AND s.team_id=t.team_id AND s.player_id=t.player_id
)
INSERT INTO nhl.shiftcharts_pairings_game
  (game_id, player_id, team_id, toi_sec,
   top_mate_player_id, top_mate_overlap_sec, top_mate_overlap_share,
   top3_overlap_share_avg, top3_overlap_share_std,
   computed_at)
SELECT
  game_id, player_id, team_id, toi_sec,
  top_mate_player_id, top_mate_overlap_sec, top_mate_overlap_share,
  top3_overlap_share_avg, top3_overlap_share_std,
  now()
FROM final
ON CONFLICT (game_id, player_id) DO UPDATE SET
  team_id                = EXCLUDED.team_id,
  toi_sec                = EXCLUDED.toi_sec,
  top_mate_player_id     = EXCLUDED.top_mate_player_id,
  top_mate_overlap_sec   = EXCLUDED.top_mate_overlap_sec,
  top_mate_overlap_share = EXCLUDED.top_mate_overlap_share,
  top3_overlap_share_avg = EXCLUDED.top3_overlap_share_avg,
  top3_overlap_share_std = EXCLUDED.top3_overlap_share_std,
  computed_at            = now();

-- Optional: quick summary
WITH games AS (
  SELECT g.game_id::bigint AS game_id
  FROM nhl.games g
  WHERE g.game_date::date = (:'run_date')::date

)
SELECT
  (:'run_date')::date
 AS game_date,
  (SELECT COUNT(*) FROM games)                                                AS games,
  (SELECT COUNT(*) FROM nhl.shiftcharts_shifts s JOIN games g USING(game_id))  AS shifts_rows,
  (SELECT COUNT(*) FROM nhl.shiftcharts_pairings_game p JOIN games g USING(game_id)) AS pairings_players;
