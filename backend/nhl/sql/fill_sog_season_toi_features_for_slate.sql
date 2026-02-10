/* ============================================================
   FILE: backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql

   PURPOSE (Option B):
     Fill season-to-date TOI + shift features for SOG pregame rows
     using ONLY primitives with stable historical coverage:

       - nhl.shiftcharts_shifts          (player shift intervals)
       - nhl.game_manpower_segments      (PP/PK windows; pp_team_id/pk_team_id)
       - nhl.skater_game_logs_raw        (player_team mapping per game)

     Derivation:
       total_shift_sec := sum(dur_sec)
       pp_sec := sum(overlap_sec where team_id == pp_team_id)
       pk_sec := sum(overlap_sec where team_id == pk_team_id)
       ev_sec := max(total_shift_sec - pp_sec - pk_sec, 0)

     Then season-to-date per-game averages (games < slate_date, same season):
       season_5on5_icetime_per_game := avg(ev_sec)
       season_5on4_icetime_per_game := avg(pp_sec)
       season_4on5_icetime_per_game := avg(pk_sec)

       szn_toi_per_game_5on5 := avg(ev_sec)/60
       szn_toi_per_game_pp   := avg(pp_sec)/60
       szn_toi_per_game_pk   := avg(pk_sec)/60

   USAGE:
     psql ... -v slate_date=YYYY-MM-DD -f backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql
   ============================================================ */

BEGIN;

-- (Columns should already exist, but keep idempotent adds if you want)
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_5on5 numeric,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pp   numeric,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pk   numeric,
  ADD COLUMN IF NOT EXISTS season_5on5_icetime_per_game numeric,
  ADD COLUMN IF NOT EXISTS season_5on4_icetime_per_game numeric,
  ADD COLUMN IF NOT EXISTS season_4on5_icetime_per_game numeric;

WITH params AS (
  SELECT (:'slate_date')::date AS slate_date
),

-- Slate rows (derive season from nhl.games, NOT from t.season)
slate_rows AS (
  SELECT DISTINCT
    t.player_id::bigint AS player_id,
    t.game_id::bigint   AS game_id,
    g.season::int       AS season
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN nhl.games g ON g.game_id = t.game_id
  JOIN params p ON TRUE
  WHERE t.game_date::date = p.slate_date
    AND t.player_id IS NOT NULL
),

prior_games AS (
  SELECT
    g.game_id::bigint AS game_id,
    g.game_date::date AS game_date,
    g.season::int     AS season
  FROM nhl.games g
  JOIN params p ON TRUE
  JOIN (SELECT DISTINCT season FROM slate_rows) ss ON ss.season = g.season
  WHERE g.game_date::date < p.slate_date
),

-- Team mapping per player+game from skater_game_logs_raw (historical stable source)
player_game_team AS (
  SELECT
    l.game_id::bigint   AS game_id,
    l.player_id::bigint AS player_id,
    l.team_id::int      AS team_id
  FROM nhl.skater_game_logs_raw l
  JOIN prior_games pg ON pg.game_id = l.game_id
  WHERE l.player_id IS NOT NULL
    AND l.team_id  IS NOT NULL
),

-- Total shift time (sec) per player+game from shiftcharts
player_game_total AS (
  SELECT
    s.game_id::bigint   AS game_id,
    s.player_id::bigint AS player_id,
    NULLIF(SUM(COALESCE(s.dur_sec, 0))::int, 0) AS total_shift_sec,
    COUNT(*)::int AS total_shifts
  FROM nhl.shiftcharts_shifts s
  JOIN prior_games pg ON pg.game_id = s.game_id
  WHERE s.player_id IS NOT NULL
  GROUP BY 1,2
),

-- Overlap seconds between a shift and a manpower segment (same period, overlapping interval)
shift_seg_overlap AS (
  SELECT
    sh.game_id::bigint   AS game_id,
    sh.player_id::bigint AS player_id,
    pg.season::int       AS season,
    pgt.team_id::int     AS team_id,

    seg.pp_team_id::int  AS pp_team_id,
    seg.pk_team_id::int  AS pk_team_id,

    GREATEST(
      LEAST(sh.end_sec,   seg.end_sec) - GREATEST(sh.start_sec, seg.start_sec),
      0
    )::int AS overlap_sec

  FROM nhl.shiftcharts_shifts sh
  JOIN prior_games pg ON pg.game_id = sh.game_id
  JOIN player_game_team pgt
    ON pgt.game_id = sh.game_id AND pgt.player_id = sh.player_id
  JOIN nhl.game_manpower_segments seg
    ON seg.game_id = sh.game_id
   AND seg.period  = sh.period
   AND sh.end_sec  > seg.start_sec
   AND sh.start_sec < seg.end_sec
  WHERE sh.player_id IS NOT NULL
),

player_game_pppk AS (
  SELECT
    game_id,
    player_id,
    season,
    SUM(CASE WHEN overlap_sec > 0 AND team_id = pp_team_id THEN overlap_sec ELSE 0 END)::int AS pp_sec,
    SUM(CASE WHEN overlap_sec > 0 AND team_id = pk_team_id THEN overlap_sec ELSE 0 END)::int AS pk_sec,
    SUM(overlap_sec)::int AS any_seg_overlap_sec
  FROM shift_seg_overlap
  GROUP BY 1,2,3
),

-- Combine totals + PP/PK, derive 5v5 remainder
player_game_derived AS (
  SELECT
    gt.player_id,
    pg.season,
    gt.game_id,
    gt.total_shift_sec,
    gt.total_shifts,

    COALESCE(pppk.pp_sec, 0)::int AS pp_sec,
    COALESCE(pppk.pk_sec, 0)::int AS pk_sec,
    GREATEST(gt.total_shift_sec - COALESCE(pppk.pp_sec,0) - COALESCE(pppk.pk_sec,0), 0)::int AS ev_sec

  FROM player_game_total gt
  JOIN prior_games pg ON pg.game_id = gt.game_id
  JOIN slate_rows sr
    ON sr.player_id = gt.player_id
   AND sr.season    = pg.season
  LEFT JOIN player_game_pppk pppk
    ON pppk.game_id = gt.game_id
   AND pppk.player_id = gt.player_id
   AND pppk.season = pg.season
  WHERE gt.total_shift_sec IS NOT NULL
),

-- Season-to-date per-game averages (only games where player had shifts)
season_aggs AS (
  SELECT
    player_id,
    season,
    AVG(ev_sec)::numeric AS season_5on5_icetime_per_game,
    AVG(pp_sec)::numeric AS season_5on4_icetime_per_game,
    AVG(pk_sec)::numeric AS season_4on5_icetime_per_game,

    (AVG(ev_sec)::numeric / 60.0) AS szn_toi_per_game_5on5,
    (AVG(pp_sec)::numeric / 60.0) AS szn_toi_per_game_pp,
    (AVG(pk_sec)::numeric / 60.0) AS szn_toi_per_game_pk
  FROM player_game_derived
  GROUP BY 1,2
)

UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  szn_toi_per_game_5on5        = a.szn_toi_per_game_5on5,
  szn_toi_per_game_pp          = a.szn_toi_per_game_pp,
  szn_toi_per_game_pk          = a.szn_toi_per_game_pk,
  season_5on5_icetime_per_game = a.season_5on5_icetime_per_game,
  season_5on4_icetime_per_game = a.season_5on4_icetime_per_game,
  season_4on5_icetime_per_game = a.season_4on5_icetime_per_game
FROM params p
JOIN nhl.games g ON TRUE
JOIN season_aggs a ON TRUE
WHERE t.game_date::date = p.slate_date
  AND g.game_id = t.game_id
  AND a.player_id = t.player_id::bigint
  AND a.season    = g.season::int;

COMMIT;
