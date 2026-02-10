/* ============================================================
   FILE: backend/nhl/sql/fill_sog_pairings_for_slate.sql

   PURPOSE:
     Fill pairings (teammate overlap) features for the SOG pregame base table
     for a single slate date, using prior games in the same season.

     Sources:
       - nhl.shift_teammate_overlap_features_game (per player+game features)
       - nhl.shift_teammate_overlap_game_recent_v2 (mate-level rows for d0 mate_id)
       - nhl.games (game_date + season)

     Output columns filled on nhl.training_features_nhl_sog_enriched_pregame_v2:
       - d0_top_mate_player_id, d0_top_mate_overlap_sec, d0_top_mate_overlap_share
       - d0_top3_overlap_share_avg, d0_top3_overlap_share_std
       - d10_* overlap share avg/std + coverage + stability fields
       - d20_* overlap share avg/std + coverage + stability fields
       - pairings_source, pairings_updated_at, mate_stability_source, mate_stability_updated_at
       - d10_pairings_* flags/buckets, d20_pairings_* flags/buckets

   USAGE:
     psql ... -v slate_date=YYYY-MM-DD -f backend/nhl/sql/fill_sog_pairings_for_slate.sql
   ============================================================ */

\set ON_ERROR_STOP on
SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 0) Ensure destination columns exist on the BASE TABLE
-- ------------------------------------------------------------
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_player_id bigint;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_sec integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_share numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_avg numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_std numeric;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS pairings_source text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS pairings_updated_at timestamptz;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_games_in_window integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate double precision;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_games_in_window integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate double precision;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_repeat_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_distinct_count integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_games_with_shiftcharts integer;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_repeat_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_distinct_count integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_games_with_shiftcharts integer;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS mate_stability_source text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS mate_stability_updated_at timestamptz;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_missing_flag integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_available boolean;

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_missing_flag integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_available boolean;

-- ------------------------------------------------------------
-- 1) Compute and write pairings for slate_date
-- ------------------------------------------------------------
WITH
params AS (
  SELECT (:'slate_date')::date AS slate_date
),

slate_rows AS (
  SELECT DISTINCT
    t.player_id::bigint AS player_id,
    t.game_id::bigint   AS game_id,
    t.game_date::date   AS game_date,
    g.season::int       AS season
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN nhl.games g
    ON g.game_id = t.game_id
  JOIN params ON TRUE
  WHERE t.game_date::date = params.slate_date
    AND t.player_id IS NOT NULL
),

prior_games AS (
  SELECT
    g.game_id::bigint   AS game_id,
    g.game_date::date   AS game_date,
    g.season::int       AS season
  FROM nhl.games g
  JOIN params ON TRUE
  JOIN (SELECT DISTINCT season FROM slate_rows) slate_seasons
    ON slate_seasons.season = g.season
  WHERE g.game_date::date < params.slate_date
),

-- Per player+game pairings features (already aggregated)
pairings_features_per_game AS (
  SELECT
    f.game_id::bigint   AS game_id,
    f.player_id::bigint AS player_id,
    pg.game_date::date  AS game_date,
    pg.season::int      AS season,

    COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
    COALESCE(t.toi_sec, 0)::int              AS toi_sec,

    -- these exist on shift_teammate_overlap_features_game
    -- these exist on shift_teammate_overlap_features_game
    f.top_mate_overlap_share::double precision   AS top_mate_overlap_share,
    f.top3_mates_overlap_share::double precision AS top3_mates_overlap_share

  FROM nhl.shift_teammate_overlap_features_game f
  JOIN prior_games pg
    ON pg.game_id = f.game_id
  LEFT JOIN (
    SELECT
      game_id::bigint,
      player_id::bigint,
      SUM(dur_sec)::int AS toi_sec
    FROM nhl.shiftcharts_shifts
    GROUP BY 1,2
  ) t
    ON t.game_id = f.game_id AND t.player_id = f.player_id
  WHERE f.player_id IS NOT NULL
),

-- Rank games per player for d10/d20 windows
ranked_games AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (PARTITION BY p.player_id ORDER BY p.game_date DESC, p.game_id DESC) AS rn
  FROM pairings_features_per_game p
),

roll_d10 AS (
  SELECT
    player_id,
    season,

    COUNT(*)::int AS games_in_window,
    COUNT(*) FILTER (WHERE shiftcharts_available)::int AS shiftcharts_games,

    AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_avg,
    STDDEV_POP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_std,

    AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_avg,
    STDDEV_POP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_std
  FROM ranked_games
  WHERE rn <= 10
  GROUP BY 1,2
),

roll_d20 AS (
  SELECT
    player_id,
    season,

    COUNT(*)::int AS games_in_window,
    COUNT(*) FILTER (WHERE shiftcharts_available)::int AS shiftcharts_games,

    AVG(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_avg,
    STDDEV_POP(top_mate_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top_mate_overlap_share_std,

    AVG(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_avg,
    STDDEV_POP(top3_mates_overlap_share) FILTER (WHERE shiftcharts_available)::double precision AS top3_mates_overlap_share_std
  FROM ranked_games
  WHERE rn <= 20
  GROUP BY 1,2
),

-- d0 top mate: pick the most recent prior game for the player, then the teammate with max overlap_share
ranked_mates AS (
  SELECT
    r.player_id::bigint AS player_id,
    r.game_id::bigint   AS game_id,
    g.game_date::date   AS game_date,
    g.season::int       AS season,

    r.teammate_id::bigint AS teammate_id,
    r.overlap_sec::int    AS overlap_sec,
    r.overlap_share::numeric AS overlap_share,

    ROW_NUMBER() OVER (
      PARTITION BY r.player_id, r.game_id
      ORDER BY r.overlap_share DESC NULLS LAST, r.overlap_sec DESC NULLS LAST, r.teammate_id
    ) AS mate_rank
  FROM nhl.shift_teammate_overlap_game_recent_v2 r
  JOIN prior_games pg
    ON pg.game_id = r.game_id
  JOIN nhl.games g
    ON g.game_id = r.game_id
),

top_mate_per_game AS (
  SELECT
    player_id,
    game_id,
    game_date,
    season,
    teammate_id,
    overlap_sec,
    overlap_share
  FROM ranked_mates
  WHERE mate_rank = 1
),

most_recent_game_with_mate AS (
  SELECT
    player_id,
    season,
    game_id,
    game_date,
    teammate_id,
    overlap_sec,
    overlap_share,
    ROW_NUMBER() OVER (
      PARTITION BY player_id, season
      ORDER BY game_date DESC, game_id DESC
    ) AS recency_rank
  FROM top_mate_per_game
),

d0_values AS (
  SELECT
    player_id,
    season,
    teammate_id AS d0_top_mate_player_id,
    overlap_sec AS d0_top_mate_overlap_sec,
    overlap_share AS d0_top_mate_overlap_share
  FROM most_recent_game_with_mate
  WHERE recency_rank = 1
),

-- Basic stability: how often the top mate repeats in last 10/20 games (only games with shiftcharts)
top_mate_history AS (
  SELECT
    player_id,
    season,
    game_date,
    rn,
    -- For stability, use teammate_id from mate-level table (may be NULL for some games)
    (SELECT tm.teammate_id
     FROM top_mate_per_game tm
     WHERE tm.player_id = ranked_games.player_id
       AND tm.game_id   = ranked_games.game_id
     LIMIT 1) AS top_mate_id
  FROM ranked_games
  WHERE shiftcharts_available
),

stability_d10 AS (
  SELECT
    player_id,
    season,
    COUNT(*)::int AS games_with_shiftcharts,
    COUNT(DISTINCT top_mate_id)::int AS distinct_top_mates,
    (COUNT(*)::double precision - COUNT(DISTINCT top_mate_id)::double precision) / NULLIF(COUNT(*)::double precision, 0) AS repeat_rate
  FROM top_mate_history
  WHERE rn <= 10
  GROUP BY 1,2
),

stability_d20 AS (
  SELECT
    player_id,
    season,
    COUNT(*)::int AS games_with_shiftcharts,
    COUNT(DISTINCT top_mate_id)::int AS distinct_top_mates,
    (COUNT(*)::double precision - COUNT(DISTINCT top_mate_id)::double precision) / NULLIF(COUNT(*)::double precision, 0) AS repeat_rate
  FROM top_mate_history
  WHERE rn <= 20
  GROUP BY 1,2
),

final_values AS (
  SELECT
    s.player_id,
    s.game_id,
    s.game_date,
    s.season,

    -- d0 (may be null if no mate history)
    d0.d0_top_mate_player_id,
    d0.d0_top_mate_overlap_sec,
    d0.d0_top_mate_overlap_share,

    -- d0 top3 stats: use d20 roll’s mean/std as a reasonable proxy if you want;
    -- but we can set d0_top3_* from roll_d20 later. Keep as NULL if you prefer strict meaning.
    NULL::numeric AS d0_top3_overlap_share_avg,
    NULL::numeric AS d0_top3_overlap_share_std,

    -- d10
    r10.top_mate_overlap_share_avg        AS d10_top_mate_overlap_share_avg,
    r10.top_mate_overlap_share_std        AS d10_top_mate_overlap_share_std,
    r10.top3_mates_overlap_share_avg      AS d10_top3_mates_overlap_share_avg,
    r10.top3_mates_overlap_share_std      AS d10_top3_mates_overlap_share_std,
    r10.games_in_window                   AS d10_games_in_window,
    r10.shiftcharts_games                 AS d10_shiftcharts_games,
    (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) AS d10_shiftcharts_coverage_rate,

    -- d20
    r20.top_mate_overlap_share_avg        AS d20_top_mate_overlap_share_avg,
    r20.top_mate_overlap_share_std        AS d20_top_mate_overlap_share_std,
    r20.top3_mates_overlap_share_avg      AS d20_top3_mates_overlap_share_avg,
    r20.top3_mates_overlap_share_std      AS d20_top3_mates_overlap_share_std,
    r20.games_in_window                   AS d20_games_in_window,
    r20.shiftcharts_games                 AS d20_shiftcharts_games,
    (r20.shiftcharts_games::double precision / NULLIF(r20.games_in_window::double precision, 0)) AS d20_shiftcharts_coverage_rate,

    -- stability
    st10.repeat_rate                      AS d10_top_mate_repeat_rate,
    st10.distinct_top_mates               AS d10_top_mate_distinct_count,
    st10.games_with_shiftcharts           AS d10_top_mate_games_with_shiftcharts,

    st20.repeat_rate                      AS d20_top_mate_repeat_rate,
    st20.distinct_top_mates               AS d20_top_mate_distinct_count,
    st20.games_with_shiftcharts           AS d20_top_mate_games_with_shiftcharts,

    -- availability / flags / buckets
    CASE WHEN COALESCE(r10.shiftcharts_games, 0) > 0 THEN true ELSE false END AS d10_pairings_available,
    CASE WHEN COALESCE(r20.shiftcharts_games, 0) > 0 THEN true ELSE false END AS d20_pairings_available,

    CASE WHEN COALESCE(r10.shiftcharts_games, 0) > 0 THEN 0 ELSE 1 END AS d10_pairings_missing_flag,
    CASE WHEN COALESCE(r20.shiftcharts_games, 0) > 0 THEN 0 ELSE 1 END AS d20_pairings_missing_flag,

    CASE
      WHEN COALESCE(r10.games_in_window, 0) = 0 THEN 'none'
      WHEN (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) >= 0.80 THEN 'high'
      WHEN (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) >= 0.50 THEN 'med'
      WHEN (r10.shiftcharts_games::double precision / NULLIF(r10.games_in_window::double precision, 0)) >  0.00 THEN 'low'
      ELSE 'none'
    END AS d10_pairings_cov_bucket,

    CASE
      WHEN COALESCE(r20.games_in_window, 0) = 0 THEN 'none'
      WHEN (r20.shiftcharts_games::double precision / NULLIF(r20.games_in_window::double precision, 0)) >= 0.80 THEN 'high'
      WHEN (r20.shiftcharts_games::double precision / NULLIF(r20.games_in_window::double precision, 0)) >= 0.50 THEN 'med'
      WHEN (r20.shiftcharts_games::double precision / NULLIF(r20.games_in_window::double precision, 0)) >  0.00 THEN 'low'
      ELSE 'none'
    END AS d20_pairings_cov_bucket

  FROM slate_rows s
  LEFT JOIN d0_values d0
    ON d0.player_id = s.player_id
   AND d0.season    = s.season
  LEFT JOIN roll_d10 r10
    ON r10.player_id = s.player_id
   AND r10.season    = s.season
  LEFT JOIN roll_d20 r20
    ON r20.player_id = s.player_id
   AND r20.season    = s.season
  LEFT JOIN stability_d10 st10
    ON st10.player_id = s.player_id
   AND st10.season    = s.season
  LEFT JOIN stability_d20 st20
    ON st20.player_id = s.player_id
   AND st20.season    = s.season
)


-- === PROBE: does final_values contain non-null season/5v5 outputs? ===
SELECT
  COUNT(*) AS n_final,
  COUNT(*) FILTER (WHERE szn_toi_per_game_5on5 IS NOT NULL) AS n_szn_5v5,
  COUNT(*) FILTER (WHERE season_5on5_icetime_per_game IS NOT NULL) AS n_season_5v5,
  COUNT(*) FILTER (WHERE szn_toi_per_game_pp IS NOT NULL) AS n_szn_pp,
  COUNT(*) FILTER (WHERE season_5on4_icetime_per_game IS NOT NULL) AS n_season_pp,
  COUNT(*) FILTER (WHERE szn_toi_per_game_pk IS NOT NULL) AS n_szn_pk,
  COUNT(*) FILTER (WHERE season_4on5_icetime_per_game IS NOT NULL) AS n_season_pk
FROM final_values;
