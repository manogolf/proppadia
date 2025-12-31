/* ============================================================
   1) FILE: backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql
   PURPOSE: Fill szn_toi_per_game_* for the SOG pregame table for a slate day,
            using ALL prior games (player-wide), and making PP meaningful by
            averaging only over PP>0 games.
   USAGE: psql ... -v slate_date=YYYY-MM-DD -f this_file.sql
   ============================================================ */

\set ON_ERROR_STOP on

ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_5on5 double precision,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pp   double precision,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pk   double precision;

WITH p AS (
  SELECT DATE :'slate_date' AS slate_date
),
hist AS (
  SELECT
    l.player_id::bigint AS player_id,

    -- base averages over all prior games
    AVG(COALESCE(l.toi_minutes, 0))::double precision    AS avg_toi_all,
    AVG(COALESCE(l.pp_toi_minutes, 0))::double precision AS avg_pp_all,

    -- PP role average: only games where PP TOI > 0 (prevents dilution by zeros)
    (AVG(l.pp_toi_minutes) FILTER (WHERE l.pp_toi_minutes IS NOT NULL AND l.pp_toi_minutes > 0))::double precision
      AS avg_pp_when_used

  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  JOIN p ON TRUE
  WHERE g.game_date < p.slate_date
  GROUP BY 1
),
upd AS (
  SELECT
    t.player_id::bigint AS player_id,
    t.game_date::date   AS game_date,

    -- 5v5 proxy: all-TOI minus all-PP (clamped)
    GREATEST(COALESCE(h.avg_toi_all, 0) - COALESCE(h.avg_pp_all, 0), 0)::double precision AS szn_5v5,

    -- PP proxy: avg only when used, else 0
    COALESCE(h.avg_pp_when_used, 0)::double precision AS szn_pp,

    -- PK not available yet (until you ingest pk TOI)
    0::double precision AS szn_pk

  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN p ON TRUE
  LEFT JOIN hist h
    ON h.player_id = t.player_id::bigint
  WHERE t.game_date = p.slate_date
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  szn_toi_per_game_5on5 = u.szn_5v5,
  szn_toi_per_game_pp   = u.szn_pp,
  szn_toi_per_game_pk   = u.szn_pk
FROM upd u
WHERE t.player_id::bigint = u.player_id
  AND t.game_date::date   = u.game_date;


/* ============================================================
   2) CLI HOOK (place in backend/nhl/cli.py)

   Put this right AFTER fill_sog_toi_features_for_slate.sql
   and BEFORE export_sog_denali_features(...)

   run_psql_file(SQL_DIR / "fill_sog_season_toi_features_for_slate.sql",
                 vars={"slate_date": slate})

        run_psql_file(SQL_DIR / "fill_sog_season_toi_features_for_slate.sql", vars={"slate_date": slate})

   ============================================================ */
