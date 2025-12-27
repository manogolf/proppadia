-- tmp/diag_denali_zero_features.sql
-- Usage:
--   psql "$DB" -v ON_ERROR_STOP=1 -v slate_date='2025-12-23' -f tmp/diag_denali_zero_features.sql

\set ON_ERROR_STOP on
\pset format aligned
\pset tuples_only on
\pset pager off

-- show what we received
SELECT 'slate_date' AS which, :'slate_date' AS value;

BEGIN;
SET LOCAL statement_timeout = '120s';

-- IMPORTANT: use the psql var here (do NOT hardcode YYYY-MM-DD anywhere)
WITH params AS (SELECT DATE :'slate_date' AS slate_date)
SELECT
  'pregame_rows_for_slate' AS which,
  COUNT(*)::int                 AS rows,
  COUNT(DISTINCT player_id)::int AS players,
  COUNT(DISTINCT game_id)::int   AS games
FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
JOIN params p ON TRUE
WHERE t.game_date = p.slate_date;

-- 2/3) Column-aware diagnostics for the “all-zero constants” you saw in scorer output
--      (won't error if the column doesn't exist in this table)

DROP TABLE IF EXISTS tmp_denali_feature_diag;
CREATE TEMP TABLE tmp_denali_feature_diag (
  col_name   text,
  status     text,   -- OK | MISSING_COLUMN
  rows       int,
  nulls      int,
  zeros      int,
  distincts  int,
  min_val    numeric,
  max_val    numeric,
  sample_val numeric
);

-- Make slate_date available inside the DO block (psql vars won't expand reliably inside $$ $$)
SET LOCAL my.slate_date = :'slate_date';

DO $$
DECLARE
  slate_date date := current_setting('my.slate_date')::date;

  cols text[] := ARRAY[
    'opp_d10_sf_per60',
    'opp_d10_sa_per60',
    'team_d10_sa_per60',
    'team_szn_5on5_top_line_xgf_share',
    'team_5v5_top_line_icetime_share',
    'team_5v5_top_line_shotattempts_share',
    'season_4on5_shifts_per_game',
    'season_5on4_shifts_per_game',
    'season_5on5_shifts_per_game',
    'season_4on5_icetime_per_game',
    'season_5on4_icetime_per_game',
    'season_5on5_icetime_per_game',
    'szn_shifts_per_game_pk',
    'szn_shifts_per_game_pp',
    'szn_shifts_per_game_5on5',
    'szn_toi_per_game_pk',
    'szn_toi_per_game_pp',
    'szn_toi_per_game_5on5'
  ];
  c text;

  n_rows int;
  n_nulls int;
  n_zeros int;
  n_dist  int;
  v_min   numeric;
  v_max   numeric;
  v_samp  numeric;
BEGIN
  FOREACH c IN ARRAY cols LOOP
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'nhl'
        AND table_name   = 'training_features_nhl_sog_enriched_pregame_v2'
        AND column_name  = c
    ) THEN
      EXECUTE format(
        'SELECT
           COUNT(*)::int,
           COUNT(*) FILTER (WHERE %1$I IS NULL)::int,
           COUNT(*) FILTER (WHERE %1$I = 0)::int,
           COUNT(DISTINCT %1$I)::int,
           MIN(%1$I)::numeric,
           MAX(%1$I)::numeric,
           (SELECT %1$I::numeric
            FROM nhl.training_features_nhl_sog_enriched_pregame_v2
            WHERE game_date = $1
            ORDER BY game_id, player_id
            LIMIT 1)
         FROM nhl.training_features_nhl_sog_enriched_pregame_v2
         WHERE game_date = $1',
        c
      )
      INTO n_rows, n_nulls, n_zeros, n_dist, v_min, v_max, v_samp
      USING slate_date;

      INSERT INTO tmp_denali_feature_diag
      VALUES (c, 'OK', n_rows, n_nulls, n_zeros, n_dist, v_min, v_max, v_samp);
    ELSE
      INSERT INTO tmp_denali_feature_diag
      VALUES (c, 'MISSING_COLUMN', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    END IF;
  END LOOP;
END $$;

SELECT
  'feature_diag' AS which,
  col_name,
  status,
  rows,
  nulls,
  zeros,
  distincts,
  min_val,
  max_val,
  sample_val
FROM tmp_denali_feature_diag
ORDER BY
  CASE WHEN status = 'MISSING_COLUMN' THEN 0 ELSE 1 END,
  col_name;

COMMIT;
