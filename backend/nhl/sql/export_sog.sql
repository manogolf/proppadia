\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Detect if the view exists (explicit 1/0) 
SELECT CASE WHEN to_regclass('nhl.v_slate_sog_features') IS NULL THEN 0 ELSE 1 END AS has_view;
\gset

\if :has_view = 1

-- Preflight: assert required columns on the view
DO $$
DECLARE missing text[];
BEGIN
  SELECT array_agg(n.col) INTO missing
  FROM (VALUES
    ('player_id'::text), ('game_id'), ('team_id'), ('opponent_id'), ('is_home'), ('game_date'),
    ('d5_sog_per60'), ('d10_sog_per60'), ('d20_sog_per60'),
    ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
    ('role_pp_share'), ('rest_days'), ('b2b_flag'), ('attempts_d10_per60'),
    ('pace_index'), ('opp_d10_sf_per60'), ('team_d10_sa_per60'), ('pace_matchup_index')
  ) AS n(col)
  LEFT JOIN information_schema.columns c
    ON c.table_schema='nhl' AND c.table_name='v_slate_sog_features' AND c.column_name=n.col
  WHERE c.column_name IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'Missing columns on nhl.v_slate_sog_features: %', missing;
  END IF;
END $$;

-- Export from the view (label left NULL at scoring time)
COPY (
  SELECT
    player_id, game_id, team_id, opponent_id, is_home, game_date,
    NULL::int AS shots_on_goal,
    d5_sog_per60, d10_sog_per60, d20_sog_per60,
    team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
    role_pp_share, rest_days, b2b_flag, attempts_d10_per60,
    pace_index, opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index
  FROM nhl.v_slate_sog_features
  WHERE game_date = :'slate_date'::date
  ORDER BY game_id, player_id
) TO STDOUT WITH CSV HEADER;
\else
-- Preflight: assert required columns on the base table fallback
DO $$
DECLARE missing text[];
BEGIN
  SELECT array_agg(n.col) INTO missing
  FROM (VALUES
    ('player_id'::text), ('game_id'), ('team_id'), ('opponent_id'), ('is_home'), ('game_date'),
    ('d5_sog_per60'), ('d10_sog_per60'), ('d20_sog_per60'),
    ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
    ('role_pp_share'), ('rest_days'), ('b2b_flag'), ('attempts_d10_per60'),
    ('pace_index'), ('opp_d10_sf_per60'), ('team_d10_sa_per60'), ('pace_matchup_index')
  ) AS n(col)
  LEFT JOIN information_schema.columns c
    ON c.table_schema='nhl' AND c.table_name='training_features_nhl_sog_v2' AND c.column_name=n.col
  WHERE c.column_name IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'Missing columns on nhl.training_features_nhl_sog_v2: %', missing;
  END IF;
END $$;

-- Export from the base table (same contract)
COPY (
  SELECT
    player_id, game_id, team_id, opponent_id, is_home, game_date,
    NULL::int AS shots_on_goal,
    d5_sog_per60, d10_sog_per60, d20_sog_per60,
    team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
    role_pp_share, rest_days, b2b_flag, attempts_d10_per60,
    pace_index, opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index
  FROM nhl.training_features_nhl_sog_v2
  WHERE game_date = :'slate_date'::date
  ORDER BY game_id, player_id
) TO STDOUT WITH CSV HEADER;
\endif
