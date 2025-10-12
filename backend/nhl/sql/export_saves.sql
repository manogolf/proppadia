\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Existence check without leaking output to STDOUT
SELECT to_regclass('nhl.v_slate_saves_features') AS v_saves \gset
\if :{?v_saves}

-- Preflight: assert required columns on the view
DO $$
DECLARE missing text[];
BEGIN
  SELECT array_agg(n.col) INTO missing
  FROM (VALUES
    ('player_id'::text), ('game_id'), ('team_id'), ('opponent_id'), ('is_home'), ('game_date'),
    ('d10_shots_faced_per60'), ('d10_save_pct'),
    ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
    ('pace_index'), ('rest_days'), ('b2b_flag'),
    ('d5_saves_per60'), ('d10_saves_per60'), ('d5_shots_faced_per60'), ('season_save_pct'),
    ('opp_d10_sf_per60'), ('team_d10_sa_per60'), ('pace_matchup_index')
  ) AS n(col)
  LEFT JOIN information_schema.columns c
    ON c.table_schema='nhl' AND c.table_name='v_slate_saves_features' AND c.column_name=n.col
  WHERE c.column_name IS NULL;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'Missing columns on nhl.v_slate_saves_features: %', missing;
  END IF;
END $$;

-- Export from the view
COPY (
  SELECT
    player_id                   AS "player_id",
    game_id                     AS "game_id",
    team_id                     AS "team_id",
    opponent_id                 AS "opponent_id",
    is_home                     AS "is_home",
    game_date::date             AS "game_date",

    -- core goalie features
    d10_shots_faced_per60       AS "d10_shots_faced_per60",
    d10_save_pct                AS "d10_save_pct",
    team_d10_sf_per_game        AS "team_d10_sf_per_game",
    opp_d10_sf_allowed_per_game AS "opp_d10_sf_allowed_per_game",
    pace_index                  AS "pace_index",
    rest_days                   AS "rest_days",
    b2b_flag                    AS "b2b_flag",
    d5_saves_per60              AS "d5_saves_per60",
    d10_saves_per60             AS "d10_saves_per60",
    d5_shots_faced_per60        AS "d5_shots_faced_per60",
    season_save_pct             AS "season_save_pct",
    opp_d10_sf_per60            AS "opp_d10_sf_per60",
    team_d10_sa_per60           AS "team_d10_sa_per60",
    pace_matchup_index          AS "pace_matchup_index",

    -- compatibility aliases (harmless extras if unused by metadata)
    d10_shots_faced_per60       AS "d10_sf_per60",
    d5_shots_faced_per60        AS "d5_sf_per60",
    d10_save_pct                AS "sv_pct_d10",
    pace_index                  AS "pace_idx",
    opp_d10_sf_per60            AS "d10_sf_per60_opp",
    team_d10_sa_per60           AS "d10_sa_per60_team"
  FROM nhl.v_slate_saves_features
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
    ('d10_shots_faced_per60'), ('d10_save_pct'),
    ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
    ('pace_index'), ('rest_days'), ('b2b_flag'),
    ('d5_saves_per60'), ('d10_saves_per60'), ('d5_shots_faced_per60'), ('season_save_pct'),
    ('opp_d10_sf_per60'), ('team_d10_sa_per60'), ('pace_matchup_index')
  ) AS n(col)
  LEFT JOIN information_schema.columns c
    ON c.table_schema='nhl' AND c.table_name='training_features_goalie_saves_v2' AND c.column_name=n.col
  WHERE c.column_name IS NULL;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'Missing columns on nhl.training_features_goalie_saves_v2: %', missing;
  END IF;
END $$;

-- Export from the base table
COPY (
  SELECT
    player_id                   AS "player_id",
    game_id                     AS "game_id",
    team_id                     AS "team_id",
    opponent_id                 AS "opponent_id",
    is_home                     AS "is_home",
    game_date::date             AS "game_date",

    d10_shots_faced_per60       AS "d10_shots_faced_per60",
    d10_save_pct                AS "d10_save_pct",
    team_d10_sf_per_game        AS "team_d10_sf_per_game",
    opp_d10_sf_allowed_per_game AS "opp_d10_sf_allowed_per_game",
    pace_index                  AS "pace_index",
    rest_days                   AS "rest_days",
    b2b_flag                    AS "b2b_flag",
    d5_saves_per60              AS "d5_saves_per60",
    d10_saves_per60             AS "d10_saves_per60",
    d5_shots_faced_per60        AS "d5_shots_faced_per60",
    season_save_pct             AS "season_save_pct",
    opp_d10_sf_per60            AS "opp_d10_sf_per60",
    team_d10_sa_per60           AS "team_d10_sa_per60",
    pace_matchup_index          AS "pace_matchup_index",

    -- compatibility aliases
    d10_shots_faced_per60       AS "d10_sf_per60",
    d5_shots_faced_per60        AS "d5_sf_per60",
    d10_save_pct                AS "sv_pct_d10",
    pace_index                  AS "pace_idx",
    opp_d10_sf_per60            AS "d10_sf_per60_opp",
    team_d10_sa_per60           AS "d10_sa_per60_team"
  FROM nhl.training_features_goalie_saves_v2
  WHERE game_date = :'slate_date'::date
  ORDER BY game_id, player_id
) TO STDOUT WITH CSV HEADER;

\endif
