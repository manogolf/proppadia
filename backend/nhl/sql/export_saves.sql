\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- 1) Does the view exist?
SELECT to_regclass('nhl.v_slate_saves_features') AS v_saves \gset

\if :{?v_saves}
  -- 2) Does the view have rows for this slate_date?
  SELECT CASE
           WHEN EXISTS (
             SELECT 1
             FROM nhl.v_slate_saves_features
             WHERE game_date = :'slate_date'::date
           ) THEN 'on' ELSE 'off'
         END AS saves_present \gset

  \if :saves_present
    -- ---- View branch (preflight + export) ----
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
        ('opp_d10_sf_per60'), ('team_d10_sa_per60'), ('pace_matchup_index'),
        -- scorer-facing aliases + new 20-game feature
        ('team_d10_sf_per60'), ('opp_d10_sa_per60'), ('d20_saves_per60')
      ) AS n(col)
      LEFT JOIN information_schema.columns c
        ON c.table_schema='nhl' AND c.table_name='v_slate_saves_features' AND c.column_name=n.col
      WHERE c.column_name IS NULL;

      IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing columns on nhl.v_slate_saves_features: %', missing;
      END IF;
    END $$;

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
        -- scorer-facing aliases + new 20-game feature
        d20_saves_per60             AS "d20_saves_per60",
        team_d10_sf_per60           AS "team_d10_sf_per60",
        opp_d10_sa_per60            AS "opp_d10_sa_per60"
      FROM nhl.v_slate_saves_features
      WHERE game_date = :'slate_date'::date
      ORDER BY game_id, player_id
    ) TO STDOUT WITH CSV HEADER;

  \else
    -- ---- Fallback branch (view has 0 rows on this date) ----
    -- Check if base table has d20_saves_per60
    SELECT CASE
             WHEN EXISTS (
               SELECT 1 FROM information_schema.columns
               WHERE table_schema='nhl'
                 AND table_name='training_features_goalie_saves_v2'
                 AND column_name='d20_saves_per60'
             ) THEN 'on' ELSE 'off'
           END AS base_has_d20 \gset

    DO $$
    DECLARE missing text[];
    BEGIN
      SELECT array_agg(n.col) INTO missing
      FROM (VALUES
        ('player_id'::text), ('game_id'), ('team_id'), ('opponent_id'), ('is_home'), ('game_date'),
        ('d10_shots_faced_per60'), ('d10_save_pct'),
        ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
        ('pace_index'), ('rest_days'), ('b2b_flag'),
        ('d5_saves_per60'), ('d10_saves_per60'), ('d5_shots_faced_per60'), ('season_save_pct')
        -- note: alias columns are derived below; not required to exist on base
      ) AS n(col)
      LEFT JOIN information_schema.columns c
        ON c.table_schema='nhl' AND c.table_name='training_features_goalie_saves_v2' AND c.column_name=n.col
      WHERE c.column_name IS NULL;

      IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing columns on nhl.training_features_goalie_saves_v2: %', missing;
      END IF;
    END $$;

    \if :base_has_d20
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
          -- legacy passthroughs if present (else NULLs)
          NULL::numeric               AS "opp_d10_sf_per60",
          NULL::numeric               AS "team_d10_sa_per60",
          NULL::numeric               AS "pace_matchup_index",
          -- d20 present on base
          d20_saves_per60             AS "d20_saves_per60",
          -- scorer-facing aliases (derived)
          team_d10_sf_per_game        AS "team_d10_sf_per60",
          opp_d10_sf_allowed_per_game AS "opp_d10_sa_per60"
        FROM nhl.training_features_goalie_saves_v2
        WHERE game_date = :'slate_date'::date
        ORDER BY game_id, player_id
      ) TO STDOUT WITH CSV HEADER;
    \else
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
          -- legacy passthroughs if present (else NULLs)
          NULL::numeric               AS "opp_d10_sf_per60",
          NULL::numeric               AS "team_d10_sa_per60",
          NULL::numeric               AS "pace_matchup_index",
          -- d20 not on base yet → emit NULL
          NULL::numeric               AS "d20_saves_per60",
          -- scorer-facing aliases (derived)
          team_d10_sf_per_game        AS "team_d10_sf_per60",
          opp_d10_sf_allowed_per_game AS "opp_d10_sa_per60"
        FROM nhl.training_features_goalie_saves_v2
        WHERE game_date = :'slate_date'::date
        ORDER BY game_id, player_id
      ) TO STDOUT WITH CSV HEADER;
    \endif
  \endif

\else
  -- ---- View missing entirely: same fallback as above ----
  SELECT CASE
           WHEN EXISTS (
             SELECT 1 FROM information_schema.columns
             WHERE table_schema='nhl'
               AND table_name='training_features_goalie_saves_v2'
               AND column_name='d20_saves_per60'
           ) THEN 'on' ELSE 'off'
         END AS base_has_d20 \gset

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
      -- legacy passthroughs if present (else NULLs)
      NULL::numeric               AS "opp_d10_sf_per60",
      NULL::numeric               AS "team_d10_sa_per60",
      NULL::numeric               AS "pace_matchup_index",
      -- conditional d20: use psql var
      CASE WHEN :'base_has_d20' = 'on' THEN d20_saves_per60 ELSE NULL::numeric END AS "d20_saves_per60",
      -- scorer-facing aliases (derived)
      team_d10_sf_per_game        AS "team_d10_sf_per60",
      opp_d10_sf_allowed_per_game AS "opp_d10_sa_per60"
    FROM nhl.training_features_goalie_saves_v2
    WHERE game_date = :'slate_date'::date
    ORDER BY game_id, player_id
  ) TO STDOUT WITH CSV HEADER;
\endif
