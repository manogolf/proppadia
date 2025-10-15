\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Expect: -v slate_date=YYYY-MM-DD

-- 1) Check if the view exists
SELECT to_regclass('nhl.v_slate_saves_features') AS v_saves \gset

\if :{?v_saves}
  -- 2) Does it have rows for this slate_date?
  SELECT EXISTS (
    SELECT 1
    FROM nhl.v_slate_saves_features
    WHERE game_date = :'slate_date'::date
  ) AS has_rows \gset

  \if :has_rows
    -- 3) Schema preflight (only when rows exist)
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
        ('d20_saves_per60'),
        ('team_d10_sf_per60'), ('opp_d10_sa_per60'),
        ('start_prob')
      ) AS n(col)
      LEFT JOIN information_schema.columns c
        ON c.table_schema='nhl'
       AND c.table_name='v_slate_saves_features'
       AND c.column_name=n.col
      WHERE c.column_name IS NULL;

      IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing columns on nhl.v_slate_saves_features: %', missing;
      END IF;
    END $$;

    -- 4) Export data rows
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
        d20_saves_per60             AS "d20_saves_per60",
        team_d10_sf_per60           AS "team_d10_sf_per60",
        opp_d10_sa_per60            AS "opp_d10_sa_per60",
        start_prob                  AS "start_prob"
      FROM nhl.v_slate_saves_features
      WHERE game_date = :'slate_date'::date
      ORDER BY game_id, player_id
    ) TO STDOUT WITH CSV HEADER;

  \else
    -- 5) No rows → header-only CSV
    COPY (
      SELECT
        NULL::bigint  AS player_id,
        NULL::bigint  AS game_id,
        NULL::bigint  AS team_id,
        NULL::bigint  AS opponent_id,
        NULL::boolean AS is_home,
        NULL::date    AS game_date,
        NULL::numeric AS d10_shots_faced_per60,
        NULL::numeric AS d10_save_pct,
        NULL::numeric AS team_d10_sf_per_game,
        NULL::numeric AS opp_d10_sf_allowed_per_game,
        NULL::numeric AS pace_index,
        NULL::int     AS rest_days,
        NULL::boolean AS b2b_flag,
        NULL::numeric AS d5_saves_per60,
        NULL::numeric AS d10_saves_per60,
        NULL::numeric AS d5_shots_faced_per60,
        NULL::numeric AS season_save_pct,
        NULL::numeric AS opp_d10_sf_per60,
        NULL::numeric AS team_d10_sa_per60,
        NULL::numeric AS pace_matchup_index,
        NULL::numeric AS d20_saves_per60,
        NULL::numeric AS team_d10_sf_per60,
        NULL::numeric AS opp_d10_sa_per60,
        NULL::numeric AS start_prob
      WHERE FALSE
    ) TO STDOUT WITH CSV HEADER;
  \endif

\else
  -- View missing → header-only CSV
  COPY (
    SELECT
      NULL::bigint  AS player_id,
      NULL::bigint  AS game_id,
      NULL::bigint  AS team_id,
      NULL::bigint  AS opponent_id,
      NULL::boolean AS is_home,
      NULL::date    AS game_date,
      NULL::numeric AS d10_shots_faced_per60,
      NULL::numeric AS d10_save_pct,
      NULL::numeric AS team_d10_sf_per_game,
      NULL::numeric AS opp_d10_sf_allowed_per_game,
      NULL::numeric AS pace_index,
      NULL::int     AS rest_days,
      NULL::boolean AS b2b_flag,
      NULL::numeric AS d5_saves_per60,
      NULL::numeric AS d10_saves_per60,
      NULL::numeric AS d5_shots_faced_per60,
      NULL::numeric AS season_save_pct,
      NULL::numeric AS opp_d10_sf_per60,
      NULL::numeric AS team_d10_sa_per60,
      NULL::numeric AS pace_matchup_index,
      NULL::numeric AS d20_saves_per60,
      NULL::numeric AS team_d10_sf_per60,
      NULL::numeric AS opp_d10_sa_per60,
      NULL::numeric AS start_prob
    WHERE FALSE
  ) TO STDOUT WITH CSV HEADER;
\endif
