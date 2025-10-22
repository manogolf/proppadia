\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Expect: -v slate_date=YYYY-MM-DD

SELECT to_regclass('nhl.v_slate_sog_features') IS NOT NULL AS has_view   \gset
SELECT to_regclass('nhl.players')                IS NOT NULL AS has_players \gset

\if :has_view
  SELECT EXISTS (
    SELECT 1
    FROM nhl.v_slate_sog_features
    WHERE game_date = :'slate_date'::date
  ) AS has_rows \gset

  \if :has_rows
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
        ON c.table_schema='nhl'
       AND c.table_name='v_slate_sog_features'
       AND c.column_name=n.col
      WHERE c.column_name IS NULL;
      IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Missing columns on nhl.v_slate_sog_features: %', missing;
      END IF;
    END $$;

    \if :has_players
      COPY (
        SELECT
          /* ---- Name selection that IGNORES placeholders ---- */
          COALESCE(
            NULLIF(
              CASE
                WHEN p.full_name ~* '^(player|unknown)\s+\d+$' THEN NULL
                ELSE btrim(p.full_name)
              END, ''
            ),
            NULLIF(
              CASE
                WHEN btrim(concat_ws(' ', p.first_name, p.last_name)) ~* '^(player|unknown)\s+\d+$' THEN NULL
                ELSE btrim(concat_ws(' ', p.first_name, p.last_name))
              END, ''
            ),
            'Player ' || vsf.player_id::text
          ) AS "full_name",

          vsf.player_id                   AS "player_id",
          vsf.game_id                     AS "game_id",
          vsf.team_id                     AS "team_id",
          vsf.opponent_id                 AS "opponent_id",
          vsf.is_home                     AS "is_home",
          vsf.game_date::date             AS "game_date",
          NULL::int                       AS "shots_on_goal",
          vsf.d5_sog_per60                AS "d5_sog_per60",
          vsf.d10_sog_per60               AS "d10_sog_per60",
          vsf.d20_sog_per60               AS "d20_sog_per60",
          vsf.team_d10_sf_per_game        AS "team_d10_sf_per_game",
          vsf.opp_d10_sf_allowed_per_game AS "opp_d10_sf_allowed_per_game",
          vsf.role_pp_share               AS "role_pp_share",
          vsf.rest_days                   AS "rest_days",
          vsf.b2b_flag                    AS "b2b_flag",
          vsf.attempts_d10_per60          AS "attempts_d10_per60",
          vsf.pace_index                  AS "pace_index",
          vsf.opp_d10_sf_per60            AS "opp_d10_sf_per60",
          vsf.team_d10_sa_per60           AS "team_d10_sa_per60",
          vsf.pace_matchup_index          AS "pace_matchup_index"
        FROM nhl.v_slate_sog_features AS vsf
        LEFT JOIN nhl.players AS p ON p.player_id = vsf.player_id
        WHERE vsf.game_date = :'slate_date'::date
        ORDER BY vsf.game_id, vsf.player_id
      ) TO STDOUT WITH CSV HEADER;
    \else
      COPY (
        SELECT
          NULL::text AS "full_name",
          vsf.*
        FROM nhl.v_slate_sog_features vsf
        WHERE FALSE
      ) TO STDOUT WITH CSV HEADER;
    \endif

  \else
    COPY (
      SELECT
        NULL::text    AS full_name,
        NULL::bigint  AS player_id,
        NULL::bigint  AS game_id,
        NULL::bigint  AS team_id,
        NULL::bigint  AS opponent_id,
        NULL::boolean AS is_home,
        NULL::date    AS game_date,
        NULL::int     AS shots_on_goal,
        NULL::numeric AS d5_sog_per60,
        NULL::numeric AS d10_sog_per60,
        NULL::numeric AS d20_sog_per60,
        NULL::numeric AS team_d10_sf_per_game,
        NULL::numeric AS opp_d10_sf_allowed_per_game,
        NULL::numeric AS role_pp_share,
        NULL::int     AS rest_days,
        NULL::boolean AS b2b_flag,
        NULL::numeric AS attempts_d10_per60,
        NULL::numeric AS pace_index,
        NULL::numeric AS opp_d10_sf_per60,
        NULL::numeric AS team_d10_sa_per60,
        NULL::numeric AS pace_matchup_index
      WHERE FALSE
    ) TO STDOUT WITH CSV HEADER;
  \endif

\else
  COPY (
    SELECT
      NULL::text    AS full_name,
      NULL::bigint  AS player_id,
      NULL::bigint  AS game_id,
      NULL::bigint  AS team_id,
      NULL::bigint  AS opponent_id,
      NULL::boolean AS is_home,
      NULL::date    AS game_date,
      NULL::int     AS shots_on_goal,
      NULL::numeric AS d5_sog_per60,
      NULL::numeric AS d10_sog_per60,
      NULL::numeric AS d20_sog_per60,
      NULL::numeric AS team_d10_sf_per_game,
      NULL::numeric AS opp_d10_sf_allowed_per_game,
      NULL::numeric AS role_pp_share,
      NULL::int     AS rest_days,
      NULL::boolean AS b2b_flag,
      NULL::numeric AS attempts_d10_per60,
      NULL::numeric AS pace_index,
      NULL::numeric AS opp_d10_sf_per60,
      NULL::numeric AS team_d10_sa_per60,
      NULL::numeric AS pace_matchup_index
    WHERE FALSE
  ) TO STDOUT WITH CSV HEADER;
\endif
