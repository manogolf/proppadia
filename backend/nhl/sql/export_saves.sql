\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on
-- Expect: -v slate_date=YYYY-MM-DD

SELECT to_regclass('nhl.v_slate_saves_features') IS NOT NULL AS has_view    \gset
SELECT to_regclass('nhl.players')                IS NOT NULL AS has_players \gset

\if :has_view
  SELECT EXISTS (
    SELECT 1 FROM nhl.v_slate_saves_features
    WHERE game_date = :'slate_date'::date
  ) AS has_rows \gset

  \if :has_rows
    DO $$
    DECLARE missing text[];
    BEGIN
      SELECT array_agg(n.col) INTO missing
      FROM (VALUES
        ('player_id'::text), ('game_id'), ('team_id'), ('opponent_id'),
        ('is_home'), ('game_date'),
        ('d10_shots_faced_per60'), ('d10_save_pct'),
        ('team_d10_sf_per_game'), ('opp_d10_sf_allowed_per_game'),
        ('pace_index'), ('rest_days'), ('b2b_flag'),
        -- goalie model req
        ('d5_saves_per60'), ('d10_saves_per60'), ('d5_shots_faced_per60'),
        ('season_save_pct'),
        -- extra context
        ('opp_d10_sf_per60'), ('team_d10_sa_per60'),
        ('pace_matchup_index'), ('d20_saves_per60'),
        ('team_d10_sf_per60'), ('opp_d10_sa_per60'),
        -- required by pipeline
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

    \if :has_players
      COPY (
        SELECT
          COALESCE(
            NULLIF(btrim(p.full_name), ''),
            NULLIF(btrim(concat_ws(' ', p.first_name, p.last_name)), ''),
            'Player ' || svf.player_id::text
          )                              AS full_name,
          svf.player_id                  AS player_id,
          svf.game_id                    AS game_id,
          svf.team_id                    AS team_id,
          svf.opponent_id                AS opponent_id,
          svf.is_home                    AS is_home,
          svf.game_date::date            AS game_date,
          -- features
          svf.d10_shots_faced_per60,
          svf.d10_save_pct,
          svf.team_d10_sf_per_game,
          svf.opp_d10_sf_allowed_per_game,
          svf.pace_index,
          svf.rest_days,
          svf.b2b_flag,
          svf.d5_saves_per60,
          svf.d10_saves_per60,
          svf.d5_shots_faced_per60,
          svf.season_save_pct,
          svf.opp_d10_sf_per60,
          svf.team_d10_sa_per60,
          svf.pace_matchup_index,
          svf.d20_saves_per60,
          svf.team_d10_sf_per60,
          svf.opp_d10_sa_per60,
          svf.start_prob
        FROM nhl.v_slate_saves_features AS svf
        LEFT JOIN nhl.players AS p USING (player_id)
        WHERE svf.game_date = :'slate_date'::date
        ORDER BY svf.game_id, svf.player_id
      ) TO STDOUT WITH CSV HEADER;
    \else
      COPY (
        SELECT NULL::text AS full_name, svf.*
        FROM nhl.v_slate_saves_features svf
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
  COPY (
    SELECT
      NULL::text    AS full_name,
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
