\set ON_ERROR_STOP on

COPY (
  WITH base AS (
    SELECT *
    FROM nhl.training_features_nhl_sog_enriched_pregame_v2
    WHERE game_date = :'slate_date'::date
  )
  SELECT
    -- IDs / context
    b.player_id,
    b.game_id,
    b.team_id,
    b.opponent_id,
    b.is_home,
    b.game_date,
    b.season,

    -- target (realized SOG; may be NULL pregame)
    b.shots_on_goal,

    -- core rolling SOG features (computed from prior logs; anchored to slate_date)
    calc.d5_sog_per60,
    calc.d10_sog_per60,
    calc.d20_sog_per60,
    calc.attempts_d10_per60,

    -- team shot / allowed context
    b.team_d10_sf_per_game,
    b.opp_d10_sf_allowed_per_game,

    -- pace features
    b.pace_matchup_index,
    b.pace_matchup_index                     AS pace_index,

    -- rest / usage
    b.rest_days,
    b.b2b_flag,
    b.role_pp_share,

    -- Denali extras (not yet populated here) → stubbed as 0.0
    0.0::numeric AS opp_d10_sf_per60,
    0.0::numeric AS team_d10_sa_per60,
    0.0::numeric AS opp_d10_sa_per60,

    0.0::numeric AS szn_toi_per_game_5on5,
    0.0::numeric AS szn_toi_per_game_pp,
    0.0::numeric AS szn_toi_per_game_pk,
    0.0::numeric AS szn_shifts_per_game_5on5,
    0.0::numeric AS szn_shifts_per_game_pp,
    0.0::numeric AS szn_shifts_per_game_pk,

    0.0::numeric AS season_5on5_icetime_per_game,
    0.0::numeric AS season_5on4_icetime_per_game,
    0.0::numeric AS season_4on5_icetime_per_game,
    0.0::numeric AS season_5on5_shifts_per_game,
    0.0::numeric AS season_5on4_shifts_per_game,
    0.0::numeric AS season_4on5_shifts_per_game,

    0.0::numeric AS team_szn_5on5_top_line_xgf_share,
    0.0::numeric AS team_5v5_top_line_icetime_share,
    0.0::numeric AS team_5v5_top_line_shotattempts_share,

    -- team + streak context
    b.last10_team_sog_share,
    b.hot_last5_flag,

    -- alias shotwasongoal → Denali SOG names
    b.num_shotwasongoal_last5          AS num_sog_last5,
    b.num_shotwasongoal_last10         AS num_sog_last10,
    b.num_shotwasongoal_season_to_date AS num_sog_szn_to_date,

    -- alias event-shot counts → Denali event names
    b.num_event_shot_last5             AS num_event_last5,
    b.num_event_shot_last10            AS num_event_last10,
    b.num_event_shot_season_to_date    AS num_event_szn_to_date,

    -- team-level counts
    b.team_num_shotwasongoal_for_last10 AS team_num_sog_last10,
    b.team_num_event_shot_for_last10    AS team_num_event_last10

  FROM base b
  CROSS JOIN LATERAL (
    WITH last20 AS (
      SELECT
        COALESCE(NULLIF(BTRIM(l.shots_on_goal::text), ''), '0')::numeric AS sog,
        COALESCE(NULLIF(BTRIM(l.shot_attempts::text), ''), '0')::numeric AS att,
        NULLIF(COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric, 0) AS toi_min,
        ROW_NUMBER() OVER (ORDER BY g.game_date DESC, g.game_id DESC) AS rn
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games g USING (game_id)
      WHERE l.player_id::bigint = b.player_id::bigint
        AND g.game_date::date < :'slate_date'::date
      ORDER BY g.game_date DESC, g.game_id DESC
      LIMIT 20
    ),
    sums AS (
      SELECT
        SUM(sog)     FILTER (WHERE rn <= 5)  AS sog_5,
        SUM(toi_min) FILTER (WHERE rn <= 5)  AS toi_5,
        SUM(sog)     FILTER (WHERE rn <= 10) AS sog_10,
        SUM(att)     FILTER (WHERE rn <= 10) AS att_10,
        SUM(toi_min) FILTER (WHERE rn <= 10) AS toi_10,
        SUM(sog)     FILTER (WHERE rn <= 20) AS sog_20,
        SUM(toi_min) FILTER (WHERE rn <= 20) AS toi_20
      FROM last20
    )
    SELECT
      CASE WHEN sums.toi_5  IS NULL OR sums.toi_5  <= 0 THEN NULL ELSE (sums.sog_5  / sums.toi_5 ) * 60 END AS d5_sog_per60,
      CASE WHEN sums.toi_10 IS NULL OR sums.toi_10 <= 0 THEN NULL ELSE (sums.sog_10 / sums.toi_10) * 60 END AS d10_sog_per60,
      CASE WHEN sums.toi_20 IS NULL OR sums.toi_20 <= 0 THEN NULL ELSE (sums.sog_20 / sums.toi_20) * 60 END AS d20_sog_per60,
      CASE WHEN sums.toi_10 IS NULL OR sums.toi_10 <= 0 THEN NULL ELSE (sums.att_10 / sums.toi_10) * 60 END AS attempts_d10_per60
    FROM sums
  ) AS calc
  ORDER BY b.game_date, b.game_id, b.player_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
