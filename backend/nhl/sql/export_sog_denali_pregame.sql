\set ON_ERROR_STOP on

COPY (
  SELECT
    -- IDs / context
    player_id,
    game_id,
    team_id,
    opponent_id,
    is_home,
    game_date,
    season,

    -- target (realized SOG; may be NULL pregame)
    shots_on_goal,

    -- core rolling SOG features
    d5_sog_per60,
    d10_sog_per60,
    d20_sog_per60,
    attempts_d10_per60,

    -- team shot / allowed context
    team_d10_sf_per_game,
    opp_d10_sf_allowed_per_game,

    -- pace features
    pace_matchup_index,
    pace_matchup_index                     AS pace_index,

    -- rest / usage
    rest_days,
    b2b_flag,
    role_pp_share,

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
    last10_team_sog_share,
    hot_last5_flag,

    -- alias shotwasongoal → Denali SOG names
    num_shotwasongoal_last5          AS num_sog_last5,
    num_shotwasongoal_last10         AS num_sog_last10,
    num_shotwasongoal_season_to_date AS num_sog_szn_to_date,

    -- alias event-shot counts → Denali event names
    num_event_shot_last5             AS num_event_last5,
    num_event_shot_last10            AS num_event_last10,
    num_event_shot_season_to_date    AS num_event_szn_to_date,

    -- team-level counts
    team_num_shotwasongoal_for_last10 AS team_num_sog_last10,
    team_num_event_shot_for_last10    AS team_num_event_last10

  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = :'slate_date'::date
  ORDER BY game_date, game_id, player_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
