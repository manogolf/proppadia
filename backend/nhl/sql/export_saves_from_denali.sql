SELECT
  NULL::text           AS full_name,   -- name join is downstream
  t.player_id,
  t.game_id,
  t.team_id,
  t.opponent_id,
  t.is_home::int       AS is_home,
  t.game_date::date    AS game_date,

  -- core features the model actually uses
  t.d10_shots_faced_per60,
  t.d10_save_pct,
  t.team_d10_sf_per_game,
  t.opp_d10_sf_allowed_per_game,
  t.pace_index,
  t.rest_days,
  t.b2b_flag,
  t.d5_saves_per60,
  t.d10_saves_per60,
  t.d5_shots_faced_per60,
  t.season_save_pct,
  t.opp_d10_sf_per60,
  t.team_d10_sa_per60,
  t.pace_matchup_index,
  t.d20_saves_per60     -- note: team_d10_sf_per60 / opp_d10_sa_per60 removed

FROM nhl.training_features_goalie_saves_v2 t
WHERE t.game_date = :'slate_date'::date
ORDER BY t.game_id, t.player_id;
