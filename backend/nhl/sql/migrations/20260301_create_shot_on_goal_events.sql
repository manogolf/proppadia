CREATE TABLE IF NOT EXISTS nhl.shot_on_goal_events (
  game_id bigint NOT NULL REFERENCES nhl.games(game_id) ON DELETE CASCADE,
  event_id bigint NOT NULL,
  season integer NOT NULL,
  game_date date NOT NULL,
  period_number integer,
  time_in_period text,
  event_abs_sec integer,
  situation_code text,
  shot_type text,
  zone_code text,
  shooting_player_id bigint,
  shooting_team_id integer,
  shooter_position_bucket text,
  defending_team_id integer,
  goalie_in_net_id bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_nhl_sog_events_game_date
  ON nhl.shot_on_goal_events (game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_sog_events_shooting_team
  ON nhl.shot_on_goal_events (shooting_team_id, game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_sog_events_defending_team
  ON nhl.shot_on_goal_events (defending_team_id, game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_sog_events_shooter_pos
  ON nhl.shot_on_goal_events (defending_team_id, shooter_position_bucket, game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_sog_events_abs_sec
  ON nhl.shot_on_goal_events (game_id, event_abs_sec);
