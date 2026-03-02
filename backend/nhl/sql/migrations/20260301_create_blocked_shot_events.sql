CREATE TABLE IF NOT EXISTS nhl.blocked_shot_events (
  game_id bigint NOT NULL REFERENCES nhl.games(game_id) ON DELETE CASCADE,
  event_id bigint NOT NULL,
  season integer NOT NULL,
  game_date date NOT NULL,
  period_number integer,
  time_in_period text,
  situation_code text,
  shot_type text,
  zone_code text,
  shooting_player_id bigint,
  shooting_team_id integer,
  shooter_position_bucket text,
  blocking_player_id bigint,
  blocking_team_id integer,
  blocker_position_bucket text,
  goalie_in_net_id bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_nhl_blocked_shot_events_game_date
  ON nhl.blocked_shot_events (game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_blocked_shot_events_shooting_team
  ON nhl.blocked_shot_events (shooting_team_id, game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_blocked_shot_events_blocking_team
  ON nhl.blocked_shot_events (blocking_team_id, game_date);

CREATE INDEX IF NOT EXISTS idx_nhl_blocked_shot_events_shooter_pos
  ON nhl.blocked_shot_events (blocking_team_id, shooter_position_bucket, game_date);
