-- backend/nhl/sql/create_player_sog_denali_base.sql

SET client_min_messages = warning;
SET search_path = nhl, public;

-- Canonical per-player, per-game SOG / attempts / TOI + rolling shot history
CREATE OR REPLACE VIEW nhl.player_sog_denali_base AS
SELECT
  h.season,
  h.game_id,
  h.player_id,

  -- Prefer the log's game_date if present, fall back to history view
  COALESCE(l.game_date, h.game_date) AS game_date,

  -- Team context from logs (numeric IDs, not text codes)
  l.team_id,
  l.opponent_id,
  l.is_home,

  -- Per-game box stats
  COALESCE(l.shots_on_goal, 0)::numeric      AS shots_on_goal,
  COALESCE(l.shot_attempts, 0)::numeric      AS shot_attempts,
  NULLIF(l.toi_minutes, 0)::numeric          AS toi_minutes,
  NULLIF(l.pp_toi_minutes, 0)::numeric       AS pp_toi_minutes,

  -- Rolling shot history (already Denali-style, from the history view)
  h.num_shotwasongoal_last5,
  h.num_shotwasongoal_last10,
  h.num_shotwasongoal_season_to_date,
  h.num_event_shot_last5,
  h.num_event_shot_last10,
  h.num_event_shot_season_to_date,

  -- Keep the original text codes around in case they’re useful for debugging
  h.teamcode,
  h.opponent_code

FROM nhl.player_shot_history_denali h
LEFT JOIN nhl.skater_game_logs_raw l
  ON l.player_id = h.player_id
 AND l.game_id   = h.game_id;
