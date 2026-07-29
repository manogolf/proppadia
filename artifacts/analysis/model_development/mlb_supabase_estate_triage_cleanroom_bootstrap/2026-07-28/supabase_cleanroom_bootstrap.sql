-- PLAN ONLY: not executed by the inventory utility.
-- Idempotent clean-room bootstrap. Review backup status and source gaps first.
BEGIN;
CREATE SCHEMA IF NOT EXISTS mlb_cleanroom_v1;
COMMENT ON SCHEMA mlb_cleanroom_v1 IS
  'Source-first MLB read-only boundary; generated 2026-07-28; no model features.';
CREATE OR REPLACE VIEW mlb_cleanroom_v1."games" AS SELECT "game_id", "game_time", "game_date", "home_team_id", "away_team_id", "home_team_abbr", "away_team_abbr", "starting_pitcher_id_home", "starting_pitcher_id_away" FROM mlb.game_info;
CREATE OR REPLACE VIEW mlb_cleanroom_v1."official_player_game_batting_and_pitching" AS SELECT "player_id", "game_id", "game_date", "team", "opponent", "is_home", "position", "hits", "total_bases", "rbis", "runs_scored", "strikeouts_batting", "walks", "singles", "doubles", "triples", "home_runs", "stolen_bases", "strikeouts_pitching", "walks_allowed", "hits_allowed", "outs_recorded", "earned_runs", "is_starter", "at_bats", "plate_appearances", "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference", "pa_source", "pa_backfilled_at" FROM mlb.player_stats;
COMMIT;
