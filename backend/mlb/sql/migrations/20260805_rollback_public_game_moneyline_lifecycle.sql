BEGIN;
DROP TABLE IF EXISTS mlb.public_game_moneyline_outcome_corrections;
DROP TABLE IF EXISTS mlb.public_game_moneyline_outcomes;
DROP TABLE IF EXISTS mlb.public_game_moneyline_predictions;
DROP TABLE IF EXISTS mlb.public_game_team_state_snapshots;
DROP TABLE IF EXISTS mlb.public_game_official_final_corrections;
DROP TABLE IF EXISTS mlb.public_game_official_finals;
DROP FUNCTION IF EXISTS mlb.reject_public_game_lifecycle_mutation();
COMMIT;
