-- Remove only the column grants added by 20261019_mlb_moneyline_lifecycle_ingest_write.sql.
-- First remove MLB_USE_RESTRICTED_INGEST=1 from bin/mlb_public_game_moneyline_daily_hook.sh.
BEGIN;

REVOKE SELECT (game_pk, game_date, scheduled_start_utc, game_number, home_team_id, away_team_id, home_runs, away_runs, official_status, official_final_effective_utc, observed_final_at_utc, source_identity, source_sha256) ON mlb.public_game_official_finals FROM proppadia_mlb_ingest;
REVOKE INSERT (game_pk, game_date, scheduled_start_utc, game_number, home_team_id, away_team_id, home_runs, away_runs, official_status, official_final_effective_utc, observed_final_at_utc, source_identity, source_sha256, content_sha256) ON mlb.public_game_official_finals FROM proppadia_mlb_ingest;
REVOKE SELECT (correction_id, game_pk, corrected_home_runs, corrected_away_runs, correction_reason, observed_at_utc, source_identity, source_sha256, corrected_content_sha256, created_at) ON mlb.public_game_official_final_corrections FROM proppadia_mlb_ingest;
REVOKE SELECT (model_version, prediction_cutoff_utc, state_hash) ON mlb.public_game_team_state_snapshots FROM proppadia_mlb_ingest;
REVOKE INSERT (model_version, prediction_cutoff_utc, state_through_game_date, state_hash, state_generated_at_utc, state_payload, payload_sha256) ON mlb.public_game_team_state_snapshots FROM proppadia_mlb_ingest;
REVOKE SELECT (game_date, game_id, model_version, prediction_snapshot_class, scheduled_start_utc, admission_status, prediction_payload, payload_sha256) ON mlb.public_game_moneyline_predictions FROM proppadia_mlb_ingest;
REVOKE INSERT (game_date, game_id, model_version, prediction_snapshot_class, scheduled_start_utc, prediction_timestamp_utc, prediction_cutoff_utc, home_team, away_team, home_win_probability, away_win_probability, predicted_winner, confidence_band, data_quality_status, model_hash, scorer_hash, source_schedule_hash, team_state_hash, admission_status, failure_reason, prediction_payload, payload_sha256) ON mlb.public_game_moneyline_predictions FROM proppadia_mlb_ingest;
REVOKE SELECT (game_date, game_id, model_version, prediction_snapshot_class, payload_sha256) ON mlb.public_game_moneyline_outcomes FROM proppadia_mlb_ingest;
REVOKE INSERT (game_date, game_id, model_version, prediction_snapshot_class, official_home_runs, official_away_runs, official_winner, prediction_correct, observed_outcome_probability, brier_contribution, log_loss_contribution, confidence_band, official_source_identity, official_source_sha256, grading_timestamp_utc, outcome_payload, payload_sha256) ON mlb.public_game_moneyline_outcomes FROM proppadia_mlb_ingest;

COMMIT;
