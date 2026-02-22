-- Remove legacy NHL relations not used by active daily/runtime pipeline
-- Generated from live nhl schema inventory after runtime dependency audit
BEGIN;
SET LOCAL search_path = nhl, public;

-- Drop legacy views
DROP VIEW IF EXISTS nhl.pairing_features_v2 CASCADE;
DROP VIEW IF EXISTS nhl.player_shot_history_denali CASCADE;
DROP VIEW IF EXISTS nhl.player_shot_phoenix_denali CASCADE;
DROP VIEW IF EXISTS nhl.player_sog_denali_base CASCADE;
DROP VIEW IF EXISTS nhl.points_training_frame_phoenix CASCADE;
DROP VIEW IF EXISTS nhl.shift_teammate_overlap_features_rolling_d20 CASCADE;
DROP VIEW IF EXISTS nhl.shift_teammate_overlap_game_v2 CASCADE;
DROP VIEW IF EXISTS nhl.shiftcharts_shifts_clean_v2 CASCADE;
DROP VIEW IF EXISTS nhl.shiftcharts_shifts_clean_v3 CASCADE;
DROP VIEW IF EXISTS nhl.skater_roll_windows_v1 CASCADE;
DROP VIEW IF EXISTS nhl.skater_rolling_v3 CASCADE;
DROP VIEW IF EXISTS nhl.skater_shot_game_totals CASCADE;
DROP VIEW IF EXISTS nhl.skater_special_teams_szn_to_date CASCADE;
DROP VIEW IF EXISTS nhl.sog_denali_rollups_v CASCADE;
DROP VIEW IF EXISTS nhl.sog_training_frame_phoenix CASCADE;
DROP VIEW IF EXISTS nhl.team_shot_history_denali CASCADE;
DROP VIEW IF EXISTS nhl.training_features_nhl_sog_enriched_pregame_v2_denali_cols CASCADE;
DROP VIEW IF EXISTS nhl.training_features_shots_v CASCADE;
DROP VIEW IF EXISTS nhl.training_features_sog_denali_export CASCADE;
DROP VIEW IF EXISTS nhl.training_features_sog_player_game_v2 CASCADE;
DROP VIEW IF EXISTS nhl.v_dqa_goalie_ready_coverage CASCADE;
DROP VIEW IF EXISTS nhl.v_dqa_sog_ready_coverage CASCADE;
DROP VIEW IF EXISTS nhl.v_site_sog_eval_publish CASCADE;
DROP VIEW IF EXISTS nhl.v_site_sog_predictions_publish CASCADE;
DROP VIEW IF EXISTS nhl.v_skater_game_logs_played CASCADE;
DROP VIEW IF EXISTS nhl.v_skater_logs_clean CASCADE;
DROP VIEW IF EXISTS nhl.v_skater_rolling_agg CASCADE;
DROP VIEW IF EXISTS nhl.v_sog_denali_rollups_per_game CASCADE;

-- Drop legacy materialized views
DROP MATERIALIZED VIEW IF EXISTS nhl.goalie_roll_feats_m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.sog_denali_rollups_mv CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.team_pp_toi_totals_by_date_team CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.tf_skater_attempts_roll10 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.training_features_nhl_saves_enr_filt CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.training_features_nhl_saves_enriched CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.training_features_nhl_sog_enriched CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.training_features_nhl_sog_enriched_pregame CASCADE;
DROP MATERIALIZED VIEW IF EXISTS nhl.training_features_nhl_sog_enriched_pregame_v2_mt CASCADE;

-- Drop legacy tables
DROP TABLE IF EXISTS nhl._points_stage CASCADE;
DROP TABLE IF EXISTS nhl.backfill_progress CASCADE;
DROP TABLE IF EXISTS nhl.data_quality_audit CASCADE;
DROP TABLE IF EXISTS nhl.games_season_audit CASCADE;
DROP TABLE IF EXISTS nhl.games_write_audit CASCADE;
DROP TABLE IF EXISTS nhl.goalie_rolling_agg CASCADE;
DROP TABLE IF EXISTS nhl.goalies2023_stage CASCADE;
DROP TABLE IF EXISTS nhl.goalies2023_stage_raw CASCADE;
DROP TABLE IF EXISTS nhl.goalies_szn_sit CASCADE;
DROP TABLE IF EXISTS nhl.import_player_external_ids_stage CASCADE;
DROP TABLE IF EXISTS nhl.import_skater_points_stage CASCADE;
DROP TABLE IF EXISTS nhl.keep_games_filter CASCADE;
DROP TABLE IF EXISTS nhl.lines_szn_sit_denali CASCADE;
DROP TABLE IF EXISTS nhl.pairing_features_store_v2 CASCADE;
DROP TABLE IF EXISTS nhl.player_game_2023_roll CASCADE;
DROP TABLE IF EXISTS nhl.player_game_2023_summary CASCADE;
DROP TABLE IF EXISTS nhl.player_game_2024_roll CASCADE;
DROP TABLE IF EXISTS nhl.player_game_2024_summary CASCADE;
DROP TABLE IF EXISTS nhl.player_game_shots_2023 CASCADE;
DROP TABLE IF EXISTS nhl.pp_roles_slate CASCADE;
DROP TABLE IF EXISTS nhl.roster_names CASCADE;
DROP TABLE IF EXISTS nhl.shift_teammate_overlap_game CASCADE;
DROP TABLE IF EXISTS nhl.shot_stats_denali CASCADE;
DROP TABLE IF EXISTS nhl.shots_all CASCADE;
DROP TABLE IF EXISTS nhl.shots_stage_2023 CASCADE;
DROP TABLE IF EXISTS nhl.shots_stage_2024 CASCADE;
DROP TABLE IF EXISTS nhl.skater_game_special_teams_exposure CASCADE;
DROP TABLE IF EXISTS nhl.skater_points_raw CASCADE;
DROP TABLE IF EXISTS nhl.skater_rolling_agg CASCADE;
DROP TABLE IF EXISTS nhl.skaters2023_stage CASCADE;
DROP TABLE IF EXISTS nhl.skaters2023_stage_raw CASCADE;
DROP TABLE IF EXISTS nhl.skaters_szn_sit CASCADE;
DROP TABLE IF EXISTS nhl.skaters_szn_sit_denali CASCADE;
DROP TABLE IF EXISTS nhl.skaters_szn_sit_stage CASCADE;
DROP TABLE IF EXISTS nhl.team_game_2023_roll CASCADE;
DROP TABLE IF EXISTS nhl.team_game_2023_summary CASCADE;
DROP TABLE IF EXISTS nhl.team_game_2024_roll CASCADE;
DROP TABLE IF EXISTS nhl.team_game_2024_summary CASCADE;
DROP TABLE IF EXISTS nhl.team_game_sit CASCADE;
DROP TABLE IF EXISTS nhl.teams_game_sit CASCADE;
DROP TABLE IF EXISTS nhl.teams_szn_sit_denali CASCADE;
DROP TABLE IF EXISTS nhl.training_features_shots CASCADE;
DROP TABLE IF EXISTS nhl.training_features_shots_2023 CASCADE;
DROP TABLE IF EXISTS nhl.training_features_shots_2024 CASCADE;
DROP TABLE IF EXISTS nhl.training_features_sog_player_game CASCADE;

COMMIT;
