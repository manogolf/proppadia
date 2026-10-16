-- PREPARATION MIGRATION ONLY — DO NOT APPLY WITHOUT SEPARATE AUTHORIZATION.
-- Creates NOLOGIN permission groups and grants a narrow NHL foundation slice.
-- It does not revoke existing owner/API privileges, create credentials, change
-- the active daily connection, or grant these roles to any login.
-- After separate activation approval and secure login creation, the intended
-- memberships are:
--   GRANT proppadia_research_ro TO proppadia_nhl_research_login;
--   GRANT proppadia_nhl_ingest TO proppadia_nhl_daily_login;
--   GRANT proppadia_nhl_ops_writer TO proppadia_nhl_operations_login;
-- The correction group is granted only to a controlled operator session.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='nhl' AND p.prosecdef
  ) THEN
    RAISE EXCEPTION 'NHL_SECURITY_DEFINER_FUNCTION_REQUIRES_REVIEW';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_research_ro') THEN
    CREATE ROLE proppadia_research_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_nhl_ingest') THEN
    CREATE ROLE proppadia_nhl_ingest NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_nhl_source_correction') THEN
    CREATE ROLE proppadia_nhl_source_correction NOLOGIN NOSUPERUSER NOCREATEDB
      NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_nhl_ops_writer') THEN
    CREATE ROLE proppadia_nhl_ops_writer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname IN ('proppadia_research_ro', 'proppadia_nhl_ingest',
                      'proppadia_nhl_source_correction', 'proppadia_nhl_ops_writer')
      AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole
           OR rolreplication OR rolbypassrls)
  ) THEN
    RAISE EXCEPTION 'NHL_PERMISSION_GROUP_ROLE_ATTRIBUTES_UNSAFE';
  END IF;
END
$$;

-- Permission groups must not inherit any other role's privileges. Login
-- membership is added only during the separately approved activation step.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_auth_members m
    JOIN pg_roles g ON g.oid = m.member
    WHERE g.rolname IN ('proppadia_research_ro', 'proppadia_nhl_ingest',
                        'proppadia_nhl_source_correction', 'proppadia_nhl_ops_writer')
  ) THEN
    RAISE EXCEPTION 'NHL_PERMISSION_GROUP_INHERITS_OTHER_ROLE';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE postgres TO
  proppadia_research_ro, proppadia_nhl_ingest,
  proppadia_nhl_source_correction, proppadia_nhl_ops_writer;
GRANT USAGE ON SCHEMA nhl TO
  proppadia_research_ro, proppadia_nhl_ingest,
  proppadia_nhl_source_correction, proppadia_nhl_ops_writer;
REVOKE CREATE ON SCHEMA nhl FROM PUBLIC, proppadia_research_ro,
  proppadia_nhl_ingest, proppadia_nhl_source_correction, proppadia_nhl_ops_writer;

-- Preprovision feature columns formerly added by runtime daily SQL.
-- Restricted operations logins intentionally have no ALTER privilege.
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d3_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d5_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d20_toi_min_avg numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_min_sd  numeric,
  ADD COLUMN IF NOT EXISTS d10_toi_cv      numeric,
  ADD COLUMN IF NOT EXISTS toi_trend_3v10  numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_games_in_window                int NULL,

  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games              int NULL,
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate       double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_std      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_std    double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_games_in_window                int NULL,

  ADD COLUMN IF NOT EXISTS d10_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_available             boolean NULL,
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket            text NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket            text NULL,

  ADD COLUMN IF NOT EXISTS pairings_source                    text NULL,
  ADD COLUMN IF NOT EXISTS pairings_updated_at                timestamptz NULL;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_player_id bigint;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_sec integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_share numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_avg numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_std numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS pairings_source text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS pairings_updated_at timestamptz;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_games_in_window integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_std double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_games_in_window integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_repeat_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_distinct_count integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_top_mate_games_with_shiftcharts integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_repeat_rate double precision;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_distinct_count integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_top_mate_games_with_shiftcharts integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS mate_stability_source text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS mate_stability_updated_at timestamptz;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_missing_flag integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_available boolean;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_missing_flag integer;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket text;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d20_pairings_available boolean;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_5on5 numeric,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pp   numeric,
  ADD COLUMN IF NOT EXISTS szn_toi_per_game_pk   numeric,
  ADD COLUMN IF NOT EXISTS season_5on5_icetime_per_game numeric,
  ADD COLUMN IF NOT EXISTS season_5on4_icetime_per_game numeric,
  ADD COLUMN IF NOT EXISTS season_4on5_icetime_per_game numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS pp_role_share_final numeric,
  ADD COLUMN IF NOT EXISTS pp_role_source text;
ALTER TABLE nhl.training_features_goalie_saves_v2
  ADD COLUMN IF NOT EXISTS start_prob numeric;
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS num_sog_last5 numeric,
  ADD COLUMN IF NOT EXISTS num_sog_last10 numeric,
  ADD COLUMN IF NOT EXISTS num_sog_szn_to_date numeric,
  ADD COLUMN IF NOT EXISTS num_event_last5 numeric,
  ADD COLUMN IF NOT EXISTS num_event_last10 numeric,
  ADD COLUMN IF NOT EXISTS num_event_szn_to_date numeric,
  ADD COLUMN IF NOT EXISTS team_num_sog_last10 numeric,
  ADD COLUMN IF NOT EXISTS team_num_event_last10 numeric,
  ADD COLUMN IF NOT EXISTS pace_index numeric;

-- Preprovision DDL-backed operational tables under the migration owner so
-- restricted daily roles never need schema CREATE or table ALTER privileges.
CREATE TABLE IF NOT EXISTS nhl.shiftcharts_shifts (
  game_id bigint NOT NULL, shift_id bigint NOT NULL, player_id bigint NOT NULL,
  team_id int NOT NULL, period int NOT NULL, start_time text, end_time text,
  duration text, start_sec int NOT NULL, end_sec int NOT NULL, dur_sec int NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (game_id, shift_id)
);
CREATE INDEX IF NOT EXISTS idx_shiftcharts_shifts_game_team
  ON nhl.shiftcharts_shifts (game_id, team_id);
CREATE INDEX IF NOT EXISTS idx_shiftcharts_shifts_game_player
  ON nhl.shiftcharts_shifts (game_id, player_id);
CREATE TABLE IF NOT EXISTS nhl.shiftcharts_pairings_game (
  game_id bigint NOT NULL, player_id bigint NOT NULL, team_id int NOT NULL,
  toi_sec int NOT NULL, top_mate_player_id bigint,
  top_mate_overlap_sec int NOT NULL DEFAULT 0,
  top_mate_overlap_share numeric, top3_overlap_share_avg numeric,
  top3_overlap_share_std numeric, computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_shiftcharts_pairings_game_team
  ON nhl.shiftcharts_pairings_game (game_id, team_id);
CREATE TABLE IF NOT EXISTS nhl.game_manpower_segments (
  game_id bigint NOT NULL, period int NOT NULL, start_sec int NOT NULL,
  end_sec int NOT NULL, pp_team_id int NOT NULL, pk_team_id int NOT NULL,
  source text NOT NULL DEFAULT 'pbp', created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, period, start_sec, end_sec, pp_team_id)
);
CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_game
  ON nhl.game_manpower_segments (game_id);
CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_team
  ON nhl.game_manpower_segments (pp_team_id, pk_team_id);
CREATE TABLE IF NOT EXISTS nhl.shiftcharts_raw (
  game_id bigint NOT NULL, shift_id bigint NOT NULL, player_id bigint, team_id int,
  period int, start_time text, end_time text, duration text,
  start_sec int, end_sec int, duration_sec int, raw_json jsonb NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (game_id, shift_id)
);
CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_game ON nhl.shiftcharts_raw (game_id);
CREATE INDEX IF NOT EXISTS idx_shiftcharts_raw_player ON nhl.shiftcharts_raw (player_id);

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_research_ro;
GRANT SELECT ON nhl.goalie_game_logs_raw, nhl.games, nhl.teams, nhl.players,
  nhl.roster_status, nhl.team_external_ids, nhl.player_external_ids,
  nhl.import_players_stage, nhl.import_skater_logs_stage,
  nhl.import_goalie_logs_stage, nhl.shiftcharts_raw, nhl.shiftcharts_shifts,
  nhl.shiftcharts_pairings_game, nhl.game_manpower_segments,
  nhl.shift_teammate_overlap_features_game,
  nhl.shift_teammate_overlap_features_rolling_d10,
  nhl.shift_teammate_overlap_game_recent_v2,
  nhl.team_roll10_m, nhl.tf_team_roll10, nhl.v_goalie_game_logs_played,
  nhl.v_slate_saves_features, nhl.v_slate_sog_features,
  nhl.training_features_sog_denali,
  nhl.training_features_nhl_sog_enriched_pregame_v2,
  nhl.training_features_goalie_saves_v2, nhl.team_context_rolling,
  nhl.predictions, nhl.eval_sog_daily
  TO proppadia_research_ro;

-- Routine acquisition/reconciliation. These grants do not authorize schema
-- changes, raw DELETE/TRUNCATE, or historical correction membership.
GRANT SELECT, INSERT, UPDATE ON nhl.games, nhl.teams, nhl.players,
  nhl.roster_status, nhl.team_external_ids, nhl.player_external_ids
  TO proppadia_nhl_ingest;
GRANT INSERT ON nhl.games_write_audit, nhl.games_season_audit
  TO proppadia_nhl_ingest;
GRANT USAGE ON SEQUENCE nhl.games_write_audit_audit_id_seq,
  nhl.games_season_audit_audit_id_seq TO proppadia_nhl_ingest;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON nhl.import_players_stage, nhl.import_skater_logs_stage,
     nhl.import_goalie_logs_stage TO proppadia_nhl_ingest;
GRANT SELECT, INSERT, UPDATE ON nhl.goalie_game_logs_raw TO proppadia_nhl_ingest;
GRANT SELECT, INSERT, UPDATE ON nhl.blocked_shot_events,
  nhl.shot_on_goal_events TO proppadia_nhl_ingest;
GRANT SELECT, INSERT, UPDATE ON nhl.shiftcharts_raw, nhl.shiftcharts_shifts,
  nhl.shiftcharts_pairings_game, nhl.game_manpower_segments
  TO proppadia_nhl_ingest;
GRANT SELECT ON nhl.v_slate_sog_features TO proppadia_nhl_ingest;

-- Feature construction, scoring, evaluation, and prediction persistence write
-- operational result tables under a separate login with no source-table DML.
GRANT SELECT ON nhl.games, nhl.teams, nhl.players, nhl.roster_status,
  nhl.skater_game_logs_raw, nhl.goalie_game_logs_raw,
  nhl.shiftcharts_shifts, nhl.shiftcharts_pairings_game,
  nhl.game_manpower_segments, nhl.shift_teammate_overlap_features_game,
  nhl.shift_teammate_overlap_features_rolling_d10,
  nhl.shift_teammate_overlap_game_recent_v2, nhl.team_roll10_m,
  nhl.tf_team_roll10, nhl.v_goalie_game_logs_played,
  nhl.v_slate_saves_features, nhl.v_slate_sog_features,
  nhl.training_features_sog_denali,
  nhl.training_features_nhl_sog_enriched_pregame_v2,
  nhl.training_features_goalie_saves_v2, nhl.team_context_rolling,
  nhl.predictions, nhl.eval_sog_daily
  TO proppadia_nhl_ops_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON nhl.training_features_sog_denali,
  nhl.training_features_nhl_sog_enriched_pregame_v2,
  nhl.training_features_goalie_saves_v2, nhl.team_context_rolling
  TO proppadia_nhl_ops_writer;
GRANT SELECT, INSERT, UPDATE ON nhl.predictions, nhl.eval_sog_daily
  TO proppadia_nhl_ops_writer;
GRANT USAGE, SELECT ON SEQUENCE nhl.predictions_prediction_id_seq
  TO proppadia_nhl_ops_writer;

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT INSERT (player_id, game_id, team_id, opponent_id, is_home, game_date,
              shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes, blocks,
              hits, fenwick_for, missed_shots, blocked_shots_taken, rebounds_for,
              takeaways, giveaways, penalties_drawn, penalties_taken,
              ev_shot_attempts, pp_shot_attempts, sh_shot_attempts,
              ev_sog, pp_sog, sh_sog, goals, assists, points)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT UPDATE (team_id, opponent_id, is_home, game_date, shots_on_goal,
              shot_attempts, toi_minutes, pp_toi_minutes, blocks, hits,
              fenwick_for, missed_shots, blocked_shots_taken, rebounds_for,
              takeaways, giveaways, penalties_drawn, penalties_taken,
              ev_shot_attempts, pp_shot_attempts, sh_shot_attempts,
              ev_sog, pp_sog, sh_sog, goals, assists, points)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON nhl.import_skater_logs_stage TO proppadia_nhl_ingest;
GRANT SELECT ON nhl.import_skater_logs_stage TO proppadia_research_ro;

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
GRANT UPDATE (shot_attempts, pp_toi_minutes)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;

-- The inspected raw table has no existing app/API write ACL. Keep the
-- foundation protected if broad grants are added later; this does not revoke
-- SELECT or affect the operational tables consumed by API routes.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON nhl.skater_game_logs_raw
  FROM PUBLIC, anon, authenticated, service_role, proppadia_research_ro,
       proppadia_nhl_ops_writer;

-- No role in this file owns the schema or tables. No login is created or
-- granted membership. Routine scoring never receives raw source DML.
