-- Rollback for phase 2: move the remaining MLB relations back to public schema.
-- This rollback intentionally covers only the phase-2 object set and does not
-- move phase-1 core runtime tables back out of mlb.

DO $$
DECLARE
    object_name text;
    object_kind "char";
    object_names constant text[] := ARRAY[
        'batter_bvp_vs_starter',
        'batter_rollups_pds',
        'bvp_pairs',
        'bvp_pitcher_game_agg',
        'bvp_rollup_prior',
        'export_today_batter_tb',
        'export_train_batter_doubles',
        'export_train_batter_hits',
        'export_train_batter_home_runs',
        'export_train_batter_hrr',
        'export_train_batter_rbis',
        'export_train_batter_runs',
        'export_train_batter_runs_rbis',
        'export_train_batter_singles',
        'export_train_batter_stolen_bases',
        'export_train_batter_strikeouts_batting',
        'export_train_batter_tb',
        'export_train_batter_total_bases',
        'export_train_batter_triples',
        'export_train_batter_walks',
        'export_train_pitcher_earned_runs',
        'export_train_pitcher_earned_runs_clean',
        'export_train_pitcher_earned_runs_final',
        'export_train_pitcher_earned_runs_starters',
        'export_train_pitcher_hits_allowed',
        'export_train_pitcher_hits_allowed_clean',
        'export_train_pitcher_hits_allowed_final',
        'export_train_pitcher_hits_allowed_starters',
        'export_train_pitcher_outs_recorded',
        'export_train_pitcher_outs_recorded_clean',
        'export_train_pitcher_outs_recorded_final',
        'export_train_pitcher_outs_recorded_starters',
        'export_train_pitcher_strikeouts_pitching',
        'export_train_pitcher_strikeouts_pitching_clean',
        'export_train_pitcher_strikeouts_pitching_final',
        'export_train_pitcher_strikeouts_pitching_starters',
        'export_train_pitcher_walks_allowed',
        'export_train_pitcher_walks_allowed_clean',
        'export_train_pitcher_walks_allowed_final',
        'export_train_pitcher_walks_allowed_starters',
        'ml_tb_features',
        'ml_tb_features_v2',
        'ml_tb_features_v3',
        'ml_tb_features_v4',
        'mlb_live_streaks',
        'mlb_team_map',
        'model_accuracy_metrics_view',
        'model_accuracy_metrics_weekly_view',
        'model_metadata',
        'opp_starter_per_game',
        'pitcher_last3_agg',
        'pitcher_rolling_per9',
        'pitcher_rolling_per9_cast',
        'player_pitching_games',
        'player_pitching_laststarts_mat',
        'player_rolling_agg_mat',
        'player_rolling_batting_agg',
        'player_rolling_pitching_rates',
        'player_stats_starters',
        'prop_source_priority',
        'prop_types',
        'starting_pitchers_mt',
        'starting_pitchers_ref',
        'starting_pitchers_ref_v2',
        'starting_pitchers_ref_v3',
        'team_game_runs',
        'team_game_runs_allowed',
        'team_rolling_agg_v1',
        'team_runs_rolling',
        'teams_by_id',
        'train_batter_total_bases'
    ];
BEGIN
    FOREACH object_name IN ARRAY object_names LOOP
        SELECT c.relkind
          INTO object_kind
          FROM pg_class c
          JOIN pg_namespace n
            ON n.oid = c.relnamespace
         WHERE n.nspname = 'mlb'
           AND c.relname = object_name
           AND c.relkind IN ('r', 'v', 'm')
         LIMIT 1;

        IF object_kind IS NULL THEN
            RAISE NOTICE 'Skipping mlb.%: not found', object_name;
            CONTINUE;
        END IF;

        IF to_regclass(format('public.%I', object_name)) IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot rollback mlb.% to public: public.% already exists', object_name, object_name;
        END IF;

        IF object_kind = 'r' THEN
            EXECUTE format('ALTER TABLE mlb.%I SET SCHEMA public', object_name);
        ELSIF object_kind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW mlb.%I SET SCHEMA public', object_name);
        ELSIF object_kind = 'v' THEN
            EXECUTE format('ALTER VIEW mlb.%I SET SCHEMA public', object_name);
        ELSE
            RAISE EXCEPTION 'Unsupported relkind % for mlb.%', object_kind, object_name;
        END IF;

        RAISE NOTICE 'Rolled back mlb.% (%) to public.%', object_name, object_kind, object_name;
    END LOOP;
END
$$;

