-- Phase 2 cutover: drop public compatibility views for MLB tables.
-- Safe after code is moved to mlb.* (or uses search_path with mlb first).

BEGIN;

DO $$
DECLARE
    table_name text;
    relkind "char";
    table_names text[] := ARRAY[
        'bvp_stats',
        'game_info',
        'model_training_props',
        'player_props',
        'player_derived_stats',
        'player_ids',
        'player_profiles_cache',
        'player_stats',
        'player_streak_history',
        'player_streak_profiles',
        'player_team_by_game',
        'prop_features_precomputed'
    ];
BEGIN
    FOREACH table_name IN ARRAY table_names LOOP
        SELECT c.relkind
          INTO relkind
          FROM pg_class c
          JOIN pg_namespace n
            ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relname = table_name;

        IF relkind = 'v' THEN
            EXECUTE format('DROP VIEW public.%I', table_name);
        END IF;
    END LOOP;
END $$;

COMMIT;
