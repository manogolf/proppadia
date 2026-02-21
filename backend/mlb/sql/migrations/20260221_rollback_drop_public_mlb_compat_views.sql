-- Rollback for phase 2 view drop: recreate public compatibility views.

BEGIN;

DO $$
DECLARE
    table_name text;
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
        IF to_regclass(format('mlb.%I', table_name)) IS NOT NULL
           AND to_regclass(format('public.%I', table_name)) IS NULL THEN
            EXECUTE format(
                'CREATE VIEW public.%I AS SELECT * FROM mlb.%I',
                table_name,
                table_name
            );
        END IF;
    END LOOP;
END $$;

COMMIT;
