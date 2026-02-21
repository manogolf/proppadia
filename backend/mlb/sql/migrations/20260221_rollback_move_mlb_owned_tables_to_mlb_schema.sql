-- Rollback for phase 1 MLB-owned schema move.
-- Drops public compatibility views, then moves base tables back to public.

BEGIN;

DO $$
DECLARE
    table_name text;
    seq_rec record;
    public_relkind "char";
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
          INTO public_relkind
          FROM pg_class c
          JOIN pg_namespace n
            ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relname = table_name;

        IF public_relkind = 'v' THEN
            EXECUTE format('DROP VIEW IF EXISTS public.%I', table_name);
        END IF;

        IF to_regclass(format('mlb.%I', table_name)) IS NOT NULL
           AND to_regclass(format('public.%I', table_name)) IS NULL THEN
            EXECUTE format('ALTER TABLE mlb.%I SET SCHEMA public', table_name);
        END IF;

        FOR seq_rec IN
            SELECT seq_ns.nspname AS seq_schema, seq.relname AS seq_name
            FROM pg_class tbl
            JOIN pg_namespace tbl_ns
              ON tbl_ns.oid = tbl.relnamespace
            JOIN pg_depend dep
              ON dep.refobjid = tbl.oid
             AND dep.deptype = 'a'
            JOIN pg_class seq
              ON seq.oid = dep.objid
             AND seq.relkind = 'S'
            JOIN pg_namespace seq_ns
              ON seq_ns.oid = seq.relnamespace
            WHERE tbl.relname = table_name
              AND tbl_ns.nspname = 'public'
              AND seq_ns.nspname = 'mlb'
        LOOP
            EXECUTE format(
                'ALTER SEQUENCE %I.%I SET SCHEMA public',
                seq_rec.seq_schema,
                seq_rec.seq_name
            );
        END LOOP;
    END LOOP;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'mlb')
       AND NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = 'mlb'
       ) THEN
        EXECUTE 'DROP SCHEMA mlb';
    END IF;
END $$;

COMMIT;
