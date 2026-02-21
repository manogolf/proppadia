-- Phase 1 schema move for MLB-owned tables.
-- Keep backward-compatible public views so existing queries keep working.
-- Shared tables (for now): none.

BEGIN;

CREATE SCHEMA IF NOT EXISTS mlb;

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

        IF public_relkind IN ('r', 'p') THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA mlb', table_name);
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
              AND tbl_ns.nspname = 'mlb'
              AND seq_ns.nspname <> 'mlb'
        LOOP
            EXECUTE format(
                'ALTER SEQUENCE %I.%I SET SCHEMA mlb',
                seq_rec.seq_schema,
                seq_rec.seq_name
            );
        END LOOP;

        IF to_regclass(format('mlb.%I', table_name)) IS NOT NULL THEN
            EXECUTE format(
                'CREATE OR REPLACE VIEW public.%I AS SELECT * FROM mlb.%I',
                table_name,
                table_name
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    role_name text;
    app_roles text[] := ARRAY['postgres', 'anon', 'authenticated', 'service_role'];
BEGIN
    FOREACH role_name IN ARRAY app_roles LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA mlb TO %I', role_name);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA mlb TO %I', role_name);
            EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA mlb TO %I', role_name);
        END IF;
    END LOOP;
END $$;

COMMIT;
