-- Fail closed: remove only routine writer access to recomputable derived rows.
-- This does not grant any direct writes to protected source or result tables.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mlb.player_derived_stats FROM proppadia_mlb_ingest;
DO $column_acl$
DECLARE c record;
BEGIN
    FOR c IN
        SELECT a.attname FROM pg_catalog.pg_attribute a
         WHERE a.attrelid='mlb.player_derived_stats'::regclass
           AND a.attnum>0 AND NOT a.attisdropped
    LOOP
        EXECUTE format('REVOKE INSERT (%I) ON mlb.player_derived_stats FROM proppadia_mlb_ingest', c.attname);
        EXECUTE format('REVOKE UPDATE (%I) ON mlb.player_derived_stats FROM proppadia_mlb_ingest', c.attname);
    END LOOP;
END
$column_acl$;
REVOKE SELECT ON mlb.player_derived_stats FROM proppadia_mlb_ingest;
