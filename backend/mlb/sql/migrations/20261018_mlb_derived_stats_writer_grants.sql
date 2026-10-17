-- Narrow routine writer access for recomputable per-game rolling features.
-- Protected source and established training-result writes remain behind the
-- separate fixed SECURITY DEFINER entry points.
DO $preflight$
DECLARE
    required_columns text[] := ARRAY[
        'player_id','game_id','game_date','team','is_home','updated_at',
        'd7_hits','d7_runs_scored','d7_rbis','d7_home_runs','d7_singles','d7_doubles','d7_triples','d7_walks','d7_strikeouts_batting','d7_stolen_bases','d7_total_bases','d7_hits_runs_rbis','d7_runs_rbis','d7_outs_recorded','d7_strikeouts_pitching','d7_walks_allowed','d7_earned_runs','d7_hits_allowed',
        'd15_hits','d15_runs_scored','d15_rbis','d15_home_runs','d15_singles','d15_doubles','d15_triples','d15_walks','d15_strikeouts_batting','d15_stolen_bases','d15_total_bases','d15_hits_runs_rbis','d15_runs_rbis','d15_outs_recorded','d15_strikeouts_pitching','d15_walks_allowed','d15_earned_runs','d15_hits_allowed',
        'd30_hits','d30_runs_scored','d30_rbis','d30_home_runs','d30_singles','d30_doubles','d30_triples','d30_walks','d30_strikeouts_batting','d30_stolen_bases','d30_total_bases','d30_hits_runs_rbis','d30_runs_rbis','d30_outs_recorded','d30_strikeouts_pitching','d30_walks_allowed','d30_earned_runs','d30_hits_allowed',
        'd7_at_bats','d15_at_bats','d30_at_bats','d7_plate_appearances','d15_plate_appearances','d30_plate_appearances'
    ];
    missing_columns text[];
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='proppadia_mlb_ingest') THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;
    SELECT array_agg(required.column_name ORDER BY required.column_name)
      INTO missing_columns
      FROM unnest(required_columns) AS required(column_name)
     WHERE NOT EXISTS (
         SELECT 1 FROM pg_catalog.pg_attribute a
          WHERE a.attrelid='mlb.player_derived_stats'::regclass
            AND a.attname=required.column_name AND a.attnum>0 AND NOT a.attisdropped
     );
    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'player_derived_stats writer columns are missing: %', array_to_string(missing_columns, ', ');
    END IF;
END
$preflight$;

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

GRANT SELECT ON mlb.player_stats, mlb.player_derived_stats TO proppadia_mlb_ingest;
GRANT INSERT (
    player_id,game_id,game_date,team,is_home,updated_at,
    d7_hits,d7_runs_scored,d7_rbis,d7_home_runs,d7_singles,d7_doubles,d7_triples,d7_walks,d7_strikeouts_batting,d7_stolen_bases,d7_total_bases,d7_hits_runs_rbis,d7_runs_rbis,d7_outs_recorded,d7_strikeouts_pitching,d7_walks_allowed,d7_earned_runs,d7_hits_allowed,
    d15_hits,d15_runs_scored,d15_rbis,d15_home_runs,d15_singles,d15_doubles,d15_triples,d15_walks,d15_strikeouts_batting,d15_stolen_bases,d15_total_bases,d15_hits_runs_rbis,d15_runs_rbis,d15_outs_recorded,d15_strikeouts_pitching,d15_walks_allowed,d15_earned_runs,d15_hits_allowed,
    d30_hits,d30_runs_scored,d30_rbis,d30_home_runs,d30_singles,d30_doubles,d30_triples,d30_walks,d30_strikeouts_batting,d30_stolen_bases,d30_total_bases,d30_hits_runs_rbis,d30_runs_rbis,d30_outs_recorded,d30_strikeouts_pitching,d30_walks_allowed,d30_earned_runs,d30_hits_allowed,
    d7_at_bats,d15_at_bats,d30_at_bats,d7_plate_appearances,d15_plate_appearances,d30_plate_appearances
) ON mlb.player_derived_stats TO proppadia_mlb_ingest;
GRANT UPDATE (
    player_id,game_id,game_date,team,is_home,updated_at,
    d7_hits,d7_runs_scored,d7_rbis,d7_home_runs,d7_singles,d7_doubles,d7_triples,d7_walks,d7_strikeouts_batting,d7_stolen_bases,d7_total_bases,d7_hits_runs_rbis,d7_runs_rbis,d7_outs_recorded,d7_strikeouts_pitching,d7_walks_allowed,d7_earned_runs,d7_hits_allowed,
    d15_hits,d15_runs_scored,d15_rbis,d15_home_runs,d15_singles,d15_doubles,d15_triples,d15_walks,d15_strikeouts_batting,d15_stolen_bases,d15_total_bases,d15_hits_runs_rbis,d15_runs_rbis,d15_outs_recorded,d15_strikeouts_pitching,d15_walks_allowed,d15_earned_runs,d15_hits_allowed,
    d30_hits,d30_runs_scored,d30_rbis,d30_home_runs,d30_singles,d30_doubles,d30_triples,d30_walks,d30_strikeouts_batting,d30_stolen_bases,d30_total_bases,d30_hits_runs_rbis,d30_runs_rbis,d30_outs_recorded,d30_strikeouts_pitching,d30_walks_allowed,d30_earned_runs,d30_hits_allowed,
    d7_at_bats,d15_at_bats,d30_at_bats,d7_plate_appearances,d15_plate_appearances,d30_plate_appearances
) ON mlb.player_derived_stats TO proppadia_mlb_ingest;
