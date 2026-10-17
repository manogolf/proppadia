-- Narrow, fill-only StatsAPI PA backfill path for the restricted MLB writer.
-- Requires the separately provisioned PA columns; does not grant table DML.
DO $preflight$
DECLARE
    required_columns text[] := ARRAY[
        'plate_appearances', 'hit_by_pitch', 'sacrifice_flies', 'sacrifice_hits',
        'catcher_interference', 'pa_source', 'pa_backfilled_at', 'position',
        'at_bats', 'hits', 'walks', 'strikeouts_batting', 'runs_scored', 'rbis', 'stolen_bases'
    ];
    missing_columns text[];
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_mlb_ingest') THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;
    IF has_table_privilege('proppadia_mlb_ingest', 'mlb.player_stats', 'INSERT')
       OR has_table_privilege('proppadia_mlb_ingest', 'mlb.player_stats', 'UPDATE')
       OR has_any_column_privilege('proppadia_mlb_ingest', 'mlb.player_stats', 'UPDATE')
       OR has_table_privilege('proppadia_mlb_ingest', 'mlb.player_stats', 'DELETE')
       OR has_table_privilege('proppadia_mlb_ingest', 'mlb.player_stats', 'TRUNCATE') THEN
        RAISE EXCEPTION 'player_stats safe write boundary must be installed before PA backfill';
    END IF;
    SELECT array_agg(required.column_name ORDER BY required.column_name)
      INTO missing_columns
      FROM unnest(required_columns) AS required(column_name)
     WHERE NOT EXISTS (
         SELECT 1 FROM pg_catalog.pg_attribute a
          WHERE a.attrelid = 'mlb.player_stats'::regclass
            AND a.attname = required.column_name AND a.attnum > 0 AND NOT a.attisdropped
     );
    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'player_stats PA backfill columns are missing: %', array_to_string(missing_columns, ', ');
    END IF;
END
$preflight$;

CREATE OR REPLACE FUNCTION mlb.safe_fill_player_stats_pa(
    p_player_id bigint,
    p_game_id bigint,
    p_plate_appearances integer,
    p_hit_by_pitch integer,
    p_sacrifice_flies integer,
    p_sacrifice_hits integer,
    p_catcher_interference integer,
    p_source text
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    current_row mlb.player_stats%ROWTYPE;
    n integer;
BEGIN
    IF p_player_id IS NULL OR p_game_id IS NULL OR p_plate_appearances IS NULL
       OR p_plate_appearances < 0 OR p_hit_by_pitch IS NULL OR p_hit_by_pitch < 0
       OR p_sacrifice_flies IS NULL OR p_sacrifice_flies < 0
       OR p_sacrifice_hits IS NULL OR p_sacrifice_hits < 0
       OR p_catcher_interference IS NULL OR p_catcher_interference < 0
       OR p_source IS DISTINCT FROM 'statsapi' THEN
        RAISE EXCEPTION 'invalid StatsAPI PA backfill input' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO current_row
      FROM mlb.player_stats
     WHERE player_id = p_player_id AND game_id = p_game_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    IF NOT (
        (NULLIF(current_row.position, '') IS NOT NULL AND current_row.position <> 'P')
        OR COALESCE(current_row.at_bats, 0) > 0
        OR COALESCE(current_row.hits, 0) > 0
        OR COALESCE(current_row.walks, 0) > 0
        OR COALESCE(current_row.strikeouts_batting, 0) > 0
        OR COALESCE(current_row.runs_scored, 0) > 0
        OR COALESCE(current_row.rbis, 0) > 0
        OR COALESCE(current_row.stolen_bases, 0) > 0
    ) THEN
        RETURN 0;
    END IF;

    IF (current_row.plate_appearances IS NOT NULL AND current_row.plate_appearances <> p_plate_appearances)
       OR (current_row.hit_by_pitch IS NOT NULL AND current_row.hit_by_pitch <> p_hit_by_pitch)
       OR (current_row.sacrifice_flies IS NOT NULL AND current_row.sacrifice_flies <> p_sacrifice_flies)
       OR (current_row.sacrifice_hits IS NOT NULL AND current_row.sacrifice_hits <> p_sacrifice_hits)
       OR (current_row.catcher_interference IS NOT NULL AND current_row.catcher_interference <> p_catcher_interference) THEN
        RAISE EXCEPTION 'StatsAPI PA backfill conflicts with populated source values'
            USING ERRCODE = '23514';
    END IF;

    IF current_row.pa_source IS NOT NULL AND current_row.pa_source <> p_source
       AND (current_row.plate_appearances IS NULL OR current_row.hit_by_pitch IS NULL
            OR current_row.sacrifice_flies IS NULL OR current_row.sacrifice_hits IS NULL
            OR current_row.catcher_interference IS NULL) THEN
        RAISE EXCEPTION 'StatsAPI PA backfill conflicts with existing source provenance'
            USING ERRCODE = '23514';
    END IF;

    UPDATE mlb.player_stats
       SET plate_appearances = COALESCE(plate_appearances, p_plate_appearances),
           hit_by_pitch = COALESCE(hit_by_pitch, p_hit_by_pitch),
           sacrifice_flies = COALESCE(sacrifice_flies, p_sacrifice_flies),
           sacrifice_hits = COALESCE(sacrifice_hits, p_sacrifice_hits),
           catcher_interference = COALESCE(catcher_interference, p_catcher_interference),
           pa_source = COALESCE(pa_source, p_source),
           pa_backfilled_at = now()
     WHERE player_id = p_player_id AND game_id = p_game_id
       AND (plate_appearances IS NULL OR hit_by_pitch IS NULL OR sacrifice_flies IS NULL
            OR sacrifice_hits IS NULL OR catcher_interference IS NULL OR pa_source IS NULL);
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$function$;

REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text) TO proppadia_mlb_ingest;
