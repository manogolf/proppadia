-- Fixed ordinary-writer entry points for roster identity/current status,
-- official schedule metadata, and recomputable BvP/PvB features.
-- Protected source/training tables remain behind earlier fixed functions.
DO $preflight$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='proppadia_mlb_ingest') THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;
    IF to_regclass('mlb.player_ids') IS NULL OR to_regclass('mlb.game_info') IS NULL
       OR to_regclass('mlb.prop_features_precomputed') IS NULL THEN
        RAISE EXCEPTION 'required adjacent MLB writer relation is missing';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='player_id' AND attnum>0 AND NOT attisdropped)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.game_info'::regclass AND attname='game_id' AND attnum>0 AND NOT attisdropped)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.prop_features_precomputed'::regclass AND attname='feature_set_tag' AND attnum>0 AND NOT attisdropped) THEN
        RAISE EXCEPTION 'required adjacent MLB writer identity column is missing';
    END IF;
END
$preflight$;

CREATE OR REPLACE FUNCTION mlb.safe_upsert_player_roster(p_row jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,mlb
AS $function$
DECLARE
    c record;
    v_exists boolean;
    v_cols text := '';
    v_values text := '';
    v_updates text := '';
    v_sql text;
    v_count integer;
BEGIN
    IF jsonb_typeof(p_row) IS DISTINCT FROM 'object'
       OR p_row - ARRAY['player_id','player_name','team','team_id'] <> '{}'::jsonb
       OR COALESCE(p_row->>'player_id','') !~ '^[1-9][0-9]*$' THEN
        RAISE EXCEPTION 'invalid roster player payload' USING ERRCODE='22023';
    END IF;
    PERFORM 1 FROM mlb.player_ids WHERE player_id=(p_row->>'player_id')::bigint FOR UPDATE;
    v_exists := FOUND;
    IF v_exists THEN
        FOR c IN
            SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid='mlb.player_ids'::regclass AND a.attnum>0 AND NOT a.attisdropped
               AND a.attname=ANY(ARRAY['player_name','team','team_id']) AND p_row ? a.attname
             ORDER BY array_position(ARRAY['player_name','team','team_id'],a.attname)
        LOOP
            v_updates := concat_ws(', ',NULLIF(v_updates,''),
                format('%1$I=COALESCE((jsonb_populate_record(NULL::mlb.player_ids,$1)).%1$I,mlb.player_ids.%1$I)',c.attname));
        END LOOP;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='active' AND attnum>0 AND NOT attisdropped) THEN
            v_updates := concat_ws(', ',NULLIF(v_updates,''),'active=TRUE');
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='status' AND attnum>0 AND NOT attisdropped) THEN
            v_updates := concat_ws(', ',NULLIF(v_updates,''),'status=''active''');
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='updated_at' AND attnum>0 AND NOT attisdropped) THEN
            v_updates := concat_ws(', ',NULLIF(v_updates,''),'updated_at=clock_timestamp()');
        END IF;
        IF v_updates='' THEN RETURN 0; END IF;
        EXECUTE format('UPDATE mlb.player_ids SET %s WHERE player_id=$2',v_updates)
          USING p_row,(p_row->>'player_id')::bigint;
        GET DIAGNOSTICS v_count=ROW_COUNT;
        RETURN v_count;
    END IF;
    IF NULLIF(btrim(p_row->>'player_name'),'') IS NULL THEN
        RAISE EXCEPTION 'new roster identity requires a verified non-empty player_name' USING ERRCODE='22023';
    END IF;
    FOR c IN
        SELECT a.attname FROM pg_catalog.pg_attribute a
         WHERE a.attrelid='mlb.player_ids'::regclass AND a.attnum>0 AND NOT a.attisdropped
           AND (a.attname='player_id' OR (a.attname=ANY(ARRAY['player_name','team','team_id']) AND p_row ? a.attname))
         ORDER BY array_position(ARRAY['player_id','player_name','team','team_id'],a.attname)
    LOOP
        v_cols := concat_ws(', ',NULLIF(v_cols,''),format('%I',c.attname));
        v_values := concat_ws(', ',NULLIF(v_values,''),format('(jsonb_populate_record(NULL::mlb.player_ids,$1)).%I',c.attname));
        IF c.attname IN ('player_name','team','team_id') THEN
            v_updates := concat_ws(', ',NULLIF(v_updates,''),format('%I=COALESCE(EXCLUDED.%I,mlb.player_ids.%I)',c.attname,c.attname,c.attname));
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='active' AND attnum>0 AND NOT attisdropped) THEN
        v_cols := v_cols || ', active'; v_values := v_values || ', TRUE'; v_updates := concat_ws(', ',NULLIF(v_updates,''),'active=TRUE');
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='status' AND attnum>0 AND NOT attisdropped) THEN
        v_cols := v_cols || ', status'; v_values := v_values || ', ''active'''; v_updates := concat_ws(', ',NULLIF(v_updates,''), 'status=''active''');
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='updated_at' AND attnum>0 AND NOT attisdropped) THEN
        v_cols := v_cols || ', updated_at'; v_values := v_values || ', clock_timestamp()'; v_updates := concat_ws(', ',NULLIF(v_updates,''),'updated_at=clock_timestamp()');
    END IF;
    v_sql := format('INSERT INTO mlb.player_ids (%s) SELECT %s FROM jsonb_populate_record(NULL::mlb.player_ids,$1) ON CONFLICT (player_id) %s',
        v_cols,v_values,CASE WHEN v_updates='' THEN 'DO NOTHING' ELSE 'DO UPDATE SET '||v_updates END);
    EXECUTE v_sql USING p_row;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count;
END
$function$;

CREATE OR REPLACE FUNCTION mlb.safe_insert_player_id_if_absent(p_row jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,mlb
AS $function$
DECLARE c record; v_cols text:=''; v_values text:=''; v_sql text; v_count integer;
BEGIN
    IF jsonb_typeof(p_row) IS DISTINCT FROM 'object'
       OR p_row - ARRAY['player_id','player_name','team','team_id','is_placeholder'] <> '{}'::jsonb
       OR COALESCE(p_row->>'player_id','') !~ '^[1-9][0-9]*$' THEN
        RAISE EXCEPTION 'invalid player identity payload' USING ERRCODE='22023';
    END IF;
    FOR c IN
        SELECT a.attname FROM pg_catalog.pg_attribute a
         WHERE a.attrelid='mlb.player_ids'::regclass AND a.attnum>0 AND NOT a.attisdropped
           AND (a.attname='player_id' OR (a.attname=ANY(ARRAY['player_name','team','team_id','is_placeholder']) AND p_row ? a.attname))
         ORDER BY array_position(ARRAY['player_id','player_name','team','team_id','is_placeholder'],a.attname)
    LOOP
        v_cols := concat_ws(', ',NULLIF(v_cols,''),format('%I',c.attname));
        v_values := concat_ws(', ',NULLIF(v_values,''),format('(jsonb_populate_record(NULL::mlb.player_ids,$1)).%I',c.attname));
    END LOOP;
    v_sql := format('INSERT INTO mlb.player_ids (%s) SELECT %s FROM jsonb_populate_record(NULL::mlb.player_ids,$1) ON CONFLICT (player_id) DO NOTHING',v_cols,v_values);
    EXECUTE v_sql USING p_row;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count;
END
$function$;

CREATE OR REPLACE FUNCTION mlb.safe_mark_player_roster_inactive(
    p_active_ids bigint[], p_successful_teams integer, p_total_teams integer
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,mlb
AS $function$
DECLARE v_updates text:=''; v_count integer;
BEGIN
    IF p_total_teams IS NULL OR p_total_teams<1 OR p_successful_teams IS NULL
       OR p_successful_teams>p_total_teams
       OR p_successful_teams<GREATEST(p_total_teams-1,1)
       OR p_active_ids IS NULL OR cardinality(p_active_ids)=0 THEN
        RAISE EXCEPTION 'roster inactive threshold or active identity set is invalid' USING ERRCODE='23514';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='active' AND attnum>0 AND NOT attisdropped) THEN
        v_updates := concat_ws(', ',NULLIF(v_updates,''),'active=FALSE');
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='status' AND attnum>0 AND NOT attisdropped) THEN
        v_updates := concat_ws(', ',NULLIF(v_updates,''),'status=''inactive''');
    END IF;
    -- The retained production player_ids shape has neither active nor status.
    -- Do not touch updated_at or report affected rows when there is no status
    -- field to mark; callers must not infer inactive state from a timestamp.
    IF v_updates='' THEN RETURN 0; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='mlb.player_ids'::regclass AND attname='updated_at' AND attnum>0 AND NOT attisdropped) THEN
        v_updates := concat_ws(', ',NULLIF(v_updates,''),'updated_at=clock_timestamp()');
    END IF;
    EXECUTE format('UPDATE mlb.player_ids SET %s WHERE NOT (player_id=ANY($1))',v_updates) USING p_active_ids;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count;
END
$function$;

CREATE OR REPLACE FUNCTION mlb.safe_upsert_game_info(p_row jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,mlb
AS $function$
DECLARE v_count integer;
BEGIN
    IF jsonb_typeof(p_row) IS DISTINCT FROM 'object'
       OR p_row - ARRAY['game_id','game_time','game_date','home_team_id','away_team_id','home_team_abbr','away_team_abbr'] <> '{}'::jsonb
       OR COALESCE(p_row->>'game_id','') !~ '^[1-9][0-9]*$'
       OR NULLIF(p_row->>'game_date','') IS NULL THEN
        RAISE EXCEPTION 'invalid game metadata payload' USING ERRCODE='22023';
    END IF;
    INSERT INTO mlb.game_info (
        game_id,game_time,game_date,home_team_id,away_team_id,home_team_abbr,away_team_abbr
    ) SELECT v.game_id,v.game_time,v.game_date,v.home_team_id,v.away_team_id,v.home_team_abbr,v.away_team_abbr
      FROM jsonb_to_record(p_row) AS v(
        game_id bigint,game_time timestamp without time zone,game_date date,home_team_id bigint,away_team_id bigint,
        home_team_abbr text,away_team_abbr text
      )
    ON CONFLICT (game_id) DO UPDATE SET
        game_time=COALESCE(mlb.game_info.game_time,EXCLUDED.game_time),game_date=EXCLUDED.game_date,
        home_team_id=COALESCE(mlb.game_info.home_team_id,EXCLUDED.home_team_id),
        away_team_id=COALESCE(mlb.game_info.away_team_id,EXCLUDED.away_team_id),
        home_team_abbr=COALESCE(mlb.game_info.home_team_abbr,EXCLUDED.home_team_abbr),
        away_team_abbr=COALESCE(mlb.game_info.away_team_abbr,EXCLUDED.away_team_abbr);
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count;
END
$function$;

CREATE OR REPLACE FUNCTION mlb.safe_upsert_prop_features_precomputed(p_rows jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,mlb
AS $function$
DECLARE v record; v_count integer; v_total integer:=0;
BEGIN
    IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'BvP/PvB feature payload must be an array' USING ERRCODE='22023';
    END IF;
    FOR v IN SELECT * FROM jsonb_to_recordset(p_rows) AS x(
        prop_type text,player_id bigint,game_id bigint,game_date date,features jsonb,feature_set_tag text,model_tag text
    ) LOOP
        IF v.prop_type IS NULL OR v.player_id IS NULL OR v.player_id<1 OR v.game_id IS NULL OR v.game_id<1
           OR v.game_date IS NULL OR jsonb_typeof(v.features) IS DISTINCT FROM 'object'
           OR NULLIF(v.feature_set_tag,'') IS NULL OR NULLIF(v.model_tag,'') IS NULL THEN
            RAISE EXCEPTION 'invalid BvP/PvB feature row' USING ERRCODE='22023';
        END IF;
        INSERT INTO mlb.prop_features_precomputed(prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag,computed_at)
        VALUES(v.prop_type,v.player_id,v.game_id,v.game_date,v.features,v.feature_set_tag,v.model_tag,clock_timestamp())
        ON CONFLICT(prop_type,player_id,game_id,feature_set_tag) DO UPDATE SET
            game_date=EXCLUDED.game_date,
            features=COALESCE(mlb.prop_features_precomputed.features,'{}'::jsonb)||EXCLUDED.features,
            model_tag=EXCLUDED.model_tag,computed_at=clock_timestamp();
        GET DIAGNOSTICS v_count=ROW_COUNT;
        v_total:=v_total+v_count;
    END LOOP;
    RETURN v_total;
END
$function$;

REVOKE ALL ON FUNCTION mlb.safe_upsert_player_roster(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_insert_player_id_if_absent(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_mark_player_roster_inactive(bigint[],integer,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_upsert_game_info(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_upsert_prop_features_precomputed(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.safe_upsert_player_roster(jsonb),mlb.safe_insert_player_id_if_absent(jsonb),mlb.safe_mark_player_roster_inactive(bigint[],integer,integer),mlb.safe_upsert_game_info(jsonb),mlb.safe_upsert_prop_features_precomputed(jsonb) TO proppadia_mlb_ingest;

REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON mlb.player_ids,mlb.game_info,mlb.prop_features_precomputed FROM proppadia_mlb_ingest;
DO $column_acl$
DECLARE c record;
BEGIN
    FOR c IN
        SELECT n.nspname,r.relname,a.attname FROM pg_catalog.pg_class r
        JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid=r.oid
        WHERE n.nspname='mlb' AND r.relname IN ('player_ids','game_info','prop_features_precomputed')
          AND a.attnum>0 AND NOT a.attisdropped
    LOOP
        EXECUTE format('REVOKE INSERT (%I) ON %I.%I FROM proppadia_mlb_ingest',c.attname,c.nspname,c.relname);
        EXECUTE format('REVOKE UPDATE (%I) ON %I.%I FROM proppadia_mlb_ingest',c.attname,c.nspname,c.relname);
    END LOOP;
END
$column_acl$;
GRANT SELECT ON mlb.game_info TO proppadia_mlb_ingest;
