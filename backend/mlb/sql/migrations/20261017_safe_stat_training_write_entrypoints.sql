-- Fixed, reviewed write boundaries for official MLB stats and training rows.
-- Apply as the existing schema owner after the preview fixture passes.
-- The application writer receives EXECUTE only; protected table DML is revoked.

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_mlb_ingest') THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;
END
$roles$;

CREATE OR REPLACE FUNCTION mlb.safe_upsert_player_stats(p_row jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v mlb.player_stats%ROWTYPE;
    n integer;
BEGIN
    IF p_row IS NULL OR jsonb_typeof(p_row) <> 'object'
       OR EXISTS (
           SELECT 1 FROM jsonb_object_keys(p_row) AS k(key)
           WHERE key <> ALL (ARRAY[
               'player_id','game_id','game_date','team','opponent','is_home','position',
               'plate_appearances','at_bats','hits','total_bases','rbis','runs_scored',
               'strikeouts_batting','walks','singles','doubles','triples','home_runs',
               'stolen_bases','strikeouts_pitching','walks_allowed','hits_allowed',
               'outs_recorded','earned_runs','is_starter'
           ])
       ) THEN
        RAISE EXCEPTION 'invalid player_stats payload' USING ERRCODE = '22023';
    END IF;
    v := jsonb_populate_record(NULL::mlb.player_stats, p_row);
    IF v.player_id IS NULL OR v.game_id IS NULL THEN
        RAISE EXCEPTION 'player_stats identity is required' USING ERRCODE = '22023';
    END IF;
    IF v.is_starter IS NOT NULL AND v.is_starter NOT IN (0, 1) THEN
        RAISE EXCEPTION 'is_starter must be NULL, 0, or 1' USING ERRCODE = '22023';
    END IF;

    INSERT INTO mlb.player_stats (
        player_id, game_id, game_date, team, opponent, is_home, position,
        plate_appearances, at_bats, hits, total_bases, rbis, runs_scored,
        strikeouts_batting, walks, singles, doubles, triples, home_runs,
        stolen_bases, strikeouts_pitching, walks_allowed, hits_allowed,
        outs_recorded, earned_runs, is_starter
    ) VALUES (
        v.player_id, v.game_id, v.game_date, v.team, v.opponent, v.is_home, v.position,
        v.plate_appearances, v.at_bats, v.hits, v.total_bases, v.rbis, v.runs_scored,
        v.strikeouts_batting, v.walks, v.singles, v.doubles, v.triples, v.home_runs,
        v.stolen_bases, v.strikeouts_pitching, v.walks_allowed, v.hits_allowed,
        v.outs_recorded, v.earned_runs, v.is_starter
    )
    ON CONFLICT (player_id, game_id) DO UPDATE SET
        game_date = COALESCE(mlb.player_stats.game_date, EXCLUDED.game_date),
        team = COALESCE(mlb.player_stats.team, EXCLUDED.team),
        opponent = COALESCE(mlb.player_stats.opponent, EXCLUDED.opponent),
        is_home = COALESCE(mlb.player_stats.is_home, EXCLUDED.is_home),
        position = COALESCE(mlb.player_stats.position, EXCLUDED.position),
        plate_appearances = COALESCE(mlb.player_stats.plate_appearances, EXCLUDED.plate_appearances),
        at_bats = COALESCE(mlb.player_stats.at_bats, EXCLUDED.at_bats),
        hits = COALESCE(mlb.player_stats.hits, EXCLUDED.hits),
        total_bases = COALESCE(mlb.player_stats.total_bases, EXCLUDED.total_bases),
        rbis = COALESCE(mlb.player_stats.rbis, EXCLUDED.rbis),
        runs_scored = COALESCE(mlb.player_stats.runs_scored, EXCLUDED.runs_scored),
        strikeouts_batting = COALESCE(mlb.player_stats.strikeouts_batting, EXCLUDED.strikeouts_batting),
        walks = COALESCE(mlb.player_stats.walks, EXCLUDED.walks),
        singles = COALESCE(mlb.player_stats.singles, EXCLUDED.singles),
        doubles = COALESCE(mlb.player_stats.doubles, EXCLUDED.doubles),
        triples = COALESCE(mlb.player_stats.triples, EXCLUDED.triples),
        home_runs = COALESCE(mlb.player_stats.home_runs, EXCLUDED.home_runs),
        stolen_bases = COALESCE(mlb.player_stats.stolen_bases, EXCLUDED.stolen_bases),
        strikeouts_pitching = COALESCE(mlb.player_stats.strikeouts_pitching, EXCLUDED.strikeouts_pitching),
        walks_allowed = COALESCE(mlb.player_stats.walks_allowed, EXCLUDED.walks_allowed),
        hits_allowed = COALESCE(mlb.player_stats.hits_allowed, EXCLUDED.hits_allowed),
        outs_recorded = COALESCE(mlb.player_stats.outs_recorded, EXCLUDED.outs_recorded),
        earned_runs = COALESCE(mlb.player_stats.earned_runs, EXCLUDED.earned_runs),
        is_starter = COALESCE(mlb.player_stats.is_starter, EXCLUDED.is_starter)
    WHERE (
        mlb.player_stats.game_date, mlb.player_stats.team, mlb.player_stats.opponent,
        mlb.player_stats.is_home, mlb.player_stats.position, mlb.player_stats.plate_appearances,
        mlb.player_stats.at_bats, mlb.player_stats.hits, mlb.player_stats.total_bases,
        mlb.player_stats.rbis, mlb.player_stats.runs_scored, mlb.player_stats.strikeouts_batting,
        mlb.player_stats.walks, mlb.player_stats.singles, mlb.player_stats.doubles,
        mlb.player_stats.triples, mlb.player_stats.home_runs, mlb.player_stats.stolen_bases,
        mlb.player_stats.strikeouts_pitching, mlb.player_stats.walks_allowed,
        mlb.player_stats.hits_allowed, mlb.player_stats.outs_recorded, mlb.player_stats.earned_runs,
        mlb.player_stats.is_starter
    ) IS DISTINCT FROM (
        EXCLUDED.game_date, EXCLUDED.team, EXCLUDED.opponent, EXCLUDED.is_home,
        EXCLUDED.position, EXCLUDED.plate_appearances, EXCLUDED.at_bats, EXCLUDED.hits,
        EXCLUDED.total_bases, EXCLUDED.rbis, EXCLUDED.runs_scored, EXCLUDED.strikeouts_batting,
        EXCLUDED.walks, EXCLUDED.singles, EXCLUDED.doubles, EXCLUDED.triples, EXCLUDED.home_runs,
        EXCLUDED.stolen_bases, EXCLUDED.strikeouts_pitching, EXCLUDED.walks_allowed,
        EXCLUDED.hits_allowed, EXCLUDED.outs_recorded, EXCLUDED.earned_runs, EXCLUDED.is_starter
    ) AND (
        mlb.player_stats.game_date IS NULL OR EXCLUDED.game_date IS NULL OR mlb.player_stats.game_date = EXCLUDED.game_date
    ) AND (
        mlb.player_stats.team IS NULL OR EXCLUDED.team IS NULL OR mlb.player_stats.team = EXCLUDED.team
    ) AND (
        mlb.player_stats.opponent IS NULL OR EXCLUDED.opponent IS NULL OR mlb.player_stats.opponent = EXCLUDED.opponent
    ) AND (
        mlb.player_stats.is_home IS NULL OR EXCLUDED.is_home IS NULL OR mlb.player_stats.is_home = EXCLUDED.is_home
    ) AND (
        mlb.player_stats.position IS NULL OR EXCLUDED.position IS NULL OR mlb.player_stats.position = EXCLUDED.position
    ) AND (
        mlb.player_stats.plate_appearances IS NULL OR EXCLUDED.plate_appearances IS NULL OR mlb.player_stats.plate_appearances = EXCLUDED.plate_appearances
    ) AND (
        mlb.player_stats.at_bats IS NULL OR EXCLUDED.at_bats IS NULL OR mlb.player_stats.at_bats = EXCLUDED.at_bats
    ) AND (
        mlb.player_stats.hits IS NULL OR EXCLUDED.hits IS NULL OR mlb.player_stats.hits = EXCLUDED.hits
    ) AND (
        mlb.player_stats.total_bases IS NULL OR EXCLUDED.total_bases IS NULL OR mlb.player_stats.total_bases = EXCLUDED.total_bases
    ) AND (
        mlb.player_stats.rbis IS NULL OR EXCLUDED.rbis IS NULL OR mlb.player_stats.rbis = EXCLUDED.rbis
    ) AND (
        mlb.player_stats.runs_scored IS NULL OR EXCLUDED.runs_scored IS NULL OR mlb.player_stats.runs_scored = EXCLUDED.runs_scored
    ) AND (
        mlb.player_stats.strikeouts_batting IS NULL OR EXCLUDED.strikeouts_batting IS NULL OR mlb.player_stats.strikeouts_batting = EXCLUDED.strikeouts_batting
    ) AND (
        mlb.player_stats.walks IS NULL OR EXCLUDED.walks IS NULL OR mlb.player_stats.walks = EXCLUDED.walks
    ) AND (
        mlb.player_stats.singles IS NULL OR EXCLUDED.singles IS NULL OR mlb.player_stats.singles = EXCLUDED.singles
    ) AND (
        mlb.player_stats.doubles IS NULL OR EXCLUDED.doubles IS NULL OR mlb.player_stats.doubles = EXCLUDED.doubles
    ) AND (
        mlb.player_stats.triples IS NULL OR EXCLUDED.triples IS NULL OR mlb.player_stats.triples = EXCLUDED.triples
    ) AND (
        mlb.player_stats.home_runs IS NULL OR EXCLUDED.home_runs IS NULL OR mlb.player_stats.home_runs = EXCLUDED.home_runs
    ) AND (
        mlb.player_stats.stolen_bases IS NULL OR EXCLUDED.stolen_bases IS NULL OR mlb.player_stats.stolen_bases = EXCLUDED.stolen_bases
    ) AND (
        mlb.player_stats.strikeouts_pitching IS NULL OR EXCLUDED.strikeouts_pitching IS NULL OR mlb.player_stats.strikeouts_pitching = EXCLUDED.strikeouts_pitching
    ) AND (
        mlb.player_stats.walks_allowed IS NULL OR EXCLUDED.walks_allowed IS NULL OR mlb.player_stats.walks_allowed = EXCLUDED.walks_allowed
    ) AND (
        mlb.player_stats.hits_allowed IS NULL OR EXCLUDED.hits_allowed IS NULL OR mlb.player_stats.hits_allowed = EXCLUDED.hits_allowed
    ) AND (
        mlb.player_stats.outs_recorded IS NULL OR EXCLUDED.outs_recorded IS NULL OR mlb.player_stats.outs_recorded = EXCLUDED.outs_recorded
    ) AND (
        mlb.player_stats.earned_runs IS NULL OR EXCLUDED.earned_runs IS NULL OR mlb.player_stats.earned_runs = EXCLUDED.earned_runs
    ) AND (
        mlb.player_stats.is_starter IS NULL OR EXCLUDED.is_starter IS NULL OR mlb.player_stats.is_starter = EXCLUDED.is_starter
    ) AND (
        mlb.player_stats.game_date IS NULL AND EXCLUDED.game_date IS NOT NULL OR
        mlb.player_stats.team IS NULL AND EXCLUDED.team IS NOT NULL OR
        mlb.player_stats.opponent IS NULL AND EXCLUDED.opponent IS NOT NULL OR
        mlb.player_stats.is_home IS NULL AND EXCLUDED.is_home IS NOT NULL OR
        mlb.player_stats.position IS NULL AND EXCLUDED.position IS NOT NULL OR
        mlb.player_stats.plate_appearances IS NULL AND EXCLUDED.plate_appearances IS NOT NULL OR
        mlb.player_stats.at_bats IS NULL AND EXCLUDED.at_bats IS NOT NULL OR
        mlb.player_stats.hits IS NULL AND EXCLUDED.hits IS NOT NULL OR
        mlb.player_stats.total_bases IS NULL AND EXCLUDED.total_bases IS NOT NULL OR
        mlb.player_stats.rbis IS NULL AND EXCLUDED.rbis IS NOT NULL OR
        mlb.player_stats.runs_scored IS NULL AND EXCLUDED.runs_scored IS NOT NULL OR
        mlb.player_stats.strikeouts_batting IS NULL AND EXCLUDED.strikeouts_batting IS NOT NULL OR
        mlb.player_stats.walks IS NULL AND EXCLUDED.walks IS NOT NULL OR
        mlb.player_stats.singles IS NULL AND EXCLUDED.singles IS NOT NULL OR
        mlb.player_stats.doubles IS NULL AND EXCLUDED.doubles IS NOT NULL OR
        mlb.player_stats.triples IS NULL AND EXCLUDED.triples IS NOT NULL OR
        mlb.player_stats.home_runs IS NULL AND EXCLUDED.home_runs IS NOT NULL OR
        mlb.player_stats.stolen_bases IS NULL AND EXCLUDED.stolen_bases IS NOT NULL OR
        mlb.player_stats.strikeouts_pitching IS NULL AND EXCLUDED.strikeouts_pitching IS NOT NULL OR
        mlb.player_stats.walks_allowed IS NULL AND EXCLUDED.walks_allowed IS NOT NULL OR
        mlb.player_stats.hits_allowed IS NULL AND EXCLUDED.hits_allowed IS NOT NULL OR
        mlb.player_stats.outs_recorded IS NULL AND EXCLUDED.outs_recorded IS NOT NULL OR
        mlb.player_stats.earned_runs IS NULL AND EXCLUDED.earned_runs IS NOT NULL OR
        mlb.player_stats.is_starter IS NULL AND EXCLUDED.is_starter IS NOT NULL
    );
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$function$;

-- Separately bounded, already-approved at-bats repair: batter rows only, and
-- only when the stored value is NULL/zero. It cannot replace a positive value.
CREATE OR REPLACE FUNCTION mlb.safe_fill_player_stats_at_bats(
    p_player_id bigint, p_game_id bigint, p_at_bats integer
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    n integer;
BEGIN
    IF p_player_id IS NULL OR p_game_id IS NULL OR p_at_bats IS NULL OR p_at_bats < 0 THEN
        RAISE EXCEPTION 'invalid at-bats repair input' USING ERRCODE = '22023';
    END IF;
    UPDATE mlb.player_stats
       SET at_bats = p_at_bats
     WHERE player_id = p_player_id
       AND game_id = p_game_id
       AND COALESCE(position, '') <> 'P'
       AND COALESCE(at_bats, 0) = 0;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$function$;

-- Training rows have an immutable result tuple. Only metadata may refresh on a
-- conflict, and only when every result field exactly matches the existing row.
CREATE OR REPLACE FUNCTION mlb.safe_upsert_model_training_props(p_row jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v mlb.model_training_props%ROWTYPE;
    n integer;
BEGIN
    IF p_row IS NULL OR jsonb_typeof(p_row) <> 'object'
       OR EXISTS (
           SELECT 1 FROM jsonb_object_keys(p_row) AS k(key)
           WHERE key <> ALL (ARRAY[
               'id','game_id','player_id','player_name','team','opponent','team_id',
               'opponent_team_id','opponent_encoded','is_home','prop_type','prop_value',
               'line','over_under','outcome','status','created_at','updated_at','prop_source',
               'was_correct','game_date','game_time','game_day_of_week','time_of_day_bucket',
               'streak_type','streak_count','game_type'
           ])
       ) THEN
        RAISE EXCEPTION 'invalid model_training_props payload' USING ERRCODE = '22023';
    END IF;
    v := jsonb_populate_record(NULL::mlb.model_training_props, p_row);
    IF v.player_id IS NULL OR v.game_id IS NULL OR v.prop_type IS NULL OR v.prop_source IS NULL THEN
        RAISE EXCEPTION 'model_training_props identity is required' USING ERRCODE = '22023';
    END IF;
    IF p_row ? 'game_type' THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute
            WHERE attrelid = 'mlb.model_training_props'::regclass
              AND attname = 'game_type' AND NOT attisdropped
        ) THEN
            RAISE EXCEPTION 'game_type payload supplied but column is unavailable' USING ERRCODE = '42703';
        END IF;
        EXECUTE $sql$
            INSERT INTO mlb.model_training_props (
                id, game_id, player_id, player_name, team, opponent, team_id,
                opponent_team_id, opponent_encoded, is_home, prop_type, prop_value,
                line, over_under, outcome, status, created_at, updated_at, prop_source,
                was_correct, game_date, game_time, game_day_of_week, time_of_day_bucket,
                streak_type, streak_count, game_type
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                $19,$20,$21,$22,$23,$24,$25,$26,$27
            ) ON CONFLICT (player_id,game_id,prop_type,prop_source) DO UPDATE SET
                team=EXCLUDED.team, opponent=EXCLUDED.opponent, team_id=EXCLUDED.team_id,
                opponent_team_id=EXCLUDED.opponent_team_id, opponent_encoded=EXCLUDED.opponent_encoded,
                is_home=EXCLUDED.is_home, updated_at=EXCLUDED.updated_at,
                game_date=EXCLUDED.game_date, game_time=EXCLUDED.game_time,
                game_day_of_week=EXCLUDED.game_day_of_week, time_of_day_bucket=EXCLUDED.time_of_day_bucket,
                streak_type=EXCLUDED.streak_type, streak_count=EXCLUDED.streak_count,
                game_type=EXCLUDED.game_type
            WHERE (mlb.model_training_props.prop_value,mlb.model_training_props.line,
                   mlb.model_training_props.over_under,mlb.model_training_props.outcome,
                   mlb.model_training_props.status,mlb.model_training_props.was_correct)
              IS NOT DISTINCT FROM
                  (EXCLUDED.prop_value,EXCLUDED.line,EXCLUDED.over_under,EXCLUDED.outcome,
                   EXCLUDED.status,EXCLUDED.was_correct)
              AND (mlb.model_training_props.game_date,mlb.model_training_props.game_time,
                   mlb.model_training_props.game_day_of_week,mlb.model_training_props.time_of_day_bucket,
                   mlb.model_training_props.streak_type,mlb.model_training_props.streak_count,
                   mlb.model_training_props.team,mlb.model_training_props.opponent,
                   mlb.model_training_props.team_id,mlb.model_training_props.opponent_team_id,
                   mlb.model_training_props.opponent_encoded,mlb.model_training_props.is_home,
                   mlb.model_training_props.game_type)
              IS DISTINCT FROM
                  (EXCLUDED.game_date,EXCLUDED.game_time,EXCLUDED.game_day_of_week,
                   EXCLUDED.time_of_day_bucket,EXCLUDED.streak_type,EXCLUDED.streak_count,
                   EXCLUDED.team,EXCLUDED.opponent,EXCLUDED.team_id,EXCLUDED.opponent_team_id,
                   EXCLUDED.opponent_encoded,EXCLUDED.is_home,EXCLUDED.game_type)
        $sql$
        USING v.id,v.game_id,v.player_id,v.player_name,v.team,v.opponent,v.team_id,
              v.opponent_team_id,v.opponent_encoded,v.is_home,v.prop_type,v.prop_value,v.line,
              v.over_under,v.outcome,v.status,v.created_at,v.updated_at,v.prop_source,
              v.was_correct,v.game_date,v.game_time,v.game_day_of_week,v.time_of_day_bucket,
              v.streak_type,v.streak_count,(p_row->>'game_type');
    ELSE
        INSERT INTO mlb.model_training_props (
            id, game_id, player_id, player_name, team, opponent, team_id,
            opponent_team_id, opponent_encoded, is_home, prop_type, prop_value,
            line, over_under, outcome, status, created_at, updated_at, prop_source,
            was_correct, game_date, game_time, game_day_of_week, time_of_day_bucket,
            streak_type, streak_count
        ) VALUES (
            v.id,v.game_id,v.player_id,v.player_name,v.team,v.opponent,v.team_id,
            v.opponent_team_id,v.opponent_encoded,v.is_home,v.prop_type,v.prop_value,
            v.line,v.over_under,v.outcome,v.status,v.created_at,v.updated_at,v.prop_source,
            v.was_correct,v.game_date,v.game_time,v.game_day_of_week,v.time_of_day_bucket,
            v.streak_type,v.streak_count
        ) ON CONFLICT (player_id,game_id,prop_type,prop_source) DO UPDATE SET
            team=EXCLUDED.team, opponent=EXCLUDED.opponent, team_id=EXCLUDED.team_id,
            opponent_team_id=EXCLUDED.opponent_team_id, opponent_encoded=EXCLUDED.opponent_encoded,
            is_home=EXCLUDED.is_home, updated_at=EXCLUDED.updated_at, game_date=EXCLUDED.game_date,
            game_time=EXCLUDED.game_time, game_day_of_week=EXCLUDED.game_day_of_week,
            time_of_day_bucket=EXCLUDED.time_of_day_bucket, streak_type=EXCLUDED.streak_type,
            streak_count=EXCLUDED.streak_count
        WHERE (mlb.model_training_props.prop_value,mlb.model_training_props.line,
               mlb.model_training_props.over_under,mlb.model_training_props.outcome,
               mlb.model_training_props.status,mlb.model_training_props.was_correct)
          IS NOT DISTINCT FROM
              (EXCLUDED.prop_value,EXCLUDED.line,EXCLUDED.over_under,EXCLUDED.outcome,
               EXCLUDED.status,EXCLUDED.was_correct)
          AND (mlb.model_training_props.game_date,mlb.model_training_props.game_time,
               mlb.model_training_props.game_day_of_week,mlb.model_training_props.time_of_day_bucket,
               mlb.model_training_props.streak_type,mlb.model_training_props.streak_count,
               mlb.model_training_props.team,mlb.model_training_props.opponent,
               mlb.model_training_props.team_id,mlb.model_training_props.opponent_team_id,
               mlb.model_training_props.opponent_encoded,mlb.model_training_props.is_home)
          IS DISTINCT FROM
              (EXCLUDED.game_date,EXCLUDED.game_time,EXCLUDED.game_day_of_week,
               EXCLUDED.time_of_day_bucket,EXCLUDED.streak_type,EXCLUDED.streak_count,
               EXCLUDED.team,EXCLUDED.opponent,EXCLUDED.team_id,EXCLUDED.opponent_team_id,
               EXCLUDED.opponent_encoded,EXCLUDED.is_home);
    END IF;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$function$;

REVOKE ALL ON FUNCTION mlb.safe_upsert_player_stats(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_upsert_model_training_props(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.safe_upsert_player_stats(jsonb) TO proppadia_mlb_ingest;
GRANT EXECUTE ON FUNCTION mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer) TO proppadia_mlb_ingest;
GRANT EXECUTE ON FUNCTION mlb.safe_upsert_model_training_props(jsonb) TO proppadia_mlb_ingest;

GRANT USAGE ON SCHEMA mlb TO proppadia_mlb_ingest;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mlb.player_stats FROM proppadia_mlb_ingest;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mlb.model_training_props FROM proppadia_mlb_ingest;
DO $column_acl$
DECLARE
    c record;
BEGIN
    FOR c IN
        SELECT n.nspname, r.relname, a.attname
        FROM pg_catalog.pg_class r
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
        WHERE n.nspname = 'mlb'
          AND r.relname IN ('player_stats', 'model_training_props')
          AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
        EXECUTE format('REVOKE INSERT (%I) ON %I.%I FROM proppadia_mlb_ingest', c.attname, c.nspname, c.relname);
        EXECUTE format('REVOKE UPDATE (%I) ON %I.%I FROM proppadia_mlb_ingest', c.attname, c.nspname, c.relname);
    END LOOP;
END
$column_acl$;
GRANT SELECT ON mlb.player_stats, mlb.model_training_props TO proppadia_mlb_ingest;
GRANT UPDATE (
    team, opponent, team_id, opponent_team_id, opponent_encoded, is_home,
    updated_at, game_date, game_time, game_day_of_week, time_of_day_bucket,
    streak_type, streak_count, rolling_result_avg_7
) ON mlb.model_training_props TO proppadia_mlb_ingest;
DO $optional_game_type$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute
        WHERE attrelid = 'mlb.model_training_props'::regclass
          AND attname = 'game_type' AND NOT attisdropped
    ) THEN
        GRANT UPDATE (game_type) ON mlb.model_training_props TO proppadia_mlb_ingest;
    END IF;
END
$optional_game_type$;

