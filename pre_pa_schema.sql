--
-- PostgreSQL database dump
--

\restrict rKxIbnTKlNMLVZjod7Bu8MAbWC8XOAzbLckTqpowtOXlTgOrfcEnrbypgBCGEhG

-- Dumped from database version 15.8
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: auth; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA auth;


ALTER SCHEMA auth OWNER TO supabase_admin;

--
-- Name: pg_cron; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION pg_cron; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_cron IS 'Job scheduler for PostgreSQL';


--
-- Name: extensions; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA extensions;


ALTER SCHEMA extensions OWNER TO postgres;

--
-- Name: graphql; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA graphql;


ALTER SCHEMA graphql OWNER TO supabase_admin;

--
-- Name: graphql_public; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA graphql_public;


ALTER SCHEMA graphql_public OWNER TO supabase_admin;

--
-- Name: mlb; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA mlb;


ALTER SCHEMA mlb OWNER TO postgres;

--
-- Name: nhl; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA nhl;


ALTER SCHEMA nhl OWNER TO postgres;

--
-- Name: pgbouncer; Type: SCHEMA; Schema: -; Owner: pgbouncer
--

CREATE SCHEMA pgbouncer;


ALTER SCHEMA pgbouncer OWNER TO pgbouncer;

--
-- Name: realtime; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA realtime;


ALTER SCHEMA realtime OWNER TO supabase_admin;

--
-- Name: storage; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA storage;


ALTER SCHEMA storage OWNER TO supabase_admin;

--
-- Name: vault; Type: SCHEMA; Schema: -; Owner: supabase_admin
--

CREATE SCHEMA vault;


ALTER SCHEMA vault OWNER TO supabase_admin;

--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA extensions;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_stat_statements IS 'track planning and execution statistics of all SQL statements executed';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: pgjwt; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgjwt WITH SCHEMA extensions;


--
-- Name: EXTENSION pgjwt; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgjwt IS 'JSON Web Token API for Postgresql';


--
-- Name: supabase_vault; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS supabase_vault WITH SCHEMA vault;


--
-- Name: EXTENSION supabase_vault; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION supabase_vault IS 'Supabase Vault Extension';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA extensions;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: aal_level; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.aal_level AS ENUM (
    'aal1',
    'aal2',
    'aal3'
);


ALTER TYPE auth.aal_level OWNER TO supabase_auth_admin;

--
-- Name: code_challenge_method; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.code_challenge_method AS ENUM (
    's256',
    'plain'
);


ALTER TYPE auth.code_challenge_method OWNER TO supabase_auth_admin;

--
-- Name: factor_status; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.factor_status AS ENUM (
    'unverified',
    'verified'
);


ALTER TYPE auth.factor_status OWNER TO supabase_auth_admin;

--
-- Name: factor_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.factor_type AS ENUM (
    'totp',
    'webauthn',
    'phone'
);


ALTER TYPE auth.factor_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_authorization_status; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_authorization_status AS ENUM (
    'pending',
    'approved',
    'denied',
    'expired'
);


ALTER TYPE auth.oauth_authorization_status OWNER TO supabase_auth_admin;

--
-- Name: oauth_client_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_client_type AS ENUM (
    'public',
    'confidential'
);


ALTER TYPE auth.oauth_client_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_registration_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_registration_type AS ENUM (
    'dynamic',
    'manual'
);


ALTER TYPE auth.oauth_registration_type OWNER TO supabase_auth_admin;

--
-- Name: oauth_response_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.oauth_response_type AS ENUM (
    'code'
);


ALTER TYPE auth.oauth_response_type OWNER TO supabase_auth_admin;

--
-- Name: one_time_token_type; Type: TYPE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TYPE auth.one_time_token_type AS ENUM (
    'confirmation_token',
    'reauthentication_token',
    'recovery_token',
    'email_change_token_new',
    'email_change_token_current',
    'phone_change_token'
);


ALTER TYPE auth.one_time_token_type OWNER TO supabase_auth_admin;

--
-- Name: action; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.action AS ENUM (
    'INSERT',
    'UPDATE',
    'DELETE',
    'TRUNCATE',
    'ERROR'
);


ALTER TYPE realtime.action OWNER TO supabase_admin;

--
-- Name: equality_op; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.equality_op AS ENUM (
    'eq',
    'neq',
    'lt',
    'lte',
    'gt',
    'gte',
    'in'
);


ALTER TYPE realtime.equality_op OWNER TO supabase_admin;

--
-- Name: user_defined_filter; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.user_defined_filter AS (
	column_name text,
	op realtime.equality_op,
	value text
);


ALTER TYPE realtime.user_defined_filter OWNER TO supabase_admin;

--
-- Name: wal_column; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.wal_column AS (
	name text,
	type_name text,
	type_oid oid,
	value jsonb,
	is_pkey boolean,
	is_selectable boolean
);


ALTER TYPE realtime.wal_column OWNER TO supabase_admin;

--
-- Name: wal_rls; Type: TYPE; Schema: realtime; Owner: supabase_admin
--

CREATE TYPE realtime.wal_rls AS (
	wal jsonb,
	is_rls_enabled boolean,
	subscription_ids uuid[],
	errors text[]
);


ALTER TYPE realtime.wal_rls OWNER TO supabase_admin;

--
-- Name: buckettype; Type: TYPE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TYPE storage.buckettype AS ENUM (
    'STANDARD',
    'ANALYTICS',
    'VECTOR'
);


ALTER TYPE storage.buckettype OWNER TO supabase_storage_admin;

--
-- Name: email(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.email() RETURNS text
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.email', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'email')
  )::text
$$;


ALTER FUNCTION auth.email() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION email(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.email() IS 'Deprecated. Use auth.jwt() -> ''email'' instead.';


--
-- Name: jwt(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.jwt() RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  select 
    coalesce(
        nullif(current_setting('request.jwt.claim', true), ''),
        nullif(current_setting('request.jwt.claims', true), '')
    )::jsonb
$$;


ALTER FUNCTION auth.jwt() OWNER TO supabase_auth_admin;

--
-- Name: role(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.role() RETURNS text
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.role', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'role')
  )::text
$$;


ALTER FUNCTION auth.role() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION role(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.role() IS 'Deprecated. Use auth.jwt() -> ''role'' instead.';


--
-- Name: uid(); Type: FUNCTION; Schema: auth; Owner: supabase_auth_admin
--

CREATE FUNCTION auth.uid() RETURNS uuid
    LANGUAGE sql STABLE
    AS $$
  select 
  coalesce(
    nullif(current_setting('request.jwt.claim.sub', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
  )::uuid
$$;


ALTER FUNCTION auth.uid() OWNER TO supabase_auth_admin;

--
-- Name: FUNCTION uid(); Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON FUNCTION auth.uid() IS 'Deprecated. Use auth.jwt() -> ''sub'' instead.';


--
-- Name: grant_pg_cron_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_cron_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF EXISTS (
    SELECT
    FROM pg_event_trigger_ddl_commands() AS ev
    JOIN pg_extension AS ext
    ON ev.objid = ext.oid
    WHERE ext.extname = 'pg_cron'
  )
  THEN
    grant usage on schema cron to postgres with grant option;

    alter default privileges in schema cron grant all on tables to postgres with grant option;
    alter default privileges in schema cron grant all on functions to postgres with grant option;
    alter default privileges in schema cron grant all on sequences to postgres with grant option;

    alter default privileges for user supabase_admin in schema cron grant all
        on sequences to postgres with grant option;
    alter default privileges for user supabase_admin in schema cron grant all
        on tables to postgres with grant option;
    alter default privileges for user supabase_admin in schema cron grant all
        on functions to postgres with grant option;

    grant all privileges on all tables in schema cron to postgres with grant option;
    revoke all on table cron.job from postgres;
    grant select on table cron.job to postgres with grant option;
  END IF;
END;
$$;


ALTER FUNCTION extensions.grant_pg_cron_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_cron_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_cron_access() IS 'Grants access to pg_cron';


--
-- Name: grant_pg_graphql_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_graphql_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    func_is_graphql_resolve bool;
BEGIN
    func_is_graphql_resolve = (
        SELECT n.proname = 'resolve'
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_proc AS n
        ON ev.objid = n.oid
    );

    IF func_is_graphql_resolve
    THEN
        -- Update public wrapper to pass all arguments through to the pg_graphql resolve func
        DROP FUNCTION IF EXISTS graphql_public.graphql;
        create or replace function graphql_public.graphql(
            "operationName" text default null,
            query text default null,
            variables jsonb default null,
            extensions jsonb default null
        )
            returns jsonb
            language sql
        as $$
            select graphql.resolve(
                query := query,
                variables := coalesce(variables, '{}'),
                "operationName" := "operationName",
                extensions := extensions
            );
        $$;

        -- This hook executes when `graphql.resolve` is created. That is not necessarily the last
        -- function in the extension so we need to grant permissions on existing entities AND
        -- update default permissions to any others that are created after `graphql.resolve`
        grant usage on schema graphql to postgres, anon, authenticated, service_role;
        grant select on all tables in schema graphql to postgres, anon, authenticated, service_role;
        grant execute on all functions in schema graphql to postgres, anon, authenticated, service_role;
        grant all on all sequences in schema graphql to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on tables to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on functions to postgres, anon, authenticated, service_role;
        alter default privileges in schema graphql grant all on sequences to postgres, anon, authenticated, service_role;

        -- Allow postgres role to allow granting usage on graphql and graphql_public schemas to custom roles
        grant usage on schema graphql_public to postgres with grant option;
        grant usage on schema graphql to postgres with grant option;
    END IF;

END;
$_$;


ALTER FUNCTION extensions.grant_pg_graphql_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_graphql_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_graphql_access() IS 'Grants access to pg_graphql';


--
-- Name: grant_pg_net_access(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.grant_pg_net_access() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_event_trigger_ddl_commands() AS ev
    JOIN pg_extension AS ext
    ON ev.objid = ext.oid
    WHERE ext.extname = 'pg_net'
  )
  THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_roles
      WHERE rolname = 'supabase_functions_admin'
    )
    THEN
      CREATE USER supabase_functions_admin NOINHERIT CREATEROLE LOGIN NOREPLICATION;
    END IF;

    GRANT USAGE ON SCHEMA net TO supabase_functions_admin, postgres, anon, authenticated, service_role;

    IF EXISTS (
      SELECT FROM pg_extension
      WHERE extname = 'pg_net'
      -- all versions in use on existing projects as of 2025-02-20
      -- version 0.12.0 onwards don't need these applied
      AND extversion IN ('0.2', '0.6', '0.7', '0.7.1', '0.8', '0.10.0', '0.11.0')
    ) THEN
      ALTER function net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) SECURITY DEFINER;
      ALTER function net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) SECURITY DEFINER;

      ALTER function net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) SET search_path = net;
      ALTER function net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) SET search_path = net;

      REVOKE ALL ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;
      REVOKE ALL ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) FROM PUBLIC;

      GRANT EXECUTE ON FUNCTION net.http_get(url text, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin, postgres, anon, authenticated, service_role;
      GRANT EXECUTE ON FUNCTION net.http_post(url text, body jsonb, params jsonb, headers jsonb, timeout_milliseconds integer) TO supabase_functions_admin, postgres, anon, authenticated, service_role;
    END IF;
  END IF;
END;
$$;


ALTER FUNCTION extensions.grant_pg_net_access() OWNER TO supabase_admin;

--
-- Name: FUNCTION grant_pg_net_access(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.grant_pg_net_access() IS 'Grants access to pg_net';


--
-- Name: pgrst_ddl_watch(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.pgrst_ddl_watch() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    IF cmd.command_tag IN (
      'CREATE SCHEMA', 'ALTER SCHEMA'
    , 'CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO', 'ALTER TABLE'
    , 'CREATE FOREIGN TABLE', 'ALTER FOREIGN TABLE'
    , 'CREATE VIEW', 'ALTER VIEW'
    , 'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW'
    , 'CREATE FUNCTION', 'ALTER FUNCTION'
    , 'CREATE TRIGGER'
    , 'CREATE TYPE', 'ALTER TYPE'
    , 'CREATE RULE'
    , 'COMMENT'
    )
    -- don't notify in case of CREATE TEMP table or other objects created on pg_temp
    AND cmd.schema_name is distinct from 'pg_temp'
    THEN
      NOTIFY pgrst, 'reload schema';
    END IF;
  END LOOP;
END; $$;


ALTER FUNCTION extensions.pgrst_ddl_watch() OWNER TO supabase_admin;

--
-- Name: pgrst_drop_watch(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.pgrst_drop_watch() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  obj record;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.object_type IN (
      'schema'
    , 'table'
    , 'foreign table'
    , 'view'
    , 'materialized view'
    , 'function'
    , 'trigger'
    , 'type'
    , 'rule'
    )
    AND obj.is_temporary IS false -- no pg_temp objects
    THEN
      NOTIFY pgrst, 'reload schema';
    END IF;
  END LOOP;
END; $$;


ALTER FUNCTION extensions.pgrst_drop_watch() OWNER TO supabase_admin;

--
-- Name: set_graphql_placeholder(); Type: FUNCTION; Schema: extensions; Owner: supabase_admin
--

CREATE FUNCTION extensions.set_graphql_placeholder() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $_$
    DECLARE
    graphql_is_dropped bool;
    BEGIN
    graphql_is_dropped = (
        SELECT ev.schema_name = 'graphql_public'
        FROM pg_event_trigger_dropped_objects() AS ev
        WHERE ev.schema_name = 'graphql_public'
    );

    IF graphql_is_dropped
    THEN
        create or replace function graphql_public.graphql(
            "operationName" text default null,
            query text default null,
            variables jsonb default null,
            extensions jsonb default null
        )
            returns jsonb
            language plpgsql
        as $$
            DECLARE
                server_version float;
            BEGIN
                server_version = (SELECT (SPLIT_PART((select version()), ' ', 2))::float);

                IF server_version >= 14 THEN
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql extension is not enabled.'
                            )
                        )
                    );
                ELSE
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql is only available on projects running Postgres 14 onwards.'
                            )
                        )
                    );
                END IF;
            END;
        $$;
    END IF;

    END;
$_$;


ALTER FUNCTION extensions.set_graphql_placeholder() OWNER TO supabase_admin;

--
-- Name: FUNCTION set_graphql_placeholder(); Type: COMMENT; Schema: extensions; Owner: supabase_admin
--

COMMENT ON FUNCTION extensions.set_graphql_placeholder() IS 'Reintroduces placeholder function for graphql_public.graphql';


--
-- Name: graphql(text, text, jsonb, jsonb); Type: FUNCTION; Schema: graphql_public; Owner: supabase_admin
--

CREATE FUNCTION graphql_public.graphql("operationName" text DEFAULT NULL::text, query text DEFAULT NULL::text, variables jsonb DEFAULT NULL::jsonb, extensions jsonb DEFAULT NULL::jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
            DECLARE
                server_version float;
            BEGIN
                server_version = (SELECT (SPLIT_PART((select version()), ' ', 2))::float);

                IF server_version >= 14 THEN
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql extension is not enabled.'
                            )
                        )
                    );
                ELSE
                    RETURN jsonb_build_object(
                        'errors', jsonb_build_array(
                            jsonb_build_object(
                                'message', 'pg_graphql is only available on projects running Postgres 14 onwards.'
                            )
                        )
                    );
                END IF;
            END;
        $$;


ALTER FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) OWNER TO supabase_admin;

--
-- Name: _safe_bigint(text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl._safe_bigint(t text) RETURNS bigint
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+\s*$' THEN t::bigint ELSE NULL END
$_$;


ALTER FUNCTION nhl._safe_bigint(t text) OWNER TO postgres;

--
-- Name: _safe_bool(text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl._safe_bool(t text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT CASE
    WHEN t IS NULL THEN NULL
    WHEN lower(trim(t)) IN ('1','t','true','y','yes') THEN true
    WHEN lower(trim(t)) IN ('0','f','false','n','no') THEN false
    ELSE NULL
  END
$$;


ALTER FUNCTION nhl._safe_bool(t text) OWNER TO postgres;

--
-- Name: _safe_int(text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl._safe_int(t text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+\s*$' THEN t::integer ELSE NULL END
$_$;


ALTER FUNCTION nhl._safe_int(t text) OWNER TO postgres;

--
-- Name: _safe_num(text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl._safe_num(t text) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+(\.\d+)?\s*$' THEN t::numeric ELSE NULL END
$_$;


ALTER FUNCTION nhl._safe_num(t text) OWNER TO postgres;

--
-- Name: audit_games_season_write(); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.audit_games_season_write() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO nhl.games_season_audit (
    op,
    game_id,
    game_date,
    season_old,
    season_new,
    application_name,
    client_addr,
    client_port
  )
  VALUES (
    TG_OP,
    NEW.game_id,
    NEW.game_date,
    CASE WHEN TG_OP = 'UPDATE' THEN OLD.season ELSE NULL END,
    NEW.season,
    current_setting('application_name', true),
    inet_client_addr(),
    inet_client_port()
  );

  RETURN NEW;
END;
$$;


ALTER FUNCTION nhl.audit_games_season_write() OWNER TO postgres;

--
-- Name: audit_games_write(); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.audit_games_write() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO nhl.games_write_audit (
    op, game_id,
    season_old, season_new,
    game_date_old, game_date_new,
    status_old, status_new,
    application_name, client_addr
  )
  VALUES (
    TG_OP, COALESCE(NEW.game_id, OLD.game_id),
    CASE WHEN TG_OP='UPDATE' THEN OLD.season ELSE NULL END,
    CASE WHEN TG_OP='DELETE' THEN NULL ELSE NEW.season END,
    CASE WHEN TG_OP='UPDATE' THEN OLD.game_date ELSE NULL END,
    CASE WHEN TG_OP='DELETE' THEN NULL ELSE NEW.game_date END,
    CASE WHEN TG_OP='UPDATE' THEN OLD.status ELSE NULL END,
    CASE WHEN TG_OP='DELETE' THEN NULL ELSE NEW.status END,
    current_setting('application_name', true),
    inet_client_addr()
  );

  RETURN COALESCE(NEW, OLD);
END;
$$;


ALTER FUNCTION nhl.audit_games_write() OWNER TO postgres;

--
-- Name: canonical_game_id(integer, integer); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.canonical_game_id(p_season integer, p_short_id integer) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
  v_type int := p_short_id / 10000;
  v_num  int := p_short_id % 10000;
BEGIN
  IF v_type NOT IN (1,2,3) THEN
    RAISE EXCEPTION 'Invalid game_type % from short_id %', v_type, p_short_id;
  END IF;

  IF v_num <= 0 OR v_num > 9999 THEN
    RAISE EXCEPTION 'Invalid game_number % from short_id %', v_num, p_short_id;
  END IF;

  RETURN (
    lpad(p_season::text, 4, '0') ||
    lpad(v_type::text,        2, '0') ||
    lpad(v_num::text,         4, '0')
  )::bigint;
END;
$$;


ALTER FUNCTION nhl.canonical_game_id(p_season integer, p_short_id integer) OWNER TO postgres;

--
-- Name: load_points_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.load_points_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text DEFAULT NULL::text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_rows INTEGER := 0;
BEGIN
  INSERT INTO nhl.predictions (
    player_id, game_id, prop, line, p_over,
    model_family, model_params, feature_hash, model_version, created_at, updated_at
  )
  SELECT
    l.player_id,
    l.game_id,
    'points'::TEXT AS prop,
    l.line,
    l.p_over,
    p_model_family,
    COALESCE(p_model_params, '{}'::jsonb),
    p_feature_hash,
    p_model_version,
    now(), now()
  FROM nhl.v_predictions_points_stage_long l
  WHERE l.p_over IS NOT NULL
  ON CONFLICT (prop, player_id, game_id, line, feature_hash)
  DO UPDATE SET
    p_over        = EXCLUDED.p_over,
    model_family  = EXCLUDED.model_family,
    model_params  = EXCLUDED.model_params,
    model_version = EXCLUDED.model_version,
    updated_at    = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;


ALTER FUNCTION nhl.load_points_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text) OWNER TO postgres;

--
-- Name: load_saves_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.load_saves_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text DEFAULT NULL::text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_rows INTEGER := 0;
BEGIN
  WITH src AS (
    SELECT
      l.player_id,
      l.game_id,
      l.line,
      l.p_over,
      ROW_NUMBER() OVER (
        PARTITION BY l.player_id, l.game_id, l.line
        ORDER BY l.p_over DESC NULLS LAST
      ) AS rn
    FROM nhl.v_predictions_saves_stage_long l
    WHERE l.p_over IS NOT NULL
  ),
  dedup AS (
    SELECT player_id, game_id, line, p_over
    FROM src
    WHERE rn = 1
  )
  INSERT INTO nhl.predictions (
    player_id, game_id, prop, line, p_over,
    model_family, model_params, feature_hash, model_version, created_at, updated_at
  )
  SELECT
    d.player_id,
    d.game_id,
    'goalie_saves'::TEXT AS prop,
    d.line,
    d.p_over,
    p_model_family,
    COALESCE(p_model_params, '{}'::jsonb),
    p_feature_hash,
    p_model_version,
    now(), now()
  FROM dedup d
  ON CONFLICT (prop, player_id, game_id, line, feature_hash)
  DO UPDATE SET
    p_over       = EXCLUDED.p_over,
    model_family = EXCLUDED.model_family,
    model_params = EXCLUDED.model_params,
    model_version= EXCLUDED.model_version,
    updated_at   = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;


ALTER FUNCTION nhl.load_saves_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text) OWNER TO postgres;

--
-- Name: load_sog_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.load_sog_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text DEFAULT NULL::text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_rows INTEGER := 0;
BEGIN
  INSERT INTO nhl.predictions (
    player_id, game_id, prop, line, p_over,
    model_family, model_params, feature_hash, model_version, created_at, updated_at
  )
  SELECT
    l.player_id,
    l.game_id,
    'shots_on_goal'::TEXT AS prop,
    l.line,
    l.p_over,
    p_model_family,
    COALESCE(p_model_params, '{}'::jsonb),
    p_feature_hash,
    p_model_version,
    now(), now()
  FROM nhl.v_predictions_sog_stage_long l
  WHERE l.p_over IS NOT NULL
  ON CONFLICT (prop, player_id, game_id, line, feature_hash)
  DO UPDATE SET
    p_over       = EXCLUDED.p_over,
    model_family = EXCLUDED.model_family,
    model_params = EXCLUDED.model_params,
    model_version= EXCLUDED.model_version,
    updated_at   = now();

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;


ALTER FUNCTION nhl.load_sog_predictions_from_stage(p_model_family text, p_model_params jsonb, p_feature_hash text, p_model_version text) OWNER TO postgres;

--
-- Name: log_games_season_change(); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.log_games_season_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  -- log inserts that have “weird” season values
  IF (TG_OP = 'INSERT') THEN
    IF NEW.season IS NOT NULL AND NEW.season NOT IN (2023, 2024, 2025) THEN
      INSERT INTO nhl.games_season_audit(action, game_id, game_date, old_season, new_season, application, client_addr, client_port)
      VALUES (
        'INSERT',
        NEW.game_id,
        NEW.game_date,
        NULL,
        NEW.season,
        current_setting('application_name', true),
        inet_client_addr(),
        inet_client_port()
      );
    END IF;
    RETURN NEW;
  END IF;

  -- log updates where season changes
  IF (TG_OP = 'UPDATE') THEN
    IF NEW.season IS DISTINCT FROM OLD.season THEN
      INSERT INTO nhl.games_season_audit(action, game_id, game_date, old_season, new_season, application, client_addr, client_port)
      VALUES (
        'UPDATE',
        NEW.game_id,
        NEW.game_date,
        OLD.season,
        NEW.season,
        current_setting('application_name', true),
        inet_client_addr(),
        inet_client_port()
      );
    END IF;
    RETURN NEW;
  END IF;

  RETURN NEW;
END;
$$;


ALTER FUNCTION nhl.log_games_season_change() OWNER TO postgres;

--
-- Name: set_updated_at(); Type: FUNCTION; Schema: nhl; Owner: postgres
--

CREATE FUNCTION nhl.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION nhl.set_updated_at() OWNER TO postgres;

--
-- Name: get_auth(text); Type: FUNCTION; Schema: pgbouncer; Owner: supabase_admin
--

CREATE FUNCTION pgbouncer.get_auth(p_usename text) RETURNS TABLE(username text, password text)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO ''
    AS $_$
  BEGIN
      RAISE DEBUG 'PgBouncer auth request: %', p_usename;

      RETURN QUERY
      SELECT
          rolname::text,
          CASE WHEN rolvaliduntil < now()
              THEN null
              ELSE rolpassword::text
          END
      FROM pg_authid
      WHERE rolname=$1 and rolcanlogin;
  END;
  $_$;


ALTER FUNCTION pgbouncer.get_auth(p_usename text) OWNER TO supabase_admin;

--
-- Name: batch_update_training_props(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.batch_update_training_props(rows jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
  with update_data as (
    select 
      (r ->> 'id')::uuid as id,
      (r ->> 'rolling_result_avg_7')::float as rolling_result_avg_7,
      (r ->> 'hit_streak')::int as hit_streak,
      (r ->> 'win_streak')::int as win_streak,
      (r ->> 'game_time')::timestamp as game_time,
      (r ->> 'is_home')::boolean as is_home,
      (r ->> 'opponent')::text as opponent,
      (r ->> 'prop_value')::float as prop_value,
      (r ->> 'over_under')::text as over_under
    from jsonb_array_elements(rows) as r
  )
  update model_training_props mtp
  set
    rolling_result_avg_7 = update_data.rolling_result_avg_7,
    hit_streak = update_data.hit_streak,
    win_streak = update_data.win_streak,
    game_time = update_data.game_time,
    is_home = update_data.is_home,
    opponent = update_data.opponent,
    prop_value = update_data.prop_value,
    over_under = update_data.over_under
  from update_data
  where mtp.id = update_data.id;
end;
$$;


ALTER FUNCTION public.batch_update_training_props(rows jsonb) OWNER TO postgres;

--
-- Name: bulk_update_training_rows(jsonb); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.bulk_update_training_rows(updates jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
  with update_data as (
    select 
      (u ->> 'id')::uuid as id,
      (u ->> 'rolling_result_avg_7')::float as rolling_result_avg_7,
      (u ->> 'hit_streak')::int as hit_streak,
      (u ->> 'win_streak')::int as win_streak,
      (u ->> 'game_time')::timestamp as game_time,
      (u ->> 'is_home')::boolean as is_home,
      (u ->> 'opponent')::text as opponent,
      (u ->> 'prop_value')::float as prop_value,
      (u ->> 'over_under')::text as over_under
    from jsonb_array_elements(updates) as u
  )
  update model_training_props mtp
  set
    rolling_result_avg_7 = update_data.rolling_result_avg_7,
    hit_streak = update_data.hit_streak,
    win_streak = update_data.win_streak,
    game_time = update_data.game_time,
    is_home = update_data.is_home,
    opponent = update_data.opponent,
    prop_value = update_data.prop_value,
    over_under = update_data.over_under
  from update_data
  where mtp.id = update_data.id;
end;
$$;


ALTER FUNCTION public.bulk_update_training_rows(updates jsonb) OWNER TO postgres;

--
-- Name: execute_raw_sql(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.execute_raw_sql(sql text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
declare
  result jsonb;
begin
  execute sql into result;
  return result;
end;
$$;


ALTER FUNCTION public.execute_raw_sql(sql text) OWNER TO postgres;

--
-- Name: fetch_bvp_game_ids(integer, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fetch_bvp_game_ids(offset_value integer, batch_size integer) RETURNS TABLE(game_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN QUERY
  SELECT sub.game_id
  FROM (
    SELECT mtp.game_id, MAX(mtp.game_date) AS max_game_date
    FROM model_training_props mtp
    WHERE mtp.prop_source = 'mlb_api'
    GROUP BY mtp.game_id
    ORDER BY max_game_date DESC
    OFFSET offset_value
    LIMIT batch_size
  ) sub;
END;
$$;


ALTER FUNCTION public.fetch_bvp_game_ids(offset_value integer, batch_size integer) OWNER TO postgres;

--
-- Name: get_daily_prop_accuracy(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_daily_prop_accuracy(target_date date) RETURNS TABLE(prop_type text, total integer, correct integer, accuracy_pct numeric)
    LANGUAGE sql
    AS $$
  select
    prop_type,
    count(*) as total,
    sum(case when predicted_outcome = outcome then 1 else 0 end) as correct,
    round(
      100.0 * sum(case when predicted_outcome = outcome then 1 else 0 end)::numeric / count(*),
      1
    ) as accuracy_pct
  from player_props
  where
    predicted_outcome is not null
    and outcome is not null
    and outcome != 'push'
    and game_date = target_date
  group by prop_type
  order by accuracy_pct desc;
$$;


ALTER FUNCTION public.get_daily_prop_accuracy(target_date date) OWNER TO postgres;

--
-- Name: get_model_accuracy_metrics(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_model_accuracy_metrics() RETURNS TABLE(prop_type text, total integer, correct integer, accuracy_pct numeric)
    LANGUAGE sql
    AS $$
  select
    prop_type,
    count(*) as total,
    sum(case when was_correct = true then 1 else 0 end) as correct,
    round(
      sum(case when was_correct = true then 1 else 0 end)::numeric / count(*) * 100, 1
    ) as accuracy_pct
  from model_training_props
  where predicted_outcome is not null and outcome is not null
  group by prop_type
  order by accuracy_pct desc;
$$;


ALTER FUNCTION public.get_model_accuracy_metrics() OWNER TO postgres;

--
-- Name: norm_player_ids_team_text(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.norm_player_ids_team_text() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.is_placeholder = FALSE AND NEW.team_id IS NOT NULL THEN
    NEW.team := NEW.team_id::text;
  END IF;
  RETURN NEW;
END; $$;


ALTER FUNCTION public.norm_player_ids_team_text() OWNER TO postgres;

--
-- Name: norm_team_ids_mtp(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.norm_team_ids_mtp() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
BEGIN
  IF NEW.team_id IS NULL THEN
    IF NEW.team IS NOT NULL AND NEW.team <> '' THEN
      IF NEW.team ~ '^[0-9]+$' THEN
        NEW.team_id := NEW.team::int;
      ELSE
        SELECT m.team_id INTO NEW.team_id
        FROM public.mlb_team_map m
        WHERE m.abbr = UPPER(TRIM(NEW.team));
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END; $_$;


ALTER FUNCTION public.norm_team_ids_mtp() OWNER TO postgres;

--
-- Name: norm_team_ids_ps(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.norm_team_ids_ps() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE tid text;
BEGIN
  -- team
  IF NEW.team IS NOT NULL AND NEW.team <> '' THEN
    IF NEW.team ~ '^[0-9]+$' THEN
      -- already numeric
    ELSIF UPPER(TRIM(NEW.team)) IN ('AL','NL') THEN
      NEW.team := NULL;
    ELSE
      SELECT m.team_id::text INTO tid FROM public.mlb_team_map m
      WHERE m.abbr = UPPER(TRIM(NEW.team));
      IF tid IS NOT NULL THEN NEW.team := tid; END IF;
    END IF;
  END IF;

  -- opponent
  IF NEW.opponent IS NOT NULL AND NEW.opponent <> '' THEN
    IF NEW.opponent ~ '^[0-9]+$' THEN
      -- already numeric
    ELSIF UPPER(TRIM(NEW.opponent)) IN ('AL','NL') THEN
      NEW.opponent := NULL;
    ELSE
      SELECT m.team_id::text INTO tid FROM public.mlb_team_map m
      WHERE m.abbr = UPPER(TRIM(NEW.opponent));
      IF tid IS NOT NULL THEN NEW.opponent := tid; END IF;
    END IF;
  END IF;

  RETURN NEW;
END; $_$;


ALTER FUNCTION public.norm_team_ids_ps() OWNER TO postgres;

--
-- Name: resolve_team_context(bigint, bigint); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.resolve_team_context(p_player_id bigint, p_game_id bigint) RETURNS TABLE(team_id bigint, team text, opponent text)
    LANGUAGE sql STABLE
    AS $$
WITH base AS (
  SELECT p_player_id AS player_id, p_game_id AS game_id
)
SELECT
  COALESCE(ptbg.team_id, mtp.team_id) AS team_id,
  ps.team, ps.opponent
FROM base b
LEFT JOIN public.player_stats ps
  ON ps.player_id = b.player_id AND ps.game_id = b.game_id
LEFT JOIN public.player_team_by_game ptbg
  ON ptbg.player_id = b.player_id AND ptbg.game_id = b.game_id
  AND ptbg.team_id IS NOT NULL
LEFT JOIN LATERAL (
  SELECT m.team_id
  FROM public.model_training_props m
  WHERE m.player_id = b.player_id AND m.game_id = b.game_id AND m.team_id IS NOT NULL
  ORDER BY m.created_at DESC
  LIMIT 1
) mtp ON TRUE
LIMIT 1;
$$;


ALTER FUNCTION public.resolve_team_context(p_player_id bigint, p_game_id bigint) OWNER TO postgres;

--
-- Name: set_is_starter(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.set_is_starter() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
  team_int   int;
  starter_id bigint;
BEGIN
  -- Normalize incoming flag to {0,1,NULL}; keep it as fallback when mapping is unavailable.
  IF NEW.is_starter IS NOT NULL AND NEW.is_starter NOT IN (0, 1) THEN
    NEW.is_starter := NULL;
  END IF;

  -- If team is missing, retain incoming value.
  IF NEW.team IS NULL OR NEW.team = '' THEN
    RETURN NEW;
  END IF;

  -- Normalize team to numeric id.
  IF NEW.team ~ '^[0-9]+$' THEN
    team_int := NEW.team::int;
  ELSE
    SELECT m.team_id INTO team_int
    FROM public.mlb_team_map m
    WHERE m.abbr = UPPER(TRIM(NEW.team));
  END IF;

  -- If team cannot be resolved, retain incoming value.
  IF team_int IS NULL THEN
    RETURN NEW;
  END IF;

  -- Fetch authoritative starter for this (game, team) when available.
  SELECT g.starter_pitcher_id
  INTO starter_id
  FROM public.opp_starter_per_game g
  WHERE g.game_id::bigint = NEW.game_id::bigint
    AND COALESCE(
          CASE WHEN g.team ~ '^[0-9]+$' THEN g.team::int END,
          (SELECT m2.team_id FROM public.mlb_team_map m2 WHERE m2.abbr = UPPER(TRIM(g.team)))
        ) = team_int
  LIMIT 1;

  -- If mapping is missing, retain ingest-provided value.
  IF starter_id IS NULL THEN
    RETURN NEW;
  END IF;

  -- Authoritative override when starter mapping exists.
  NEW.is_starter := CASE WHEN NEW.player_id = starter_id THEN 1 ELSE 0 END;
  RETURN NEW;
END;
$_$;


ALTER FUNCTION public.set_is_starter() OWNER TO postgres;

--
-- Name: update_timestamp(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_timestamp() OWNER TO postgres;

--
-- Name: apply_rls(jsonb, integer); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer DEFAULT (1024 * 1024)) RETURNS SETOF realtime.wal_rls
    LANGUAGE plpgsql
    AS $$
declare
-- Regclass of the table e.g. public.notes
entity_ regclass = (quote_ident(wal ->> 'schema') || '.' || quote_ident(wal ->> 'table'))::regclass;

-- I, U, D, T: insert, update ...
action realtime.action = (
    case wal ->> 'action'
        when 'I' then 'INSERT'
        when 'U' then 'UPDATE'
        when 'D' then 'DELETE'
        else 'ERROR'
    end
);

-- Is row level security enabled for the table
is_rls_enabled bool = relrowsecurity from pg_class where oid = entity_;

subscriptions realtime.subscription[] = array_agg(subs)
    from
        realtime.subscription subs
    where
        subs.entity = entity_
        -- Filter by action early - only get subscriptions interested in this action
        -- action_filter column can be: '*' (all), 'INSERT', 'UPDATE', or 'DELETE'
        and (subs.action_filter = '*' or subs.action_filter = action::text);

-- Subscription vars
roles regrole[] = array_agg(distinct us.claims_role::text)
    from
        unnest(subscriptions) us;

working_role regrole;
claimed_role regrole;
claims jsonb;

subscription_id uuid;
subscription_has_access bool;
visible_to_subscription_ids uuid[] = '{}';

-- structured info for wal's columns
columns realtime.wal_column[];
-- previous identity values for update/delete
old_columns realtime.wal_column[];

error_record_exceeds_max_size boolean = octet_length(wal::text) > max_record_bytes;

-- Primary jsonb output for record
output jsonb;

begin
perform set_config('role', null, true);

columns =
    array_agg(
        (
            x->>'name',
            x->>'type',
            x->>'typeoid',
            realtime.cast(
                (x->'value') #>> '{}',
                coalesce(
                    (x->>'typeoid')::regtype, -- null when wal2json version <= 2.4
                    (x->>'type')::regtype
                )
            ),
            (pks ->> 'name') is not null,
            true
        )::realtime.wal_column
    )
    from
        jsonb_array_elements(wal -> 'columns') x
        left join jsonb_array_elements(wal -> 'pk') pks
            on (x ->> 'name') = (pks ->> 'name');

old_columns =
    array_agg(
        (
            x->>'name',
            x->>'type',
            x->>'typeoid',
            realtime.cast(
                (x->'value') #>> '{}',
                coalesce(
                    (x->>'typeoid')::regtype, -- null when wal2json version <= 2.4
                    (x->>'type')::regtype
                )
            ),
            (pks ->> 'name') is not null,
            true
        )::realtime.wal_column
    )
    from
        jsonb_array_elements(wal -> 'identity') x
        left join jsonb_array_elements(wal -> 'pk') pks
            on (x ->> 'name') = (pks ->> 'name');

for working_role in select * from unnest(roles) loop

    -- Update `is_selectable` for columns and old_columns
    columns =
        array_agg(
            (
                c.name,
                c.type_name,
                c.type_oid,
                c.value,
                c.is_pkey,
                pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
            )::realtime.wal_column
        )
        from
            unnest(columns) c;

    old_columns =
            array_agg(
                (
                    c.name,
                    c.type_name,
                    c.type_oid,
                    c.value,
                    c.is_pkey,
                    pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
                )::realtime.wal_column
            )
            from
                unnest(old_columns) c;

    if action <> 'DELETE' and count(1) = 0 from unnest(columns) c where c.is_pkey then
        return next (
            jsonb_build_object(
                'schema', wal ->> 'schema',
                'table', wal ->> 'table',
                'type', action
            ),
            is_rls_enabled,
            -- subscriptions is already filtered by entity
            (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
            array['Error 400: Bad Request, no primary key']
        )::realtime.wal_rls;

    -- The claims role does not have SELECT permission to the primary key of entity
    elsif action <> 'DELETE' and sum(c.is_selectable::int) <> count(1) from unnest(columns) c where c.is_pkey then
        return next (
            jsonb_build_object(
                'schema', wal ->> 'schema',
                'table', wal ->> 'table',
                'type', action
            ),
            is_rls_enabled,
            (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
            array['Error 401: Unauthorized']
        )::realtime.wal_rls;

    else
        output = jsonb_build_object(
            'schema', wal ->> 'schema',
            'table', wal ->> 'table',
            'type', action,
            'commit_timestamp', to_char(
                ((wal ->> 'timestamp')::timestamptz at time zone 'utc'),
                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
            ),
            'columns', (
                select
                    jsonb_agg(
                        jsonb_build_object(
                            'name', pa.attname,
                            'type', pt.typname
                        )
                        order by pa.attnum asc
                    )
                from
                    pg_attribute pa
                    join pg_type pt
                        on pa.atttypid = pt.oid
                where
                    attrelid = entity_
                    and attnum > 0
                    and pg_catalog.has_column_privilege(working_role, entity_, pa.attname, 'SELECT')
            )
        )
        -- Add "record" key for insert and update
        || case
            when action in ('INSERT', 'UPDATE') then
                jsonb_build_object(
                    'record',
                    (
                        select
                            jsonb_object_agg(
                                -- if unchanged toast, get column name and value from old record
                                coalesce((c).name, (oc).name),
                                case
                                    when (c).name is null then (oc).value
                                    else (c).value
                                end
                            )
                        from
                            unnest(columns) c
                            full outer join unnest(old_columns) oc
                                on (c).name = (oc).name
                        where
                            coalesce((c).is_selectable, (oc).is_selectable)
                            and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                    )
                )
            else '{}'::jsonb
        end
        -- Add "old_record" key for update and delete
        || case
            when action = 'UPDATE' then
                jsonb_build_object(
                        'old_record',
                        (
                            select jsonb_object_agg((c).name, (c).value)
                            from unnest(old_columns) c
                            where
                                (c).is_selectable
                                and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                        )
                    )
            when action = 'DELETE' then
                jsonb_build_object(
                    'old_record',
                    (
                        select jsonb_object_agg((c).name, (c).value)
                        from unnest(old_columns) c
                        where
                            (c).is_selectable
                            and ( not error_record_exceeds_max_size or (octet_length((c).value::text) <= 64))
                            and ( not is_rls_enabled or (c).is_pkey ) -- if RLS enabled, we can't secure deletes so filter to pkey
                    )
                )
            else '{}'::jsonb
        end;

        -- Create the prepared statement
        if is_rls_enabled and action <> 'DELETE' then
            if (select 1 from pg_prepared_statements where name = 'walrus_rls_stmt' limit 1) > 0 then
                deallocate walrus_rls_stmt;
            end if;
            execute realtime.build_prepared_statement_sql('walrus_rls_stmt', entity_, columns);
        end if;

        visible_to_subscription_ids = '{}';

        for subscription_id, claims in (
                select
                    subs.subscription_id,
                    subs.claims
                from
                    unnest(subscriptions) subs
                where
                    subs.entity = entity_
                    and subs.claims_role = working_role
                    and (
                        realtime.is_visible_through_filters(columns, subs.filters)
                        or (
                          action = 'DELETE'
                          and realtime.is_visible_through_filters(old_columns, subs.filters)
                        )
                    )
        ) loop

            if not is_rls_enabled or action = 'DELETE' then
                visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
            else
                -- Check if RLS allows the role to see the record
                perform
                    -- Trim leading and trailing quotes from working_role because set_config
                    -- doesn't recognize the role as valid if they are included
                    set_config('role', trim(both '"' from working_role::text), true),
                    set_config('request.jwt.claims', claims::text, true);

                execute 'execute walrus_rls_stmt' into subscription_has_access;

                if subscription_has_access then
                    visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
                end if;
            end if;
        end loop;

        perform set_config('role', null, true);

        return next (
            output,
            is_rls_enabled,
            visible_to_subscription_ids,
            case
                when error_record_exceeds_max_size then array['Error 413: Payload Too Large']
                else '{}'
            end
        )::realtime.wal_rls;

    end if;
end loop;

perform set_config('role', null, true);
end;
$$;


ALTER FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) OWNER TO supabase_admin;

--
-- Name: broadcast_changes(text, text, text, text, text, record, record, text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text DEFAULT 'ROW'::text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Declare a variable to hold the JSONB representation of the row
    row_data jsonb := '{}'::jsonb;
BEGIN
    IF level = 'STATEMENT' THEN
        RAISE EXCEPTION 'function can only be triggered for each row, not for each statement';
    END IF;
    -- Check the operation type and handle accordingly
    IF operation = 'INSERT' OR operation = 'UPDATE' OR operation = 'DELETE' THEN
        row_data := jsonb_build_object('old_record', OLD, 'record', NEW, 'operation', operation, 'table', table_name, 'schema', table_schema);
        PERFORM realtime.send (row_data, event_name, topic_name);
    ELSE
        RAISE EXCEPTION 'Unexpected operation type: %', operation;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Failed to process the row: %', SQLERRM;
END;

$$;


ALTER FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) OWNER TO supabase_admin;

--
-- Name: build_prepared_statement_sql(text, regclass, realtime.wal_column[]); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) RETURNS text
    LANGUAGE sql
    AS $$
      /*
      Builds a sql string that, if executed, creates a prepared statement to
      tests retrive a row from *entity* by its primary key columns.
      Example
          select realtime.build_prepared_statement_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
      */
          select
      'prepare ' || prepared_statement_name || ' as
          select
              exists(
                  select
                      1
                  from
                      ' || entity || '
                  where
                      ' || string_agg(quote_ident(pkc.name) || '=' || quote_nullable(pkc.value #>> '{}') , ' and ') || '
              )'
          from
              unnest(columns) pkc
          where
              pkc.is_pkey
          group by
              entity
      $$;


ALTER FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) OWNER TO supabase_admin;

--
-- Name: cast(text, regtype); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime."cast"(val text, type_ regtype) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    AS $$
declare
  res jsonb;
begin
  if type_::text = 'bytea' then
    return to_jsonb(val);
  end if;
  execute format('select to_jsonb(%L::'|| type_::text || ')', val) into res;
  return res;
end
$$;


ALTER FUNCTION realtime."cast"(val text, type_ regtype) OWNER TO supabase_admin;

--
-- Name: check_equality_op(realtime.equality_op, regtype, text, text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$
      /*
      Casts *val_1* and *val_2* as type *type_* and check the *op* condition for truthiness
      */
      declare
          op_symbol text = (
              case
                  when op = 'eq' then '='
                  when op = 'neq' then '!='
                  when op = 'lt' then '<'
                  when op = 'lte' then '<='
                  when op = 'gt' then '>'
                  when op = 'gte' then '>='
                  when op = 'in' then '= any'
                  else 'UNKNOWN OP'
              end
          );
          res boolean;
      begin
          execute format(
              'select %L::'|| type_::text || ' ' || op_symbol
              || ' ( %L::'
              || (
                  case
                      when op = 'in' then type_::text || '[]'
                      else type_::text end
              )
              || ')', val_1, val_2) into res;
          return res;
      end;
      $$;


ALTER FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) OWNER TO supabase_admin;

--
-- Name: is_visible_through_filters(realtime.wal_column[], realtime.user_defined_filter[]); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    /*
    Should the record be visible (true) or filtered out (false) after *filters* are applied
    */
        select
            -- Default to allowed when no filters present
            $2 is null -- no filters. this should not happen because subscriptions has a default
            or array_length($2, 1) is null -- array length of an empty array is null
            or bool_and(
                coalesce(
                    realtime.check_equality_op(
                        op:=f.op,
                        type_:=coalesce(
                            col.type_oid::regtype, -- null when wal2json version <= 2.4
                            col.type_name::regtype
                        ),
                        -- cast jsonb to text
                        val_1:=col.value #>> '{}',
                        val_2:=f.value
                    ),
                    false -- if null, filter does not match
                )
            )
        from
            unnest(filters) f
            join unnest(columns) col
                on f.column_name = col.name;
    $_$;


ALTER FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) OWNER TO supabase_admin;

--
-- Name: list_changes(name, name, integer, integer); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) RETURNS TABLE(wal jsonb, is_rls_enabled boolean, subscription_ids uuid[], errors text[], slot_changes_count bigint)
    LANGUAGE sql
    SET log_min_messages TO 'fatal'
    AS $$
  WITH pub AS (
    SELECT
      concat_ws(
        ',',
        CASE WHEN bool_or(pubinsert) THEN 'insert' ELSE NULL END,
        CASE WHEN bool_or(pubupdate) THEN 'update' ELSE NULL END,
        CASE WHEN bool_or(pubdelete) THEN 'delete' ELSE NULL END
      ) AS w2j_actions,
      coalesce(
        string_agg(
          realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass),
          ','
        ) filter (WHERE ppt.tablename IS NOT NULL AND ppt.tablename NOT LIKE '% %'),
        ''
      ) AS w2j_add_tables
    FROM pg_publication pp
    LEFT JOIN pg_publication_tables ppt ON pp.pubname = ppt.pubname
    WHERE pp.pubname = publication
    GROUP BY pp.pubname
    LIMIT 1
  ),
  -- MATERIALIZED ensures pg_logical_slot_get_changes is called exactly once
  w2j AS MATERIALIZED (
    SELECT x.*, pub.w2j_add_tables
    FROM pub,
         pg_logical_slot_get_changes(
           slot_name, null, max_changes,
           'include-pk', 'true',
           'include-transaction', 'false',
           'include-timestamp', 'true',
           'include-type-oids', 'true',
           'format-version', '2',
           'actions', pub.w2j_actions,
           'add-tables', pub.w2j_add_tables
         ) x
  ),
  -- Count raw slot entries before apply_rls/subscription filter
  slot_count AS (
    SELECT count(*)::bigint AS cnt
    FROM w2j
    WHERE w2j.w2j_add_tables <> ''
  ),
  -- Apply RLS and filter as before
  rls_filtered AS (
    SELECT xyz.wal, xyz.is_rls_enabled, xyz.subscription_ids, xyz.errors
    FROM w2j,
         realtime.apply_rls(
           wal := w2j.data::jsonb,
           max_record_bytes := max_record_bytes
         ) xyz(wal, is_rls_enabled, subscription_ids, errors)
    WHERE w2j.w2j_add_tables <> ''
      AND xyz.subscription_ids[1] IS NOT NULL
  )
  -- Real rows with slot count attached
  SELECT rf.wal, rf.is_rls_enabled, rf.subscription_ids, rf.errors, sc.cnt
  FROM rls_filtered rf, slot_count sc

  UNION ALL

  -- Sentinel row: always returned when no real rows exist so Elixir can
  -- always read slot_changes_count. Identified by wal IS NULL.
  SELECT null, null, null, null, sc.cnt
  FROM slot_count sc
  WHERE NOT EXISTS (SELECT 1 FROM rls_filtered)
$$;


ALTER FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) OWNER TO supabase_admin;

--
-- Name: quote_wal2json(regclass); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.quote_wal2json(entity regclass) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
      select
        (
          select string_agg('' || ch,'')
          from unnest(string_to_array(nsp.nspname::text, null)) with ordinality x(ch, idx)
          where
            not (x.idx = 1 and x.ch = '"')
            and not (
              x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
              and x.ch = '"'
            )
        )
        || '.'
        || (
          select string_agg('' || ch,'')
          from unnest(string_to_array(pc.relname::text, null)) with ordinality x(ch, idx)
          where
            not (x.idx = 1 and x.ch = '"')
            and not (
              x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
              and x.ch = '"'
            )
          )
      from
        pg_class pc
        join pg_namespace nsp
          on pc.relnamespace = nsp.oid
      where
        pc.oid = entity
    $$;


ALTER FUNCTION realtime.quote_wal2json(entity regclass) OWNER TO supabase_admin;

--
-- Name: send(jsonb, text, text, boolean); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
  generated_id uuid;
  final_payload jsonb;
BEGIN
  BEGIN
    -- Generate a new UUID for the id
    generated_id := gen_random_uuid();

    -- Check if payload has an 'id' key, if not, add the generated UUID
    IF payload ? 'id' THEN
      final_payload := payload;
    ELSE
      final_payload := jsonb_set(payload, '{id}', to_jsonb(generated_id));
    END IF;

    -- Set the topic configuration
    EXECUTE format('SET LOCAL realtime.topic TO %L', topic);

    -- Attempt to insert the message
    INSERT INTO realtime.messages (id, payload, event, topic, private, extension)
    VALUES (generated_id, final_payload, event, topic, private, 'broadcast');
  EXCEPTION
    WHEN OTHERS THEN
      -- Capture and notify the error
      RAISE WARNING 'ErrorSendingBroadcastMessage: %', SQLERRM;
  END;
END;
$$;


ALTER FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) OWNER TO supabase_admin;

--
-- Name: subscription_check_filters(); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.subscription_check_filters() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    /*
    Validates that the user defined filters for a subscription:
    - refer to valid columns that the claimed role may access
    - values are coercable to the correct column type
    */
    declare
        col_names text[] = coalesce(
                array_agg(c.column_name order by c.ordinal_position),
                '{}'::text[]
            )
            from
                information_schema.columns c
            where
                format('%I.%I', c.table_schema, c.table_name)::regclass = new.entity
                and pg_catalog.has_column_privilege(
                    (new.claims ->> 'role'),
                    format('%I.%I', c.table_schema, c.table_name)::regclass,
                    c.column_name,
                    'SELECT'
                );
        filter realtime.user_defined_filter;
        col_type regtype;

        in_val jsonb;
    begin
        for filter in select * from unnest(new.filters) loop
            -- Filtered column is valid
            if not filter.column_name = any(col_names) then
                raise exception 'invalid column for filter %', filter.column_name;
            end if;

            -- Type is sanitized and safe for string interpolation
            col_type = (
                select atttypid::regtype
                from pg_catalog.pg_attribute
                where attrelid = new.entity
                      and attname = filter.column_name
            );
            if col_type is null then
                raise exception 'failed to lookup type for column %', filter.column_name;
            end if;

            -- Set maximum number of entries for in filter
            if filter.op = 'in'::realtime.equality_op then
                in_val = realtime.cast(filter.value, (col_type::text || '[]')::regtype);
                if coalesce(jsonb_array_length(in_val), 0) > 100 then
                    raise exception 'too many values for `in` filter. Maximum 100';
                end if;
            else
                -- raises an exception if value is not coercable to type
                perform realtime.cast(filter.value, col_type);
            end if;

        end loop;

        -- Apply consistent order to filters so the unique constraint on
        -- (subscription_id, entity, filters) can't be tricked by a different filter order
        new.filters = coalesce(
            array_agg(f order by f.column_name, f.op, f.value),
            '{}'
        ) from unnest(new.filters) f;

        return new;
    end;
    $$;


ALTER FUNCTION realtime.subscription_check_filters() OWNER TO supabase_admin;

--
-- Name: to_regrole(text); Type: FUNCTION; Schema: realtime; Owner: supabase_admin
--

CREATE FUNCTION realtime.to_regrole(role_name text) RETURNS regrole
    LANGUAGE sql IMMUTABLE
    AS $$ select role_name::regrole $$;


ALTER FUNCTION realtime.to_regrole(role_name text) OWNER TO supabase_admin;

--
-- Name: topic(); Type: FUNCTION; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE FUNCTION realtime.topic() RETURNS text
    LANGUAGE sql STABLE
    AS $$
select nullif(current_setting('realtime.topic', true), '')::text;
$$;


ALTER FUNCTION realtime.topic() OWNER TO supabase_realtime_admin;

--
-- Name: allow_any_operation(text[]); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.allow_any_operation(expected_operations text[]) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
  WITH current_operation AS (
    SELECT storage.operation() AS raw_operation
  ),
  normalized AS (
    SELECT CASE
      WHEN raw_operation LIKE 'storage.%' THEN substr(raw_operation, 9)
      ELSE raw_operation
    END AS current_operation
    FROM current_operation
  )
  SELECT EXISTS (
    SELECT 1
    FROM normalized n
    CROSS JOIN LATERAL unnest(expected_operations) AS expected_operation
    WHERE expected_operation IS NOT NULL
      AND expected_operation <> ''
      AND n.current_operation = CASE
        WHEN expected_operation LIKE 'storage.%' THEN substr(expected_operation, 9)
        ELSE expected_operation
      END
  );
$$;


ALTER FUNCTION storage.allow_any_operation(expected_operations text[]) OWNER TO supabase_storage_admin;

--
-- Name: allow_only_operation(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.allow_only_operation(expected_operation text) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
  WITH current_operation AS (
    SELECT storage.operation() AS raw_operation
  ),
  normalized AS (
    SELECT
      CASE
        WHEN raw_operation LIKE 'storage.%' THEN substr(raw_operation, 9)
        ELSE raw_operation
      END AS current_operation,
      CASE
        WHEN expected_operation LIKE 'storage.%' THEN substr(expected_operation, 9)
        ELSE expected_operation
      END AS requested_operation
    FROM current_operation
  )
  SELECT CASE
    WHEN requested_operation IS NULL OR requested_operation = '' THEN FALSE
    ELSE COALESCE(current_operation = requested_operation, FALSE)
  END
  FROM normalized;
$$;


ALTER FUNCTION storage.allow_only_operation(expected_operation text) OWNER TO supabase_storage_admin;

--
-- Name: can_insert_object(text, text, uuid, jsonb); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.can_insert_object(bucketid text, name text, owner uuid, metadata jsonb) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  INSERT INTO "storage"."objects" ("bucket_id", "name", "owner", "metadata") VALUES (bucketid, name, owner, metadata);
  -- hack to rollback the successful insert
  RAISE sqlstate 'PT200' using
  message = 'ROLLBACK',
  detail = 'rollback successful insert';
END
$$;


ALTER FUNCTION storage.can_insert_object(bucketid text, name text, owner uuid, metadata jsonb) OWNER TO supabase_storage_admin;

--
-- Name: delete_leaf_prefixes(text[], text[]); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.delete_leaf_prefixes(bucket_ids text[], names text[]) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_rows_deleted integer;
BEGIN
    LOOP
        WITH candidates AS (
            SELECT DISTINCT
                t.bucket_id,
                unnest(storage.get_prefixes(t.name)) AS name
            FROM unnest(bucket_ids, names) AS t(bucket_id, name)
        ),
        uniq AS (
             SELECT
                 bucket_id,
                 name,
                 storage.get_level(name) AS level
             FROM candidates
             WHERE name <> ''
             GROUP BY bucket_id, name
        ),
        leaf AS (
             SELECT
                 p.bucket_id,
                 p.name,
                 p.level
             FROM storage.prefixes AS p
                  JOIN uniq AS u
                       ON u.bucket_id = p.bucket_id
                           AND u.name = p.name
                           AND u.level = p.level
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM storage.objects AS o
                 WHERE o.bucket_id = p.bucket_id
                   AND o.level = p.level + 1
                   AND o.name COLLATE "C" LIKE p.name || '/%'
             )
             AND NOT EXISTS (
                 SELECT 1
                 FROM storage.prefixes AS c
                 WHERE c.bucket_id = p.bucket_id
                   AND c.level = p.level + 1
                   AND c.name COLLATE "C" LIKE p.name || '/%'
             )
        )
        DELETE
        FROM storage.prefixes AS p
            USING leaf AS l
        WHERE p.bucket_id = l.bucket_id
          AND p.name = l.name
          AND p.level = l.level;

        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;
        EXIT WHEN v_rows_deleted = 0;
    END LOOP;
END;
$$;


ALTER FUNCTION storage.delete_leaf_prefixes(bucket_ids text[], names text[]) OWNER TO supabase_storage_admin;

--
-- Name: enforce_bucket_name_length(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.enforce_bucket_name_length() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    if length(new.name) > 100 then
        raise exception 'bucket name "%" is too long (% characters). Max is 100.', new.name, length(new.name);
    end if;
    return new;
end;
$$;


ALTER FUNCTION storage.enforce_bucket_name_length() OWNER TO supabase_storage_admin;

--
-- Name: extension(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.extension(name text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    _parts text[];
    _filename text;
BEGIN
    SELECT string_to_array(name, '/') INTO _parts;
    SELECT _parts[array_length(_parts,1)] INTO _filename;
    RETURN reverse(split_part(reverse(_filename), '.', 1));
END
$$;


ALTER FUNCTION storage.extension(name text) OWNER TO supabase_storage_admin;

--
-- Name: filename(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.filename(name text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
_parts text[];
BEGIN
	select string_to_array(name, '/') into _parts;
	return _parts[array_length(_parts,1)];
END
$$;


ALTER FUNCTION storage.filename(name text) OWNER TO supabase_storage_admin;

--
-- Name: foldername(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.foldername(name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    _parts text[];
BEGIN
    -- Split on "/" to get path segments
    SELECT string_to_array(name, '/') INTO _parts;
    -- Return everything except the last segment
    RETURN _parts[1 : array_length(_parts,1) - 1];
END
$$;


ALTER FUNCTION storage.foldername(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_common_prefix(text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_common_prefix(p_key text, p_prefix text, p_delimiter text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$
SELECT CASE
    WHEN position(p_delimiter IN substring(p_key FROM length(p_prefix) + 1)) > 0
    THEN left(p_key, length(p_prefix) + position(p_delimiter IN substring(p_key FROM length(p_prefix) + 1)))
    ELSE NULL
END;
$$;


ALTER FUNCTION storage.get_common_prefix(p_key text, p_prefix text, p_delimiter text) OWNER TO supabase_storage_admin;

--
-- Name: get_level(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_level(name text) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
SELECT array_length(string_to_array("name", '/'), 1);
$$;


ALTER FUNCTION storage.get_level(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_prefix(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_prefix(name text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
SELECT
    CASE WHEN strpos("name", '/') > 0 THEN
             regexp_replace("name", '[\/]{1}[^\/]+\/?$', '')
         ELSE
             ''
        END;
$_$;


ALTER FUNCTION storage.get_prefix(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_prefixes(text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_prefixes(name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $$
DECLARE
    parts text[];
    prefixes text[];
    prefix text;
BEGIN
    -- Split the name into parts by '/'
    parts := string_to_array("name", '/');
    prefixes := '{}';

    -- Construct the prefixes, stopping one level below the last part
    FOR i IN 1..array_length(parts, 1) - 1 LOOP
            prefix := array_to_string(parts[1:i], '/');
            prefixes := array_append(prefixes, prefix);
    END LOOP;

    RETURN prefixes;
END;
$$;


ALTER FUNCTION storage.get_prefixes(name text) OWNER TO supabase_storage_admin;

--
-- Name: get_size_by_bucket(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.get_size_by_bucket() RETURNS TABLE(size bigint, bucket_id text)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    return query
        select sum((metadata->>'size')::bigint) as size, obj.bucket_id
        from "storage".objects as obj
        group by obj.bucket_id;
END
$$;


ALTER FUNCTION storage.get_size_by_bucket() OWNER TO supabase_storage_admin;

--
-- Name: list_multipart_uploads_with_delimiter(text, text, text, integer, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer DEFAULT 100, next_key_token text DEFAULT ''::text, next_upload_token text DEFAULT ''::text) RETURNS TABLE(key text, id text, created_at timestamp with time zone)
    LANGUAGE plpgsql
    AS $_$
BEGIN
    RETURN QUERY EXECUTE
        'SELECT DISTINCT ON(key COLLATE "C") * from (
            SELECT
                CASE
                    WHEN position($2 IN substring(key from length($1) + 1)) > 0 THEN
                        substring(key from 1 for length($1) + position($2 IN substring(key from length($1) + 1)))
                    ELSE
                        key
                END AS key, id, created_at
            FROM
                storage.s3_multipart_uploads
            WHERE
                bucket_id = $5 AND
                key ILIKE $1 || ''%'' AND
                CASE
                    WHEN $4 != '''' AND $6 = '''' THEN
                        CASE
                            WHEN position($2 IN substring(key from length($1) + 1)) > 0 THEN
                                substring(key from 1 for length($1) + position($2 IN substring(key from length($1) + 1))) COLLATE "C" > $4
                            ELSE
                                key COLLATE "C" > $4
                            END
                    ELSE
                        true
                END AND
                CASE
                    WHEN $6 != '''' THEN
                        id COLLATE "C" > $6
                    ELSE
                        true
                    END
            ORDER BY
                key COLLATE "C" ASC, created_at ASC) as e order by key COLLATE "C" LIMIT $3'
        USING prefix_param, delimiter_param, max_keys, next_key_token, bucket_id, next_upload_token;
END;
$_$;


ALTER FUNCTION storage.list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer, next_key_token text, next_upload_token text) OWNER TO supabase_storage_admin;

--
-- Name: list_objects_with_delimiter(text, text, text, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.list_objects_with_delimiter(_bucket_id text, prefix_param text, delimiter_param text, max_keys integer DEFAULT 100, start_after text DEFAULT ''::text, next_token text DEFAULT ''::text, sort_order text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, metadata jsonb, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
    v_peek_name TEXT;
    v_current RECORD;
    v_common_prefix TEXT;

    -- Configuration
    v_is_asc BOOLEAN;
    v_prefix TEXT;
    v_start TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;

    -- Seek state
    v_next_seek TEXT;
    v_count INT := 0;

    -- Dynamic SQL for batch query only
    v_batch_query TEXT;

BEGIN
    -- ========================================================================
    -- INITIALIZATION
    -- ========================================================================
    v_is_asc := lower(coalesce(sort_order, 'asc')) = 'asc';
    v_prefix := coalesce(prefix_param, '');
    v_start := CASE WHEN coalesce(next_token, '') <> '' THEN next_token ELSE coalesce(start_after, '') END;
    v_file_batch_size := LEAST(GREATEST(max_keys * 2, 100), 1000);

    -- Calculate upper bound for prefix filtering (bytewise, using COLLATE "C")
    IF v_prefix = '' THEN
        v_upper_bound := NULL;
    ELSIF right(v_prefix, 1) = delimiter_param THEN
        v_upper_bound := left(v_prefix, -1) || chr(ascii(delimiter_param) + 1);
    ELSE
        v_upper_bound := left(v_prefix, -1) || chr(ascii(right(v_prefix, 1)) + 1);
    END IF;

    -- Build batch query (dynamic SQL - called infrequently, amortized over many rows)
    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" >= $2 ' ||
                'AND o.name COLLATE "C" < $3 ORDER BY o.name COLLATE "C" ASC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" >= $2 ' ||
                'ORDER BY o.name COLLATE "C" ASC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" < $2 ' ||
                'AND o.name COLLATE "C" >= $3 ORDER BY o.name COLLATE "C" DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" < $2 ' ||
                'ORDER BY o.name COLLATE "C" DESC LIMIT $4';
        END IF;
    END IF;

    -- ========================================================================
    -- SEEK INITIALIZATION: Determine starting position
    -- ========================================================================
    IF v_start = '' THEN
        IF v_is_asc THEN
            v_next_seek := v_prefix;
        ELSE
            -- DESC without cursor: find the last item in range
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_prefix AND o.name COLLATE "C" < v_upper_bound
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix <> '' THEN
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_prefix
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            END IF;

            IF v_next_seek IS NOT NULL THEN
                v_next_seek := v_next_seek || delimiter_param;
            ELSE
                RETURN;
            END IF;
        END IF;
    ELSE
        -- Cursor provided: determine if it refers to a folder or leaf
        IF EXISTS (
            SELECT 1 FROM storage.objects o
            WHERE o.bucket_id = _bucket_id
              AND o.name COLLATE "C" LIKE v_start || delimiter_param || '%'
            LIMIT 1
        ) THEN
            -- Cursor refers to a folder
            IF v_is_asc THEN
                v_next_seek := v_start || chr(ascii(delimiter_param) + 1);
            ELSE
                v_next_seek := v_start || delimiter_param;
            END IF;
        ELSE
            -- Cursor refers to a leaf object
            IF v_is_asc THEN
                v_next_seek := v_start || delimiter_param;
            ELSE
                v_next_seek := v_start;
            END IF;
        END IF;
    END IF;

    -- ========================================================================
    -- MAIN LOOP: Hybrid peek-then-batch algorithm
    -- Uses STATIC SQL for peek (hot path) and DYNAMIC SQL for batch
    -- ========================================================================
    LOOP
        EXIT WHEN v_count >= max_keys;

        -- STEP 1: PEEK using STATIC SQL (plan cached, very fast)
        IF v_is_asc THEN
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_next_seek AND o.name COLLATE "C" < v_upper_bound
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_next_seek
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" < v_next_seek AND o.name COLLATE "C" >= v_prefix
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" < v_next_seek AND o.name COLLATE "C" >= v_prefix
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" < v_next_seek
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            END IF;
        END IF;

        EXIT WHEN v_peek_name IS NULL;

        -- STEP 2: Check if this is a FOLDER or FILE
        v_common_prefix := storage.get_common_prefix(v_peek_name, v_prefix, delimiter_param);

        IF v_common_prefix IS NOT NULL THEN
            -- FOLDER: Emit and skip to next folder (no heap access needed)
            name := rtrim(v_common_prefix, delimiter_param);
            id := NULL;
            updated_at := NULL;
            created_at := NULL;
            last_accessed_at := NULL;
            metadata := NULL;
            RETURN NEXT;
            v_count := v_count + 1;

            -- Advance seek past the folder range
            IF v_is_asc THEN
                v_next_seek := left(v_common_prefix, -1) || chr(ascii(delimiter_param) + 1);
            ELSE
                v_next_seek := v_common_prefix;
            END IF;
        ELSE
            -- FILE: Batch fetch using DYNAMIC SQL (overhead amortized over many rows)
            -- For ASC: upper_bound is the exclusive upper limit (< condition)
            -- For DESC: prefix is the inclusive lower limit (>= condition)
            FOR v_current IN EXECUTE v_batch_query USING _bucket_id, v_next_seek,
                CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix) ELSE v_prefix END, v_file_batch_size
            LOOP
                v_common_prefix := storage.get_common_prefix(v_current.name, v_prefix, delimiter_param);

                IF v_common_prefix IS NOT NULL THEN
                    -- Hit a folder: exit batch, let peek handle it
                    v_next_seek := v_current.name;
                    EXIT;
                END IF;

                -- Emit file
                name := v_current.name;
                id := v_current.id;
                updated_at := v_current.updated_at;
                created_at := v_current.created_at;
                last_accessed_at := v_current.last_accessed_at;
                metadata := v_current.metadata;
                RETURN NEXT;
                v_count := v_count + 1;

                -- Advance seek past this file
                IF v_is_asc THEN
                    v_next_seek := v_current.name || delimiter_param;
                ELSE
                    v_next_seek := v_current.name;
                END IF;

                EXIT WHEN v_count >= max_keys;
            END LOOP;
        END IF;
    END LOOP;
END;
$_$;


ALTER FUNCTION storage.list_objects_with_delimiter(_bucket_id text, prefix_param text, delimiter_param text, max_keys integer, start_after text, next_token text, sort_order text) OWNER TO supabase_storage_admin;

--
-- Name: operation(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.operation() RETURNS text
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    RETURN current_setting('storage.operation', true);
END;
$$;


ALTER FUNCTION storage.operation() OWNER TO supabase_storage_admin;

--
-- Name: protect_delete(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.protect_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Check if storage.allow_delete_query is set to 'true'
    IF COALESCE(current_setting('storage.allow_delete_query', true), 'false') != 'true' THEN
        RAISE EXCEPTION 'Direct deletion from storage tables is not allowed. Use the Storage API instead.'
            USING HINT = 'This prevents accidental data loss from orphaned objects.',
                  ERRCODE = '42501';
    END IF;
    RETURN NULL;
END;
$$;


ALTER FUNCTION storage.protect_delete() OWNER TO supabase_storage_admin;

--
-- Name: search(text, text, integer, integer, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search(prefix text, bucketname text, limits integer DEFAULT 100, levels integer DEFAULT 1, offsets integer DEFAULT 0, search text DEFAULT ''::text, sortcolumn text DEFAULT 'name'::text, sortorder text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
    v_peek_name TEXT;
    v_current RECORD;
    v_common_prefix TEXT;
    v_delimiter CONSTANT TEXT := '/';

    -- Configuration
    v_limit INT;
    v_prefix TEXT;
    v_prefix_lower TEXT;
    v_is_asc BOOLEAN;
    v_order_by TEXT;
    v_sort_order TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;

    -- Dynamic SQL for batch query only
    v_batch_query TEXT;

    -- Seek state
    v_next_seek TEXT;
    v_count INT := 0;
    v_skipped INT := 0;
BEGIN
    -- ========================================================================
    -- INITIALIZATION
    -- ========================================================================
    v_limit := LEAST(coalesce(limits, 100), 1500);
    v_prefix := coalesce(prefix, '') || coalesce(search, '');
    v_prefix_lower := lower(v_prefix);
    v_is_asc := lower(coalesce(sortorder, 'asc')) = 'asc';
    v_file_batch_size := LEAST(GREATEST(v_limit * 2, 100), 1000);

    -- Validate sort column
    CASE lower(coalesce(sortcolumn, 'name'))
        WHEN 'name' THEN v_order_by := 'name';
        WHEN 'updated_at' THEN v_order_by := 'updated_at';
        WHEN 'created_at' THEN v_order_by := 'created_at';
        WHEN 'last_accessed_at' THEN v_order_by := 'last_accessed_at';
        ELSE v_order_by := 'name';
    END CASE;

    v_sort_order := CASE WHEN v_is_asc THEN 'asc' ELSE 'desc' END;

    -- ========================================================================
    -- NON-NAME SORTING: Use path_tokens approach (unchanged)
    -- ========================================================================
    IF v_order_by != 'name' THEN
        RETURN QUERY EXECUTE format(
            $sql$
            WITH folders AS (
                SELECT path_tokens[$1] AS folder
                FROM storage.objects
                WHERE objects.name ILIKE $2 || '%%'
                  AND bucket_id = $3
                  AND array_length(objects.path_tokens, 1) <> $1
                GROUP BY folder
                ORDER BY folder %s
            )
            (SELECT folder AS "name",
                   NULL::uuid AS id,
                   NULL::timestamptz AS updated_at,
                   NULL::timestamptz AS created_at,
                   NULL::timestamptz AS last_accessed_at,
                   NULL::jsonb AS metadata FROM folders)
            UNION ALL
            (SELECT path_tokens[$1] AS "name",
                   id, updated_at, created_at, last_accessed_at, metadata
             FROM storage.objects
             WHERE objects.name ILIKE $2 || '%%'
               AND bucket_id = $3
               AND array_length(objects.path_tokens, 1) = $1
             ORDER BY %I %s)
            LIMIT $4 OFFSET $5
            $sql$, v_sort_order, v_order_by, v_sort_order
        ) USING levels, v_prefix, bucketname, v_limit, offsets;
        RETURN;
    END IF;

    -- ========================================================================
    -- NAME SORTING: Hybrid skip-scan with batch optimization
    -- ========================================================================

    -- Calculate upper bound for prefix filtering
    IF v_prefix_lower = '' THEN
        v_upper_bound := NULL;
    ELSIF right(v_prefix_lower, 1) = v_delimiter THEN
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(v_delimiter) + 1);
    ELSE
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(right(v_prefix_lower, 1)) + 1);
    END IF;

    -- Build batch query (dynamic SQL - called infrequently, amortized over many rows)
    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                'AND lower(o.name) COLLATE "C" < $3 ORDER BY lower(o.name) COLLATE "C" ASC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                'ORDER BY lower(o.name) COLLATE "C" ASC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                'AND lower(o.name) COLLATE "C" >= $3 ORDER BY lower(o.name) COLLATE "C" DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                'ORDER BY lower(o.name) COLLATE "C" DESC LIMIT $4';
        END IF;
    END IF;

    -- Initialize seek position
    IF v_is_asc THEN
        v_next_seek := v_prefix_lower;
    ELSE
        -- DESC: find the last item in range first (static SQL)
        IF v_upper_bound IS NOT NULL THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower AND lower(o.name) COLLATE "C" < v_upper_bound
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSIF v_prefix_lower <> '' THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSE
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        END IF;

        IF v_peek_name IS NOT NULL THEN
            v_next_seek := lower(v_peek_name) || v_delimiter;
        ELSE
            RETURN;
        END IF;
    END IF;

    -- ========================================================================
    -- MAIN LOOP: Hybrid peek-then-batch algorithm
    -- Uses STATIC SQL for peek (hot path) and DYNAMIC SQL for batch
    -- ========================================================================
    LOOP
        EXIT WHEN v_count >= v_limit;

        -- STEP 1: PEEK using STATIC SQL (plan cached, very fast)
        IF v_is_asc THEN
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek AND lower(o.name) COLLATE "C" < v_upper_bound
                ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek
                ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix_lower <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            END IF;
        END IF;

        EXIT WHEN v_peek_name IS NULL;

        -- STEP 2: Check if this is a FOLDER or FILE
        v_common_prefix := storage.get_common_prefix(lower(v_peek_name), v_prefix_lower, v_delimiter);

        IF v_common_prefix IS NOT NULL THEN
            -- FOLDER: Handle offset, emit if needed, skip to next folder
            IF v_skipped < offsets THEN
                v_skipped := v_skipped + 1;
            ELSE
                name := split_part(rtrim(storage.get_common_prefix(v_peek_name, v_prefix, v_delimiter), v_delimiter), v_delimiter, levels);
                id := NULL;
                updated_at := NULL;
                created_at := NULL;
                last_accessed_at := NULL;
                metadata := NULL;
                RETURN NEXT;
                v_count := v_count + 1;
            END IF;

            -- Advance seek past the folder range
            IF v_is_asc THEN
                v_next_seek := lower(left(v_common_prefix, -1)) || chr(ascii(v_delimiter) + 1);
            ELSE
                v_next_seek := lower(v_common_prefix);
            END IF;
        ELSE
            -- FILE: Batch fetch using DYNAMIC SQL (overhead amortized over many rows)
            -- For ASC: upper_bound is the exclusive upper limit (< condition)
            -- For DESC: prefix_lower is the inclusive lower limit (>= condition)
            FOR v_current IN EXECUTE v_batch_query
                USING bucketname, v_next_seek,
                    CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix_lower) ELSE v_prefix_lower END, v_file_batch_size
            LOOP
                v_common_prefix := storage.get_common_prefix(lower(v_current.name), v_prefix_lower, v_delimiter);

                IF v_common_prefix IS NOT NULL THEN
                    -- Hit a folder: exit batch, let peek handle it
                    v_next_seek := lower(v_current.name);
                    EXIT;
                END IF;

                -- Handle offset skipping
                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    -- Emit file
                    name := split_part(v_current.name, v_delimiter, levels);
                    id := v_current.id;
                    updated_at := v_current.updated_at;
                    created_at := v_current.created_at;
                    last_accessed_at := v_current.last_accessed_at;
                    metadata := v_current.metadata;
                    RETURN NEXT;
                    v_count := v_count + 1;
                END IF;

                -- Advance seek past this file
                IF v_is_asc THEN
                    v_next_seek := lower(v_current.name) || v_delimiter;
                ELSE
                    v_next_seek := lower(v_current.name);
                END IF;

                EXIT WHEN v_count >= v_limit;
            END LOOP;
        END IF;
    END LOOP;
END;
$_$;


ALTER FUNCTION storage.search(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) OWNER TO supabase_storage_admin;

--
-- Name: search_by_timestamp(text, text, integer, integer, text, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_by_timestamp(p_prefix text, p_bucket_id text, p_limit integer, p_level integer, p_start_after text, p_sort_order text, p_sort_column text, p_sort_column_after text) RETURNS TABLE(key text, name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
    v_cursor_op text;
    v_query text;
    v_prefix text;
BEGIN
    v_prefix := coalesce(p_prefix, '');

    IF p_sort_order = 'asc' THEN
        v_cursor_op := '>';
    ELSE
        v_cursor_op := '<';
    END IF;

    v_query := format($sql$
        WITH raw_objects AS (
            SELECT
                o.name AS obj_name,
                o.id AS obj_id,
                o.updated_at AS obj_updated_at,
                o.created_at AS obj_created_at,
                o.last_accessed_at AS obj_last_accessed_at,
                o.metadata AS obj_metadata,
                storage.get_common_prefix(o.name, $1, '/') AS common_prefix
            FROM storage.objects o
            WHERE o.bucket_id = $2
              AND o.name COLLATE "C" LIKE $1 || '%%'
        ),
        -- Aggregate common prefixes (folders)
        -- Both created_at and updated_at use MIN(obj_created_at) to match the old prefixes table behavior
        aggregated_prefixes AS (
            SELECT
                rtrim(common_prefix, '/') AS name,
                NULL::uuid AS id,
                MIN(obj_created_at) AS updated_at,
                MIN(obj_created_at) AS created_at,
                NULL::timestamptz AS last_accessed_at,
                NULL::jsonb AS metadata,
                TRUE AS is_prefix
            FROM raw_objects
            WHERE common_prefix IS NOT NULL
            GROUP BY common_prefix
        ),
        leaf_objects AS (
            SELECT
                obj_name AS name,
                obj_id AS id,
                obj_updated_at AS updated_at,
                obj_created_at AS created_at,
                obj_last_accessed_at AS last_accessed_at,
                obj_metadata AS metadata,
                FALSE AS is_prefix
            FROM raw_objects
            WHERE common_prefix IS NULL
        ),
        combined AS (
            SELECT * FROM aggregated_prefixes
            UNION ALL
            SELECT * FROM leaf_objects
        ),
        filtered AS (
            SELECT *
            FROM combined
            WHERE (
                $5 = ''
                OR ROW(
                    date_trunc('milliseconds', %I),
                    name COLLATE "C"
                ) %s ROW(
                    COALESCE(NULLIF($6, '')::timestamptz, 'epoch'::timestamptz),
                    $5
                )
            )
        )
        SELECT
            split_part(name, '/', $3) AS key,
            name,
            id,
            updated_at,
            created_at,
            last_accessed_at,
            metadata
        FROM filtered
        ORDER BY
            COALESCE(date_trunc('milliseconds', %I), 'epoch'::timestamptz) %s,
            name COLLATE "C" %s
        LIMIT $4
    $sql$,
        p_sort_column,
        v_cursor_op,
        p_sort_column,
        p_sort_order,
        p_sort_order
    );

    RETURN QUERY EXECUTE v_query
    USING v_prefix, p_bucket_id, p_level, p_limit, p_start_after, p_sort_column_after;
END;
$_$;


ALTER FUNCTION storage.search_by_timestamp(p_prefix text, p_bucket_id text, p_limit integer, p_level integer, p_start_after text, p_sort_order text, p_sort_column text, p_sort_column_after text) OWNER TO supabase_storage_admin;

--
-- Name: search_legacy_v1(text, text, integer, integer, integer, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_legacy_v1(prefix text, bucketname text, limits integer DEFAULT 100, levels integer DEFAULT 1, offsets integer DEFAULT 0, search text DEFAULT ''::text, sortcolumn text DEFAULT 'name'::text, sortorder text DEFAULT 'asc'::text) RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $_$
declare
    v_order_by text;
    v_sort_order text;
begin
    case
        when sortcolumn = 'name' then
            v_order_by = 'name';
        when sortcolumn = 'updated_at' then
            v_order_by = 'updated_at';
        when sortcolumn = 'created_at' then
            v_order_by = 'created_at';
        when sortcolumn = 'last_accessed_at' then
            v_order_by = 'last_accessed_at';
        else
            v_order_by = 'name';
        end case;

    case
        when sortorder = 'asc' then
            v_sort_order = 'asc';
        when sortorder = 'desc' then
            v_sort_order = 'desc';
        else
            v_sort_order = 'asc';
        end case;

    v_order_by = v_order_by || ' ' || v_sort_order;

    return query execute
        'with folders as (
           select path_tokens[$1] as folder
           from storage.objects
             where objects.name ilike $2 || $3 || ''%''
               and bucket_id = $4
               and array_length(objects.path_tokens, 1) <> $1
           group by folder
           order by folder ' || v_sort_order || '
     )
     (select folder as "name",
            null as id,
            null as updated_at,
            null as created_at,
            null as last_accessed_at,
            null as metadata from folders)
     union all
     (select path_tokens[$1] as "name",
            id,
            updated_at,
            created_at,
            last_accessed_at,
            metadata
     from storage.objects
     where objects.name ilike $2 || $3 || ''%''
       and bucket_id = $4
       and array_length(objects.path_tokens, 1) = $1
     order by ' || v_order_by || ')
     limit $5
     offset $6' using levels, prefix, search, bucketname, limits, offsets;
end;
$_$;


ALTER FUNCTION storage.search_legacy_v1(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) OWNER TO supabase_storage_admin;

--
-- Name: search_v2(text, text, integer, integer, text, text, text, text); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.search_v2(prefix text, bucket_name text, limits integer DEFAULT 100, levels integer DEFAULT 1, start_after text DEFAULT ''::text, sort_order text DEFAULT 'asc'::text, sort_column text DEFAULT 'name'::text, sort_column_after text DEFAULT ''::text) RETURNS TABLE(key text, name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
    v_sort_col text;
    v_sort_ord text;
    v_limit int;
BEGIN
    -- Cap limit to maximum of 1500 records
    v_limit := LEAST(coalesce(limits, 100), 1500);

    -- Validate and normalize sort_order
    v_sort_ord := lower(coalesce(sort_order, 'asc'));
    IF v_sort_ord NOT IN ('asc', 'desc') THEN
        v_sort_ord := 'asc';
    END IF;

    -- Validate and normalize sort_column
    v_sort_col := lower(coalesce(sort_column, 'name'));
    IF v_sort_col NOT IN ('name', 'updated_at', 'created_at') THEN
        v_sort_col := 'name';
    END IF;

    -- Route to appropriate implementation
    IF v_sort_col = 'name' THEN
        -- Use list_objects_with_delimiter for name sorting (most efficient: O(k * log n))
        RETURN QUERY
        SELECT
            split_part(l.name, '/', levels) AS key,
            l.name AS name,
            l.id,
            l.updated_at,
            l.created_at,
            l.last_accessed_at,
            l.metadata
        FROM storage.list_objects_with_delimiter(
            bucket_name,
            coalesce(prefix, ''),
            '/',
            v_limit,
            start_after,
            '',
            v_sort_ord
        ) l;
    ELSE
        -- Use aggregation approach for timestamp sorting
        -- Not efficient for large datasets but supports correct pagination
        RETURN QUERY SELECT * FROM storage.search_by_timestamp(
            prefix, bucket_name, v_limit, levels, start_after,
            v_sort_ord, v_sort_col, sort_column_after
        );
    END IF;
END;
$$;


ALTER FUNCTION storage.search_v2(prefix text, bucket_name text, limits integer, levels integer, start_after text, sort_order text, sort_column text, sort_column_after text) OWNER TO supabase_storage_admin;

--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: storage; Owner: supabase_storage_admin
--

CREATE FUNCTION storage.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW; 
END;
$$;


ALTER FUNCTION storage.update_updated_at_column() OWNER TO supabase_storage_admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: audit_log_entries; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.audit_log_entries (
    instance_id uuid,
    id uuid NOT NULL,
    payload json,
    created_at timestamp with time zone,
    ip_address character varying(64) DEFAULT ''::character varying NOT NULL
);


ALTER TABLE auth.audit_log_entries OWNER TO supabase_auth_admin;

--
-- Name: TABLE audit_log_entries; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.audit_log_entries IS 'Auth: Audit trail for user actions.';


--
-- Name: custom_oauth_providers; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.custom_oauth_providers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    provider_type text NOT NULL,
    identifier text NOT NULL,
    name text NOT NULL,
    client_id text NOT NULL,
    client_secret text NOT NULL,
    acceptable_client_ids text[] DEFAULT '{}'::text[] NOT NULL,
    scopes text[] DEFAULT '{}'::text[] NOT NULL,
    pkce_enabled boolean DEFAULT true NOT NULL,
    attribute_mapping jsonb DEFAULT '{}'::jsonb NOT NULL,
    authorization_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    email_optional boolean DEFAULT false NOT NULL,
    issuer text,
    discovery_url text,
    skip_nonce_check boolean DEFAULT false NOT NULL,
    cached_discovery jsonb,
    discovery_cached_at timestamp with time zone,
    authorization_url text,
    token_url text,
    userinfo_url text,
    jwks_uri text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT custom_oauth_providers_authorization_url_https CHECK (((authorization_url IS NULL) OR (authorization_url ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_authorization_url_length CHECK (((authorization_url IS NULL) OR (char_length(authorization_url) <= 2048))),
    CONSTRAINT custom_oauth_providers_client_id_length CHECK (((char_length(client_id) >= 1) AND (char_length(client_id) <= 512))),
    CONSTRAINT custom_oauth_providers_discovery_url_length CHECK (((discovery_url IS NULL) OR (char_length(discovery_url) <= 2048))),
    CONSTRAINT custom_oauth_providers_identifier_format CHECK ((identifier ~ '^[a-z0-9][a-z0-9:-]{0,48}[a-z0-9]$'::text)),
    CONSTRAINT custom_oauth_providers_issuer_length CHECK (((issuer IS NULL) OR ((char_length(issuer) >= 1) AND (char_length(issuer) <= 2048)))),
    CONSTRAINT custom_oauth_providers_jwks_uri_https CHECK (((jwks_uri IS NULL) OR (jwks_uri ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_jwks_uri_length CHECK (((jwks_uri IS NULL) OR (char_length(jwks_uri) <= 2048))),
    CONSTRAINT custom_oauth_providers_name_length CHECK (((char_length(name) >= 1) AND (char_length(name) <= 100))),
    CONSTRAINT custom_oauth_providers_oauth2_requires_endpoints CHECK (((provider_type <> 'oauth2'::text) OR ((authorization_url IS NOT NULL) AND (token_url IS NOT NULL) AND (userinfo_url IS NOT NULL)))),
    CONSTRAINT custom_oauth_providers_oidc_discovery_url_https CHECK (((provider_type <> 'oidc'::text) OR (discovery_url IS NULL) OR (discovery_url ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_oidc_issuer_https CHECK (((provider_type <> 'oidc'::text) OR (issuer IS NULL) OR (issuer ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_oidc_requires_issuer CHECK (((provider_type <> 'oidc'::text) OR (issuer IS NOT NULL))),
    CONSTRAINT custom_oauth_providers_provider_type_check CHECK ((provider_type = ANY (ARRAY['oauth2'::text, 'oidc'::text]))),
    CONSTRAINT custom_oauth_providers_token_url_https CHECK (((token_url IS NULL) OR (token_url ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_token_url_length CHECK (((token_url IS NULL) OR (char_length(token_url) <= 2048))),
    CONSTRAINT custom_oauth_providers_userinfo_url_https CHECK (((userinfo_url IS NULL) OR (userinfo_url ~~ 'https://%'::text))),
    CONSTRAINT custom_oauth_providers_userinfo_url_length CHECK (((userinfo_url IS NULL) OR (char_length(userinfo_url) <= 2048)))
);


ALTER TABLE auth.custom_oauth_providers OWNER TO supabase_auth_admin;

--
-- Name: flow_state; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.flow_state (
    id uuid NOT NULL,
    user_id uuid,
    auth_code text,
    code_challenge_method auth.code_challenge_method,
    code_challenge text,
    provider_type text NOT NULL,
    provider_access_token text,
    provider_refresh_token text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    authentication_method text NOT NULL,
    auth_code_issued_at timestamp with time zone,
    invite_token text,
    referrer text,
    oauth_client_state_id uuid,
    linking_target_id uuid,
    email_optional boolean DEFAULT false NOT NULL
);


ALTER TABLE auth.flow_state OWNER TO supabase_auth_admin;

--
-- Name: TABLE flow_state; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.flow_state IS 'Stores metadata for all OAuth/SSO login flows';


--
-- Name: identities; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.identities (
    provider_id text NOT NULL,
    user_id uuid NOT NULL,
    identity_data jsonb NOT NULL,
    provider text NOT NULL,
    last_sign_in_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    email text GENERATED ALWAYS AS (lower((identity_data ->> 'email'::text))) STORED,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE auth.identities OWNER TO supabase_auth_admin;

--
-- Name: TABLE identities; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.identities IS 'Auth: Stores identities associated to a user.';


--
-- Name: COLUMN identities.email; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.identities.email IS 'Auth: Email is a generated column that references the optional email property in the identity_data';


--
-- Name: instances; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.instances (
    id uuid NOT NULL,
    uuid uuid,
    raw_base_config text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


ALTER TABLE auth.instances OWNER TO supabase_auth_admin;

--
-- Name: TABLE instances; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.instances IS 'Auth: Manages users across multiple sites.';


--
-- Name: mfa_amr_claims; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_amr_claims (
    session_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    authentication_method text NOT NULL,
    id uuid NOT NULL
);


ALTER TABLE auth.mfa_amr_claims OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_amr_claims; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_amr_claims IS 'auth: stores authenticator method reference claims for multi factor authentication';


--
-- Name: mfa_challenges; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_challenges (
    id uuid NOT NULL,
    factor_id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    verified_at timestamp with time zone,
    ip_address inet NOT NULL,
    otp_code text,
    web_authn_session_data jsonb
);


ALTER TABLE auth.mfa_challenges OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_challenges; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_challenges IS 'auth: stores metadata about challenge requests made';


--
-- Name: mfa_factors; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.mfa_factors (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    friendly_name text,
    factor_type auth.factor_type NOT NULL,
    status auth.factor_status NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    secret text,
    phone text,
    last_challenged_at timestamp with time zone,
    web_authn_credential jsonb,
    web_authn_aaguid uuid,
    last_webauthn_challenge_data jsonb
);


ALTER TABLE auth.mfa_factors OWNER TO supabase_auth_admin;

--
-- Name: TABLE mfa_factors; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.mfa_factors IS 'auth: stores metadata about factors';


--
-- Name: COLUMN mfa_factors.last_webauthn_challenge_data; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.mfa_factors.last_webauthn_challenge_data IS 'Stores the latest WebAuthn challenge data including attestation/assertion for customer verification';


--
-- Name: oauth_authorizations; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_authorizations (
    id uuid NOT NULL,
    authorization_id text NOT NULL,
    client_id uuid NOT NULL,
    user_id uuid,
    redirect_uri text NOT NULL,
    scope text NOT NULL,
    state text,
    resource text,
    code_challenge text,
    code_challenge_method auth.code_challenge_method,
    response_type auth.oauth_response_type DEFAULT 'code'::auth.oauth_response_type NOT NULL,
    status auth.oauth_authorization_status DEFAULT 'pending'::auth.oauth_authorization_status NOT NULL,
    authorization_code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '00:03:00'::interval) NOT NULL,
    approved_at timestamp with time zone,
    nonce text,
    CONSTRAINT oauth_authorizations_authorization_code_length CHECK ((char_length(authorization_code) <= 255)),
    CONSTRAINT oauth_authorizations_code_challenge_length CHECK ((char_length(code_challenge) <= 128)),
    CONSTRAINT oauth_authorizations_expires_at_future CHECK ((expires_at > created_at)),
    CONSTRAINT oauth_authorizations_nonce_length CHECK ((char_length(nonce) <= 255)),
    CONSTRAINT oauth_authorizations_redirect_uri_length CHECK ((char_length(redirect_uri) <= 2048)),
    CONSTRAINT oauth_authorizations_resource_length CHECK ((char_length(resource) <= 2048)),
    CONSTRAINT oauth_authorizations_scope_length CHECK ((char_length(scope) <= 4096)),
    CONSTRAINT oauth_authorizations_state_length CHECK ((char_length(state) <= 4096))
);


ALTER TABLE auth.oauth_authorizations OWNER TO supabase_auth_admin;

--
-- Name: oauth_client_states; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_client_states (
    id uuid NOT NULL,
    provider_type text NOT NULL,
    code_verifier text,
    created_at timestamp with time zone NOT NULL
);


ALTER TABLE auth.oauth_client_states OWNER TO supabase_auth_admin;

--
-- Name: TABLE oauth_client_states; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.oauth_client_states IS 'Stores OAuth states for third-party provider authentication flows where Supabase acts as the OAuth client.';


--
-- Name: oauth_clients; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_clients (
    id uuid NOT NULL,
    client_secret_hash text,
    registration_type auth.oauth_registration_type NOT NULL,
    redirect_uris text NOT NULL,
    grant_types text NOT NULL,
    client_name text,
    client_uri text,
    logo_uri text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    client_type auth.oauth_client_type DEFAULT 'confidential'::auth.oauth_client_type NOT NULL,
    token_endpoint_auth_method text NOT NULL,
    CONSTRAINT oauth_clients_client_name_length CHECK ((char_length(client_name) <= 1024)),
    CONSTRAINT oauth_clients_client_uri_length CHECK ((char_length(client_uri) <= 2048)),
    CONSTRAINT oauth_clients_logo_uri_length CHECK ((char_length(logo_uri) <= 2048)),
    CONSTRAINT oauth_clients_token_endpoint_auth_method_check CHECK ((token_endpoint_auth_method = ANY (ARRAY['client_secret_basic'::text, 'client_secret_post'::text, 'none'::text])))
);


ALTER TABLE auth.oauth_clients OWNER TO supabase_auth_admin;

--
-- Name: oauth_consents; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.oauth_consents (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    client_id uuid NOT NULL,
    scopes text NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT oauth_consents_revoked_after_granted CHECK (((revoked_at IS NULL) OR (revoked_at >= granted_at))),
    CONSTRAINT oauth_consents_scopes_length CHECK ((char_length(scopes) <= 2048)),
    CONSTRAINT oauth_consents_scopes_not_empty CHECK ((char_length(TRIM(BOTH FROM scopes)) > 0))
);


ALTER TABLE auth.oauth_consents OWNER TO supabase_auth_admin;

--
-- Name: one_time_tokens; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.one_time_tokens (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    token_type auth.one_time_token_type NOT NULL,
    token_hash text NOT NULL,
    relates_to text NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT one_time_tokens_token_hash_check CHECK ((char_length(token_hash) > 0))
);


ALTER TABLE auth.one_time_tokens OWNER TO supabase_auth_admin;

--
-- Name: refresh_tokens; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.refresh_tokens (
    instance_id uuid,
    id bigint NOT NULL,
    token character varying(255),
    user_id character varying(255),
    revoked boolean,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    parent character varying(255),
    session_id uuid
);


ALTER TABLE auth.refresh_tokens OWNER TO supabase_auth_admin;

--
-- Name: TABLE refresh_tokens; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.refresh_tokens IS 'Auth: Store of tokens used to refresh JWT tokens once they expire.';


--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE; Schema: auth; Owner: supabase_auth_admin
--

CREATE SEQUENCE auth.refresh_tokens_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE auth.refresh_tokens_id_seq OWNER TO supabase_auth_admin;

--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: auth; Owner: supabase_auth_admin
--

ALTER SEQUENCE auth.refresh_tokens_id_seq OWNED BY auth.refresh_tokens.id;


--
-- Name: saml_providers; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.saml_providers (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    entity_id text NOT NULL,
    metadata_xml text NOT NULL,
    metadata_url text,
    attribute_mapping jsonb,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    name_id_format text,
    CONSTRAINT "entity_id not empty" CHECK ((char_length(entity_id) > 0)),
    CONSTRAINT "metadata_url not empty" CHECK (((metadata_url = NULL::text) OR (char_length(metadata_url) > 0))),
    CONSTRAINT "metadata_xml not empty" CHECK ((char_length(metadata_xml) > 0))
);


ALTER TABLE auth.saml_providers OWNER TO supabase_auth_admin;

--
-- Name: TABLE saml_providers; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.saml_providers IS 'Auth: Manages SAML Identity Provider connections.';


--
-- Name: saml_relay_states; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.saml_relay_states (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    request_id text NOT NULL,
    for_email text,
    redirect_to text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    flow_state_id uuid,
    CONSTRAINT "request_id not empty" CHECK ((char_length(request_id) > 0))
);


ALTER TABLE auth.saml_relay_states OWNER TO supabase_auth_admin;

--
-- Name: TABLE saml_relay_states; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.saml_relay_states IS 'Auth: Contains SAML Relay State information for each Service Provider initiated login.';


--
-- Name: schema_migrations; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.schema_migrations (
    version character varying(255) NOT NULL
);


ALTER TABLE auth.schema_migrations OWNER TO supabase_auth_admin;

--
-- Name: TABLE schema_migrations; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.schema_migrations IS 'Auth: Manages updates to the auth system.';


--
-- Name: sessions; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sessions (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    factor_id uuid,
    aal auth.aal_level,
    not_after timestamp with time zone,
    refreshed_at timestamp without time zone,
    user_agent text,
    ip inet,
    tag text,
    oauth_client_id uuid,
    refresh_token_hmac_key text,
    refresh_token_counter bigint,
    scopes text,
    CONSTRAINT sessions_scopes_length CHECK ((char_length(scopes) <= 4096))
);


ALTER TABLE auth.sessions OWNER TO supabase_auth_admin;

--
-- Name: TABLE sessions; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sessions IS 'Auth: Stores session data associated to a user.';


--
-- Name: COLUMN sessions.not_after; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sessions.not_after IS 'Auth: Not after is a nullable column that contains a timestamp after which the session should be regarded as expired.';


--
-- Name: COLUMN sessions.refresh_token_hmac_key; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sessions.refresh_token_hmac_key IS 'Holds a HMAC-SHA256 key used to sign refresh tokens for this session.';


--
-- Name: COLUMN sessions.refresh_token_counter; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sessions.refresh_token_counter IS 'Holds the ID (counter) of the last issued refresh token.';


--
-- Name: sso_domains; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sso_domains (
    id uuid NOT NULL,
    sso_provider_id uuid NOT NULL,
    domain text NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    CONSTRAINT "domain not empty" CHECK ((char_length(domain) > 0))
);


ALTER TABLE auth.sso_domains OWNER TO supabase_auth_admin;

--
-- Name: TABLE sso_domains; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sso_domains IS 'Auth: Manages SSO email address domain mapping to an SSO Identity Provider.';


--
-- Name: sso_providers; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.sso_providers (
    id uuid NOT NULL,
    resource_id text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    disabled boolean,
    CONSTRAINT "resource_id not empty" CHECK (((resource_id = NULL::text) OR (char_length(resource_id) > 0)))
);


ALTER TABLE auth.sso_providers OWNER TO supabase_auth_admin;

--
-- Name: TABLE sso_providers; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.sso_providers IS 'Auth: Manages SSO identity provider information; see saml_providers for SAML.';


--
-- Name: COLUMN sso_providers.resource_id; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.sso_providers.resource_id IS 'Auth: Uniquely identifies a SSO provider according to a user-chosen resource ID (case insensitive), useful in infrastructure as code.';


--
-- Name: users; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.users (
    instance_id uuid,
    id uuid NOT NULL,
    aud character varying(255),
    role character varying(255),
    email character varying(255),
    encrypted_password character varying(255),
    email_confirmed_at timestamp with time zone,
    invited_at timestamp with time zone,
    confirmation_token character varying(255),
    confirmation_sent_at timestamp with time zone,
    recovery_token character varying(255),
    recovery_sent_at timestamp with time zone,
    email_change_token_new character varying(255),
    email_change character varying(255),
    email_change_sent_at timestamp with time zone,
    last_sign_in_at timestamp with time zone,
    raw_app_meta_data jsonb,
    raw_user_meta_data jsonb,
    is_super_admin boolean,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    phone text DEFAULT NULL::character varying,
    phone_confirmed_at timestamp with time zone,
    phone_change text DEFAULT ''::character varying,
    phone_change_token character varying(255) DEFAULT ''::character varying,
    phone_change_sent_at timestamp with time zone,
    confirmed_at timestamp with time zone GENERATED ALWAYS AS (LEAST(email_confirmed_at, phone_confirmed_at)) STORED,
    email_change_token_current character varying(255) DEFAULT ''::character varying,
    email_change_confirm_status smallint DEFAULT 0,
    banned_until timestamp with time zone,
    reauthentication_token character varying(255) DEFAULT ''::character varying,
    reauthentication_sent_at timestamp with time zone,
    is_sso_user boolean DEFAULT false NOT NULL,
    deleted_at timestamp with time zone,
    is_anonymous boolean DEFAULT false NOT NULL,
    CONSTRAINT users_email_change_confirm_status_check CHECK (((email_change_confirm_status >= 0) AND (email_change_confirm_status <= 2)))
);


ALTER TABLE auth.users OWNER TO supabase_auth_admin;

--
-- Name: TABLE users; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON TABLE auth.users IS 'Auth: Stores user login data within a secure schema.';


--
-- Name: COLUMN users.is_sso_user; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON COLUMN auth.users.is_sso_user IS 'Auth: Set this column to true when the account comes from SSO. These accounts can have duplicate emails.';


--
-- Name: webauthn_challenges; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.webauthn_challenges (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid,
    challenge_type text NOT NULL,
    session_data jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT webauthn_challenges_challenge_type_check CHECK ((challenge_type = ANY (ARRAY['signup'::text, 'registration'::text, 'authentication'::text])))
);


ALTER TABLE auth.webauthn_challenges OWNER TO supabase_auth_admin;

--
-- Name: webauthn_credentials; Type: TABLE; Schema: auth; Owner: supabase_auth_admin
--

CREATE TABLE auth.webauthn_credentials (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    credential_id bytea NOT NULL,
    public_key bytea NOT NULL,
    attestation_type text DEFAULT ''::text NOT NULL,
    aaguid uuid,
    sign_count bigint DEFAULT 0 NOT NULL,
    transports jsonb DEFAULT '[]'::jsonb NOT NULL,
    backup_eligible boolean DEFAULT false NOT NULL,
    backed_up boolean DEFAULT false NOT NULL,
    friendly_name text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone
);


ALTER TABLE auth.webauthn_credentials OWNER TO supabase_auth_admin;

--
-- Name: bvp_stats; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.bvp_stats (
    game_id bigint NOT NULL,
    batter_id bigint NOT NULL,
    pitcher_id bigint NOT NULL,
    bvp_plate_appearances integer,
    bvp_at_bats integer,
    bvp_hits integer,
    bvp_home_runs integer,
    bvp_strikeouts integer,
    bvp_walks integer,
    bvp_rbi integer,
    bvp_total_bases integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT (now() AT TIME ZONE 'utc'::text),
    CONSTRAINT bvp_stats_batter_id_pos CHECK ((batter_id > 0)),
    CONSTRAINT bvp_stats_game_id_pos CHECK ((game_id > 0)),
    CONSTRAINT bvp_stats_pitcher_id_pos CHECK ((pitcher_id > 0))
);


ALTER TABLE mlb.bvp_stats OWNER TO postgres;

--
-- Name: game_info; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.game_info (
    game_id bigint NOT NULL,
    game_time timestamp without time zone,
    game_date date,
    home_team_id bigint,
    away_team_id bigint,
    home_team_abbr text,
    away_team_abbr text,
    starting_pitcher_id_home bigint,
    starting_pitcher_id_away bigint
);


ALTER TABLE mlb.game_info OWNER TO postgres;

--
-- Name: model_training_props; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.model_training_props (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    game_date date,
    player_name text,
    team text,
    "position" text,
    prop_type text,
    prop_value double precision,
    over_under text,
    result double precision,
    status text,
    game_id bigint NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    opponent text,
    home_away text,
    game_time timestamp without time zone,
    outcome text,
    streak_count integer,
    is_pitcher boolean,
    predicted_outcome text,
    confidence_score numeric,
    prediction_timestamp timestamp with time zone,
    was_correct boolean,
    rolling_result_avg_7 double precision,
    player_id bigint NOT NULL,
    hit_streak integer,
    win_streak integer,
    streak_type text,
    opponent_avg_result_vs_player numeric,
    is_home boolean,
    opponent_avg_win_rate double precision,
    prop_source text DEFAULT 'user-added'::text NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    game_day_of_week text,
    time_of_day_bucket text,
    corrected_result numeric,
    correction_applied_at timestamp with time zone,
    outcome_corrected_at timestamp with time zone,
    team_id bigint NOT NULL,
    line_diff numeric,
    opponent_encoded text,
    line numeric,
    opponent_player_id bigint,
    opposing_batter_id bigint,
    position_backfill_status text,
    opponent_team_id integer,
    starting_pitcher_id bigint,
    game_id_txt text GENERATED ALWAYS AS ((game_id)::text) STORED,
    CONSTRAINT model_training_props_game_id_pos CHECK ((game_id > 0)),
    CONSTRAINT model_training_props_player_id_pos CHECK ((player_id > 0))
);


ALTER TABLE mlb.model_training_props OWNER TO postgres;

--
-- Name: player_stats; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_stats (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    team text,
    opponent text,
    is_home boolean,
    "position" text,
    hits integer,
    total_bases integer,
    rbis integer,
    runs_scored integer,
    strikeouts_batting integer,
    walks integer,
    singles integer,
    doubles integer,
    triples integer,
    home_runs integer,
    stolen_bases integer,
    strikeouts_pitching integer,
    walks_allowed integer,
    hits_allowed integer,
    outs_recorded integer,
    earned_runs integer,
    is_starter smallint,
    at_bats integer,
    CONSTRAINT player_stats_game_id_pos CHECK ((game_id > 0)),
    CONSTRAINT player_stats_player_id_pos CHECK ((player_id > 0))
);


ALTER TABLE mlb.player_stats OWNER TO postgres;

--
-- Name: pitcher_game_v4_base; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_base AS
 SELECT player_stats.player_id,
    player_stats.game_id,
    player_stats.game_date,
    player_stats.team,
    player_stats.opponent,
    player_stats.is_home,
    player_stats."position",
    player_stats.is_starter,
    player_stats.outs_recorded,
    player_stats.strikeouts_pitching,
    player_stats.walks_allowed,
    player_stats.hits_allowed,
    player_stats.earned_runs
   FROM mlb.player_stats
  WHERE ((player_stats."position" = 'P'::text) AND (player_stats.is_starter = 1));


ALTER VIEW mlb.pitcher_game_v4_base OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_1; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_1 AS
 SELECT b.player_id,
    b.game_id,
    b.game_date,
    b.team,
    b.opponent,
    b.is_home,
    b.outs_recorded,
    ( SELECT p1.outs_recorded
           FROM mlb.pitcher_game_v4_base p1
          WHERE ((p1.player_id = b.player_id) AND (p1.game_date < b.game_date))
          ORDER BY p1.game_date DESC, p1.game_id DESC
         LIMIT 1) AS prev_outs_1,
    ( SELECT avg(x.outs_recorded) AS avg
           FROM ( SELECT p2.outs_recorded
                   FROM mlb.pitcher_game_v4_base p2
                  WHERE ((p2.player_id = b.player_id) AND (p2.game_date < b.game_date))
                  ORDER BY p2.game_date DESC, p2.game_id DESC
                 LIMIT 3) x) AS avg_outs_last_3,
    ( SELECT avg(x.hits_allowed) AS avg
           FROM ( SELECT p3.hits_allowed
                   FROM mlb.pitcher_game_v4_base p3
                  WHERE ((p3.player_id = b.player_id) AND (p3.game_date < b.game_date))
                  ORDER BY p3.game_date DESC, p3.game_id DESC
                 LIMIT 3) x) AS avg_hits_allowed_last_3,
    ( SELECT avg(x.walks_allowed) AS avg
           FROM ( SELECT p4.walks_allowed
                   FROM mlb.pitcher_game_v4_base p4
                  WHERE ((p4.player_id = b.player_id) AND (p4.game_date < b.game_date))
                  ORDER BY p4.game_date DESC, p4.game_id DESC
                 LIMIT 3) x) AS avg_walks_allowed_last_3,
    ( SELECT avg(x.earned_runs) AS avg
           FROM ( SELECT p5.earned_runs
                   FROM mlb.pitcher_game_v4_base p5
                  WHERE ((p5.player_id = b.player_id) AND (p5.game_date < b.game_date))
                  ORDER BY p5.game_date DESC, p5.game_id DESC
                 LIMIT 3) x) AS avg_earned_runs_last_3,
    ( SELECT avg(x.strikeouts_pitching) AS avg
           FROM ( SELECT p6.strikeouts_pitching
                   FROM mlb.pitcher_game_v4_base p6
                  WHERE ((p6.player_id = b.player_id) AND (p6.game_date < b.game_date))
                  ORDER BY p6.game_date DESC, p6.game_id DESC
                 LIMIT 3) x) AS avg_strikeouts_last_3,
    ( SELECT (count(*))::integer AS count
           FROM mlb.pitcher_game_v4_base p7
          WHERE ((p7.player_id = b.player_id) AND (p7.game_date < b.game_date))) AS n_prior_starts
   FROM mlb.pitcher_game_v4_base b;


ALTER VIEW mlb.pitcher_game_v4_features_1 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_2; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_2 AS
 SELECT f.player_id,
    f.game_id,
    f.game_date,
    f.team,
    f.opponent,
    f.is_home,
    f.outs_recorded,
    f.prev_outs_1,
    f.avg_outs_last_3,
    f.avg_hits_allowed_last_3,
    f.avg_walks_allowed_last_3,
    f.avg_earned_runs_last_3,
    f.avg_strikeouts_last_3,
    f.n_prior_starts,
    (f.game_date - ( SELECT max(b_prev.game_date) AS max
           FROM mlb.pitcher_game_v4_base b_prev
          WHERE ((b_prev.player_id = f.player_id) AND (b_prev.game_date < f.game_date)))) AS days_rest
   FROM mlb.pitcher_game_v4_features_1 f;


ALTER VIEW mlb.pitcher_game_v4_features_2 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_2_clean; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_2_clean AS
 SELECT f2.player_id,
    f2.game_id,
    f2.game_date,
    f2.team,
    f2.opponent,
    f2.is_home,
    f2.outs_recorded,
    f2.prev_outs_1,
    f2.avg_outs_last_3,
    f2.avg_hits_allowed_last_3,
    f2.avg_walks_allowed_last_3,
    f2.avg_earned_runs_last_3,
    f2.avg_strikeouts_last_3,
    f2.n_prior_starts,
    f2.days_rest,
        CASE
            WHEN (f2.days_rest > 14) THEN NULL::integer
            ELSE f2.days_rest
        END AS days_rest_capped
   FROM mlb.pitcher_game_v4_features_2 f2;


ALTER VIEW mlb.pitcher_game_v4_features_2_clean OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_3; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_3 AS
 SELECT f2c.player_id,
    f2c.game_id,
    f2c.game_date,
    f2c.team,
    f2c.opponent,
    f2c.is_home,
    f2c.outs_recorded,
    f2c.prev_outs_1,
    f2c.avg_outs_last_3,
    f2c.avg_hits_allowed_last_3,
    f2c.avg_walks_allowed_last_3,
    f2c.avg_earned_runs_last_3,
    f2c.avg_strikeouts_last_3,
    f2c.n_prior_starts,
    f2c.days_rest,
    f2c.days_rest_capped,
    ( SELECT p.earned_runs
           FROM mlb.pitcher_game_v4_base p
          WHERE ((p.player_id = f2c.player_id) AND (p.game_date < f2c.game_date))
          ORDER BY p.game_date DESC, p.game_id DESC
         LIMIT 1) AS prev_earned_runs_1
   FROM mlb.pitcher_game_v4_features_2_clean f2c;


ALTER VIEW mlb.pitcher_game_v4_features_3 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_4; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_4 AS
 SELECT f3.player_id,
    f3.game_id,
    f3.game_date,
    f3.team,
    f3.opponent,
    f3.is_home,
    f3.outs_recorded,
    f3.prev_outs_1,
    f3.avg_outs_last_3,
    f3.avg_hits_allowed_last_3,
    f3.avg_walks_allowed_last_3,
    f3.avg_earned_runs_last_3,
    f3.avg_strikeouts_last_3,
    f3.n_prior_starts,
    f3.days_rest,
    f3.days_rest_capped,
    f3.prev_earned_runs_1,
    ( SELECT max(x.outs_recorded) AS max
           FROM ( SELECT p.outs_recorded
                   FROM mlb.pitcher_game_v4_base p
                  WHERE ((p.player_id = f3.player_id) AND (p.game_date < f3.game_date))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 3) x) AS max_outs_last_3
   FROM mlb.pitcher_game_v4_features_3 f3;


ALTER VIEW mlb.pitcher_game_v4_features_4 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_5; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_5 AS
 SELECT f4.player_id,
    f4.game_id,
    f4.game_date,
    f4.team,
    f4.opponent,
    f4.is_home,
    f4.outs_recorded,
    f4.prev_outs_1,
    f4.avg_outs_last_3,
    f4.avg_hits_allowed_last_3,
    f4.avg_walks_allowed_last_3,
    f4.avg_earned_runs_last_3,
    f4.avg_strikeouts_last_3,
    f4.n_prior_starts,
    f4.days_rest,
    f4.days_rest_capped,
    f4.prev_earned_runs_1,
    f4.max_outs_last_3,
    ( SELECT avg(x.outs_recorded) AS avg
           FROM ( SELECT b.outs_recorded
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.opponent = f4.opponent) AND (b.game_date < f4.game_date) AND (b."position" = 'P'::text) AND (b.is_starter = 1))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS opponent_avg_outs_allowed_last_3
   FROM mlb.pitcher_game_v4_features_4 f4;


ALTER VIEW mlb.pitcher_game_v4_features_5 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_6; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_6 AS
 SELECT f5.player_id,
    f5.game_id,
    f5.game_date,
    f5.team,
    f5.opponent,
    f5.is_home,
    f5.outs_recorded,
    f5.prev_outs_1,
    f5.avg_outs_last_3,
    f5.avg_hits_allowed_last_3,
    f5.avg_walks_allowed_last_3,
    f5.avg_earned_runs_last_3,
    f5.avg_strikeouts_last_3,
    f5.n_prior_starts,
    f5.days_rest,
    f5.days_rest_capped,
    f5.prev_earned_runs_1,
    f5.max_outs_last_3,
    f5.opponent_avg_outs_allowed_last_3,
    ( SELECT avg(x.k_per_out) AS avg
           FROM ( SELECT ((b.strikeouts_pitching)::numeric / NULLIF((b.outs_recorded)::numeric, (0)::numeric)) AS k_per_out
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.player_id = f5.player_id) AND (b.game_date < f5.game_date))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS k_per_out_last_3,
    ( SELECT avg(x.bb_per_out) AS avg
           FROM ( SELECT ((b.walks_allowed)::numeric / NULLIF((b.outs_recorded)::numeric, (0)::numeric)) AS bb_per_out
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.player_id = f5.player_id) AND (b.game_date < f5.game_date))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS bb_per_out_last_3,
    ( SELECT avg(x.h_per_out) AS avg
           FROM ( SELECT ((b.hits_allowed)::numeric / NULLIF((b.outs_recorded)::numeric, (0)::numeric)) AS h_per_out
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.player_id = f5.player_id) AND (b.game_date < f5.game_date))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS h_per_out_last_3
   FROM mlb.pitcher_game_v4_features_5 f5;


ALTER VIEW mlb.pitcher_game_v4_features_6 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_7; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_7 AS
 WITH global_means AS (
         SELECT avg(((pitcher_game_v4_base.strikeouts_pitching)::numeric / NULLIF((pitcher_game_v4_base.outs_recorded)::numeric, (0)::numeric))) AS mu_k,
            avg(((pitcher_game_v4_base.walks_allowed)::numeric / NULLIF((pitcher_game_v4_base.outs_recorded)::numeric, (0)::numeric))) AS mu_bb,
            avg(((pitcher_game_v4_base.hits_allowed)::numeric / NULLIF((pitcher_game_v4_base.outs_recorded)::numeric, (0)::numeric))) AS mu_h
           FROM mlb.pitcher_game_v4_base
          WHERE (pitcher_game_v4_base.is_starter = 1)
        ), calc AS (
         SELECT f6.player_id,
            f6.game_id,
            f6.game_date,
            f6.team,
            f6.opponent,
            f6.is_home,
            f6.outs_recorded,
            f6.prev_outs_1,
            f6.avg_outs_last_3,
            f6.avg_hits_allowed_last_3,
            f6.avg_walks_allowed_last_3,
            f6.avg_earned_runs_last_3,
            f6.avg_strikeouts_last_3,
            f6.n_prior_starts,
            f6.days_rest,
            f6.days_rest_capped,
            f6.prev_earned_runs_1,
            f6.max_outs_last_3,
            f6.opponent_avg_outs_allowed_last_3,
            f6.k_per_out_last_3,
            f6.bb_per_out_last_3,
            f6.h_per_out_last_3,
            gm.mu_k,
            gm.mu_bb,
            gm.mu_h,
            (5)::numeric AS lambda,
            (LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric AS n_eff,
            ((((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric * COALESCE(f6.k_per_out_last_3, gm.mu_k)) + ((5)::numeric * gm.mu_k)) / ((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric + (5)::numeric)) AS k_rate_shrunk_raw,
            ((((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric * COALESCE(f6.bb_per_out_last_3, gm.mu_bb)) + ((5)::numeric * gm.mu_bb)) / ((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric + (5)::numeric)) AS bb_rate_shrunk_raw,
            ((((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric * COALESCE(f6.h_per_out_last_3, gm.mu_h)) + ((5)::numeric * gm.mu_h)) / ((LEAST(COALESCE(f6.n_prior_starts, 0), 3))::numeric + (5)::numeric)) AS h_rate_shrunk_raw
           FROM (mlb.pitcher_game_v4_features_6 f6
             CROSS JOIN global_means gm)
        )
 SELECT calc.player_id,
    calc.game_id,
    calc.game_date,
    calc.team,
    calc.opponent,
    calc.is_home,
    calc.outs_recorded,
    calc.prev_outs_1,
    calc.avg_outs_last_3,
    calc.avg_hits_allowed_last_3,
    calc.avg_walks_allowed_last_3,
    calc.avg_earned_runs_last_3,
    calc.avg_strikeouts_last_3,
    calc.n_prior_starts,
    calc.days_rest,
    calc.days_rest_capped,
    calc.prev_earned_runs_1,
    calc.max_outs_last_3,
    calc.opponent_avg_outs_allowed_last_3,
    calc.k_per_out_last_3,
    calc.bb_per_out_last_3,
    calc.h_per_out_last_3,
    calc.mu_k,
    calc.mu_bb,
    calc.mu_h,
    calc.lambda,
    calc.n_eff,
    calc.k_rate_shrunk_raw,
    calc.bb_rate_shrunk_raw,
    calc.h_rate_shrunk_raw,
    LEAST(GREATEST(calc.k_rate_shrunk_raw, 0.10), 0.60) AS k_rate_shrunk,
    LEAST(GREATEST(calc.bb_rate_shrunk_raw, 0.02), 0.30) AS bb_rate_shrunk,
    LEAST(GREATEST(calc.h_rate_shrunk_raw, 0.10), 0.70) AS h_rate_shrunk
   FROM calc;


ALTER VIEW mlb.pitcher_game_v4_features_7 OWNER TO postgres;

--
-- Name: pitcher_game_v4_features_8; Type: VIEW; Schema: mlb; Owner: postgres
--

CREATE VIEW mlb.pitcher_game_v4_features_8 AS
 SELECT f7.player_id,
    f7.game_id,
    f7.game_date,
    f7.team,
    f7.opponent,
    f7.is_home,
    f7.outs_recorded,
    f7.prev_outs_1,
    f7.avg_outs_last_3,
    f7.avg_hits_allowed_last_3,
    f7.avg_walks_allowed_last_3,
    f7.avg_earned_runs_last_3,
    f7.avg_strikeouts_last_3,
    f7.n_prior_starts,
    f7.days_rest,
    f7.days_rest_capped,
    f7.prev_earned_runs_1,
    f7.max_outs_last_3,
    f7.opponent_avg_outs_allowed_last_3,
    f7.k_per_out_last_3,
    f7.bb_per_out_last_3,
    f7.h_per_out_last_3,
    f7.mu_k,
    f7.mu_bb,
    f7.mu_h,
    f7.lambda,
    f7.n_eff,
    f7.k_rate_shrunk_raw,
    f7.bb_rate_shrunk_raw,
    f7.h_rate_shrunk_raw,
    f7.k_rate_shrunk,
    f7.bb_rate_shrunk,
    f7.h_rate_shrunk,
    ( SELECT avg(x.expected_pitches) AS avg
           FROM ( SELECT (((b.outs_recorded + b.walks_allowed) + b.hits_allowed))::numeric AS expected_pitches
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.player_id = f7.player_id) AND (b.game_date < f7.game_date))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS expected_pitches_last_3,
    ( SELECT avg(x.pitches_per_out) AS avg
           FROM ( SELECT ((((b.outs_recorded + b.walks_allowed) + b.hits_allowed))::numeric / NULLIF((b.outs_recorded)::numeric, (0)::numeric)) AS pitches_per_out
                   FROM mlb.pitcher_game_v4_base b
                  WHERE ((b.player_id = f7.player_id) AND (b.game_date < f7.game_date))
                  ORDER BY b.game_date DESC, b.game_id DESC
                 LIMIT 3) x) AS pitches_per_out_last_3
   FROM mlb.pitcher_game_v4_features_7 f7;


ALTER VIEW mlb.pitcher_game_v4_features_8 OWNER TO postgres;

--
-- Name: player_derived_stats; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_derived_stats (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    team character varying(10),
    is_home boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    d7_hits numeric,
    d7_runs_scored numeric,
    d7_rbis numeric,
    d7_home_runs numeric,
    d7_singles numeric,
    d7_doubles numeric,
    d7_triples numeric,
    d7_walks numeric,
    d7_strikeouts_batting numeric,
    d7_stolen_bases numeric,
    d7_total_bases numeric,
    d7_hits_runs_rbis numeric,
    d7_runs_rbis numeric,
    d7_outs_recorded numeric,
    d7_strikeouts_pitching numeric,
    d7_walks_allowed numeric,
    d7_earned_runs numeric,
    d7_hits_allowed numeric,
    d15_hits numeric,
    d15_runs_scored numeric,
    d15_rbis numeric,
    d15_home_runs numeric,
    d15_singles numeric,
    d15_doubles numeric,
    d15_triples numeric,
    d15_walks numeric,
    d15_strikeouts_batting numeric,
    d15_stolen_bases numeric,
    d15_total_bases numeric,
    d15_hits_runs_rbis numeric,
    d15_runs_rbis numeric,
    d15_outs_recorded numeric,
    d15_strikeouts_pitching numeric,
    d15_walks_allowed numeric,
    d15_earned_runs numeric,
    d15_hits_allowed numeric,
    d30_hits numeric,
    d30_runs_scored numeric,
    d30_rbis numeric,
    d30_home_runs numeric,
    d30_singles numeric,
    d30_doubles numeric,
    d30_triples numeric,
    d30_walks numeric,
    d30_strikeouts_batting numeric,
    d30_stolen_bases numeric,
    d30_total_bases numeric,
    d30_hits_runs_rbis numeric,
    d30_runs_rbis numeric,
    d30_outs_recorded numeric,
    d30_strikeouts_pitching numeric,
    d30_walks_allowed numeric,
    d30_earned_runs numeric,
    d30_hits_allowed numeric,
    d7_at_bats numeric,
    d15_at_bats numeric,
    d30_at_bats numeric,
    CONSTRAINT player_derived_stats_game_id_pos CHECK ((game_id > 0)),
    CONSTRAINT player_derived_stats_player_id_pos CHECK ((player_id > 0))
);


ALTER TABLE mlb.player_derived_stats OWNER TO postgres;

--
-- Name: player_ids; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_ids (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    player_name text NOT NULL,
    team text,
    player_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    team_id bigint,
    is_placeholder boolean DEFAULT false NOT NULL,
    CONSTRAINT player_ids_player_name_not_blank CHECK (((player_name IS NOT NULL) AND (btrim(player_name) <> ''::text)))
);


ALTER TABLE mlb.player_ids OWNER TO postgres;

--
-- Name: player_profiles_cache; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_profiles_cache (
    player_id bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now(),
    data jsonb,
    CONSTRAINT player_profiles_cache_player_id_pos CHECK ((player_id > 0))
);


ALTER TABLE mlb.player_profiles_cache OWNER TO postgres;

--
-- Name: player_props; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_props (
    id uuid DEFAULT extensions.uuid_generate_v4() NOT NULL,
    game_date date NOT NULL,
    player_name text NOT NULL,
    team text,
    "position" text,
    prop_type text NOT NULL,
    prop_value double precision,
    result text,
    outcome text,
    is_pitcher boolean DEFAULT false,
    streak_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    over_under text,
    status text DEFAULT 'Pending'::text,
    game_id bigint NOT NULL,
    predicted_outcome text,
    confidence_score double precision,
    was_correct boolean,
    prediction_timestamp timestamp without time zone,
    player_id bigint NOT NULL,
    prop_source text DEFAULT 'user_added'::text NOT NULL,
    user_id uuid,
    opponent_encoded integer,
    is_home boolean,
    home_away text,
    opponent_team_id integer,
    game_day_of_week text,
    time_of_day_bucket text,
    opponent text,
    game_time timestamp with time zone,
    starting_pitcher_id bigint,
    team_id bigint,
    CONSTRAINT player_props_game_id_pos CHECK ((game_id > 0)),
    CONSTRAINT player_props_player_id_pos CHECK ((player_id > 0)),
    CONSTRAINT status_check CHECK ((status = ANY (ARRAY['pending'::text, 'win'::text, 'loss'::text, 'push'::text, 'dnp'::text])))
);


ALTER TABLE mlb.player_props OWNER TO postgres;

--
-- Name: player_streak_history; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_streak_history (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    prop_type text NOT NULL,
    prop_source text DEFAULT 'profile'::text NOT NULL,
    streak_type text,
    streak_count integer,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT psh_streak_type_check CHECK (((streak_type IS NULL) OR (lower(streak_type) = ANY (ARRAY['hot'::text, 'cold'::text, 'neutral'::text]))))
);


ALTER TABLE mlb.player_streak_history OWNER TO postgres;

--
-- Name: player_streak_profiles; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_streak_profiles (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    player_id bigint NOT NULL,
    player_name text,
    team text,
    prop_type text NOT NULL,
    streak_type text NOT NULL,
    last_updated timestamp with time zone DEFAULT timezone('utc'::text, now()),
    is_active boolean DEFAULT true,
    streak_broken timestamp without time zone,
    last_game_date date,
    last_outcome text,
    recent_outcomes text[],
    updated_at timestamp with time zone,
    streak_count integer,
    hit_streak integer DEFAULT 0,
    win_streak integer,
    rolling_result_avg_7 double precision,
    source text DEFAULT 'unknown'::text,
    prop_source text,
    CONSTRAINT player_streak_profiles_streak_type_check CHECK ((streak_type = ANY (ARRAY['hot'::text, 'cold'::text, 'neutral'::text])))
);


ALTER TABLE mlb.player_streak_profiles OWNER TO postgres;

--
-- Name: player_team_by_game; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.player_team_by_game (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE mlb.player_team_by_game OWNER TO postgres;

--
-- Name: prop_features_precomputed; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.prop_features_precomputed (
    prop_type text NOT NULL,
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    features jsonb NOT NULL,
    feature_set_tag text DEFAULT 'v1'::text NOT NULL,
    model_tag text,
    lineup_slot integer,
    is_probable_sp boolean,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE mlb.prop_features_precomputed OWNER TO postgres;

--
-- Name: today_odds_book_rows; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.today_odds_book_rows (
    slate_date date NOT NULL,
    game_date date NOT NULL,
    snapshot_ts timestamp with time zone NOT NULL,
    snapshot_file text NOT NULL,
    event_id text,
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    player_name text NOT NULL,
    player_name_norm text NOT NULL,
    home_team_code text NOT NULL,
    away_team_code text NOT NULL,
    team text,
    opponent text,
    is_home boolean,
    prop_type text NOT NULL,
    market_key text NOT NULL,
    line numeric(8,3) NOT NULL,
    bookmaker_key text NOT NULL,
    price_over_american numeric,
    price_under_american numeric
);


ALTER TABLE mlb.today_odds_book_rows OWNER TO postgres;

--
-- Name: today_slate_rows; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.today_slate_rows (
    slate_date date NOT NULL,
    game_date date NOT NULL,
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    player_name text NOT NULL,
    player_name_norm text NOT NULL,
    home_team_code text NOT NULL,
    away_team_code text NOT NULL,
    prop_type text NOT NULL,
    market_key text,
    line numeric(8,3) NOT NULL,
    prob_over numeric,
    prob_under numeric
);


ALTER TABLE mlb.today_slate_rows OWNER TO postgres;

--
-- Name: today_market_snapshot; Type: MATERIALIZED VIEW; Schema: mlb; Owner: postgres
--

CREATE MATERIALIZED VIEW mlb.today_market_snapshot AS
 WITH active_slate AS (
         SELECT max(today_slate_rows.slate_date) AS slate_date
           FROM mlb.today_slate_rows
        ), base AS (
         SELECT o.slate_date,
            o.game_date,
            o.snapshot_ts,
            o.snapshot_file,
            o.event_id,
            o.game_id,
            o.player_id,
            o.player_name,
            o.player_name_norm,
            o.home_team_code,
            o.away_team_code,
            o.team,
            o.opponent,
            o.is_home,
            o.prop_type,
            o.market_key,
            o.line,
            o.bookmaker_key,
            o.price_over_american,
            o.price_under_american,
                CASE
                    WHEN ((o.price_over_american IS NOT NULL) AND ((abs(o.price_over_american) >= (100)::numeric) AND (abs(o.price_over_american) <= (500)::numeric))) THEN o.price_over_american
                    ELSE NULL::numeric
                END AS price_over_american_clean,
                CASE
                    WHEN ((o.price_under_american IS NOT NULL) AND ((abs(o.price_under_american) >= (100)::numeric) AND (abs(o.price_under_american) <= (500)::numeric))) THEN o.price_under_american
                    ELSE NULL::numeric
                END AS price_under_american_clean
           FROM (mlb.today_odds_book_rows o
             JOIN active_slate a_1 ON ((o.slate_date = a_1.slate_date)))
        ), latest AS (
         SELECT b.slate_date,
            b.game_date,
            b.snapshot_ts,
            b.snapshot_file,
            b.event_id,
            b.game_id,
            b.player_id,
            b.player_name,
            b.player_name_norm,
            b.home_team_code,
            b.away_team_code,
            b.team,
            b.opponent,
            b.is_home,
            b.prop_type,
            b.market_key,
            b.line,
            b.bookmaker_key,
            b.price_over_american,
            b.price_under_american,
            b.price_over_american_clean,
            b.price_under_american_clean
           FROM (base b
             JOIN ( SELECT base.player_id,
                    base.game_id,
                    base.prop_type,
                    base.line,
                    max(base.snapshot_ts) AS last_snapshot_ts
                   FROM base
                  WHERE ((base.price_over_american_clean IS NOT NULL) OR (base.price_under_american_clean IS NOT NULL))
                  GROUP BY base.player_id, base.game_id, base.prop_type, base.line) l ON (((b.player_id = l.player_id) AND (b.game_id = l.game_id) AND (b.prop_type = l.prop_type) AND (b.line = l.line) AND (b.snapshot_ts = l.last_snapshot_ts))))
          WHERE ((b.price_over_american_clean IS NOT NULL) OR (b.price_under_american_clean IS NOT NULL))
        ), best_over AS (
         SELECT DISTINCT ON (latest.player_id, latest.game_id, latest.prop_type, latest.line) latest.player_id,
            latest.game_id,
            latest.prop_type,
            latest.line,
            latest.bookmaker_key AS best_over_book,
            latest.price_over_american_clean AS best_over_price
           FROM latest
          WHERE (latest.price_over_american_clean IS NOT NULL)
          ORDER BY latest.player_id, latest.game_id, latest.prop_type, latest.line, latest.price_over_american_clean DESC, latest.bookmaker_key
        ), best_under AS (
         SELECT DISTINCT ON (latest.player_id, latest.game_id, latest.prop_type, latest.line) latest.player_id,
            latest.game_id,
            latest.prop_type,
            latest.line,
            latest.bookmaker_key AS best_under_book,
            latest.price_under_american_clean AS best_under_price
           FROM latest
          WHERE (latest.price_under_american_clean IS NOT NULL)
          ORDER BY latest.player_id, latest.game_id, latest.prop_type, latest.line, latest.price_under_american_clean DESC, latest.bookmaker_key
        ), agg AS (
         SELECT latest.game_date,
            latest.game_id,
            latest.player_id,
            max(latest.player_name) AS player_name,
            max(latest.team) AS team,
            max(latest.opponent) AS opponent,
            bool_or(latest.is_home) AS is_home,
            latest.prop_type,
            latest.line,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((latest.price_over_american_clean)::double precision)) AS market_median_over_price_raw,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((latest.price_under_american_clean)::double precision)) AS market_median_under_price_raw,
            max(latest.price_over_american_clean) AS market_max_over_price,
            min(latest.price_over_american_clean) AS market_min_over_price,
            max(latest.price_under_american_clean) AS market_max_under_price,
            min(latest.price_under_american_clean) AS market_min_under_price,
            count(*) FILTER (WHERE (latest.price_over_american_clean IS NOT NULL)) AS book_count_over,
            count(*) FILTER (WHERE (latest.price_under_american_clean IS NOT NULL)) AS book_count_under,
            stddev_pop(latest.price_over_american_clean) AS price_dispersion_over,
            stddev_pop(latest.price_under_american_clean) AS price_dispersion_under,
            max(latest.snapshot_ts) AS last_snapshot_ts
           FROM latest
          GROUP BY latest.game_date, latest.game_id, latest.player_id, latest.prop_type, latest.line
         HAVING ((count(*) FILTER (WHERE (latest.price_over_american_clean IS NOT NULL)) > 0) AND (count(*) FILTER (WHERE (latest.price_under_american_clean IS NOT NULL)) > 0))
        )
 SELECT a.game_date,
    a.game_id,
    a.player_id,
    a.player_name,
    a.team,
    a.opponent,
    a.is_home,
    a.prop_type,
    a.line,
    bo.best_over_price,
    bu.best_under_price,
    bo.best_over_book,
    bu.best_under_book,
    a.market_median_over_price_raw AS market_median_over_price,
    a.market_median_under_price_raw AS market_median_under_price,
    (a.market_max_over_price - a.market_min_over_price) AS market_range_over,
    (a.market_max_under_price - a.market_min_under_price) AS market_range_under,
    a.book_count_over,
    a.book_count_under,
    a.price_dispersion_over,
    a.price_dispersion_under,
    a.last_snapshot_ts
   FROM ((agg a
     LEFT JOIN best_over bo ON (((a.player_id = bo.player_id) AND (a.game_id = bo.game_id) AND (a.prop_type = bo.prop_type) AND (a.line = bo.line))))
     LEFT JOIN best_under bu ON (((a.player_id = bu.player_id) AND (a.game_id = bu.game_id) AND (a.prop_type = bu.prop_type) AND (a.line = bu.line))))
  WITH NO DATA;


ALTER MATERIALIZED VIEW mlb.today_market_snapshot OWNER TO postgres;

--
-- Name: today_market_timing_signal; Type: MATERIALIZED VIEW; Schema: mlb; Owner: postgres
--

CREATE MATERIALIZED VIEW mlb.today_market_timing_signal AS
 WITH active_slate AS (
         SELECT max(today_slate_rows.slate_date) AS slate_date
           FROM mlb.today_slate_rows
        ), base AS (
         SELECT o_1.slate_date,
            o_1.game_date,
            o_1.snapshot_ts,
            o_1.snapshot_file,
            o_1.event_id,
            o_1.game_id,
            o_1.player_id,
            o_1.player_name,
            o_1.player_name_norm,
            o_1.home_team_code,
            o_1.away_team_code,
            o_1.team,
            o_1.opponent,
            o_1.is_home,
            o_1.prop_type,
            o_1.market_key,
            o_1.line,
            o_1.bookmaker_key,
            o_1.price_over_american,
            o_1.price_under_american,
                CASE
                    WHEN ((o_1.price_over_american IS NOT NULL) AND ((abs(o_1.price_over_american) >= (100)::numeric) AND (abs(o_1.price_over_american) <= (500)::numeric))) THEN o_1.price_over_american
                    ELSE NULL::numeric
                END AS price_over_american_clean,
                CASE
                    WHEN ((o_1.price_under_american IS NOT NULL) AND ((abs(o_1.price_under_american) >= (100)::numeric) AND (abs(o_1.price_under_american) <= (500)::numeric))) THEN o_1.price_under_american
                    ELSE NULL::numeric
                END AS price_under_american_clean
           FROM (mlb.today_odds_book_rows o_1
             JOIN active_slate a ON ((o_1.slate_date = a.slate_date)))
        ), snap AS (
         SELECT base.player_id,
            base.game_id,
            base.prop_type,
            base.line,
            base.snapshot_ts,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((base.price_over_american_clean)::double precision)) AS snap_over_median_raw,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((base.price_under_american_clean)::double precision)) AS snap_under_median_raw,
            max(base.price_over_american_clean) AS snap_best_over,
            max(base.price_under_american_clean) AS snap_best_under
           FROM base
          GROUP BY base.player_id, base.game_id, base.prop_type, base.line, base.snapshot_ts
         HAVING ((count(*) FILTER (WHERE (base.price_over_american_clean IS NOT NULL)) > 0) AND (count(*) FILTER (WHERE (base.price_under_american_clean IS NOT NULL)) > 0))
        ), ranked AS (
         SELECT s.player_id,
            s.game_id,
            s.prop_type,
            s.line,
            s.snapshot_ts,
            s.snap_over_median_raw,
            s.snap_under_median_raw,
            s.snap_best_over,
            s.snap_best_under,
            row_number() OVER (PARTITION BY s.player_id, s.game_id, s.prop_type, s.line ORDER BY s.snapshot_ts) AS rn_open,
            row_number() OVER (PARTITION BY s.player_id, s.game_id, s.prop_type, s.line ORDER BY s.snapshot_ts DESC) AS rn_latest,
            count(*) OVER (PARTITION BY s.player_id, s.game_id, s.prop_type, s.line) AS num_snapshots
           FROM snap s
        ), open_rows AS (
         SELECT ranked.player_id,
            ranked.game_id,
            ranked.prop_type,
            ranked.line,
            ranked.snapshot_ts,
            ranked.snap_over_median_raw,
            ranked.snap_under_median_raw,
            ranked.snap_best_over,
            ranked.snap_best_under,
            ranked.rn_open,
            ranked.rn_latest,
            ranked.num_snapshots
           FROM ranked
          WHERE (ranked.rn_open = 1)
        ), latest_rows AS (
         SELECT ranked.player_id,
            ranked.game_id,
            ranked.prop_type,
            ranked.line,
            ranked.snapshot_ts,
            ranked.snap_over_median_raw,
            ranked.snap_under_median_raw,
            ranked.snap_best_over,
            ranked.snap_best_under,
            ranked.rn_open,
            ranked.rn_latest,
            ranked.num_snapshots
           FROM ranked
          WHERE (ranked.rn_latest = 1)
        ), vol AS (
         SELECT snap.player_id,
            snap.game_id,
            snap.prop_type,
            snap.line,
            (max(snap.snap_over_median_raw) - min(snap.snap_over_median_raw)) AS over_span,
            (max(snap.snap_under_median_raw) - min(snap.snap_under_median_raw)) AS under_span
           FROM snap
          GROUP BY snap.player_id, snap.game_id, snap.prop_type, snap.line
        )
 SELECT l.player_id,
    l.game_id,
    l.prop_type,
    l.line,
        CASE
            WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
            ELSE NULL::double precision
        END AS open_over_price,
        CASE
            WHEN ((o.snap_under_median_raw IS NOT NULL) AND ((abs(o.snap_under_median_raw) >= (100)::double precision) AND (abs(o.snap_under_median_raw) <= (500)::double precision))) THEN o.snap_under_median_raw
            ELSE NULL::double precision
        END AS open_under_price,
        CASE
            WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
            ELSE NULL::double precision
        END AS latest_over_price,
        CASE
            WHEN ((l.snap_under_median_raw IS NOT NULL) AND ((abs(l.snap_under_median_raw) >= (100)::double precision) AND (abs(l.snap_under_median_raw) <= (500)::double precision))) THEN l.snap_under_median_raw
            ELSE NULL::double precision
        END AS latest_under_price,
    l.snap_best_over AS best_over_price_now,
    l.snap_best_under AS best_under_price_now,
    (EXTRACT(epoch FROM (l.snapshot_ts - o.snapshot_ts)) / 60.0) AS minutes_since_open,
    l.num_snapshots,
    (
        CASE
            WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
            ELSE NULL::double precision
        END -
        CASE
            WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
            ELSE NULL::double precision
        END) AS over_price_change_from_open,
    (
        CASE
            WHEN ((l.snap_under_median_raw IS NOT NULL) AND ((abs(l.snap_under_median_raw) >= (100)::double precision) AND (abs(l.snap_under_median_raw) <= (500)::double precision))) THEN l.snap_under_median_raw
            ELSE NULL::double precision
        END -
        CASE
            WHEN ((o.snap_under_median_raw IS NOT NULL) AND ((abs(o.snap_under_median_raw) >= (100)::double precision) AND (abs(o.snap_under_median_raw) <= (500)::double precision))) THEN o.snap_under_median_raw
            ELSE NULL::double precision
        END) AS under_price_change_from_open,
    v.over_span,
    v.under_span,
    (COALESCE(l.snap_best_over, o.snap_best_over) > o.snap_best_over) AS best_price_improved_since_open,
    (COALESCE(l.snap_best_over, o.snap_best_over) < o.snap_best_over) AS best_price_worsened_since_open,
        CASE
            WHEN (GREATEST(COALESCE(v.over_span, (0)::double precision), COALESCE(v.under_span, (0)::double precision)) >= (25)::double precision) THEN 'VOLATILE'::text
            WHEN (COALESCE((
            CASE
                WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
                ELSE NULL::double precision
            END -
            CASE
                WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
                ELSE NULL::double precision
            END), (0)::double precision) >= (10)::double precision) THEN 'WAIT'::text
            WHEN (COALESCE((
            CASE
                WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
                ELSE NULL::double precision
            END -
            CASE
                WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
                ELSE NULL::double precision
            END), (0)::double precision) <= ('-10'::integer)::double precision) THEN 'EARLY'::text
            ELSE 'STABLE'::text
        END AS timing_signal,
        CASE
            WHEN (GREATEST(COALESCE(v.over_span, (0)::double precision), COALESCE(v.under_span, (0)::double precision)) >= (25)::double precision) THEN 'Large intraday movement'::text
            WHEN (COALESCE((
            CASE
                WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
                ELSE NULL::double precision
            END -
            CASE
                WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
                ELSE NULL::double precision
            END), (0)::double precision) >= (10)::double precision) THEN 'Current price better than open'::text
            WHEN (COALESCE((
            CASE
                WHEN ((l.snap_over_median_raw IS NOT NULL) AND ((abs(l.snap_over_median_raw) >= (100)::double precision) AND (abs(l.snap_over_median_raw) <= (500)::double precision))) THEN l.snap_over_median_raw
                ELSE NULL::double precision
            END -
            CASE
                WHEN ((o.snap_over_median_raw IS NOT NULL) AND ((abs(o.snap_over_median_raw) >= (100)::double precision) AND (abs(o.snap_over_median_raw) <= (500)::double precision))) THEN o.snap_over_median_raw
                ELSE NULL::double precision
            END), (0)::double precision) <= ('-10'::integer)::double precision) THEN 'Current price worse than open'::text
            ELSE 'Little intraday movement'::text
        END AS timing_reason
   FROM ((latest_rows l
     JOIN open_rows o ON (((l.player_id = o.player_id) AND (l.game_id = o.game_id) AND (l.prop_type = o.prop_type) AND (l.line = o.line))))
     LEFT JOIN vol v ON (((l.player_id = v.player_id) AND (l.game_id = v.game_id) AND (l.prop_type = v.prop_type) AND (l.line = v.line))))
  WITH NO DATA;


ALTER MATERIALIZED VIEW mlb.today_market_timing_signal OWNER TO postgres;

--
-- Name: today_player_context; Type: MATERIALIZED VIEW; Schema: mlb; Owner: postgres
--

CREATE MATERIALIZED VIEW mlb.today_player_context AS
 WITH active_slate AS (
         SELECT max(today_slate_rows.slate_date) AS slate_date
           FROM mlb.today_slate_rows
        ), hist AS (
         SELECT m.player_id,
            max(m.player_name) AS player_name,
            lower(TRIM(BOTH FROM m.prop_type)) AS prop_type,
            m.game_id,
            m.game_date,
            (m.prop_value)::numeric AS prop_value,
            m.line,
            (
                CASE
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'over'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'win'::text)) THEN 1
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'over'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'loss'::text)) THEN 0
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'under'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'win'::text)) THEN 0
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'under'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'loss'::text)) THEN 1
                    ELSE NULL::integer
                END)::numeric AS over_hit_flag
           FROM (mlb.model_training_props m
             JOIN active_slate a ON (((m.game_date < a.slate_date) AND (m.game_date >= (date_trunc('year'::text, (a.slate_date)::timestamp without time zone))::date))))
          WHERE ((m.player_id IS NOT NULL) AND (m.game_id IS NOT NULL) AND (m.prop_type IS NOT NULL) AND (m.prop_value IS NOT NULL) AND (m.line IS NOT NULL) AND (lower(TRIM(BOTH FROM COALESCE(m.prop_source, ''::text))) = 'mlb_api'::text) AND (lower(TRIM(BOTH FROM COALESCE(m.outcome, ''::text))) = ANY (ARRAY['win'::text, 'loss'::text, 'push'::text])) AND (lower(TRIM(BOTH FROM COALESCE(m.over_under, ''::text))) = ANY (ARRAY['over'::text, 'under'::text])))
          GROUP BY m.player_id, (lower(TRIM(BOTH FROM m.prop_type))), m.game_id, m.game_date, ((m.prop_value)::numeric), m.line, ((
                CASE
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'over'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'win'::text)) THEN 1
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'over'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'loss'::text)) THEN 0
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'under'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'win'::text)) THEN 0
                    WHEN ((lower(TRIM(BOTH FROM m.over_under)) = 'under'::text) AND (lower(TRIM(BOTH FROM m.outcome)) = 'loss'::text)) THEN 1
                    ELSE NULL::integer
                END)::numeric)
        ), ranked AS (
         SELECT h.player_id,
            h.player_name,
            h.prop_type,
            h.game_id,
            h.game_date,
            h.prop_value,
            h.line,
            h.over_hit_flag,
            row_number() OVER (PARTITION BY h.player_id, h.prop_type ORDER BY h.game_date DESC, h.game_id DESC) AS rn
           FROM hist h
        ), agg AS (
         SELECT ranked.player_id,
            max(ranked.player_name) AS player_name,
            ranked.prop_type,
            avg(ranked.prop_value) FILTER (WHERE (ranked.rn <= 5)) AS last_5_avg,
            avg(ranked.prop_value) FILTER (WHERE (ranked.rn <= 10)) AS last_10_avg,
            avg(ranked.prop_value) AS season_avg,
            avg(ranked.over_hit_flag) FILTER (WHERE ((ranked.rn <= 5) AND (ranked.over_hit_flag IS NOT NULL))) AS hit_rate_last_5,
            avg(ranked.over_hit_flag) FILTER (WHERE ((ranked.rn <= 10) AND (ranked.over_hit_flag IS NOT NULL))) AS hit_rate_last_10,
            avg(ranked.over_hit_flag) FILTER (WHERE (ranked.over_hit_flag IS NOT NULL)) AS hit_rate_season,
            stddev_pop(ranked.prop_value) FILTER (WHERE (ranked.rn <= 10)) AS stddev_last_10
           FROM ranked
          GROUP BY ranked.player_id, ranked.prop_type
        ), hit_ordered AS (
         SELECT r.player_id,
            r.prop_type,
            r.game_id,
            r.game_date,
            r.over_hit_flag,
            lag(r.over_hit_flag) OVER (PARTITION BY r.player_id, r.prop_type ORDER BY r.game_date DESC, r.game_id DESC) AS prev_hit
           FROM ranked r
          WHERE (r.over_hit_flag IS NOT NULL)
        ), hit_grouped AS (
         SELECT h.player_id,
            h.prop_type,
            h.game_id,
            h.game_date,
            h.over_hit_flag,
            h.prev_hit,
            sum(
                CASE
                    WHEN (h.prev_hit IS DISTINCT FROM h.over_hit_flag) THEN 1
                    ELSE 0
                END) OVER (PARTITION BY h.player_id, h.prop_type ORDER BY h.game_date DESC, h.game_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_group
           FROM hit_ordered h
        ), streak AS (
         SELECT hit_grouped.player_id,
            hit_grouped.prop_type,
                CASE
                    WHEN (max(hit_grouped.over_hit_flag) FILTER (WHERE (hit_grouped.streak_group = 1)) = (1)::numeric) THEN 'HOT'::text
                    WHEN (max(hit_grouped.over_hit_flag) FILTER (WHERE (hit_grouped.streak_group = 1)) = (0)::numeric) THEN 'COLD'::text
                    ELSE 'NEUTRAL'::text
                END AS streak_type,
            count(*) FILTER (WHERE (hit_grouped.streak_group = 1)) AS streak_count
           FROM hit_grouped
          GROUP BY hit_grouped.player_id, hit_grouped.prop_type
        ), scored AS (
         SELECT a.player_id,
            a.player_name,
            a.prop_type,
            a.last_5_avg,
            a.last_10_avg,
            a.season_avg,
            a.hit_rate_last_5,
            a.hit_rate_last_10,
            a.hit_rate_season,
            a.stddev_last_10,
            (a.stddev_last_10 / NULLIF(abs(a.last_10_avg), (0)::numeric)) AS cv_last_10,
            (a.hit_rate_last_10 - a.hit_rate_season) AS baseline_delta
           FROM agg a
        )
 SELECT s.player_id,
    s.player_name,
    s.prop_type,
    s.last_5_avg,
    s.last_10_avg,
    s.season_avg,
    s.hit_rate_last_5,
    s.hit_rate_last_10,
    s.hit_rate_season,
    s.stddev_last_10,
    s.cv_last_10,
    round((((100.0)::double precision * ((1.0)::double precision - percent_rank() OVER (PARTITION BY s.prop_type ORDER BY s.cv_last_10))))::numeric, 1) AS consistency_score,
    COALESCE(st.streak_type, 'NEUTRAL'::text) AS streak_type,
    COALESCE(st.streak_count, (0)::bigint) AS streak_count,
    s.baseline_delta,
        CASE
            WHEN ((COALESCE(st.streak_type, 'NEUTRAL'::text) = 'HOT'::text) AND (COALESCE(st.streak_count, (0)::bigint) >= 3)) THEN 'HOT'::text
            WHEN ((COALESCE(st.streak_type, 'NEUTRAL'::text) = 'COLD'::text) AND (COALESCE(st.streak_count, (0)::bigint) >= 3)) THEN 'COLD'::text
            WHEN (s.baseline_delta >= 0.10) THEN 'ABOVE_BASELINE'::text
            WHEN (s.baseline_delta <= '-0.10'::numeric) THEN 'BELOW_BASELINE'::text
            ELSE 'NEUTRAL'::text
        END AS streak_context_label
   FROM (scored s
     LEFT JOIN streak st ON (((s.player_id = st.player_id) AND (s.prop_type = st.prop_type))))
  WITH NO DATA;


ALTER MATERIALIZED VIEW mlb.today_player_context OWNER TO postgres;

--
-- Name: today_wide_rows; Type: TABLE; Schema: mlb; Owner: postgres
--

CREATE TABLE mlb.today_wide_rows (
    slate_date date NOT NULL,
    game_date date NOT NULL,
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    player_name text NOT NULL,
    player_name_norm text NOT NULL,
    prop_type text NOT NULL,
    team text,
    opponent text,
    is_home boolean
);


ALTER TABLE mlb.today_wide_rows OWNER TO postgres;

--
-- Name: today_workspace_mlb; Type: MATERIALIZED VIEW; Schema: mlb; Owner: postgres
--

CREATE MATERIALIZED VIEW mlb.today_workspace_mlb AS
 WITH joined AS (
         SELECT ms.game_date,
            ms.game_id,
            ms.player_id,
            ms.player_name,
            ms.team,
            ms.opponent,
            ms.is_home,
            ms.prop_type,
            ms.line,
            ms.best_over_price,
            ms.best_under_price,
            ms.best_over_book,
            ms.best_under_book,
            ms.market_median_over_price,
            ms.market_median_under_price,
            ms.market_range_over,
            ms.market_range_under,
            ms.book_count_over,
            ms.book_count_under,
            ms.price_dispersion_over,
            ms.price_dispersion_under,
            ts.open_over_price,
            ts.open_under_price,
            ts.latest_over_price,
            ts.latest_under_price,
            ts.minutes_since_open,
            ts.num_snapshots,
            ts.over_price_change_from_open,
            ts.under_price_change_from_open,
            ts.over_span,
            ts.under_span,
            pc.streak_context_label,
            pc.consistency_score,
            pc.last_5_avg,
            pc.last_10_avg,
            pc.season_avg,
            pc.hit_rate_last_5,
            pc.hit_rate_last_10,
            pc.hit_rate_season,
            pc.streak_type,
            pc.streak_count,
            pc.baseline_delta
           FROM ((mlb.today_market_snapshot ms
             LEFT JOIN mlb.today_market_timing_signal ts ON (((ms.player_id = ts.player_id) AND (ms.game_id = ts.game_id) AND (ms.prop_type = ts.prop_type) AND (ms.line = ts.line))))
             LEFT JOIN mlb.today_player_context pc ON (((ms.player_id = pc.player_id) AND (ms.prop_type = pc.prop_type))))
        ), side_rows AS (
         SELECT j.game_date,
            j.game_id,
            j.player_id,
            j.player_name,
            j.team,
            j.opponent,
            j.is_home,
            j.prop_type,
            j.line,
            'OVER'::text AS side,
            j.best_over_price AS best_price,
            j.best_over_book AS best_price_book,
            j.market_median_over_price AS market_median,
            j.market_range_over AS market_range,
            j.open_over_price AS open_price,
            j.latest_over_price AS latest_price,
            j.over_price_change_from_open AS price_change_from_open,
            j.book_count_over AS book_count,
            j.price_dispersion_over AS price_dispersion,
            j.over_span AS intraday_span,
            j.minutes_since_open,
            j.num_snapshots,
            j.streak_context_label,
            j.consistency_score,
            j.last_5_avg,
            j.last_10_avg,
            j.season_avg,
            j.hit_rate_last_5,
            j.hit_rate_last_10,
            j.hit_rate_season,
            j.streak_type,
            j.streak_count,
            j.baseline_delta
           FROM joined j
        UNION ALL
         SELECT j.game_date,
            j.game_id,
            j.player_id,
            j.player_name,
            j.team,
            j.opponent,
            j.is_home,
            j.prop_type,
            j.line,
            'UNDER'::text AS side,
            j.best_under_price AS best_price,
            j.best_under_book AS best_price_book,
            j.market_median_under_price AS market_median,
            j.market_range_under AS market_range,
            j.open_under_price AS open_price,
            j.latest_under_price AS latest_price,
            j.under_price_change_from_open AS price_change_from_open,
            j.book_count_under AS book_count,
            j.price_dispersion_under AS price_dispersion,
            j.under_span AS intraday_span,
            j.minutes_since_open,
            j.num_snapshots,
            j.streak_context_label,
            j.consistency_score,
            j.last_5_avg,
            j.last_10_avg,
            j.season_avg,
            j.hit_rate_last_5,
            j.hit_rate_last_10,
            j.hit_rate_season,
            j.streak_type,
            j.streak_count,
            j.baseline_delta
           FROM joined j
        ), scored AS (
         SELECT s.game_date,
            s.game_id,
            s.player_id,
            s.player_name,
            s.team,
            s.opponent,
            s.is_home,
            s.prop_type,
            s.line,
            s.side,
            s.best_price,
            s.best_price_book,
            s.market_median,
            s.market_range,
            ((s.best_price)::double precision - s.market_median) AS value_vs_market,
            s.open_price,
            s.latest_price,
            s.minutes_since_open,
            s.num_snapshots,
            s.price_change_from_open,
            s.book_count,
            s.price_dispersion,
                CASE
                    WHEN ((s.best_price IS NULL) OR (s.market_median IS NULL)) THEN 'UNRELIABLE'::text
                    WHEN (COALESCE(s.book_count, (0)::bigint) < 2) THEN 'THIN'::text
                    WHEN (COALESCE(s.num_snapshots, (0)::bigint) <= 1) THEN 'LIMITED'::text
                    WHEN (s.market_range IS NULL) THEN 'LIMITED'::text
                    WHEN (s.market_range >= (120)::numeric) THEN 'LIMITED'::text
                    WHEN ((COALESCE(s.book_count, (0)::bigint) >= 4) AND (COALESCE(s.num_snapshots, (0)::bigint) >= 3) AND (s.market_range <= (40)::numeric)) THEN 'STRONG'::text
                    WHEN ((COALESCE(s.book_count, (0)::bigint) >= 3) AND (COALESCE(s.num_snapshots, (0)::bigint) >= 2) AND (s.market_range <= (80)::numeric)) THEN 'GOOD'::text
                    ELSE 'LIMITED'::text
                END AS coverage_quality_label,
                CASE
                    WHEN ((s.best_price IS NULL) OR (s.market_median IS NULL)) THEN 'No reliable median'::text
                    WHEN (COALESCE(s.book_count, (0)::bigint) < 2) THEN 'Few books available'::text
                    WHEN (COALESCE(s.num_snapshots, (0)::bigint) <= 1) THEN 'Sparse snapshot coverage'::text
                    WHEN (s.market_range IS NULL) THEN 'Incomplete market range'::text
                    WHEN (s.market_range >= (120)::numeric) THEN 'Wide market spread'::text
                    WHEN ((COALESCE(s.book_count, (0)::bigint) >= 4) AND (COALESCE(s.num_snapshots, (0)::bigint) >= 3) AND (s.market_range <= (40)::numeric)) THEN 'Median available across multiple books with tight range'::text
                    WHEN ((COALESCE(s.book_count, (0)::bigint) >= 3) AND (COALESCE(s.num_snapshots, (0)::bigint) >= 2) AND (s.market_range <= (80)::numeric)) THEN 'Median available with solid book and snapshot coverage'::text
                    ELSE 'Partial market coverage'::text
                END AS coverage_quality_reason,
                CASE
                    WHEN (COALESCE(s.intraday_span, (0)::double precision) >= (25)::double precision) THEN 'VOLATILE'::text
                    WHEN (COALESCE(s.price_change_from_open, (0)::double precision) >= (10)::double precision) THEN 'WAIT'::text
                    WHEN (COALESCE(s.price_change_from_open, (0)::double precision) <= ('-10'::integer)::double precision) THEN 'EARLY'::text
                    ELSE 'STABLE'::text
                END AS timing_signal,
                CASE
                    WHEN (COALESCE(s.intraday_span, (0)::double precision) >= (25)::double precision) THEN 'Large intraday movement'::text
                    WHEN (COALESCE(s.price_change_from_open, (0)::double precision) >= (10)::double precision) THEN 'Current price better than open'::text
                    WHEN (COALESCE(s.price_change_from_open, (0)::double precision) <= ('-10'::integer)::double precision) THEN 'Current price worse than open'::text
                    ELSE 'Little intraday movement'::text
                END AS timing_reason,
            s.streak_context_label,
            s.consistency_score,
            s.last_5_avg,
            s.last_10_avg,
            s.season_avg,
            s.hit_rate_last_5,
            s.hit_rate_last_10,
            s.hit_rate_season,
            s.streak_type,
            s.streak_count,
            s.baseline_delta
           FROM side_rows s
        )
 SELECT sc.game_date,
    sc.game_id,
    sc.player_id,
    sc.player_name,
    sc.team,
    sc.opponent,
    sc.is_home,
    sc.prop_type,
    sc.line,
    sc.side,
    sc.best_price,
    sc.best_price_book,
    sc.market_median,
    sc.market_range,
    sc.value_vs_market,
    sc.open_price,
    sc.latest_price,
    sc.minutes_since_open,
    sc.num_snapshots,
    sc.price_change_from_open,
    sc.book_count,
    sc.price_dispersion,
    sc.coverage_quality_label,
    sc.coverage_quality_reason,
    sc.timing_signal,
    sc.timing_reason,
    sc.streak_context_label,
    sc.consistency_score,
    sc.last_5_avg,
    sc.last_10_avg,
    sc.season_avg,
    sc.hit_rate_last_5,
    sc.hit_rate_last_10,
    sc.hit_rate_season,
    sc.streak_type,
    sc.streak_count,
    sc.baseline_delta,
        CASE
            WHEN (sc.value_vs_market IS NULL) THEN 'Neutral'::text
            WHEN ((abs(sc.value_vs_market) >= (40)::double precision) AND (sc.coverage_quality_label = ANY (ARRAY['GOOD'::text, 'STRONG'::text])) AND (sc.timing_signal = ANY (ARRAY['STABLE'::text, 'EARLY'::text]))) THEN 'Strong signal'::text
            WHEN ((abs(sc.value_vs_market) >= (25)::double precision) AND ((sc.timing_signal = 'VOLATILE'::text) OR (sc.coverage_quality_label = ANY (ARRAY['LIMITED'::text, 'THIN'::text])))) THEN 'Monitor'::text
            ELSE 'Neutral'::text
        END AS decision_label,
        CASE
            WHEN (sc.value_vs_market IS NULL) THEN 'Neutral: no clear market signal'::text
            WHEN ((abs(sc.value_vs_market) >= (40)::double precision) AND (sc.coverage_quality_label = ANY (ARRAY['GOOD'::text, 'STRONG'::text])) AND (sc.timing_signal = ANY (ARRAY['STABLE'::text, 'EARLY'::text]))) THEN 'Strong market signal: notable price gap with reliable/stable coverage'::text
            WHEN ((abs(sc.value_vs_market) >= (25)::double precision) AND (sc.timing_signal = 'VOLATILE'::text)) THEN 'Monitor: notable price gap, but market is volatile'::text
            WHEN ((abs(sc.value_vs_market) >= (25)::double precision) AND (sc.coverage_quality_label = ANY (ARRAY['LIMITED'::text, 'THIN'::text]))) THEN 'Monitor: notable price gap, but market coverage is limited'::text
            ELSE 'Neutral: no clear market signal'::text
        END AS decision_reason
   FROM scored sc
  WITH NO DATA;


ALTER MATERIALIZED VIEW mlb.today_workspace_mlb OWNER TO postgres;

--
-- Name: _points_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE UNLOGGED TABLE nhl._points_stage (
    player_id bigint,
    game_id bigint,
    game_date date,
    goals integer,
    assists integer
);


ALTER TABLE nhl._points_stage OWNER TO postgres;

--
-- Name: backfill_progress; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.backfill_progress (
    task text NOT NULL,
    last_game_id bigint,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE nhl.backfill_progress OWNER TO postgres;

--
-- Name: blocked_shot_events; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.blocked_shot_events (
    game_id bigint NOT NULL,
    event_id bigint NOT NULL,
    season integer NOT NULL,
    game_date date NOT NULL,
    period_number integer,
    time_in_period text,
    situation_code text,
    shot_type text,
    zone_code text,
    shooting_player_id bigint,
    shooting_team_id integer,
    shooter_position_bucket text,
    blocking_player_id bigint,
    blocking_team_id integer,
    blocker_position_bucket text,
    goalie_in_net_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.blocked_shot_events OWNER TO postgres;

--
-- Name: data_quality_audit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.data_quality_audit (
    audit_date date NOT NULL,
    check_name text NOT NULL,
    level text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT data_quality_audit_level_check CHECK ((level = ANY (ARRAY['info'::text, 'warn'::text, 'error'::text])))
);


ALTER TABLE nhl.data_quality_audit OWNER TO postgres;

--
-- Name: eval_sog_daily; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.eval_sog_daily (
    game_date date NOT NULL,
    model_family text DEFAULT ''::text NOT NULL,
    model_version text DEFAULT ''::text NOT NULL,
    line numeric(3,1) NOT NULL,
    segment_type text NOT NULL,
    segment_value text NOT NULL,
    n_pred integer NOT NULL,
    n_eval integer NOT NULL,
    n_pos integer NOT NULL,
    truth_coverage numeric(10,6) NOT NULL,
    games_on_date integer NOT NULL,
    skater_rows_date integer NOT NULL,
    is_low_sample boolean NOT NULL,
    hit_rate numeric(10,6),
    avg_p numeric(10,6),
    auc numeric(10,6),
    logloss numeric(10,6),
    brier numeric(10,6),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.eval_sog_daily OWNER TO postgres;

--
-- Name: game_manpower_segments; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.game_manpower_segments (
    game_id bigint NOT NULL,
    period integer NOT NULL,
    start_sec integer NOT NULL,
    end_sec integer NOT NULL,
    pp_team_id integer NOT NULL,
    pk_team_id integer NOT NULL,
    source text DEFAULT 'pbp'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.game_manpower_segments OWNER TO postgres;

--
-- Name: games; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.games (
    game_id bigint NOT NULL,
    season integer,
    short_game_id integer,
    game_type smallint,
    game_number integer,
    game_date date,
    start_time_utc timestamp with time zone,
    home_team_code text NOT NULL,
    away_team_code text NOT NULL,
    home_team_id bigint NOT NULL,
    away_team_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    status text,
    CONSTRAINT games_canonical_id_chk CHECK ((game_id = nhl.canonical_game_id(season, short_game_id))),
    CONSTRAINT games_game_type_chk CHECK ((game_type = ANY (ARRAY[1, 2, 3]))),
    CONSTRAINT games_season_start_year_chk CHECK (((season >= 1900) AND (season <= 2200))),
    CONSTRAINT games_short_consistency_chk CHECK ((short_game_id = ((game_type * 10000) + game_number)))
);


ALTER TABLE nhl.games OWNER TO postgres;

--
-- Name: games_season_audit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.games_season_audit (
    audit_id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    op text NOT NULL,
    game_id bigint NOT NULL,
    game_date date,
    season_old integer,
    season_new integer,
    db_user text DEFAULT CURRENT_USER NOT NULL,
    application_name text,
    client_addr inet,
    client_port integer,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.games_season_audit OWNER TO postgres;

--
-- Name: games_season_audit_audit_id_seq; Type: SEQUENCE; Schema: nhl; Owner: postgres
--

CREATE SEQUENCE nhl.games_season_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE nhl.games_season_audit_audit_id_seq OWNER TO postgres;

--
-- Name: games_season_audit_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: nhl; Owner: postgres
--

ALTER SEQUENCE nhl.games_season_audit_audit_id_seq OWNED BY nhl.games_season_audit.audit_id;


--
-- Name: games_write_audit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.games_write_audit (
    audit_id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    op text NOT NULL,
    game_id bigint,
    season_old integer,
    season_new integer,
    game_date_old date,
    game_date_new date,
    status_old text,
    status_new text,
    db_user text DEFAULT CURRENT_USER NOT NULL,
    application_name text,
    client_addr inet
);


ALTER TABLE nhl.games_write_audit OWNER TO postgres;

--
-- Name: games_write_audit_audit_id_seq; Type: SEQUENCE; Schema: nhl; Owner: postgres
--

CREATE SEQUENCE nhl.games_write_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE nhl.games_write_audit_audit_id_seq OWNER TO postgres;

--
-- Name: games_write_audit_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: nhl; Owner: postgres
--

ALTER SEQUENCE nhl.games_write_audit_audit_id_seq OWNED BY nhl.games_write_audit.audit_id;


--
-- Name: goalie_game_logs_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.goalie_game_logs_raw (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    opponent_id bigint NOT NULL,
    toi_minutes numeric(5,2),
    shots_faced smallint,
    saves smallint,
    goals_allowed smallint,
    start_flag boolean,
    pulled_flag boolean,
    is_home boolean DEFAULT false,
    start_prob numeric(3,2),
    game_date date,
    created_at timestamp with time zone DEFAULT now(),
    ev_shots_faced integer,
    pp_shots_faced integer,
    sh_shots_faced integer,
    high_danger_shots_faced integer,
    rebounds_allowed integer,
    CONSTRAINT goalie_game_logs_raw_goals_allowed_check CHECK ((goals_allowed >= 0)),
    CONSTRAINT goalie_game_logs_raw_saves_check CHECK ((saves >= 0)),
    CONSTRAINT goalie_game_logs_raw_shots_faced_check CHECK ((shots_faced >= 0)),
    CONSTRAINT goalie_game_logs_raw_toi_minutes_check CHECK ((toi_minutes >= (0)::numeric))
);


ALTER TABLE nhl.goalie_game_logs_raw OWNER TO postgres;

--
-- Name: v_goalie_game_logs_played; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_goalie_game_logs_played AS
 SELECT goalie_game_logs_raw.player_id,
    goalie_game_logs_raw.game_id,
    goalie_game_logs_raw.team_id,
    goalie_game_logs_raw.opponent_id,
    goalie_game_logs_raw.toi_minutes,
    goalie_game_logs_raw.shots_faced,
    goalie_game_logs_raw.saves,
    goalie_game_logs_raw.goals_allowed,
    goalie_game_logs_raw.start_flag,
    goalie_game_logs_raw.pulled_flag,
    goalie_game_logs_raw.is_home,
    goalie_game_logs_raw.start_prob,
    goalie_game_logs_raw.game_date,
    goalie_game_logs_raw.created_at
   FROM nhl.goalie_game_logs_raw
  WHERE (COALESCE(goalie_game_logs_raw.toi_minutes, (0)::numeric) > (0)::numeric);


ALTER VIEW nhl.v_goalie_game_logs_played OWNER TO postgres;

--
-- Name: goalie_roll_feats_m; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.goalie_roll_feats_m AS
 WITH gl AS (
         SELECT gl.player_id,
            gl.game_id,
            gl.team_id,
            gl.opponent_id,
            gl.is_home,
            gl.game_date,
            (COALESCE((gl.shots_faced)::integer, 0))::numeric AS sf,
            (COALESCE((gl.saves)::integer, 0))::numeric AS sv,
            COALESCE(NULLIF(gl.toi_minutes, (0)::numeric), (60)::numeric) AS toi
           FROM nhl.v_goalie_game_logs_played gl
        ), roll AS (
         SELECT gl.player_id,
            gl.game_id,
            gl.team_id,
            gl.opponent_id,
            gl.is_home,
            gl.game_date,
            sum(gl.sf) OVER w5 AS sf_d5,
            sum(gl.sv) OVER w5 AS sv_d5,
            sum(gl.toi) OVER w5 AS toi_d5,
            sum(gl.sf) OVER w10 AS sf_d10,
            sum(gl.sv) OVER w10 AS sv_d10,
            sum(gl.toi) OVER w10 AS toi_d10,
            sum(gl.sf) OVER w20 AS sf_d20,
            sum(gl.sv) OVER w20 AS sv_d20,
            sum(gl.toi) OVER w20 AS toi_d20
           FROM gl
          WINDOW w5 AS (PARTITION BY gl.player_id ORDER BY gl.game_date, gl.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w10 AS (PARTITION BY gl.player_id ORDER BY gl.game_date, gl.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), w20 AS (PARTITION BY gl.player_id ORDER BY gl.game_date, gl.game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
        )
 SELECT roll.player_id,
    roll.game_id,
    roll.team_id,
    roll.opponent_id,
    roll.is_home,
    roll.game_date,
        CASE
            WHEN (roll.toi_d5 > (0)::numeric) THEN (((60)::numeric * roll.sf_d5) / roll.toi_d5)
            ELSE NULL::numeric
        END AS d5_shots_faced_per60,
        CASE
            WHEN (roll.toi_d5 > (0)::numeric) THEN (((60)::numeric * roll.sv_d5) / roll.toi_d5)
            ELSE NULL::numeric
        END AS d5_saves_per60,
        CASE
            WHEN (roll.toi_d10 > (0)::numeric) THEN (((60)::numeric * roll.sf_d10) / roll.toi_d10)
            ELSE NULL::numeric
        END AS d10_shots_faced_per60,
        CASE
            WHEN (roll.toi_d10 > (0)::numeric) THEN (((60)::numeric * roll.sv_d10) / roll.toi_d10)
            ELSE NULL::numeric
        END AS d10_saves_per60,
        CASE
            WHEN (roll.sf_d10 > (0)::numeric) THEN (roll.sv_d10 / roll.sf_d10)
            ELSE NULL::numeric
        END AS d10_save_pct,
        CASE
            WHEN (roll.toi_d20 > (0)::numeric) THEN (((60)::numeric * roll.sv_d20) / roll.toi_d20)
            ELSE NULL::numeric
        END AS d20_saves_per60
   FROM roll
  ORDER BY roll.game_date, roll.player_id, roll.game_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.goalie_roll_feats_m OWNER TO postgres;

--
-- Name: goalie_rolling_agg; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.goalie_rolling_agg (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    d5_saves_per60 numeric(6,3),
    d10_saves_per60 numeric(6,3),
    d20_saves_per60 numeric(6,3),
    d5_shots_faced_per60 numeric(6,3),
    season_save_pct numeric(5,3),
    rest_days smallint,
    b2b_flag boolean,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    d10_shots_faced_per60 numeric,
    d10_save_pct numeric
);


ALTER TABLE nhl.goalie_rolling_agg OWNER TO postgres;

--
-- Name: goalies2023_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.goalies2023_stage (
    playerid text,
    season text,
    name text,
    team text,
    catches text,
    raw_6 text,
    raw_7 text,
    raw_8 text,
    raw_9 text,
    raw_10 text,
    raw_11 text,
    raw_12 text,
    raw_13 text,
    raw_14 text,
    raw_15 text,
    raw_16 text,
    raw_17 text,
    raw_18 text,
    raw_19 text,
    raw_20 text,
    raw_21 text,
    raw_22 text,
    raw_23 text,
    raw_24 text,
    raw_25 text,
    raw_26 text,
    raw_27 text,
    raw_28 text,
    raw_29 text,
    raw_30 text,
    raw_31 text,
    raw_32 text,
    raw_33 text,
    raw_34 text,
    raw_35 text,
    raw_36 text
);


ALTER TABLE nhl.goalies2023_stage OWNER TO postgres;

--
-- Name: goalies2023_stage_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.goalies2023_stage_raw (
    c1 text,
    c2 text,
    c3 text,
    c4 text,
    c5 text,
    c6 text,
    c7 text,
    c8 text,
    c9 text,
    c10 text,
    c11 text,
    c12 text,
    c13 text,
    c14 text,
    c15 text,
    c16 text,
    c17 text,
    c18 text,
    c19 text,
    c20 text,
    c21 text,
    c22 text,
    c23 text,
    c24 text,
    c25 text,
    c26 text,
    c27 text,
    c28 text,
    c29 text,
    c30 text,
    c31 text,
    c32 text,
    c33 text,
    c34 text,
    c35 text,
    c36 text
);


ALTER TABLE nhl.goalies2023_stage_raw OWNER TO postgres;

--
-- Name: goalies_szn_sit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.goalies_szn_sit (
    player_id bigint,
    season integer,
    goalie_name text,
    team_abbr text,
    "position" text,
    situation text,
    games_played integer,
    icetime_s numeric,
    xgoals numeric,
    goals integer,
    unblocked_shot_attempts integer,
    xrebounds numeric,
    rebounds integer,
    xfreeze numeric,
    freeze_ct integer,
    x_on_goal numeric,
    on_goal integer,
    x_play_stopped numeric,
    play_stopped integer,
    x_play_continued_in_zone numeric,
    play_continued_in_zone integer,
    x_play_continued_out_zone numeric,
    play_continued_out_zone integer,
    flurry_adjusted_xgoals numeric,
    low_danger_shots integer,
    medium_danger_shots integer,
    high_danger_shots integer,
    low_danger_xgoals numeric,
    medium_danger_xgoals numeric,
    high_danger_xgoals numeric,
    low_danger_goals integer,
    medium_danger_goals integer,
    high_danger_goals integer,
    blocked_shot_attempts integer,
    pimin integer,
    penalties integer,
    _raw jsonb
);


ALTER TABLE nhl.goalies_szn_sit OWNER TO postgres;

--
-- Name: import_goalie_logs_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.import_goalie_logs_stage (
    player_id integer,
    game_id bigint,
    team_id integer,
    opponent_id integer,
    is_home boolean,
    shots_faced integer,
    saves integer,
    goals_allowed integer,
    toi_minutes numeric(6,2),
    start_prob numeric(3,2),
    game_date date,
    ev_shots_faced integer,
    pp_shots_faced integer,
    sh_shots_faced integer,
    high_danger_shots_faced integer,
    rebounds_allowed integer
);


ALTER TABLE nhl.import_goalie_logs_stage OWNER TO postgres;

--
-- Name: import_player_external_ids_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.import_player_external_ids_stage (
    player_id integer,
    provider text,
    provider_player_id text
);


ALTER TABLE nhl.import_player_external_ids_stage OWNER TO postgres;

--
-- Name: import_players_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.import_players_stage (
    player_id integer,
    team_id integer,
    first_name text,
    last_name text,
    "position" text,
    shoots_catches text,
    active boolean
);


ALTER TABLE nhl.import_players_stage OWNER TO postgres;

--
-- Name: import_skater_logs_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.import_skater_logs_stage (
    player_id integer,
    game_id bigint,
    team_id integer,
    opponent_id integer,
    is_home boolean,
    shots_on_goal integer,
    shot_attempts integer,
    toi_minutes numeric(6,2),
    pp_toi_minutes numeric(6,2),
    game_date date,
    fenwick_for integer,
    missed_shots integer,
    blocked_shots_taken integer,
    rebounds_for integer,
    hits integer,
    takeaways integer,
    giveaways integer,
    penalties_drawn integer,
    penalties_taken integer,
    ev_shot_attempts integer,
    pp_shot_attempts integer,
    sh_shot_attempts integer,
    ev_sog integer,
    pp_sog integer,
    sh_sog integer,
    goals integer,
    assists integer,
    blocks integer
);


ALTER TABLE nhl.import_skater_logs_stage OWNER TO postgres;

--
-- Name: import_skater_points_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.import_skater_points_stage (
    player_id bigint,
    game_id bigint,
    team_id bigint,
    opponent_id bigint,
    is_home boolean,
    game_date date,
    goals integer,
    assists integer,
    points integer,
    toi_minutes numeric,
    pp_toi_minutes numeric,
    source text,
    ingested_at timestamp with time zone DEFAULT now()
);


ALTER TABLE nhl.import_skater_points_stage OWNER TO postgres;

--
-- Name: keep_games_filter; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.keep_games_filter (
    game_id bigint NOT NULL
);


ALTER TABLE nhl.keep_games_filter OWNER TO postgres;

--
-- Name: lines_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.lines_szn_sit_denali (
    "lineId" numeric,
    season integer,
    name text,
    team text,
    "position" text,
    situation text,
    games_played numeric,
    icetime numeric,
    "iceTimeRank" numeric,
    "xGoalsPercentage" numeric,
    "corsiPercentage" numeric,
    "fenwickPercentage" numeric,
    "xOnGoalFor" numeric,
    "xGoalsFor" numeric,
    "xReboundsFor" numeric,
    "xFreezeFor" numeric,
    "xPlayStoppedFor" numeric,
    "xPlayContinuedInZoneFor" numeric,
    "xPlayContinuedOutsideZoneFor" numeric,
    "flurryAdjustedxGoalsFor" numeric,
    "scoreVenueAdjustedxGoalsFor" numeric,
    "flurryScoreVenueAdjustedxGoalsFor" numeric,
    "shotsOnGoalFor" numeric,
    "missedShotsFor" numeric,
    "blockedShotAttemptsFor" numeric,
    "shotAttemptsFor" numeric,
    "goalsFor" numeric,
    "reboundsFor" numeric,
    "reboundGoalsFor" numeric,
    "freezeFor" numeric,
    "playStoppedFor" numeric,
    "playContinuedInZoneFor" numeric,
    "playContinuedOutsideZoneFor" numeric,
    "savedShotsOnGoalFor" numeric,
    "savedUnblockedShotAttemptsFor" numeric,
    "penaltiesFor" numeric,
    "penalityMinutesFor" numeric,
    "faceOffsWonFor" numeric,
    "hitsFor" numeric,
    "takeawaysFor" numeric,
    "giveawaysFor" numeric,
    "lowDangerShotsFor" numeric,
    "mediumDangerShotsFor" numeric,
    "highDangerShotsFor" numeric,
    "lowDangerxGoalsFor" numeric,
    "mediumDangerxGoalsFor" numeric,
    "highDangerxGoalsFor" numeric,
    "lowDangerGoalsFor" numeric,
    "mediumDangerGoalsFor" numeric,
    "highDangerGoalsFor" numeric,
    "scoreAdjustedShotsAttemptsFor" numeric,
    "unblockedShotAttemptsFor" numeric,
    "scoreAdjustedUnblockedShotAttemptsFor" numeric,
    "dZoneGiveawaysFor" numeric,
    "xGoalsFromxReboundsOfShotsFor" numeric,
    "xGoalsFromActualReboundsOfShotsFor" numeric,
    "reboundxGoalsFor" numeric,
    "totalShotCreditFor" numeric,
    "scoreAdjustedTotalShotCreditFor" numeric,
    "scoreFlurryAdjustedTotalShotCreditFor" numeric,
    "xOnGoalAgainst" numeric,
    "xGoalsAgainst" numeric,
    "xReboundsAgainst" numeric,
    "xFreezeAgainst" numeric,
    "xPlayStoppedAgainst" numeric,
    "xPlayContinuedInZoneAgainst" numeric,
    "xPlayContinuedOutsideZoneAgainst" numeric,
    "flurryAdjustedxGoalsAgainst" numeric,
    "scoreVenueAdjustedxGoalsAgainst" numeric,
    "flurryScoreVenueAdjustedxGoalsAgainst" numeric,
    "shotsOnGoalAgainst" numeric,
    "missedShotsAgainst" numeric,
    "blockedShotAttemptsAgainst" numeric,
    "shotAttemptsAgainst" numeric,
    "goalsAgainst" numeric,
    "reboundsAgainst" numeric,
    "reboundGoalsAgainst" numeric,
    "freezeAgainst" numeric,
    "playStoppedAgainst" numeric,
    "playContinuedInZoneAgainst" numeric,
    "playContinuedOutsideZoneAgainst" numeric,
    "savedShotsOnGoalAgainst" numeric,
    "savedUnblockedShotAttemptsAgainst" numeric,
    "penaltiesAgainst" numeric,
    "penalityMinutesAgainst" numeric,
    "faceOffsWonAgainst" numeric,
    "hitsAgainst" numeric,
    "takeawaysAgainst" numeric,
    "giveawaysAgainst" numeric,
    "lowDangerShotsAgainst" numeric,
    "mediumDangerShotsAgainst" numeric,
    "highDangerShotsAgainst" numeric,
    "lowDangerxGoalsAgainst" numeric,
    "mediumDangerxGoalsAgainst" numeric,
    "highDangerxGoalsAgainst" numeric,
    "lowDangerGoalsAgainst" numeric,
    "mediumDangerGoalsAgainst" numeric,
    "highDangerGoalsAgainst" numeric,
    "scoreAdjustedShotsAttemptsAgainst" numeric,
    "unblockedShotAttemptsAgainst" numeric,
    "scoreAdjustedUnblockedShotAttemptsAgainst" numeric,
    "dZoneGiveawaysAgainst" numeric,
    "xGoalsFromxReboundsOfShotsAgainst" numeric,
    "xGoalsFromActualReboundsOfShotsAgainst" numeric,
    "reboundxGoalsAgainst" numeric,
    "totalShotCreditAgainst" numeric,
    "scoreAdjustedTotalShotCreditAgainst" numeric,
    "scoreFlurryAdjustedTotalShotCreditAgainst" numeric
);


ALTER TABLE nhl.lines_szn_sit_denali OWNER TO postgres;

--
-- Name: pairing_features_store_v2; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.pairing_features_store_v2 (
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id integer NOT NULL,
    toi_sec integer NOT NULL,
    shiftcharts_available integer NOT NULL,
    top_mate_overlap_sec integer NOT NULL,
    top3_mates_overlap_sec integer NOT NULL,
    top_mate_overlap_share numeric,
    top3_mates_overlap_share numeric,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.pairing_features_store_v2 OWNER TO postgres;

--
-- Name: shift_teammate_overlap_game_recent_v2; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shift_teammate_overlap_game_recent_v2 (
    game_id bigint NOT NULL,
    team_id integer NOT NULL,
    player_id bigint NOT NULL,
    teammate_id bigint NOT NULL,
    toi_sec integer NOT NULL,
    overlap_sec integer NOT NULL,
    overlap_share numeric
);


ALTER TABLE nhl.shift_teammate_overlap_game_recent_v2 OWNER TO postgres;

--
-- Name: shiftcharts_shifts; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shiftcharts_shifts (
    game_id bigint NOT NULL,
    shift_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id integer NOT NULL,
    period integer NOT NULL,
    start_time text,
    end_time text,
    duration text,
    start_sec integer NOT NULL,
    end_sec integer NOT NULL,
    dur_sec integer NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.shiftcharts_shifts OWNER TO postgres;

--
-- Name: shiftcharts_shifts_clean_v2; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shiftcharts_shifts_clean_v2 AS
 WITH base AS (
         SELECT s.game_id,
            s.shift_id,
            s.player_id,
            s.team_id,
            s.period,
            ((GREATEST(s.period, 1) - 1) * 1200) AS period_offset,
            s.start_sec AS start_sec_period,
            s.end_sec AS end_sec_period,
            (((GREATEST(s.period, 1) - 1) * 1200) + s.start_sec) AS start_sec_game,
            (((GREATEST(s.period, 1) - 1) * 1200) + s.end_sec) AS end_sec_game,
            COALESCE(NULLIF(s.dur_sec, 0), ((((GREATEST(s.period, 1) - 1) * 1200) + s.end_sec) - (((GREATEST(s.period, 1) - 1) * 1200) + s.start_sec))) AS dur_sec
           FROM nhl.shiftcharts_shifts s
        ), clean AS (
         SELECT base.game_id,
            base.shift_id,
            base.player_id,
            base.team_id,
            base.period,
            base.period_offset,
            base.start_sec_period,
            base.end_sec_period,
            base.start_sec_game,
            base.end_sec_game,
            base.dur_sec
           FROM base
          WHERE ((base.start_sec_game IS NOT NULL) AND (base.end_sec_game IS NOT NULL) AND (base.end_sec_game > base.start_sec_game) AND (base.dur_sec IS NOT NULL) AND (base.dur_sec = (base.end_sec_game - base.start_sec_game)) AND ((base.dur_sec >= 1) AND (base.dur_sec <= 599)))
        )
 SELECT clean.game_id,
    clean.shift_id,
    clean.player_id,
    clean.team_id,
    clean.period,
    clean.period_offset,
    clean.start_sec_period,
    clean.end_sec_period,
    clean.start_sec_game,
    clean.end_sec_game,
    clean.dur_sec
   FROM clean;


ALTER VIEW nhl.shiftcharts_shifts_clean_v2 OWNER TO postgres;

--
-- Name: pairing_features_v2; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.pairing_features_v2 AS
 WITH toi AS (
         SELECT shiftcharts_shifts_clean_v2.game_id,
            shiftcharts_shifts_clean_v2.player_id,
            shiftcharts_shifts_clean_v2.team_id,
            (sum(shiftcharts_shifts_clean_v2.dur_sec))::integer AS toi_sec
           FROM nhl.shiftcharts_shifts_clean_v2
          GROUP BY shiftcharts_shifts_clean_v2.game_id, shiftcharts_shifts_clean_v2.player_id, shiftcharts_shifts_clean_v2.team_id
        ), ov AS (
         SELECT shift_teammate_overlap_game_recent_v2.game_id,
            shift_teammate_overlap_game_recent_v2.player_id,
            shift_teammate_overlap_game_recent_v2.team_id,
            shift_teammate_overlap_game_recent_v2.teammate_id,
            shift_teammate_overlap_game_recent_v2.overlap_sec
           FROM nhl.shift_teammate_overlap_game_recent_v2
        ), ranked AS (
         SELECT o.game_id,
            o.player_id,
            o.team_id,
            o.teammate_id,
            o.overlap_sec,
            row_number() OVER (PARTITION BY o.game_id, o.team_id, o.player_id ORDER BY o.overlap_sec DESC, o.teammate_id) AS rn
           FROM ov o
        ), topk AS (
         SELECT ranked.game_id,
            ranked.team_id,
            ranked.player_id,
            max(
                CASE
                    WHEN (ranked.rn = 1) THEN ranked.overlap_sec
                    ELSE NULL::integer
                END) AS top_mate_overlap_sec,
            (sum(
                CASE
                    WHEN (ranked.rn <= 3) THEN ranked.overlap_sec
                    ELSE 0
                END))::integer AS top3_mates_overlap_sec
           FROM ranked
          GROUP BY ranked.game_id, ranked.team_id, ranked.player_id
        )
 SELECT t.game_id,
    t.player_id,
    t.toi_sec,
        CASE
            WHEN (t.toi_sec > 0) THEN 1
            ELSE 0
        END AS shiftcharts_available,
    COALESCE(k.top_mate_overlap_sec, 0) AS top_mate_overlap_sec,
    COALESCE(k.top3_mates_overlap_sec, 0) AS top3_mates_overlap_sec,
    ((COALESCE(k.top_mate_overlap_sec, 0))::numeric / (NULLIF(t.toi_sec, 0))::numeric) AS top_mate_overlap_share,
    ((COALESCE(k.top3_mates_overlap_sec, 0))::numeric / (NULLIF(t.toi_sec, 0))::numeric) AS top3_mates_overlap_share
   FROM (toi t
     LEFT JOIN topk k ON (((k.game_id = t.game_id) AND (k.team_id = t.team_id) AND (k.player_id = t.player_id))));


ALTER VIEW nhl.pairing_features_v2 OWNER TO postgres;

--
-- Name: player_external_ids; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_external_ids (
    player_id integer NOT NULL,
    provider text NOT NULL,
    provider_player_id text NOT NULL
);


ALTER TABLE nhl.player_external_ids OWNER TO postgres;

--
-- Name: player_game_2023_roll; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_game_2023_roll (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    num_event_shot bigint,
    num_event_miss bigint,
    num_event_goal bigint,
    num_goal_flag bigint,
    num_shotwasongoal_flag bigint,
    num_event_shot_last5 numeric,
    num_event_goal_last5 numeric,
    num_shotwasongoal_last5 numeric,
    num_event_shot_last10 numeric,
    num_event_goal_last10 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_event_goal_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric
);


ALTER TABLE nhl.player_game_2023_roll OWNER TO postgres;

--
-- Name: player_game_2023_summary; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_game_2023_summary (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    num_event_shot bigint,
    num_event_miss bigint,
    num_event_goal bigint,
    num_goal_flag bigint,
    num_shotwasongoal_flag bigint
);


ALTER TABLE nhl.player_game_2023_summary OWNER TO postgres;

--
-- Name: player_game_2024_roll; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_game_2024_roll (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    num_event_shot bigint,
    num_event_miss bigint,
    num_event_goal bigint,
    num_goal_flag bigint,
    num_shotwasongoal_flag bigint,
    num_event_shot_last5 numeric,
    num_event_goal_last5 numeric,
    num_shotwasongoal_last5 numeric,
    num_event_shot_last10 numeric,
    num_event_goal_last10 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_event_goal_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric
);


ALTER TABLE nhl.player_game_2024_roll OWNER TO postgres;

--
-- Name: player_game_2024_summary; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_game_2024_summary (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    num_event_shot bigint,
    num_event_miss bigint,
    num_event_goal bigint,
    num_goal_flag bigint,
    num_shotwasongoal_flag bigint
);


ALTER TABLE nhl.player_game_2024_summary OWNER TO postgres;

--
-- Name: player_game_shots_2023; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.player_game_shots_2023 (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    sog bigint,
    goals bigint
);


ALTER TABLE nhl.player_game_shots_2023 OWNER TO postgres;

--
-- Name: shots_all; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shots_all (
    game_id bigint NOT NULL,
    season integer NOT NULL,
    shotid text,
    hometeamcode text NOT NULL,
    awayteamcode text NOT NULL,
    teamcode text NOT NULL,
    ishometeam boolean NOT NULL,
    shooterplayerid bigint,
    shootername text,
    goalieidforshot bigint,
    goalienameforshot text,
    period integer,
    "time" text,
    xcord double precision,
    ycord double precision,
    shottype text,
    event text NOT NULL,
    goal integer,
    shotwasongoal integer
);


ALTER TABLE nhl.shots_all OWNER TO postgres;

--
-- Name: player_shot_history_denali; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.player_shot_history_denali AS
 WITH per_game AS (
         SELECT s.season,
            s.game_id,
            g.game_date,
            s.shooterplayerid AS player_id,
            s.teamcode,
                CASE
                    WHEN s.ishometeam THEN s.awayteamcode
                    ELSE s.hometeamcode
                END AS opponent_code,
            count(*) AS num_event_shot,
            count(*) FILTER (WHERE (s.shotwasongoal = 1)) AS num_shotwasongoal
           FROM (nhl.shots_all s
             JOIN nhl.games g ON ((g.game_id = s.game_id)))
          WHERE (s.shooterplayerid IS NOT NULL)
          GROUP BY s.season, s.game_id, g.game_date, s.shooterplayerid, s.teamcode,
                CASE
                    WHEN s.ishometeam THEN s.awayteamcode
                    ELSE s.hometeamcode
                END
        ), ordered AS (
         SELECT per_game.season,
            per_game.game_id,
            per_game.game_date,
            per_game.player_id,
            per_game.teamcode,
            per_game.opponent_code,
            per_game.num_event_shot,
            per_game.num_shotwasongoal
           FROM per_game
        )
 SELECT ordered.season,
    ordered.game_id,
    ordered.player_id,
    ordered.game_date,
    ordered.teamcode,
    ordered.opponent_code,
    COALESCE(sum(ordered.num_shotwasongoal) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_shotwasongoal_last5,
    COALESCE(sum(ordered.num_shotwasongoal) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_shotwasongoal_last10,
    COALESCE(sum(ordered.num_shotwasongoal) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_shotwasongoal_season_to_date,
    COALESCE(sum(ordered.num_event_shot) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_event_shot_last5,
    COALESCE(sum(ordered.num_event_shot) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_event_shot_last10,
    COALESCE(sum(ordered.num_event_shot) OVER (PARTITION BY ordered.season, ordered.player_id ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), (0)::numeric) AS num_event_shot_season_to_date
   FROM ordered;


ALTER VIEW nhl.player_shot_history_denali OWNER TO postgres;

--
-- Name: team_shot_history_denali; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.team_shot_history_denali AS
 WITH per_game_team AS (
         SELECT s.season,
            s.game_id,
            g.game_date,
            s.teamcode,
                CASE
                    WHEN s.ishometeam THEN s.awayteamcode
                    ELSE s.hometeamcode
                END AS opponent_code,
            count(*) AS team_num_event_shot,
            count(*) FILTER (WHERE (s.shotwasongoal = 1)) AS team_num_shotwasongoal
           FROM (nhl.shots_all s
             JOIN nhl.games g ON ((g.game_id = s.game_id)))
          GROUP BY s.season, s.game_id, g.game_date, s.teamcode,
                CASE
                    WHEN s.ishometeam THEN s.awayteamcode
                    ELSE s.hometeamcode
                END
        ), ordered AS (
         SELECT per_game_team.season,
            per_game_team.game_id,
            per_game_team.game_date,
            per_game_team.teamcode,
            per_game_team.opponent_code,
            per_game_team.team_num_event_shot,
            per_game_team.team_num_shotwasongoal
           FROM per_game_team
        )
 SELECT ordered.season,
    ordered.game_id,
    ordered.game_date,
    ordered.teamcode,
    ordered.opponent_code,
    ordered.team_num_event_shot,
    ordered.team_num_shotwasongoal,
    COALESCE(sum(ordered.team_num_event_shot) OVER (PARTITION BY ordered.season, ordered.teamcode ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), (0)::numeric) AS team_num_event_shot_for_last10,
    COALESCE(sum(ordered.team_num_shotwasongoal) OVER (PARTITION BY ordered.season, ordered.teamcode ORDER BY ordered.game_date, ordered.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), (0)::numeric) AS team_num_shotwasongoal_for_last10
   FROM ordered;


ALTER VIEW nhl.team_shot_history_denali OWNER TO postgres;

--
-- Name: player_shot_phoenix_denali; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.player_shot_phoenix_denali AS
 SELECT p.season,
    p.game_id,
    p.player_id,
    p.game_date,
    p.teamcode,
    p.opponent_code,
    p.num_shotwasongoal_last5,
    p.num_shotwasongoal_last10,
    p.num_shotwasongoal_season_to_date,
    p.num_event_shot_last5,
    p.num_event_shot_last10,
    p.num_event_shot_season_to_date,
    t.team_num_event_shot_for_last10,
    t.team_num_shotwasongoal_for_last10,
        CASE
            WHEN (t.team_num_shotwasongoal_for_last10 > (0)::numeric) THEN (p.num_shotwasongoal_last10 / t.team_num_shotwasongoal_for_last10)
            ELSE (0)::numeric
        END AS last10_team_sog_share,
        CASE
            WHEN ((p.num_shotwasongoal_last5 / 5.0) >= 3.0) THEN 1
            ELSE 0
        END AS hot_last5_flag
   FROM (nhl.player_shot_history_denali p
     LEFT JOIN nhl.team_shot_history_denali t ON (((t.season = p.season) AND (t.game_id = p.game_id) AND (t.teamcode = p.teamcode))));


ALTER VIEW nhl.player_shot_phoenix_denali OWNER TO postgres;

--
-- Name: skater_game_logs_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skater_game_logs_raw (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    opponent_id bigint NOT NULL,
    toi_minutes numeric(5,2),
    pp_toi_minutes numeric(5,2),
    shots_on_goal smallint,
    shot_attempts smallint,
    ixg numeric(6,3),
    blocks smallint,
    hits smallint,
    penalties smallint,
    game_state_splits jsonb,
    is_home boolean DEFAULT false,
    game_date date,
    created_at timestamp with time zone DEFAULT now(),
    fenwick_for integer,
    missed_shots integer,
    blocked_shots_taken integer,
    rebounds_for integer,
    takeaways integer,
    giveaways integer,
    penalties_drawn integer,
    penalties_taken integer,
    ev_shot_attempts integer,
    pp_shot_attempts integer,
    sh_shot_attempts integer,
    ev_sog integer,
    pp_sog integer,
    sh_sog integer,
    goals smallint,
    assists smallint,
    points integer,
    CONSTRAINT skater_game_logs_raw_blocks_check CHECK ((blocks >= 0)),
    CONSTRAINT skater_game_logs_raw_hits_check CHECK ((hits >= 0)),
    CONSTRAINT skater_game_logs_raw_penalties_check CHECK ((penalties >= 0)),
    CONSTRAINT skater_game_logs_raw_pp_toi_minutes_check CHECK ((pp_toi_minutes >= (0)::numeric)),
    CONSTRAINT skater_game_logs_raw_shot_attempts_check CHECK ((shot_attempts >= 0)),
    CONSTRAINT skater_game_logs_raw_shots_on_goal_check CHECK ((shots_on_goal >= 0)),
    CONSTRAINT skater_game_logs_raw_toi_minutes_check CHECK ((toi_minutes >= (0)::numeric))
);


ALTER TABLE nhl.skater_game_logs_raw OWNER TO postgres;

--
-- Name: player_sog_denali_base; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.player_sog_denali_base AS
 SELECT h.season,
    h.game_id,
    h.player_id,
    COALESCE(l.game_date, h.game_date) AS game_date,
    l.team_id,
    l.opponent_id,
    l.is_home,
    (COALESCE((l.shots_on_goal)::integer, 0))::numeric AS shots_on_goal,
    (COALESCE((l.shot_attempts)::integer, 0))::numeric AS shot_attempts,
    (NULLIF(l.toi_minutes, (0)::numeric))::numeric AS toi_minutes,
    (NULLIF(l.pp_toi_minutes, (0)::numeric))::numeric AS pp_toi_minutes,
    h.num_shotwasongoal_last5,
    h.num_shotwasongoal_last10,
    h.num_shotwasongoal_season_to_date,
    h.num_event_shot_last5,
    h.num_event_shot_last10,
    h.num_event_shot_season_to_date,
    h.teamcode,
    h.opponent_code
   FROM (nhl.player_shot_history_denali h
     LEFT JOIN nhl.skater_game_logs_raw l ON (((l.player_id = h.player_id) AND (l.game_id = h.game_id))));


ALTER VIEW nhl.player_sog_denali_base OWNER TO postgres;

--
-- Name: players; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.players (
    player_id bigint NOT NULL,
    full_name text NOT NULL,
    current_team_id bigint,
    "position" text NOT NULL,
    shoots_catches text,
    status text DEFAULT 'active'::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    team_id integer,
    first_name text,
    last_name text,
    active boolean DEFAULT true,
    CONSTRAINT players_no_placeholder_fullname CHECK (((full_name IS NULL) OR (full_name !~* '^(player|unknown)\s*#?\s*\d+$'::text))),
    CONSTRAINT players_position_check CHECK (("position" = ANY (ARRAY['F'::text, 'D'::text, 'G'::text]))),
    CONSTRAINT players_shoots_catches_check CHECK ((shoots_catches = ANY (ARRAY['L'::text, 'R'::text])))
);


ALTER TABLE nhl.players OWNER TO postgres;

--
-- Name: skater_points_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skater_points_raw (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint,
    opponent_id bigint,
    is_home boolean,
    game_date date,
    goals integer,
    assists integer,
    points integer,
    toi_minutes numeric,
    pp_toi_minutes numeric,
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT skater_points_points_chk CHECK (((points IS NULL) OR (goals IS NULL) OR (assists IS NULL) OR (points = (goals + assists))))
);


ALTER TABLE nhl.skater_points_raw OWNER TO postgres;

--
-- Name: training_features_sog_player_game; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_sog_player_game (
    season integer,
    game_id bigint,
    teamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    y_sog integer,
    shots_attempts_game bigint,
    sog_game bigint,
    num_event_shot_last5 numeric,
    num_event_shot_last10 numeric,
    num_shotwasongoal_last5 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric,
    team_num_event_shot_for_last10 numeric,
    team_num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.training_features_sog_player_game OWNER TO postgres;

--
-- Name: training_features_sog_player_game_v2; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.training_features_sog_player_game_v2 AS
 WITH base AS (
         SELECT training_features_sog_player_game.season,
            training_features_sog_player_game.game_id,
            training_features_sog_player_game.teamcode,
            training_features_sog_player_game.ishometeam,
            training_features_sog_player_game.shooterplayerid,
            training_features_sog_player_game.y_sog,
            training_features_sog_player_game.shots_attempts_game,
            training_features_sog_player_game.sog_game,
            training_features_sog_player_game.num_event_shot_last5,
            training_features_sog_player_game.num_event_shot_last10,
            training_features_sog_player_game.num_shotwasongoal_last5,
            training_features_sog_player_game.num_shotwasongoal_last10,
            training_features_sog_player_game.num_event_shot_season_to_date,
            training_features_sog_player_game.num_shotwasongoal_season_to_date,
            training_features_sog_player_game.team_num_event_shot_for_last10,
            training_features_sog_player_game.team_num_shotwasongoal_for_last10
           FROM nhl.training_features_sog_player_game
        ), with_counts AS (
         SELECT b.season,
            b.game_id,
            b.teamcode,
            b.ishometeam,
            b.shooterplayerid,
            b.y_sog,
            b.shots_attempts_game,
            b.sog_game,
            b.num_event_shot_last5,
            b.num_event_shot_last10,
            b.num_shotwasongoal_last5,
            b.num_shotwasongoal_last10,
            b.num_event_shot_season_to_date,
            b.num_shotwasongoal_season_to_date,
            b.team_num_event_shot_for_last10,
            b.team_num_shotwasongoal_for_last10,
            count(*) OVER (PARTITION BY b.season, b.shooterplayerid ORDER BY b.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS gp_last5,
            count(*) OVER (PARTITION BY b.season, b.shooterplayerid ORDER BY b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS gp_last10
           FROM base b
        )
 SELECT with_counts.season,
    with_counts.game_id,
    with_counts.teamcode,
    with_counts.ishometeam,
    with_counts.shooterplayerid,
    with_counts.y_sog,
    with_counts.num_shotwasongoal_last5,
    with_counts.num_shotwasongoal_last10,
    with_counts.num_shotwasongoal_season_to_date,
    with_counts.num_event_shot_last5,
    with_counts.num_event_shot_last10,
    with_counts.num_event_shot_season_to_date,
    with_counts.team_num_event_shot_for_last10,
    with_counts.team_num_shotwasongoal_for_last10,
        CASE
            WHEN (with_counts.gp_last5 > 0) THEN ((with_counts.num_shotwasongoal_last5)::double precision / (with_counts.gp_last5)::double precision)
            ELSE NULL::double precision
        END AS last5_sog_per_game,
        CASE
            WHEN (with_counts.gp_last10 > 0) THEN ((with_counts.num_shotwasongoal_last10)::double precision / (with_counts.gp_last10)::double precision)
            ELSE NULL::double precision
        END AS last10_sog_per_game,
        CASE
            WHEN (with_counts.gp_last10 > 0) THEN ((with_counts.num_event_shot_last10)::double precision / (with_counts.gp_last10)::double precision)
            ELSE NULL::double precision
        END AS last10_shot_attempts_per_game,
        CASE
            WHEN (with_counts.team_num_shotwasongoal_for_last10 > (0)::numeric) THEN ((with_counts.num_shotwasongoal_last10)::double precision / (with_counts.team_num_shotwasongoal_for_last10)::double precision)
            ELSE NULL::double precision
        END AS last10_team_sog_share,
        CASE
            WHEN with_counts.ishometeam THEN 1
            ELSE 0
        END AS home_flag,
        CASE
            WHEN ((with_counts.gp_last5 >= 3) AND (with_counts.num_shotwasongoal_last5 >= (10)::numeric)) THEN 1
            ELSE 0
        END AS hot_last5_flag
   FROM with_counts;


ALTER VIEW nhl.training_features_sog_player_game_v2 OWNER TO postgres;

--
-- Name: sog_training_frame_phoenix; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.sog_training_frame_phoenix AS
 WITH game_index AS (
         SELECT g.season,
            g.game_id,
            row_number() OVER (PARTITION BY g.season ORDER BY g.game_id) AS game_seq
           FROM ( SELECT DISTINCT training_features_sog_player_game_v2.season,
                    training_features_sog_player_game_v2.game_id
                   FROM nhl.training_features_sog_player_game_v2) g
        )
 SELECT tf.season,
    tf.game_id,
    tf.shooterplayerid,
    tf.teamcode,
        CASE
            WHEN (tf.home_flag = 1) THEN 1
            ELSE 0
        END AS is_home,
    tf.y_sog AS shots_on_goal,
    gi.game_seq,
    ((((tf.season)::text || '-10-01'::text))::date + (((gi.game_seq - 1))::double precision * '1 day'::interval)) AS game_date,
    tf.last5_sog_per_game AS d5_sog_per60,
    tf.last10_sog_per_game AS d10_sog_per60,
    tf.last10_shot_attempts_per_game AS attempts_d10_per60,
        CASE
            WHEN (tf.team_num_event_shot_for_last10 IS NOT NULL) THEN ((tf.team_num_event_shot_for_last10)::double precision / (10.0)::double precision)
            ELSE NULL::double precision
        END AS team_d10_sf_per_game,
    tf.last10_team_sog_share,
    tf.num_shotwasongoal_last5,
    tf.num_shotwasongoal_last10,
    tf.num_shotwasongoal_season_to_date,
    tf.num_event_shot_last5,
    tf.num_event_shot_last10,
    tf.num_event_shot_season_to_date,
    tf.team_num_event_shot_for_last10,
    tf.team_num_shotwasongoal_for_last10,
    tf.home_flag,
    tf.hot_last5_flag
   FROM (nhl.training_features_sog_player_game_v2 tf
     JOIN game_index gi ON (((gi.season = tf.season) AND (gi.game_id = tf.game_id))));


ALTER VIEW nhl.sog_training_frame_phoenix OWNER TO postgres;

--
-- Name: points_training_frame_phoenix; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.points_training_frame_phoenix AS
 WITH sog AS (
         SELECT sog_training_frame_phoenix.season,
            sog_training_frame_phoenix.game_id,
            sog_training_frame_phoenix.shooterplayerid,
            sog_training_frame_phoenix.teamcode,
            sog_training_frame_phoenix.is_home,
            sog_training_frame_phoenix.game_seq,
            sog_training_frame_phoenix.game_date,
            sog_training_frame_phoenix.d5_sog_per60,
            sog_training_frame_phoenix.d10_sog_per60,
            sog_training_frame_phoenix.attempts_d10_per60,
            sog_training_frame_phoenix.team_d10_sf_per_game,
            sog_training_frame_phoenix.last10_team_sog_share,
            sog_training_frame_phoenix.num_shotwasongoal_last5,
            sog_training_frame_phoenix.num_shotwasongoal_last10,
            sog_training_frame_phoenix.num_shotwasongoal_season_to_date,
            sog_training_frame_phoenix.num_event_shot_last5,
            sog_training_frame_phoenix.num_event_shot_last10,
            sog_training_frame_phoenix.num_event_shot_season_to_date,
            sog_training_frame_phoenix.team_num_event_shot_for_last10,
            sog_training_frame_phoenix.team_num_shotwasongoal_for_last10,
            sog_training_frame_phoenix.home_flag,
            sog_training_frame_phoenix.hot_last5_flag
           FROM nhl.sog_training_frame_phoenix
        ), points_raw AS (
         SELECT DISTINCT g.season,
            g.game_id,
            spr.player_id,
            (COALESCE(spr.goals, 0) + COALESCE(spr.assists, 0)) AS y_points
           FROM (nhl.skater_points_raw spr
             JOIN nhl.games g ON (((spr.game_id = g.game_id) OR (spr.game_id = g.short_game_id))))
          WHERE ((spr.player_id IS NOT NULL) AND (spr.game_id IS NOT NULL))
        )
 SELECT s.season,
    s.game_id,
    s.shooterplayerid AS player_id,
    s.teamcode,
    s.is_home,
    s.game_seq,
    s.game_date,
    p.y_points,
    s.d5_sog_per60,
    s.d10_sog_per60,
    s.attempts_d10_per60,
    s.team_d10_sf_per_game,
    s.last10_team_sog_share,
    s.num_shotwasongoal_last5,
    s.num_shotwasongoal_last10,
    s.num_shotwasongoal_season_to_date,
    s.num_event_shot_last5,
    s.num_event_shot_last10,
    s.num_event_shot_season_to_date,
    s.team_num_event_shot_for_last10,
    s.team_num_shotwasongoal_for_last10,
    s.home_flag,
    s.hot_last5_flag
   FROM (sog s
     JOIN points_raw p ON (((p.season = s.season) AND (p.game_id = s.game_id) AND (p.player_id = s.shooterplayerid))));


ALTER VIEW nhl.points_training_frame_phoenix OWNER TO postgres;

--
-- Name: pp_roles_slate; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.pp_roles_slate (
    game_date date NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL,
    pp_share numeric,
    pp_unit text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.pp_roles_slate OWNER TO postgres;

--
-- Name: predictions; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.predictions (
    prediction_id bigint NOT NULL,
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    prop text NOT NULL,
    line numeric(4,1) NOT NULL,
    p_over double precision NOT NULL,
    model_family text NOT NULL,
    model_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    feature_hash text DEFAULT 'phoenix_v2'::text NOT NULL,
    model_version text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.predictions OWNER TO postgres;

--
-- Name: predictions_prediction_id_seq; Type: SEQUENCE; Schema: nhl; Owner: postgres
--

CREATE SEQUENCE nhl.predictions_prediction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE nhl.predictions_prediction_id_seq OWNER TO postgres;

--
-- Name: predictions_prediction_id_seq; Type: SEQUENCE OWNED BY; Schema: nhl; Owner: postgres
--

ALTER SEQUENCE nhl.predictions_prediction_id_seq OWNED BY nhl.predictions.prediction_id;


--
-- Name: roster_names; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.roster_names (
    player_id bigint NOT NULL,
    full_name text,
    first_name text,
    last_name text,
    "position" text,
    shoots_catches text,
    team_abbr text
);


ALTER TABLE nhl.roster_names OWNER TO postgres;

--
-- Name: roster_status; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.roster_status (
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL,
    active_flag boolean NOT NULL,
    asof_ts timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.roster_status OWNER TO postgres;

--
-- Name: shiftcharts_pairings_game; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shiftcharts_pairings_game (
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id integer NOT NULL,
    toi_sec integer NOT NULL,
    top_mate_player_id bigint,
    top_mate_overlap_sec integer DEFAULT 0 NOT NULL,
    top_mate_overlap_share numeric,
    top3_overlap_share_avg numeric,
    top3_overlap_share_std numeric,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.shiftcharts_pairings_game OWNER TO postgres;

--
-- Name: shift_teammate_overlap_features_game; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shift_teammate_overlap_features_game AS
 SELECT shiftcharts_pairings_game.game_id,
    shiftcharts_pairings_game.player_id,
    shiftcharts_pairings_game.team_id,
    (COALESCE(shiftcharts_pairings_game.toi_sec, 0) > 0) AS shiftcharts_available,
    (shiftcharts_pairings_game.top_mate_overlap_share)::double precision AS top_mate_overlap_share,
    (shiftcharts_pairings_game.top3_overlap_share_avg)::double precision AS top3_mates_overlap_share
   FROM nhl.shiftcharts_pairings_game;


ALTER VIEW nhl.shift_teammate_overlap_features_game OWNER TO postgres;

--
-- Name: shift_teammate_overlap_features_rolling_d10; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shift_teammate_overlap_features_rolling_d10 AS
 WITH base AS (
         SELECT s.player_id,
            s.game_id,
            g.game_date,
            COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
            f.top_mate_overlap_share,
            f.top3_mates_overlap_share
           FROM ((nhl.skater_game_logs_raw s
             JOIN nhl.games g ON ((g.game_id = s.game_id)))
             LEFT JOIN nhl.shift_teammate_overlap_features_game f ON (((f.player_id = s.player_id) AND (f.game_id = s.game_id))))
        )
 SELECT base.player_id,
    base.game_id,
    base.game_date,
    base.shiftcharts_available,
    avg(base.top_mate_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev10 AS d10_top_mate_overlap_share_avg,
    stddev_samp(base.top_mate_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev10 AS d10_top_mate_overlap_share_std,
    avg(base.top3_mates_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev10 AS d10_top3_mates_overlap_share_avg,
    stddev_samp(base.top3_mates_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev10 AS d10_top3_mates_overlap_share_std,
    count(*) OVER w_prev10 AS d10_games_in_window,
    count(*) FILTER (WHERE base.shiftcharts_available) OVER w_prev10 AS d10_shiftcharts_games,
    ((count(*) FILTER (WHERE base.shiftcharts_available) OVER w_prev10)::double precision / NULLIF((count(*) OVER w_prev10)::double precision, (0.0)::double precision)) AS d10_shiftcharts_coverage_rate
   FROM base
  WINDOW w_prev10 AS (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING);


ALTER VIEW nhl.shift_teammate_overlap_features_rolling_d10 OWNER TO postgres;

--
-- Name: shift_teammate_overlap_features_rolling_d20; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shift_teammate_overlap_features_rolling_d20 AS
 WITH base AS (
         SELECT s.player_id,
            s.game_id,
            g.game_date,
            COALESCE(f.shiftcharts_available, false) AS shiftcharts_available,
            f.top_mate_overlap_share,
            f.top3_mates_overlap_share
           FROM ((nhl.skater_game_logs_raw s
             JOIN nhl.games g ON ((g.game_id = s.game_id)))
             LEFT JOIN nhl.shift_teammate_overlap_features_game f ON (((f.player_id = s.player_id) AND (f.game_id = s.game_id))))
        )
 SELECT base.player_id,
    base.game_id,
    base.game_date,
    base.shiftcharts_available,
    avg(base.top_mate_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev20 AS d20_top_mate_overlap_share_avg,
    stddev_samp(base.top_mate_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev20 AS d20_top_mate_overlap_share_std,
    avg(base.top3_mates_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev20 AS d20_top3_mates_overlap_share_avg,
    stddev_samp(base.top3_mates_overlap_share) FILTER (WHERE base.shiftcharts_available) OVER w_prev20 AS d20_top3_mates_overlap_share_std,
    count(*) OVER w_prev20 AS d20_games_in_window,
    count(*) FILTER (WHERE base.shiftcharts_available) OVER w_prev20 AS d20_shiftcharts_games,
    ((count(*) FILTER (WHERE base.shiftcharts_available) OVER w_prev20)::double precision / NULLIF((count(*) OVER w_prev20)::double precision, (0.0)::double precision)) AS d20_shiftcharts_coverage_rate
   FROM base
  WINDOW w_prev20 AS (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING);


ALTER VIEW nhl.shift_teammate_overlap_features_rolling_d20 OWNER TO postgres;

--
-- Name: shift_teammate_overlap_game; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shift_teammate_overlap_game (
    game_id bigint NOT NULL,
    team_abbrev text NOT NULL,
    player_id bigint NOT NULL,
    mate_id bigint NOT NULL,
    overlap_sec integer NOT NULL
);


ALTER TABLE nhl.shift_teammate_overlap_game OWNER TO postgres;

--
-- Name: shift_teammate_overlap_game_v2; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shift_teammate_overlap_game_v2 AS
 WITH shifts AS (
         SELECT shiftcharts_shifts_clean_v2.game_id,
            shiftcharts_shifts_clean_v2.team_id,
            shiftcharts_shifts_clean_v2.player_id,
            shiftcharts_shifts_clean_v2.start_sec_game,
            shiftcharts_shifts_clean_v2.end_sec_game,
            shiftcharts_shifts_clean_v2.dur_sec
           FROM nhl.shiftcharts_shifts_clean_v2
        ), pairwise AS (
         SELECT a_1.game_id,
            a_1.team_id,
            a_1.player_id,
            b.player_id AS teammate_id,
            GREATEST(0, (LEAST(a_1.end_sec_game, b.end_sec_game) - GREATEST(a_1.start_sec_game, b.start_sec_game))) AS overlap_sec_piece
           FROM (shifts a_1
             JOIN shifts b ON (((b.game_id = a_1.game_id) AND (b.team_id = a_1.team_id) AND (b.player_id <> a_1.player_id) AND (b.start_sec_game < a_1.end_sec_game) AND (b.end_sec_game > a_1.start_sec_game))))
        ), agg AS (
         SELECT pairwise.game_id,
            pairwise.team_id,
            pairwise.player_id,
            pairwise.teammate_id,
            (sum(pairwise.overlap_sec_piece))::integer AS overlap_sec
           FROM pairwise
          GROUP BY pairwise.game_id, pairwise.team_id, pairwise.player_id, pairwise.teammate_id
        ), toi AS (
         SELECT shifts.game_id,
            shifts.team_id,
            shifts.player_id,
            (sum(shifts.dur_sec))::integer AS toi_sec
           FROM shifts
          GROUP BY shifts.game_id, shifts.team_id, shifts.player_id
        )
 SELECT a.game_id,
    a.team_id,
    a.player_id,
    a.teammate_id,
    t.toi_sec,
    a.overlap_sec,
    ((a.overlap_sec)::numeric / (NULLIF(t.toi_sec, 0))::numeric) AS overlap_share
   FROM (agg a
     JOIN toi t ON (((t.game_id = a.game_id) AND (t.team_id = a.team_id) AND (t.player_id = a.player_id))));


ALTER VIEW nhl.shift_teammate_overlap_game_v2 OWNER TO postgres;

--
-- Name: shiftcharts_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shiftcharts_raw (
    shift_id bigint NOT NULL,
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id bigint,
    team_abbrev text,
    team_name text,
    first_name text,
    last_name text,
    period integer,
    shift_number integer,
    start_time text,
    end_time text,
    duration text,
    start_sec integer,
    end_sec integer,
    duration_sec integer,
    type_code integer,
    detail_code integer,
    event_number integer,
    event_description text,
    event_details text,
    hex_value text,
    raw_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    raw jsonb,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT shiftcharts_raw_duration_nonneg_chk CHECK (((duration_sec IS NULL) OR (duration_sec >= 0))),
    CONSTRAINT shiftcharts_raw_time_order_chk CHECK (((start_sec IS NULL) OR (end_sec IS NULL) OR (start_sec <= end_sec)))
);


ALTER TABLE nhl.shiftcharts_raw OWNER TO postgres;

--
-- Name: TABLE shiftcharts_raw; Type: COMMENT; Schema: nhl; Owner: postgres
--

COMMENT ON TABLE nhl.shiftcharts_raw IS 'Raw NHL shiftcharts rows per game; source rows stored in raw_json plus parsed time fields for analytics.';


--
-- Name: shiftcharts_shifts_clean_v3; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.shiftcharts_shifts_clean_v3 AS
 SELECT s.game_id,
    s.shift_id,
    s.player_id,
    s.team_id,
    s.period,
    s.start_time,
    s.end_time,
    s.duration,
    s.start_sec AS start_sec_period,
    s.end_sec AS end_sec_period,
    s.dur_sec,
    ((GREATEST(s.period, 1) - 1) * 1200) AS period_offset,
    (((GREATEST(s.period, 1) - 1) * 1200) + s.start_sec) AS start_sec_game,
    (((GREATEST(s.period, 1) - 1) * 1200) + s.end_sec) AS end_sec_game
   FROM nhl.shiftcharts_shifts s
  WHERE ((s.end_sec > s.start_sec) AND ((COALESCE(s.dur_sec, (s.end_sec - s.start_sec)) >= 1) AND (COALESCE(s.dur_sec, (s.end_sec - s.start_sec)) <= 599)));


ALTER VIEW nhl.shiftcharts_shifts_clean_v3 OWNER TO postgres;

--
-- Name: shot_on_goal_events; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shot_on_goal_events (
    game_id bigint NOT NULL,
    event_id bigint NOT NULL,
    season integer NOT NULL,
    game_date date NOT NULL,
    period_number integer,
    time_in_period text,
    event_abs_sec integer,
    situation_code text,
    shot_type text,
    zone_code text,
    shooting_player_id bigint,
    shooting_team_id integer,
    shooter_position_bucket text,
    defending_team_id integer,
    goalie_in_net_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.shot_on_goal_events OWNER TO postgres;

--
-- Name: shot_stats_denali; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shot_stats_denali (
    game_id bigint NOT NULL,
    season integer NOT NULL,
    game_date date NOT NULL,
    shooterplayerid bigint NOT NULL,
    teamcode text NOT NULL,
    ishometeam boolean NOT NULL,
    shot_attempts integer NOT NULL,
    shots_on_goal integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.shot_stats_denali OWNER TO postgres;

--
-- Name: shots_stage_2023; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shots_stage_2023 (
    shotid text,
    arenaadjustedshotdistance text,
    arenaadjustedxcord text,
    arenaadjustedxcordabs text,
    arenaadjustedycord text,
    arenaadjustedycordabs text,
    averagerestdifference text,
    awayemptynet text,
    awaypenalty1length text,
    awaypenalty1timeleft text,
    awayskatersonice text,
    awayteamcode text,
    awayteamgoals text,
    defendingteamaveragetimeonice text,
    defendingteamaveragetimeoniceofdefencemen text,
    defendingteamaveragetimeoniceofdefencemensincefaceoff text,
    defendingteamaveragetimeoniceofforwards text,
    defendingteamaveragetimeoniceofforwardssincefaceoff text,
    defendingteamaveragetimeonicesincefaceoff text,
    defendingteamdefencemenonice text,
    defendingteamforwardsonice text,
    defendingteammaxtimeonice text,
    defendingteammaxtimeoniceofdefencemen text,
    defendingteammaxtimeoniceofdefencemensincefaceoff text,
    defendingteammaxtimeoniceofforwards text,
    defendingteammaxtimeoniceofforwardssincefaceoff text,
    defendingteammaxtimeonicesincefaceoff text,
    defendingteammintimeonice text,
    defendingteammintimeoniceofdefencemen text,
    defendingteammintimeoniceofdefencemensincefaceoff text,
    defendingteammintimeoniceofforwards text,
    defendingteammintimeoniceofforwardssincefaceoff text,
    defendingteammintimeonicesincefaceoff text,
    distancefromlastevent text,
    event text,
    game_id text,
    goal text,
    goalieidforshot text,
    goalienameforshot text,
    homeemptynet text,
    homepenalty1length text,
    homepenalty1timeleft text,
    homeskatersonice text,
    hometeamcode text,
    hometeamgoals text,
    hometeamwon text,
    id text,
    ishometeam text,
    isplayoffgame text,
    lasteventcategory text,
    lasteventshotangle text,
    lasteventshotdistance text,
    lasteventteam text,
    lasteventxcord text,
    lasteventxcord_adjusted text,
    lasteventycord text,
    lasteventycord_adjusted text,
    location text,
    offwing text,
    period text,
    playernumthatdidevent text,
    playernumthatdidlastevent text,
    playerpositionthatdidevent text,
    season text,
    shooterleftright text,
    shootername text,
    shooterplayerid text,
    shootertimeonice text,
    shootertimeonicesincefaceoff text,
    shootingteamaveragetimeonice text,
    shootingteamaveragetimeoniceofdefencemen text,
    shootingteamaveragetimeoniceofdefencemensincefaceoff text,
    shootingteamaveragetimeoniceofforwards text,
    shootingteamaveragetimeoniceofforwardssincefaceoff text,
    shootingteamaveragetimeonicesincefaceoff text,
    shootingteamdefencemenonice text,
    shootingteamforwardsonice text,
    shootingteammaxtimeonice text,
    shootingteammaxtimeoniceofdefencemen text,
    shootingteammaxtimeoniceofdefencemensincefaceoff text,
    shootingteammaxtimeoniceofforwards text,
    shootingteammaxtimeoniceofforwardssincefaceoff text,
    shootingteammaxtimeonicesincefaceoff text,
    shootingteammintimeonice text,
    shootingteammintimeoniceofdefencemen text,
    shootingteammintimeoniceofdefencemensincefaceoff text,
    shootingteammintimeoniceofforwards text,
    shootingteammintimeoniceofforwardssincefaceoff text,
    shootingteammintimeonicesincefaceoff text,
    shotangle text,
    shotangleadjusted text,
    shotangleplusrebound text,
    shotangleplusreboundspeed text,
    shotanglereboundroyalroad text,
    shotdistance text,
    shotgeneratedrebound text,
    shotgoaliefroze text,
    shotonemptynet text,
    shotplaycontinuedinzone text,
    shotplaycontinuedoutsidezone text,
    shotplaystopped text,
    shotrebound text,
    shotrush text,
    shottype text,
    shotwasongoal text,
    speedfromlastevent text,
    team text,
    teamcode text,
    "time" text,
    timedifferencesincechange text,
    timesincefaceoff text,
    timesincelastevent text,
    timeuntilnextevent text,
    xcord text,
    xcordadjusted text,
    xfroze text,
    xgoal text,
    xplaycontinuedinzone text,
    xplaycontinuedoutsidezone text,
    xplaystopped text,
    xrebound text,
    xshotwasongoal text,
    ycord text,
    ycordadjusted text
);


ALTER TABLE nhl.shots_stage_2023 OWNER TO postgres;

--
-- Name: shots_stage_2024; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.shots_stage_2024 (
    shotid text,
    arenaadjustedshotdistance text,
    arenaadjustedxcord text,
    arenaadjustedxcordabs text,
    arenaadjustedycord text,
    arenaadjustedycordabs text,
    averagerestdifference text,
    awayemptynet text,
    awaypenalty1length text,
    awaypenalty1timeleft text,
    awayskatersonice text,
    awayteamcode text,
    awayteamgoals text,
    defendingteamaveragetimeonice text,
    defendingteamaveragetimeoniceofdefencemen text,
    defendingteamaveragetimeoniceofdefencemensincefaceoff text,
    defendingteamaveragetimeoniceofforwards text,
    defendingteamaveragetimeoniceofforwardssincefaceoff text,
    defendingteamaveragetimeonicesincefaceoff text,
    defendingteamdefencemenonice text,
    defendingteamforwardsonice text,
    defendingteammaxtimeonice text,
    defendingteammaxtimeoniceofdefencemen text,
    defendingteammaxtimeoniceofdefencemensincefaceoff text,
    defendingteammaxtimeoniceofforwards text,
    defendingteammaxtimeoniceofforwardssincefaceoff text,
    defendingteammaxtimeonicesincefaceoff text,
    defendingteammintimeonice text,
    defendingteammintimeoniceofdefencemen text,
    defendingteammintimeoniceofdefencemensincefaceoff text,
    defendingteammintimeoniceofforwards text,
    defendingteammintimeoniceofforwardssincefaceoff text,
    defendingteammintimeonicesincefaceoff text,
    distancefromlastevent text,
    event text,
    gameover text,
    game_id text,
    goal text,
    goalieidforshot text,
    goalienameforshot text,
    homeemptynet text,
    homepenalty1length text,
    homepenalty1timeleft text,
    homeskatersonice text,
    hometeamcode text,
    hometeamgoals text,
    hometeamscore text,
    hometeamwon text,
    homewinprobability text,
    id text,
    ishometeam text,
    isplayoffgame text,
    lasteventcategory text,
    lasteventshotangle text,
    lasteventshotdistance text,
    lasteventteam text,
    lasteventxcord text,
    lasteventxcord_adjusted text,
    lasteventycord text,
    lasteventycord_adjusted text,
    location text,
    offwing text,
    penaltylength text,
    period text,
    playernumthatdidevent text,
    playernumthatdidlastevent text,
    playerpositionthatdidevent text,
    playoffgame text,
    roadteamcode text,
    roadteamscore text,
    season text,
    shooterleftright text,
    shootername text,
    shooterplayerid text,
    shootertimeonice text,
    shootertimeonicesincefaceoff text,
    shootingteamaveragetimeonice text,
    shootingteamaveragetimeoniceofdefencemen text,
    shootingteamaveragetimeoniceofdefencemensincefaceoff text,
    shootingteamaveragetimeoniceofforwards text,
    shootingteamaveragetimeoniceofforwardssincefaceoff text,
    shootingteamaveragetimeonicesincefaceoff text,
    shootingteamdefencemenonice text,
    shootingteamforwardsonice text,
    shootingteammaxtimeonice text,
    shootingteammaxtimeoniceofdefencemen text,
    shootingteammaxtimeoniceofdefencemensincefaceoff text,
    shootingteammaxtimeoniceofforwards text,
    shootingteammaxtimeoniceofforwardssincefaceoff text,
    shootingteammaxtimeonicesincefaceoff text,
    shootingteammintimeonice text,
    shootingteammintimeoniceofdefencemen text,
    shootingteammintimeoniceofdefencemensincefaceoff text,
    shootingteammintimeoniceofforwards text,
    shootingteammintimeoniceofforwardssincefaceoff text,
    shootingteammintimeonicesincefaceoff text,
    shotangle text,
    shotangleadjusted text,
    shotangleplusrebound text,
    shotangleplusreboundspeed text,
    shotanglereboundroyalroad text,
    shotdistance text,
    shotgeneratedrebound text,
    shotgoalprobability text,
    shotgoaliefroze text,
    shotonemptynet text,
    shotplaycontinued text,
    shotplaycontinuedinzone text,
    shotplaycontinuedoutsidezone text,
    shotplaystopped text,
    shotrebound text,
    shotrush text,
    shottype text,
    shotwasongoal text,
    speedfromlastevent text,
    team text,
    teamcode text,
    "time" text,
    timebetweenevents text,
    timedifferencesincechange text,
    timeleft text,
    timesincefaceoff text,
    timesincelastevent text,
    timeuntilnextevent text,
    wenttoot text,
    wenttoshootout text,
    xcord text,
    xcordadjusted text,
    xfroze text,
    xgoal text,
    xplaycontinuedinzone text,
    xplaycontinuedoutsidezone text,
    xplaystopped text,
    xrebound text,
    xshotwasongoal text,
    ycord text,
    ycordadjusted text
);


ALTER TABLE nhl.shots_stage_2024 OWNER TO postgres;

--
-- Name: skater_game_special_teams_exposure; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skater_game_special_teams_exposure (
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id integer NOT NULL,
    pp_toi_seconds integer DEFAULT 0 NOT NULL,
    pk_toi_seconds integer DEFAULT 0 NOT NULL,
    pp_shifts integer DEFAULT 0 NOT NULL,
    pk_shifts integer DEFAULT 0 NOT NULL,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.skater_game_special_teams_exposure OWNER TO postgres;

--
-- Name: skater_roll_windows_v1; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.skater_roll_windows_v1 AS
 WITH base AS (
         SELECT s.player_id,
            s.game_id,
            s.team_id,
            s.opponent_id,
            s.is_home,
            s.game_date,
            s.shots_on_goal,
            s.shot_attempts,
            s.toi_minutes,
            s.pp_toi_minutes,
            (sp.points)::double precision AS points
           FROM (nhl.skater_game_logs_raw s
             LEFT JOIN nhl.skater_points_raw sp ON (((sp.player_id = s.player_id) AND (sp.game_id = s.game_id))))
        )
 SELECT b.player_id,
    b.game_id,
    b.team_id,
    b.opponent_id,
    b.is_home,
    b.game_date,
    avg(b.points) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS d5_points_avg,
    avg(b.points) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_points_avg,
    avg((b.shots_on_goal)::double precision) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_sog_avg,
    avg((b.shot_attempts)::double precision) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_attempts_avg,
    avg((b.toi_minutes)::double precision) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_toi_min_avg,
    avg((b.pp_toi_minutes)::double precision) OVER (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_pp_min_avg
   FROM base b;


ALTER VIEW nhl.skater_roll_windows_v1 OWNER TO postgres;

--
-- Name: skater_rolling_agg; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skater_rolling_agg (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    d5_sog_per60 numeric(6,3),
    d10_sog_per60 numeric(6,3),
    d20_sog_per60 numeric(6,3),
    d5_attempts_per60 numeric(6,3),
    d10_pp_toi numeric(6,3),
    role_pp_share numeric(5,3),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE nhl.skater_rolling_agg OWNER TO postgres;

--
-- Name: skater_rolling_v3; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.skater_rolling_v3 AS
 WITH params AS (
         SELECT (5.0)::double precision AS min_toi_minutes
        ), base AS (
         SELECT r.player_id,
            r.game_id,
            r.game_date,
            (COALESCE((r.shots_on_goal)::integer, 0))::double precision AS sog,
            (COALESCE((r.shot_attempts)::integer, 0))::double precision AS attempts,
            (NULLIF(r.toi_minutes, (0)::numeric))::double precision AS toi_min,
            (NULLIF(r.pp_toi_minutes, (0)::numeric))::double precision AS pp_toi_min
           FROM nhl.skater_game_logs_raw r
        )
 SELECT b.player_id,
    b.game_id,
    (( SELECT
                CASE
                    WHEN (sum(x.toi_min) > (0)::double precision) THEN (((60.0)::double precision * sum(x.sog)) / sum(x.toi_min))
                    ELSE NULL::double precision
                END AS "case"
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 5) x))::numeric(10,3) AS d5_sog_per60,
    (( SELECT
                CASE
                    WHEN (sum(x.toi_min) > (0)::double precision) THEN (((60.0)::double precision * sum(x.sog)) / sum(x.toi_min))
                    ELSE NULL::double precision
                END AS "case"
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 10) x))::numeric(10,3) AS d10_sog_per60,
    (( SELECT
                CASE
                    WHEN (sum(x.toi_min) > (0)::double precision) THEN (((60.0)::double precision * sum(x.sog)) / sum(x.toi_min))
                    ELSE NULL::double precision
                END AS "case"
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 20) x))::numeric(10,3) AS d20_sog_per60,
    (( SELECT
                CASE
                    WHEN (sum(x.toi_min) > (0)::double precision) THEN (((60.0)::double precision * sum(x.attempts)) / sum(x.toi_min))
                    ELSE NULL::double precision
                END AS "case"
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 5) x))::numeric(10,3) AS d5_attempts_per60,
    (( SELECT avg(x.pp_toi_min) AS avg
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 10) x))::numeric(10,3) AS d10_pp_toi,
    (( SELECT
                CASE
                    WHEN (sum(x.toi_min) > (0)::double precision) THEN (sum(COALESCE(x.pp_toi_min, (0)::double precision)) / sum(x.toi_min))
                    ELSE NULL::double precision
                END AS "case"
           FROM ( SELECT p.player_id,
                    p.game_id,
                    p.game_date,
                    p.sog,
                    p.attempts,
                    p.toi_min,
                    p.pp_toi_min
                   FROM base p,
                    params par
                  WHERE ((p.player_id = b.player_id) AND (ROW(p.game_date, p.game_id) < ROW(b.game_date, b.game_id)) AND (p.toi_min >= par.min_toi_minutes))
                  ORDER BY p.game_date DESC, p.game_id DESC
                 LIMIT 10) x))::numeric(10,4) AS role_pp_share,
    (now() AT TIME ZONE 'utc'::text) AS created_at
   FROM base b;


ALTER VIEW nhl.skater_rolling_v3 OWNER TO postgres;

--
-- Name: skater_shot_game_totals; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.skater_shot_game_totals AS
 SELECT s.season,
    s.game_id,
    s.shooterplayerid AS player_id,
    s.teamcode AS team_code,
    s.ishometeam AS is_home,
    count(*) FILTER (WHERE (s.event = ANY (ARRAY['SHOT'::text, 'MISSED_SHOT'::text, 'BLOCKED_SHOT'::text, 'GOAL'::text]))) AS shot_attempts,
    count(*) FILTER (WHERE (s.shotwasongoal = 1)) AS shots_on_goal
   FROM nhl.shots_all s
  WHERE (s.shooterplayerid IS NOT NULL)
  GROUP BY s.season, s.game_id, s.shooterplayerid, s.teamcode, s.ishometeam;


ALTER VIEW nhl.skater_shot_game_totals OWNER TO postgres;

--
-- Name: skater_special_teams_szn_to_date; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.skater_special_teams_szn_to_date AS
 WITH base AS (
         SELECT g.season,
            g.game_date,
            e.player_id,
            (sum(e.pp_toi_seconds))::integer AS pp_sec,
            (sum(e.pk_toi_seconds))::integer AS pk_sec,
            (sum(e.pp_shifts))::integer AS pp_shifts,
            (sum(e.pk_shifts))::integer AS pk_shifts,
            (count(DISTINCT e.game_id))::integer AS games_played
           FROM (nhl.skater_game_special_teams_exposure e
             JOIN nhl.games g ON ((g.game_id = e.game_id)))
          GROUP BY g.season, g.game_date, e.player_id
        ), cum AS (
         SELECT base.season,
            base.game_date,
            base.player_id,
            sum(base.pp_sec) OVER (PARTITION BY base.season, base.player_id ORDER BY base.game_date) AS pp_sec_cum,
            sum(base.pk_sec) OVER (PARTITION BY base.season, base.player_id ORDER BY base.game_date) AS pk_sec_cum,
            sum(base.pp_shifts) OVER (PARTITION BY base.season, base.player_id ORDER BY base.game_date) AS pp_shifts_cum,
            sum(base.pk_shifts) OVER (PARTITION BY base.season, base.player_id ORDER BY base.game_date) AS pk_shifts_cum,
            sum(base.games_played) OVER (PARTITION BY base.season, base.player_id ORDER BY base.game_date) AS games_played_cum
           FROM base
        )
 SELECT cum.season,
    cum.game_date,
    cum.player_id,
    cum.games_played_cum AS games_played,
    (((cum.pp_sec_cum)::numeric / 60.0) / (NULLIF(cum.games_played_cum, 0))::numeric) AS szn_toi_per_game_pp,
    (((cum.pk_sec_cum)::numeric / 60.0) / (NULLIF(cum.games_played_cum, 0))::numeric) AS szn_toi_per_game_pk,
    ((cum.pp_shifts_cum)::numeric / (NULLIF(cum.games_played_cum, 0))::numeric) AS szn_shifts_per_game_pp,
    ((cum.pk_shifts_cum)::numeric / (NULLIF(cum.games_played_cum, 0))::numeric) AS szn_shifts_per_game_pk
   FROM cum;


ALTER VIEW nhl.skater_special_teams_szn_to_date OWNER TO postgres;

--
-- Name: skaters2023_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skaters2023_stage (
    playerid text,
    season text,
    name text,
    team text,
    "position" text,
    situation text,
    games_played text,
    icetime text,
    shifts text,
    gamescore text,
    onice_xgoalspercentage text,
    office_xgoalspercentage text,
    onice_corsipercentage text,
    office_corsipercentage text,
    onice_fenwickpercentage text,
    office_fenwickpercentage text,
    icetimerank text,
    i_f_xongoal text,
    i_f_xgoals text,
    i_f_xrebounds text,
    i_f_xfreeze text,
    i_f_xplaystopped text,
    i_f_xplaycontinuedinzone text,
    i_f_xplaycontinuedoutsidezone text,
    i_f_flurryadjustedxgoals text,
    i_f_scorevenueadjustedxgoals text,
    i_f_flurryscorevenueadjustedxgoals text,
    i_f_primaryassists text,
    i_f_secondaryassists text,
    i_f_shotsongoal text,
    i_f_missedshots text,
    i_f_blockedshotattempts text,
    i_f_shotattempts text,
    i_f_points text,
    i_f_goals text,
    i_f_rebounds text,
    i_f_reboundgoals text,
    i_f_freeze text,
    i_f_playstopped text,
    i_f_playcontinuedinzone text,
    i_f_playcontinuedoutsidezone text,
    i_f_savedshotsongoal text,
    i_f_savedunblockedshotattempts text,
    penalties text,
    i_f_penalityminutes text,
    i_f_faceoffswon text,
    i_f_hits text,
    i_f_takeaways text,
    i_f_giveaways text,
    i_f_lowdangershots text,
    i_f_mediumdangershots text,
    i_f_highdangershots text,
    i_f_lowdangerxgoals text,
    i_f_mediumdangerxgoals text,
    i_f_highdangerxgoals text,
    i_f_lowdangergoals text,
    i_f_mediumdangergoals text,
    i_f_highdangergoals text,
    i_f_scoreadjustedshotsattempts text,
    i_f_unblockedshotattempts text,
    i_f_scoreadjustedunblockedshotattempts text,
    i_f_dzonegiveaways text,
    i_f_xgoalsfromxreboundsofshots text,
    i_f_xgoalsfromactualreboundsofshots text,
    i_f_reboundxgoals text,
    i_f_xgoals_with_earned_rebounds text,
    i_f_xgoals_with_earned_rebounds_scoreadjusted text,
    i_f_xgoals_with_earned_rebounds_scoreflurryadjusted text,
    i_f_shifts text,
    i_f_ozoneshiftstarts text,
    i_f_dzoneshiftstarts text,
    i_f_neutralzoneshiftstarts text,
    i_f_flyshiftstarts text,
    i_f_ozoneshiftends text,
    i_f_dzoneshiftends text,
    i_f_neutralzoneshiftends text,
    i_f_flyshiftends text,
    faceoffswon text,
    faceoffslost text,
    timeonbench text,
    penalityminutes text,
    penalityminutesdrawn text,
    penaltiesdrawn text,
    shotsblockedbyplayer text,
    onice_f_xongoal text,
    onice_f_xgoals text,
    onice_f_flurryadjustedxgoals text,
    onice_f_scorevenueadjustedxgoals text,
    onice_f_flurryscorevenueadjustedxgoals text,
    onice_f_shotsongoal text,
    onice_f_missedshots text,
    onice_f_blockedshotattempts text,
    onice_f_shotattempts text,
    onice_f_goals text,
    onice_f_rebounds text,
    onice_f_reboundgoals text,
    onice_f_lowdangershots text,
    onice_f_mediumdangershots text,
    onice_f_highdangershots text,
    onice_f_lowdangerxgoals text,
    onice_f_mediumdangerxgoals text,
    onice_f_highdangerxgoals text,
    onice_f_lowdangergoals text,
    onice_f_mediumdangergoals text,
    onice_f_highdangergoals text,
    onice_f_scoreadjustedshotsattempts text,
    onice_f_unblockedshotattempts text,
    onice_f_scoreadjustedunblockedshotattempts text,
    onice_f_xgoalsfromxreboundsofshots text,
    onice_f_xgoalsfromactualreboundsofshots text,
    onice_f_reboundxgoals text,
    onice_f_xgoals_with_earned_rebounds text,
    onice_f_xgoals_with_earned_rebounds_scoreadjusted text,
    onice_f_xgoals_with_earned_rebounds_scoreflurryadjusted text,
    onice_a_xongoal text,
    onice_a_xgoals text,
    onice_a_flurryadjustedxgoals text,
    onice_a_scorevenueadjustedxgoals text,
    onice_a_flurryscorevenueadjustedxgoals text,
    onice_a_shotsongoal text,
    onice_a_missedshots text,
    onice_a_blockedshotattempts text,
    onice_a_shotattempts text,
    onice_a_goals text,
    onice_a_rebounds text,
    onice_a_reboundgoals text,
    onice_a_lowdangershots text,
    onice_a_mediumdangershots text,
    onice_a_highdangershots text,
    onice_a_lowdangerxgoals text,
    onice_a_mediumdangerxgoals text,
    onice_a_highdangerxgoals text,
    onice_a_lowdangergoals text,
    onice_a_mediumdangergoals text,
    onice_a_highdangergoals text,
    onice_a_scoreadjustedshotsattempts text,
    onice_a_unblockedshotattempts text,
    onice_a_scoreadjustedunblockedshotattempts text,
    onice_a_xgoalsfromxreboundsofshots text,
    onice_a_xgoalsfromactualreboundsofshots text,
    onice_a_reboundxgoals text,
    onice_a_xgoals_with_earned_rebounds text,
    onice_a_xgoals_with_earned_rebounds_scoreadjusted text,
    onice_a_xgoals_with_earned_rebounds_scoreflurryadjusted text,
    office_f_xgoals text,
    office_a_xgoals text,
    office_f_shotattempts text,
    office_a_shotattempts text,
    xgoalsforaftershifts text,
    xgoalsagainstaftershifts text,
    corsiforaftershifts text,
    corsiagainstaftershifts text,
    fenwickforaftershifts text,
    fenwickagainstaftershifts text
);


ALTER TABLE nhl.skaters2023_stage OWNER TO postgres;

--
-- Name: skaters2023_stage_raw; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skaters2023_stage_raw (
    c1 text,
    c2 text,
    c3 text,
    c4 text,
    c5 text,
    c6 text,
    c7 text,
    c8 text,
    c9 text,
    c10 text,
    c11 text,
    c12 text,
    c13 text,
    c14 text,
    c15 text,
    c16 text,
    c17 text,
    c18 text,
    c19 text,
    c20 text,
    c21 text,
    c22 text,
    c23 text,
    c24 text,
    c25 text,
    c26 text,
    c27 text,
    c28 text,
    c29 text,
    c30 text,
    c31 text,
    c32 text,
    c33 text,
    c34 text,
    c35 text,
    c36 text,
    c37 text,
    c38 text,
    c39 text,
    c40 text,
    c41 text,
    c42 text,
    c43 text,
    c44 text,
    c45 text,
    c46 text,
    c47 text,
    c48 text,
    c49 text,
    c50 text,
    c51 text,
    c52 text,
    c53 text,
    c54 text,
    c55 text,
    c56 text,
    c57 text,
    c58 text,
    c59 text,
    c60 text,
    c61 text,
    c62 text,
    c63 text,
    c64 text,
    c65 text,
    c66 text,
    c67 text,
    c68 text,
    c69 text,
    c70 text,
    c71 text,
    c72 text,
    c73 text,
    c74 text,
    c75 text,
    c76 text,
    c77 text,
    c78 text,
    c79 text,
    c80 text,
    c81 text,
    c82 text,
    c83 text,
    c84 text,
    c85 text,
    c86 text,
    c87 text,
    c88 text,
    c89 text,
    c90 text,
    c91 text,
    c92 text,
    c93 text,
    c94 text,
    c95 text,
    c96 text,
    c97 text,
    c98 text,
    c99 text,
    c100 text,
    c101 text,
    c102 text,
    c103 text,
    c104 text,
    c105 text,
    c106 text,
    c107 text,
    c108 text,
    c109 text,
    c110 text,
    c111 text,
    c112 text,
    c113 text,
    c114 text,
    c115 text,
    c116 text,
    c117 text,
    c118 text,
    c119 text,
    c120 text,
    c121 text,
    c122 text,
    c123 text,
    c124 text,
    c125 text,
    c126 text,
    c127 text,
    c128 text,
    c129 text,
    c130 text,
    c131 text,
    c132 text,
    c133 text,
    c134 text,
    c135 text,
    c136 text,
    c137 text,
    c138 text,
    c139 text,
    c140 text,
    c141 text,
    c142 text,
    c143 text,
    c144 text,
    c145 text,
    c146 text,
    c147 text,
    c148 text,
    c149 text,
    c150 text,
    c151 text,
    c152 text,
    c153 text,
    c154 text,
    c155 text,
    c156 text,
    c157 text,
    c158 text,
    c159 text,
    c160 text,
    c161 text,
    c162 text,
    c163 text,
    c164 text,
    c165 text,
    c166 text,
    c167 text,
    c168 text,
    c169 text,
    c170 text,
    c171 text,
    c172 text,
    c173 text,
    c174 text,
    c175 text,
    c176 text,
    c177 text,
    c178 text,
    c179 text,
    c180 text,
    c181 text,
    c182 text,
    c183 text,
    c184 text,
    c185 text,
    c186 text,
    c187 text,
    c188 text,
    c189 text,
    c190 text,
    c191 text,
    c192 text,
    c193 text,
    c194 text,
    c195 text,
    c196 text,
    c197 text,
    c198 text,
    c199 text,
    c200 text
);


ALTER TABLE nhl.skaters2023_stage_raw OWNER TO postgres;

--
-- Name: skaters_szn_sit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skaters_szn_sit (
    player_id bigint,
    season integer,
    player_name text,
    team_abbr text,
    "position" text,
    situation text,
    games_played integer,
    icetime_sec numeric,
    shifts numeric,
    game_score numeric,
    ice_time_rank numeric,
    onice_xg_pct numeric,
    office_xg_pct numeric,
    onice_corsi_pct numeric,
    office_corsi_pct numeric,
    onice_fenwick_pct numeric,
    office_fenwick_pct numeric,
    i_f_x_on_goal numeric,
    i_f_x_goals numeric,
    i_f_x_rebounds numeric,
    i_f_x_freeze numeric,
    i_f_x_play_stopped numeric,
    i_f_x_play_cont_in_zone numeric,
    i_f_x_play_cont_out_zone numeric,
    i_f_flurry_adj_xg numeric,
    i_f_score_venue_adj_xg numeric,
    i_f_flurry_score_venue_adj_xg numeric,
    i_f_primary_ast numeric,
    i_f_secondary_ast numeric,
    i_f_sog numeric,
    i_f_missed numeric,
    i_f_blocked_att numeric,
    i_f_sa numeric,
    i_f_points numeric,
    i_f_goals numeric,
    i_f_rebounds numeric,
    i_f_rebound_goals numeric,
    i_f_freeze numeric,
    i_f_play_stopped_cnt numeric,
    i_f_play_cont_in_zone_cnt numeric,
    i_f_play_cont_out_zone_cnt numeric,
    i_f_saved_sog numeric,
    i_f_saved_unblocked numeric,
    i_f_pim numeric,
    i_f_faceoffs_won numeric,
    i_f_hits numeric,
    i_f_takeaways numeric,
    i_f_giveaways numeric,
    i_f_low_shots numeric,
    i_f_med_shots numeric,
    i_f_high_shots numeric,
    i_f_low_xg numeric,
    i_f_med_xg numeric,
    i_f_high_xg numeric,
    i_f_low_goals numeric,
    i_f_med_goals numeric,
    i_f_high_goals numeric,
    i_f_score_adj_sa numeric,
    i_f_unblocked_sa numeric,
    i_f_score_adj_unblocked_sa numeric,
    i_f_dzone_giveaways numeric,
    i_f_xg_from_xreb_shots numeric,
    i_f_xg_from_actual_reb numeric,
    i_f_rebound_xg numeric,
    i_f_xg_with_earned_reb numeric,
    i_f_xg_with_earned_reb_score_adj numeric,
    i_f_xg_with_earned_reb_flurry_adj numeric,
    i_f_shifts numeric,
    i_f_oz_starts numeric,
    i_f_dz_starts numeric,
    i_f_nz_starts numeric,
    i_f_fly_starts numeric,
    i_f_oz_ends numeric,
    i_f_dz_ends numeric,
    i_f_nz_ends numeric,
    i_f_fly_ends numeric,
    faceoffs_won numeric,
    faceoffs_lost numeric,
    time_on_bench numeric,
    pim numeric,
    pim_drawn numeric,
    penalties_drawn numeric,
    shots_blocked_by_player numeric,
    onice_f_x_on_goal numeric,
    onice_f_x_goals numeric,
    onice_f_flurry_adj_xg numeric,
    onice_f_score_venue_adj_xg numeric,
    onice_f_flurry_score_venue_adj_xg numeric,
    onice_f_sog numeric,
    onice_f_missed numeric,
    onice_f_blocked_att numeric,
    onice_f_sa numeric,
    onice_f_goals numeric,
    onice_f_rebounds numeric,
    onice_f_rebound_goals numeric,
    onice_f_low_shots numeric,
    onice_f_med_shots numeric,
    onice_f_high_shots numeric,
    onice_f_low_xg numeric,
    onice_f_med_xg numeric,
    onice_f_high_xg numeric,
    onice_f_low_goals numeric,
    onice_f_med_goals numeric,
    onice_f_high_goals numeric,
    onice_f_score_adj_sa numeric,
    onice_f_unblocked_sa numeric,
    onice_f_score_adj_unblocked_sa numeric,
    onice_f_xg_from_xreb_shots numeric,
    onice_f_xg_from_actual_reb numeric,
    onice_f_rebound_xg numeric,
    onice_f_xg_with_earned_reb numeric,
    onice_f_xg_with_earned_reb_score_adj numeric,
    onice_f_xg_with_earned_reb_flurry_adj numeric,
    onice_a_x_on_goal numeric,
    onice_a_x_goals numeric,
    onice_a_flurry_adj_xg numeric,
    onice_a_score_venue_adj_xg numeric,
    onice_a_flurry_score_venue_adj_xg numeric,
    onice_a_sog numeric,
    onice_a_missed numeric,
    onice_a_blocked_att numeric,
    onice_a_sa numeric,
    onice_a_goals numeric,
    onice_a_rebounds numeric,
    onice_a_rebound_goals numeric,
    onice_a_low_shots numeric,
    onice_a_med_shots numeric,
    onice_a_high_shots numeric,
    onice_a_low_xg numeric,
    onice_a_med_xg numeric,
    onice_a_high_xg numeric,
    onice_a_low_goals numeric,
    onice_a_med_goals numeric,
    onice_a_high_goals numeric,
    onice_a_score_adj_sa numeric,
    onice_a_unblocked_sa numeric,
    onice_a_score_adj_unblocked_sa numeric,
    onice_a_xg_from_xreb_shots numeric,
    onice_a_xg_from_actual_reb numeric,
    onice_a_rebound_xg numeric,
    onice_a_xg_with_earned_reb numeric,
    onice_a_xg_with_earned_reb_score_adj numeric,
    onice_a_xg_with_earned_reb_flurry_adj numeric,
    office_f_xgoals numeric,
    office_a_xgoals numeric,
    office_f_shot_attempts numeric,
    office_a_shot_attempts numeric,
    xg_for_after_shifts numeric,
    xg_against_after_shifts numeric,
    corsi_for_after_shifts numeric,
    corsi_against_after_shifts numeric,
    fenwick_for_after_shifts numeric,
    fenwick_against_after_shifts numeric,
    _raw jsonb
);


ALTER TABLE nhl.skaters_szn_sit OWNER TO postgres;

--
-- Name: skaters_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skaters_szn_sit_denali (
    "playerId" bigint,
    season integer,
    name text,
    team text,
    "position" text,
    situation text,
    games_played numeric,
    icetime numeric,
    shifts numeric,
    "gameScore" numeric,
    "onIce_xGoalsPercentage" numeric,
    "offIce_xGoalsPercentage" numeric,
    "onIce_corsiPercentage" numeric,
    "offIce_corsiPercentage" numeric,
    "onIce_fenwickPercentage" numeric,
    "offIce_fenwickPercentage" numeric,
    "iceTimeRank" numeric,
    "I_F_xOnGoal" numeric,
    "I_F_xGoals" numeric,
    "I_F_xRebounds" numeric,
    "I_F_xFreeze" numeric,
    "I_F_xPlayStopped" numeric,
    "I_F_xPlayContinuedInZone" numeric,
    "I_F_xPlayContinuedOutsideZone" numeric,
    "I_F_flurryAdjustedxGoals" numeric,
    "I_F_scoreVenueAdjustedxGoals" numeric,
    "I_F_flurryScoreVenueAdjustedxGoals" numeric,
    "I_F_primaryAssists" numeric,
    "I_F_secondaryAssists" numeric,
    "I_F_shotsOnGoal" numeric,
    "I_F_missedShots" numeric,
    "I_F_blockedShotAttempts" numeric,
    "I_F_shotAttempts" numeric,
    "I_F_points" numeric,
    "I_F_goals" numeric,
    "I_F_rebounds" numeric,
    "I_F_reboundGoals" numeric,
    "I_F_freeze" numeric,
    "I_F_playStopped" numeric,
    "I_F_playContinuedInZone" numeric,
    "I_F_playContinuedOutsideZone" numeric,
    "I_F_savedShotsOnGoal" numeric,
    "I_F_savedUnblockedShotAttempts" numeric,
    penalties numeric,
    "I_F_penalityMinutes" numeric,
    "I_F_faceOffsWon" numeric,
    "I_F_hits" numeric,
    "I_F_takeaways" numeric,
    "I_F_giveaways" numeric,
    "I_F_lowDangerShots" numeric,
    "I_F_mediumDangerShots" numeric,
    "I_F_highDangerShots" numeric,
    "I_F_lowDangerxGoals" numeric,
    "I_F_mediumDangerxGoals" numeric,
    "I_F_highDangerxGoals" numeric,
    "I_F_lowDangerGoals" numeric,
    "I_F_mediumDangerGoals" numeric,
    "I_F_highDangerGoals" numeric,
    "I_F_scoreAdjustedShotsAttempts" numeric,
    "I_F_unblockedShotAttempts" numeric,
    "I_F_scoreAdjustedUnblockedShotAttempts" numeric,
    "I_F_dZoneGiveaways" numeric,
    "I_F_xGoalsFromxReboundsOfShots" numeric,
    "I_F_xGoalsFromActualReboundsOfShots" numeric,
    "I_F_reboundxGoals" numeric,
    "I_F_xGoals_with_earned_rebounds" numeric,
    "I_F_xGoals_with_earned_rebounds_scoreAdjusted" numeric,
    "I_F_xGoals_with_earned_rebounds_scoreFlurryAdjusted" numeric,
    "I_F_shifts" numeric,
    "I_F_oZoneShiftStarts" numeric,
    "I_F_dZoneShiftStarts" numeric,
    "I_F_neutralZoneShiftStarts" numeric,
    "I_F_flyShiftStarts" numeric,
    "I_F_oZoneShiftEnds" numeric,
    "I_F_dZoneShiftEnds" numeric,
    "I_F_neutralZoneShiftEnds" numeric,
    "I_F_flyShiftEnds" numeric,
    "faceoffsWon" numeric,
    "faceoffsLost" numeric,
    "timeOnBench" numeric,
    "penalityMinutes" numeric,
    "penalityMinutesDrawn" numeric,
    "penaltiesDrawn" numeric,
    "shotsBlockedByPlayer" numeric,
    "OnIce_F_xOnGoal" numeric,
    "OnIce_F_xGoals" numeric,
    "OnIce_F_flurryAdjustedxGoals" numeric,
    "OnIce_F_scoreVenueAdjustedxGoals" numeric,
    "OnIce_F_flurryScoreVenueAdjustedxGoals" numeric,
    "OnIce_F_shotsOnGoal" numeric,
    "OnIce_F_missedShots" numeric,
    "OnIce_F_blockedShotAttempts" numeric,
    "OnIce_F_shotAttempts" numeric,
    "OnIce_F_goals" numeric,
    "OnIce_F_rebounds" numeric,
    "OnIce_F_reboundGoals" numeric,
    "OnIce_F_lowDangerShots" numeric,
    "OnIce_F_mediumDangerShots" numeric,
    "OnIce_F_highDangerShots" numeric,
    "OnIce_F_lowDangerxGoals" numeric,
    "OnIce_F_mediumDangerxGoals" numeric,
    "OnIce_F_highDangerxGoals" numeric,
    "OnIce_F_lowDangerGoals" numeric,
    "OnIce_F_mediumDangerGoals" numeric,
    "OnIce_F_highDangerGoals" numeric,
    "OnIce_F_scoreAdjustedShotsAttempts" numeric,
    "OnIce_F_unblockedShotAttempts" numeric,
    "OnIce_F_scoreAdjustedUnblockedShotAttempts" numeric,
    "OnIce_F_xGoalsFromxReboundsOfShots" numeric,
    "OnIce_F_xGoalsFromActualReboundsOfShots" numeric,
    "OnIce_F_reboundxGoals" numeric,
    "OnIce_F_xGoals_with_earned_rebounds" numeric,
    "OnIce_F_xGoals_with_earned_rebounds_scoreAdjusted" numeric,
    "OnIce_F_xGoals_with_earned_rebounds_scoreFlurryAdjusted" numeric,
    "OnIce_A_xOnGoal" numeric,
    "OnIce_A_xGoals" numeric,
    "OnIce_A_flurryAdjustedxGoals" numeric,
    "OnIce_A_scoreVenueAdjustedxGoals" numeric,
    "OnIce_A_flurryScoreVenueAdjustedxGoals" numeric,
    "OnIce_A_shotsOnGoal" numeric,
    "OnIce_A_missedShots" numeric,
    "OnIce_A_blockedShotAttempts" numeric,
    "OnIce_A_shotAttempts" numeric,
    "OnIce_A_goals" numeric,
    "OnIce_A_rebounds" numeric,
    "OnIce_A_reboundGoals" numeric,
    "OnIce_A_lowDangerShots" numeric,
    "OnIce_A_mediumDangerShots" numeric,
    "OnIce_A_highDangerShots" numeric,
    "OnIce_A_lowDangerxGoals" numeric,
    "OnIce_A_mediumDangerxGoals" numeric,
    "OnIce_A_highDangerxGoals" numeric,
    "OnIce_A_lowDangerGoals" numeric,
    "OnIce_A_mediumDangerGoals" numeric,
    "OnIce_A_highDangerGoals" numeric,
    "OnIce_A_scoreAdjustedShotsAttempts" numeric,
    "OnIce_A_unblockedShotAttempts" numeric,
    "OnIce_A_scoreAdjustedUnblockedShotAttempts" numeric,
    "OnIce_A_xGoalsFromxReboundsOfShots" numeric,
    "OnIce_A_xGoalsFromActualReboundsOfShots" numeric,
    "OnIce_A_reboundxGoals" numeric,
    "OnIce_A_xGoals_with_earned_rebounds" numeric,
    "OnIce_A_xGoals_with_earned_rebounds_scoreAdjusted" numeric,
    "OnIce_A_xGoals_with_earned_rebounds_scoreFlurryAdjusted" numeric,
    "OffIce_F_xGoals" numeric,
    "OffIce_A_xGoals" numeric,
    "OffIce_F_shotAttempts" numeric,
    "OffIce_A_shotAttempts" numeric,
    "xGoalsForAfterShifts" numeric,
    "xGoalsAgainstAfterShifts" numeric,
    "corsiForAfterShifts" numeric,
    "corsiAgainstAfterShifts" numeric,
    "fenwickForAfterShifts" numeric,
    "fenwickAgainstAfterShifts" numeric,
    is_ev boolean,
    is_pp boolean,
    is_sh boolean
);


ALTER TABLE nhl.skaters_szn_sit_denali OWNER TO postgres;

--
-- Name: skaters_szn_sit_stage; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.skaters_szn_sit_stage (
    "playerId" text,
    season text,
    name text,
    team text,
    "position" text,
    situation text,
    games_played text,
    icetime text,
    shifts text,
    "gameScore" text,
    "onIce_xGoalsPercentage" text,
    "offIce_xGoalsPercentage" text,
    "onIce_corsiPercentage" text,
    "offIce_corsiPercentage" text,
    "onIce_fenwickPercentage" text,
    "offIce_fenwickPercentage" text,
    "iceTimeRank" text,
    "I_F_xOnGoal" text,
    "I_F_xGoals" text,
    "I_F_xRebounds" text,
    "I_F_xFreeze" text,
    "I_F_xPlayStopped" text,
    "I_F_xPlayContinuedInZone" text,
    "I_F_xPlayContinuedOutsideZone" text,
    "I_F_flurryAdjustedxGoals" text,
    "I_F_scoreVenueAdjustedxGoals" text,
    "I_F_flurryScoreVenueAdjustedxGoals" text,
    "I_F_primaryAssists" text,
    "I_F_secondaryAssists" text,
    "I_F_shotsOnGoal" text,
    "I_F_missedShots" text,
    "I_F_blockedShotAttempts" text,
    "I_F_shotAttempts" text,
    "I_F_points" text,
    "I_F_goals" text,
    "I_F_rebounds" text,
    "I_F_reboundGoals" text,
    "I_F_freeze" text,
    "I_F_playStopped" text,
    "I_F_playContinuedInZone" text,
    "I_F_playContinuedOutsideZone" text,
    "I_F_savedShotsOnGoal" text,
    "I_F_savedUnblockedShotAttempts" text,
    penalties text,
    "I_F_penalityMinutes" text,
    "I_F_faceOffsWon" text,
    "I_F_hits" text,
    "I_F_takeaways" text,
    "I_F_giveaways" text,
    "I_F_lowDangerShots" text,
    "I_F_mediumDangerShots" text,
    "I_F_highDangerShots" text,
    "I_F_lowDangerxGoals" text,
    "I_F_mediumDangerxGoals" text,
    "I_F_highDangerxGoals" text,
    "I_F_lowDangerGoals" text,
    "I_F_mediumDangerGoals" text,
    "I_F_highDangerGoals" text,
    "I_F_scoreAdjustedShotsAttempts" text,
    "I_F_unblockedShotAttempts" text,
    "I_F_scoreAdjustedUnblockedShotAttempts" text,
    "I_F_dZoneGiveaways" text,
    "I_F_xGoalsFromxReboundsOfShots" text,
    "I_F_xGoalsFromActualReboundsOfShots" text,
    "I_F_reboundxGoals" text,
    "I_F_xGoals_with_earned_rebounds" text,
    "I_F_xGoals_with_earned_rebounds_scoreAdjusted" text,
    "I_F_xGoals_with_earned_rebounds_scoreFlurryAdjusted" text,
    "I_F_shifts" text,
    "I_F_oZoneShiftStarts" text,
    "I_F_dZoneShiftStarts" text,
    "I_F_neutralZoneShiftStarts" text,
    "I_F_flyShiftStarts" text,
    "I_F_oZoneShiftEnds" text,
    "I_F_dZoneShiftEnds" text,
    "I_F_neutralZoneShiftEnds" text,
    "I_F_flyShiftEnds" text,
    "faceoffsWon" text,
    "faceoffsLost" text,
    "timeOnBench" text,
    "penalityMinutes" text,
    "penalityMinutesDrawn" text,
    "penaltiesDrawn" text,
    "shotsBlockedByPlayer" text,
    "OnIce_F_xOnGoal" text,
    "OnIce_F_xGoals" text,
    "OnIce_F_flurryAdjustedxGoals" text,
    "OnIce_F_scoreVenueAdjustedxGoals" text,
    "OnIce_F_flurryScoreVenueAdjustedxGoals" text,
    "OnIce_F_shotsOnGoal" text,
    "OnIce_F_missedShots" text,
    "OnIce_F_blockedShotAttempts" text,
    "OnIce_F_shotAttempts" text,
    "OnIce_F_goals" text,
    "OnIce_F_rebounds" text,
    "OnIce_F_reboundGoals" text,
    "OnIce_F_lowDangerShots" text,
    "OnIce_F_mediumDangerShots" text,
    "OnIce_F_highDangerShots" text,
    "OnIce_F_lowDangerxGoals" text,
    "OnIce_F_mediumDangerxGoals" text,
    "OnIce_F_highDangerxGoals" text,
    "OnIce_F_lowDangerGoals" text,
    "OnIce_F_mediumDangerGoals" text,
    "OnIce_F_highDangerGoals" text,
    "OnIce_F_scoreAdjustedShotsAttempts" text,
    "OnIce_F_unblockedShotAttempts" text,
    "OnIce_F_scoreAdjustedUnblockedShotAttempts" text,
    "OnIce_F_xGoalsFromxReboundsOfShots" text,
    "OnIce_F_xGoalsFromActualReboundsOfShots" text,
    "OnIce_F_reboundxGoals" text,
    "OnIce_F_xGoals_with_earned_rebounds" text,
    "OnIce_F_xGoals_with_earned_rebounds_scoreAdjusted" text,
    "OnIce_F_xGoals_with_earned_rebounds_scoreFlurryAdjusted" text,
    "OnIce_A_xOnGoal" text,
    "OnIce_A_xGoals" text,
    "OnIce_A_flurryAdjustedxGoals" text,
    "OnIce_A_scoreVenueAdjustedxGoals" text,
    "OnIce_A_flurryScoreVenueAdjustedxGoals" text,
    "OnIce_A_shotsOnGoal" text,
    "OnIce_A_missedShots" text,
    "OnIce_A_blockedShotAttempts" text,
    "OnIce_A_shotAttempts" text,
    "OnIce_A_goals" text,
    "OnIce_A_rebounds" text,
    "OnIce_A_reboundGoals" text,
    "OnIce_A_lowDangerShots" text,
    "OnIce_A_mediumDangerShots" text,
    "OnIce_A_highDangerShots" text,
    "OnIce_A_lowDangerxGoals" text,
    "OnIce_A_mediumDangerxGoals" text,
    "OnIce_A_highDangerxGoals" text,
    "OnIce_A_lowDangerGoals" text,
    "OnIce_A_mediumDangerGoals" text,
    "OnIce_A_highDangerGoals" text,
    "OnIce_A_scoreAdjustedShotsAttempts" text,
    "OnIce_A_unblockedShotAttempts" text,
    "OnIce_A_scoreAdjustedUnblockedShotAttempts" text,
    "OnIce_A_xGoalsFromxReboundsOfShots" text,
    "OnIce_A_xGoalsFromActualReboundsOfShots" text,
    "OnIce_A_reboundxGoals" text,
    "OnIce_A_xGoals_with_earned_rebounds" text,
    "OnIce_A_xGoals_with_earned_rebounds_scoreAdjusted" text,
    "OnIce_A_xGoals_with_earned_rebounds_scoreFlurryAdjusted" text,
    "OffIce_F_xGoals" text,
    "OffIce_A_xGoals" text,
    "OffIce_F_shotAttempts" text,
    "OffIce_A_shotAttempts" text,
    "xGoalsForAfterShifts" text,
    "xGoalsAgainstAfterShifts" text,
    "corsiForAfterShifts" text,
    "corsiAgainstAfterShifts" text,
    "fenwickForAfterShifts" text,
    "fenwickAgainstAfterShifts" text
);


ALTER TABLE nhl.skaters_szn_sit_stage OWNER TO postgres;

--
-- Name: sog_denali_rollups_mv; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.sog_denali_rollups_mv AS
 WITH base AS (
         SELECT skater_game_logs_raw.player_id,
            skater_game_logs_raw.game_id,
            skater_game_logs_raw.game_date,
            (COALESCE((skater_game_logs_raw.shots_on_goal)::integer, 0))::numeric AS sog,
            (COALESCE((skater_game_logs_raw.shot_attempts)::integer, 0))::numeric AS att,
            (NULLIF(skater_game_logs_raw.toi_minutes, (0)::numeric))::numeric AS toi_min
           FROM nhl.skater_game_logs_raw
          WHERE (skater_game_logs_raw.game_date >= '2025-10-01'::date)
        ), w AS (
         SELECT base.player_id,
            base.game_id,
            base.game_date,
            sum(base.sog) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sog_5,
            sum(base.sog) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sog_10,
            sum(base.sog) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sog_20,
            sum(base.att) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS att_10,
            sum(base.toi_min) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS toi_5,
            sum(base.toi_min) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS toi_10,
            sum(base.toi_min) OVER (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS toi_20
           FROM base
        )
 SELECT w.player_id,
    w.game_id,
    w.game_date,
        CASE
            WHEN (w.toi_5 >= (30)::numeric) THEN ((w.sog_5 * (60)::numeric) / w.toi_5)
            ELSE NULL::numeric
        END AS d5_sog_per60,
        CASE
            WHEN (w.toi_10 >= (60)::numeric) THEN ((w.sog_10 * (60)::numeric) / w.toi_10)
            ELSE NULL::numeric
        END AS d10_sog_per60,
        CASE
            WHEN (w.toi_20 >= (120)::numeric) THEN ((w.sog_20 * (60)::numeric) / w.toi_20)
            ELSE NULL::numeric
        END AS d20_sog_per60,
        CASE
            WHEN (w.toi_10 >= (60)::numeric) THEN ((w.att_10 * (60)::numeric) / w.toi_10)
            ELSE NULL::numeric
        END AS attempts_d10_per60,
    w.sog_10 AS roll_sog_last10,
    w.att_10 AS roll_att_last10,
    w.toi_10 AS roll_toi_min_last10
   FROM w
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.sog_denali_rollups_mv OWNER TO postgres;

--
-- Name: sog_denali_rollups_v; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.sog_denali_rollups_v AS
 SELECT sog_denali_rollups_mv.player_id,
    sog_denali_rollups_mv.game_id,
    sog_denali_rollups_mv.game_date,
    sog_denali_rollups_mv.d5_sog_per60,
    sog_denali_rollups_mv.d10_sog_per60,
    sog_denali_rollups_mv.d20_sog_per60,
    sog_denali_rollups_mv.attempts_d10_per60,
    sog_denali_rollups_mv.roll_sog_last10,
    sog_denali_rollups_mv.roll_att_last10,
    sog_denali_rollups_mv.roll_toi_min_last10
   FROM nhl.sog_denali_rollups_mv;


ALTER VIEW nhl.sog_denali_rollups_v OWNER TO postgres;

--
-- Name: team_context_rolling; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_context_rolling (
    team_id bigint NOT NULL,
    game_id bigint NOT NULL,
    d10_sf_per60 numeric(6,3),
    d10_sa_per60 numeric(6,3),
    opp_d10_sf_per60 numeric(6,3),
    opp_d10_sa_per60 numeric(6,3),
    pace_matchup_index numeric(6,3)
);


ALTER TABLE nhl.team_context_rolling OWNER TO postgres;

--
-- Name: team_external_ids; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_external_ids (
    team_id integer NOT NULL,
    provider text NOT NULL,
    provider_team_id text NOT NULL
);


ALTER TABLE nhl.team_external_ids OWNER TO postgres;

--
-- Name: team_game_2023_roll; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_game_2023_roll (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    num_event_shot_for bigint,
    num_event_miss_for bigint,
    num_event_goal_for bigint,
    num_shotwasongoal_for bigint,
    num_event_shot_for_last10 numeric,
    num_event_goal_for_last10 numeric,
    num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.team_game_2023_roll OWNER TO postgres;

--
-- Name: team_game_2023_summary; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_game_2023_summary (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    num_event_shot_for bigint,
    num_event_miss_for bigint,
    num_event_goal_for bigint,
    num_shotwasongoal_for bigint
);


ALTER TABLE nhl.team_game_2023_summary OWNER TO postgres;

--
-- Name: team_game_2024_roll; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_game_2024_roll (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    num_event_shot_for bigint,
    num_event_miss_for bigint,
    num_event_goal_for bigint,
    num_shotwasongoal_for bigint,
    num_event_shot_for_last10 numeric,
    num_event_goal_for_last10 numeric,
    num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.team_game_2024_roll OWNER TO postgres;

--
-- Name: team_game_2024_summary; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_game_2024_summary (
    game_id bigint,
    season integer,
    teamcode text,
    ishometeam boolean,
    num_event_shot_for bigint,
    num_event_miss_for bigint,
    num_event_goal_for bigint,
    num_shotwasongoal_for bigint
);


ALTER TABLE nhl.team_game_2024_summary OWNER TO postgres;

--
-- Name: team_game_sit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.team_game_sit (
    game_id bigint NOT NULL,
    season integer NOT NULL,
    team_code text NOT NULL,
    opponent_code text NOT NULL,
    home_or_away text,
    game_date date,
    situation text NOT NULL,
    xgoals_pct double precision,
    corsi_pct double precision,
    fenwick_pct double precision,
    ice_time_seconds integer,
    metrics_for jsonb,
    metrics_against jsonb
);


ALTER TABLE nhl.team_game_sit OWNER TO postgres;

--
-- Name: team_pp_toi_totals_by_date_team; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.team_pp_toi_totals_by_date_team AS
 SELECT g.game_date,
    l.team_id,
    sum(COALESCE(l.pp_toi_minutes, (0)::numeric)) AS team_pp_toi_min
   FROM (nhl.skater_game_logs_raw l
     JOIN nhl.games g USING (game_id))
  GROUP BY g.game_date, l.team_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.team_pp_toi_totals_by_date_team OWNER TO postgres;

--
-- Name: team_roll10_m; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.team_roll10_m AS
 WITH g AS (
         SELECT gl.game_id,
            gl.game_date,
            gl.team_id,
            gl.opponent_id,
            (COALESCE((gl.shots_faced)::integer, 0))::numeric AS shots_faced
           FROM nhl.v_goalie_game_logs_played gl
        ), team_sa AS (
         SELECT g.team_id,
            g.game_id,
            g.game_date,
            sum(g.shots_faced) AS sa_per_game
           FROM g
          GROUP BY g.team_id, g.game_id, g.game_date
        ), team_sf AS (
         SELECT g.opponent_id AS team_id,
            g.game_id,
            g.game_date,
            sum(g.shots_faced) AS sf_per_game
           FROM g
          GROUP BY g.opponent_id, g.game_id, g.game_date
        ), base AS (
         SELECT COALESCE(sa.team_id, sf.team_id) AS team_id,
            COALESCE(sa.game_id, sf.game_id) AS game_id,
            COALESCE(sa.game_date, sf.game_date) AS game_date,
            sa.sa_per_game,
            sf.sf_per_game
           FROM (team_sa sa
             FULL JOIN team_sf sf USING (team_id, game_id, game_date))
        )
 SELECT base.team_id,
    base.game_id,
    base.game_date,
    avg(base.sf_per_game) OVER (PARTITION BY base.team_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS team_d10_sf_per_game,
    avg(base.sa_per_game) OVER (PARTITION BY base.team_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS opp_d10_sf_allowed_per_game
   FROM base
  ORDER BY base.team_id, base.game_date, base.game_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.team_roll10_m OWNER TO postgres;

--
-- Name: teams; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.teams (
    team_id bigint NOT NULL,
    full_team_name text NOT NULL,
    team text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    city text,
    conference text,
    division text,
    active boolean DEFAULT true
);


ALTER TABLE nhl.teams OWNER TO postgres;

--
-- Name: teams_game_sit; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.teams_game_sit (
    team_name text,
    season integer,
    team_abbr text,
    opp_abbr text,
    game_id bigint,
    situation text,
    is_home boolean,
    game_date date,
    pos_group text,
    xg_pct numeric,
    corsi_pct numeric,
    fenwick_pct numeric,
    icetime_s numeric,
    x_on_goal_for numeric,
    xg_for numeric,
    xreb_for numeric,
    xfreeze_for numeric,
    xplay_stopped_for numeric,
    xcont_in_zone_for numeric,
    xcont_out_zone_for numeric,
    xg_flurry_adj_for numeric,
    xg_score_venue_adj_for numeric,
    xg_flurry_score_venue_adj_for numeric,
    sog_for integer,
    missed_for integer,
    blk_att_for integer,
    att_for integer,
    goals_for integer,
    rebounds_for integer,
    rebound_goals_for integer,
    freeze_for integer,
    play_stopped_for integer,
    cont_in_zone_for integer,
    cont_out_zone_for integer,
    saved_sog_for integer,
    saved_unblk_for integer,
    penalties_for integer,
    pimin_for integer,
    faceoffs_won_for integer,
    hits_for integer,
    takeaways_for integer,
    giveaways_for integer,
    ld_shots_for integer,
    md_shots_for integer,
    hd_shots_for integer,
    ld_xg_for numeric,
    md_xg_for numeric,
    hd_xg_for numeric,
    ld_goals_for integer,
    md_goals_for integer,
    hd_goals_for integer,
    sa_attempts_for integer,
    unblk_attempts_for integer,
    sa_unblk_attempts_for integer,
    dz_giveaways_for integer,
    xg_from_xreb_for numeric,
    xg_from_act_reb_for numeric,
    rebound_xg_for numeric,
    total_shot_credit_for numeric,
    sa_total_shot_credit_for numeric,
    flurry_sa_total_shot_credit_for numeric,
    x_on_goal_against numeric,
    xg_against numeric,
    xreb_against numeric,
    xfreeze_against numeric,
    xplay_stopped_against numeric,
    xcont_in_zone_against numeric,
    xcont_out_zone_against numeric,
    xg_flurry_adj_against numeric,
    xg_score_venue_adj_against numeric,
    xg_flurry_score_venue_adj_against numeric,
    sog_against integer,
    missed_against integer,
    blk_att_against integer,
    att_against integer,
    goals_against integer,
    rebounds_against integer,
    rebound_goals_against integer,
    freeze_against integer,
    play_stopped_against integer,
    cont_in_zone_against integer,
    cont_out_zone_against integer,
    saved_sog_against integer,
    saved_unblk_against integer,
    penalties_against integer,
    pimin_against integer,
    faceoffs_won_against integer,
    hits_against integer,
    takeaways_against integer,
    giveaways_against integer,
    ld_shots_against integer,
    md_shots_against integer,
    hd_shots_against integer,
    ld_xg_against numeric,
    md_xg_against numeric,
    hd_xg_against numeric,
    ld_goals_against integer,
    md_goals_against integer,
    hd_goals_against integer,
    sa_attempts_against integer,
    unblk_attempts_against integer,
    sa_unblk_attempts_against integer,
    dz_giveaways_against integer,
    xg_from_xreb_against numeric,
    xg_from_act_reb_against numeric,
    rebound_xg_against numeric,
    total_shot_credit_against numeric,
    sa_total_shot_credit_against numeric,
    flurry_sa_total_shot_credit_against numeric,
    is_playoff_game boolean,
    _raw jsonb
);


ALTER TABLE nhl.teams_game_sit OWNER TO postgres;

--
-- Name: teams_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.teams_szn_sit_denali (
    team text,
    season integer,
    name text,
    team_1 text,
    "position" text,
    situation text,
    games_played numeric,
    "xGoalsPercentage" numeric,
    "corsiPercentage" numeric,
    "fenwickPercentage" numeric,
    "iceTime" numeric,
    "xOnGoalFor" numeric,
    "xGoalsFor" numeric,
    "xReboundsFor" numeric,
    "xFreezeFor" numeric,
    "xPlayStoppedFor" numeric,
    "xPlayContinuedInZoneFor" numeric,
    "xPlayContinuedOutsideZoneFor" numeric,
    "flurryAdjustedxGoalsFor" numeric,
    "scoreVenueAdjustedxGoalsFor" numeric,
    "flurryScoreVenueAdjustedxGoalsFor" numeric,
    "shotsOnGoalFor" numeric,
    "missedShotsFor" numeric,
    "blockedShotAttemptsFor" numeric,
    "shotAttemptsFor" numeric,
    "goalsFor" numeric,
    "reboundsFor" numeric,
    "reboundGoalsFor" numeric,
    "freezeFor" numeric,
    "playStoppedFor" numeric,
    "playContinuedInZoneFor" numeric,
    "playContinuedOutsideZoneFor" numeric,
    "savedShotsOnGoalFor" numeric,
    "savedUnblockedShotAttemptsFor" numeric,
    "penaltiesFor" numeric,
    "penalityMinutesFor" numeric,
    "faceOffsWonFor" numeric,
    "hitsFor" numeric,
    "takeawaysFor" numeric,
    "giveawaysFor" numeric,
    "lowDangerShotsFor" numeric,
    "mediumDangerShotsFor" numeric,
    "highDangerShotsFor" numeric,
    "lowDangerxGoalsFor" numeric,
    "mediumDangerxGoalsFor" numeric,
    "highDangerxGoalsFor" numeric,
    "lowDangerGoalsFor" numeric,
    "mediumDangerGoalsFor" numeric,
    "highDangerGoalsFor" numeric,
    "scoreAdjustedShotsAttemptsFor" numeric,
    "unblockedShotAttemptsFor" numeric,
    "scoreAdjustedUnblockedShotAttemptsFor" numeric,
    "dZoneGiveawaysFor" numeric,
    "xGoalsFromxReboundsOfShotsFor" numeric,
    "xGoalsFromActualReboundsOfShotsFor" numeric,
    "reboundxGoalsFor" numeric,
    "totalShotCreditFor" numeric,
    "scoreAdjustedTotalShotCreditFor" numeric,
    "scoreFlurryAdjustedTotalShotCreditFor" numeric,
    "xOnGoalAgainst" numeric,
    "xGoalsAgainst" numeric,
    "xReboundsAgainst" numeric,
    "xFreezeAgainst" numeric,
    "xPlayStoppedAgainst" numeric,
    "xPlayContinuedInZoneAgainst" numeric,
    "xPlayContinuedOutsideZoneAgainst" numeric,
    "flurryAdjustedxGoalsAgainst" numeric,
    "scoreVenueAdjustedxGoalsAgainst" numeric,
    "flurryScoreVenueAdjustedxGoalsAgainst" numeric,
    "shotsOnGoalAgainst" numeric,
    "missedShotsAgainst" numeric,
    "blockedShotAttemptsAgainst" numeric,
    "shotAttemptsAgainst" numeric,
    "goalsAgainst" numeric,
    "reboundsAgainst" numeric,
    "reboundGoalsAgainst" numeric,
    "freezeAgainst" numeric,
    "playStoppedAgainst" numeric,
    "playContinuedInZoneAgainst" numeric,
    "playContinuedOutsideZoneAgainst" numeric,
    "savedShotsOnGoalAgainst" numeric,
    "savedUnblockedShotAttemptsAgainst" numeric,
    "penaltiesAgainst" numeric,
    "penalityMinutesAgainst" numeric,
    "faceOffsWonAgainst" numeric,
    "hitsAgainst" numeric,
    "takeawaysAgainst" numeric,
    "giveawaysAgainst" numeric,
    "lowDangerShotsAgainst" numeric,
    "mediumDangerShotsAgainst" numeric,
    "highDangerShotsAgainst" numeric,
    "lowDangerxGoalsAgainst" numeric,
    "mediumDangerxGoalsAgainst" numeric,
    "highDangerxGoalsAgainst" numeric,
    "lowDangerGoalsAgainst" numeric,
    "mediumDangerGoalsAgainst" numeric,
    "highDangerGoalsAgainst" numeric,
    "scoreAdjustedShotsAttemptsAgainst" numeric,
    "unblockedShotAttemptsAgainst" numeric,
    "scoreAdjustedUnblockedShotAttemptsAgainst" numeric,
    "dZoneGiveawaysAgainst" numeric,
    "xGoalsFromxReboundsOfShotsAgainst" numeric,
    "xGoalsFromActualReboundsOfShotsAgainst" numeric,
    "reboundxGoalsAgainst" numeric,
    "totalShotCreditAgainst" numeric,
    "scoreAdjustedTotalShotCreditAgainst" numeric,
    "scoreFlurryAdjustedTotalShotCreditAgainst" numeric
);


ALTER TABLE nhl.teams_szn_sit_denali OWNER TO postgres;

--
-- Name: tf_skater_attempts_roll10; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.tf_skater_attempts_roll10 AS
 WITH per_game AS (
         SELECT s.game_id,
            s.shooterplayerid AS player_id,
            count(*) FILTER (WHERE ((s.shooterplayerid IS NOT NULL) AND ((s.event = ANY (ARRAY['SHOT'::text, 'GOAL'::text, 'MISS'::text, 'BLOCK'::text])) OR (s.shotwasongoal = 1) OR (s.goal = 1)))) AS attempts
           FROM nhl.shots_all s
          WHERE (s.shooterplayerid IS NOT NULL)
          GROUP BY s.game_id, s.shooterplayerid
        ), joined AS (
         SELECT g.game_date,
            g.game_id,
            p.player_id,
            p.attempts
           FROM (per_game p
             JOIN nhl.games g USING (game_id))
        )
 SELECT j.player_id,
    j.game_id,
    j.game_date,
    (sum(j.attempts) OVER w / (LEAST((10)::bigint, count(*) OVER w))::numeric) AS attempts_d10_per_game
   FROM joined j
  WINDOW w AS (PARTITION BY j.player_id ORDER BY j.game_date, j.game_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.tf_skater_attempts_roll10 OWNER TO postgres;

--
-- Name: tf_team_roll10; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.tf_team_roll10 (
    team_id integer,
    opponent_id integer,
    game_id bigint,
    game_date date,
    team_sog numeric,
    opp_sog numeric,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    pace_index numeric
);


ALTER TABLE nhl.tf_team_roll10 OWNER TO postgres;

--
-- Name: training_features_goalie_saves_v2; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_goalie_saves_v2 (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint,
    opponent_id bigint,
    is_home boolean,
    game_date date,
    d10_shots_faced_per60 numeric,
    d10_save_pct numeric,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    pace_index numeric,
    rest_days smallint,
    b2b_flag boolean,
    d5_saves_per60 numeric,
    d10_saves_per60 numeric,
    d5_shots_faced_per60 numeric,
    season_save_pct numeric,
    opp_d10_sf_per60 numeric,
    team_d10_sa_per60 numeric,
    pace_matchup_index numeric,
    d20_saves_per60 numeric,
    start_prob numeric
);


ALTER TABLE nhl.training_features_goalie_saves_v2 OWNER TO postgres;

--
-- Name: training_features_goalie_saves_v2_ready; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_goalie_saves_v2_ready AS
 SELECT t.player_id,
    t.game_id,
    t.team_id,
    t.opponent_id,
    t.is_home,
    t.game_date,
    t.d10_shots_faced_per60,
    t.d10_save_pct,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.pace_index,
    t.rest_days,
    t.b2b_flag,
    t.d5_saves_per60,
    t.d10_saves_per60,
    t.d5_shots_faced_per60,
    t.season_save_pct,
    t.opp_d10_sf_per60,
    t.team_d10_sa_per60,
    t.pace_matchup_index,
    t.d20_saves_per60,
    t.start_prob
   FROM nhl.training_features_goalie_saves_v2 t
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_goalie_saves_v2_ready OWNER TO postgres;

--
-- Name: training_features_nhl_saves_enriched; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_saves_enriched AS
 WITH league AS (
         SELECT avg(t.team_sf) AS lg_sf_per_game
           FROM ( SELECT skater_game_logs_raw.team_id,
                    skater_game_logs_raw.game_id,
                    (sum(skater_game_logs_raw.shots_on_goal))::numeric AS team_sf
                   FROM nhl.skater_game_logs_raw
                  GROUP BY skater_game_logs_raw.team_id, skater_game_logs_raw.game_id) t
        ), gk AS (
         SELECT r_1.player_id,
            r_1.game_id,
            r_1.team_id,
            r_1.opponent_id,
            r_1.is_home,
            r_1.game_date,
            (COALESCE((r_1.saves)::integer, 0))::numeric AS saves,
            (COALESCE((r_1.shots_faced)::integer, 0))::numeric AS shots_faced,
            (COALESCE((r_1.goals_allowed)::integer, 0))::numeric AS goals_allowed,
            (NULLIF(r_1.toi_minutes, (0)::numeric))::numeric AS toi_minutes,
            (COALESCE(r_1.ev_shots_faced, 0))::numeric AS ev_shots_faced,
            (COALESCE(r_1.pp_shots_faced, 0))::numeric AS pp_shots_faced,
            (COALESCE(r_1.sh_shots_faced, 0))::numeric AS sh_shots_faced,
            (COALESCE(r_1.high_danger_shots_faced, 0))::numeric AS hd_shots_faced,
            (COALESCE(r_1.rebounds_allowed, 0))::numeric AS rebounds_allowed
           FROM nhl.goalie_game_logs_raw r_1
          WHERE ((r_1.toi_minutes IS NOT NULL) AND (r_1.toi_minutes > (0)::numeric))
        ), gk_rates AS (
         SELECT g.player_id,
            g.game_id,
            g.team_id,
            g.opponent_id,
            g.is_home,
            g.game_date,
            g.saves,
            g.shots_faced,
            g.goals_allowed,
            g.toi_minutes,
            g.ev_shots_faced,
            g.pp_shots_faced,
            g.sh_shots_faced,
            g.hd_shots_faced,
            g.rebounds_allowed,
            ((g.shots_faced / NULLIF(g.toi_minutes, (0)::numeric)) * 60.0) AS shots_faced_per60,
            ((g.saves / NULLIF(g.toi_minutes, (0)::numeric)) * 60.0) AS saves_per60,
                CASE
                    WHEN (g.shots_faced > (0)::numeric) THEN (g.saves / g.shots_faced)
                    ELSE NULL::numeric
                END AS save_pct
           FROM gk g
        ), gk_roll AS (
         SELECT g.player_id,
            g.game_id,
            g.team_id,
            g.opponent_id,
            g.is_home,
            g.game_date,
            g.saves,
            g.shots_faced,
            g.goals_allowed,
            g.toi_minutes,
            g.ev_shots_faced,
            g.pp_shots_faced,
            g.sh_shots_faced,
            g.hd_shots_faced,
            g.rebounds_allowed,
            g.shots_faced_per60,
            g.saves_per60,
            g.save_pct,
            avg(g.shots_faced) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_shots_faced,
            avg(g.shots_faced_per60) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_shots_faced_per60,
            avg(g.saves_per60) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_saves_per60,
            avg(g.save_pct) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_save_pct,
            avg(g.ev_shots_faced) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_ev_sa,
            avg(g.pp_shots_faced) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_pp_sa,
            avg(g.sh_shots_faced) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_sh_sa,
            avg(g.hd_shots_faced) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_hd_sa,
            avg(g.rebounds_allowed) OVER (PARTITION BY g.player_id ORDER BY g.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_rebounds_allowed
           FROM gk_rates g
        ), team_game_sf AS (
         SELECT skater_game_logs_raw.team_id,
            skater_game_logs_raw.opponent_id,
            skater_game_logs_raw.game_id,
            skater_game_logs_raw.game_date,
            (sum(skater_game_logs_raw.shots_on_goal))::numeric AS team_sf
           FROM nhl.skater_game_logs_raw
          GROUP BY skater_game_logs_raw.team_id, skater_game_logs_raw.opponent_id, skater_game_logs_raw.game_id, skater_game_logs_raw.game_date
        ), team_def_ctx AS (
         SELECT o.opponent_id AS team_id,
            o.game_date,
            avg(o.team_sf) OVER (PARTITION BY o.opponent_id ORDER BY o.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS team_d10_sa_per_game
           FROM team_game_sf o
        ), opp_off_ctx AS (
         SELECT t.team_id AS opponent_id,
            t.game_date,
            avg(t.team_sf) OVER (PARTITION BY t.team_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS opp_d10_sf_per_game
           FROM team_game_sf t
        )
 SELECT r.player_id,
    r.game_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.game_date,
    r.saves,
    r.d10_shots_faced,
    r.d10_shots_faced_per60,
    r.d10_saves_per60,
    r.d10_save_pct,
    r.d10_ev_sa,
    r.d10_pp_sa,
    r.d10_sh_sa,
    r.d10_hd_sa,
    r.d10_rebounds_allowed,
    td.team_d10_sa_per_game,
    oo.opp_d10_sf_per_game,
        CASE
            WHEN ((l.lg_sf_per_game IS NULL) OR (l.lg_sf_per_game = (0)::numeric)) THEN NULL::numeric
            ELSE ((COALESCE(td.team_d10_sa_per_game, (0)::numeric) + COALESCE(oo.opp_d10_sf_per_game, (0)::numeric)) / (2.0 * l.lg_sf_per_game))
        END AS pace_matchup_index,
    (r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) AS rest_days,
        CASE
            WHEN ((r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) = 1) THEN true
            ELSE false
        END AS b2b_flag
   FROM (((gk_roll r
     LEFT JOIN team_def_ctx td ON (((td.team_id = r.team_id) AND (td.game_date = r.game_date))))
     LEFT JOIN opp_off_ctx oo ON (((oo.opponent_id = r.opponent_id) AND (oo.game_date = r.game_date))))
     CROSS JOIN league l)
  WHERE (r.saves IS NOT NULL)
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_nhl_saves_enriched OWNER TO postgres;

--
-- Name: training_features_nhl_saves_enr_filt; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_saves_enr_filt AS
 SELECT m.player_id,
    m.game_id,
    m.team_id,
    m.opponent_id,
    m.is_home,
    m.game_date,
    m.saves,
    m.d10_shots_faced,
    m.d10_shots_faced_per60,
    m.d10_saves_per60,
    m.d10_save_pct,
    m.d10_ev_sa,
    m.d10_pp_sa,
    m.d10_sh_sa,
    m.d10_hd_sa,
    m.d10_rebounds_allowed,
    m.team_d10_sa_per_game,
    m.opp_d10_sf_per_game,
    m.pace_matchup_index,
    m.rest_days,
    m.b2b_flag
   FROM (nhl.training_features_nhl_saves_enriched m
     JOIN nhl.keep_games_filter k USING (game_id))
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_nhl_saves_enr_filt OWNER TO postgres;

--
-- Name: training_features_nhl_sog_enriched; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched AS
 WITH league AS (
         SELECT avg(t.team_sf) AS lg_sf_per_game
           FROM ( SELECT r_1.team_id,
                    r_1.game_id,
                    (sum(r_1.shots_on_goal))::numeric AS team_sf
                   FROM nhl.skater_game_logs_raw r_1
                  GROUP BY r_1.team_id, r_1.game_id) t
        ), sk AS (
         SELECT r_1.player_id,
            r_1.game_id,
            r_1.team_id,
            r_1.opponent_id,
            r_1.is_home,
            r_1.game_date,
            (COALESCE((r_1.shots_on_goal)::integer, 0))::numeric AS shots_on_goal,
            (NULLIF(r_1.toi_minutes, (0)::numeric))::numeric AS toi_minutes,
            COALESCE(r_1.pp_toi_minutes, (0)::numeric) AS pp_toi_minutes
           FROM nhl.skater_game_logs_raw r_1
          WHERE ((r_1.toi_minutes IS NOT NULL) AND (r_1.toi_minutes > (0)::numeric))
        ), team_game_sf AS (
         SELECT r_1.team_id,
            r_1.opponent_id,
            r_1.game_id,
            r_1.game_date,
            (sum(r_1.shots_on_goal))::numeric AS team_sf
           FROM nhl.skater_game_logs_raw r_1
          GROUP BY r_1.team_id, r_1.opponent_id, r_1.game_id, r_1.game_date
        ), team_off_ctx AS (
         SELECT t.team_id,
            t.game_date,
            avg(t.team_sf) OVER (PARTITION BY t.team_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS team_d10_sf_per_game
           FROM team_game_sf t
        ), opp_def_ctx AS (
         SELECT t.opponent_id AS team_id,
            t.game_date,
            avg(t.team_sf) OVER (PARTITION BY t.opponent_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS opp_d10_sf_allowed_per_game
           FROM team_game_sf t
        ), sk_rates AS (
         SELECT s.player_id,
            s.game_id,
            s.team_id,
            s.opponent_id,
            s.is_home,
            s.game_date,
            s.shots_on_goal,
            s.toi_minutes,
            s.pp_toi_minutes,
            ((s.shots_on_goal / NULLIF(s.toi_minutes, (0)::numeric)) * 60.0) AS sog_per60
           FROM sk s
        ), sk_roll AS (
         SELECT r_1.player_id,
            r_1.game_id,
            r_1.team_id,
            r_1.opponent_id,
            r_1.is_home,
            r_1.game_date,
            r_1.shots_on_goal,
            r_1.toi_minutes,
            r_1.pp_toi_minutes,
            r_1.sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS d5_sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS d10_sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS d20_sog_per60
           FROM sk_rates r_1
        )
 SELECT r.player_id,
    r.game_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.game_date,
    r.shots_on_goal,
    r.d5_sog_per60,
    r.d10_sog_per60,
    r.d20_sog_per60,
    toff.team_d10_sf_per_game,
    odef.opp_d10_sf_allowed_per_game,
        CASE
            WHEN ((l.lg_sf_per_game IS NULL) OR (l.lg_sf_per_game = (0)::numeric)) THEN NULL::numeric
            ELSE ((COALESCE(toff.team_d10_sf_per_game, (0)::numeric) + COALESCE(odef.opp_d10_sf_allowed_per_game, (0)::numeric)) / (2.0 * l.lg_sf_per_game))
        END AS pace_matchup_index,
        CASE
            WHEN (r.toi_minutes > (0)::numeric) THEN LEAST(GREATEST((r.pp_toi_minutes / r.toi_minutes), (0)::numeric), (1)::numeric)
            ELSE NULL::numeric
        END AS role_pp_share,
    (r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) AS rest_days,
        CASE
            WHEN ((r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) = 1) THEN true
            ELSE false
        END AS b2b_flag
   FROM (((sk_roll r
     LEFT JOIN team_off_ctx toff ON (((toff.team_id = r.team_id) AND (toff.game_date = r.game_date))))
     LEFT JOIN opp_def_ctx odef ON (((odef.team_id = r.opponent_id) AND (odef.game_date = r.game_date))))
     CROSS JOIN league l)
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched OWNER TO postgres;

--
-- Name: training_features_nhl_sog_enriched_pregame; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched_pregame AS
 WITH league AS (
         SELECT avg(t.team_sf) AS lg_sf_per_game
           FROM ( SELECT r_1.team_id,
                    r_1.game_id,
                    (sum(r_1.shots_on_goal))::numeric AS team_sf
                   FROM nhl.skater_game_logs_raw r_1
                  GROUP BY r_1.team_id, r_1.game_id) t
        ), sk AS (
         SELECT r_1.player_id,
            r_1.game_id,
            r_1.team_id,
            r_1.opponent_id,
            r_1.is_home,
            r_1.game_date,
            (COALESCE((r_1.shots_on_goal)::integer, 0))::numeric AS shots_on_goal,
            (NULLIF(r_1.toi_minutes, (0)::numeric))::numeric AS toi_minutes,
            COALESCE(r_1.pp_toi_minutes, (0)::numeric) AS pp_toi_minutes
           FROM nhl.skater_game_logs_raw r_1
          WHERE ((r_1.toi_minutes IS NOT NULL) AND (r_1.toi_minutes > (0)::numeric))
        ), team_game_sf AS (
         SELECT r_1.team_id,
            r_1.opponent_id,
            r_1.game_id,
            r_1.game_date,
            (sum(r_1.shots_on_goal))::numeric AS team_sf
           FROM nhl.skater_game_logs_raw r_1
          GROUP BY r_1.team_id, r_1.opponent_id, r_1.game_id, r_1.game_date
        ), team_off_ctx AS (
         SELECT t.team_id,
            t.game_date,
            avg(t.team_sf) OVER (PARTITION BY t.team_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS team_d10_sf_per_game
           FROM team_game_sf t
        ), opp_def_ctx AS (
         SELECT o.opponent_id AS team_id,
            o.game_date,
            avg(o.team_sf) OVER (PARTITION BY o.opponent_id ORDER BY o.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS opp_d10_sf_allowed_per_game
           FROM team_game_sf o
        ), sk_rates AS (
         SELECT s.player_id,
            s.game_id,
            s.team_id,
            s.opponent_id,
            s.is_home,
            s.game_date,
            s.shots_on_goal,
            s.toi_minutes,
            s.pp_toi_minutes,
            ((s.shots_on_goal / NULLIF(s.toi_minutes, (0)::numeric)) * 60.0) AS sog_per60
           FROM sk s
        ), sk_roll AS (
         SELECT r_1.player_id,
            r_1.game_id,
            r_1.team_id,
            r_1.opponent_id,
            r_1.is_home,
            r_1.game_date,
            r_1.shots_on_goal,
            r_1.toi_minutes,
            r_1.pp_toi_minutes,
            r_1.sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS d5_sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS d10_sog_per60,
            avg(r_1.sog_per60) OVER (PARTITION BY r_1.player_id ORDER BY r_1.game_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS d20_sog_per60
           FROM sk_rates r_1
        )
 SELECT r.player_id,
    r.game_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.game_date,
    r.shots_on_goal,
    r.d5_sog_per60,
    r.d10_sog_per60,
    r.d20_sog_per60,
    toff.team_d10_sf_per_game,
    odef.opp_d10_sf_allowed_per_game,
        CASE
            WHEN ((l.lg_sf_per_game IS NULL) OR (l.lg_sf_per_game = (0)::numeric)) THEN NULL::numeric
            ELSE ((COALESCE(toff.team_d10_sf_per_game, (0)::numeric) + COALESCE(odef.opp_d10_sf_allowed_per_game, (0)::numeric)) / (2.0 * l.lg_sf_per_game))
        END AS pace_matchup_index,
        CASE
            WHEN (r.toi_minutes > (0)::numeric) THEN LEAST(GREATEST((r.pp_toi_minutes / r.toi_minutes), (0)::numeric), (1)::numeric)
            ELSE NULL::numeric
        END AS role_pp_share,
    (r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) AS rest_days,
    ((r.game_date - lag(r.game_date) OVER (PARTITION BY r.player_id ORDER BY r.game_date)) = 1) AS b2b_flag
   FROM (((sk_roll r
     LEFT JOIN team_off_ctx toff ON (((toff.team_id = r.team_id) AND (toff.game_date = r.game_date))))
     LEFT JOIN opp_def_ctx odef ON (((odef.team_id = r.opponent_id) AND (odef.game_date = r.game_date))))
     CROSS JOIN league l)
  WHERE (r.shots_on_goal IS NOT NULL)
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched_pregame OWNER TO postgres;

--
-- Name: training_features_nhl_sog_enriched_pregame_v2; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_nhl_sog_enriched_pregame_v2 (
    player_id bigint,
    game_id bigint,
    team_id bigint,
    opponent_id bigint,
    is_home boolean,
    game_date date,
    shots_on_goal numeric,
    d5_sog_per60 numeric,
    d10_sog_per60 numeric,
    d20_sog_per60 numeric,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    pace_matchup_index numeric,
    role_pp_share numeric,
    rest_days integer,
    b2b_flag boolean,
    season integer,
    attempts_d10_per60 numeric,
    last10_team_sog_share numeric,
    hot_last5_flag boolean,
    num_shotwasongoal_last5 integer,
    num_shotwasongoal_last10 integer,
    num_shotwasongoal_season_to_date integer,
    num_event_shot_last5 integer,
    num_event_shot_last10 integer,
    num_event_shot_season_to_date integer,
    team_num_event_shot_for_last10 integer,
    team_num_shotwasongoal_for_last10 integer,
    szn_toi_per_game_5on5 numeric,
    szn_toi_per_game_pp numeric,
    szn_toi_per_game_pk numeric,
    szn_shifts_per_game_5on5 numeric,
    szn_shifts_per_game_pp numeric,
    szn_shifts_per_game_pk numeric,
    season_5on5_icetime_per_game numeric,
    season_5on4_icetime_per_game numeric,
    season_4on5_icetime_per_game numeric,
    season_5on5_shifts_per_game numeric,
    season_5on4_shifts_per_game numeric,
    season_4on5_shifts_per_game numeric,
    num_sog_last5 numeric,
    num_sog_last10 numeric,
    num_sog_szn_to_date numeric,
    num_event_last5 numeric,
    num_event_last10 numeric,
    num_event_szn_to_date numeric,
    team_num_sog_last10 numeric,
    team_num_event_last10 numeric,
    pace_index numeric,
    d3_toi_min_avg numeric,
    d5_toi_min_avg numeric,
    d10_toi_min_avg numeric,
    d20_toi_min_avg numeric,
    d10_toi_min_sd numeric,
    d10_toi_cv numeric,
    toi_trend_3v10 numeric,
    toi_hist_games integer,
    toi_missing_flag integer,
    toi_hist_ok boolean,
    unit_pp_share numeric,
    unit_pp_share_missing_flag integer,
    pp_role_share_final numeric,
    pp_role_source text,
    d0_top_mate_player_id bigint,
    d0_top_mate_overlap_sec integer,
    d0_top_mate_overlap_share numeric,
    d0_top3_overlap_share_avg numeric,
    d0_top3_overlap_share_std numeric,
    pairings_source text,
    pairings_updated_at timestamp with time zone,
    d10_top_mate_overlap_share_avg double precision,
    d10_top_mate_overlap_share_std double precision,
    d10_top3_mates_overlap_share_avg double precision,
    d10_top3_mates_overlap_share_std double precision,
    d10_games_in_window integer,
    d10_shiftcharts_games integer,
    d10_shiftcharts_coverage_rate double precision,
    d20_top_mate_overlap_share_avg double precision,
    d20_top_mate_overlap_share_std double precision,
    d20_top3_mates_overlap_share_avg double precision,
    d20_top3_mates_overlap_share_std double precision,
    d20_games_in_window integer,
    d20_shiftcharts_games integer,
    d20_shiftcharts_coverage_rate double precision,
    d10_top_mate_repeat_rate double precision,
    d10_top_mate_distinct_count integer,
    d10_top_mate_games_with_shiftcharts integer,
    d20_top_mate_repeat_rate double precision,
    d20_top_mate_distinct_count integer,
    d20_top_mate_games_with_shiftcharts integer,
    mate_stability_source text,
    mate_stability_updated_at timestamp with time zone,
    d10_pairings_missing_flag integer,
    d10_pairings_cov_bucket text,
    d20_pairings_missing_flag integer,
    d20_pairings_cov_bucket text,
    d10_pairings_available boolean,
    d20_pairings_available boolean,
    CONSTRAINT sog_pregame_v2_season_start_year_chk CHECK (((season >= 1900) AND (season <= 2200)))
);


ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2 OWNER TO postgres;

--
-- Name: training_features_nhl_sog_enriched_pregame_v2_denali_cols; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.training_features_nhl_sog_enriched_pregame_v2_denali_cols AS
 SELECT v.player_id,
    v.game_id,
    v.team_id,
    v.opponent_id,
    v.is_home,
    v.game_date,
    v.shots_on_goal,
    v.d5_sog_per60,
    v.d10_sog_per60,
    v.d20_sog_per60,
    v.team_d10_sf_per_game,
    v.opp_d10_sf_allowed_per_game,
    v.pace_matchup_index,
    v.role_pp_share,
    v.rest_days,
    v.b2b_flag,
    v.season,
    v.attempts_d10_per60,
    v.last10_team_sog_share,
    v.hot_last5_flag,
    v.num_shotwasongoal_last5,
    v.num_shotwasongoal_last10,
    v.num_shotwasongoal_season_to_date,
    v.num_event_shot_last5,
    v.num_event_shot_last10,
    v.num_event_shot_season_to_date,
    v.team_num_event_shot_for_last10,
    v.team_num_shotwasongoal_for_last10,
    v.szn_toi_per_game_5on5,
    v.szn_toi_per_game_pp,
    v.szn_toi_per_game_pk,
    v.szn_shifts_per_game_5on5,
    v.szn_shifts_per_game_pp,
    v.szn_shifts_per_game_pk,
    v.season_5on5_icetime_per_game,
    v.season_5on4_icetime_per_game,
    v.season_4on5_icetime_per_game,
    v.season_5on5_shifts_per_game,
    v.season_5on4_shifts_per_game,
    v.season_4on5_shifts_per_game,
    v.num_sog_last5,
    v.num_sog_last10,
    v.num_sog_szn_to_date,
    v.num_event_last5,
    v.num_event_last10,
    v.num_event_szn_to_date,
    v.team_num_sog_last10,
    v.team_num_event_last10,
    v.pace_index,
    v.d3_toi_min_avg,
    v.d5_toi_min_avg,
    v.d10_toi_min_avg,
    v.d20_toi_min_avg,
    v.d10_toi_min_sd,
    v.d10_toi_cv,
    v.toi_trend_3v10,
    v.toi_hist_games,
    v.toi_missing_flag,
    v.toi_hist_ok,
    v.d10_pairings_available,
    v.d20_pairings_available,
    v.d10_pairings_cov_bucket,
    v.d20_pairings_cov_bucket,
    v.d20_top_mate_repeat_rate,
    (tc.opp_d10_sf_per60)::numeric AS opp_d10_sf_per60,
    (tc.d10_sa_per60)::numeric AS team_d10_sa_per60,
    (tc.opp_d10_sa_per60)::numeric AS opp_d10_sa_per60
   FROM (nhl.training_features_nhl_sog_enriched_pregame_v2 v
     LEFT JOIN nhl.team_context_rolling tc ON (((tc.game_id = v.game_id) AND (tc.team_id = v.team_id))));


ALTER VIEW nhl.training_features_nhl_sog_enriched_pregame_v2_denali_cols OWNER TO postgres;

--
-- Name: training_features_nhl_sog_enriched_pregame_v2_mt; Type: MATERIALIZED VIEW; Schema: nhl; Owner: postgres
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched_pregame_v2_mt AS
 WITH skate AS (
         SELECT DISTINCT rs.player_id,
            rs.game_id,
            rs.team_id,
                CASE
                    WHEN (rs.team_id = g.home_team_id) THEN g.away_team_id
                    ELSE g.home_team_id
                END AS opponent_id,
            (rs.team_id = g.home_team_id) AS is_home,
            g.game_date
           FROM ((nhl.roster_status rs
             JOIN nhl.games g USING (game_id))
             JOIN nhl.players p ON ((p.player_id = rs.player_id)))
          WHERE (COALESCE(p."position", ''::text) <> 'G'::text)
        ), league AS (
         SELECT avg(t.team_sf) AS lg_sf_per_game
           FROM ( SELECT skater_game_logs_raw.team_id,
                    skater_game_logs_raw.game_id,
                    (sum(skater_game_logs_raw.shots_on_goal))::numeric AS team_sf
                   FROM nhl.skater_game_logs_raw
                  GROUP BY skater_game_logs_raw.team_id, skater_game_logs_raw.game_id) t
        ), team_game_sf AS (
         SELECT skater_game_logs_raw.team_id,
            skater_game_logs_raw.opponent_id,
            skater_game_logs_raw.game_id,
            skater_game_logs_raw.game_date,
            (sum(skater_game_logs_raw.shots_on_goal))::numeric AS team_sf
           FROM nhl.skater_game_logs_raw
          GROUP BY skater_game_logs_raw.team_id, skater_game_logs_raw.opponent_id, skater_game_logs_raw.game_id, skater_game_logs_raw.game_date
        ), team_off_ctx AS (
         SELECT t.team_id,
            t.game_date,
            avg(t.team_sf) OVER (PARTITION BY t.team_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS team_d10_sf_per_game
           FROM team_game_sf t
        ), opp_def_ctx AS (
         SELECT o.opponent_id AS team_id,
            o.game_date,
            avg(o.team_sf) OVER (PARTITION BY o.opponent_id ORDER BY o.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS opp_d10_sf_allowed_per_game
           FROM team_game_sf o
        ), sk_hist AS (
         SELECT r.player_id,
            r.game_date,
            (NULLIF(r.toi_minutes, (0)::numeric))::numeric AS toi_minutes,
            COALESCE(r.pp_toi_minutes, (0)::numeric) AS pp_toi_minutes,
            (((COALESCE((r.shots_on_goal)::integer, 0))::numeric / (NULLIF(r.toi_minutes, (0)::numeric))::numeric) * 60.0) AS sog_per60
           FROM nhl.skater_game_logs_raw r
          WHERE ((r.toi_minutes IS NOT NULL) AND (r.toi_minutes > (0)::numeric))
        ), sk_roll AS (
         SELECT h.player_id,
            h.game_date,
            avg(h.sog_per60) OVER (PARTITION BY h.player_id ORDER BY h.game_date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS d5_sog_per60,
            avg(h.sog_per60) OVER (PARTITION BY h.player_id ORDER BY h.game_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS d10_sog_per60,
            avg(h.sog_per60) OVER (PARTITION BY h.player_id ORDER BY h.game_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS d20_sog_per60,
            lag(h.game_date) OVER (PARTITION BY h.player_id ORDER BY h.game_date) AS prev_game_date,
                CASE
                    WHEN (h.toi_minutes > (0)::numeric) THEN LEAST(GREATEST((h.pp_toi_minutes / h.toi_minutes), (0)::numeric), (1)::numeric)
                    ELSE NULL::numeric
                END AS role_pp_share_hist
           FROM sk_hist h
        ), joined AS (
         SELECT s.player_id,
            s.game_id,
            s.team_id,
            s.opponent_id,
            s.is_home,
            s.game_date,
            NULL::numeric AS shots_on_goal,
            r10.d5_sog_per60,
            r10.d10_sog_per60,
            r10.d20_sog_per60,
            toff.team_d10_sf_per_game,
            odef.opp_d10_sf_allowed_per_game,
                CASE
                    WHEN ((l.lg_sf_per_game IS NULL) OR (l.lg_sf_per_game = (0)::numeric)) THEN NULL::numeric
                    ELSE ((COALESCE(toff.team_d10_sf_per_game, (0)::numeric) + COALESCE(odef.opp_d10_sf_allowed_per_game, (0)::numeric)) / (2.0 * l.lg_sf_per_game))
                END AS pace_matchup_index,
            r10.role_pp_share_hist AS role_pp_share,
                CASE
                    WHEN (r10.prev_game_date IS NOT NULL) THEN GREATEST(0, (s.game_date - r10.prev_game_date))
                    ELSE NULL::integer
                END AS rest_days,
                CASE
                    WHEN (r10.prev_game_date IS NOT NULL) THEN ((s.game_date - r10.prev_game_date) = 1)
                    ELSE NULL::boolean
                END AS b2b_flag
           FROM ((((skate s
             LEFT JOIN LATERAL ( SELECT x.player_id,
                    x.game_date,
                    x.d5_sog_per60,
                    x.d10_sog_per60,
                    x.d20_sog_per60,
                    x.prev_game_date,
                    x.role_pp_share_hist
                   FROM sk_roll x
                  WHERE ((x.player_id = s.player_id) AND (x.game_date < s.game_date))
                  ORDER BY x.game_date DESC
                 LIMIT 1) r10 ON (true))
             LEFT JOIN team_off_ctx toff ON (((toff.team_id = s.team_id) AND (toff.game_date = s.game_date))))
             LEFT JOIN opp_def_ctx odef ON (((odef.team_id = s.opponent_id) AND (odef.game_date = s.game_date))))
             CROSS JOIN league l)
        )
 SELECT joined.player_id,
    joined.game_id,
    joined.team_id,
    joined.opponent_id,
    joined.is_home,
    joined.game_date,
    joined.shots_on_goal,
    joined.d5_sog_per60,
    joined.d10_sog_per60,
    joined.d20_sog_per60,
    joined.team_d10_sf_per_game,
    joined.opp_d10_sf_allowed_per_game,
    joined.pace_matchup_index,
    joined.role_pp_share,
    joined.rest_days,
    joined.b2b_flag
   FROM joined
  WITH NO DATA;


ALTER MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched_pregame_v2_mt OWNER TO postgres;

--
-- Name: training_features_shots; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_shots (
    game_id bigint,
    season integer,
    shotid text,
    teamcode text,
    hometeamcode text,
    awayteamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    shootername text,
    goalieidforshot bigint,
    goalienameforshot text,
    period integer,
    "time" text,
    xcord double precision,
    ycord double precision,
    shottype text,
    event text,
    goal integer,
    shotwasongoal integer,
    target_goal integer,
    target_shotwasongoal integer,
    num_event_shot_last5 numeric,
    num_event_goal_last5 numeric,
    num_shotwasongoal_last5 numeric,
    num_event_shot_last10 numeric,
    num_event_goal_last10 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_event_goal_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric,
    num_event_shot_for_last10 numeric,
    num_event_goal_for_last10 numeric,
    num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.training_features_shots OWNER TO postgres;

--
-- Name: training_features_shots_2023; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_shots_2023 (
    game_id bigint,
    season integer,
    shotid text,
    teamcode text,
    hometeamcode text,
    awayteamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    shootername text,
    goalieidforshot bigint,
    goalienameforshot text,
    period integer,
    "time" text,
    xcord double precision,
    ycord double precision,
    shottype text,
    event text,
    goal integer,
    shotwasongoal integer,
    target_goal integer,
    target_shotwasongoal integer,
    num_event_shot_last5 numeric,
    num_event_goal_last5 numeric,
    num_shotwasongoal_last5 numeric,
    num_event_shot_last10 numeric,
    num_event_goal_last10 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_event_goal_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric,
    num_event_shot_for_last10 numeric,
    num_event_goal_for_last10 numeric,
    num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.training_features_shots_2023 OWNER TO postgres;

--
-- Name: training_features_shots_2024; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_shots_2024 (
    game_id bigint,
    season integer,
    shotid text,
    teamcode text,
    hometeamcode text,
    awayteamcode text,
    ishometeam boolean,
    shooterplayerid bigint,
    shootername text,
    goalieidforshot bigint,
    goalienameforshot text,
    period integer,
    "time" text,
    xcord double precision,
    ycord double precision,
    shottype text,
    event text,
    goal integer,
    shotwasongoal integer,
    target_goal integer,
    target_shotwasongoal integer,
    num_event_shot_last5 numeric,
    num_event_goal_last5 numeric,
    num_shotwasongoal_last5 numeric,
    num_event_shot_last10 numeric,
    num_event_goal_last10 numeric,
    num_shotwasongoal_last10 numeric,
    num_event_shot_season_to_date numeric,
    num_event_goal_season_to_date numeric,
    num_shotwasongoal_season_to_date numeric,
    num_event_shot_for_last10 numeric,
    num_event_goal_for_last10 numeric,
    num_shotwasongoal_for_last10 numeric
);


ALTER TABLE nhl.training_features_shots_2024 OWNER TO postgres;

--
-- Name: training_features_shots_v; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.training_features_shots_v AS
 SELECT training_features_shots.game_id,
    training_features_shots.season,
    training_features_shots.shotid,
    training_features_shots.teamcode,
    training_features_shots.hometeamcode,
    training_features_shots.awayteamcode,
    training_features_shots.ishometeam,
    training_features_shots.shooterplayerid,
    training_features_shots.shootername,
    training_features_shots.goalieidforshot,
    training_features_shots.goalienameforshot,
    training_features_shots.period,
    training_features_shots."time",
    training_features_shots.xcord,
    training_features_shots.ycord,
    training_features_shots.shottype,
    training_features_shots.event,
    training_features_shots.goal,
    training_features_shots.shotwasongoal,
    training_features_shots.target_goal,
    training_features_shots.target_shotwasongoal,
    training_features_shots.num_event_shot_last5,
    training_features_shots.num_event_goal_last5,
    training_features_shots.num_shotwasongoal_last5,
    training_features_shots.num_event_shot_last10,
    training_features_shots.num_event_goal_last10,
    training_features_shots.num_shotwasongoal_last10,
    training_features_shots.num_event_shot_season_to_date,
    training_features_shots.num_event_goal_season_to_date,
    training_features_shots.num_shotwasongoal_season_to_date,
    training_features_shots.num_event_shot_for_last10,
    training_features_shots.num_event_goal_for_last10,
    training_features_shots.num_shotwasongoal_for_last10
   FROM nhl.training_features_shots;


ALTER VIEW nhl.training_features_shots_v OWNER TO postgres;

--
-- Name: training_features_sog_denali; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.training_features_sog_denali (
    player_id bigint,
    game_id bigint,
    team_id bigint,
    opponent_id bigint,
    is_home boolean,
    game_date date,
    shots_on_goal numeric,
    d5_sog_per60 numeric,
    d10_sog_per60 numeric,
    d20_sog_per60 numeric,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    pace_matchup_index numeric,
    role_pp_share numeric,
    rest_days integer,
    b2b_flag boolean,
    attempts_d10_per60 numeric,
    opp_d10_sf_per60 numeric,
    team_d10_sa_per60 numeric,
    pace_index numeric,
    last10_team_sog_share numeric,
    hot_last5_flag boolean,
    num_shotwasongoal_last5 integer,
    num_shotwasongoal_last10 integer,
    num_shotwasongoal_season_to_date integer,
    num_event_shot_last5 integer,
    num_event_shot_last10 integer,
    num_event_shot_season_to_date integer,
    team_num_event_shot_for_last10 integer,
    team_num_shotwasongoal_for_last10 integer,
    opp_d10_sa_per60 numeric,
    season integer,
    szn_toi_per_game_5on5 numeric,
    szn_toi_per_game_pp numeric,
    szn_toi_per_game_pk numeric,
    szn_shifts_per_game_5on5 numeric,
    szn_shifts_per_game_pp numeric,
    szn_shifts_per_game_pk numeric,
    team_szn_5on5_top_line_xgf_share numeric,
    team_szn_pp_top_line_xgf_share numeric,
    season_5on5_icetime_per_game numeric,
    season_5on4_icetime_per_game numeric,
    season_4on5_icetime_per_game numeric,
    season_5on5_shifts_per_game numeric,
    season_5on4_shifts_per_game numeric,
    season_4on5_shifts_per_game numeric,
    team_5v5_top_line_icetime_share numeric,
    team_5v5_top_line_shotattempts_share numeric,
    team_num_sog_last10 numeric,
    team_num_event_last10 numeric,
    num_sog_last5 numeric,
    num_sog_last10 numeric,
    num_sog_szn_to_date numeric,
    num_event_last5 numeric,
    num_event_last10 numeric,
    num_event_szn_to_date numeric,
    team text
);


ALTER TABLE nhl.training_features_sog_denali OWNER TO postgres;

--
-- Name: training_features_sog_denali_export; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.training_features_sog_denali_export AS
 SELECT p.player_id,
    p.game_id,
    p.team_id,
    p.opponent_id,
    p.game_date,
    l.shots_on_goal,
    p.d5_sog_per60,
    p.d10_sog_per60,
    p.d20_sog_per60,
    p.team_d10_sf_per_game,
    p.opp_d10_sf_allowed_per_game,
    p.pace_matchup_index,
    p.role_pp_share,
    p.rest_days,
    p.season,
    p.attempts_d10_per60,
    p.last10_team_sog_share,
    p.num_shotwasongoal_last5,
    p.num_shotwasongoal_last10,
    p.num_shotwasongoal_season_to_date,
    p.num_event_shot_last5,
    p.num_event_shot_last10,
    p.num_event_shot_season_to_date,
    p.team_num_event_shot_for_last10,
    p.team_num_shotwasongoal_for_last10,
    p.szn_toi_per_game_5on5,
    p.szn_toi_per_game_pp,
    p.szn_toi_per_game_pk,
    p.season_5on5_icetime_per_game,
    p.season_5on4_icetime_per_game,
    p.season_4on5_icetime_per_game,
    p.num_sog_last5,
    p.num_sog_last10,
    p.num_sog_szn_to_date,
    p.num_event_last5,
    p.num_event_last10,
    p.num_event_szn_to_date,
    p.team_num_sog_last10,
    p.team_num_event_last10,
    p.pace_index,
    p.d3_toi_min_avg,
    p.d5_toi_min_avg,
    p.d10_toi_min_avg,
    p.d20_toi_min_avg,
    p.d10_toi_min_sd,
    p.d10_toi_cv,
    p.toi_trend_3v10,
    p.toi_hist_games,
    p.toi_missing_flag,
    p.unit_pp_share,
    p.unit_pp_share_missing_flag,
    p.pp_role_share_final,
    p.pp_role_source,
    p.d0_top_mate_player_id,
    p.d0_top_mate_overlap_sec,
    p.d0_top_mate_overlap_share,
    p.d0_top3_overlap_share_avg,
    p.d0_top3_overlap_share_std,
    p.pairings_source,
    p.pairings_updated_at,
    p.d10_top_mate_overlap_share_avg,
    p.d10_top_mate_overlap_share_std,
    p.d10_top3_mates_overlap_share_avg,
    p.d10_top3_mates_overlap_share_std,
    p.d10_games_in_window,
    p.d10_shiftcharts_games,
    p.d10_shiftcharts_coverage_rate,
    p.d20_top_mate_overlap_share_avg,
    p.d20_top_mate_overlap_share_std,
    p.d20_top3_mates_overlap_share_avg,
    p.d20_top3_mates_overlap_share_std,
    p.d20_games_in_window,
    p.d20_shiftcharts_games,
    p.d20_shiftcharts_coverage_rate,
    p.d10_top_mate_repeat_rate,
    p.d10_top_mate_distinct_count,
    p.d10_top_mate_games_with_shiftcharts,
    p.d20_top_mate_repeat_rate,
    p.d20_top_mate_distinct_count,
    p.d20_top_mate_games_with_shiftcharts,
    p.mate_stability_source,
    p.mate_stability_updated_at,
    p.d10_pairings_missing_flag,
    p.d20_pairings_missing_flag,
        CASE
            WHEN p.is_home THEN 1
            ELSE 0
        END AS is_home,
        CASE
            WHEN p.b2b_flag THEN 1
            ELSE 0
        END AS b2b_flag,
        CASE
            WHEN p.hot_last5_flag THEN 1
            ELSE 0
        END AS hot_last5_flag,
        CASE
            WHEN p.toi_hist_ok THEN 1
            ELSE 0
        END AS toi_hist_ok,
        CASE
            WHEN p.d10_pairings_available THEN 1
            ELSE 0
        END AS d10_pairings_available,
        CASE
            WHEN p.d20_pairings_available THEN 1
            ELSE 0
        END AS d20_pairings_available,
        CASE
            WHEN (p.d10_pairings_cov_bucket IS NULL) THEN 0
            WHEN (p.d10_pairings_cov_bucket = ANY (ARRAY['none'::text, '0'::text])) THEN 0
            WHEN (p.d10_pairings_cov_bucket = ANY (ARRAY['high'::text, '3'::text])) THEN 3
            ELSE (NULLIF(p.d10_pairings_cov_bucket, ''::text))::integer
        END AS d10_pairings_cov_bucket,
        CASE
            WHEN (p.d20_pairings_cov_bucket IS NULL) THEN 0
            WHEN (p.d20_pairings_cov_bucket = ANY (ARRAY['none'::text, '0'::text])) THEN 0
            WHEN (p.d20_pairings_cov_bucket = ANY (ARRAY['high'::text, '3'::text])) THEN 3
            ELSE (NULLIF(p.d20_pairings_cov_bucket, ''::text))::integer
        END AS d20_pairings_cov_bucket,
    tc.opp_d10_sf_per60,
    tc.d10_sa_per60 AS team_d10_sa_per60,
    tc.opp_d10_sa_per60
   FROM ((nhl.training_features_nhl_sog_enriched_pregame_v2 p
     JOIN nhl.skater_game_logs_raw l ON (((l.game_id = p.game_id) AND (l.player_id = p.player_id))))
     LEFT JOIN nhl.team_context_rolling tc ON (((tc.game_id = p.game_id) AND (tc.team_id = p.team_id))))
  WHERE ((l.shots_on_goal IS NOT NULL) AND (p.season = 2025));


ALTER VIEW nhl.training_features_sog_denali_export OWNER TO postgres;

--
-- Name: user_props; Type: TABLE; Schema: nhl; Owner: postgres
--

CREATE TABLE nhl.user_props (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    prediction_timestamp timestamp with time zone DEFAULT now() NOT NULL,
    game_id bigint NOT NULL,
    game_date date,
    player_id bigint NOT NULL,
    player_name text,
    team text,
    team_id bigint,
    opponent_id bigint,
    prop_type text NOT NULL,
    prop_value numeric(6,2) NOT NULL,
    over_under text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    outcome text,
    prop_source text DEFAULT 'nhl_user_added'::text NOT NULL,
    predicted_outcome text,
    confidence_score double precision,
    user_id text,
    CONSTRAINT nhl_user_props_over_under_check CHECK ((over_under = ANY (ARRAY['over'::text, 'under'::text]))),
    CONSTRAINT nhl_user_props_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text])))
);


ALTER TABLE nhl.user_props OWNER TO postgres;

--
-- Name: v_dqa_goalie_ready_coverage; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_dqa_goalie_ready_coverage AS
 SELECT data_quality_audit.audit_date,
    ((data_quality_audit.result ->> 'rows_total'::text))::bigint AS rows_total,
    ((data_quality_audit.result ->> 'd10_shots_faced_per60_nn'::text))::bigint AS d10_shots_faced_per60_nn,
    ((data_quality_audit.result ->> 'd10_save_pct_nn'::text))::bigint AS d10_save_pct_nn,
    ((data_quality_audit.result ->> 'team_d10_sf_per_game_nn'::text))::bigint AS team_d10_sf_per_game_nn,
    ((data_quality_audit.result ->> 'opp_d10_sf_allowed_per_game_nn'::text))::bigint AS opp_d10_sf_allowed_per_game_nn,
    ((data_quality_audit.result ->> 'pace_index_nn'::text))::bigint AS pace_index_nn,
    ((data_quality_audit.result ->> 'rest_days_nn'::text))::bigint AS rest_days_nn,
    ((data_quality_audit.result ->> 'b2b_flag_nn'::text))::bigint AS b2b_flag_nn,
    ((data_quality_audit.result ->> 'd5_saves_per60_nn'::text))::bigint AS d5_saves_per60_nn,
    ((data_quality_audit.result ->> 'd10_saves_per60_nn'::text))::bigint AS d10_saves_per60_nn,
    ((data_quality_audit.result ->> 'd5_shots_faced_per60_nn'::text))::bigint AS d5_shots_faced_per60_nn,
    ((data_quality_audit.result ->> 'season_save_pct_nn'::text))::bigint AS season_save_pct_nn
   FROM nhl.data_quality_audit
  WHERE (data_quality_audit.check_name = 'goalie_ready_coverage'::text);


ALTER VIEW nhl.v_dqa_goalie_ready_coverage OWNER TO postgres;

--
-- Name: v_dqa_sog_ready_coverage; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_dqa_sog_ready_coverage AS
 SELECT data_quality_audit.audit_date,
    ((data_quality_audit.result ->> 'rows_total'::text))::bigint AS rows_total,
    ((data_quality_audit.result ->> 'd10_sog_per60_nn'::text))::bigint AS d10_sog_per60_nn,
    ((data_quality_audit.result ->> 'attempts_d10_per60_nn'::text))::bigint AS attempts_d10_per60_nn,
    ((data_quality_audit.result ->> 'team_d10_sf_per_game_nn'::text))::bigint AS team_d10_sf_per_game_nn,
    ((data_quality_audit.result ->> 'opp_d10_sf_allowed_per_game_nn'::text))::bigint AS opp_d10_sf_allowed_per_game_nn,
    ((data_quality_audit.result ->> 'pace_index_nn'::text))::bigint AS pace_index_nn,
    ((data_quality_audit.result ->> 'role_pp_share_nn'::text))::bigint AS role_pp_share_nn,
    ((data_quality_audit.result ->> 'rest_days_nn'::text))::bigint AS rest_days_nn,
    ((data_quality_audit.result ->> 'b2b_flag_nn'::text))::bigint AS b2b_flag_nn,
    ((data_quality_audit.result ->> 'opp_d10_sf_per60_nn'::text))::bigint AS opp_d10_sf_per60_nn,
    ((data_quality_audit.result ->> 'team_d10_sa_per60_nn'::text))::bigint AS team_d10_sa_per60_nn,
    ((data_quality_audit.result ->> 'pace_matchup_index_nn'::text))::bigint AS pace_matchup_index_nn
   FROM nhl.data_quality_audit
  WHERE (data_quality_audit.check_name = 'sog_ready_coverage'::text);


ALTER VIEW nhl.v_dqa_sog_ready_coverage OWNER TO postgres;

--
-- Name: v_site_sog_eval_publish; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_site_sog_eval_publish AS
 WITH latest_day AS (
         SELECT max(eval_sog_daily.game_date) AS game_date
           FROM nhl.eval_sog_daily
          WHERE ((eval_sog_daily.segment_type = 'all'::text) AND (eval_sog_daily.segment_value = 'all'::text) AND (eval_sog_daily.n_eval > 0))
        ), daily AS (
         SELECT e.game_date,
            e.line,
            e.games_on_date,
            e.is_low_sample,
            e.n_eval,
            e.n_pred,
            e.truth_coverage,
            e.hit_rate,
            e.avg_p,
            e.auc,
            e.logloss,
            e.brier
           FROM (nhl.eval_sog_daily e
             JOIN latest_day ld ON ((ld.game_date = e.game_date)))
          WHERE ((e.segment_type = 'all'::text) AND (e.segment_value = 'all'::text) AND (e.line = ANY (ARRAY[1.5, 2.5, 3.5])))
        ), week_all AS (
         SELECT e.line,
            sum(e.n_eval) AS n_eval_week,
            sum(e.n_pred) AS n_pred_week,
            ((sum(e.n_pos))::numeric / (NULLIF(sum(e.n_eval), 0))::numeric) AS hit_rate_pooled,
            (sum((e.brier * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS brier_w,
            (sum((e.logloss * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS logloss_w,
            (sum((e.avg_p * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS avg_p_w,
            ((sum(e.n_eval))::numeric / (NULLIF(sum(e.n_pred), 0))::numeric) AS truth_coverage_w,
            sum(
                CASE
                    WHEN e.is_low_sample THEN 1
                    ELSE 0
                END) AS low_sample_days,
            count(DISTINCT e.game_date) AS days_included
           FROM nhl.eval_sog_daily e
          WHERE ((e.segment_type = 'all'::text) AND (e.segment_value = 'all'::text) AND (e.line = ANY (ARRAY[1.5, 2.5, 3.5])) AND (e.game_date >= (CURRENT_DATE - 7)) AND (e.n_eval > 0))
          GROUP BY e.line
        ), week_strict AS (
         SELECT e.line,
            sum(e.n_eval) AS n_eval_week_strict,
            sum(e.n_pred) AS n_pred_week_strict,
            ((sum(e.n_pos))::numeric / (NULLIF(sum(e.n_eval), 0))::numeric) AS hit_rate_pooled_strict,
            (sum((e.brier * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS brier_w_strict,
            (sum((e.logloss * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS logloss_w_strict,
            (sum((e.avg_p * (e.n_eval)::numeric)) / (NULLIF(sum(e.n_eval), 0))::numeric) AS avg_p_w_strict,
            ((sum(e.n_eval))::numeric / (NULLIF(sum(e.n_pred), 0))::numeric) AS truth_coverage_w_strict,
            count(DISTINCT e.game_date) AS days_included_strict
           FROM nhl.eval_sog_daily e
          WHERE ((e.segment_type = 'all'::text) AND (e.segment_value = 'all'::text) AND (e.line = ANY (ARRAY[1.5, 2.5, 3.5])) AND (e.game_date >= (CURRENT_DATE - 7)) AND (e.n_eval > 0) AND (NOT e.is_low_sample))
          GROUP BY e.line
        )
 SELECT d.game_date AS latest_eval_date,
    d.line,
    d.games_on_date,
    d.is_low_sample,
    d.n_eval,
    d.n_pred,
    d.truth_coverage,
    d.hit_rate,
    d.avg_p,
    d.auc,
    d.logloss,
    d.brier,
    wa.days_included,
    wa.low_sample_days,
    wa.n_eval_week,
    wa.truth_coverage_w,
    wa.hit_rate_pooled,
    wa.avg_p_w,
    wa.logloss_w,
    wa.brier_w,
    ws.days_included_strict,
    ws.n_eval_week_strict,
    ws.truth_coverage_w_strict,
    ws.hit_rate_pooled_strict,
    ws.avg_p_w_strict,
    ws.logloss_w_strict,
    ws.brier_w_strict
   FROM ((daily d
     LEFT JOIN week_all wa USING (line))
     LEFT JOIN week_strict ws USING (line))
  ORDER BY d.line;


ALTER VIEW nhl.v_site_sog_eval_publish OWNER TO postgres;

--
-- Name: v_site_sog_predictions_publish; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_site_sog_predictions_publish AS
 WITH base AS (
         SELECT p.player_id,
            p.game_id,
            p.line,
            p.p_over,
            p.model_family,
            p.model_version,
            p.model_params,
            p.feature_hash,
            p.created_at,
            p.updated_at
           FROM nhl.predictions p
          WHERE ((p.prop = 'shots_on_goal'::text) AND (p.line = ANY (ARRAY[1.5, 2.5, 3.5])))
        ), dedup AS (
         SELECT DISTINCT ON (base.player_id, base.game_id, base.line, base.model_family, base.model_version) base.player_id,
            base.game_id,
            base.line,
            base.p_over,
            base.model_family,
            base.model_version,
            base.model_params,
            base.feature_hash,
            base.created_at,
            base.updated_at
           FROM base
          ORDER BY base.player_id, base.game_id, base.line, base.model_family, base.model_version, COALESCE(base.updated_at, base.created_at) DESC
        ), joined AS (
         SELECT d.player_id,
            pl.full_name,
            d.game_id,
            gm.game_date,
            gm.home_team_id,
            gm.away_team_id,
            COALESCE(pl.current_team_id, (pl.team_id)::bigint) AS team_id,
            d.line,
            d.p_over,
            d.model_family,
            d.model_version,
            d.model_params,
            d.feature_hash,
            d.created_at,
            d.updated_at,
            ((d.created_at AT TIME ZONE 'America/New_York'::text))::date AS created_date_et
           FROM ((dedup d
             JOIN nhl.games gm ON ((gm.game_id = d.game_id)))
             JOIN nhl.players pl ON ((pl.player_id = d.player_id)))
        )
 SELECT j.player_id,
    j.full_name,
    j.game_id,
    j.game_date,
    j.team_id,
    t.team AS team_abbr,
    t.full_team_name,
        CASE
            WHEN (j.team_id IS NULL) THEN NULL::boolean
            WHEN (j.team_id = j.home_team_id) THEN true
            WHEN (j.team_id = j.away_team_id) THEN false
            ELSE NULL::boolean
        END AS is_home,
        CASE
            WHEN (j.team_id IS NULL) THEN NULL::bigint
            WHEN (j.team_id = j.home_team_id) THEN j.away_team_id
            WHEN (j.team_id = j.away_team_id) THEN j.home_team_id
            ELSE NULL::bigint
        END AS opponent_id,
    ot.team AS opponent_abbr,
    ot.full_team_name AS opponent_full_team_name,
    j.line,
    j.p_over,
    j.model_family,
    j.model_version,
    j.feature_hash,
    j.created_at,
    j.updated_at,
    j.created_date_et
   FROM ((joined j
     LEFT JOIN nhl.teams t ON ((t.team_id = j.team_id)))
     LEFT JOIN nhl.teams ot ON ((ot.team_id =
        CASE
            WHEN (j.team_id IS NULL) THEN NULL::bigint
            WHEN (j.team_id = j.home_team_id) THEN j.away_team_id
            WHEN (j.team_id = j.away_team_id) THEN j.home_team_id
            ELSE NULL::bigint
        END)));


ALTER VIEW nhl.v_site_sog_predictions_publish OWNER TO postgres;

--
-- Name: v_skater_game_logs_played; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_skater_game_logs_played AS
 SELECT skater_game_logs_raw.player_id,
    skater_game_logs_raw.game_id,
    skater_game_logs_raw.team_id,
    skater_game_logs_raw.opponent_id,
    skater_game_logs_raw.toi_minutes,
    skater_game_logs_raw.pp_toi_minutes,
    skater_game_logs_raw.shots_on_goal,
    skater_game_logs_raw.shot_attempts,
    skater_game_logs_raw.ixg,
    skater_game_logs_raw.blocks,
    skater_game_logs_raw.hits,
    skater_game_logs_raw.penalties,
    skater_game_logs_raw.game_state_splits,
    skater_game_logs_raw.is_home,
    skater_game_logs_raw.game_date,
    skater_game_logs_raw.created_at
   FROM nhl.skater_game_logs_raw
  WHERE (COALESCE(skater_game_logs_raw.toi_minutes, (0)::numeric) > (0)::numeric);


ALTER VIEW nhl.v_skater_game_logs_played OWNER TO postgres;

--
-- Name: v_skater_logs_clean; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_skater_logs_clean AS
 SELECT l.player_id,
    l.game_id,
    g.game_date,
    l.team_id,
    (COALESCE((l.shots_on_goal)::integer, 0))::numeric AS sog,
    (COALESCE((l.shot_attempts)::integer, 0))::numeric AS attempts,
    (NULLIF(l.toi_minutes, (0)::numeric))::numeric AS toi_min,
    (NULLIF(l.pp_toi_minutes, (0)::numeric))::numeric AS pp_min
   FROM (nhl.skater_game_logs_raw l
     JOIN nhl.games g USING (game_id))
  WHERE (g.game_date IS NOT NULL);


ALTER VIEW nhl.v_skater_logs_clean OWNER TO postgres;

--
-- Name: v_skater_rolling_agg; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_skater_rolling_agg AS
 WITH base AS (
         SELECT skater_game_logs_raw.player_id,
            skater_game_logs_raw.game_id,
            skater_game_logs_raw.game_date,
            (COALESCE((skater_game_logs_raw.shots_on_goal)::integer, 0))::double precision AS sog,
            (COALESCE((skater_game_logs_raw.shot_attempts)::integer, 0))::double precision AS attempts,
            (NULLIF(skater_game_logs_raw.toi_minutes, (0)::numeric))::double precision AS toi_min,
            (NULLIF(skater_game_logs_raw.pp_toi_minutes, (0)::numeric))::double precision AS pp_toi_min
           FROM nhl.skater_game_logs_raw
          WHERE (skater_game_logs_raw.game_date IS NOT NULL)
        )
 SELECT base.player_id,
    base.game_id,
        CASE
            WHEN (NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5, (0)::double precision) IS NULL) THEN NULL::double precision
            ELSE (((60.0)::double precision * sum(base.sog) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5) / NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5, (0)::double precision))
        END AS d5_sog_per60,
        CASE
            WHEN (NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w10, (0)::double precision) IS NULL) THEN NULL::double precision
            ELSE (((60.0)::double precision * sum(base.sog) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w10) / NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w10, (0)::double precision))
        END AS d10_sog_per60,
        CASE
            WHEN (NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w20, (0)::double precision) IS NULL) THEN NULL::double precision
            ELSE (((60.0)::double precision * sum(base.sog) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w20) / NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w20, (0)::double precision))
        END AS d20_sog_per60,
        CASE
            WHEN (NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5, (0)::double precision) IS NULL) THEN NULL::double precision
            ELSE (((60.0)::double precision * sum(base.attempts) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5) / NULLIF(sum(base.toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w5, (0)::double precision))
        END AS d5_attempts_per60,
    avg(base.pp_toi_min) FILTER (WHERE (base.toi_min >= (5.0)::double precision)) OVER w10 AS d10_pp_toi,
    avg(
        CASE
            WHEN ((base.toi_min >= (5.0)::double precision) AND (base.toi_min > (0)::double precision) AND (base.pp_toi_min IS NOT NULL)) THEN (base.pp_toi_min / base.toi_min)
            ELSE NULL::double precision
        END) OVER w10 AS role_pp_share
   FROM base
  WINDOW w5 AS (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w10 AS (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), w20 AS (PARTITION BY base.player_id ORDER BY base.game_date, base.game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING);


ALTER VIEW nhl.v_skater_rolling_agg OWNER TO postgres;

--
-- Name: v_slate_saves_features; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_slate_saves_features AS
 SELECT r.player_id,
    r.game_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.game_date,
    r.d10_shots_faced_per60,
    r.d10_save_pct,
    r.team_d10_sf_per_game,
    r.opp_d10_sf_allowed_per_game,
    r.pace_index,
    r.rest_days,
    r.b2b_flag,
    r.d5_saves_per60,
    r.d10_saves_per60,
    r.d5_shots_faced_per60,
    r.season_save_pct,
    r.d20_saves_per60,
    r.start_prob,
    r.team_d10_sf_per_game AS team_d10_sf_per60,
    r.opp_d10_sf_allowed_per_game AS opp_d10_sa_per60,
    r.opp_d10_sf_per60,
    r.team_d10_sa_per60,
    r.pace_matchup_index
   FROM nhl.training_features_goalie_saves_v2_ready r;


ALTER VIEW nhl.v_slate_saves_features OWNER TO postgres;

--
-- Name: v_slate_sog_features; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_slate_sog_features AS
 SELECT r.player_id,
    r.game_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.game_date,
    r.season,
    r.shots_on_goal,
    r.d5_sog_per60,
    r.d10_sog_per60,
    r.d20_sog_per60,
    r.team_d10_sf_per_game,
    r.opp_d10_sf_allowed_per_game,
    r.pace_matchup_index,
    r.role_pp_share,
    r.rest_days,
    r.b2b_flag,
    r.attempts_d10_per60,
    r.last10_team_sog_share,
    r.hot_last5_flag,
    r.num_shotwasongoal_last5,
    r.num_shotwasongoal_last10,
    r.num_shotwasongoal_season_to_date,
    r.num_event_shot_last5,
    r.num_event_shot_last10,
    r.num_event_shot_season_to_date,
    r.team_num_event_shot_for_last10,
    r.team_num_shotwasongoal_for_last10,
    g.game_type,
    g.home_team_id,
    g.away_team_id,
    (r.team_id = g.home_team_id) AS is_home_team
   FROM (nhl.training_features_nhl_sog_enriched_pregame_v2 r
     JOIN nhl.games g ON ((r.game_id = g.game_id)));


ALTER VIEW nhl.v_slate_sog_features OWNER TO postgres;

--
-- Name: v_sog_denali_rollups_per_game; Type: VIEW; Schema: nhl; Owner: postgres
--

CREATE VIEW nhl.v_sog_denali_rollups_per_game AS
 WITH base AS (
         SELECT v_skater_logs_clean.player_id,
            v_skater_logs_clean.game_id,
            v_skater_logs_clean.team_id,
            v_skater_logs_clean.game_date,
            v_skater_logs_clean.sog,
            v_skater_logs_clean.attempts,
            v_skater_logs_clean.toi_min,
            sum(v_skater_logs_clean.sog) OVER w5 AS sum_sog_5,
            sum(v_skater_logs_clean.toi_min) OVER w5 AS sum_toi_5,
            sum(v_skater_logs_clean.sog) OVER w10 AS sum_sog_10,
            sum(v_skater_logs_clean.attempts) OVER w10 AS sum_att_10,
            sum(v_skater_logs_clean.toi_min) OVER w10 AS sum_toi_10,
            sum(v_skater_logs_clean.sog) OVER w20 AS sum_sog_20,
            sum(v_skater_logs_clean.toi_min) OVER w20 AS sum_toi_20,
            sum(v_skater_logs_clean.sog) OVER w5 AS num_sog_last5,
            sum(v_skater_logs_clean.sog) OVER w10 AS num_sog_last10,
            sum(v_skater_logs_clean.attempts) OVER w5 AS num_event_last5,
            sum(v_skater_logs_clean.attempts) OVER w10 AS num_event_last10
           FROM nhl.v_skater_logs_clean
          WINDOW w5 AS (PARTITION BY v_skater_logs_clean.player_id ORDER BY v_skater_logs_clean.game_date, v_skater_logs_clean.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w10 AS (PARTITION BY v_skater_logs_clean.player_id ORDER BY v_skater_logs_clean.game_date, v_skater_logs_clean.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), w20 AS (PARTITION BY v_skater_logs_clean.player_id ORDER BY v_skater_logs_clean.game_date, v_skater_logs_clean.game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
        )
 SELECT base.player_id,
    base.game_id,
    base.team_id,
    base.game_date,
        CASE
            WHEN (base.sum_toi_5 > (0)::numeric) THEN ((base.sum_sog_5 / base.sum_toi_5) * (60)::numeric)
            ELSE NULL::numeric
        END AS d5_sog_per60,
        CASE
            WHEN (base.sum_toi_10 > (0)::numeric) THEN ((base.sum_sog_10 / base.sum_toi_10) * (60)::numeric)
            ELSE NULL::numeric
        END AS d10_sog_per60,
        CASE
            WHEN (base.sum_toi_20 > (0)::numeric) THEN ((base.sum_sog_20 / base.sum_toi_20) * (60)::numeric)
            ELSE NULL::numeric
        END AS d20_sog_per60,
        CASE
            WHEN (base.sum_toi_10 > (0)::numeric) THEN ((base.sum_att_10 / base.sum_toi_10) * (60)::numeric)
            ELSE NULL::numeric
        END AS attempts_d10_per60,
    base.num_sog_last5,
    base.num_sog_last10,
    base.num_event_last5,
    base.num_event_last10
   FROM base;


ALTER VIEW nhl.v_sog_denali_rollups_per_game OWNER TO postgres;

--
-- Name: mlb_team_map; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mlb_team_map (
    abbr text NOT NULL,
    team_id bigint NOT NULL
);


ALTER TABLE public.mlb_team_map OWNER TO postgres;

--
-- Name: opp_starter_per_game; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.opp_starter_per_game (
    game_id bigint NOT NULL,
    team text NOT NULL,
    starter_pitcher_id bigint
);


ALTER TABLE public.opp_starter_per_game OWNER TO postgres;

--
-- Name: messages; Type: TABLE; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE TABLE realtime.messages (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
)
PARTITION BY RANGE (inserted_at);


ALTER TABLE realtime.messages OWNER TO supabase_realtime_admin;

--
-- Name: messages_2026_02_21; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_21 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_21 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_22; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_22 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_22 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_23; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_23 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_23 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_24; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_24 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_24 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_25; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_25 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_25 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_26; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_26 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_26 OWNER TO supabase_admin;

--
-- Name: messages_2026_02_27; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.messages_2026_02_27 (
    topic text NOT NULL,
    extension text NOT NULL,
    payload jsonb,
    event text,
    private boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


ALTER TABLE realtime.messages_2026_02_27 OWNER TO supabase_admin;

--
-- Name: schema_migrations; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.schema_migrations (
    version bigint NOT NULL,
    inserted_at timestamp(0) without time zone
);


ALTER TABLE realtime.schema_migrations OWNER TO supabase_admin;

--
-- Name: subscription; Type: TABLE; Schema: realtime; Owner: supabase_admin
--

CREATE TABLE realtime.subscription (
    id bigint NOT NULL,
    subscription_id uuid NOT NULL,
    entity regclass NOT NULL,
    filters realtime.user_defined_filter[] DEFAULT '{}'::realtime.user_defined_filter[] NOT NULL,
    claims jsonb NOT NULL,
    claims_role regrole GENERATED ALWAYS AS (realtime.to_regrole((claims ->> 'role'::text))) STORED NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    action_filter text DEFAULT '*'::text,
    CONSTRAINT subscription_action_filter_check CHECK ((action_filter = ANY (ARRAY['*'::text, 'INSERT'::text, 'UPDATE'::text, 'DELETE'::text])))
);


ALTER TABLE realtime.subscription OWNER TO supabase_admin;

--
-- Name: subscription_id_seq; Type: SEQUENCE; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE realtime.subscription ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME realtime.subscription_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: buckets; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.buckets (
    id text NOT NULL,
    name text NOT NULL,
    owner uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    public boolean DEFAULT false,
    avif_autodetection boolean DEFAULT false,
    file_size_limit bigint,
    allowed_mime_types text[],
    owner_id text,
    type storage.buckettype DEFAULT 'STANDARD'::storage.buckettype NOT NULL
);


ALTER TABLE storage.buckets OWNER TO supabase_storage_admin;

--
-- Name: COLUMN buckets.owner; Type: COMMENT; Schema: storage; Owner: supabase_storage_admin
--

COMMENT ON COLUMN storage.buckets.owner IS 'Field is deprecated, use owner_id instead';


--
-- Name: buckets_analytics; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.buckets_analytics (
    name text NOT NULL,
    type storage.buckettype DEFAULT 'ANALYTICS'::storage.buckettype NOT NULL,
    format text DEFAULT 'ICEBERG'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    deleted_at timestamp with time zone
);


ALTER TABLE storage.buckets_analytics OWNER TO supabase_storage_admin;

--
-- Name: buckets_vectors; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.buckets_vectors (
    id text NOT NULL,
    type storage.buckettype DEFAULT 'VECTOR'::storage.buckettype NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.buckets_vectors OWNER TO supabase_storage_admin;

--
-- Name: migrations; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.migrations (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    hash character varying(40) NOT NULL,
    executed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE storage.migrations OWNER TO supabase_storage_admin;

--
-- Name: objects; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.objects (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    bucket_id text,
    name text,
    owner uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    last_accessed_at timestamp with time zone DEFAULT now(),
    metadata jsonb,
    path_tokens text[] GENERATED ALWAYS AS (string_to_array(name, '/'::text)) STORED,
    version text,
    owner_id text,
    user_metadata jsonb
);


ALTER TABLE storage.objects OWNER TO supabase_storage_admin;

--
-- Name: COLUMN objects.owner; Type: COMMENT; Schema: storage; Owner: supabase_storage_admin
--

COMMENT ON COLUMN storage.objects.owner IS 'Field is deprecated, use owner_id instead';


--
-- Name: s3_multipart_uploads; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.s3_multipart_uploads (
    id text NOT NULL,
    in_progress_size bigint DEFAULT 0 NOT NULL,
    upload_signature text NOT NULL,
    bucket_id text NOT NULL,
    key text NOT NULL COLLATE pg_catalog."C",
    version text NOT NULL,
    owner_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    user_metadata jsonb,
    metadata jsonb
);


ALTER TABLE storage.s3_multipart_uploads OWNER TO supabase_storage_admin;

--
-- Name: s3_multipart_uploads_parts; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.s3_multipart_uploads_parts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    upload_id text NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    part_number integer NOT NULL,
    bucket_id text NOT NULL,
    key text NOT NULL COLLATE pg_catalog."C",
    etag text NOT NULL,
    owner_id text,
    version text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.s3_multipart_uploads_parts OWNER TO supabase_storage_admin;

--
-- Name: vector_indexes; Type: TABLE; Schema: storage; Owner: supabase_storage_admin
--

CREATE TABLE storage.vector_indexes (
    id text DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    bucket_id text NOT NULL,
    data_type text NOT NULL,
    dimension integer NOT NULL,
    distance_metric text NOT NULL,
    metadata_configuration jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE storage.vector_indexes OWNER TO supabase_storage_admin;

--
-- Name: messages_2026_02_21; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_21 FOR VALUES FROM ('2026-02-21 00:00:00') TO ('2026-02-22 00:00:00');


--
-- Name: messages_2026_02_22; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_22 FOR VALUES FROM ('2026-02-22 00:00:00') TO ('2026-02-23 00:00:00');


--
-- Name: messages_2026_02_23; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_23 FOR VALUES FROM ('2026-02-23 00:00:00') TO ('2026-02-24 00:00:00');


--
-- Name: messages_2026_02_24; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_24 FOR VALUES FROM ('2026-02-24 00:00:00') TO ('2026-02-25 00:00:00');


--
-- Name: messages_2026_02_25; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_25 FOR VALUES FROM ('2026-02-25 00:00:00') TO ('2026-02-26 00:00:00');


--
-- Name: messages_2026_02_26; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_26 FOR VALUES FROM ('2026-02-26 00:00:00') TO ('2026-02-27 00:00:00');


--
-- Name: messages_2026_02_27; Type: TABLE ATTACH; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages ATTACH PARTITION realtime.messages_2026_02_27 FOR VALUES FROM ('2026-02-27 00:00:00') TO ('2026-02-28 00:00:00');


--
-- Name: refresh_tokens id; Type: DEFAULT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens ALTER COLUMN id SET DEFAULT nextval('auth.refresh_tokens_id_seq'::regclass);


--
-- Name: games_season_audit audit_id; Type: DEFAULT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games_season_audit ALTER COLUMN audit_id SET DEFAULT nextval('nhl.games_season_audit_audit_id_seq'::regclass);


--
-- Name: games_write_audit audit_id; Type: DEFAULT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games_write_audit ALTER COLUMN audit_id SET DEFAULT nextval('nhl.games_write_audit_audit_id_seq'::regclass);


--
-- Name: predictions prediction_id; Type: DEFAULT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.predictions ALTER COLUMN prediction_id SET DEFAULT nextval('nhl.predictions_prediction_id_seq'::regclass);


--
-- Name: mfa_amr_claims amr_id_pk; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT amr_id_pk PRIMARY KEY (id);


--
-- Name: audit_log_entries audit_log_entries_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.audit_log_entries
    ADD CONSTRAINT audit_log_entries_pkey PRIMARY KEY (id);


--
-- Name: custom_oauth_providers custom_oauth_providers_identifier_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.custom_oauth_providers
    ADD CONSTRAINT custom_oauth_providers_identifier_key UNIQUE (identifier);


--
-- Name: custom_oauth_providers custom_oauth_providers_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.custom_oauth_providers
    ADD CONSTRAINT custom_oauth_providers_pkey PRIMARY KEY (id);


--
-- Name: flow_state flow_state_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.flow_state
    ADD CONSTRAINT flow_state_pkey PRIMARY KEY (id);


--
-- Name: identities identities_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_pkey PRIMARY KEY (id);


--
-- Name: identities identities_provider_id_provider_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_provider_id_provider_unique UNIQUE (provider_id, provider);


--
-- Name: instances instances_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.instances
    ADD CONSTRAINT instances_pkey PRIMARY KEY (id);


--
-- Name: mfa_amr_claims mfa_amr_claims_session_id_authentication_method_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT mfa_amr_claims_session_id_authentication_method_pkey UNIQUE (session_id, authentication_method);


--
-- Name: mfa_challenges mfa_challenges_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_challenges
    ADD CONSTRAINT mfa_challenges_pkey PRIMARY KEY (id);


--
-- Name: mfa_factors mfa_factors_last_challenged_at_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_last_challenged_at_key UNIQUE (last_challenged_at);


--
-- Name: mfa_factors mfa_factors_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_pkey PRIMARY KEY (id);


--
-- Name: oauth_authorizations oauth_authorizations_authorization_code_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_authorization_code_key UNIQUE (authorization_code);


--
-- Name: oauth_authorizations oauth_authorizations_authorization_id_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_authorization_id_key UNIQUE (authorization_id);


--
-- Name: oauth_authorizations oauth_authorizations_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_pkey PRIMARY KEY (id);


--
-- Name: oauth_client_states oauth_client_states_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_client_states
    ADD CONSTRAINT oauth_client_states_pkey PRIMARY KEY (id);


--
-- Name: oauth_clients oauth_clients_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_clients
    ADD CONSTRAINT oauth_clients_pkey PRIMARY KEY (id);


--
-- Name: oauth_consents oauth_consents_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_pkey PRIMARY KEY (id);


--
-- Name: oauth_consents oauth_consents_user_client_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_user_client_unique UNIQUE (user_id, client_id);


--
-- Name: one_time_tokens one_time_tokens_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.one_time_tokens
    ADD CONSTRAINT one_time_tokens_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_token_unique; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_token_unique UNIQUE (token);


--
-- Name: saml_providers saml_providers_entity_id_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_entity_id_key UNIQUE (entity_id);


--
-- Name: saml_providers saml_providers_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_pkey PRIMARY KEY (id);


--
-- Name: saml_relay_states saml_relay_states_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);


--
-- Name: sso_domains sso_domains_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_domains
    ADD CONSTRAINT sso_domains_pkey PRIMARY KEY (id);


--
-- Name: sso_providers sso_providers_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_providers
    ADD CONSTRAINT sso_providers_pkey PRIMARY KEY (id);


--
-- Name: users users_phone_key; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.users
    ADD CONSTRAINT users_phone_key UNIQUE (phone);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: webauthn_challenges webauthn_challenges_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.webauthn_challenges
    ADD CONSTRAINT webauthn_challenges_pkey PRIMARY KEY (id);


--
-- Name: webauthn_credentials webauthn_credentials_pkey; Type: CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.webauthn_credentials
    ADD CONSTRAINT webauthn_credentials_pkey PRIMARY KEY (id);


--
-- Name: bvp_stats bvp_stats_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.bvp_stats
    ADD CONSTRAINT bvp_stats_pkey PRIMARY KEY (game_id, batter_id, pitcher_id);


--
-- Name: game_info game_info_game_id_key; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.game_info
    ADD CONSTRAINT game_info_game_id_key UNIQUE (game_id);


--
-- Name: game_info game_info_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.game_info
    ADD CONSTRAINT game_info_pkey PRIMARY KEY (game_id);


--
-- Name: model_training_props model_training_props_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.model_training_props
    ADD CONSTRAINT model_training_props_pkey PRIMARY KEY (id);


--
-- Name: model_training_props mtp_team_text_no_leagues; Type: CHECK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.model_training_props
    ADD CONSTRAINT mtp_team_text_no_leagues CHECK (((team IS NULL) OR (upper(team) <> ALL (ARRAY['AL'::text, 'NL'::text])))) NOT VALID;


--
-- Name: model_training_props mtp_team_text_numeric; Type: CHECK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.model_training_props
    ADD CONSTRAINT mtp_team_text_numeric CHECK (((team IS NULL) OR (team = ''::text) OR (team ~ '^[0-9]+$'::text))) NOT VALID;


--
-- Name: player_derived_stats player_derived_stats_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_derived_stats_pkey PRIMARY KEY (id);


--
-- Name: player_derived_stats player_derived_stats_player_id_game_id_key; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_derived_stats_player_id_game_id_key UNIQUE (player_id, game_id);


--
-- Name: player_derived_stats player_derived_stats_unique_key; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_derived_stats_unique_key UNIQUE (player_id, game_id, game_date);


--
-- Name: player_derived_stats player_game_unique; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_game_unique UNIQUE (player_id, game_date);


--
-- Name: player_ids player_ids_no_unknown_real; Type: CHECK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_ids
    ADD CONSTRAINT player_ids_no_unknown_real CHECK (((is_placeholder = true) OR (COALESCE(lower(player_name), ''::text) !~~ 'unknown%'::text))) NOT VALID;


--
-- Name: player_ids player_ids_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_ids
    ADD CONSTRAINT player_ids_pkey PRIMARY KEY (id);


--
-- Name: player_ids player_ids_player_id_key; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_ids
    ADD CONSTRAINT player_ids_player_id_key UNIQUE (player_id);


--
-- Name: player_ids player_ids_player_id_unique; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_ids
    ADD CONSTRAINT player_ids_player_id_unique UNIQUE (player_id);


--
-- Name: player_profiles_cache player_profiles_cache_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_profiles_cache
    ADD CONSTRAINT player_profiles_cache_pkey PRIMARY KEY (player_id);


--
-- Name: player_props player_props_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_props
    ADD CONSTRAINT player_props_pkey PRIMARY KEY (id);


--
-- Name: player_stats player_stats_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_stats
    ADD CONSTRAINT player_stats_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: player_streak_history player_streak_history_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_streak_history
    ADD CONSTRAINT player_streak_history_pkey PRIMARY KEY (player_id, game_id, prop_type, prop_source);


--
-- Name: player_streak_profiles player_streak_profiles_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_streak_profiles
    ADD CONSTRAINT player_streak_profiles_pkey PRIMARY KEY (id);


--
-- Name: player_team_by_game player_team_by_game_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_team_by_game
    ADD CONSTRAINT player_team_by_game_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: prop_features_precomputed prop_features_precomputed_pkey; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.prop_features_precomputed
    ADD CONSTRAINT prop_features_precomputed_pkey PRIMARY KEY (prop_type, player_id, game_id, feature_set_tag);


--
-- Name: model_training_props unique_player_game_prop; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.model_training_props
    ADD CONSTRAINT unique_player_game_prop UNIQUE (player_id, game_id, prop_type, prop_source);


--
-- Name: player_streak_profiles unique_player_prop_source; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_streak_profiles
    ADD CONSTRAINT unique_player_prop_source UNIQUE (player_id, prop_type, prop_source);


--
-- Name: player_props uq_player_props_by_src_value; Type: CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_props
    ADD CONSTRAINT uq_player_props_by_src_value UNIQUE (prop_source, player_id, game_id, prop_type, prop_value);


--
-- Name: backfill_progress backfill_progress_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.backfill_progress
    ADD CONSTRAINT backfill_progress_pkey PRIMARY KEY (task);


--
-- Name: blocked_shot_events blocked_shot_events_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.blocked_shot_events
    ADD CONSTRAINT blocked_shot_events_pkey PRIMARY KEY (game_id, event_id);


--
-- Name: data_quality_audit data_quality_audit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.data_quality_audit
    ADD CONSTRAINT data_quality_audit_pkey PRIMARY KEY (audit_date, check_name);


--
-- Name: eval_sog_daily eval_sog_daily_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.eval_sog_daily
    ADD CONSTRAINT eval_sog_daily_pkey PRIMARY KEY (game_date, model_family, model_version, line, segment_type, segment_value);


--
-- Name: game_manpower_segments game_manpower_segments_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.game_manpower_segments
    ADD CONSTRAINT game_manpower_segments_pkey PRIMARY KEY (game_id, period, start_sec, end_sec, pp_team_id);


--
-- Name: games games_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);


--
-- Name: games_season_audit games_season_audit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games_season_audit
    ADD CONSTRAINT games_season_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: games games_unique_short; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_unique_short UNIQUE (season, short_game_id);


--
-- Name: games_write_audit games_write_audit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games_write_audit
    ADD CONSTRAINT games_write_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: goalie_rolling_agg goalie_rolling_agg_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_rolling_agg
    ADD CONSTRAINT goalie_rolling_agg_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: keep_games_filter keep_games_filter_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.keep_games_filter
    ADD CONSTRAINT keep_games_filter_pkey PRIMARY KEY (game_id);


--
-- Name: pairing_features_store_v2 pairing_features_store_v2_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.pairing_features_store_v2
    ADD CONSTRAINT pairing_features_store_v2_pkey PRIMARY KEY (game_id, player_id);


--
-- Name: pp_roles_slate pk_pp_roles_slate; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.pp_roles_slate
    ADD CONSTRAINT pk_pp_roles_slate PRIMARY KEY (game_date, team_id, player_id);


--
-- Name: player_external_ids player_external_ids_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_pkey PRIMARY KEY (player_id, provider);


--
-- Name: player_external_ids player_external_ids_provider_provider_player_id_key; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_provider_provider_player_id_key UNIQUE (provider, provider_player_id);


--
-- Name: players players_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);


--
-- Name: predictions predictions_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.predictions
    ADD CONSTRAINT predictions_pkey PRIMARY KEY (prediction_id);


--
-- Name: roster_names roster_names_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.roster_names
    ADD CONSTRAINT roster_names_pkey PRIMARY KEY (player_id);


--
-- Name: roster_status roster_status_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_pkey PRIMARY KEY (game_id, team_id, player_id, asof_ts);


--
-- Name: roster_status roster_status_unique; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_unique UNIQUE (team_id, asof_ts, game_id, player_id);


--
-- Name: shift_teammate_overlap_game shift_teammate_overlap_game_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shift_teammate_overlap_game
    ADD CONSTRAINT shift_teammate_overlap_game_pkey PRIMARY KEY (game_id, player_id, mate_id);


--
-- Name: shift_teammate_overlap_game_recent_v2 shift_teammate_overlap_game_recent_v2_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shift_teammate_overlap_game_recent_v2
    ADD CONSTRAINT shift_teammate_overlap_game_recent_v2_pkey PRIMARY KEY (game_id, player_id, teammate_id);


--
-- Name: shiftcharts_pairings_game shiftcharts_pairings_game_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shiftcharts_pairings_game
    ADD CONSTRAINT shiftcharts_pairings_game_pkey PRIMARY KEY (game_id, player_id);


--
-- Name: shiftcharts_raw shiftcharts_raw_pk; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shiftcharts_raw
    ADD CONSTRAINT shiftcharts_raw_pk PRIMARY KEY (game_id, shift_id);


--
-- Name: shiftcharts_raw shiftcharts_raw_shift_id_uniq; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shiftcharts_raw
    ADD CONSTRAINT shiftcharts_raw_shift_id_uniq UNIQUE (shift_id);


--
-- Name: shiftcharts_shifts shiftcharts_shifts_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shiftcharts_shifts
    ADD CONSTRAINT shiftcharts_shifts_pkey PRIMARY KEY (game_id, shift_id);


--
-- Name: shot_on_goal_events shot_on_goal_events_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shot_on_goal_events
    ADD CONSTRAINT shot_on_goal_events_pkey PRIMARY KEY (game_id, event_id);


--
-- Name: shot_stats_denali shot_stats_denali_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shot_stats_denali
    ADD CONSTRAINT shot_stats_denali_pkey PRIMARY KEY (shooterplayerid, game_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: skater_game_special_teams_exposure skater_game_special_teams_exposure_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_game_special_teams_exposure
    ADD CONSTRAINT skater_game_special_teams_exposure_pkey PRIMARY KEY (game_id, player_id);


--
-- Name: skater_points_raw skater_points_raw_pk; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_points_raw
    ADD CONSTRAINT skater_points_raw_pk PRIMARY KEY (player_id, game_id);


--
-- Name: skater_rolling_agg skater_rolling_agg_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: team_context_rolling team_context_rolling_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_context_rolling
    ADD CONSTRAINT team_context_rolling_pkey PRIMARY KEY (team_id, game_id);


--
-- Name: team_external_ids team_external_ids_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_pkey PRIMARY KEY (team_id, provider);


--
-- Name: team_external_ids team_external_ids_provider_provider_team_id_key; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_provider_provider_team_id_key UNIQUE (provider, provider_team_id);


--
-- Name: team_game_sit team_game_sit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_game_sit
    ADD CONSTRAINT team_game_sit_pkey PRIMARY KEY (game_id, team_code, situation);


--
-- Name: teams teams_abbr_key; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_abbr_key UNIQUE (team);


--
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- Name: tf_team_roll10 tf_team_roll10_team_date_unique; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.tf_team_roll10
    ADD CONSTRAINT tf_team_roll10_team_date_unique UNIQUE (team_id, game_date);


--
-- Name: training_features_goalie_saves_v2 training_features_goalie_saves_v2_pk; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.training_features_goalie_saves_v2
    ADD CONSTRAINT training_features_goalie_saves_v2_pk PRIMARY KEY (player_id, game_id);


--
-- Name: player_external_ids uq_player_external_ids_player_provider; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT uq_player_external_ids_player_provider UNIQUE (player_id, provider);


--
-- Name: predictions uq_pred; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.predictions
    ADD CONSTRAINT uq_pred UNIQUE (prop, player_id, game_id, line, feature_hash);


--
-- Name: user_props user_props_pkey; Type: CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_pkey PRIMARY KEY (id);


--
-- Name: mlb_team_map mlb_team_map_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mlb_team_map
    ADD CONSTRAINT mlb_team_map_pkey PRIMARY KEY (abbr);


--
-- Name: opp_starter_per_game opp_starter_per_game_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.opp_starter_per_game
    ADD CONSTRAINT opp_starter_per_game_pkey PRIMARY KEY (game_id, team);


--
-- Name: messages messages_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER TABLE ONLY realtime.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_21 messages_2026_02_21_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_21
    ADD CONSTRAINT messages_2026_02_21_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_22 messages_2026_02_22_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_22
    ADD CONSTRAINT messages_2026_02_22_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_23 messages_2026_02_23_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_23
    ADD CONSTRAINT messages_2026_02_23_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_24 messages_2026_02_24_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_24
    ADD CONSTRAINT messages_2026_02_24_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_25 messages_2026_02_25_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_25
    ADD CONSTRAINT messages_2026_02_25_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_26 messages_2026_02_26_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_26
    ADD CONSTRAINT messages_2026_02_26_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: messages_2026_02_27 messages_2026_02_27_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.messages_2026_02_27
    ADD CONSTRAINT messages_2026_02_27_pkey PRIMARY KEY (id, inserted_at);


--
-- Name: subscription pk_subscription; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.subscription
    ADD CONSTRAINT pk_subscription PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: realtime; Owner: supabase_admin
--

ALTER TABLE ONLY realtime.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: buckets_analytics buckets_analytics_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.buckets_analytics
    ADD CONSTRAINT buckets_analytics_pkey PRIMARY KEY (id);


--
-- Name: buckets buckets_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.buckets
    ADD CONSTRAINT buckets_pkey PRIMARY KEY (id);


--
-- Name: buckets_vectors buckets_vectors_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.buckets_vectors
    ADD CONSTRAINT buckets_vectors_pkey PRIMARY KEY (id);


--
-- Name: migrations migrations_name_key; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.migrations
    ADD CONSTRAINT migrations_name_key UNIQUE (name);


--
-- Name: migrations migrations_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.migrations
    ADD CONSTRAINT migrations_pkey PRIMARY KEY (id);


--
-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_pkey PRIMARY KEY (id);


--
-- Name: s3_multipart_uploads s3_multipart_uploads_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads
    ADD CONSTRAINT s3_multipart_uploads_pkey PRIMARY KEY (id);


--
-- Name: vector_indexes vector_indexes_pkey; Type: CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.vector_indexes
    ADD CONSTRAINT vector_indexes_pkey PRIMARY KEY (id);


--
-- Name: audit_logs_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX audit_logs_instance_id_idx ON auth.audit_log_entries USING btree (instance_id);


--
-- Name: confirmation_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX confirmation_token_idx ON auth.users USING btree (confirmation_token) WHERE ((confirmation_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: custom_oauth_providers_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX custom_oauth_providers_created_at_idx ON auth.custom_oauth_providers USING btree (created_at);


--
-- Name: custom_oauth_providers_enabled_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX custom_oauth_providers_enabled_idx ON auth.custom_oauth_providers USING btree (enabled);


--
-- Name: custom_oauth_providers_identifier_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX custom_oauth_providers_identifier_idx ON auth.custom_oauth_providers USING btree (identifier);


--
-- Name: custom_oauth_providers_provider_type_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX custom_oauth_providers_provider_type_idx ON auth.custom_oauth_providers USING btree (provider_type);


--
-- Name: email_change_token_current_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX email_change_token_current_idx ON auth.users USING btree (email_change_token_current) WHERE ((email_change_token_current)::text !~ '^[0-9 ]*$'::text);


--
-- Name: email_change_token_new_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX email_change_token_new_idx ON auth.users USING btree (email_change_token_new) WHERE ((email_change_token_new)::text !~ '^[0-9 ]*$'::text);


--
-- Name: factor_id_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX factor_id_created_at_idx ON auth.mfa_factors USING btree (user_id, created_at);


--
-- Name: flow_state_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX flow_state_created_at_idx ON auth.flow_state USING btree (created_at DESC);


--
-- Name: identities_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX identities_email_idx ON auth.identities USING btree (email text_pattern_ops);


--
-- Name: INDEX identities_email_idx; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON INDEX auth.identities_email_idx IS 'Auth: Ensures indexed queries on the email column';


--
-- Name: identities_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX identities_user_id_idx ON auth.identities USING btree (user_id);


--
-- Name: idx_auth_code; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX idx_auth_code ON auth.flow_state USING btree (auth_code);


--
-- Name: idx_oauth_client_states_created_at; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX idx_oauth_client_states_created_at ON auth.oauth_client_states USING btree (created_at);


--
-- Name: idx_user_id_auth_method; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX idx_user_id_auth_method ON auth.flow_state USING btree (user_id, authentication_method);


--
-- Name: mfa_challenge_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX mfa_challenge_created_at_idx ON auth.mfa_challenges USING btree (created_at DESC);


--
-- Name: mfa_factors_user_friendly_name_unique; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX mfa_factors_user_friendly_name_unique ON auth.mfa_factors USING btree (friendly_name, user_id) WHERE (TRIM(BOTH FROM friendly_name) <> ''::text);


--
-- Name: mfa_factors_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX mfa_factors_user_id_idx ON auth.mfa_factors USING btree (user_id);


--
-- Name: oauth_auth_pending_exp_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_auth_pending_exp_idx ON auth.oauth_authorizations USING btree (expires_at) WHERE (status = 'pending'::auth.oauth_authorization_status);


--
-- Name: oauth_clients_deleted_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_clients_deleted_at_idx ON auth.oauth_clients USING btree (deleted_at);


--
-- Name: oauth_consents_active_client_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_active_client_idx ON auth.oauth_consents USING btree (client_id) WHERE (revoked_at IS NULL);


--
-- Name: oauth_consents_active_user_client_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_active_user_client_idx ON auth.oauth_consents USING btree (user_id, client_id) WHERE (revoked_at IS NULL);


--
-- Name: oauth_consents_user_order_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX oauth_consents_user_order_idx ON auth.oauth_consents USING btree (user_id, granted_at DESC);


--
-- Name: one_time_tokens_relates_to_hash_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX one_time_tokens_relates_to_hash_idx ON auth.one_time_tokens USING hash (relates_to);


--
-- Name: one_time_tokens_token_hash_hash_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX one_time_tokens_token_hash_hash_idx ON auth.one_time_tokens USING hash (token_hash);


--
-- Name: one_time_tokens_user_id_token_type_key; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX one_time_tokens_user_id_token_type_key ON auth.one_time_tokens USING btree (user_id, token_type);


--
-- Name: reauthentication_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX reauthentication_token_idx ON auth.users USING btree (reauthentication_token) WHERE ((reauthentication_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: recovery_token_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX recovery_token_idx ON auth.users USING btree (recovery_token) WHERE ((recovery_token)::text !~ '^[0-9 ]*$'::text);


--
-- Name: refresh_tokens_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_instance_id_idx ON auth.refresh_tokens USING btree (instance_id);


--
-- Name: refresh_tokens_instance_id_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_instance_id_user_id_idx ON auth.refresh_tokens USING btree (instance_id, user_id);


--
-- Name: refresh_tokens_parent_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_parent_idx ON auth.refresh_tokens USING btree (parent);


--
-- Name: refresh_tokens_session_id_revoked_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_session_id_revoked_idx ON auth.refresh_tokens USING btree (session_id, revoked);


--
-- Name: refresh_tokens_updated_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX refresh_tokens_updated_at_idx ON auth.refresh_tokens USING btree (updated_at DESC);


--
-- Name: saml_providers_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_providers_sso_provider_id_idx ON auth.saml_providers USING btree (sso_provider_id);


--
-- Name: saml_relay_states_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_created_at_idx ON auth.saml_relay_states USING btree (created_at DESC);


--
-- Name: saml_relay_states_for_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_for_email_idx ON auth.saml_relay_states USING btree (for_email);


--
-- Name: saml_relay_states_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX saml_relay_states_sso_provider_id_idx ON auth.saml_relay_states USING btree (sso_provider_id);


--
-- Name: sessions_not_after_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_not_after_idx ON auth.sessions USING btree (not_after DESC);


--
-- Name: sessions_oauth_client_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_oauth_client_id_idx ON auth.sessions USING btree (oauth_client_id);


--
-- Name: sessions_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sessions_user_id_idx ON auth.sessions USING btree (user_id);


--
-- Name: sso_domains_domain_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX sso_domains_domain_idx ON auth.sso_domains USING btree (lower(domain));


--
-- Name: sso_domains_sso_provider_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sso_domains_sso_provider_id_idx ON auth.sso_domains USING btree (sso_provider_id);


--
-- Name: sso_providers_resource_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX sso_providers_resource_id_idx ON auth.sso_providers USING btree (lower(resource_id));


--
-- Name: sso_providers_resource_id_pattern_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX sso_providers_resource_id_pattern_idx ON auth.sso_providers USING btree (resource_id text_pattern_ops);


--
-- Name: unique_phone_factor_per_user; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX unique_phone_factor_per_user ON auth.mfa_factors USING btree (user_id, phone);


--
-- Name: user_id_created_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX user_id_created_at_idx ON auth.sessions USING btree (user_id, created_at);


--
-- Name: users_email_partial_key; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX users_email_partial_key ON auth.users USING btree (email) WHERE (is_sso_user = false);


--
-- Name: INDEX users_email_partial_key; Type: COMMENT; Schema: auth; Owner: supabase_auth_admin
--

COMMENT ON INDEX auth.users_email_partial_key IS 'Auth: A partial unique index that applies only when is_sso_user is false';


--
-- Name: users_instance_id_email_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_instance_id_email_idx ON auth.users USING btree (instance_id, lower((email)::text));


--
-- Name: users_instance_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_instance_id_idx ON auth.users USING btree (instance_id);


--
-- Name: users_is_anonymous_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX users_is_anonymous_idx ON auth.users USING btree (is_anonymous);


--
-- Name: webauthn_challenges_expires_at_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX webauthn_challenges_expires_at_idx ON auth.webauthn_challenges USING btree (expires_at);


--
-- Name: webauthn_challenges_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX webauthn_challenges_user_id_idx ON auth.webauthn_challenges USING btree (user_id);


--
-- Name: webauthn_credentials_credential_id_key; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE UNIQUE INDEX webauthn_credentials_credential_id_key ON auth.webauthn_credentials USING btree (credential_id);


--
-- Name: webauthn_credentials_user_id_idx; Type: INDEX; Schema: auth; Owner: supabase_auth_admin
--

CREATE INDEX webauthn_credentials_user_id_idx ON auth.webauthn_credentials USING btree (user_id);


--
-- Name: bvp_stats_game_batter_pitcher_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX bvp_stats_game_batter_pitcher_idx ON mlb.bvp_stats USING btree (game_id, batter_id, pitcher_id);


--
-- Name: bvp_stats_game_batter_pitcher_uniq; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX bvp_stats_game_batter_pitcher_uniq ON mlb.bvp_stats USING btree (game_id, batter_id, pitcher_id);


--
-- Name: idx_bvp_stats_batter_pitcher; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_bvp_stats_batter_pitcher ON mlb.bvp_stats USING btree (batter_id, pitcher_id);


--
-- Name: idx_bvp_stats_game_batter_pitcher; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_bvp_stats_game_batter_pitcher ON mlb.bvp_stats USING btree (game_id, batter_id, pitcher_id);


--
-- Name: idx_bvp_stats_pitcher_game; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_bvp_stats_pitcher_game ON mlb.bvp_stats USING btree (pitcher_id, game_id);


--
-- Name: idx_game_info_away_team; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_game_info_away_team ON mlb.game_info USING btree (away_team_id);


--
-- Name: idx_game_info_game_date; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_game_info_game_date ON mlb.game_info USING btree (game_date);


--
-- Name: idx_game_info_home_team; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_game_info_home_team ON mlb.game_info USING btree (home_team_id);


--
-- Name: idx_line_diff; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_line_diff ON mlb.model_training_props USING btree (line_diff);


--
-- Name: idx_model_training_prediction_filter; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_model_training_prediction_filter ON mlb.model_training_props USING btree (prop_type, prop_source);


--
-- Name: idx_model_training_props_game_date_player_id; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_model_training_props_game_date_player_id ON mlb.model_training_props USING btree (game_date, player_id);


--
-- Name: idx_model_training_props_outcome; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_model_training_props_outcome ON mlb.model_training_props USING btree (outcome);


--
-- Name: idx_model_training_props_predicted_outcome; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_model_training_props_predicted_outcome ON mlb.model_training_props USING btree (predicted_outcome);


--
-- Name: idx_model_training_props_prop_type; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_model_training_props_prop_type ON mlb.model_training_props USING btree (prop_type);


--
-- Name: idx_mtp_lookup; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_lookup ON mlb.model_training_props USING btree (player_id, game_id, prop_type, created_at DESC);


--
-- Name: idx_mtp_pid_gid; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_pid_gid ON mlb.model_training_props USING btree (player_id, game_id);


--
-- Name: idx_mtp_profile_player_date_mlb_api; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_profile_player_date_mlb_api ON mlb.model_training_props USING btree (player_id, game_date DESC) WHERE (prop_source = 'mlb_api'::text);


--
-- Name: idx_mtp_profile_player_prop_date_mlb_api; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_profile_player_prop_date_mlb_api ON mlb.model_training_props USING btree (player_id, prop_type, game_date DESC) WHERE (prop_source = 'mlb_api'::text);


--
-- Name: idx_mtp_prop_date; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_prop_date ON mlb.model_training_props USING btree (prop_type, game_date);


--
-- Name: idx_mtp_type_date_labeled; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_mtp_type_date_labeled ON mlb.model_training_props USING btree (prop_type, game_date) WHERE (result = ANY (ARRAY[(0)::double precision, (1)::double precision]));


--
-- Name: idx_pds_pid_gid; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_pds_pid_gid ON mlb.player_derived_stats USING btree (player_id, game_id);


--
-- Name: idx_pfp_by_game; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_pfp_by_game ON mlb.prop_features_precomputed USING btree (game_id, prop_type);


--
-- Name: idx_pfp_by_player_date; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_pfp_by_player_date ON mlb.prop_features_precomputed USING btree (player_id, game_date);


--
-- Name: idx_player_stats_bat_order; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_player_stats_bat_order ON mlb.player_stats USING btree (player_id, game_date, game_id);


--
-- Name: idx_predicted_outcome_incomplete; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_predicted_outcome_incomplete ON mlb.model_training_props USING btree (game_date, predicted_outcome, outcome);


--
-- Name: idx_prop_type_outcome; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_prop_type_outcome ON mlb.model_training_props USING btree (prop_type, outcome);


--
-- Name: idx_ps_pid_gdate_pos; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_ps_pid_gdate_pos ON mlb.player_stats USING btree (player_id, game_date DESC) WHERE ("position" IS NOT NULL);


--
-- Name: idx_ps_player_date; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_ps_player_date ON mlb.player_stats USING btree (player_id, game_date);


--
-- Name: idx_psh_pid_gdate; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_psh_pid_gdate ON mlb.player_streak_history USING btree (player_id, game_date);


--
-- Name: idx_psh_pid_gid_prop; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_psh_pid_gid_prop ON mlb.player_streak_history USING btree (player_id, game_id, prop_type);


--
-- Name: idx_today_market_snapshot_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX idx_today_market_snapshot_key ON mlb.today_market_snapshot USING btree (player_id, game_id, prop_type, line);


--
-- Name: idx_today_market_timing_signal_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX idx_today_market_timing_signal_key ON mlb.today_market_timing_signal USING btree (player_id, game_id, prop_type, line);


--
-- Name: idx_today_odds_book; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_today_odds_book ON mlb.today_odds_book_rows USING btree (slate_date, bookmaker_key);


--
-- Name: idx_today_odds_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_today_odds_key ON mlb.today_odds_book_rows USING btree (slate_date, game_id, player_id, prop_type, line, snapshot_ts);


--
-- Name: idx_today_player_context_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX idx_today_player_context_key ON mlb.today_player_context USING btree (player_id, prop_type);


--
-- Name: idx_today_slate_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_today_slate_key ON mlb.today_slate_rows USING btree (slate_date, game_id, player_id, prop_type, line);


--
-- Name: idx_today_wide_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX idx_today_wide_key ON mlb.today_wide_rows USING btree (slate_date, game_id, player_id, prop_type);


--
-- Name: idx_today_workspace_mlb_key; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX idx_today_workspace_mlb_key ON mlb.today_workspace_mlb USING btree (player_id, game_id, prop_type, line, side);


--
-- Name: model_training_props_game_player_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX model_training_props_game_player_idx ON mlb.model_training_props USING btree (game_id, player_id);


--
-- Name: model_training_props_gpp_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX model_training_props_gpp_idx ON mlb.model_training_props USING btree (game_id, player_id, prop_type);


--
-- Name: model_training_props_player_id_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX model_training_props_player_id_idx ON mlb.model_training_props USING btree (player_id);


--
-- Name: model_training_props_unique_src_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX model_training_props_unique_src_idx ON mlb.model_training_props USING btree (game_id, player_id, prop_type, prop_source) WHERE (prop_source IS NOT NULL);


--
-- Name: mtp_ix_player_game_null; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX mtp_ix_player_game_null ON mlb.model_training_props USING btree (player_id, game_id) WHERE (team_id IS NULL);


--
-- Name: mtp_ix_team_id; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX mtp_ix_team_id ON mlb.model_training_props USING btree (team_id);


--
-- Name: mtp_ix_team_norm_null; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX mtp_ix_team_norm_null ON mlb.model_training_props USING btree (TRIM(BOTH FROM upper(team))) WHERE ((team_id IS NULL) AND (team IS NOT NULL) AND (team <> ''::text));


--
-- Name: player_derived_stats_game_player_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_derived_stats_game_player_idx ON mlb.player_derived_stats USING btree (game_id, player_id);


--
-- Name: player_derived_stats_game_player_uniq; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX player_derived_stats_game_player_uniq ON mlb.player_derived_stats USING btree (game_id, player_id);


--
-- Name: player_ids_unique_player_team_real; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX player_ids_unique_player_team_real ON mlb.player_ids USING btree (player_name, team) WHERE (is_placeholder = false);


--
-- Name: player_profiles_cache_player_id_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_profiles_cache_player_id_idx ON mlb.player_profiles_cache USING btree (player_id);


--
-- Name: player_props_game_id_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_props_game_id_idx ON mlb.player_props USING btree (game_id);


--
-- Name: player_props_player_id_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_props_player_id_idx ON mlb.player_props USING btree (player_id);


--
-- Name: player_stats_game_player_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_stats_game_player_idx ON mlb.player_stats USING btree (game_id, player_id);


--
-- Name: player_stats_game_player_uniq; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX player_stats_game_player_uniq ON mlb.player_stats USING btree (game_id, player_id);


--
-- Name: player_stats_player_id_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_stats_player_id_idx ON mlb.player_stats USING btree (player_id);


--
-- Name: player_stats_starter_candidates_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_stats_starter_candidates_idx ON mlb.player_stats USING btree (game_id, team, player_id) WHERE (("position" = 'P'::text) AND (strikeouts_pitching IS NOT NULL));


--
-- Name: player_streak_profiles_player_id_prop_type_idx; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX player_streak_profiles_player_id_prop_type_idx ON mlb.player_streak_profiles USING btree (player_id, prop_type);


--
-- Name: ps_ix_player_game; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE INDEX ps_ix_player_game ON mlb.player_stats USING btree (player_id, game_id);


--
-- Name: unique_player_prop_streak; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX unique_player_prop_streak ON mlb.player_streak_profiles USING btree (player_id, prop_type, prop_source);


--
-- Name: unique_player_team; Type: INDEX; Schema: mlb; Owner: postgres
--

CREATE UNIQUE INDEX unique_player_team ON mlb.player_ids USING btree (player_name, team);


--
-- Name: eval_sog_daily_game_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX eval_sog_daily_game_date_idx ON nhl.eval_sog_daily USING btree (game_date);


--
-- Name: eval_sog_daily_segment_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX eval_sog_daily_segment_idx ON nhl.eval_sog_daily USING btree (segment_type, segment_value);


--
-- Name: goalie_logs_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX goalie_logs_game_idx ON nhl.goalie_game_logs_raw USING btree (game_id);


--
-- Name: goalie_logs_opp_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX goalie_logs_opp_idx ON nhl.goalie_game_logs_raw USING btree (opponent_id);


--
-- Name: goalie_logs_team_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX goalie_logs_team_idx ON nhl.goalie_game_logs_raw USING btree (team_id);


--
-- Name: goalie_roll_feats_m_uniq; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX goalie_roll_feats_m_uniq ON nhl.goalie_roll_feats_m USING btree (player_id, game_id);


--
-- Name: goalie_roll_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX goalie_roll_idx ON nhl.goalie_rolling_agg USING btree (game_id);


--
-- Name: gss_player_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX gss_player_idx ON nhl.goalies_szn_sit USING btree (player_id);


--
-- Name: gss_player_sit_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX gss_player_sit_idx ON nhl.goalies_szn_sit USING btree (player_id, season, situation);


--
-- Name: gss_sit_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX gss_sit_idx ON nhl.goalies_szn_sit USING btree (situation);


--
-- Name: gss_team_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX gss_team_idx ON nhl.goalies_szn_sit USING btree (team_abbr, season);


--
-- Name: idx_feat_saves_goalie_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_feat_saves_goalie_date ON nhl.training_features_nhl_saves_enriched USING btree (player_id, game_date);


--
-- Name: idx_games_game_id_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_games_game_id_date ON nhl.games USING btree (game_id, game_date);


--
-- Name: idx_games_season_short; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_games_season_short ON nhl.games USING btree (season, short_game_id);


--
-- Name: idx_games_teams_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_games_teams_date ON nhl.games USING btree (game_date, home_team_id, away_team_id);


--
-- Name: idx_goalie_raw_game_id; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_goalie_raw_game_id ON nhl.goalie_game_logs_raw USING btree (game_id);


--
-- Name: idx_goalie_raw_player_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_goalie_raw_player_date ON nhl.goalie_game_logs_raw USING btree (player_id, game_date);


--
-- Name: idx_goalie_raw_team_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_goalie_raw_team_date ON nhl.goalie_game_logs_raw USING btree (team_id, game_date);


--
-- Name: idx_nhl_blocked_shot_events_blocking_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_blocked_shot_events_blocking_team ON nhl.blocked_shot_events USING btree (blocking_team_id, game_date);


--
-- Name: idx_nhl_blocked_shot_events_game_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_blocked_shot_events_game_date ON nhl.blocked_shot_events USING btree (game_date);


--
-- Name: idx_nhl_blocked_shot_events_shooter_pos; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_blocked_shot_events_shooter_pos ON nhl.blocked_shot_events USING btree (blocking_team_id, shooter_position_bucket, game_date);


--
-- Name: idx_nhl_blocked_shot_events_shooting_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_blocked_shot_events_shooting_team ON nhl.blocked_shot_events USING btree (shooting_team_id, game_date);


--
-- Name: idx_nhl_sog_events_abs_sec; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_sog_events_abs_sec ON nhl.shot_on_goal_events USING btree (game_id, event_abs_sec);


--
-- Name: idx_nhl_sog_events_defending_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_sog_events_defending_team ON nhl.shot_on_goal_events USING btree (defending_team_id, game_date);


--
-- Name: idx_nhl_sog_events_game_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_sog_events_game_date ON nhl.shot_on_goal_events USING btree (game_date);


--
-- Name: idx_nhl_sog_events_shooter_pos; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_sog_events_shooter_pos ON nhl.shot_on_goal_events USING btree (defending_team_id, shooter_position_bucket, game_date);


--
-- Name: idx_nhl_sog_events_shooting_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_nhl_sog_events_shooting_team ON nhl.shot_on_goal_events USING btree (shooting_team_id, game_date);


--
-- Name: idx_overlap_recent_v2_game_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_overlap_recent_v2_game_player ON nhl.shift_teammate_overlap_game_recent_v2 USING btree (game_id, player_id);


--
-- Name: idx_overlap_recent_v2_game_team_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_overlap_recent_v2_game_team_player ON nhl.shift_teammate_overlap_game_recent_v2 USING btree (game_id, team_id, player_id);


--
-- Name: idx_pairing_store_v2_game_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_pairing_store_v2_game_team ON nhl.pairing_features_store_v2 USING btree (game_id, team_id);


--
-- Name: idx_pairing_store_v2_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_pairing_store_v2_player ON nhl.pairing_features_store_v2 USING btree (player_id);


--
-- Name: idx_predictions_created; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_predictions_created ON nhl.predictions USING btree (created_at);


--
-- Name: idx_predictions_lookup; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_predictions_lookup ON nhl.predictions USING btree (prop, game_id, player_id, line);


--
-- Name: idx_saves_ready_date_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_saves_ready_date_game ON nhl.training_features_goalie_saves_v2_ready USING btree (game_date, game_id);


--
-- Name: idx_saves_ready_date_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_saves_ready_date_player ON nhl.training_features_goalie_saves_v2_ready USING btree (game_date, player_id);


--
-- Name: idx_shift_teammate_overlap_game_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shift_teammate_overlap_game_game ON nhl.shift_teammate_overlap_game USING btree (game_id);


--
-- Name: idx_shift_teammate_overlap_game_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shift_teammate_overlap_game_player ON nhl.shift_teammate_overlap_game USING btree (player_id, game_id);


--
-- Name: idx_shiftcharts_pairings_game_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_pairings_game_team ON nhl.shiftcharts_pairings_game USING btree (game_id, team_id);


--
-- Name: idx_shiftcharts_raw_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_game ON nhl.shiftcharts_raw USING btree (game_id);


--
-- Name: idx_shiftcharts_raw_game_period; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_game_period ON nhl.shiftcharts_raw USING btree (game_id, period);


--
-- Name: idx_shiftcharts_raw_game_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_game_team ON nhl.shiftcharts_raw USING btree (game_id, team_abbrev);


--
-- Name: idx_shiftcharts_raw_json_gin; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_json_gin ON nhl.shiftcharts_raw USING gin (raw_json);


--
-- Name: idx_shiftcharts_raw_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_player ON nhl.shiftcharts_raw USING btree (player_id);


--
-- Name: idx_shiftcharts_raw_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_raw_player_game ON nhl.shiftcharts_raw USING btree (player_id, game_id);


--
-- Name: idx_shiftcharts_shifts_game_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_shifts_game_player ON nhl.shiftcharts_shifts USING btree (game_id, player_id);


--
-- Name: idx_shiftcharts_shifts_game_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shiftcharts_shifts_game_team ON nhl.shiftcharts_shifts USING btree (game_id, team_id);


--
-- Name: idx_shot_stats_denali_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shot_stats_denali_game ON nhl.shot_stats_denali USING btree (game_id);


--
-- Name: idx_shot_stats_denali_season_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shot_stats_denali_season_player ON nhl.shot_stats_denali USING btree (season, shooterplayerid);


--
-- Name: idx_shots_all_game_id; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shots_all_game_id ON nhl.shots_all USING btree (game_id);


--
-- Name: idx_shots_all_season; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shots_all_season ON nhl.shots_all USING btree (season);


--
-- Name: idx_shots_all_shooter; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_shots_all_shooter ON nhl.shots_all USING btree (shooterplayerid);


--
-- Name: idx_skater_game_logs_raw_player_date_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_game_logs_raw_player_date_game ON nhl.skater_game_logs_raw USING btree (player_id, game_date, game_id);


--
-- Name: idx_skater_logs_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_logs_player_game ON nhl.skater_game_logs_raw USING btree (player_id, game_id);


--
-- Name: idx_skater_raw_game_id; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_raw_game_id ON nhl.skater_game_logs_raw USING btree (game_id);


--
-- Name: idx_skater_raw_player_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_raw_player_date ON nhl.skater_game_logs_raw USING btree (player_id, game_date);


--
-- Name: idx_skater_raw_team_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_raw_team_date ON nhl.skater_game_logs_raw USING btree (team_id, game_date);


--
-- Name: idx_skater_rolling_agg_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_skater_rolling_agg_player_game ON nhl.skater_rolling_agg USING btree (player_id, game_id);


--
-- Name: idx_sog_enriched_v2_player; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_sog_enriched_v2_player ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (player_id);


--
-- Name: idx_sog_enriched_v2_season_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_sog_enriched_v2_season_game ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (season, game_id);


--
-- Name: idx_sog_enriched_v2_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_sog_enriched_v2_team ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (team_id);


--
-- Name: idx_sog_pregame_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_sog_pregame_date ON nhl.training_features_nhl_sog_enriched_pregame USING btree (game_date);


--
-- Name: idx_sog_pregame_player_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_sog_pregame_player_date ON nhl.training_features_nhl_sog_enriched_pregame USING btree (player_id, game_date);


--
-- Name: idx_team_game_sit_date; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_team_game_sit_date ON nhl.team_game_sit USING btree (game_date);


--
-- Name: idx_team_game_sit_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_team_game_sit_team ON nhl.team_game_sit USING btree (team_code, season, situation);


--
-- Name: idx_tf_shots_season; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_tf_shots_season ON nhl.training_features_shots USING btree (season);


--
-- Name: idx_tf_team_roll10_game_team_opp; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_tf_team_roll10_game_team_opp ON nhl.tf_team_roll10 USING btree (game_id, team_id, opponent_id);


--
-- Name: idx_tf_team_roll10_team_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_tf_team_roll10_team_game ON nhl.tf_team_roll10 USING btree (team_id, game_id);


--
-- Name: idx_training_features_sog_denali_player_date_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_training_features_sog_denali_player_date_game ON nhl.training_features_sog_denali USING btree (player_id, game_date, game_id);


--
-- Name: idx_training_goalie_game_team_opp; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX idx_training_goalie_game_team_opp ON nhl.training_features_goalie_saves_v2 USING btree (game_id, team_id, opponent_id);


--
-- Name: import_skater_points_stage_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX import_skater_points_stage_game_idx ON nhl.import_skater_points_stage USING btree (game_date, game_id);


--
-- Name: import_skater_points_stage_pid_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX import_skater_points_stage_pid_idx ON nhl.import_skater_points_stage USING btree (player_id);


--
-- Name: ix_game_manpower_segments_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_game_manpower_segments_game ON nhl.game_manpower_segments USING btree (game_id);


--
-- Name: ix_game_manpower_segments_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_game_manpower_segments_team ON nhl.game_manpower_segments USING btree (pp_team_id, pk_team_id);


--
-- Name: ix_goalies2023_stage_player_season; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_goalies2023_stage_player_season ON nhl.goalies2023_stage USING btree (((playerid)::bigint), ((season)::integer));


--
-- Name: ix_player_external_ids_provider; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_player_external_ids_provider ON nhl.player_external_ids USING btree (provider);


--
-- Name: ix_skaters2023_stage_player_season; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_skaters2023_stage_player_season ON nhl.skaters2023_stage USING btree (((playerid)::bigint), ((season)::integer));


--
-- Name: ix_st_expo_game_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX ix_st_expo_game_team ON nhl.skater_game_special_teams_exposure USING btree (game_id, team_id);


--
-- Name: nhl_user_props_lookup_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX nhl_user_props_lookup_idx ON nhl.user_props USING btree (game_id, player_id, prop_type);


--
-- Name: nhl_user_props_unique_prop_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX nhl_user_props_unique_prop_idx ON nhl.user_props USING btree (game_id, player_id, prop_type, over_under, prop_value, prop_source);


--
-- Name: roster_status_latest_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX roster_status_latest_idx ON nhl.roster_status USING btree (game_id, player_id, asof_ts DESC);


--
-- Name: roster_status_uk; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX roster_status_uk ON nhl.roster_status USING btree (game_id, team_id, player_id);


--
-- Name: sk_logs_game_player_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX sk_logs_game_player_idx ON nhl.skater_game_logs_raw USING btree (game_id, player_id);


--
-- Name: skater_logs_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_logs_game_idx ON nhl.skater_game_logs_raw USING btree (game_id);


--
-- Name: skater_logs_opp_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_logs_opp_idx ON nhl.skater_game_logs_raw USING btree (opponent_id);


--
-- Name: skater_logs_team_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_logs_team_idx ON nhl.skater_game_logs_raw USING btree (team_id);


--
-- Name: skater_points_raw_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_points_raw_date_idx ON nhl.skater_points_raw USING btree (game_date);


--
-- Name: skater_points_raw_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_points_raw_game_idx ON nhl.skater_points_raw USING btree (game_date, game_id);


--
-- Name: skater_points_raw_pid_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_points_raw_pid_idx ON nhl.skater_points_raw USING btree (player_id);


--
-- Name: skater_points_raw_uniq; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX skater_points_raw_uniq ON nhl.skater_points_raw USING btree (player_id, game_id);


--
-- Name: skater_roll_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skater_roll_idx ON nhl.skater_rolling_agg USING btree (game_id);


--
-- Name: skaters_szn_sit_player_season_sit_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skaters_szn_sit_player_season_sit_idx ON nhl.skaters_szn_sit USING btree (player_id, season, situation);


--
-- Name: skaters_szn_sit_team_season_sit_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skaters_szn_sit_team_season_sit_idx ON nhl.skaters_szn_sit USING btree (team_abbr, season, situation);


--
-- Name: skglr_player_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skglr_player_date_idx ON nhl.skater_game_logs_raw USING btree (player_id, game_date, game_id);


--
-- Name: skglr_team_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX skglr_team_date_idx ON nhl.skater_game_logs_raw USING btree (team_id, game_date, game_id);


--
-- Name: sklr_offenders_by_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX sklr_offenders_by_game_idx ON nhl.skater_game_logs_raw USING btree (game_id) WHERE ((shots_on_goal > 0) AND ((ev_sog IS NULL) OR (pp_sog IS NULL) OR (sh_sog IS NULL) OR (((COALESCE(ev_sog, 0) + COALESCE(pp_sog, 0)) + COALESCE(sh_sog, 0)) <> shots_on_goal)));


--
-- Name: spr_game_team_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX spr_game_team_idx ON nhl.skater_points_raw USING btree (game_id, team_id);


--
-- Name: team_roll10_m_uniq; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX team_roll10_m_uniq ON nhl.team_roll10_m USING btree (team_id, game_date, game_id);


--
-- Name: tf_points_enriched_game_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tf_points_enriched_game_date_idx ON nhl.skater_points_raw USING btree (game_date);


--
-- Name: tf_points_enriched_player_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tf_points_enriched_player_idx ON nhl.skater_points_raw USING btree (player_id, game_date, game_id);


--
-- Name: tf_team_roll10_team_id_game_id_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tf_team_roll10_team_id_game_id_idx ON nhl.tf_team_roll10 USING btree (team_id, game_id);


--
-- Name: tgs_date_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tgs_date_idx ON nhl.teams_game_sit USING btree (game_date);


--
-- Name: tgs_game_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tgs_game_idx ON nhl.teams_game_sit USING btree (game_id);


--
-- Name: tgs_opp_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tgs_opp_idx ON nhl.teams_game_sit USING btree (opp_abbr, season);


--
-- Name: tgs_team_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tgs_team_idx ON nhl.teams_game_sit USING btree (team_abbr, season);


--
-- Name: tgs_team_sit_idx; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE INDEX tgs_team_sit_idx ON nhl.teams_game_sit USING btree (team_abbr, situation);


--
-- Name: uq_import_goalie_logs_stage_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_import_goalie_logs_stage_player_game ON nhl.import_goalie_logs_stage USING btree (player_id, game_id);


--
-- Name: uq_nhl_predictions_player_game_prop_line; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_nhl_predictions_player_game_prop_line ON nhl.predictions USING btree (player_id, game_id, prop, line);


--
-- Name: uq_saves_ready_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_saves_ready_player_game ON nhl.training_features_goalie_saves_v2_ready USING btree (player_id, game_id);


--
-- Name: uq_sog_pregame_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_sog_pregame_player_game ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (player_id, game_id);


--
-- Name: uq_team_pp_toi_totals_by_date_team; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_team_pp_toi_totals_by_date_team ON nhl.team_pp_toi_totals_by_date_team USING btree (game_date, team_id);


--
-- Name: uq_tf_skater_attempts_roll10; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_tf_skater_attempts_roll10 ON nhl.tf_skater_attempts_roll10 USING btree (player_id, game_id);


--
-- Name: uq_training_features_goalie_saves_v2_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_training_features_goalie_saves_v2_player_game ON nhl.training_features_goalie_saves_v2 USING btree (player_id, game_id);


--
-- Name: uq_training_features_sog_denali_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX uq_training_features_sog_denali_player_game ON nhl.training_features_sog_denali USING btree (player_id, game_id);


--
-- Name: ux_import_skater_logs_stage_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_import_skater_logs_stage_player_game ON nhl.import_skater_logs_stage USING btree (player_id, game_id);


--
-- Name: ux_player_external_ids_provider_key; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_player_external_ids_provider_key ON nhl.player_external_ids USING btree (provider, provider_player_id);


--
-- Name: ux_predictions_unique; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_predictions_unique ON nhl.predictions USING btree (prop, player_id, game_id, line, feature_hash);


--
-- Name: ux_saves_enr; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_saves_enr ON nhl.training_features_nhl_saves_enriched USING btree (player_id, game_id);


--
-- Name: ux_saves_enr_filt; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_saves_enr_filt ON nhl.training_features_nhl_saves_enr_filt USING btree (player_id, game_id);


--
-- Name: ux_sog_pregame_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_sog_pregame_player_game ON nhl.training_features_nhl_sog_enriched_pregame USING btree (player_id, game_id);


--
-- Name: ux_sog_pregame_v2_player_game; Type: INDEX; Schema: nhl; Owner: postgres
--

CREATE UNIQUE INDEX ux_sog_pregame_v2_player_game ON nhl.training_features_nhl_sog_enriched_pregame_v2_mt USING btree (player_id, game_id);


--
-- Name: ix_realtime_subscription_entity; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX ix_realtime_subscription_entity ON realtime.subscription USING btree (entity);


--
-- Name: messages_inserted_at_topic_index; Type: INDEX; Schema: realtime; Owner: supabase_realtime_admin
--

CREATE INDEX messages_inserted_at_topic_index ON ONLY realtime.messages USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_21_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_21_inserted_at_topic_idx ON realtime.messages_2026_02_21 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_22_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_22_inserted_at_topic_idx ON realtime.messages_2026_02_22 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_23_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_23_inserted_at_topic_idx ON realtime.messages_2026_02_23 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_24_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_24_inserted_at_topic_idx ON realtime.messages_2026_02_24 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_25_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_25_inserted_at_topic_idx ON realtime.messages_2026_02_25 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_26_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_26_inserted_at_topic_idx ON realtime.messages_2026_02_26 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: messages_2026_02_27_inserted_at_topic_idx; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE INDEX messages_2026_02_27_inserted_at_topic_idx ON realtime.messages_2026_02_27 USING btree (inserted_at DESC, topic) WHERE ((extension = 'broadcast'::text) AND (private IS TRUE));


--
-- Name: subscription_subscription_id_entity_filters_action_filter_key; Type: INDEX; Schema: realtime; Owner: supabase_admin
--

CREATE UNIQUE INDEX subscription_subscription_id_entity_filters_action_filter_key ON realtime.subscription USING btree (subscription_id, entity, filters, action_filter);


--
-- Name: bname; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX bname ON storage.buckets USING btree (name);


--
-- Name: bucketid_objname; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX bucketid_objname ON storage.objects USING btree (bucket_id, name);


--
-- Name: buckets_analytics_unique_name_idx; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX buckets_analytics_unique_name_idx ON storage.buckets_analytics USING btree (name) WHERE (deleted_at IS NULL);


--
-- Name: idx_multipart_uploads_list; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_multipart_uploads_list ON storage.s3_multipart_uploads USING btree (bucket_id, key, created_at);


--
-- Name: idx_objects_bucket_id_name; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_objects_bucket_id_name ON storage.objects USING btree (bucket_id, name COLLATE "C");


--
-- Name: idx_objects_bucket_id_name_lower; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX idx_objects_bucket_id_name_lower ON storage.objects USING btree (bucket_id, lower(name) COLLATE "C");


--
-- Name: name_prefix_search; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE INDEX name_prefix_search ON storage.objects USING btree (name text_pattern_ops);


--
-- Name: vector_indexes_name_bucket_id_idx; Type: INDEX; Schema: storage; Owner: supabase_storage_admin
--

CREATE UNIQUE INDEX vector_indexes_name_bucket_id_idx ON storage.vector_indexes USING btree (name, bucket_id);


--
-- Name: messages_2026_02_21_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_21_inserted_at_topic_idx;


--
-- Name: messages_2026_02_21_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_21_pkey;


--
-- Name: messages_2026_02_22_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_22_inserted_at_topic_idx;


--
-- Name: messages_2026_02_22_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_22_pkey;


--
-- Name: messages_2026_02_23_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_23_inserted_at_topic_idx;


--
-- Name: messages_2026_02_23_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_23_pkey;


--
-- Name: messages_2026_02_24_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_24_inserted_at_topic_idx;


--
-- Name: messages_2026_02_24_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_24_pkey;


--
-- Name: messages_2026_02_25_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_25_inserted_at_topic_idx;


--
-- Name: messages_2026_02_25_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_25_pkey;


--
-- Name: messages_2026_02_26_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_26_inserted_at_topic_idx;


--
-- Name: messages_2026_02_26_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_26_pkey;


--
-- Name: messages_2026_02_27_inserted_at_topic_idx; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_inserted_at_topic_index ATTACH PARTITION realtime.messages_2026_02_27_inserted_at_topic_idx;


--
-- Name: messages_2026_02_27_pkey; Type: INDEX ATTACH; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER INDEX realtime.messages_pkey ATTACH PARTITION realtime.messages_2026_02_27_pkey;


--
-- Name: model_training_props set_updated_at; Type: TRIGGER; Schema: mlb; Owner: postgres
--

CREATE TRIGGER set_updated_at BEFORE UPDATE ON mlb.model_training_props FOR EACH ROW EXECUTE FUNCTION public.update_timestamp();


--
-- Name: player_ids trg_norm_player_ids_team_text; Type: TRIGGER; Schema: mlb; Owner: postgres
--

CREATE TRIGGER trg_norm_player_ids_team_text BEFORE INSERT OR UPDATE OF team_id ON mlb.player_ids FOR EACH ROW EXECUTE FUNCTION public.norm_player_ids_team_text();


--
-- Name: model_training_props trg_norm_team_ids_mtp; Type: TRIGGER; Schema: mlb; Owner: postgres
--

CREATE TRIGGER trg_norm_team_ids_mtp BEFORE INSERT OR UPDATE OF team, team_id ON mlb.model_training_props FOR EACH ROW EXECUTE FUNCTION public.norm_team_ids_mtp();


--
-- Name: player_stats trg_norm_team_ids_ps; Type: TRIGGER; Schema: mlb; Owner: postgres
--

CREATE TRIGGER trg_norm_team_ids_ps BEFORE INSERT OR UPDATE OF team, opponent ON mlb.player_stats FOR EACH ROW EXECUTE FUNCTION public.norm_team_ids_ps();


--
-- Name: player_stats trg_set_is_starter; Type: TRIGGER; Schema: mlb; Owner: postgres
--

CREATE TRIGGER trg_set_is_starter BEFORE INSERT OR UPDATE OF player_id, game_id, team ON mlb.player_stats FOR EACH ROW EXECUTE FUNCTION public.set_is_starter();


--
-- Name: games set_updated_at_nhl_games; Type: TRIGGER; Schema: nhl; Owner: postgres
--

CREATE TRIGGER set_updated_at_nhl_games BEFORE UPDATE ON nhl.games FOR EACH ROW EXECUTE FUNCTION nhl.set_updated_at();


--
-- Name: shot_stats_denali set_updated_at_nhl_shot_stats_denali; Type: TRIGGER; Schema: nhl; Owner: postgres
--

CREATE TRIGGER set_updated_at_nhl_shot_stats_denali BEFORE UPDATE ON nhl.shot_stats_denali FOR EACH ROW EXECUTE FUNCTION nhl.set_updated_at();


--
-- Name: games trg_audit_games_season_write; Type: TRIGGER; Schema: nhl; Owner: postgres
--

CREATE TRIGGER trg_audit_games_season_write AFTER INSERT OR UPDATE OF season ON nhl.games FOR EACH ROW EXECUTE FUNCTION nhl.audit_games_season_write();


--
-- Name: games trg_audit_games_write; Type: TRIGGER; Schema: nhl; Owner: postgres
--

CREATE TRIGGER trg_audit_games_write AFTER INSERT OR DELETE OR UPDATE ON nhl.games FOR EACH ROW EXECUTE FUNCTION nhl.audit_games_write();


--
-- Name: games trg_games_season_audit; Type: TRIGGER; Schema: nhl; Owner: postgres
--

CREATE TRIGGER trg_games_season_audit AFTER INSERT OR UPDATE OF season ON nhl.games FOR EACH ROW EXECUTE FUNCTION nhl.log_games_season_change();


--
-- Name: subscription tr_check_filters; Type: TRIGGER; Schema: realtime; Owner: supabase_admin
--

CREATE TRIGGER tr_check_filters BEFORE INSERT OR UPDATE ON realtime.subscription FOR EACH ROW EXECUTE FUNCTION realtime.subscription_check_filters();


--
-- Name: buckets enforce_bucket_name_length_trigger; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER enforce_bucket_name_length_trigger BEFORE INSERT OR UPDATE OF name ON storage.buckets FOR EACH ROW EXECUTE FUNCTION storage.enforce_bucket_name_length();


--
-- Name: buckets protect_buckets_delete; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER protect_buckets_delete BEFORE DELETE ON storage.buckets FOR EACH STATEMENT EXECUTE FUNCTION storage.protect_delete();


--
-- Name: objects protect_objects_delete; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER protect_objects_delete BEFORE DELETE ON storage.objects FOR EACH STATEMENT EXECUTE FUNCTION storage.protect_delete();


--
-- Name: objects update_objects_updated_at; Type: TRIGGER; Schema: storage; Owner: supabase_storage_admin
--

CREATE TRIGGER update_objects_updated_at BEFORE UPDATE ON storage.objects FOR EACH ROW EXECUTE FUNCTION storage.update_updated_at_column();


--
-- Name: identities identities_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.identities
    ADD CONSTRAINT identities_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: mfa_amr_claims mfa_amr_claims_session_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_amr_claims
    ADD CONSTRAINT mfa_amr_claims_session_id_fkey FOREIGN KEY (session_id) REFERENCES auth.sessions(id) ON DELETE CASCADE;


--
-- Name: mfa_challenges mfa_challenges_auth_factor_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_challenges
    ADD CONSTRAINT mfa_challenges_auth_factor_id_fkey FOREIGN KEY (factor_id) REFERENCES auth.mfa_factors(id) ON DELETE CASCADE;


--
-- Name: mfa_factors mfa_factors_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.mfa_factors
    ADD CONSTRAINT mfa_factors_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: oauth_authorizations oauth_authorizations_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_client_id_fkey FOREIGN KEY (client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: oauth_authorizations oauth_authorizations_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_authorizations
    ADD CONSTRAINT oauth_authorizations_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: oauth_consents oauth_consents_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_client_id_fkey FOREIGN KEY (client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: oauth_consents oauth_consents_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.oauth_consents
    ADD CONSTRAINT oauth_consents_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: one_time_tokens one_time_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.one_time_tokens
    ADD CONSTRAINT one_time_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: refresh_tokens refresh_tokens_session_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_session_id_fkey FOREIGN KEY (session_id) REFERENCES auth.sessions(id) ON DELETE CASCADE;


--
-- Name: saml_providers saml_providers_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_providers
    ADD CONSTRAINT saml_providers_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: saml_relay_states saml_relay_states_flow_state_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_flow_state_id_fkey FOREIGN KEY (flow_state_id) REFERENCES auth.flow_state(id) ON DELETE CASCADE;


--
-- Name: saml_relay_states saml_relay_states_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.saml_relay_states
    ADD CONSTRAINT saml_relay_states_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_oauth_client_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_oauth_client_id_fkey FOREIGN KEY (oauth_client_id) REFERENCES auth.oauth_clients(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: sso_domains sso_domains_sso_provider_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.sso_domains
    ADD CONSTRAINT sso_domains_sso_provider_id_fkey FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers(id) ON DELETE CASCADE;


--
-- Name: webauthn_challenges webauthn_challenges_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.webauthn_challenges
    ADD CONSTRAINT webauthn_challenges_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: webauthn_credentials webauthn_credentials_user_id_fkey; Type: FK CONSTRAINT; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE ONLY auth.webauthn_credentials
    ADD CONSTRAINT webauthn_credentials_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;


--
-- Name: bvp_stats bvp_stats_batter_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.bvp_stats
    ADD CONSTRAINT bvp_stats_batter_id_fkey FOREIGN KEY (batter_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: bvp_stats bvp_stats_game_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.bvp_stats
    ADD CONSTRAINT bvp_stats_game_id_fkey FOREIGN KEY (game_id) REFERENCES mlb.game_info(game_id);


--
-- Name: bvp_stats bvp_stats_pitcher_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.bvp_stats
    ADD CONSTRAINT bvp_stats_pitcher_id_fkey FOREIGN KEY (pitcher_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: model_training_props model_training_props_game_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.model_training_props
    ADD CONSTRAINT model_training_props_game_id_fkey FOREIGN KEY (game_id) REFERENCES mlb.game_info(game_id);


--
-- Name: model_training_props model_training_props_player_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.model_training_props
    ADD CONSTRAINT model_training_props_player_id_fkey FOREIGN KEY (player_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: player_derived_stats player_derived_stats_game_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_derived_stats_game_id_fkey FOREIGN KEY (game_id) REFERENCES mlb.game_info(game_id);


--
-- Name: player_derived_stats player_derived_stats_player_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_derived_stats
    ADD CONSTRAINT player_derived_stats_player_id_fkey FOREIGN KEY (player_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: player_profiles_cache player_profiles_cache_player_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_profiles_cache
    ADD CONSTRAINT player_profiles_cache_player_id_fkey FOREIGN KEY (player_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: player_props player_props_game_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_props
    ADD CONSTRAINT player_props_game_id_fkey FOREIGN KEY (game_id) REFERENCES mlb.game_info(game_id);


--
-- Name: player_props player_props_player_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_props
    ADD CONSTRAINT player_props_player_id_fkey FOREIGN KEY (player_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: player_stats player_stats_game_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_stats
    ADD CONSTRAINT player_stats_game_id_fkey FOREIGN KEY (game_id) REFERENCES mlb.game_info(game_id);


--
-- Name: player_stats player_stats_player_id_fkey; Type: FK CONSTRAINT; Schema: mlb; Owner: postgres
--

ALTER TABLE ONLY mlb.player_stats
    ADD CONSTRAINT player_stats_player_id_fkey FOREIGN KEY (player_id) REFERENCES mlb.player_ids(player_id);


--
-- Name: blocked_shot_events blocked_shot_events_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.blocked_shot_events
    ADD CONSTRAINT blocked_shot_events_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id) ON DELETE CASCADE;


--
-- Name: games games_away_team_fk; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_away_team_fk FOREIGN KEY (away_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: games games_home_team_fk; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_home_team_fk FOREIGN KEY (home_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_rolling_agg goalie_rolling_agg_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.goalie_rolling_agg
    ADD CONSTRAINT goalie_rolling_agg_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: player_external_ids player_external_ids_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id) ON DELETE CASCADE;


--
-- Name: players players_current_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.players
    ADD CONSTRAINT players_current_team_id_fkey FOREIGN KEY (current_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: roster_status roster_status_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: roster_status roster_status_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: shot_on_goal_events shot_on_goal_events_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.shot_on_goal_events
    ADD CONSTRAINT shot_on_goal_events_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id) ON DELETE CASCADE;


--
-- Name: skater_game_logs_raw skater_game_logs_raw_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: skater_rolling_agg skater_rolling_agg_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: team_context_rolling team_context_rolling_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_context_rolling
    ADD CONSTRAINT team_context_rolling_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: team_external_ids team_external_ids_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id) ON DELETE CASCADE;


--
-- Name: user_props user_props_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: user_props user_props_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: user_props user_props_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: postgres
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: objects objects_bucketId_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.objects
    ADD CONSTRAINT "objects_bucketId_fkey" FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads s3_multipart_uploads_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads
    ADD CONSTRAINT s3_multipart_uploads_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets(id);


--
-- Name: s3_multipart_uploads_parts s3_multipart_uploads_parts_upload_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.s3_multipart_uploads_parts
    ADD CONSTRAINT s3_multipart_uploads_parts_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES storage.s3_multipart_uploads(id) ON DELETE CASCADE;


--
-- Name: vector_indexes vector_indexes_bucket_id_fkey; Type: FK CONSTRAINT; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE ONLY storage.vector_indexes
    ADD CONSTRAINT vector_indexes_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES storage.buckets_vectors(id);


--
-- Name: audit_log_entries; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.audit_log_entries ENABLE ROW LEVEL SECURITY;

--
-- Name: flow_state; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.flow_state ENABLE ROW LEVEL SECURITY;

--
-- Name: identities; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.identities ENABLE ROW LEVEL SECURITY;

--
-- Name: instances; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.instances ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_amr_claims; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_amr_claims ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_challenges; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_challenges ENABLE ROW LEVEL SECURITY;

--
-- Name: mfa_factors; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.mfa_factors ENABLE ROW LEVEL SECURITY;

--
-- Name: one_time_tokens; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.one_time_tokens ENABLE ROW LEVEL SECURITY;

--
-- Name: refresh_tokens; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.refresh_tokens ENABLE ROW LEVEL SECURITY;

--
-- Name: saml_providers; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.saml_providers ENABLE ROW LEVEL SECURITY;

--
-- Name: saml_relay_states; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.saml_relay_states ENABLE ROW LEVEL SECURITY;

--
-- Name: schema_migrations; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.schema_migrations ENABLE ROW LEVEL SECURITY;

--
-- Name: sessions; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sessions ENABLE ROW LEVEL SECURITY;

--
-- Name: sso_domains; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sso_domains ENABLE ROW LEVEL SECURITY;

--
-- Name: sso_providers; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.sso_providers ENABLE ROW LEVEL SECURITY;

--
-- Name: users; Type: ROW SECURITY; Schema: auth; Owner: supabase_auth_admin
--

ALTER TABLE auth.users ENABLE ROW LEVEL SECURITY;

--
-- Name: model_training_props Allow anon insert; Type: POLICY; Schema: mlb; Owner: postgres
--

CREATE POLICY "Allow anon insert" ON mlb.model_training_props FOR INSERT TO anon WITH CHECK (true);


--
-- Name: model_training_props Allow anon select; Type: POLICY; Schema: mlb; Owner: postgres
--

CREATE POLICY "Allow anon select" ON mlb.model_training_props FOR SELECT TO anon USING (true);


--
-- Name: model_training_props Allow upsert for anon key; Type: POLICY; Schema: mlb; Owner: postgres
--

CREATE POLICY "Allow upsert for anon key" ON mlb.model_training_props USING (true) WITH CHECK (true);


--
-- Name: model_training_props Enable read access for all users; Type: POLICY; Schema: mlb; Owner: postgres
--

CREATE POLICY "Enable read access for all users" ON mlb.model_training_props FOR SELECT USING (true);


--
-- Name: bvp_stats; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.bvp_stats ENABLE ROW LEVEL SECURITY;

--
-- Name: model_training_props; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.model_training_props ENABLE ROW LEVEL SECURITY;

--
-- Name: player_derived_stats; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_derived_stats ENABLE ROW LEVEL SECURITY;

--
-- Name: player_ids; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_ids ENABLE ROW LEVEL SECURITY;

--
-- Name: player_profiles_cache; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_profiles_cache ENABLE ROW LEVEL SECURITY;

--
-- Name: player_stats; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_stats ENABLE ROW LEVEL SECURITY;

--
-- Name: player_streak_profiles; Type: ROW SECURITY; Schema: mlb; Owner: postgres
--

ALTER TABLE mlb.player_streak_profiles ENABLE ROW LEVEL SECURITY;

--
-- Name: mlb_team_map; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.mlb_team_map ENABLE ROW LEVEL SECURITY;

--
-- Name: opp_starter_per_game; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.opp_starter_per_game ENABLE ROW LEVEL SECURITY;

--
-- Name: messages; Type: ROW SECURITY; Schema: realtime; Owner: supabase_realtime_admin
--

ALTER TABLE realtime.messages ENABLE ROW LEVEL SECURITY;

--
-- Name: buckets; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.buckets ENABLE ROW LEVEL SECURITY;

--
-- Name: buckets_analytics; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.buckets_analytics ENABLE ROW LEVEL SECURITY;

--
-- Name: buckets_vectors; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.buckets_vectors ENABLE ROW LEVEL SECURITY;

--
-- Name: migrations; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.migrations ENABLE ROW LEVEL SECURITY;

--
-- Name: objects; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;

--
-- Name: s3_multipart_uploads; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.s3_multipart_uploads ENABLE ROW LEVEL SECURITY;

--
-- Name: s3_multipart_uploads_parts; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.s3_multipart_uploads_parts ENABLE ROW LEVEL SECURITY;

--
-- Name: vector_indexes; Type: ROW SECURITY; Schema: storage; Owner: supabase_storage_admin
--

ALTER TABLE storage.vector_indexes ENABLE ROW LEVEL SECURITY;

--
-- Name: supabase_realtime; Type: PUBLICATION; Schema: -; Owner: postgres
--

CREATE PUBLICATION supabase_realtime WITH (publish = 'insert, update, delete, truncate');


ALTER PUBLICATION supabase_realtime OWNER TO postgres;

--
-- Name: supabase_realtime_messages_publication; Type: PUBLICATION; Schema: -; Owner: supabase_admin
--

CREATE PUBLICATION supabase_realtime_messages_publication WITH (publish = 'insert, update, delete, truncate');


ALTER PUBLICATION supabase_realtime_messages_publication OWNER TO supabase_admin;

--
-- Name: supabase_realtime_messages_publication messages; Type: PUBLICATION TABLE; Schema: realtime; Owner: supabase_admin
--

ALTER PUBLICATION supabase_realtime_messages_publication ADD TABLE ONLY realtime.messages;


--
-- Name: SCHEMA auth; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA auth TO anon;
GRANT USAGE ON SCHEMA auth TO authenticated;
GRANT USAGE ON SCHEMA auth TO service_role;
GRANT ALL ON SCHEMA auth TO supabase_auth_admin;
GRANT ALL ON SCHEMA auth TO dashboard_user;
GRANT USAGE ON SCHEMA auth TO postgres;


--
-- Name: SCHEMA cron; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA cron TO postgres WITH GRANT OPTION;


--
-- Name: SCHEMA extensions; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA extensions TO anon;
GRANT USAGE ON SCHEMA extensions TO authenticated;
GRANT USAGE ON SCHEMA extensions TO service_role;
GRANT ALL ON SCHEMA extensions TO dashboard_user;


--
-- Name: SCHEMA mlb; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA mlb TO anon;
GRANT USAGE ON SCHEMA mlb TO authenticated;
GRANT USAGE ON SCHEMA mlb TO service_role;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO postgres;
GRANT USAGE ON SCHEMA public TO anon;
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE ON SCHEMA public TO service_role;


--
-- Name: SCHEMA realtime; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA realtime TO postgres;
GRANT USAGE ON SCHEMA realtime TO anon;
GRANT USAGE ON SCHEMA realtime TO authenticated;
GRANT USAGE ON SCHEMA realtime TO service_role;
GRANT ALL ON SCHEMA realtime TO supabase_realtime_admin;


--
-- Name: SCHEMA storage; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA storage TO postgres WITH GRANT OPTION;
GRANT USAGE ON SCHEMA storage TO anon;
GRANT USAGE ON SCHEMA storage TO authenticated;
GRANT USAGE ON SCHEMA storage TO service_role;
GRANT ALL ON SCHEMA storage TO supabase_storage_admin;
GRANT ALL ON SCHEMA storage TO dashboard_user;


--
-- Name: SCHEMA vault; Type: ACL; Schema: -; Owner: supabase_admin
--

GRANT USAGE ON SCHEMA vault TO postgres WITH GRANT OPTION;
GRANT USAGE ON SCHEMA vault TO service_role;


--
-- Name: FUNCTION email(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.email() TO dashboard_user;
GRANT ALL ON FUNCTION auth.email() TO postgres;


--
-- Name: FUNCTION jwt(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.jwt() TO postgres;
GRANT ALL ON FUNCTION auth.jwt() TO dashboard_user;


--
-- Name: FUNCTION role(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.role() TO dashboard_user;
GRANT ALL ON FUNCTION auth.role() TO postgres;


--
-- Name: FUNCTION uid(); Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON FUNCTION auth.uid() TO dashboard_user;
GRANT ALL ON FUNCTION auth.uid() TO postgres;


--
-- Name: FUNCTION alter_job(job_id bigint, schedule text, command text, database text, username text, active boolean); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.alter_job(job_id bigint, schedule text, command text, database text, username text, active boolean) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION job_cache_invalidate(); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.job_cache_invalidate() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION schedule(schedule text, command text); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.schedule(schedule text, command text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION schedule(job_name text, schedule text, command text); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.schedule(job_name text, schedule text, command text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION schedule_in_database(job_name text, schedule text, command text, database text, username text, active boolean); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.schedule_in_database(job_name text, schedule text, command text, database text, username text, active boolean) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION unschedule(job_id bigint); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.unschedule(job_id bigint) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION unschedule(job_name text); Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT ALL ON FUNCTION cron.unschedule(job_name text) TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION algorithm_sign(signables text, secret text, algorithm text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.algorithm_sign(signables text, secret text, algorithm text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.algorithm_sign(signables text, secret text, algorithm text) TO dashboard_user;


--
-- Name: FUNCTION armor(bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.armor(bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.armor(bytea) TO dashboard_user;


--
-- Name: FUNCTION armor(bytea, text[], text[]); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.armor(bytea, text[], text[]) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.armor(bytea, text[], text[]) TO dashboard_user;


--
-- Name: FUNCTION crypt(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.crypt(text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.crypt(text, text) TO dashboard_user;


--
-- Name: FUNCTION dearmor(text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.dearmor(text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.dearmor(text) TO dashboard_user;


--
-- Name: FUNCTION decrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.decrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.decrypt(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION decrypt_iv(bytea, bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.decrypt_iv(bytea, bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.decrypt_iv(bytea, bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION digest(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.digest(bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.digest(bytea, text) TO dashboard_user;


--
-- Name: FUNCTION digest(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.digest(text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.digest(text, text) TO dashboard_user;


--
-- Name: FUNCTION encrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.encrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.encrypt(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION encrypt_iv(bytea, bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.encrypt_iv(bytea, bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.encrypt_iv(bytea, bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION gen_random_bytes(integer); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_random_bytes(integer) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.gen_random_bytes(integer) TO dashboard_user;


--
-- Name: FUNCTION gen_random_uuid(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_random_uuid() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.gen_random_uuid() TO dashboard_user;


--
-- Name: FUNCTION gen_salt(text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_salt(text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.gen_salt(text) TO dashboard_user;


--
-- Name: FUNCTION gen_salt(text, integer); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.gen_salt(text, integer) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.gen_salt(text, integer) TO dashboard_user;


--
-- Name: FUNCTION grant_pg_cron_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION extensions.grant_pg_cron_access() FROM supabase_admin;
GRANT ALL ON FUNCTION extensions.grant_pg_cron_access() TO supabase_admin WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.grant_pg_cron_access() TO dashboard_user;


--
-- Name: FUNCTION grant_pg_graphql_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.grant_pg_graphql_access() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION grant_pg_net_access(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION extensions.grant_pg_net_access() FROM supabase_admin;
GRANT ALL ON FUNCTION extensions.grant_pg_net_access() TO supabase_admin WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.grant_pg_net_access() TO dashboard_user;


--
-- Name: FUNCTION hmac(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.hmac(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.hmac(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION hmac(text, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.hmac(text, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.hmac(text, text, text) TO dashboard_user;


--
-- Name: FUNCTION pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT blk_read_time double precision, OUT blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT blk_read_time double precision, OUT blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT blk_read_time double precision, OUT blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision) TO dashboard_user;


--
-- Name: FUNCTION pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone) TO dashboard_user;


--
-- Name: FUNCTION pg_stat_statements_reset(userid oid, dbid oid, queryid bigint); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pg_stat_statements_reset(userid oid, dbid oid, queryid bigint) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pg_stat_statements_reset(userid oid, dbid oid, queryid bigint) TO dashboard_user;


--
-- Name: FUNCTION pgp_armor_headers(text, OUT key text, OUT value text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_armor_headers(text, OUT key text, OUT value text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_armor_headers(text, OUT key text, OUT value text) TO dashboard_user;


--
-- Name: FUNCTION pgp_key_id(bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_key_id(bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_key_id(bytea) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt(bytea, bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_decrypt_bytea(bytea, bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_encrypt(text, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_encrypt(text, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_encrypt_bytea(bytea, bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea) TO dashboard_user;


--
-- Name: FUNCTION pgp_pub_encrypt_bytea(bytea, bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_decrypt(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_decrypt(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_decrypt_bytea(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_decrypt_bytea(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_encrypt(text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_encrypt(text, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_encrypt_bytea(bytea, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text) TO dashboard_user;


--
-- Name: FUNCTION pgp_sym_encrypt_bytea(bytea, text, text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text, text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text, text) TO dashboard_user;


--
-- Name: FUNCTION pgrst_ddl_watch(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgrst_ddl_watch() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION pgrst_drop_watch(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.pgrst_drop_watch() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION set_graphql_placeholder(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.set_graphql_placeholder() TO postgres WITH GRANT OPTION;


--
-- Name: FUNCTION sign(payload json, secret text, algorithm text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.sign(payload json, secret text, algorithm text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.sign(payload json, secret text, algorithm text) TO dashboard_user;


--
-- Name: FUNCTION try_cast_double(inp text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.try_cast_double(inp text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.try_cast_double(inp text) TO dashboard_user;


--
-- Name: FUNCTION url_decode(data text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.url_decode(data text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.url_decode(data text) TO dashboard_user;


--
-- Name: FUNCTION url_encode(data bytea); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.url_encode(data bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.url_encode(data bytea) TO dashboard_user;


--
-- Name: FUNCTION uuid_generate_v1(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v1() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_generate_v1() TO dashboard_user;


--
-- Name: FUNCTION uuid_generate_v1mc(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v1mc() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_generate_v1mc() TO dashboard_user;


--
-- Name: FUNCTION uuid_generate_v3(namespace uuid, name text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v3(namespace uuid, name text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_generate_v3(namespace uuid, name text) TO dashboard_user;


--
-- Name: FUNCTION uuid_generate_v4(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v4() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_generate_v4() TO dashboard_user;


--
-- Name: FUNCTION uuid_generate_v5(namespace uuid, name text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_generate_v5(namespace uuid, name text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_generate_v5(namespace uuid, name text) TO dashboard_user;


--
-- Name: FUNCTION uuid_nil(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_nil() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_nil() TO dashboard_user;


--
-- Name: FUNCTION uuid_ns_dns(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_dns() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_ns_dns() TO dashboard_user;


--
-- Name: FUNCTION uuid_ns_oid(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_oid() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_ns_oid() TO dashboard_user;


--
-- Name: FUNCTION uuid_ns_url(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_url() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_ns_url() TO dashboard_user;


--
-- Name: FUNCTION uuid_ns_x500(); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.uuid_ns_x500() TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.uuid_ns_x500() TO dashboard_user;


--
-- Name: FUNCTION verify(token text, secret text, algorithm text); Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT ALL ON FUNCTION extensions.verify(token text, secret text, algorithm text) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION extensions.verify(token text, secret text, algorithm text) TO dashboard_user;


--
-- Name: FUNCTION graphql("operationName" text, query text, variables jsonb, extensions jsonb); Type: ACL; Schema: graphql_public; Owner: supabase_admin
--

GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO postgres;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO anon;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO authenticated;
GRANT ALL ON FUNCTION graphql_public.graphql("operationName" text, query text, variables jsonb, extensions jsonb) TO service_role;


--
-- Name: FUNCTION get_auth(p_usename text); Type: ACL; Schema: pgbouncer; Owner: supabase_admin
--

REVOKE ALL ON FUNCTION pgbouncer.get_auth(p_usename text) FROM PUBLIC;
GRANT ALL ON FUNCTION pgbouncer.get_auth(p_usename text) TO pgbouncer;


--
-- Name: FUNCTION batch_update_training_props(rows jsonb); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.batch_update_training_props(rows jsonb) TO anon;
GRANT ALL ON FUNCTION public.batch_update_training_props(rows jsonb) TO authenticated;
GRANT ALL ON FUNCTION public.batch_update_training_props(rows jsonb) TO service_role;


--
-- Name: FUNCTION bulk_update_training_rows(updates jsonb); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.bulk_update_training_rows(updates jsonb) TO anon;
GRANT ALL ON FUNCTION public.bulk_update_training_rows(updates jsonb) TO authenticated;
GRANT ALL ON FUNCTION public.bulk_update_training_rows(updates jsonb) TO service_role;


--
-- Name: FUNCTION execute_raw_sql(sql text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.execute_raw_sql(sql text) TO anon;
GRANT ALL ON FUNCTION public.execute_raw_sql(sql text) TO authenticated;
GRANT ALL ON FUNCTION public.execute_raw_sql(sql text) TO service_role;


--
-- Name: FUNCTION fetch_bvp_game_ids(offset_value integer, batch_size integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.fetch_bvp_game_ids(offset_value integer, batch_size integer) TO anon;
GRANT ALL ON FUNCTION public.fetch_bvp_game_ids(offset_value integer, batch_size integer) TO authenticated;
GRANT ALL ON FUNCTION public.fetch_bvp_game_ids(offset_value integer, batch_size integer) TO service_role;


--
-- Name: FUNCTION get_daily_prop_accuracy(target_date date); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_daily_prop_accuracy(target_date date) TO anon;
GRANT ALL ON FUNCTION public.get_daily_prop_accuracy(target_date date) TO authenticated;
GRANT ALL ON FUNCTION public.get_daily_prop_accuracy(target_date date) TO service_role;


--
-- Name: FUNCTION get_model_accuracy_metrics(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_model_accuracy_metrics() TO anon;
GRANT ALL ON FUNCTION public.get_model_accuracy_metrics() TO authenticated;
GRANT ALL ON FUNCTION public.get_model_accuracy_metrics() TO service_role;


--
-- Name: FUNCTION norm_player_ids_team_text(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.norm_player_ids_team_text() TO anon;
GRANT ALL ON FUNCTION public.norm_player_ids_team_text() TO authenticated;
GRANT ALL ON FUNCTION public.norm_player_ids_team_text() TO service_role;


--
-- Name: FUNCTION norm_team_ids_mtp(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.norm_team_ids_mtp() TO anon;
GRANT ALL ON FUNCTION public.norm_team_ids_mtp() TO authenticated;
GRANT ALL ON FUNCTION public.norm_team_ids_mtp() TO service_role;


--
-- Name: FUNCTION norm_team_ids_ps(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.norm_team_ids_ps() TO anon;
GRANT ALL ON FUNCTION public.norm_team_ids_ps() TO authenticated;
GRANT ALL ON FUNCTION public.norm_team_ids_ps() TO service_role;


--
-- Name: FUNCTION resolve_team_context(p_player_id bigint, p_game_id bigint); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.resolve_team_context(p_player_id bigint, p_game_id bigint) TO anon;
GRANT ALL ON FUNCTION public.resolve_team_context(p_player_id bigint, p_game_id bigint) TO authenticated;
GRANT ALL ON FUNCTION public.resolve_team_context(p_player_id bigint, p_game_id bigint) TO service_role;


--
-- Name: FUNCTION set_is_starter(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.set_is_starter() TO anon;
GRANT ALL ON FUNCTION public.set_is_starter() TO authenticated;
GRANT ALL ON FUNCTION public.set_is_starter() TO service_role;


--
-- Name: FUNCTION update_timestamp(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.update_timestamp() TO anon;
GRANT ALL ON FUNCTION public.update_timestamp() TO authenticated;
GRANT ALL ON FUNCTION public.update_timestamp() TO service_role;


--
-- Name: FUNCTION apply_rls(wal jsonb, max_record_bytes integer); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO postgres;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO anon;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO authenticated;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO service_role;
GRANT ALL ON FUNCTION realtime.apply_rls(wal jsonb, max_record_bytes integer) TO supabase_realtime_admin;


--
-- Name: FUNCTION broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) TO postgres;
GRANT ALL ON FUNCTION realtime.broadcast_changes(topic_name text, event_name text, operation text, table_name text, table_schema text, new record, old record, level text) TO dashboard_user;


--
-- Name: FUNCTION build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO postgres;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO anon;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO authenticated;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO service_role;
GRANT ALL ON FUNCTION realtime.build_prepared_statement_sql(prepared_statement_name text, entity regclass, columns realtime.wal_column[]) TO supabase_realtime_admin;


--
-- Name: FUNCTION "cast"(val text, type_ regtype); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO postgres;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO dashboard_user;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO anon;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO authenticated;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO service_role;
GRANT ALL ON FUNCTION realtime."cast"(val text, type_ regtype) TO supabase_realtime_admin;


--
-- Name: FUNCTION check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO postgres;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO anon;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO authenticated;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO service_role;
GRANT ALL ON FUNCTION realtime.check_equality_op(op realtime.equality_op, type_ regtype, val_1 text, val_2 text) TO supabase_realtime_admin;


--
-- Name: FUNCTION is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO postgres;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO anon;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO authenticated;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO service_role;
GRANT ALL ON FUNCTION realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[]) TO supabase_realtime_admin;


--
-- Name: FUNCTION list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO postgres;
GRANT ALL ON FUNCTION realtime.list_changes(publication name, slot_name name, max_changes integer, max_record_bytes integer) TO dashboard_user;


--
-- Name: FUNCTION quote_wal2json(entity regclass); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO postgres;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO anon;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO authenticated;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO service_role;
GRANT ALL ON FUNCTION realtime.quote_wal2json(entity regclass) TO supabase_realtime_admin;


--
-- Name: FUNCTION send(payload jsonb, event text, topic text, private boolean); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) TO postgres;
GRANT ALL ON FUNCTION realtime.send(payload jsonb, event text, topic text, private boolean) TO dashboard_user;


--
-- Name: FUNCTION subscription_check_filters(); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO postgres;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO dashboard_user;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO anon;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO authenticated;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO service_role;
GRANT ALL ON FUNCTION realtime.subscription_check_filters() TO supabase_realtime_admin;


--
-- Name: FUNCTION to_regrole(role_name text); Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO postgres;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO dashboard_user;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO anon;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO authenticated;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO service_role;
GRANT ALL ON FUNCTION realtime.to_regrole(role_name text) TO supabase_realtime_admin;


--
-- Name: FUNCTION topic(); Type: ACL; Schema: realtime; Owner: supabase_realtime_admin
--

GRANT ALL ON FUNCTION realtime.topic() TO postgres;
GRANT ALL ON FUNCTION realtime.topic() TO dashboard_user;


--
-- Name: FUNCTION can_insert_object(bucketid text, name text, owner uuid, metadata jsonb); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.can_insert_object(bucketid text, name text, owner uuid, metadata jsonb) TO postgres;


--
-- Name: FUNCTION extension(name text); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.extension(name text) TO postgres;


--
-- Name: FUNCTION filename(name text); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.filename(name text) TO postgres;


--
-- Name: FUNCTION foldername(name text); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.foldername(name text) TO postgres;


--
-- Name: FUNCTION list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer, next_key_token text, next_upload_token text); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.list_multipart_uploads_with_delimiter(bucket_id text, prefix_param text, delimiter_param text, max_keys integer, next_key_token text, next_upload_token text) TO postgres;


--
-- Name: FUNCTION operation(); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.operation() TO postgres;


--
-- Name: FUNCTION search(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.search(prefix text, bucketname text, limits integer, levels integer, offsets integer, search text, sortcolumn text, sortorder text) TO postgres;


--
-- Name: FUNCTION update_updated_at_column(); Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT ALL ON FUNCTION storage.update_updated_at_column() TO postgres;


--
-- Name: FUNCTION _crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault._crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault._crypto_aead_det_decrypt(message bytea, additional bytea, key_id bigint, context bytea, nonce bytea) TO service_role;


--
-- Name: FUNCTION create_secret(new_secret text, new_name text, new_description text, new_key_id uuid); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault.create_secret(new_secret text, new_name text, new_description text, new_key_id uuid) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault.create_secret(new_secret text, new_name text, new_description text, new_key_id uuid) TO service_role;


--
-- Name: FUNCTION update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid); Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT ALL ON FUNCTION vault.update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION vault.update_secret(secret_id uuid, new_secret text, new_name text, new_description text, new_key_id uuid) TO service_role;


--
-- Name: TABLE audit_log_entries; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.audit_log_entries TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.audit_log_entries TO postgres;
GRANT SELECT ON TABLE auth.audit_log_entries TO postgres WITH GRANT OPTION;


--
-- Name: TABLE custom_oauth_providers; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.custom_oauth_providers TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.custom_oauth_providers TO dashboard_user;


--
-- Name: TABLE flow_state; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.flow_state TO postgres;
GRANT SELECT ON TABLE auth.flow_state TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.flow_state TO dashboard_user;


--
-- Name: TABLE identities; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.identities TO postgres;
GRANT SELECT ON TABLE auth.identities TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.identities TO dashboard_user;


--
-- Name: TABLE instances; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.instances TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.instances TO postgres;
GRANT SELECT ON TABLE auth.instances TO postgres WITH GRANT OPTION;


--
-- Name: TABLE mfa_amr_claims; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_amr_claims TO postgres;
GRANT SELECT ON TABLE auth.mfa_amr_claims TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_amr_claims TO dashboard_user;


--
-- Name: TABLE mfa_challenges; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_challenges TO postgres;
GRANT SELECT ON TABLE auth.mfa_challenges TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_challenges TO dashboard_user;


--
-- Name: TABLE mfa_factors; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_factors TO postgres;
GRANT SELECT ON TABLE auth.mfa_factors TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.mfa_factors TO dashboard_user;


--
-- Name: TABLE oauth_authorizations; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_authorizations TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_authorizations TO dashboard_user;


--
-- Name: TABLE oauth_client_states; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_client_states TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_client_states TO dashboard_user;


--
-- Name: TABLE oauth_clients; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_clients TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_clients TO dashboard_user;


--
-- Name: TABLE oauth_consents; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_consents TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.oauth_consents TO dashboard_user;


--
-- Name: TABLE one_time_tokens; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.one_time_tokens TO postgres;
GRANT SELECT ON TABLE auth.one_time_tokens TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.one_time_tokens TO dashboard_user;


--
-- Name: TABLE refresh_tokens; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.refresh_tokens TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.refresh_tokens TO postgres;
GRANT SELECT ON TABLE auth.refresh_tokens TO postgres WITH GRANT OPTION;


--
-- Name: SEQUENCE refresh_tokens_id_seq; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT ALL ON SEQUENCE auth.refresh_tokens_id_seq TO dashboard_user;
GRANT ALL ON SEQUENCE auth.refresh_tokens_id_seq TO postgres;


--
-- Name: TABLE saml_providers; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.saml_providers TO postgres;
GRANT SELECT ON TABLE auth.saml_providers TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.saml_providers TO dashboard_user;


--
-- Name: TABLE saml_relay_states; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.saml_relay_states TO postgres;
GRANT SELECT ON TABLE auth.saml_relay_states TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.saml_relay_states TO dashboard_user;


--
-- Name: TABLE sessions; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sessions TO postgres;
GRANT SELECT ON TABLE auth.sessions TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sessions TO dashboard_user;


--
-- Name: TABLE sso_domains; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sso_domains TO postgres;
GRANT SELECT ON TABLE auth.sso_domains TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sso_domains TO dashboard_user;


--
-- Name: TABLE sso_providers; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sso_providers TO postgres;
GRANT SELECT ON TABLE auth.sso_providers TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.sso_providers TO dashboard_user;


--
-- Name: TABLE users; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.users TO dashboard_user;
GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.users TO postgres;
GRANT SELECT ON TABLE auth.users TO postgres WITH GRANT OPTION;


--
-- Name: TABLE webauthn_challenges; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.webauthn_challenges TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.webauthn_challenges TO dashboard_user;


--
-- Name: TABLE webauthn_credentials; Type: ACL; Schema: auth; Owner: supabase_auth_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.webauthn_credentials TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE auth.webauthn_credentials TO dashboard_user;


--
-- Name: TABLE job; Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT SELECT ON TABLE cron.job TO postgres WITH GRANT OPTION;


--
-- Name: TABLE job_run_details; Type: ACL; Schema: cron; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE cron.job_run_details TO postgres WITH GRANT OPTION;


--
-- Name: TABLE pg_stat_statements; Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE extensions.pg_stat_statements TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE extensions.pg_stat_statements TO dashboard_user;


--
-- Name: TABLE pg_stat_statements_info; Type: ACL; Schema: extensions; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE extensions.pg_stat_statements_info TO postgres WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE extensions.pg_stat_statements_info TO dashboard_user;


--
-- Name: TABLE bvp_stats; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.bvp_stats TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.bvp_stats TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.bvp_stats TO service_role;


--
-- Name: TABLE game_info; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.game_info TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.game_info TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.game_info TO service_role;


--
-- Name: TABLE model_training_props; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.model_training_props TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.model_training_props TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.model_training_props TO service_role;


--
-- Name: TABLE player_stats; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_stats TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_stats TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_stats TO service_role;


--
-- Name: TABLE player_derived_stats; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_derived_stats TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_derived_stats TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_derived_stats TO service_role;


--
-- Name: TABLE player_ids; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_ids TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_ids TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_ids TO service_role;


--
-- Name: TABLE player_profiles_cache; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_profiles_cache TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_profiles_cache TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_profiles_cache TO service_role;


--
-- Name: TABLE player_props; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_props TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_props TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_props TO service_role;


--
-- Name: TABLE player_streak_history; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_history TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_history TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_history TO service_role;


--
-- Name: TABLE player_streak_profiles; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_profiles TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_profiles TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_streak_profiles TO service_role;


--
-- Name: TABLE player_team_by_game; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_team_by_game TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_team_by_game TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.player_team_by_game TO service_role;


--
-- Name: TABLE prop_features_precomputed; Type: ACL; Schema: mlb; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.prop_features_precomputed TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.prop_features_precomputed TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE mlb.prop_features_precomputed TO service_role;


--
-- Name: TABLE mlb_team_map; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.mlb_team_map TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.mlb_team_map TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.mlb_team_map TO service_role;


--
-- Name: TABLE opp_starter_per_game; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.opp_starter_per_game TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.opp_starter_per_game TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE public.opp_starter_per_game TO service_role;


--
-- Name: TABLE messages; Type: ACL; Schema: realtime; Owner: supabase_realtime_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages TO dashboard_user;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO anon;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO authenticated;
GRANT SELECT,INSERT,UPDATE ON TABLE realtime.messages TO service_role;


--
-- Name: TABLE messages_2026_02_21; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_21 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_21 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_22; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_22 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_22 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_23; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_23 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_23 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_24; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_24 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_24 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_25; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_25 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_25 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_26; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_26 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_26 TO dashboard_user;


--
-- Name: TABLE messages_2026_02_27; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_27 TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.messages_2026_02_27 TO dashboard_user;


--
-- Name: TABLE schema_migrations; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.schema_migrations TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.schema_migrations TO dashboard_user;
GRANT SELECT ON TABLE realtime.schema_migrations TO anon;
GRANT SELECT ON TABLE realtime.schema_migrations TO authenticated;
GRANT SELECT ON TABLE realtime.schema_migrations TO service_role;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.schema_migrations TO supabase_realtime_admin;


--
-- Name: TABLE subscription; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.subscription TO postgres;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.subscription TO dashboard_user;
GRANT SELECT ON TABLE realtime.subscription TO anon;
GRANT SELECT ON TABLE realtime.subscription TO authenticated;
GRANT SELECT ON TABLE realtime.subscription TO service_role;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE realtime.subscription TO supabase_realtime_admin;


--
-- Name: SEQUENCE subscription_id_seq; Type: ACL; Schema: realtime; Owner: supabase_admin
--

GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO postgres;
GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO dashboard_user;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO anon;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO authenticated;
GRANT USAGE ON SEQUENCE realtime.subscription_id_seq TO service_role;
GRANT ALL ON SEQUENCE realtime.subscription_id_seq TO supabase_realtime_admin;


--
-- Name: TABLE buckets; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

REVOKE SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets FROM supabase_storage_admin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets TO supabase_storage_admin WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets TO service_role;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets TO postgres WITH GRANT OPTION;


--
-- Name: TABLE buckets_analytics; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets_analytics TO service_role;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets_analytics TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.buckets_analytics TO anon;


--
-- Name: TABLE buckets_vectors; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT SELECT ON TABLE storage.buckets_vectors TO service_role;
GRANT SELECT ON TABLE storage.buckets_vectors TO authenticated;
GRANT SELECT ON TABLE storage.buckets_vectors TO anon;


--
-- Name: TABLE objects; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

REVOKE SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects FROM supabase_storage_admin;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects TO supabase_storage_admin WITH GRANT OPTION;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects TO authenticated;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects TO service_role;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.objects TO postgres WITH GRANT OPTION;


--
-- Name: TABLE s3_multipart_uploads; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.s3_multipart_uploads TO service_role;
GRANT SELECT ON TABLE storage.s3_multipart_uploads TO authenticated;
GRANT SELECT ON TABLE storage.s3_multipart_uploads TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.s3_multipart_uploads TO postgres;


--
-- Name: TABLE s3_multipart_uploads_parts; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.s3_multipart_uploads_parts TO service_role;
GRANT SELECT ON TABLE storage.s3_multipart_uploads_parts TO authenticated;
GRANT SELECT ON TABLE storage.s3_multipart_uploads_parts TO anon;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE storage.s3_multipart_uploads_parts TO postgres;


--
-- Name: TABLE vector_indexes; Type: ACL; Schema: storage; Owner: supabase_storage_admin
--

GRANT SELECT ON TABLE storage.vector_indexes TO service_role;
GRANT SELECT ON TABLE storage.vector_indexes TO authenticated;
GRANT SELECT ON TABLE storage.vector_indexes TO anon;


--
-- Name: TABLE secrets; Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT SELECT,REFERENCES,DELETE,TRUNCATE ON TABLE vault.secrets TO postgres WITH GRANT OPTION;
GRANT SELECT,DELETE ON TABLE vault.secrets TO service_role;


--
-- Name: TABLE decrypted_secrets; Type: ACL; Schema: vault; Owner: supabase_admin
--

GRANT SELECT,REFERENCES,DELETE,TRUNCATE ON TABLE vault.decrypted_secrets TO postgres WITH GRANT OPTION;
GRANT SELECT,DELETE ON TABLE vault.decrypted_secrets TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON SEQUENCES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT ALL ON FUNCTIONS TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: auth; Owner: supabase_auth_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: cron; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA cron GRANT ALL ON SEQUENCES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: cron; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA cron GRANT ALL ON FUNCTIONS TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: cron; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA cron GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT ALL ON SEQUENCES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT ALL ON FUNCTIONS TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: extensions; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA extensions GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: graphql; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: graphql_public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA graphql_public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON SEQUENCES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT ALL ON FUNCTIONS TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: realtime; Owner: supabase_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA realtime GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO dashboard_user;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: storage; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA storage GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO service_role;


--
-- Name: issue_graphql_placeholder; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_graphql_placeholder ON sql_drop
         WHEN TAG IN ('DROP EXTENSION')
   EXECUTE FUNCTION extensions.set_graphql_placeholder();


ALTER EVENT TRIGGER issue_graphql_placeholder OWNER TO supabase_admin;

--
-- Name: issue_pg_cron_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_cron_access ON ddl_command_end
         WHEN TAG IN ('CREATE EXTENSION')
   EXECUTE FUNCTION extensions.grant_pg_cron_access();


ALTER EVENT TRIGGER issue_pg_cron_access OWNER TO supabase_admin;

--
-- Name: issue_pg_graphql_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_graphql_access ON ddl_command_end
         WHEN TAG IN ('CREATE FUNCTION')
   EXECUTE FUNCTION extensions.grant_pg_graphql_access();


ALTER EVENT TRIGGER issue_pg_graphql_access OWNER TO supabase_admin;

--
-- Name: issue_pg_net_access; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER issue_pg_net_access ON ddl_command_end
         WHEN TAG IN ('CREATE EXTENSION')
   EXECUTE FUNCTION extensions.grant_pg_net_access();


ALTER EVENT TRIGGER issue_pg_net_access OWNER TO supabase_admin;

--
-- Name: pgrst_ddl_watch; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER pgrst_ddl_watch ON ddl_command_end
   EXECUTE FUNCTION extensions.pgrst_ddl_watch();


ALTER EVENT TRIGGER pgrst_ddl_watch OWNER TO supabase_admin;

--
-- Name: pgrst_drop_watch; Type: EVENT TRIGGER; Schema: -; Owner: supabase_admin
--

CREATE EVENT TRIGGER pgrst_drop_watch ON sql_drop
   EXECUTE FUNCTION extensions.pgrst_drop_watch();


ALTER EVENT TRIGGER pgrst_drop_watch OWNER TO supabase_admin;

--
-- PostgreSQL database dump complete
--

\unrestrict rKxIbnTKlNMLVZjod7Bu8MAbWC8XOAzbLckTqpowtOXlTgOrfcEnrbypgBCGEhG

