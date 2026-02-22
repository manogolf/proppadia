--
-- PostgreSQL database dump
--

\restrict Jb6A4y0wvbUAM4HIz6xxPpNIkTycIJqRNnPqfEcRdeixLRJdPaouLIDpidHILSC

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
-- Name: nhl; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA nhl;


--
-- Name: _safe_bigint(text); Type: FUNCTION; Schema: nhl; Owner: -
--

CREATE FUNCTION nhl._safe_bigint(t text) RETURNS bigint
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+\s*$' THEN t::bigint ELSE NULL END
$_$;


--
-- Name: _safe_bool(text); Type: FUNCTION; Schema: nhl; Owner: -
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


--
-- Name: _safe_int(text); Type: FUNCTION; Schema: nhl; Owner: -
--

CREATE FUNCTION nhl._safe_int(t text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+\s*$' THEN t::integer ELSE NULL END
$_$;


--
-- Name: _safe_num(text); Type: FUNCTION; Schema: nhl; Owner: -
--

CREATE FUNCTION nhl._safe_num(t text) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    AS $_$
  SELECT CASE WHEN t ~ '^\s*-?\d+(\.\d+)?\s*$' THEN t::numeric ELSE NULL END
$_$;


--
-- Name: canonical_game_id(integer, integer); Type: FUNCTION; Schema: nhl; Owner: -
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


--
-- Name: load_points_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: -
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


--
-- Name: load_saves_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: -
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


--
-- Name: load_sog_predictions_from_stage(text, jsonb, text, text); Type: FUNCTION; Schema: nhl; Owner: -
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


--
-- Name: set_updated_at(); Type: FUNCTION; Schema: nhl; Owner: -
--

CREATE FUNCTION nhl.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _points_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE UNLOGGED TABLE nhl._points_stage (
    player_id bigint,
    game_id bigint,
    game_date date,
    goals integer,
    assists integer
);


--
-- Name: backfill_progress; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.backfill_progress (
    task text NOT NULL,
    last_game_id bigint,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: data_quality_audit; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.data_quality_audit (
    audit_date date NOT NULL,
    check_name text NOT NULL,
    level text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT data_quality_audit_level_check CHECK ((level = ANY (ARRAY['info'::text, 'warn'::text, 'error'::text])))
);


--
-- Name: games; Type: TABLE; Schema: nhl; Owner: -
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
    CONSTRAINT games_short_consistency_chk CHECK ((short_game_id = ((game_type * 10000) + game_number)))
);


--
-- Name: goalie_game_logs_raw; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: v_goalie_game_logs_played; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: goalie_roll_feats_m; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: goalie_rolling_agg; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: goalies2023_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: goalies2023_stage_raw; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: goalies_szn_sit; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: goalies_szn_sit_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.goalies_szn_sit_stage (
    "playerId" text,
    season text,
    name text,
    team text,
    "position" text,
    situation text,
    games_played text,
    icetime text,
    "xGoals" text,
    goals text,
    unblocked_shot_attempts text,
    "xRebounds" text,
    rebounds text,
    "xFreeze" text,
    "freeze" text,
    "xOnGoal" text,
    ongoal text,
    "xPlayStopped" text,
    "playStopped" text,
    "xPlayContinuedInZone" text,
    "playContinuedInZone" text,
    "xPlayContinuedOutsideZone" text,
    "playContinuedOutsideZone" text,
    "flurryAdjustedxGoals" text,
    "lowDangerShots" text,
    "mediumDangerShots" text,
    "highDangerShots" text,
    "lowDangerxGoals" text,
    "mediumDangerxGoals" text,
    "highDangerxGoals" text,
    "lowDangerGoals" text,
    "mediumDangerGoals" text,
    "highDangerGoals" text,
    blocked_shot_attempts text,
    "penalityMinutes" text,
    penalties text
);


--
-- Name: import_game_external_ids_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_game_external_ids_stage (
    game_id bigint,
    provider text,
    provider_game_id text
);


--
-- Name: import_games_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_games_stage (
    game_id bigint,
    game_date date,
    start_time_utc timestamp with time zone,
    season text,
    game_type text,
    home_team_id integer,
    away_team_id integer
);


--
-- Name: import_goalie_logs_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: import_player_external_ids_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_player_external_ids_stage (
    player_id integer,
    provider text,
    provider_player_id text
);


--
-- Name: import_players_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: import_skater_logs_stage; Type: TABLE; Schema: nhl; Owner: -
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
    assists integer
);


--
-- Name: import_skater_logs_unmapped; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_skater_logs_unmapped (
    game_id bigint NOT NULL,
    nhl_id bigint,
    full_name text,
    team_side text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: import_skater_points_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: import_team_external_ids_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_team_external_ids_stage (
    team_id integer,
    provider text,
    provider_team_id text
);


--
-- Name: import_teams_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.import_teams_stage (
    team_id integer,
    abbr text,
    name text,
    city text,
    conference text,
    division text,
    active boolean
);


--
-- Name: keep_games_filter; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.keep_games_filter (
    game_id bigint NOT NULL
);


--
-- Name: lines_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: model_metadata; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.model_metadata (
    prop_type text NOT NULL,
    version text NOT NULL,
    trained_at timestamp with time zone DEFAULT now() NOT NULL,
    family text NOT NULL,
    params jsonb NOT NULL,
    training_start_date date,
    training_end_date date,
    metrics jsonb NOT NULL,
    feature_hash text NOT NULL,
    artifact_refs jsonb,
    CONSTRAINT model_metadata_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text])))
);


--
-- Name: model_versions; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.model_versions (
    prop_type text NOT NULL,
    version text NOT NULL,
    promoted_at timestamp with time zone DEFAULT now() NOT NULL,
    promoted_by text,
    reason text,
    is_active boolean DEFAULT false NOT NULL,
    CONSTRAINT model_versions_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text])))
);


--
-- Name: player_external_ids; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.player_external_ids (
    player_id integer NOT NULL,
    provider text NOT NULL,
    provider_player_id text NOT NULL
);


--
-- Name: player_game_2023_roll; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: player_game_2023_summary; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: player_game_2024_roll; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: player_game_2024_summary; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: player_game_shots_2023; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: shots_all; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: player_shot_history_denali; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: team_shot_history_denali; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: player_shot_phoenix_denali; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: players; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skater_points_raw; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_sog_player_game; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_sog_player_game_v2; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: sog_training_frame_phoenix; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: points_training_frame_phoenix; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: pp_roles_slate; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.pp_roles_slate (
    game_date date NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL,
    pp_share numeric,
    pp_unit text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: predictions; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: predictions_points_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.predictions_points_stage (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    "p_over_0.5" double precision,
    "p_over_1.5" double precision,
    "p_over_2.5" double precision,
    game_date date,
    p_over_0_5 double precision,
    p_over_1_5 double precision,
    p_over_2_5 double precision
);


--
-- Name: predictions_prediction_id_seq; Type: SEQUENCE; Schema: nhl; Owner: -
--

CREATE SEQUENCE nhl.predictions_prediction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: predictions_prediction_id_seq; Type: SEQUENCE OWNED BY; Schema: nhl; Owner: -
--

ALTER SEQUENCE nhl.predictions_prediction_id_seq OWNED BY nhl.predictions.prediction_id;


--
-- Name: predictions_saves_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.predictions_saves_stage (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    "p_over_24.5" double precision,
    "p_over_28.5" double precision,
    game_date date,
    p_over_24_5 double precision,
    p_over_28_5 double precision
);


--
-- Name: predictions_saves_stage_local; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.predictions_saves_stage_local (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    prop_line numeric NOT NULL,
    p_over double precision NOT NULL,
    model_family text NOT NULL,
    model_version text NOT NULL,
    slate_date date NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: predictions_sog_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.predictions_sog_stage (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    "p_over_0.5" double precision,
    "p_over_1.5" double precision,
    "p_over_2.5" double precision,
    "p_over_3.5" double precision,
    game_date date,
    p_over_0_5 double precision,
    p_over_1_5 double precision,
    p_over_2_5 double precision,
    p_over_3_5 double precision
);


--
-- Name: roster_names; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: roster_status; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.roster_status (
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL,
    active_flag boolean NOT NULL,
    line_role text,
    pp_unit text DEFAULT 'None'::text,
    asof_ts timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT roster_status_pp_unit_check CHECK ((pp_unit = ANY (ARRAY['PP1'::text, 'PP2'::text, 'None'::text])))
);


--
-- Name: schedule_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.schedule_stage (
    game_pk bigint NOT NULL,
    season integer NOT NULL,
    game_type smallint NOT NULL,
    game_date date NOT NULL,
    start_time_utc timestamp with time zone,
    home_team_code text NOT NULL,
    away_team_code text NOT NULL
);


--
-- Name: shift_toi_denali; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.shift_toi_denali (
    game_id bigint NOT NULL,
    season integer NOT NULL,
    player_id bigint NOT NULL,
    team text NOT NULL,
    is_home boolean,
    shifts integer NOT NULL,
    toi_seconds numeric NOT NULL,
    ev_toi_seconds numeric,
    pp_toi_seconds numeric,
    sh_toi_seconds numeric,
    oz_starts integer,
    dz_starts integer,
    nz_starts integer,
    fly_starts integer,
    oz_ends integer,
    dz_ends integer,
    nz_ends integer,
    fly_ends integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: shot_stats_denali; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: shots_rejects; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.shots_rejects (
    season integer,
    game_id integer,
    reason text,
    raw jsonb NOT NULL
);


--
-- Name: shots_stage_2023; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: shots_stage_2024; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skater_game_logs_raw; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skater_points_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.skater_points_stage (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    goals integer,
    assists integer,
    points integer,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: skater_roll_windows_v1; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: skater_rolling_agg; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skater_shot_game_totals; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: skaters2023_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skaters2023_stage_raw; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skaters_szn_sit; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skaters_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: skaters_szn_sit_stage; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: starters_goalies; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.starters_goalies (
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    goalie_id bigint NOT NULL,
    status text DEFAULT 'projected'::text NOT NULL,
    asof_ts timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT starters_goalies_status_check CHECK ((status = ANY (ARRAY['projected'::text, 'confirmed'::text, 'scratched'::text, 'unknown'::text])))
);


--
-- Name: team_context_rolling; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_external_ids; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.team_external_ids (
    team_id integer NOT NULL,
    provider text NOT NULL,
    provider_team_id text NOT NULL
);


--
-- Name: team_game_2023_roll; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_game_2023_summary; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_game_2024_roll; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_game_2024_summary; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_game_rates_raw; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.team_game_rates_raw (
    team_id bigint NOT NULL,
    game_id bigint NOT NULL,
    sf_per60 numeric(6,3),
    sa_per60 numeric(6,3),
    pp_time_minutes numeric(5,2),
    pk_time_minutes numeric(5,2),
    pace_index numeric(6,3)
);


--
-- Name: team_game_sit; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: team_game_sit_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.team_game_sit_stage (
    team text,
    season integer,
    name text,
    gameid bigint,
    playerteam text,
    opposingteam text,
    home_or_away text,
    gamedate date,
    "position" text,
    situation text,
    xgoalspercentage numeric,
    corsipercentage numeric,
    fenwickpercentage numeric,
    icetime numeric,
    xongoalfor numeric,
    xgoalsfor numeric,
    xreboundsfor numeric,
    xfreezefor numeric,
    xplaystoppedfor numeric,
    xplaycontinuedinzonefor numeric,
    xplaycontinuedoutsidezonefor numeric,
    flurryadjustedxgoalsfor numeric,
    scorevenueadjustedxgoalsfor numeric,
    flurryscorevenueadjustedxgoalsfor numeric,
    shotsongoalfor numeric,
    missedshotsfor numeric,
    blockedshotattemptsfor numeric,
    shotattemptsfor numeric,
    goalsfor numeric,
    reboundsfor numeric,
    reboundgoalsfor numeric,
    freezefor numeric,
    playstoppedfor numeric,
    playcontinuedinzonefor numeric,
    playcontinuedoutsidezonefor numeric,
    savedshotsongoalfor numeric,
    savedunblockedshotattemptsfor numeric,
    penaltiesfor numeric,
    penalityminutesfor numeric,
    faceoffswonfor numeric,
    hitsfor numeric,
    takeawaysfor numeric,
    giveawaysfor numeric,
    lowdangershotsfor numeric,
    mediumdangershotsfor numeric,
    highdangershotsfor numeric,
    lowdangerxgoalsfor numeric,
    mediumdangerxgoalsfor numeric,
    highdangerxgoalsfor numeric,
    lowdangergoalsfor numeric,
    mediumdangergoalsfor numeric,
    highdangergoalsfor numeric,
    scoreadjustedshotsattemptsfor numeric,
    unblockedshotattemptsfor numeric,
    scoreadjustedunblockedshotattemptsfor numeric,
    dzonegiveawaysfor numeric,
    xgoalsfromxreboundsofshotsfor numeric,
    xgoalsfromactualreboundsofshotsfor numeric,
    reboundxgoalsfor numeric,
    totalshotcreditfor numeric,
    scoreadjustedtotalshotcreditfor numeric,
    scoreflurryadjustedtotalshotcreditfor numeric,
    xongoalagainst numeric,
    xgoalsagainst numeric,
    xreboundsagainst numeric,
    xfreezeagainst numeric,
    xplaystoppedagainst numeric,
    xplaycontinuedinzoneagainst numeric,
    xplaycontinuedoutsidezoneagainst numeric,
    flurryadjustedxgoalsagainst numeric,
    scorevenueadjustedxgoalsagainst numeric,
    flurryscorevenueadjustedxgoalsagainst numeric,
    shotsongoalagainst numeric,
    missedshotsagainst numeric,
    blockedshotattemptsagainst numeric,
    shotattemptsagainst numeric,
    goalsagainst numeric,
    reboundsagainst numeric,
    reboundgoalsagainst numeric,
    freezeagainst numeric,
    playstoppedagainst numeric,
    playcontinuedinzoneagainst numeric,
    playcontinuedoutsidezoneagainst numeric,
    savedshotsongoalagainst numeric,
    savedunblockedshotattemptsagainst numeric,
    penaltiesagainst numeric,
    penalityminutesagainst numeric,
    faceoffswonagainst numeric,
    hitsagainst numeric,
    takeawaysagainst numeric,
    giveawaysagainst numeric,
    lowdangershotsagainst numeric,
    mediumdangershotsagainst numeric,
    highdangershotsagainst numeric,
    lowdangerxgoalsagainst numeric,
    mediumdangerxgoalsagainst numeric,
    highdangerxgoalsagainst numeric,
    lowdangergoalsagainst numeric,
    mediumdangergoalsagainst numeric,
    highdangergoalsagainst numeric,
    scoreadjustedshotsattemptsagainst numeric,
    unblockedshotattemptsagainst numeric,
    scoreadjustedunblockedshotattemptsagainst numeric,
    dzonegiveawaysagainst numeric,
    xgoalsfromxreboundsofshotsagainst numeric,
    xgoalsfromactualreboundsofshotsagainst numeric,
    reboundxgoalsagainst numeric,
    totalshotcreditagainst numeric,
    scoreadjustedtotalshotcreditagainst numeric,
    scoreflurryadjustedtotalshotcreditagainst numeric,
    playoffgame boolean
);


--
-- Name: team_roll10_m; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: teams; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: teams_game_sit; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: teams_game_sit_stage; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.teams_game_sit_stage (
    team text,
    season text,
    name text,
    "gameId" text,
    "playerTeam" text,
    "opposingTeam" text,
    home_or_away text,
    "gameDate" text,
    "position" text,
    situation text,
    "xGoalsPercentage" text,
    "corsiPercentage" text,
    "fenwickPercentage" text,
    "iceTime" text,
    "xOnGoalFor" text,
    "xGoalsFor" text,
    "xReboundsFor" text,
    "xFreezeFor" text,
    "xPlayStoppedFor" text,
    "xPlayContinuedInZoneFor" text,
    "xPlayContinuedOutsideZoneFor" text,
    "flurryAdjustedxGoalsFor" text,
    "scoreVenueAdjustedxGoalsFor" text,
    "flurryScoreVenueAdjustedxGoalsFor" text,
    "shotsOnGoalFor" text,
    "missedShotsFor" text,
    "blockedShotAttemptsFor" text,
    "shotAttemptsFor" text,
    "goalsFor" text,
    "reboundsFor" text,
    "reboundGoalsFor" text,
    "freezeFor" text,
    "playStoppedFor" text,
    "playContinuedInZoneFor" text,
    "playContinuedOutsideZoneFor" text,
    "savedShotsOnGoalFor" text,
    "savedUnblockedShotAttemptsFor" text,
    "penaltiesFor" text,
    "penalityMinutesFor" text,
    "faceOffsWonFor" text,
    "hitsFor" text,
    "takeawaysFor" text,
    "giveawaysFor" text,
    "lowDangerShotsFor" text,
    "mediumDangerShotsFor" text,
    "highDangerShotsFor" text,
    "lowDangerxGoalsFor" text,
    "mediumDangerxGoalsFor" text,
    "highDangerxGoalsFor" text,
    "lowDangerGoalsFor" text,
    "mediumDangerGoalsFor" text,
    "highDangerGoalsFor" text,
    "scoreAdjustedShotsAttemptsFor" text,
    "unblockedShotAttemptsFor" text,
    "scoreAdjustedUnblockedShotAttemptsFor" text,
    "dZoneGiveawaysFor" text,
    "xGoalsFromxReboundsOfShotsFor" text,
    "xGoalsFromActualReboundsOfShotsFor" text,
    "reboundxGoalsFor" text,
    "totalShotCreditFor" text,
    "scoreAdjustedTotalShotCreditFor" text,
    "scoreFlurryAdjustedTotalShotCreditFor" text,
    "xOnGoalAgainst" text,
    "xGoalsAgainst" text,
    "xReboundsAgainst" text,
    "xFreezeAgainst" text,
    "xPlayStoppedAgainst" text,
    "xPlayContinuedInZoneAgainst" text,
    "xPlayContinuedOutsideZoneAgainst" text,
    "flurryAdjustedxGoalsAgainst" text,
    "scoreVenueAdjustedxGoalsAgainst" text,
    "flurryScoreVenueAdjustedxGoalsAgainst" text,
    "shotsOnGoalAgainst" text,
    "missedShotsAgainst" text,
    "blockedShotAttemptsAgainst" text,
    "shotAttemptsAgainst" text,
    "goalsAgainst" text,
    "reboundsAgainst" text,
    "reboundGoalsAgainst" text,
    "freezeAgainst" text,
    "playStoppedAgainst" text,
    "playContinuedInZoneAgainst" text,
    "playContinuedOutsideZoneAgainst" text,
    "savedShotsOnGoalAgainst" text,
    "savedUnblockedShotAttemptsAgainst" text,
    "penaltiesAgainst" text,
    "penalityMinutesAgainst" text,
    "faceOffsWonAgainst" text,
    "hitsAgainst" text,
    "takeawaysAgainst" text,
    "giveawaysAgainst" text,
    "lowDangerShotsAgainst" text,
    "mediumDangerShotsAgainst" text,
    "highDangerShotsAgainst" text,
    "lowDangerxGoalsAgainst" text,
    "mediumDangerxGoalsAgainst" text,
    "highDangerxGoalsAgainst" text,
    "lowDangerGoalsAgainst" text,
    "mediumDangerGoalsAgainst" text,
    "highDangerGoalsAgainst" text,
    "scoreAdjustedShotsAttemptsAgainst" text,
    "unblockedShotAttemptsAgainst" text,
    "scoreAdjustedUnblockedShotAttemptsAgainst" text,
    "dZoneGiveawaysAgainst" text,
    "xGoalsFromxReboundsOfShotsAgainst" text,
    "xGoalsFromActualReboundsOfShotsAgainst" text,
    "reboundxGoalsAgainst" text,
    "totalShotCreditAgainst" text,
    "scoreAdjustedTotalShotCreditAgainst" text,
    "scoreFlurryAdjustedTotalShotCreditAgainst" text,
    "playoffGame" text
);


--
-- Name: teams_szn_sit_denali; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: tf_skater_attempts_roll10; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: tf_team_roll10; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_goalie_saves_v2; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_goalie_saves_v2_ready; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_nhl_saves_enriched; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_nhl_saves_enr_filt; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_nhl_sog_enriched; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_nhl_sog_enriched_pregame; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_nhl_sog_enriched_pregame_v2; Type: TABLE; Schema: nhl; Owner: -
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
    team_num_shotwasongoal_for_last10 integer
);


--
-- Name: training_features_nhl_sog_enriched_pregame_v2_mt; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_shots; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_shots_2023; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_shots_2024; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: training_features_shots_v; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: training_features_sog_denali; Type: TABLE; Schema: nhl; Owner: -
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


--
-- Name: user_props; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.user_props (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    game_id bigint NOT NULL,
    player_id bigint NOT NULL,
    team_id bigint NOT NULL,
    opponent_id bigint NOT NULL,
    prop_type text NOT NULL,
    line numeric(4,1) NOT NULL,
    over_under text NOT NULL,
    CONSTRAINT user_props_over_under_check CHECK ((over_under = ANY (ARRAY['over'::text, 'under'::text]))),
    CONSTRAINT user_props_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text])))
);


--
-- Name: v_dqa_goalie_ready_coverage; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: v_dqa_sog_ready_coverage; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: v_predictions_points_stage_long; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_predictions_points_stage_long AS
 SELECT s.player_id,
    s.game_id,
    0.5::numeric(4,1) AS line,
    s.p_over_0_5 AS p_over
   FROM nhl.predictions_points_stage s
  WHERE (s.p_over_0_5 IS NOT NULL)
UNION ALL
 SELECT s.player_id,
    s.game_id,
    1.5::numeric(4,1) AS line,
    s.p_over_1_5 AS p_over
   FROM nhl.predictions_points_stage s
  WHERE (s.p_over_1_5 IS NOT NULL)
UNION ALL
 SELECT s.player_id,
    s.game_id,
    2.5::numeric(4,1) AS line,
    s.p_over_2_5 AS p_over
   FROM nhl.predictions_points_stage s
  WHERE (s.p_over_2_5 IS NOT NULL);


--
-- Name: v_predictions_saves_stage_long; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_predictions_saves_stage_long AS
 SELECT predictions_saves_stage.player_id,
    predictions_saves_stage.game_id,
    24.5::numeric(4,1) AS line,
    predictions_saves_stage.p_over_24_5 AS p_over
   FROM nhl.predictions_saves_stage
  WHERE (predictions_saves_stage.p_over_24_5 IS NOT NULL)
UNION ALL
 SELECT predictions_saves_stage.player_id,
    predictions_saves_stage.game_id,
    28.5::numeric(4,1) AS line,
    predictions_saves_stage.p_over_28_5 AS p_over
   FROM nhl.predictions_saves_stage
  WHERE (predictions_saves_stage.p_over_28_5 IS NOT NULL);


--
-- Name: v_predictions_sog_stage_long; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_predictions_sog_stage_long AS
 SELECT predictions_sog_stage.player_id,
    predictions_sog_stage.game_id,
    0.5::numeric(4,1) AS line,
    predictions_sog_stage.p_over_0_5 AS p_over
   FROM nhl.predictions_sog_stage
  WHERE (predictions_sog_stage.p_over_0_5 IS NOT NULL)
UNION ALL
 SELECT predictions_sog_stage.player_id,
    predictions_sog_stage.game_id,
    1.5::numeric(4,1) AS line,
    predictions_sog_stage.p_over_1_5 AS p_over
   FROM nhl.predictions_sog_stage
  WHERE (predictions_sog_stage.p_over_1_5 IS NOT NULL)
UNION ALL
 SELECT predictions_sog_stage.player_id,
    predictions_sog_stage.game_id,
    2.5::numeric(4,1) AS line,
    predictions_sog_stage.p_over_2_5 AS p_over
   FROM nhl.predictions_sog_stage
  WHERE (predictions_sog_stage.p_over_2_5 IS NOT NULL)
UNION ALL
 SELECT predictions_sog_stage.player_id,
    predictions_sog_stage.game_id,
    3.5::numeric(4,1) AS line,
    predictions_sog_stage.p_over_3_5 AS p_over
   FROM nhl.predictions_sog_stage
  WHERE (predictions_sog_stage.p_over_3_5 IS NOT NULL);


--
-- Name: v_skater_game_logs_played; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: v_slate_saves_features; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: v_slate_sog_features; Type: VIEW; Schema: nhl; Owner: -
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


--
-- Name: predictions prediction_id; Type: DEFAULT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.predictions ALTER COLUMN prediction_id SET DEFAULT nextval('nhl.predictions_prediction_id_seq'::regclass);


--
-- Name: backfill_progress backfill_progress_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.backfill_progress
    ADD CONSTRAINT backfill_progress_pkey PRIMARY KEY (task);


--
-- Name: data_quality_audit data_quality_audit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.data_quality_audit
    ADD CONSTRAINT data_quality_audit_pkey PRIMARY KEY (audit_date, check_name);


--
-- Name: games games_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);


--
-- Name: games games_unique_short; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_unique_short UNIQUE (season, short_game_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: goalie_rolling_agg goalie_rolling_agg_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_rolling_agg
    ADD CONSTRAINT goalie_rolling_agg_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: keep_games_filter keep_games_filter_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.keep_games_filter
    ADD CONSTRAINT keep_games_filter_pkey PRIMARY KEY (game_id);


--
-- Name: model_metadata model_metadata_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.model_metadata
    ADD CONSTRAINT model_metadata_pkey PRIMARY KEY (prop_type, version);


--
-- Name: model_versions model_versions_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.model_versions
    ADD CONSTRAINT model_versions_pkey PRIMARY KEY (prop_type, version);


--
-- Name: pp_roles_slate pk_pp_roles_slate; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.pp_roles_slate
    ADD CONSTRAINT pk_pp_roles_slate PRIMARY KEY (game_date, team_id, player_id);


--
-- Name: player_external_ids player_external_ids_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_pkey PRIMARY KEY (player_id, provider);


--
-- Name: player_external_ids player_external_ids_provider_provider_player_id_key; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_provider_provider_player_id_key UNIQUE (provider, provider_player_id);


--
-- Name: players players_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);


--
-- Name: predictions predictions_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.predictions
    ADD CONSTRAINT predictions_pkey PRIMARY KEY (prediction_id);


--
-- Name: roster_names roster_names_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_names
    ADD CONSTRAINT roster_names_pkey PRIMARY KEY (player_id);


--
-- Name: roster_status roster_status_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_pkey PRIMARY KEY (game_id, team_id, player_id, asof_ts);


--
-- Name: roster_status roster_status_unique; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_unique UNIQUE (team_id, asof_ts, game_id, player_id);


--
-- Name: shift_toi_denali shift_toi_denali_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.shift_toi_denali
    ADD CONSTRAINT shift_toi_denali_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: shot_stats_denali shot_stats_denali_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.shot_stats_denali
    ADD CONSTRAINT shot_stats_denali_pkey PRIMARY KEY (shooterplayerid, game_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: skater_points_raw skater_points_raw_pk; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_points_raw
    ADD CONSTRAINT skater_points_raw_pk PRIMARY KEY (player_id, game_id);


--
-- Name: skater_rolling_agg skater_rolling_agg_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: starters_goalies starters_goalies_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.starters_goalies
    ADD CONSTRAINT starters_goalies_pkey PRIMARY KEY (game_id, team_id, asof_ts);


--
-- Name: team_context_rolling team_context_rolling_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_context_rolling
    ADD CONSTRAINT team_context_rolling_pkey PRIMARY KEY (team_id, game_id);


--
-- Name: team_external_ids team_external_ids_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_pkey PRIMARY KEY (team_id, provider);


--
-- Name: team_external_ids team_external_ids_provider_provider_team_id_key; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_provider_provider_team_id_key UNIQUE (provider, provider_team_id);


--
-- Name: team_game_rates_raw team_game_rates_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_game_rates_raw
    ADD CONSTRAINT team_game_rates_raw_pkey PRIMARY KEY (team_id, game_id);


--
-- Name: team_game_sit team_game_sit_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_game_sit
    ADD CONSTRAINT team_game_sit_pkey PRIMARY KEY (game_id, team_code, situation);


--
-- Name: teams teams_abbr_key; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_abbr_key UNIQUE (team);


--
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- Name: tf_team_roll10 tf_team_roll10_team_date_unique; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.tf_team_roll10
    ADD CONSTRAINT tf_team_roll10_team_date_unique UNIQUE (team_id, game_date);


--
-- Name: training_features_goalie_saves_v2 training_features_goalie_saves_v2_pk; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.training_features_goalie_saves_v2
    ADD CONSTRAINT training_features_goalie_saves_v2_pk PRIMARY KEY (player_id, game_id);


--
-- Name: player_external_ids uq_player_external_ids_player_provider; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT uq_player_external_ids_player_provider UNIQUE (player_id, provider);


--
-- Name: predictions uq_pred; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.predictions
    ADD CONSTRAINT uq_pred UNIQUE (prop, player_id, game_id, line, feature_hash);


--
-- Name: user_props user_props_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_pkey PRIMARY KEY (id);


--
-- Name: goalie_logs_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX goalie_logs_game_idx ON nhl.goalie_game_logs_raw USING btree (game_id);


--
-- Name: goalie_logs_opp_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX goalie_logs_opp_idx ON nhl.goalie_game_logs_raw USING btree (opponent_id);


--
-- Name: goalie_logs_team_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX goalie_logs_team_idx ON nhl.goalie_game_logs_raw USING btree (team_id);


--
-- Name: goalie_roll_feats_m_uniq; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX goalie_roll_feats_m_uniq ON nhl.goalie_roll_feats_m USING btree (player_id, game_id);


--
-- Name: goalie_roll_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX goalie_roll_idx ON nhl.goalie_rolling_agg USING btree (game_id);


--
-- Name: gss_player_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX gss_player_idx ON nhl.goalies_szn_sit USING btree (player_id);


--
-- Name: gss_player_sit_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX gss_player_sit_idx ON nhl.goalies_szn_sit USING btree (player_id, season, situation);


--
-- Name: gss_sit_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX gss_sit_idx ON nhl.goalies_szn_sit USING btree (situation);


--
-- Name: gss_team_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX gss_team_idx ON nhl.goalies_szn_sit USING btree (team_abbr, season);


--
-- Name: idx_feat_saves_goalie_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_feat_saves_goalie_date ON nhl.training_features_nhl_saves_enriched USING btree (player_id, game_date);


--
-- Name: idx_games_season_short; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_games_season_short ON nhl.games USING btree (season, short_game_id);


--
-- Name: idx_games_teams_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_games_teams_date ON nhl.games USING btree (game_date, home_team_id, away_team_id);


--
-- Name: idx_goalie_raw_game_id; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_goalie_raw_game_id ON nhl.goalie_game_logs_raw USING btree (game_id);


--
-- Name: idx_goalie_raw_player_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_goalie_raw_player_date ON nhl.goalie_game_logs_raw USING btree (player_id, game_date);


--
-- Name: idx_goalie_raw_team_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_goalie_raw_team_date ON nhl.goalie_game_logs_raw USING btree (team_id, game_date);


--
-- Name: idx_predictions_created; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_predictions_created ON nhl.predictions USING btree (created_at);


--
-- Name: idx_predictions_lookup; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_predictions_lookup ON nhl.predictions USING btree (prop, game_id, player_id, line);


--
-- Name: idx_saves_ready_date_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_saves_ready_date_game ON nhl.training_features_goalie_saves_v2_ready USING btree (game_date, game_id);


--
-- Name: idx_saves_ready_date_player; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_saves_ready_date_player ON nhl.training_features_goalie_saves_v2_ready USING btree (game_date, player_id);


--
-- Name: idx_saves_stage_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_saves_stage_date ON nhl.predictions_saves_stage USING btree (game_date);


--
-- Name: idx_shift_toi_denali_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shift_toi_denali_game ON nhl.shift_toi_denali USING btree (game_id);


--
-- Name: idx_shift_toi_denali_player; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shift_toi_denali_player ON nhl.shift_toi_denali USING btree (player_id);


--
-- Name: idx_shift_toi_denali_season_team; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shift_toi_denali_season_team ON nhl.shift_toi_denali USING btree (season, team);


--
-- Name: idx_shot_stats_denali_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shot_stats_denali_game ON nhl.shot_stats_denali USING btree (game_id);


--
-- Name: idx_shot_stats_denali_season_player; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shot_stats_denali_season_player ON nhl.shot_stats_denali USING btree (season, shooterplayerid);


--
-- Name: idx_shots_all_game_id; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shots_all_game_id ON nhl.shots_all USING btree (game_id);


--
-- Name: idx_shots_all_season; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shots_all_season ON nhl.shots_all USING btree (season);


--
-- Name: idx_shots_all_shooter; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_shots_all_shooter ON nhl.shots_all USING btree (shooterplayerid);


--
-- Name: idx_skater_game_logs_raw_player_date_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_skater_game_logs_raw_player_date_game ON nhl.skater_game_logs_raw USING btree (player_id, game_date, game_id);


--
-- Name: idx_skater_raw_game_id; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_skater_raw_game_id ON nhl.skater_game_logs_raw USING btree (game_id);


--
-- Name: idx_skater_raw_player_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_skater_raw_player_date ON nhl.skater_game_logs_raw USING btree (player_id, game_date);


--
-- Name: idx_skater_raw_team_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_skater_raw_team_date ON nhl.skater_game_logs_raw USING btree (team_id, game_date);


--
-- Name: idx_skater_rolling_agg_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_skater_rolling_agg_player_game ON nhl.skater_rolling_agg USING btree (player_id, game_id);


--
-- Name: idx_sog_enriched_v2_player; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_enriched_v2_player ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (player_id);


--
-- Name: idx_sog_enriched_v2_season_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_enriched_v2_season_game ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (season, game_id);


--
-- Name: idx_sog_enriched_v2_team; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_enriched_v2_team ON nhl.training_features_nhl_sog_enriched_pregame_v2 USING btree (team_id);


--
-- Name: idx_sog_pregame_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_pregame_date ON nhl.training_features_nhl_sog_enriched_pregame USING btree (game_date);


--
-- Name: idx_sog_pregame_player_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_pregame_player_date ON nhl.training_features_nhl_sog_enriched_pregame USING btree (player_id, game_date);


--
-- Name: idx_sog_stage_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_stage_date ON nhl.predictions_sog_stage USING btree (game_date);


--
-- Name: idx_team_game_sit_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_team_game_sit_date ON nhl.team_game_sit USING btree (game_date);


--
-- Name: idx_team_game_sit_team; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_team_game_sit_team ON nhl.team_game_sit USING btree (team_code, season, situation);


--
-- Name: idx_tf_shots_season; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_tf_shots_season ON nhl.training_features_shots USING btree (season);


--
-- Name: idx_tf_team_roll10_game_team_opp; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_tf_team_roll10_game_team_opp ON nhl.tf_team_roll10 USING btree (game_id, team_id, opponent_id);


--
-- Name: idx_tf_team_roll10_team_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_tf_team_roll10_team_game ON nhl.tf_team_roll10 USING btree (team_id, game_id);


--
-- Name: idx_training_features_sog_denali_player_date_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_training_features_sog_denali_player_date_game ON nhl.training_features_sog_denali USING btree (player_id, game_date, game_id);


--
-- Name: idx_training_goalie_game_team_opp; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_training_goalie_game_team_opp ON nhl.training_features_goalie_saves_v2 USING btree (game_id, team_id, opponent_id);


--
-- Name: import_skater_points_stage_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX import_skater_points_stage_game_idx ON nhl.import_skater_points_stage USING btree (game_date, game_id);


--
-- Name: import_skater_points_stage_pid_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX import_skater_points_stage_pid_idx ON nhl.import_skater_points_stage USING btree (player_id);


--
-- Name: ix_goalies2023_stage_player_season; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_goalies2023_stage_player_season ON nhl.goalies2023_stage USING btree (((playerid)::bigint), ((season)::integer));


--
-- Name: ix_player_external_ids_provider; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_player_external_ids_provider ON nhl.player_external_ids USING btree (provider);


--
-- Name: ix_skaters2023_stage_player_season; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_skaters2023_stage_player_season ON nhl.skaters2023_stage USING btree (((playerid)::bigint), ((season)::integer));


--
-- Name: ix_unmapped_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_unmapped_game ON nhl.import_skater_logs_unmapped USING btree (game_id);


--
-- Name: model_versions_one_active_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX model_versions_one_active_idx ON nhl.model_versions USING btree (prop_type) WHERE (is_active = true);


--
-- Name: roster_status_latest_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX roster_status_latest_idx ON nhl.roster_status USING btree (game_id, player_id, asof_ts DESC);


--
-- Name: roster_status_uk; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX roster_status_uk ON nhl.roster_status USING btree (game_id, team_id, player_id);


--
-- Name: sk_logs_game_player_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX sk_logs_game_player_idx ON nhl.skater_game_logs_raw USING btree (game_id, player_id);


--
-- Name: skater_logs_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_logs_game_idx ON nhl.skater_game_logs_raw USING btree (game_id);


--
-- Name: skater_logs_opp_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_logs_opp_idx ON nhl.skater_game_logs_raw USING btree (opponent_id);


--
-- Name: skater_logs_team_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_logs_team_idx ON nhl.skater_game_logs_raw USING btree (team_id);


--
-- Name: skater_points_raw_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_points_raw_date_idx ON nhl.skater_points_raw USING btree (game_date);


--
-- Name: skater_points_raw_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_points_raw_game_idx ON nhl.skater_points_raw USING btree (game_date, game_id);


--
-- Name: skater_points_raw_pid_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_points_raw_pid_idx ON nhl.skater_points_raw USING btree (player_id);


--
-- Name: skater_points_raw_uniq; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX skater_points_raw_uniq ON nhl.skater_points_raw USING btree (player_id, game_id);


--
-- Name: skater_roll_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_roll_idx ON nhl.skater_rolling_agg USING btree (game_id);


--
-- Name: skaters_szn_sit_player_season_sit_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skaters_szn_sit_player_season_sit_idx ON nhl.skaters_szn_sit USING btree (player_id, season, situation);


--
-- Name: skaters_szn_sit_team_season_sit_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skaters_szn_sit_team_season_sit_idx ON nhl.skaters_szn_sit USING btree (team_abbr, season, situation);


--
-- Name: skglr_player_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skglr_player_date_idx ON nhl.skater_game_logs_raw USING btree (player_id, game_date, game_id);


--
-- Name: skglr_team_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skglr_team_date_idx ON nhl.skater_game_logs_raw USING btree (team_id, game_date, game_id);


--
-- Name: sklr_offenders_by_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX sklr_offenders_by_game_idx ON nhl.skater_game_logs_raw USING btree (game_id) WHERE ((shots_on_goal > 0) AND ((ev_sog IS NULL) OR (pp_sog IS NULL) OR (sh_sog IS NULL) OR (((COALESCE(ev_sog, 0) + COALESCE(pp_sog, 0)) + COALESCE(sh_sog, 0)) <> shots_on_goal)));


--
-- Name: spr_game_team_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX spr_game_team_idx ON nhl.skater_points_raw USING btree (game_id, team_id);


--
-- Name: starters_goalies_latest_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX starters_goalies_latest_idx ON nhl.starters_goalies USING btree (game_id, team_id, asof_ts DESC);


--
-- Name: team_roll10_m_uniq; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX team_roll10_m_uniq ON nhl.team_roll10_m USING btree (team_id, game_date, game_id);


--
-- Name: tf_points_enriched_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_points_enriched_game_date_idx ON nhl.skater_points_raw USING btree (game_date);


--
-- Name: tf_points_enriched_player_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_points_enriched_player_idx ON nhl.skater_points_raw USING btree (player_id, game_date, game_id);


--
-- Name: tf_team_roll10_team_id_game_id_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_team_roll10_team_id_game_id_idx ON nhl.tf_team_roll10 USING btree (team_id, game_id);


--
-- Name: tgs_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tgs_date_idx ON nhl.teams_game_sit USING btree (game_date);


--
-- Name: tgs_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tgs_game_idx ON nhl.teams_game_sit USING btree (game_id);


--
-- Name: tgs_opp_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tgs_opp_idx ON nhl.teams_game_sit USING btree (opp_abbr, season);


--
-- Name: tgs_team_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tgs_team_idx ON nhl.teams_game_sit USING btree (team_abbr, season);


--
-- Name: tgs_team_sit_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tgs_team_sit_idx ON nhl.teams_game_sit USING btree (team_abbr, situation);


--
-- Name: uq_import_goalie_logs_stage_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_import_goalie_logs_stage_player_game ON nhl.import_goalie_logs_stage USING btree (player_id, game_id);


--
-- Name: uq_nhl_predictions_player_game_prop_line; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_nhl_predictions_player_game_prop_line ON nhl.predictions USING btree (player_id, game_id, prop, line);


--
-- Name: uq_saves_ready_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_saves_ready_player_game ON nhl.training_features_goalie_saves_v2_ready USING btree (player_id, game_id);


--
-- Name: uq_tf_skater_attempts_roll10; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_tf_skater_attempts_roll10 ON nhl.tf_skater_attempts_roll10 USING btree (player_id, game_id);


--
-- Name: uq_training_features_goalie_saves_v2_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_training_features_goalie_saves_v2_player_game ON nhl.training_features_goalie_saves_v2 USING btree (player_id, game_id);


--
-- Name: uq_training_features_sog_denali_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX uq_training_features_sog_denali_player_game ON nhl.training_features_sog_denali USING btree (player_id, game_id);


--
-- Name: user_props_lookup_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX user_props_lookup_idx ON nhl.user_props USING btree (game_id, player_id, prop_type);


--
-- Name: ux_import_skater_logs_stage_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_import_skater_logs_stage_player_game ON nhl.import_skater_logs_stage USING btree (player_id, game_id);


--
-- Name: ux_player_external_ids_provider_key; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_player_external_ids_provider_key ON nhl.player_external_ids USING btree (provider, provider_player_id);


--
-- Name: ux_predictions_unique; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_predictions_unique ON nhl.predictions USING btree (prop, player_id, game_id, line, feature_hash);


--
-- Name: ux_saves_enr; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_saves_enr ON nhl.training_features_nhl_saves_enriched USING btree (player_id, game_id);


--
-- Name: ux_saves_enr_filt; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_saves_enr_filt ON nhl.training_features_nhl_saves_enr_filt USING btree (player_id, game_id);


--
-- Name: ux_sog_pregame_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_sog_pregame_player_game ON nhl.training_features_nhl_sog_enriched_pregame USING btree (player_id, game_id);


--
-- Name: ux_sog_pregame_v2_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_sog_pregame_v2_player_game ON nhl.training_features_nhl_sog_enriched_pregame_v2_mt USING btree (player_id, game_id);


--
-- Name: games set_updated_at_nhl_games; Type: TRIGGER; Schema: nhl; Owner: -
--

CREATE TRIGGER set_updated_at_nhl_games BEFORE UPDATE ON nhl.games FOR EACH ROW EXECUTE FUNCTION nhl.set_updated_at();


--
-- Name: shift_toi_denali set_updated_at_nhl_shift_toi_denali; Type: TRIGGER; Schema: nhl; Owner: -
--

CREATE TRIGGER set_updated_at_nhl_shift_toi_denali BEFORE UPDATE ON nhl.shift_toi_denali FOR EACH ROW EXECUTE FUNCTION nhl.set_updated_at();


--
-- Name: shot_stats_denali set_updated_at_nhl_shot_stats_denali; Type: TRIGGER; Schema: nhl; Owner: -
--

CREATE TRIGGER set_updated_at_nhl_shot_stats_denali BEFORE UPDATE ON nhl.shot_stats_denali FOR EACH ROW EXECUTE FUNCTION nhl.set_updated_at();


--
-- Name: games games_away_team_fk; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_away_team_fk FOREIGN KEY (away_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: games games_home_team_fk; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_home_team_fk FOREIGN KEY (home_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_rolling_agg goalie_rolling_agg_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_rolling_agg
    ADD CONSTRAINT goalie_rolling_agg_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: player_external_ids player_external_ids_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.player_external_ids
    ADD CONSTRAINT player_external_ids_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id) ON DELETE CASCADE;


--
-- Name: players players_current_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.players
    ADD CONSTRAINT players_current_team_id_fkey FOREIGN KEY (current_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: roster_status roster_status_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: roster_status roster_status_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: skater_game_logs_raw skater_game_logs_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: skater_rolling_agg skater_rolling_agg_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: starters_goalies starters_goalies_goalie_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.starters_goalies
    ADD CONSTRAINT starters_goalies_goalie_id_fkey FOREIGN KEY (goalie_id) REFERENCES nhl.players(player_id);


--
-- Name: starters_goalies starters_goalies_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.starters_goalies
    ADD CONSTRAINT starters_goalies_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: team_context_rolling team_context_rolling_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_context_rolling
    ADD CONSTRAINT team_context_rolling_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: team_external_ids team_external_ids_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_external_ids
    ADD CONSTRAINT team_external_ids_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id) ON DELETE CASCADE;


--
-- Name: team_game_rates_raw team_game_rates_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_game_rates_raw
    ADD CONSTRAINT team_game_rates_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: user_props user_props_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: user_props user_props_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: user_props user_props_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- PostgreSQL database dump complete
--

\unrestrict Jb6A4y0wvbUAM4HIz6xxPpNIkTycIJqRNnPqfEcRdeixLRJdPaouLIDpidHILSC

