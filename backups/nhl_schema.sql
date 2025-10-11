--
-- PostgreSQL database dump
--

\restrict T6vcX5DtuLcgcTFeBQ1MhegAkPq7CVdPFKBXx8fCGUueQKUkKpH2nJzBCfKkML5

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


SET default_tablespace = '';

SET default_table_access_method = heap;

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
    pace_matchup_index numeric
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
    g.saves,
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
    t.season_save_pct
   FROM (nhl.training_features_goalie_saves_v2 t
     JOIN nhl.goalie_game_logs_raw g ON (((g.player_id = t.player_id) AND (g.game_id = t.game_id))))
  WHERE ((t.d10_shots_faced_per60 IS NOT NULL) AND (t.d10_save_pct IS NOT NULL) AND (t.team_d10_sf_per_game IS NOT NULL) AND (t.opp_d10_sf_allowed_per_game IS NOT NULL) AND (t.pace_index IS NOT NULL))
  WITH NO DATA;


--
-- Name: export_training_goalie_saves_v2; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.export_training_goalie_saves_v2 AS
 SELECT r.game_date,
    r.player_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.saves AS y_saves,
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
    r.season_save_pct
   FROM nhl.training_features_goalie_saves_v2_ready r;


--
-- Name: training_features_nhl_sog_v2; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.training_features_nhl_sog_v2 (
    player_id integer,
    game_id bigint,
    team_id integer,
    opponent_id integer,
    is_home boolean,
    game_date date,
    shots_on_goal integer,
    d5_sog_per60 numeric,
    d10_sog_per60 numeric,
    d20_sog_per60 numeric,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    role_pp_share numeric,
    rest_days integer,
    b2b_flag boolean,
    attempts_d10_per60 numeric(6,3),
    pace_index numeric,
    opp_d10_sf_per60 numeric,
    team_d10_sa_per60 numeric,
    pace_matchup_index numeric
);


--
-- Name: training_features_nhl_sog_v2_ready; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_sog_v2_ready AS
 SELECT t.player_id,
    t.game_id,
    t.team_id,
    t.opponent_id,
    t.is_home,
    t.game_date,
    t.shots_on_goal,
    t.d10_sog_per60,
    t.attempts_d10_per60,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.pace_index,
    t.role_pp_share,
    t.rest_days,
    t.b2b_flag,
    t.opp_d10_sf_per60,
    t.team_d10_sa_per60,
    t.pace_matchup_index
   FROM nhl.training_features_nhl_sog_v2 t
  WHERE ((t.d10_sog_per60 IS NOT NULL) AND (t.team_d10_sf_per_game IS NOT NULL) AND (t.opp_d10_sf_allowed_per_game IS NOT NULL) AND (t.pace_index IS NOT NULL))
  WITH NO DATA;


--
-- Name: export_training_nhl_sog_v2; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.export_training_nhl_sog_v2 AS
 SELECT r.game_date,
    r.player_id,
    r.team_id,
    r.opponent_id,
    r.is_home,
    r.shots_on_goal AS y_shots_on_goal,
    r.d10_sog_per60,
    r.attempts_d10_per60,
    r.team_d10_sf_per_game,
    r.opp_d10_sf_allowed_per_game,
    r.pace_index,
    r.role_pp_share,
    r.rest_days,
    r.b2b_flag,
    r.opp_d10_sf_per60,
    r.team_d10_sa_per60,
    r.pace_matchup_index
   FROM nhl.training_features_nhl_sog_v2_ready r;


--
-- Name: game_context_derived; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.game_context_derived (
    player_id bigint NOT NULL,
    game_id bigint NOT NULL,
    team_id bigint NOT NULL,
    opponent_id bigint NOT NULL,
    is_home boolean NOT NULL,
    rest_days smallint,
    b2b_flag boolean,
    start_prob numeric(3,2)
);


--
-- Name: game_external_ids; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.game_external_ids (
    game_id bigint NOT NULL,
    provider text NOT NULL,
    provider_game_id text NOT NULL
);


--
-- Name: games; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.games (
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    start_time timestamp with time zone,
    home_team_id bigint NOT NULL,
    away_team_id bigint NOT NULL,
    status text DEFAULT 'scheduled'::text NOT NULL,
    venue text,
    season text,
    game_type text,
    start_time_utc timestamp with time zone,
    CONSTRAINT games_status_check CHECK ((status = ANY (ARRAY['scheduled'::text, 'live'::text, 'final'::text, 'postponed'::text, 'canceled'::text])))
);


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
    sh_sog integer
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
    CONSTRAINT model_metadata_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text])))
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
    CONSTRAINT model_versions_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text])))
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
    CONSTRAINT players_position_check CHECK (("position" = ANY (ARRAY['F'::text, 'D'::text, 'G'::text]))),
    CONSTRAINT players_shoots_catches_check CHECK ((shoots_catches = ANY (ARRAY['L'::text, 'R'::text])))
);


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
    feature_hash text NOT NULL,
    model_version text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
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
    CONSTRAINT skater_game_logs_raw_blocks_check CHECK ((blocks >= 0)),
    CONSTRAINT skater_game_logs_raw_hits_check CHECK ((hits >= 0)),
    CONSTRAINT skater_game_logs_raw_penalties_check CHECK ((penalties >= 0)),
    CONSTRAINT skater_game_logs_raw_pp_toi_minutes_check CHECK ((pp_toi_minutes >= (0)::numeric)),
    CONSTRAINT skater_game_logs_raw_shot_attempts_check CHECK ((shot_attempts >= 0)),
    CONSTRAINT skater_game_logs_raw_shots_on_goal_check CHECK ((shots_on_goal >= 0)),
    CONSTRAINT skater_game_logs_raw_toi_minutes_check CHECK ((toi_minutes >= (0)::numeric))
);


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
-- Name: sog_bad_snapshot_20251009; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.sog_bad_snapshot_20251009 (
    game_id bigint,
    player_id bigint,
    shots_on_goal smallint,
    ev_sog_before integer,
    pp_sog_before integer,
    sh_sog_before integer
);


--
-- Name: sog_feat_attempts_d10_per60; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.sog_feat_attempts_d10_per60 AS
 WITH base AS (
         SELECT s.player_id,
            s.game_id,
            s.game_date,
            COALESCE(s.shot_attempts, 0) AS shot_attempts,
            NULLIF(s.toi_minutes, NULL::numeric) AS toi_minutes
           FROM nhl.import_skater_logs_stage s
        ), w AS (
         SELECT b.player_id,
            b.game_id,
            sum(b.shot_attempts) OVER win_prev10 AS attempts_sum_prev10,
            sum(b.toi_minutes) OVER win_prev10 AS toi_sum_prev10
           FROM base b
          WINDOW win_prev10 AS (PARTITION BY b.player_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING)
        )
 SELECT w.player_id,
    w.game_id,
    (
        CASE
            WHEN ((w.toi_sum_prev10 IS NULL) OR (w.toi_sum_prev10 <= (0)::numeric)) THEN NULL::numeric
            ELSE (((w.attempts_sum_prev10)::numeric * 60.0) / w.toi_sum_prev10)
        END)::double precision AS attempts_d10_per60
   FROM w;


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
-- Name: teams; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.teams (
    team_id bigint NOT NULL,
    name text NOT NULL,
    abbr text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    city text,
    conference text,
    division text,
    active boolean DEFAULT true
);


--
-- Name: tf_sog_base; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.tf_sog_base (
    player_id integer,
    game_id bigint,
    team_id integer,
    opponent_id integer,
    is_home boolean,
    game_date date,
    sog integer,
    toi_min numeric,
    pp_toi_min numeric
);


--
-- Name: tf_sog_player_roll; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.tf_sog_player_roll (
    player_id integer,
    game_id bigint,
    team_id integer,
    opponent_id integer,
    is_home boolean,
    game_date date,
    sog integer,
    toi_min numeric,
    pp_toi_min numeric,
    sog_per60 numeric,
    d5_sog_per60 numeric,
    d10_sog_per60 numeric,
    d20_sog_per60 numeric,
    role_pp_share numeric,
    rest_days integer,
    b2b_flag boolean,
    attempts_d10_per60 numeric
);


--
-- Name: tf_team_game_sog; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.tf_team_game_sog (
    team_id integer,
    opponent_id integer,
    game_id bigint,
    game_date date,
    team_sog integer
);


--
-- Name: tf_team_roll10; Type: TABLE; Schema: nhl; Owner: -
--

CREATE TABLE nhl.tf_team_roll10 (
    team_id integer,
    opponent_id integer,
    game_id bigint,
    game_date date,
    team_sog integer,
    opp_sog integer,
    team_d10_sf_per_game numeric,
    opp_d10_sf_allowed_per_game numeric,
    pace_index numeric
);


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
           FROM ( SELECT skater_game_logs_raw.team_id,
                    skater_game_logs_raw.game_id,
                    (sum(skater_game_logs_raw.shots_on_goal))::numeric AS team_sf
                   FROM nhl.skater_game_logs_raw
                  GROUP BY skater_game_logs_raw.team_id, skater_game_logs_raw.game_id) t
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
            avg(t.team_sf) OVER (PARTITION BY t.team_id ORDER BY t.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS team_d10_sf_per_game
           FROM team_game_sf t
        ), opp_def_ctx AS (
         SELECT o.opponent_id AS team_id,
            o.game_date,
            avg(o.team_sf) OVER (PARTITION BY o.opponent_id ORDER BY o.game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS opp_d10_sf_allowed_per_game
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
     LEFT JOIN opp_def_ctx odef ON (((odef.team_id = r.team_id) AND (odef.game_date = r.game_date))))
     CROSS JOIN league l)
  WHERE (r.shots_on_goal IS NOT NULL)
  WITH NO DATA;


--
-- Name: training_features_nhl_sog_enr_filt; Type: MATERIALIZED VIEW; Schema: nhl; Owner: -
--

CREATE MATERIALIZED VIEW nhl.training_features_nhl_sog_enr_filt AS
 SELECT m.player_id,
    m.game_id,
    m.team_id,
    m.opponent_id,
    m.is_home,
    m.game_date,
    m.shots_on_goal,
    m.d5_sog_per60,
    m.d10_sog_per60,
    m.d20_sog_per60,
    m.team_d10_sf_per_game,
    m.opp_d10_sf_allowed_per_game,
    m.pace_matchup_index,
    m.role_pp_share,
    m.rest_days,
    m.b2b_flag
   FROM (nhl.training_features_nhl_sog_enriched m
     JOIN nhl.keep_games_filter k USING (game_id))
  WITH NO DATA;


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
    CONSTRAINT user_props_prop_type_check CHECK ((prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text])))
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
-- Name: v_prediction_scores; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_prediction_scores AS
 WITH actuals AS (
         SELECT g.game_id,
            g.game_date,
            g.status,
            sgr.player_id AS skater_id,
            sgr.shots_on_goal AS skater_sog,
            ggr.player_id AS goalie_id,
            ggr.saves AS goalie_saves
           FROM ((nhl.games g
             LEFT JOIN nhl.skater_game_logs_raw sgr ON ((sgr.game_id = g.game_id)))
             LEFT JOIN nhl.goalie_game_logs_raw ggr ON ((ggr.game_id = g.game_id)))
        )
 SELECT p.prediction_id,
    p.player_id,
    p.game_id,
    a.game_date,
    p.prop,
    p.line,
    p.p_over,
    (
        CASE
            WHEN (p.prop = 'shots_on_goal'::text) THEN a.skater_sog
            WHEN (p.prop = 'goalie_saves'::text) THEN a.goalie_saves
            ELSE NULL::smallint
        END)::numeric AS actual_value,
        CASE
            WHEN (p.prop = 'shots_on_goal'::text) THEN
            CASE
                WHEN ((a.skater_sog)::numeric >= p.line) THEN 1
                ELSE 0
            END
            WHEN (p.prop = 'goalie_saves'::text) THEN
            CASE
                WHEN ((a.goalie_saves)::numeric >= p.line) THEN 1
                ELSE 0
            END
            ELSE NULL::integer
        END AS over_hit,
    (((p.p_over - (
        CASE
            WHEN (p.prop = 'shots_on_goal'::text) THEN
            CASE
                WHEN ((a.skater_sog)::numeric >= p.line) THEN 1
                ELSE 0
            END
            WHEN (p.prop = 'goalie_saves'::text) THEN
            CASE
                WHEN ((a.goalie_saves)::numeric >= p.line) THEN 1
                ELSE 0
            END
            ELSE NULL::integer
        END)::double precision))::numeric ^ (2)::numeric) AS brier,
    (- (
        CASE
            WHEN (
            CASE
                WHEN (p.prop = 'shots_on_goal'::text) THEN
                CASE
                    WHEN ((a.skater_sog)::numeric >= p.line) THEN 1
                    ELSE 0
                END
                WHEN (p.prop = 'goalie_saves'::text) THEN
                CASE
                    WHEN ((a.goalie_saves)::numeric >= p.line) THEN 1
                    ELSE 0
                END
                ELSE NULL::integer
            END = 1) THEN ln(LEAST(GREATEST(p.p_over, (0.000001)::double precision), (((1)::numeric - 0.000001))::double precision))
            ELSE ln(LEAST(GREATEST(((1)::double precision - p.p_over), (0.000001)::double precision), (((1)::numeric - 0.000001))::double precision))
        END)::numeric) AS log_loss,
    p.created_at
   FROM (nhl.predictions p
     JOIN actuals a ON ((a.game_id = p.game_id)))
  WHERE (a.status = 'final'::text);


--
-- Name: v_predictions_saves_pretty; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_predictions_saves_pretty AS
 SELECT p.prediction_id,
    g.game_date,
    p.game_id,
    pl.player_id,
    pl.full_name AS player_name,
    t.team_id,
    t.abbr AS team_abbr,
    t.name AS team_name,
    p.prop,
    p.line,
    round((p.p_over)::numeric, 4) AS p_over,
    p.model_family,
    p.feature_hash,
    p.model_version,
    p.created_at
   FROM (((nhl.predictions p
     LEFT JOIN nhl.games g ON ((g.game_id = p.game_id)))
     LEFT JOIN nhl.players pl ON ((pl.player_id = p.player_id)))
     LEFT JOIN nhl.teams t ON ((t.team_id = pl.current_team_id)))
  WHERE (p.prop = 'goalie_saves'::text);


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
-- Name: v_predictions_sog_pretty; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_predictions_sog_pretty AS
 SELECT p.prediction_id,
    g.game_date,
    p.game_id,
    pl.player_id,
    pl.full_name AS player_name,
    t.team_id,
    t.abbr AS team_abbr,
    t.name AS team_name,
    p.prop,
    p.line,
    round((p.p_over)::numeric, 4) AS p_over,
    p.model_family,
    p.feature_hash,
    p.model_version,
    p.created_at
   FROM (((nhl.predictions p
     LEFT JOIN nhl.games g ON ((g.game_id = p.game_id)))
     LEFT JOIN nhl.players pl ON ((pl.player_id = p.player_id)))
     LEFT JOIN nhl.teams t ON ((t.team_id = pl.current_team_id)))
  WHERE (p.prop = 'shots_on_goal'::text);


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
-- Name: v_skater_base_casts; Type: VIEW; Schema: nhl; Owner: -
--

CREATE VIEW nhl.v_skater_base_casts AS
 SELECT s.player_id,
    s.game_id,
    s.team_id,
    s.opponent_id,
    s.is_home,
    g.game_date,
        CASE
            WHEN ((pg_typeof(s.toi_minutes))::text = ANY (ARRAY['text'::text, 'character varying'::text, 'character'::text])) THEN
            CASE
                WHEN (TRIM(BOTH FROM (s.toi_minutes)::text) ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN ((s.toi_minutes)::text)::numeric
                ELSE NULL::numeric
            END
            ELSE (s.toi_minutes)::numeric
        END AS toi_minutes,
        CASE
            WHEN ((pg_typeof(s.pp_toi_minutes))::text = ANY (ARRAY['text'::text, 'character varying'::text, 'character'::text])) THEN
            CASE
                WHEN (TRIM(BOTH FROM (s.pp_toi_minutes)::text) ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN ((s.pp_toi_minutes)::text)::numeric
                ELSE NULL::numeric
            END
            ELSE (s.pp_toi_minutes)::numeric
        END AS pp_toi_minutes,
        CASE
            WHEN ((pg_typeof(s.shot_attempts))::text = ANY (ARRAY['text'::text, 'character varying'::text, 'character'::text])) THEN
            CASE
                WHEN (TRIM(BOTH FROM (s.shot_attempts)::text) ~ '^[0-9]+$'::text) THEN ((s.shot_attempts)::text)::integer
                ELSE NULL::integer
            END
            ELSE (s.shot_attempts)::integer
        END AS shot_attempts
   FROM (nhl.skater_game_logs_raw s
     JOIN nhl.games g USING (game_id));


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
-- Name: game_context_derived game_context_derived_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_context_derived
    ADD CONSTRAINT game_context_derived_pkey PRIMARY KEY (player_id, game_id);


--
-- Name: game_external_ids game_external_ids_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_external_ids
    ADD CONSTRAINT game_external_ids_pkey PRIMARY KEY (game_id, provider);


--
-- Name: game_external_ids game_external_ids_provider_provider_game_id_key; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_external_ids
    ADD CONSTRAINT game_external_ids_provider_provider_game_id_key UNIQUE (provider, provider_game_id);


--
-- Name: games games_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);


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
-- Name: skater_game_logs_raw skater_game_logs_raw_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_pkey PRIMARY KEY (player_id, game_id);


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
-- Name: teams teams_abbr_key; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_abbr_key UNIQUE (abbr);


--
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- Name: training_features_goalie_saves_v2 training_features_goalie_saves_v2_pk; Type: CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.training_features_goalie_saves_v2
    ADD CONSTRAINT training_features_goalie_saves_v2_pk PRIMARY KEY (player_id, game_id);


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
-- Name: games_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX games_date_idx ON nhl.games USING btree (game_date);


--
-- Name: games_status_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX games_status_idx ON nhl.games USING btree (status);


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
-- Name: goalie_roll_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX goalie_roll_idx ON nhl.goalie_rolling_agg USING btree (game_id);


--
-- Name: idx_feat_saves_goalie_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_feat_saves_goalie_date ON nhl.training_features_nhl_saves_enriched USING btree (player_id, game_date);


--
-- Name: idx_feat_sog_player_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_feat_sog_player_date ON nhl.training_features_nhl_sog_enriched USING btree (player_id, game_date);


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
-- Name: idx_goalie_saves_v2_ready_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_goalie_saves_v2_ready_date ON nhl.training_features_goalie_saves_v2_ready USING btree (game_date);


--
-- Name: idx_goalie_saves_v2_ready_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_goalie_saves_v2_ready_player_game ON nhl.training_features_goalie_saves_v2_ready USING btree (player_id, game_id);


--
-- Name: idx_predictions_created; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_predictions_created ON nhl.predictions USING btree (created_at);


--
-- Name: idx_predictions_lookup; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_predictions_lookup ON nhl.predictions USING btree (prop, game_id, player_id, line);


--
-- Name: idx_saves_stage_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_saves_stage_date ON nhl.predictions_saves_stage USING btree (game_date);


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
-- Name: idx_sog_stage_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_stage_date ON nhl.predictions_sog_stage USING btree (game_date);


--
-- Name: idx_sog_v2_ready_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_v2_ready_date ON nhl.training_features_nhl_sog_v2_ready USING btree (game_date);


--
-- Name: idx_sog_v2_ready_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_sog_v2_ready_player_game ON nhl.training_features_nhl_sog_v2_ready USING btree (player_id, game_id);


--
-- Name: idx_tf_sog_player_roll_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_tf_sog_player_roll_player_game ON nhl.tf_sog_player_roll USING btree (player_id, game_id);


--
-- Name: idx_tf_team_roll10_game_team_opp; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_tf_team_roll10_game_team_opp ON nhl.tf_team_roll10 USING btree (game_id, team_id, opponent_id);


--
-- Name: idx_training_goalie_game_team_opp; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_training_goalie_game_team_opp ON nhl.training_features_goalie_saves_v2 USING btree (game_id, team_id, opponent_id);


--
-- Name: idx_training_sog_player_game; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX idx_training_sog_player_game ON nhl.training_features_nhl_sog_v2 USING btree (player_id, game_id);


--
-- Name: ix_games_date; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_games_date ON nhl.games USING btree (game_date, game_id);


--
-- Name: ix_games_teams; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX ix_games_teams ON nhl.games USING btree (home_team_id, away_team_id);


--
-- Name: model_versions_one_active_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX model_versions_one_active_idx ON nhl.model_versions USING btree (prop_type) WHERE (is_active = true);


--
-- Name: roster_status_latest_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX roster_status_latest_idx ON nhl.roster_status USING btree (game_id, player_id, asof_ts DESC);


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
-- Name: skater_roll_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX skater_roll_idx ON nhl.skater_rolling_agg USING btree (game_id);


--
-- Name: sklr_offenders_by_game_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX sklr_offenders_by_game_idx ON nhl.skater_game_logs_raw USING btree (game_id) WHERE ((shots_on_goal > 0) AND ((ev_sog IS NULL) OR (pp_sog IS NULL) OR (sh_sog IS NULL) OR (((COALESCE(ev_sog, 0) + COALESCE(pp_sog, 0)) + COALESCE(sh_sog, 0)) <> shots_on_goal)));


--
-- Name: starters_goalies_latest_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX starters_goalies_latest_idx ON nhl.starters_goalies USING btree (game_id, team_id, asof_ts DESC);


--
-- Name: tf_sog_base_game_id_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_sog_base_game_id_idx ON nhl.tf_sog_base USING btree (game_id);


--
-- Name: tf_sog_base_player_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_sog_base_player_id_game_date_idx ON nhl.tf_sog_base USING btree (player_id, game_date);


--
-- Name: tf_sog_base_team_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_sog_base_team_id_game_date_idx ON nhl.tf_sog_base USING btree (team_id, game_date);


--
-- Name: tf_sog_player_roll_player_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_sog_player_roll_player_id_game_date_idx ON nhl.tf_sog_player_roll USING btree (player_id, game_date);


--
-- Name: tf_sog_player_roll_team_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_sog_player_roll_team_id_game_date_idx ON nhl.tf_sog_player_roll USING btree (team_id, game_date);


--
-- Name: tf_team_game_sog_opponent_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_team_game_sog_opponent_id_game_date_idx ON nhl.tf_team_game_sog USING btree (opponent_id, game_date);


--
-- Name: tf_team_game_sog_team_id_game_date_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_team_game_sog_team_id_game_date_idx ON nhl.tf_team_game_sog USING btree (team_id, game_date);


--
-- Name: tf_team_roll10_team_id_game_id_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX tf_team_roll10_team_id_game_id_idx ON nhl.tf_team_roll10 USING btree (team_id, game_id);


--
-- Name: user_props_lookup_idx; Type: INDEX; Schema: nhl; Owner: -
--

CREATE INDEX user_props_lookup_idx ON nhl.user_props USING btree (game_id, player_id, prop_type);


--
-- Name: ux_games_triplet; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_games_triplet ON nhl.games USING btree (game_date, home_team_id, away_team_id);


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
-- Name: ux_sog_enr; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_sog_enr ON nhl.training_features_nhl_sog_enriched USING btree (player_id, game_id);


--
-- Name: ux_sog_enr_filt; Type: INDEX; Schema: nhl; Owner: -
--

CREATE UNIQUE INDEX ux_sog_enr_filt ON nhl.training_features_nhl_sog_enr_filt USING btree (player_id, game_id);


--
-- Name: game_context_derived game_context_derived_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_context_derived
    ADD CONSTRAINT game_context_derived_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


--
-- Name: game_context_derived game_context_derived_opponent_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_context_derived
    ADD CONSTRAINT game_context_derived_opponent_id_fkey FOREIGN KEY (opponent_id) REFERENCES nhl.teams(team_id);


--
-- Name: game_context_derived game_context_derived_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_context_derived
    ADD CONSTRAINT game_context_derived_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: game_context_derived game_context_derived_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_context_derived
    ADD CONSTRAINT game_context_derived_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: game_external_ids game_external_ids_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.game_external_ids
    ADD CONSTRAINT game_external_ids_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id) ON DELETE CASCADE;


--
-- Name: games games_away_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_away_team_id_fkey FOREIGN KEY (away_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: games games_home_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.games
    ADD CONSTRAINT games_home_team_id_fkey FOREIGN KEY (home_team_id) REFERENCES nhl.teams(team_id);


--
-- Name: goalie_game_logs_raw goalie_game_logs_raw_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_game_logs_raw
    ADD CONSTRAINT goalie_game_logs_raw_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: goalie_rolling_agg goalie_rolling_agg_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.goalie_rolling_agg
    ADD CONSTRAINT goalie_rolling_agg_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: roster_status roster_status_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.roster_status
    ADD CONSTRAINT roster_status_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: skater_game_logs_raw skater_game_logs_raw_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_game_logs_raw
    ADD CONSTRAINT skater_game_logs_raw_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: skater_rolling_agg skater_rolling_agg_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


--
-- Name: skater_rolling_agg skater_rolling_agg_player_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.skater_rolling_agg
    ADD CONSTRAINT skater_rolling_agg_player_id_fkey FOREIGN KEY (player_id) REFERENCES nhl.players(player_id);


--
-- Name: starters_goalies starters_goalies_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.starters_goalies
    ADD CONSTRAINT starters_goalies_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: team_context_rolling team_context_rolling_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_context_rolling
    ADD CONSTRAINT team_context_rolling_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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
-- Name: team_game_rates_raw team_game_rates_raw_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_game_rates_raw
    ADD CONSTRAINT team_game_rates_raw_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


--
-- Name: team_game_rates_raw team_game_rates_raw_team_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.team_game_rates_raw
    ADD CONSTRAINT team_game_rates_raw_team_id_fkey FOREIGN KEY (team_id) REFERENCES nhl.teams(team_id);


--
-- Name: user_props user_props_game_id_fkey; Type: FK CONSTRAINT; Schema: nhl; Owner: -
--

ALTER TABLE ONLY nhl.user_props
    ADD CONSTRAINT user_props_game_id_fkey FOREIGN KEY (game_id) REFERENCES nhl.games(game_id);


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

\unrestrict T6vcX5DtuLcgcTFeBQ1MhegAkPq7CVdPFKBXx8fCGUueQKUkKpH2nJzBCfKkML5

