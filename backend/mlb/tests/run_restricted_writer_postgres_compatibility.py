"""Rollback-only PostgreSQL compatibility probe for the MLB stat writers.

The probe accepts only the independently identified NHL protection preview.
All test objects, grants, and data live in one transaction that is always
rolled back. It deliberately records when PostgreSQL's UPDATE ACL requirements
conflict with the application-level guarded UPSERTs.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import psycopg

from backend.mlb.scripts import insert_mlb_stat_derived as writer
from backend.mlb.scripts.backfill_mlb_player_pa import PaRow, _update_player_stats_pa
from backend.mlb.scripts import refresh_mlb_players_rosters as roster_writer
from backend.mlb.scripts import refresh_mlb_bvp_pvb as bvp_writer


EXPECTED_PROJECT_REF = "vbnzmvmhuwvpkijxeuyq"
WRITER_ROLE = "proppadia_mlb_ingest"
CORRECTION_ROLE = "proppadia_mlb_fixture_correction_20261016"
RESEARCH_ROLE = "proppadia_mlb_research_ro"
RESEARCH_LOGIN = "proppadia_mlb_research_login"
PLAYER_STATS_FIELDS = writer.PLAYER_STATS_SOURCE_FIELDS
PROTECTED_RESULT_FIELDS = ("prop_value", "line", "over_under", "outcome", "status", "was_correct")


def _record(results: list[dict[str, Any]], name: str, status: str, **details: Any) -> None:
    results.append({"assertion": name, "status": status, **details})


def _expect_denied(conn: psycopg.Connection, name: str, sql: str, results: list[dict[str, Any]]) -> None:
    conn.execute("SAVEPOINT expected_denial")
    try:
        conn.execute(sql)
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT expected_denial")
        conn.execute("RELEASE SAVEPOINT expected_denial")
        status = "PASS" if exc.sqlstate == "42501" else "FAIL"
        _record(results, name, status, sqlstate=exc.sqlstate, exception=type(exc).__name__)
        return
    conn.execute("ROLLBACK TO SAVEPOINT expected_denial")
    conn.execute("RELEASE SAVEPOINT expected_denial")
    _record(results, name, "FAIL", detail="operation was permitted")


def _source_row(player_id: int, game_id: int, *, hits: int | None = 2) -> dict[str, Any]:
    row = {
        "player_id": player_id,
        "game_id": game_id,
        "game_date": "2026-09-23",
        "team": "NYY",
        "opponent": "BOS",
        "is_home": True,
        "position": "LF",
        "plate_appearances": 4,
        "at_bats": 3,
        "hits": hits,
        "total_bases": hits,
        "rbis": 0,
        "runs_scored": 0,
        "strikeouts_batting": 1,
        "walks": 0,
        "singles": hits,
        "doubles": 0,
        "triples": 0,
        "home_runs": 0,
        "stolen_bases": 0,
        "strikeouts_pitching": None,
        "walks_allowed": None,
        "hits_allowed": None,
        "outs_recorded": None,
        "earned_runs": None,
        # Production stores starter status as smallint 0/1, not boolean.
        "is_starter": 0,
    }
    return row


def _training_row(game_id: int, *, outcome: str = "win", game_date: str = "2026-09-23") -> dict[str, Any]:
    now = datetime(2026, 9, 23, 20).isoformat()
    return {
        "id": f"00000000-0000-0000-0000-{game_id:012d}",
        "game_id": game_id,
        "player_id": "453286",
        "player_name": "fixture-player",
        "team": "1",
        "opponent": "2",
        "team_id": 1,
        "opponent_team_id": 2,
        "opponent_encoded": "2",
        "is_home": True,
        "prop_type": "hits",
        "prop_value": 1.0,
        "line": 0.5,
        "over_under": "over",
        "outcome": outcome,
        "status": "resolved",
        "created_at": now,
        "updated_at": now,
        "prop_source": "fixture",
        "was_correct": outcome == "win",
        "game_date": game_date,
        "game_time": now,
        "game_day_of_week": "Wednesday",
        "time_of_day_bucket": "evening",
        "streak_type": None,
        "streak_count": None,
    }


def _create_fixture(conn: psycopg.Connection) -> None:
    conn.execute(f"CREATE ROLE {CORRECTION_ROLE} NOLOGIN NOINHERIT NOCREATEROLE NOCREATEDB")
    conn.execute(f"CREATE ROLE {RESEARCH_ROLE} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
    conn.execute(f"CREATE ROLE {RESEARCH_LOGIN} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
    conn.execute(f"GRANT {RESEARCH_ROLE} TO {RESEARCH_LOGIN}")
    conn.execute(f"GRANT {RESEARCH_LOGIN} TO postgres")
    conn.execute(f"GRANT {WRITER_ROLE} TO postgres")
    conn.execute("CREATE SCHEMA mlb")
    conn.execute(
        """
        CREATE TABLE mlb.player_stats (
            player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date NOT NULL,
            team text, opponent text, is_home boolean, position text,
            plate_appearances integer, at_bats integer, hits integer, total_bases integer,
            rbis integer, runs_scored integer, strikeouts_batting integer, walks integer,
            hit_by_pitch integer, sacrifice_flies integer, sacrifice_hits integer,
            catcher_interference integer, pa_source text, pa_backfilled_at timestamptz,
            singles integer, doubles integer, triples integer, home_runs integer,
            stolen_bases integer, strikeouts_pitching integer, walks_allowed integer,
            hits_allowed integer, outs_recorded integer, earned_runs integer, is_starter smallint,
            PRIMARY KEY(player_id, game_id)
        )
        """
    )
    metric_columns = ", ".join(
        f"{prefix}_{metric} numeric" for prefix in ("d7", "d15", "d30")
        for metric in writer.ROLLING_METRICS
    )
    pa_aggregate_columns = ", ".join(
        f"{prefix}_plate_appearances numeric" for prefix in ("d7", "d15", "d30")
    )
    conn.execute(
        f"""CREATE TABLE mlb.player_derived_stats (
            id uuid DEFAULT gen_random_uuid() NOT NULL, player_id bigint NOT NULL,
            game_id bigint NOT NULL, game_date date NOT NULL, team varchar(10),
            is_home boolean, created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now(), {metric_columns},
            {pa_aggregate_columns}, PRIMARY KEY(id), UNIQUE(player_id, game_id), UNIQUE(player_id, game_date)
        )"""
    )
    conn.execute(
        """
        CREATE TABLE mlb.model_training_props (
            id uuid DEFAULT gen_random_uuid() NOT NULL, game_id bigint NOT NULL, player_id bigint NOT NULL,
            player_name text, team text, position text, opponent text, home_away text,
            team_id bigint NOT NULL, opponent_team_id integer, starting_pitcher_id bigint,
            opponent_player_id bigint, opposing_batter_id bigint, opponent_encoded text,
            is_home boolean, is_pitcher boolean, prop_type text,
            prop_value double precision, line numeric, over_under text, outcome text, status text,
            created_at timestamptz DEFAULT now() NOT NULL, updated_at timestamp without time zone DEFAULT now(), prop_source text DEFAULT 'user-added' NOT NULL,
            was_correct boolean, game_date date, game_time timestamp without time zone,
            predicted_outcome text, confidence_score numeric, prediction_timestamp timestamptz,
            game_day_of_week text, time_of_day_bucket text, streak_type text,
            streak_count integer, rolling_result_avg_7 double precision,
            result double precision, corrected_result numeric, correction_applied_at timestamptz,
            outcome_corrected_at timestamptz, line_diff numeric,
            opponent_avg_result_vs_player numeric, opponent_avg_win_rate double precision,
            hit_streak integer, win_streak integer, position_backfill_status text,
            game_id_txt text GENERATED ALWAYS AS (game_id::text) STORED,
            CONSTRAINT model_training_props_game_id_pos CHECK(game_id > 0),
            CONSTRAINT model_training_props_player_id_pos CHECK(player_id > 0),
            PRIMARY KEY(id), UNIQUE(player_id, game_id, prop_type, prop_source)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.player_ids (
            id uuid DEFAULT gen_random_uuid() NOT NULL, player_name text NOT NULL,
            team text, player_id bigint NOT NULL, created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now(), team_id bigint,
            is_placeholder boolean DEFAULT false NOT NULL, PRIMARY KEY(id), UNIQUE(player_id),
            CONSTRAINT player_ids_player_name_not_blank CHECK(player_name IS NOT NULL AND btrim(player_name) <> '')
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.game_info (
            game_id bigint PRIMARY KEY, game_time timestamp without time zone, game_date date NOT NULL,
            home_team_id bigint, away_team_id bigint, home_team_abbr text, away_team_abbr text,
            starting_pitcher_id_home bigint, starting_pitcher_id_away bigint
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.prop_features_precomputed (
            prop_type text NOT NULL, player_id bigint NOT NULL, game_id bigint NOT NULL,
            game_date date NOT NULL, features jsonb NOT NULL, feature_set_tag text DEFAULT 'v1' NOT NULL,
            model_tag text, lineup_slot integer, is_probable_sp boolean,
            computed_at timestamptz DEFAULT now() NOT NULL,
            PRIMARY KEY(prop_type,player_id,game_id,feature_set_tag)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.player_streak_profiles (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(), player_id bigint NOT NULL,
            player_name text, team text, prop_type text, streak_type text,
            last_updated timestamptz, is_active boolean, streak_broken timestamp,
            last_game_date date, last_outcome text, recent_outcomes text[],
            updated_at timestamptz, streak_count integer, hit_streak integer,
            win_streak integer, rolling_result_avg_7 double precision,
            source text, prop_source text
        )
        """
    )
    for relation in (
        "player_stats", "player_derived_stats", "model_training_props",
        "player_ids", "player_streak_profiles",
    ):
        conn.execute(f"ALTER TABLE mlb.{relation} ENABLE ROW LEVEL SECURITY")
    conn.execute(f"GRANT USAGE ON SCHEMA mlb TO {WRITER_ROLE}, {RESEARCH_ROLE}")


def _run_bvp_research_read_tests(conn: psycopg.Connection, results: list[dict[str, Any]]) -> None:
    conn.execute("RESET ROLE")
    conn.execute(
        """INSERT INTO mlb.prop_features_precomputed
               (prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag)
             VALUES ('hits',8001,900801,'2026-09-23',
                     '{\"bvp_sample\":4,\"bvp_avg\":0.5}'::jsonb,'v1','fixture-v1')"""
    )
    conn.execute(f"SET ROLE {RESEARCH_LOGIN}")
    _expect_denied(
        conn, "bvp_research_read_denied_before_grant",
        "SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag,computed_at FROM mlb.prop_features_precomputed LIMIT 1",
        results,
    )
    conn.execute("RESET ROLE")
    migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261017_mlb_bvp_research_read.sql"
    migration_sql = migration_path.read_text()
    conn.execute(migration_sql, prepare=False)

    conn.execute(f"SET ROLE {RESEARCH_LOGIN}")
    acl_rows = conn.execute(
        """SELECT a.attname, has_column_privilege(current_user,a.attrelid,a.attname,'SELECT')
             FROM pg_attribute a
            WHERE a.attrelid='mlb.prop_features_precomputed'::regclass
              AND a.attnum>0 AND NOT a.attisdropped
            ORDER BY a.attname"""
    ).fetchall()
    granted = {name for name, allowed in acl_rows if allowed}
    expected = {"prop_type", "player_id", "game_id", "game_date", "features", "feature_set_tag", "model_tag", "computed_at"}
    no_write = conn.execute(
        """SELECT NOT has_table_privilege(current_user,'mlb.prop_features_precomputed','INSERT')
                    AND NOT has_any_column_privilege(current_user,'mlb.prop_features_precomputed','INSERT'),
                  NOT has_table_privilege(current_user,'mlb.prop_features_precomputed','UPDATE')
                    AND NOT has_any_column_privilege(current_user,'mlb.prop_features_precomputed','UPDATE'),
                  NOT has_table_privilege(current_user,'mlb.prop_features_precomputed','DELETE'),
                  NOT has_table_privilege(current_user,'mlb.prop_features_precomputed','TRUNCATE')"""
    ).fetchone()
    no_table_select = not conn.execute(
        "SELECT has_table_privilege(current_user,'mlb.prop_features_precomputed','SELECT')"
    ).fetchone()[0]
    _record(results, "bvp_research_read_exact_columns_no_writes", "PASS" if granted == expected and no_table_select and no_write == (True, True, True, True) else "FAIL", granted=sorted(granted), table_select_denied=no_table_select, write_denials=list(no_write or ()))

    row = conn.execute(
        """SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag,computed_at
             FROM mlb.prop_features_precomputed
            WHERE (prop_type,player_id,game_id,feature_set_tag)=(%s,%s,%s,%s)""",
        ("hits", 8001, 900801, "v1"),
    ).fetchone()
    read_ok = bool(row and row[:7] == ("hits", 8001, 900801, date(2026, 9, 23), {"bvp_sample": 4, "bvp_avg": 0.5}, "v1", "fixture-v1"))
    _record(results, "bvp_research_verifier_read_succeeds", "PASS" if read_ok else "FAIL", row_count=int(row is not None))
    _expect_denied(conn, "bvp_research_unrelated_column_read_denied", "SELECT lineup_slot FROM mlb.prop_features_precomputed LIMIT 1", results)
    _expect_denied(conn, "bvp_research_protected_source_read_denied", "SELECT hits FROM mlb.player_stats LIMIT 1", results)
    _expect_denied(conn, "bvp_research_insert_denied", "INSERT INTO mlb.prop_features_precomputed (prop_type,player_id,game_id,game_date,features) VALUES ('hits',8002,900802,'2026-09-23','{}')", results)
    _expect_denied(conn, "bvp_research_update_denied", "UPDATE mlb.prop_features_precomputed SET model_tag='changed' WHERE player_id=8001", results)
    _expect_denied(conn, "bvp_research_delete_denied", "DELETE FROM mlb.prop_features_precomputed WHERE player_id=8001", results)
    _expect_denied(conn, "bvp_research_truncate_denied", "TRUNCATE mlb.prop_features_precomputed", results)
    conn.execute("RESET ROLE")


def _run_streak_read_tests(conn: psycopg.Connection, results: list[dict[str, Any]]) -> None:
    # _run_role_tests intentionally leaves the simulated writer role active;
    # seed fixture-only rows as the preview administrator before switching back.
    conn.execute("RESET ROLE")
    conn.execute(
        """
        INSERT INTO mlb.player_streak_profiles
            (player_id,player_name,team,prop_type,streak_type,last_game_date,streak_count,source,prop_source)
        VALUES
            (7001,'fixture-player','SEA','hits','hot','2026-09-20',5,'fixture','mlb_api'),
            (7001,'fixture-player','SEA','hits','cold','2026-09-19',8,'fixture','manual'),
            (7001,'fixture-player','SEA','hits','hot','2026-09-25',9,'fixture','mlb_api'),
            (7002,'fixture-player','SEA','hits','hot',NULL,10,'fixture','mlb_api')
        """
    )
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    required_columns = (
        "player_id", "prop_type", "prop_source", "last_game_date", "streak_type", "streak_count"
    )
    acl_rows = conn.execute(
        """SELECT a.attname,has_column_privilege(current_user,a.attrelid,a.attname,'SELECT')
             FROM pg_attribute a
            WHERE a.attrelid='mlb.player_streak_profiles'::regclass
              AND a.attnum>0 AND NOT a.attisdropped
            ORDER BY a.attnum"""
    ).fetchall()
    granted = {name for name, allowed in acl_rows if allowed}
    exact_acl = granted == set(required_columns)
    rls_policy = conn.execute(
        """SELECT c.relrowsecurity, count(p.*)=1,
                  bool_and(p.cmd='SELECT'
                    AND p.roles=ARRAY['proppadia_mlb_ingest']::name[]
                    AND p.qual='((prop_source = ''mlb_api''::text) AND (last_game_date IS NOT NULL))')
             FROM pg_class c
             LEFT JOIN pg_policies p ON p.schemaname='mlb'
                AND p.tablename='player_streak_profiles'
                AND p.policyname='proppadia_mlb_ingest_streak_select'
            WHERE c.oid='mlb.player_streak_profiles'::regclass
            GROUP BY c.relrowsecurity"""
    ).fetchone()
    write_denials = conn.execute(
        """SELECT NOT has_any_column_privilege(current_user,'mlb.player_streak_profiles','INSERT'),
                  NOT has_any_column_privilege(current_user,'mlb.player_streak_profiles','UPDATE'),
                  NOT has_table_privilege(current_user,'mlb.player_streak_profiles','DELETE'),
                  NOT has_table_privilege(current_user,'mlb.player_streak_profiles','TRUNCATE')"""
    ).fetchone()
    _record(
        results, "streak_read_gate_exact_columns_and_source_policy",
        "PASS" if exact_acl and rls_policy == (True, True, True) and write_denials == (True, True, True, True) else "FAIL",
        selected_columns=sorted(granted), policy=list(rls_policy or ()), write_denials=list(write_denials or ()),
    )
    visible_sources = conn.execute(
        "SELECT prop_source FROM mlb.player_streak_profiles ORDER BY last_game_date"
    ).fetchall()
    _record(
        results, "streak_rls_limits_rows_to_mlb_api_source",
        "PASS" if visible_sources == [("mlb_api",), ("mlb_api",)] else "FAIL",
        visible_sources=[row[0] for row in visible_sources],
    )
    prior = writer._get_streak(conn, 7001, "hits", "2026-09-22")
    too_early = writer._get_streak(conn, 7001, "hits", "2026-09-20")
    _record(
        results, "stat_derived_streak_query_source_and_strict_prior_behavior",
        "PASS" if prior == ("hot", 5) and too_early == (None, None) else "FAIL",
        eligible=list(prior), same_day_excluded=list(too_early),
    )
    _expect_denied(
        conn, "streak_ungranted_column_select_denied",
        "SELECT player_name FROM mlb.player_streak_profiles LIMIT 1", results,
    )
    _expect_denied(
        conn, "streak_direct_insert_denied",
        "INSERT INTO mlb.player_streak_profiles (player_id,prop_source) VALUES (7002,'mlb_api')", results,
    )
    _expect_denied(
        conn, "streak_direct_update_denied",
        "UPDATE mlb.player_streak_profiles SET streak_count=99 WHERE player_id=7001", results,
    )
    _expect_denied(
        conn, "streak_direct_delete_denied",
        "DELETE FROM mlb.player_streak_profiles WHERE player_id=7001", results,
    )
    _expect_denied(conn, "streak_direct_truncate_denied", "TRUNCATE mlb.player_streak_profiles", results)
    _expect_denied(
        conn, "streak_policy_escalation_denied",
        "CREATE POLICY fixture_escalation ON mlb.player_streak_profiles FOR SELECT USING (true)", results,
    )
    conn.execute("RESET ROLE")


def _run_role_tests(conn: psycopg.Connection, results: list[dict[str, Any]]) -> None:
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    current_user = conn.execute("SELECT current_user").fetchone()[0]
    _record(results, "writer_role_switch_identity", "PASS" if current_user == WRITER_ROLE else "FAIL", role=current_user)
    writer_flags = conn.execute(
        "SELECT rolcanlogin, rolsuper, rolcreaterole, rolinherit FROM pg_roles WHERE rolname=%s",
        (WRITER_ROLE,),
    ).fetchone()
    correction_flags = conn.execute(
        "SELECT rolcanlogin, rolsuper, rolcreaterole, rolinherit FROM pg_roles WHERE rolname=%s",
        (CORRECTION_ROLE,),
    ).fetchone()
    _record(
        results,
        "test_role_restriction_flags",
        "PASS" if writer_flags == (False, False, False, False) and correction_flags == (False, False, False, False) else "FAIL",
        writer=list(writer_flags),
        correction=list(correction_flags),
    )
    _expect_denied(conn, "direct_source_insert_denied", "INSERT INTO mlb.player_stats (player_id, game_id) VALUES (103,900103)", results)
    _expect_denied(conn, "direct_training_insert_denied", "INSERT INTO mlb.model_training_props (id,game_id,player_id,prop_type,prop_source) VALUES ('00000000-0000-0000-0000-000000900104',900104,453286,'hits','fixture')", results)
    _expect_denied(conn, "source_populated_update_denied", "UPDATE mlb.player_stats SET hits = 99 WHERE player_id = 101", results)
    _expect_denied(conn, "source_at_bats_direct_update_denied", "UPDATE mlb.player_stats SET at_bats = 99 WHERE player_id = 101", results)
    _expect_denied(conn, "pa_component_direct_update_denied", "UPDATE mlb.player_stats SET plate_appearances = 99 WHERE player_id = 101", results)
    _expect_denied(conn, "training_outcome_update_denied", "UPDATE mlb.model_training_props SET outcome = 'loss' WHERE player_id = 453286", results)
    _expect_denied(conn, "prediction_direct_update_denied", "UPDATE mlb.model_training_props SET predicted_outcome='under' WHERE id='00000000-0000-0000-0000-000000900501'", results)
    _expect_denied(conn, "prediction_result_direct_update_denied", "UPDATE mlb.model_training_props SET was_correct=false WHERE id='00000000-0000-0000-0000-000000900501'", results)
    _expect_denied(conn, "delete_denied", "DELETE FROM mlb.player_stats WHERE player_id = 101", results)
    _expect_denied(conn, "truncate_denied", "TRUNCATE mlb.player_stats", results)
    _expect_denied(conn, "derived_delete_denied", "DELETE FROM mlb.player_derived_stats WHERE player_id=1", results)
    _expect_denied(conn, "derived_truncate_denied", "TRUNCATE mlb.player_derived_stats", results)
    for relation, key in (("player_ids", "player_id=1"), ("game_info", "game_id=1"), ("prop_features_precomputed", "player_id=1")):
        _expect_denied(conn, f"{relation}_direct_insert_denied", f"INSERT INTO mlb.{relation} DEFAULT VALUES", results)
        _expect_denied(conn, f"{relation}_direct_update_denied", f"UPDATE mlb.{relation} SET {key.split('=')[0]}={key.split('=')[1]} WHERE {key}", results)
        _expect_denied(conn, f"{relation}_delete_denied", f"DELETE FROM mlb.{relation} WHERE {key}", results)
        _expect_denied(conn, f"{relation}_truncate_denied", f"TRUNCATE mlb.{relation}", results)
    _expect_denied(conn, "player_ids_team_replacement_direct_denied", "UPDATE mlb.player_ids SET team_id=999 WHERE player_id=1", results)
    _expect_denied(conn, "game_info_team_replacement_direct_denied", "UPDATE mlb.game_info SET home_team_id=999 WHERE game_id=1", results)
    _expect_denied(conn, "game_info_date_replacement_direct_denied", "UPDATE mlb.game_info SET game_date='1900-01-01' WHERE game_id=1", results)
    _expect_denied(conn, "bvp_features_direct_replacement_denied", "UPDATE mlb.prop_features_precomputed SET features='{}'::jsonb WHERE player_id=1", results)
    _expect_denied(conn, "correction_role_switch_denied", f"SET ROLE {CORRECTION_ROLE}", results)
    _expect_denied(conn, "role_grant_escalation_denied", f"GRANT {CORRECTION_ROLE} TO {WRITER_ROLE}", results)
    _expect_denied(conn, "function_replace_denied", "CREATE OR REPLACE FUNCTION mlb.safe_upsert_player_stats(jsonb) RETURNS integer LANGUAGE sql AS 'SELECT 0'", results)
    _expect_denied(conn, "function_drop_denied", "DROP FUNCTION mlb.safe_upsert_player_stats(jsonb)", results)

    predicted = "00000000-0000-0000-0000-000000900501"
    pred_ts = "2026-09-23T21:00:00+00:00"
    first_prediction = conn.execute(
        "SELECT mlb.safe_fill_prediction_backfill(%s::uuid,'over',0.72,%s::timestamptz)",
        (predicted, pred_ts),
    ).fetchone()[0]
    replay_prediction = conn.execute(
        "SELECT mlb.safe_fill_prediction_backfill(%s::uuid,'over',0.72,%s::timestamptz)",
        (predicted, pred_ts),
    ).fetchone()[0]
    conflict_prediction = False
    conn.execute("SAVEPOINT prediction_conflict")
    try:
        conn.execute(
            "SELECT mlb.safe_fill_prediction_backfill(%s::uuid,'under',0.28,%s::timestamptz)",
            (predicted, pred_ts),
        )
    except psycopg.Error as exc:
        conflict_prediction = exc.sqlstate == "23514"
        conn.execute("ROLLBACK TO SAVEPOINT prediction_conflict")
        conn.execute("RELEASE SAVEPOINT prediction_conflict")
    else:
        conn.execute("ROLLBACK TO SAVEPOINT prediction_conflict")
        conn.execute("RELEASE SAVEPOINT prediction_conflict")
    pred_state = conn.execute(
        "SELECT predicted_outcome,confidence_score,was_correct FROM mlb.model_training_props WHERE id=%s::uuid",
        (predicted,),
    ).fetchone()
    conflict_existing_row = False
    conn.execute("SAVEPOINT prediction_existing_conflict")
    try:
        conn.execute(
            "SELECT mlb.safe_fill_prediction_backfill(%s::uuid,'over',0.2,%s::timestamptz)",
            ("00000000-0000-0000-0000-000000900502", pred_ts),
        )
    except psycopg.Error as exc:
        conflict_existing_row = exc.sqlstate == "23514"
        conn.execute("ROLLBACK TO SAVEPOINT prediction_existing_conflict")
        conn.execute("RELEASE SAVEPOINT prediction_existing_conflict")
    else:
        conn.execute("ROLLBACK TO SAVEPOINT prediction_existing_conflict")
        conn.execute("RELEASE SAVEPOINT prediction_existing_conflict")
    _record(
        results,"prediction_initial_fill_replay_conflict_and_result_preservation",
        "PASS" if first_prediction == 1 and replay_prediction == 0 and conflict_prediction and conflict_existing_row and pred_state is not None and pred_state[0] == "over" and str(pred_state[1]) == "0.72" and pred_state[2] is True
        else "FAIL", writes=[first_prediction,replay_prediction], conflict=conflict_prediction,
        retained=[pred_state[0],str(pred_state[1]),pred_state[2]] if pred_state else None,
    )

    inserted = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    replay = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    _record(results, "source_insert_and_same_value_replay", "PASS" if inserted == 1 and replay == 0 else "FAIL", inserted=int(inserted), replay=int(replay))
    _expect_denied(conn, "source_populated_to_null_denied", "UPDATE mlb.player_stats SET hits = NULL WHERE player_id=201 AND game_id=900201", results)
    conflict = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=3))
    source_value = conn.execute("SELECT hits FROM mlb.player_stats WHERE player_id=201 AND game_id=900201").fetchone()[0]
    _record(results, "source_conflict_preserved", "PASS" if conflict.conflicts and source_value == 2 else "FAIL", conflicts=list(conflict.conflicts[0]["conflicting_fields"]) if conflict.conflicts else [], retained=source_value)

    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats(player_id,game_id,game_date,is_starter) VALUES (207,900207,'2026-09-23',NULL)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    starter_row = _source_row(207,900207,hits=1)
    starter_row["is_starter"] = 1
    starter_fill = writer._upsert_player_stats_row(conn,starter_row)
    starter_value = conn.execute("SELECT is_starter FROM mlb.player_stats WHERE player_id=207 AND game_id=900207").fetchone()[0]
    replay_starter = writer._upsert_player_stats_row(conn,starter_row)
    _record(results,"smallint_starter_null_fill_replay", "PASS" if int(starter_fill) == 1 and int(replay_starter) == 0 and starter_value == 1 else "FAIL", writes=[int(starter_fill),int(replay_starter)], starter=starter_value)
    zero_fill_row = _source_row(209,900209,hits=1)
    zero_fill_row["is_starter"] = 0
    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats(player_id,game_id,game_date,is_starter) VALUES (209,900209,'2026-09-23',NULL)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    zero_fill = writer._upsert_player_stats_row(conn,zero_fill_row)
    zero_value = conn.execute("SELECT is_starter FROM mlb.player_stats WHERE player_id=209 AND game_id=900209").fetchone()[0]
    zero_replay = writer._upsert_player_stats_row(conn,zero_fill_row)
    _record(results,"smallint_starter_null_to_zero_fill_replay", "PASS" if int(zero_fill) == 1 and int(zero_replay) == 0 and zero_value == 0 else "FAIL", writes=[int(zero_fill),int(zero_replay)], starter=zero_value)
    zero_conflict_row = _source_row(101,900101,hits=2)
    zero_conflict_row["is_starter"] = 1
    zero_conflict = writer._upsert_player_stats_row(conn,zero_conflict_row)
    retained_zero = conn.execute("SELECT is_starter FROM mlb.player_stats WHERE player_id=101 AND game_id=900101").fetchone()[0]
    _record(results,"smallint_starter_populated_zero_conflict_preserved", "PASS" if zero_conflict.conflicts and "is_starter" in zero_conflict.conflicts[0]["conflicting_fields"] and retained_zero == 0 else "FAIL", conflicts=list(zero_conflict.conflicts[0]["conflicting_fields"]) if zero_conflict.conflicts else [], retained=retained_zero)
    invalid_starter = _source_row(208,900208,hits=1)
    invalid_starter["is_starter"] = 2
    conn.execute("SAVEPOINT invalid_starter_state")
    try:
        writer._upsert_player_stats_row(conn, invalid_starter)
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT invalid_starter_state")
        conn.execute("RELEASE SAVEPOINT invalid_starter_state")
        _record(results, "smallint_starter_rejects_invalid_state", "PASS" if exc.sqlstate == "22023" else "FAIL", sqlstate=exc.sqlstate)
    else:
        conn.execute("ROLLBACK TO SAVEPOINT invalid_starter_state")
        conn.execute("RELEASE SAVEPOINT invalid_starter_state")
        _record(results, "smallint_starter_rejects_invalid_state", "FAIL", detail="invalid smallint state accepted")

    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats (player_id,game_id,game_date,position,at_bats,plate_appearances,hit_by_pitch,sacrifice_flies,sacrifice_hits,catcher_interference) VALUES (205,900205,'2026-09-23','LF',3,NULL,NULL,NULL,NULL,NULL)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    pa_row = PaRow(game_date="2026-09-23", game_id=900205, player_id=205, player_name="fixture-player", team="NYY", opponent="BOS", at_bats=3, walks=1, hit_by_pitch=0, sacrifice_flies=0, sacrifice_hits=0, catcher_interference=0, plate_appearances_direct=4, plate_appearances_formula=4, plate_appearances=4, formula_matches_direct=True, used_direct_pa=True, missing_components="")
    pa_fill = _update_player_stats_pa(conn, [pa_row], source="statsapi")
    pa_replay = _update_player_stats_pa(conn, [pa_row], source="statsapi")
    pa_values = conn.execute("SELECT plate_appearances,hit_by_pitch,sacrifice_flies,sacrifice_hits,catcher_interference,pa_source FROM mlb.player_stats WHERE player_id=205 AND game_id=900205").fetchone()
    _record(results, "pa_verified_null_fill_and_identical_replay", "PASS" if pa_fill == 1 and pa_replay == 0 and pa_values == (4,0,0,0,0,"statsapi") else "FAIL", first=pa_fill, replay=pa_replay, values=list(pa_values))
    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats (player_id,game_id,game_date,position) VALUES (206,900206,'2026-09-23',NULL)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    ineligible_fill = conn.execute("SELECT mlb.safe_fill_player_stats_pa(206,900206,1,0,0,0,0,'statsapi')").fetchone()[0]
    ineligible_pa = conn.execute("SELECT plate_appearances FROM mlb.player_stats WHERE player_id=206 AND game_id=900206").fetchone()[0]
    _record(results, "pa_unknown_role_without_batting_evidence_unchanged", "PASS" if ineligible_fill == 0 and ineligible_pa is None else "FAIL", writes=ineligible_fill, retained=ineligible_pa)
    conflict_row = PaRow(**{**pa_row.__dict__, "plate_appearances": 5, "plate_appearances_direct": 5})
    conflict_writes = _update_player_stats_pa(conn, [conflict_row], source="statsapi")
    retained_pa = conn.execute("SELECT plate_appearances FROM mlb.player_stats WHERE player_id=205 AND game_id=900205").fetchone()[0]
    _record(results, "pa_populated_conflict_reported_and_preserved", "PASS" if conflict_writes == 0 and conflict_row.conflicting_fields == ["plate_appearances"] and retained_pa == 4 else "FAIL", writes=conflict_writes, conflicts=conflict_row.conflicting_fields, retained=retained_pa)
    conn.execute("SAVEPOINT pa_entrypoint_conflict")
    try:
        conn.execute("SELECT mlb.safe_fill_player_stats_pa(205,900205,5,0,0,0,0,'statsapi')")
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT pa_entrypoint_conflict")
        conn.execute("RELEASE SAVEPOINT pa_entrypoint_conflict")
        _record(results, "pa_entrypoint_populated_conflict_fails_closed", "PASS" if exc.sqlstate == "23514" else "FAIL", sqlstate=exc.sqlstate)
    else:
        conn.execute("ROLLBACK TO SAVEPOINT pa_entrypoint_conflict")
        conn.execute("RELEASE SAVEPOINT pa_entrypoint_conflict")
        _record(results, "pa_entrypoint_populated_conflict_fails_closed", "FAIL", detail="populated correction was permitted")
    conn.execute("SAVEPOINT pa_invalid_source")
    try:
        conn.execute("SELECT mlb.safe_fill_player_stats_pa(205,900205,4,0,0,0,0,'unverified')")
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT pa_invalid_source")
        conn.execute("RELEASE SAVEPOINT pa_invalid_source")
        _record(results, "pa_source_restricted_to_statsapi", "PASS" if exc.sqlstate == "22023" else "FAIL", sqlstate=exc.sqlstate)
    else:
        conn.execute("ROLLBACK TO SAVEPOINT pa_invalid_source")
        conn.execute("RELEASE SAVEPOINT pa_invalid_source")
        _record(results, "pa_source_restricted_to_statsapi", "FAIL", detail="unapproved source was accepted")
    _expect_denied(results=results, conn=conn, name="pa_function_replace_denied", sql="CREATE OR REPLACE FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text) RETURNS integer LANGUAGE sql AS 'SELECT 0'")
    _expect_denied(results=results, conn=conn, name="pa_function_drop_denied", sql="DROP FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text)")

    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats (player_id,game_id,game_date,hits,total_bases) VALUES (202,900202,'2026-09-23',NULL,NULL)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    fill = writer._upsert_player_stats_row(conn, _source_row(202,900202,hits=2))
    fill_values = conn.execute("SELECT hits,total_bases FROM mlb.player_stats WHERE player_id=202 AND game_id=900202").fetchone()
    _record(results, "verified_null_fill", "PASS" if fill == 1 and fill_values == (2,2) else "FAIL", writes=int(fill), values=list(fill_values))

    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats (player_id,game_id,game_date,position,at_bats) VALUES (203,900203,'2026-09-23','LF',0),(204,900204,'2026-09-23','P',0)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    ab_fill = writer._backfill_player_stats_at_bats(conn, player_id=203, game_id=900203, at_bats=3)
    ab_replay = writer._backfill_player_stats_at_bats(conn, player_id=203, game_id=900203, at_bats=4)
    pitcher_fill = writer._backfill_player_stats_at_bats(conn, player_id=204, game_id=900204, at_bats=3)
    ab_values = conn.execute("SELECT at_bats FROM mlb.player_stats WHERE player_id=203 AND game_id=900203").fetchone()[0]
    _record(results, "bounded_zero_at_bats_repair", "PASS" if ab_fill == 1 and ab_replay == 0 and pitcher_fill == 0 and ab_values == 3 else "FAIL", first=int(ab_fill), replay=int(ab_replay), pitcher=int(pitcher_fill), retained=ab_values)

    conn.execute("RESET ROLE")
    training_insert = writer._upsert_training_row(conn, _training_row(900301))
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    training_replay = writer._upsert_training_row(conn, _training_row(900301))
    training_conflict = writer._upsert_training_row(conn, _training_row(900301, outcome="loss"))
    result_tuple = conn.execute("SELECT prop_value,line,over_under,outcome,status,was_correct FROM mlb.model_training_props WHERE game_id=900301").fetchone()
    _record(results, "training_insert_replay_and_historical_outcome_protection", "PASS" if training_insert == 1 and training_replay == 0 and training_conflict.conflicts and result_tuple == (1,0.5,"over","win","resolved",True) else "FAIL", inserted=int(training_insert), replay=int(training_replay), conflict=bool(training_conflict.conflicts), result=list(result_tuple) if result_tuple else None)
    production_shape_row = _training_row(900302)
    production_shape_row["game_type"] = "R"
    typed_insert = writer._upsert_training_row(conn, production_shape_row, include_game_type=False)
    typed_replay = writer._upsert_training_row(conn, production_shape_row, include_game_type=False)
    _record(results, "production_shape_omits_optional_game_type", "PASS" if typed_insert == 1 and typed_replay == 0 else "FAIL", inserted=int(typed_insert), replay=int(typed_replay))
    metadata = dict(_training_row(900301))
    metadata["game_date"] = "2026-09-24"
    metadata_update = writer._upsert_training_row(conn, metadata)
    refreshed = conn.execute("SELECT game_date::text,outcome FROM mlb.model_training_props WHERE game_id=900301").fetchone()
    _record(results, "training_metadata_refresh_with_identical_outcome", "PASS" if metadata_update == 1 and refreshed == ("2026-09-24", "win") else "FAIL", writes=int(metadata_update), values=list(refreshed))

    # Derived data is explicitly recomputable. The allowed derived UPDATE
    # path is exercised and repeated, then checked against an identity collision.
    first = writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    second = writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    _record(results, "derived_recompute_and_replay", "PASS" if first > 0 and second == 0 else "FAIL", first=first, replay=second)
    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, is_home) VALUES (102, 900102, '2026-09-23', 'NYY', true)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    conn.execute(
        "INSERT INTO mlb.player_derived_stats (player_id, game_id, game_date, team, is_home) VALUES (102, 900099, '2026-09-23', 'NYY', true)"
    )
    try:
        writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    except writer.LegacyDerivedIdentityCollision as exc:
        _record(results, "derived_identity_collision_preserved", "PASS", category=type(exc).__name__)
    else:
        _record(results, "derived_identity_collision_preserved", "FAIL", detail="collision was not rejected")


def _run_adjacent_writer_tests(conn: psycopg.Connection, results: list[dict[str, Any]]) -> None:
    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.player_ids(player_name,player_id,team,team_id) VALUES ('Original Name',6001,'BAL',99)")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    with conn.cursor() as cur:
        all_roster_columns = {"player_name", "team", "team_id", "updated_at", "is_placeholder"}
        roster_writer._upsert_player(cur, all_roster_columns, player_id=6001, player_name=None, team_abbr=None, team_id=None)
        roster_writer._upsert_player(cur, all_roster_columns, player_id=6001, player_name="Changed Name", team_abbr="BOS", team_id=111)
        placeholder = writer._upsert_player_id_min(
            conn, player_id=6002, player_name="Placeholder", team_abbr="NYY", team_id=147,
        )
        roster_inactive = cur.execute("SELECT mlb.safe_mark_player_roster_inactive(%s::bigint[],%s,%s)", ([6002], 29, 30)).fetchone()[0]
        _record(results, "roster_inactive_absent_from_production_shape_is_noop", "PASS" if roster_inactive == 0 else "FAIL", changed=roster_inactive)
        conn.execute("SAVEPOINT roster_threshold")
        try:
            cur.execute("SELECT mlb.safe_mark_player_roster_inactive(%s::bigint[],%s,%s)", ([6001], 20, 30))
        except psycopg.Error as exc:
            conn.execute("ROLLBACK TO SAVEPOINT roster_threshold")
            conn.execute("RELEASE SAVEPOINT roster_threshold")
            _record(results, "roster_inactive_threshold_fail_closed", "PASS" if exc.sqlstate == "23514" else "FAIL", sqlstate=exc.sqlstate)
        else:
            conn.execute("ROLLBACK TO SAVEPOINT roster_threshold")
            conn.execute("RELEASE SAVEPOINT roster_threshold")
            _record(results, "roster_inactive_threshold_fail_closed", "FAIL", detail="insufficient team success was accepted")
    conn.execute("RESET ROLE")
    roster_state = conn.execute("SELECT player_name,team,team_id,is_placeholder FROM mlb.player_ids WHERE player_id=6001").fetchone()
    placeholder_state = conn.execute("SELECT player_name,team,team_id,is_placeholder FROM mlb.player_ids WHERE player_id=6002").fetchone()
    _record(results, "roster_conflict_replay_and_null_preservation", "PASS" if roster_state == ("Changed Name","BOS",111,False) else "FAIL", observed=list(roster_state or ()))
    _record(results, "stat_derived_placeholder_insert_if_absent", "PASS" if placeholder == 1 and placeholder_state == ("Placeholder","NYY",147,True) else "FAIL", inserted=placeholder, observed=list(placeholder_state or ()))

    schedule = {
        "gamePk": 900901,
        "officialDate": "2026-09-23",
        "gameDate": "2026-09-23T23:00:00Z",
        "gameType": "R",
        "season": "2026",
        "teams": {"home": {"team": {"id": 147, "abbreviation": "NYY"}}, "away": {"team": {"id": 111, "abbreviation": "BOS"}}},
    }
    source_hash = "a" * 64
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    conn.execute("RESET ROLE")
    conn.execute("INSERT INTO mlb.game_info(game_id,game_date) VALUES (900902,'2026-09-23')")
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    null_fill_game = dict(schedule, gamePk=900902)
    null_fill = writer._upsert_game_info_min(conn, null_fill_game, source_sha256=source_hash)
    first = writer._upsert_game_info_min(conn, schedule, source_sha256=source_hash)
    replay = writer._upsert_game_info_min(conn, schedule, source_sha256=source_hash)
    changed_date = dict(schedule, officialDate="2026-09-24")
    date_refresh = writer._upsert_game_info_min(conn, changed_date, source_sha256=source_hash)
    bad_phase = {"game_id":900901,"game_date":"2026-09-25","source_season":2026,"source_game_type":"P"}
    conn.execute("SAVEPOINT game_phase_conflict")
    try:
        conn.execute("SELECT mlb.safe_upsert_game_info(%s::jsonb)", (json.dumps(bad_phase,separators=(",",":")),))
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT game_phase_conflict")
        conn.execute("RELEASE SAVEPOINT game_phase_conflict")
        phase_payload_denied = exc.sqlstate == "22023"
        _record(results, "duplicate_phase_payload_rejected", "PASS" if phase_payload_denied else "FAIL", sqlstate=exc.sqlstate)
    else:
        conn.execute("ROLLBACK TO SAVEPOINT game_phase_conflict")
        conn.execute("RELEASE SAVEPOINT game_phase_conflict")
        _record(results, "duplicate_phase_payload_rejected", "FAIL", detail="phase payload was accepted")
    conn.execute("RESET ROLE")
    game_state = conn.execute("SELECT game_date,home_team_id,home_team_abbr FROM mlb.game_info WHERE game_id=900901").fetchone()
    game_state_values = (str(game_state[0]), *game_state[1:]) if game_state else None
    null_fill_state = conn.execute("SELECT game_time IS NOT NULL,home_team_id,away_team_id,home_team_abbr,away_team_abbr FROM mlb.game_info WHERE game_id=900902").fetchone()
    _record(results, "game_info_null_fill_official_date_refresh_and_replay", "PASS" if null_fill == 1 and all(null_fill_state) and first == 1 and replay == 1 and date_refresh == 1 and game_state_values == ("2026-09-24",147,"NYY") else "FAIL", writes=[null_fill,first,replay,date_refresh], observed=list(game_state_values or ()), null_fill_row=list(null_fill_state or ()))

    feature_rows = [("hits",6001,900901,"2026-09-24",{"vs_avg":0.25,"sample":4},"v1","fixture")]
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    feature_first = bvp_writer._upsert_rows_on_connection(conn, feature_rows, batch_size=10)
    feature_replay = bvp_writer._upsert_rows_on_connection(conn, feature_rows, batch_size=10)
    feature_update = bvp_writer._upsert_rows_on_connection(conn, [("hits",6001,900901,"2026-09-24",{"vs_avg":0.5,"new_sample":2},"v1","fixture-v2")], batch_size=10)
    conn.execute("RESET ROLE")
    feature_state = conn.execute("SELECT features,model_tag FROM mlb.prop_features_precomputed WHERE prop_type='hits' AND player_id=6001 AND game_id=900901 AND feature_set_tag='v1'").fetchone()
    features = dict(feature_state[0]) if feature_state else {}
    bvp_ok = feature_first == 1 and feature_replay == 1 and feature_update == 1 and features == {"vs_avg":0.5,"sample":4,"new_sample":2} and feature_state[1] == "fixture-v2"
    _record(results, "bvp_feature_merge_replay_and_recompute", "PASS" if bvp_ok else "FAIL", writes=[feature_first,feature_replay,feature_update], features=features)


def run() -> dict[str, Any]:
    dsn = os.environ.get("NHL_PP_TOI_TEST_DSN")
    parsed = urlsplit(dsn or "")
    username = unquote(parsed.username or "")
    hostname = (parsed.hostname or "").lower()
    direct_preview_host = hostname == f"db.{EXPECTED_PROJECT_REF}.supabase.co"
    preview_pooler = hostname.endswith(".pooler.supabase.com") and username == f"postgres.{EXPECTED_PROJECT_REF}"
    reference_match = bool(
        dsn
        and (direct_preview_host or preview_pooler)
    )
    if not reference_match:
        return {"project_reference_match": False, "status": "BLOCKED", "reason": "DSN absent or preview reference mismatch"}

    report: dict[str, Any] = {
        "project_reference_match": True,
        "connection": "NOT_CONNECTED",
        "fixture": "NOT_RUN",
        "results": [],
        "production_objects_modified": False,
        "cleanup": "NOT_STARTED",
        "restricted_grant_shape": {
            "player_stats": "SELECT + fixed function EXECUTE; no direct INSERT/UPDATE/DELETE/TRUNCATE",
            "player_derived_stats": "SELECT + INSERT + UPDATE derived columns; no DELETE/TRUNCATE",
            "model_training_props": "SELECT + fixed function EXECUTE + metadata-only UPDATE; no direct INSERT/protected-tuple UPDATE",
            "player_ids": "fixed roster/identity EXECUTE only; no direct table DML",
            "game_info": "SELECT + fixed metadata EXECUTE; no direct table DML",
            "prop_features_precomputed": "writer fixed derived-feature EXECUTE only; research group gets eight verifier columns SELECT only",
            "correction_role": "NOLOGIN; no membership granted to writer",
        },
        "protected_write_boundary": {
            "player_stats_columns": list(PLAYER_STATS_FIELDS),
            "model_training_props_result_tuple": list(PROTECTED_RESULT_FIELDS),
            "model_training_props_metadata_update": [
                "team", "opponent", "team_id", "opponent_team_id", "opponent_encoded",
                "is_home", "updated_at", "game_date", "game_time", "game_day_of_week",
                "time_of_day_bucket", "streak_type", "streak_count", "rolling_result_avg_7",
            ],
        },
    }
    conn = psycopg.connect(dsn, connect_timeout=20)
    try:
        database, session_user, current_user, version = conn.execute(
            "SELECT current_database(), session_user, current_user, version()"
        ).fetchone()
        can_create_roles = bool(conn.execute("SELECT rolcreaterole FROM pg_roles WHERE rolname=current_user").fetchone()[0])
        migration_count = conn.execute("SELECT count(*) FROM supabase_migrations.schema_migrations").fetchone()[0]
        report.update({
            "connection": "PASS",
            "database": database,
            "session_user": session_user,
            "current_user": current_user,
            "postgresql_version": str(version).split(",")[0],
            "migration_version_count": migration_count,
        })
        if current_user != "postgres" or session_user != "postgres" or not can_create_roles:
            report.update({"status": "BLOCKED", "reason": "expected preview fixture administrator/CREATEROLE capability unavailable"})
            conn.rollback()
            return report
        existing_schemas = [row[0] for row in conn.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname IN ('mlb','nhl') ORDER BY nspname"
        ).fetchall()]
        existing_roles = [row[0] for row in conn.execute(
            "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE, RESEARCH_ROLE, RESEARCH_LOGIN],)
        ).fetchall()]
        if existing_schemas or existing_roles:
            report.update({"status": "BLOCKED", "reason": "preview fixture baseline is not clean", "existing_schemas": existing_schemas, "existing_test_roles": existing_roles})
            conn.rollback()
            return report

        report["initial_state"] = "clean_preview; no mlb/nhl schemas or fixture roles"
        report["phase"] = "fixture_setup"
        role_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261016_mlb_restricted_ingest_role.sql"
        role_migration_sql = role_migration_path.read_text()
        report["role_migration_sha256"] = __import__("hashlib").sha256(role_migration_sql.encode()).hexdigest()
        conn.execute(role_migration_sql, prepare=False)
        conn.execute(role_migration_sql, prepare=False)
        role_flags = conn.execute(
            "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname=%s",
            (WRITER_ROLE,),
        ).fetchone()
        expected_role_flags = (False, False, False, False, False, False, False)
        _record(report["results"], "restricted_role_provisioning_idempotent", "PASS" if role_flags == expected_role_flags else "FAIL", flags=list(role_flags), expected=list(expected_role_flags))
        _create_fixture(conn)
        conn.execute("INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, opponent, is_home, position, at_bats, hits, total_bases, singles, is_starter) VALUES (101,900101,'2026-09-23','NYY','BOS',true,'LF',3,2,2,2,0)")
        fixture_types = conn.execute("SELECT pg_typeof(is_starter)::text,game_date IS NOT NULL FROM mlb.player_stats LIMIT 1").fetchone()
        _record(report["results"], "production_player_stats_shape", "PASS" if fixture_types == ("smallint", True) else "FAIL", observed=list(fixture_types or ()))
        migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261017_safe_stat_training_write_entrypoints.sql"
        migration_sql = migration_path.read_text()
        report["migration_sha256"] = __import__("hashlib").sha256(migration_sql.encode()).hexdigest()
        conn.execute(migration_sql, prepare=False)
        conn.execute(migration_sql, prepare=False)
        pa_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261018_safe_player_stats_pa_backfill.sql"
        pa_migration_sql = pa_migration_path.read_text()
        report["pa_backfill_migration_sha256"] = __import__("hashlib").sha256(pa_migration_sql.encode()).hexdigest()
        conn.execute(pa_migration_sql, prepare=False)
        conn.execute(pa_migration_sql, prepare=False)
        derived_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261018_mlb_derived_stats_writer_grants.sql"
        derived_migration_sql = derived_migration_path.read_text()
        report["derived_writer_migration_sha256"] = __import__("hashlib").sha256(derived_migration_sql.encode()).hexdigest()
        conn.execute(derived_migration_sql, prepare=False)
        conn.execute(derived_migration_sql, prepare=False)
        adjacent_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261019_mlb_adjacent_writer_entrypoints.sql"
        adjacent_migration_sql = adjacent_migration_path.read_text()
        report["adjacent_writer_migration_sha256"] = __import__("hashlib").sha256(adjacent_migration_sql.encode()).hexdigest()
        conn.execute(adjacent_migration_sql, prepare=False)
        conn.execute(adjacent_migration_sql, prepare=False)
        prediction_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261020_safe_prediction_backfill_entrypoint.sql"
        prediction_migration_sql = prediction_migration_path.read_text()
        report["prediction_backfill_migration_sha256"] = __import__("hashlib").sha256(prediction_migration_sql.encode()).hexdigest()
        conn.execute(prediction_migration_sql, prepare=False)
        conn.execute(prediction_migration_sql, prepare=False)
        rls_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261021_mlb_restricted_ingest_rls_policies.sql"
        rls_migration_sql = rls_migration_path.read_text()
        report["rls_policy_migration_sha256"] = __import__("hashlib").sha256(rls_migration_sql.encode()).hexdigest()
        conn.execute(rls_migration_sql, prepare=False)
        conn.execute(rls_migration_sql, prepare=False)
        streak_migration_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261022_mlb_ingest_streak_profile_read.sql"
        streak_migration_sql = streak_migration_path.read_text()
        report["streak_profile_read_migration_sha256"] = __import__("hashlib").sha256(streak_migration_sql.encode()).hexdigest()
        conn.execute(streak_migration_sql, prepare=False)
        conn.execute(streak_migration_sql, prepare=False)
        streak_types = {
            row[0]: row[1] for row in conn.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_schema='mlb' AND table_name='player_streak_profiles'"
            ).fetchall()
        }
        expected_streak_types = {
            "player_id": "bigint", "prop_type": "text", "prop_source": "text",
            "last_game_date": "date", "streak_type": "text", "streak_count": "integer",
        }
        _record(report["results"], "streak_profile_production_read_shape", "PASS" if all(streak_types.get(k) == v for k, v in expected_streak_types.items()) else "FAIL", observed={k: streak_types.get(k) for k in expected_streak_types})
        rls_checks = conn.execute(
            """SELECT c.relname,c.relrowsecurity,
                      EXISTS (SELECT 1 FROM pg_policies p WHERE p.schemaname='mlb' AND p.tablename=c.relname AND p.policyname='proppadia_mlb_ingest_select')
                 FROM pg_class c WHERE c.oid IN ('mlb.player_stats'::regclass,'mlb.model_training_props'::regclass,'mlb.player_derived_stats'::regclass)
                 ORDER BY c.relname"""
        ).fetchall()
        rls_ok = len(rls_checks) == 3 and all(bool(row[1]) and bool(row[2]) for row in rls_checks)
        _record(report["results"], "production_rls_policy_shape", "PASS" if rls_ok else "FAIL", relations=[list(row) for row in rls_checks])
        prediction_columns = {row[0] for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='mlb' AND table_name='game_info'").fetchall()}
        phase_column_names = {"source_season","source_game_type","season_phase","postseason_round","season_name","source_round","schedule_relationships","game_type_source_sha256"}
        expected_game_info_columns = {"game_id","game_time","game_date","home_team_id","away_team_id","home_team_abbr","away_team_abbr","starting_pitcher_id_home","starting_pitcher_id_away"}
        _record(report["results"], "game_info_production_shape", "PASS" if prediction_columns == expected_game_info_columns else "FAIL", observed=sorted(prediction_columns), unexpected_phase=sorted(prediction_columns & phase_column_names))
        training_shape = {
            row[0]: (row[1], row[2]) for row in conn.execute(
                "SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='mlb' AND table_name='model_training_props' AND column_name IN ('game_time','game_day_of_week','opponent_encoded','team_id','game_type')"
            ).fetchall()
        }
        training_shape_ok = training_shape == {
            "game_time": ("timestamp without time zone", "YES"),
            "game_day_of_week": ("text", "YES"),
            "opponent_encoded": ("text", "YES"),
            "team_id": ("bigint", "NO"),
        }
        _record(report["results"], "model_training_props_production_types", "PASS" if training_shape_ok else "FAIL", columns=training_shape)
        player_id_shape = {row[0]: row[1] for row in conn.execute(
            "SELECT column_name,is_nullable FROM information_schema.columns WHERE table_schema='mlb' AND table_name='player_ids' AND column_name IN ('player_name','active','status')"
        ).fetchall()}
        _record(report["results"], "player_ids_production_shape", "PASS" if player_id_shape == {"player_name":"NO"} else "FAIL", columns=player_id_shape)
        conn.execute("INSERT INTO mlb.model_training_props(id,game_id,player_id,team_id,prop_type,prop_source,outcome,status,was_correct) VALUES ('00000000-0000-0000-0000-000000900501',900501,453286,1,'hits','mlb_api','win','resolved',true)")
        conn.execute("INSERT INTO mlb.model_training_props(id,game_id,player_id,team_id,prop_type,prop_source,outcome,status,was_correct,predicted_outcome,confidence_score,prediction_timestamp) VALUES ('00000000-0000-0000-0000-000000900502',900502,453286,1,'hits','mlb_api','loss','resolved',false,'under',0.8,'2026-09-23T20:00:00Z')")
        _record(report["results"], "migration_idempotent_replay", "PASS")
        report["fixture_object_owners"] = {
            "player_stats": conn.execute("SELECT r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname='mlb' AND c.relname='player_stats'").fetchone()[0],
            "source_function": conn.execute("SELECT r.rolname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner WHERE n.nspname='mlb' AND p.proname='safe_upsert_player_stats'").fetchone()[0],
        }
        grants = conn.execute(
            """SELECT
                 has_table_privilege(%s,'mlb.player_stats','INSERT'),
                 has_table_privilege(%s,'mlb.player_stats','UPDATE'),
                 has_table_privilege(%s,'mlb.model_training_props','INSERT'),
                 has_table_privilege(%s,'mlb.model_training_props','UPDATE'),
                 has_column_privilege(%s,'mlb.model_training_props','outcome','UPDATE'),
                 has_column_privilege(%s,'mlb.model_training_props','rolling_result_avg_7','UPDATE'),
                 has_function_privilege(%s,'mlb.safe_upsert_player_stats(jsonb)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_upsert_model_training_props(jsonb)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamp with time zone)','EXECUTE')""",
            (WRITER_ROLE,) * 11,
        ).fetchone()
        expected_grants = (False, False, False, False, False, True, True, True, True, True, True)
        _record(report["results"], "entrypoint_grant_shape", "PASS" if grants == expected_grants else "FAIL", observed=list(grants), expected=list(expected_grants))
        derived_acl = conn.execute(
            """SELECT has_table_privilege(%s,'mlb.player_derived_stats','SELECT'),
                      has_table_privilege(%s,'mlb.player_derived_stats','INSERT'),
                      has_any_column_privilege(%s,'mlb.player_derived_stats','INSERT'),
                      has_table_privilege(%s,'mlb.player_derived_stats','UPDATE'),
                      has_any_column_privilege(%s,'mlb.player_derived_stats','UPDATE'),
                      has_table_privilege(%s,'mlb.player_derived_stats','DELETE'),
                      has_table_privilege(%s,'mlb.player_derived_stats','TRUNCATE')""",
            (WRITER_ROLE,) * 7,
        ).fetchone()
        expected_derived_acl = (True, False, True, False, True, False, False)
        _record(report["results"], "derived_writer_exact_acl", "PASS" if derived_acl == expected_derived_acl else "FAIL", observed=list(derived_acl), expected=list(expected_derived_acl))
        adjacent_privileges = conn.execute(
            """SELECT
                 has_table_privilege(%s,'mlb.player_ids','INSERT') OR has_any_column_privilege(%s,'mlb.player_ids','INSERT'),
                 has_table_privilege(%s,'mlb.player_ids','UPDATE') OR has_any_column_privilege(%s,'mlb.player_ids','UPDATE'),
                 has_table_privilege(%s,'mlb.game_info','INSERT') OR has_any_column_privilege(%s,'mlb.game_info','INSERT'),
                 has_table_privilege(%s,'mlb.game_info','UPDATE') OR has_any_column_privilege(%s,'mlb.game_info','UPDATE'),
                 has_table_privilege(%s,'mlb.prop_features_precomputed','INSERT') OR has_any_column_privilege(%s,'mlb.prop_features_precomputed','INSERT'),
                 has_table_privilege(%s,'mlb.prop_features_precomputed','UPDATE') OR has_any_column_privilege(%s,'mlb.prop_features_precomputed','UPDATE'),
                 has_table_privilege(%s,'mlb.player_ids','DELETE') OR has_table_privilege(%s,'mlb.player_ids','TRUNCATE'),
                 has_table_privilege(%s,'mlb.game_info','DELETE') OR has_table_privilege(%s,'mlb.game_info','TRUNCATE'),
                 has_table_privilege(%s,'mlb.prop_features_precomputed','DELETE') OR has_table_privilege(%s,'mlb.prop_features_precomputed','TRUNCATE'),
                 has_function_privilege(%s,'mlb.safe_upsert_player_roster(jsonb)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_insert_player_id_if_absent(jsonb)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_mark_player_roster_inactive(bigint[],integer,integer)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_upsert_game_info(jsonb)','EXECUTE'),
                 has_function_privilege(%s,'mlb.safe_upsert_prop_features_precomputed(jsonb)','EXECUTE')""",
            (WRITER_ROLE,) * 23,
        ).fetchone()
        expected_adjacent_privileges = (False,False,False,False,False,False,False,False,False,True,True,True,True,True)
        _record(report["results"], "adjacent_writer_exact_privilege_shape", "PASS" if adjacent_privileges == expected_adjacent_privileges else "FAIL", observed=list(adjacent_privileges), expected=list(expected_adjacent_privileges))
        report["phase"] = "restricted_role_behavior"
        _run_role_tests(conn, report["results"])
        _run_streak_read_tests(conn, report["results"])
        _run_adjacent_writer_tests(conn, report["results"])
        _run_bvp_research_read_tests(conn, report["results"])

        report["phase"] = "migration_rollback"
        conn.execute("RESET ROLE")
        bvp_read_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261017_mlb_bvp_research_read_rollback.sql"
        bvp_read_rollback_sql = bvp_read_rollback_path.read_text()
        report["bvp_research_read_rollback_sha256"] = __import__("hashlib").sha256(bvp_read_rollback_sql.encode()).hexdigest()
        conn.execute(bvp_read_rollback_sql, prepare=False)
        bvp_read_remains = conn.execute(
            """SELECT has_any_column_privilege(%s,'mlb.prop_features_precomputed','SELECT')
                    OR has_table_privilege(%s,'mlb.prop_features_precomputed','SELECT')""",
            (RESEARCH_ROLE, RESEARCH_ROLE),
        ).fetchone()[0]
        _record(report["results"], "bvp_research_read_rollback_removes_grant", "PASS" if not bvp_read_remains else "FAIL", grants_remain=bool(bvp_read_remains))
        streak_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261022_mlb_ingest_streak_profile_read_rollback.sql"
        streak_rollback_sql = streak_rollback_path.read_text()
        report["streak_profile_read_rollback_sha256"] = __import__("hashlib").sha256(streak_rollback_sql.encode()).hexdigest()
        conn.execute(streak_rollback_sql, prepare=False)
        streak_policy_remaining = conn.execute(
            "SELECT count(*) FROM pg_policies WHERE schemaname='mlb' "
            "AND tablename='player_streak_profiles' AND policyname='proppadia_mlb_ingest_streak_select'"
        ).fetchone()[0]
        streak_select_remaining = conn.execute(
            "SELECT bool_or(has_column_privilege(%s,'mlb.player_streak_profiles',column_name,'SELECT')) "
            "FROM unnest(ARRAY['player_id','prop_type','prop_source','last_game_date','streak_type','streak_count']::name[]) column_name",
            (WRITER_ROLE,),
        ).fetchone()[0]
        _record(report["results"], "streak_profile_read_rollback_restores_acl_and_policy_baseline", "PASS" if streak_policy_remaining == 0 and not streak_select_remaining else "FAIL", policy_remaining=streak_policy_remaining, select_privilege_remaining=bool(streak_select_remaining))
        rls_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261021_mlb_restricted_ingest_rls_policies_rollback.sql"
        rls_rollback_sql = rls_rollback_path.read_text()
        report["rls_policy_rollback_sha256"] = __import__("hashlib").sha256(rls_rollback_sql.encode()).hexdigest()
        conn.execute(rls_rollback_sql, prepare=False)
        rls_policies_remain = conn.execute(
            "SELECT count(*) FROM pg_policies WHERE schemaname='mlb' AND policyname LIKE 'proppadia_mlb_ingest_%'"
        ).fetchone()[0]
        _record(report["results"], "rls_policy_rollback_removes_only_ingest_policies", "PASS" if rls_policies_remain == 0 else "FAIL", remaining=rls_policies_remain)
        prediction_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261020_safe_prediction_backfill_entrypoint_rollback.sql"
        prediction_rollback_sql = prediction_rollback_path.read_text()
        report["prediction_backfill_rollback_sha256"] = __import__("hashlib").sha256(prediction_rollback_sql.encode()).hexdigest()
        conn.execute(prediction_rollback_sql, prepare=False)
        prediction_function_remaining = conn.execute("SELECT to_regprocedure('mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz)') IS NOT NULL").fetchone()[0]
        _record(report["results"], "prediction_backfill_rollback_removes_only_entrypoint", "PASS" if not prediction_function_remaining else "FAIL", function_remaining=bool(prediction_function_remaining))
        adjacent_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261019_mlb_adjacent_writer_entrypoints_rollback.sql"
        adjacent_rollback_sql = adjacent_rollback_path.read_text()
        report["adjacent_writer_rollback_sha256"] = __import__("hashlib").sha256(adjacent_rollback_sql.encode()).hexdigest()
        conn.execute(adjacent_rollback_sql, prepare=False)
        adjacent_functions_remaining = conn.execute(
            "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='mlb' AND p.proname IN ('safe_upsert_player_roster','safe_insert_player_id_if_absent','safe_mark_player_roster_inactive','safe_upsert_game_info','safe_upsert_prop_features_precomputed')"
        ).fetchone()[0]
        direct_adjacent_dml = conn.execute(
            """SELECT
                 has_table_privilege(%s,'mlb.player_ids','INSERT') OR has_any_column_privilege(%s,'mlb.player_ids','INSERT') OR has_table_privilege(%s,'mlb.player_ids','UPDATE') OR has_any_column_privilege(%s,'mlb.player_ids','UPDATE'),
                 has_table_privilege(%s,'mlb.game_info','INSERT') OR has_any_column_privilege(%s,'mlb.game_info','INSERT') OR has_table_privilege(%s,'mlb.game_info','UPDATE') OR has_any_column_privilege(%s,'mlb.game_info','UPDATE'),
                 has_table_privilege(%s,'mlb.prop_features_precomputed','INSERT') OR has_any_column_privilege(%s,'mlb.prop_features_precomputed','INSERT') OR has_table_privilege(%s,'mlb.prop_features_precomputed','UPDATE') OR has_any_column_privilege(%s,'mlb.prop_features_precomputed','UPDATE')""",
            (WRITER_ROLE,) * 12,
        ).fetchone()
        adjacent_select_remains = conn.execute("SELECT has_table_privilege(%s,'mlb.game_info','SELECT')", (WRITER_ROLE,)).fetchone()[0]
        _record(report["results"], "adjacent_writer_rollback_removes_functions_and_dml", "PASS" if adjacent_functions_remaining == 0 and not any(direct_adjacent_dml) and not adjacent_select_remains else "FAIL", functions_remaining=adjacent_functions_remaining, direct_dml=list(direct_adjacent_dml), game_info_select_remains=bool(adjacent_select_remains))
        pa_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261018_safe_player_stats_pa_backfill_rollback.sql"
        pa_rollback_sql = pa_rollback_path.read_text()
        report["pa_backfill_rollback_sha256"] = __import__("hashlib").sha256(pa_rollback_sql.encode()).hexdigest()
        conn.execute(pa_rollback_sql, prepare=False)
        pa_functions_remaining = conn.execute(
            "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='mlb' AND p.proname='safe_fill_player_stats_pa'"
        ).fetchone()[0]
        _record(report["results"], "pa_backfill_rollback_removes_only_entrypoint", "PASS" if pa_functions_remaining == 0 else "FAIL", functions_remaining=pa_functions_remaining)
        derived_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261018_mlb_derived_stats_writer_grants_rollback.sql"
        derived_rollback_sql = derived_rollback_path.read_text()
        report["derived_writer_rollback_sha256"] = __import__("hashlib").sha256(derived_rollback_sql.encode()).hexdigest()
        conn.execute(derived_rollback_sql, prepare=False)
        derived_dml_remains = conn.execute(
            "SELECT has_any_column_privilege(%s,'mlb.player_derived_stats','INSERT') OR has_any_column_privilege(%s,'mlb.player_derived_stats','UPDATE') OR has_table_privilege(%s,'mlb.player_derived_stats','DELETE') OR has_table_privilege(%s,'mlb.player_derived_stats','TRUNCATE')",
            (WRITER_ROLE,) * 4,
        ).fetchone()[0]
        _record(report["results"], "derived_writer_rollback_removes_grants", "PASS" if not derived_dml_remains else "FAIL", grants_remain=bool(derived_dml_remains))
        rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261017_safe_stat_training_write_entrypoints_rollback.sql"
        rollback_sql = rollback_path.read_text()
        report["rollback_sha256"] = __import__("hashlib").sha256(rollback_sql.encode()).hexdigest()
        conn.execute(rollback_sql, prepare=False)
        functions_remaining = conn.execute(
            "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='mlb' AND p.proname IN ('safe_upsert_player_stats','safe_fill_player_stats_at_bats','safe_fill_player_stats_pa','safe_upsert_model_training_props')"
        ).fetchone()[0]
        direct_dml = conn.execute(
            "SELECT has_table_privilege(%s,'mlb.player_stats','INSERT') OR has_table_privilege(%s,'mlb.player_stats','UPDATE') OR has_table_privilege(%s,'mlb.model_training_props','INSERT')",
            (WRITER_ROLE,) * 3,
        ).fetchone()[0]
        _record(report["results"], "fail_closed_migration_rollback", "PASS" if functions_remaining == 0 and not direct_dml else "FAIL", functions_remaining=functions_remaining, direct_dml_restored=bool(direct_dml))

        conn.execute(f"REVOKE {WRITER_ROLE} FROM postgres")
        role_rollback_path = Path(__file__).resolve().parents[1] / "sql/migrations/20261016_mlb_restricted_ingest_role_rollback.sql"
        role_rollback_sql = role_rollback_path.read_text()
        report["role_rollback_sha256"] = __import__("hashlib").sha256(role_rollback_sql.encode()).hexdigest()
        conn.execute(role_rollback_sql, prepare=False)
        role_remaining = conn.execute("SELECT count(*) FROM pg_roles WHERE rolname=%s", (WRITER_ROLE,)).fetchone()[0]
        _record(report["results"], "role_rollback_removes_only_fixture_group", "PASS" if role_remaining == 0 else "FAIL", roles_remaining=role_remaining)

        report["fixture"] = "FAIL" if any(item["status"] in {"FAIL", "BLOCKED"} for item in report["results"]) else "PASS"
        report["status"] = "BLOCKED" if report["fixture"] == "FAIL" else "PASS"
        report["cleanup"] = "ROLLBACK_PENDING"
        conn.rollback()
        remaining_schemas = [row[0] for row in conn.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname IN ('mlb','nhl') ORDER BY nspname"
        ).fetchall()]
        remaining_roles = [row[0] for row in conn.execute(
            "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE, RESEARCH_ROLE, RESEARCH_LOGIN],)
        ).fetchall()]
        clean = not remaining_schemas and not remaining_roles
        report["cleanup"] = "COMPLETE" if clean else "PARTIAL"
        report["fixture_objects_after_rollback"] = "NONE" if clean else {"schemas": remaining_schemas, "roles": remaining_roles}
        conn.rollback()
        return report
    except Exception as exc:
        try:
            conn.rollback()
            remaining_schemas = [row[0] for row in conn.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname IN ('mlb','nhl') ORDER BY nspname"
            ).fetchall()]
            remaining_roles = [row[0] for row in conn.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE, RESEARCH_ROLE, RESEARCH_LOGIN],)
            ).fetchall()]
            clean = not remaining_schemas and not remaining_roles
            report["cleanup"] = "COMPLETE" if clean else "PARTIAL"
            report["fixture_objects_after_rollback"] = "NONE" if clean else {"schemas": remaining_schemas, "roles": remaining_roles}
            conn.rollback()
        except Exception:
            report["cleanup"] = "ROLLBACK_FAILED"
        diag = getattr(exc, "diag", None)
        report.update({
            "fixture": "FAIL",
            "status": "BLOCKED",
            "phase": report.get("phase"),
            "sanitized_exception": type(exc).__name__,
            "sqlstate": getattr(exc, "sqlstate", None),
            "sanitized_message": getattr(diag, "message_primary", None),
            "sanitized_context": getattr(diag, "context", None),
            "error_schema": getattr(diag, "schema_name", None),
            "error_table": getattr(diag, "table_name", None),
            "error_column": getattr(diag, "column_name", None),
        })
        return report
    finally:
        conn.close()


def main() -> int:
    report = run()
    output = json.dumps(report, sort_keys=True, indent=2, default=str) + "\n"
    sys.stdout.write(output)
    receipt_path = os.environ.get("MLB_WRITER_PG_RECEIPT")
    if receipt_path:
        target = Path(receipt_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(output)
        target.chmod(0o600)
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
