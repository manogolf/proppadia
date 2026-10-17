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
from datetime import datetime, timezone
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
        "is_starter": False,
    }
    return row


def _training_row(game_id: int, *, outcome: str = "win", game_date: str = "2026-09-23") -> dict[str, Any]:
    now = datetime(2026, 9, 23, 20, tzinfo=timezone.utc).isoformat()
    return {
        "id": f"00000000-0000-0000-0000-{game_id:012d}",
        "game_id": game_id,
        "player_id": "453286",
        "player_name": "fixture-player",
        "team": "1",
        "opponent": "2",
        "team_id": 1,
        "opponent_team_id": 2,
        "opponent_encoded": 2,
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
        "game_day_of_week": 2,
        "time_of_day_bucket": "evening",
        "streak_type": None,
        "streak_count": None,
    }


def _create_fixture(conn: psycopg.Connection) -> None:
    conn.execute(f"CREATE ROLE {CORRECTION_ROLE} NOLOGIN NOINHERIT NOCREATEROLE NOCREATEDB")
    conn.execute(f"GRANT {WRITER_ROLE} TO postgres")
    conn.execute("CREATE SCHEMA mlb")
    conn.execute(
        """
        CREATE TABLE mlb.player_stats (
            player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date,
            team text, opponent text, is_home boolean, position text,
            plate_appearances integer, at_bats integer, hits integer, total_bases integer,
            rbis integer, runs_scored integer, strikeouts_batting integer, walks integer,
            hit_by_pitch integer, sacrifice_flies integer, sacrifice_hits integer,
            catcher_interference integer, pa_source text, pa_backfilled_at timestamptz,
            singles integer, doubles integer, triples integer, home_runs integer,
            stolen_bases integer, strikeouts_pitching integer, walks_allowed integer,
            hits_allowed integer, outs_recorded integer, earned_runs numeric, is_starter boolean,
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
            player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date NOT NULL,
            team text, is_home boolean, {metric_columns}, updated_at timestamptz,
            {pa_aggregate_columns}, PRIMARY KEY(player_id, game_id), UNIQUE(player_id, game_date)
        )"""
    )
    conn.execute(
        """
        CREATE TABLE mlb.model_training_props (
            id uuid NOT NULL, game_id bigint NOT NULL, player_id bigint NOT NULL,
            player_name text, team text, opponent text, team_id bigint, opponent_team_id bigint,
            opponent_encoded bigint, is_home boolean, prop_type text NOT NULL,
            prop_value numeric, line numeric, over_under text, outcome text, status text,
            created_at timestamptz, updated_at timestamptz, prop_source text NOT NULL,
            was_correct boolean, game_date date, game_time timestamptz,
            game_day_of_week integer, time_of_day_bucket text, streak_type text,
            streak_count integer, rolling_result_avg_7 numeric, game_type text,
            UNIQUE(player_id, game_id, prop_type, prop_source)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.player_ids (
            player_id bigint PRIMARY KEY, player_name text, team text, team_id bigint,
            active boolean, status text, updated_at timestamptz, is_placeholder boolean
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.game_info (
            game_id bigint PRIMARY KEY, game_time timestamp without time zone, game_date date NOT NULL,
            home_team_id bigint, away_team_id bigint, home_team_abbr text, away_team_abbr text,
            source_season integer, source_game_type text, season_phase text, postseason_round text,
            season_name text, source_round text, schedule_relationships jsonb NOT NULL DEFAULT '{}'::jsonb,
            game_type_source_sha256 text
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE mlb.prop_features_precomputed (
            prop_type text NOT NULL, player_id bigint NOT NULL, game_id bigint NOT NULL,
            game_date date NOT NULL, features jsonb, feature_set_tag text NOT NULL,
            model_tag text NOT NULL, computed_at timestamptz NOT NULL,
            PRIMARY KEY(prop_type,player_id,game_id,feature_set_tag)
        )
        """
    )
    conn.execute(f"GRANT USAGE ON SCHEMA mlb TO {WRITER_ROLE}")


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
    _expect_denied(conn, "game_info_phase_replacement_direct_denied", "UPDATE mlb.game_info SET source_game_type='P' WHERE game_id=1", results)
    _expect_denied(conn, "game_info_date_replacement_direct_denied", "UPDATE mlb.game_info SET game_date='1900-01-01' WHERE game_id=1", results)
    _expect_denied(conn, "bvp_features_direct_replacement_denied", "UPDATE mlb.prop_features_precomputed SET features='{}'::jsonb WHERE player_id=1", results)
    _expect_denied(conn, "correction_role_switch_denied", f"SET ROLE {CORRECTION_ROLE}", results)
    _expect_denied(conn, "role_grant_escalation_denied", f"GRANT {CORRECTION_ROLE} TO {WRITER_ROLE}", results)
    _expect_denied(conn, "function_replace_denied", "CREATE OR REPLACE FUNCTION mlb.safe_upsert_player_stats(jsonb) RETURNS integer LANGUAGE sql AS 'SELECT 0'", results)
    _expect_denied(conn, "function_drop_denied", "DROP FUNCTION mlb.safe_upsert_player_stats(jsonb)", results)

    inserted = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    replay = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    _record(results, "source_insert_and_same_value_replay", "PASS" if inserted == 1 and replay == 0 else "FAIL", inserted=int(inserted), replay=int(replay))
    _expect_denied(conn, "source_populated_to_null_denied", "UPDATE mlb.player_stats SET hits = NULL WHERE player_id=201 AND game_id=900201", results)
    conflict = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=3))
    source_value = conn.execute("SELECT hits FROM mlb.player_stats WHERE player_id=201 AND game_id=900201").fetchone()[0]
    _record(results, "source_conflict_preserved", "PASS" if conflict.conflicts and source_value == 2 else "FAIL", conflicts=list(conflict.conflicts[0]["conflicting_fields"]) if conflict.conflicts else [], retained=source_value)

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
    typed_row = _training_row(900302)
    typed_row["game_type"] = "R"
    typed_insert = writer._upsert_training_row(conn, typed_row, include_game_type=True)
    typed_replay = writer._upsert_training_row(conn, typed_row, include_game_type=True)
    typed_value = conn.execute("SELECT game_type FROM mlb.model_training_props WHERE game_id=900302").fetchone()[0]
    _record(results, "optional_game_type_entrypoint", "PASS" if typed_insert == 1 and typed_replay == 0 and typed_value == "R" else "FAIL", inserted=int(typed_insert), replay=int(typed_replay), value=typed_value)
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
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    with conn.cursor() as cur:
        all_roster_columns = {"player_name", "team", "team_id", "active", "status", "updated_at"}
        roster_writer._upsert_player(cur, all_roster_columns, player_id=6001, player_name=None, team_abbr=None, team_id=None)
        roster_writer._upsert_player(cur, all_roster_columns, player_id=6001, player_name="Roster Name", team_abbr="NYY", team_id=147)
        roster_writer._upsert_player(cur, all_roster_columns, player_id=6001, player_name="Changed Name", team_abbr="BOS", team_id=111)
        placeholder = writer._upsert_player_id_min(
            conn, player_id=6002, player_name="Placeholder", team_abbr="NYY", team_id=147,
        )
        roster_writer._mark_inactive(cur, all_roster_columns, [6002], successful_teams=29, total_teams=30)
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
    roster_state = conn.execute("SELECT player_name,team,team_id,active,status FROM mlb.player_ids WHERE player_id=6001").fetchone()
    placeholder_state = conn.execute("SELECT player_name,team,team_id,is_placeholder FROM mlb.player_ids WHERE player_id=6002").fetchone()
    _record(results, "roster_conflict_replay_and_inactive_mark", "PASS" if roster_state == ("Changed Name","BOS",111,False,"inactive") else "FAIL", observed=list(roster_state or ()))
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
    phase_seed = {
        "game_id":900902,"game_time":None,"game_date":"2026-09-23","home_team_id":None,
        "away_team_id":None,"home_team_abbr":None,"away_team_abbr":None,
        "source_season":2026,"source_game_type":"R","season_phase":"REGULAR_SEASON",
        "postseason_round":None,"season_name":"MLB_2026_REGULAR_SEASON","source_round":None,
        "schedule_relationships":{},"game_type_source_sha256":source_hash,
    }
    conn.execute("SELECT mlb.safe_upsert_game_info(%s::jsonb)", (json.dumps(phase_seed,separators=(",",":")),))
    null_fill_game = dict(schedule, gamePk=900902)
    null_fill = writer._upsert_game_info_min(conn, null_fill_game, source_sha256=source_hash)
    first = writer._upsert_game_info_min(conn, schedule, source_sha256=source_hash)
    replay = writer._upsert_game_info_min(conn, schedule, source_sha256=source_hash)
    changed_date = dict(schedule, officialDate="2026-09-24")
    date_refresh = writer._upsert_game_info_min(conn, changed_date, source_sha256=source_hash)
    bad_phase = {"game_id":900901,"game_date":"2026-09-25","source_season":2026,"source_game_type":"P","season_phase":"POSTSEASON","postseason_round":None,"season_name":"2026 Postseason","source_round":None,"schedule_relationships":{},"game_type_source_sha256":"b"*64}
    conn.execute("SAVEPOINT game_phase_conflict")
    try:
        conn.execute("SELECT mlb.safe_upsert_game_info(%s::jsonb)", (json.dumps(bad_phase,separators=(",",":")),))
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT game_phase_conflict")
        conn.execute("RELEASE SAVEPOINT game_phase_conflict")
        phase_conflict = exc.sqlstate == "23514"
        _record(results, "game_phase_conflict_denied_by_entrypoint", "PASS" if phase_conflict else "FAIL", sqlstate=exc.sqlstate)
    else:
        conn.execute("ROLLBACK TO SAVEPOINT game_phase_conflict")
        conn.execute("RELEASE SAVEPOINT game_phase_conflict")
        _record(results, "game_phase_conflict_denied_by_entrypoint", "FAIL", detail="phase conflict was accepted")
    conn.execute("RESET ROLE")
    game_state = conn.execute("SELECT game_date,source_game_type,home_team_id,home_team_abbr FROM mlb.game_info WHERE game_id=900901").fetchone()
    game_state_values = (str(game_state[0]), *game_state[1:]) if game_state else None
    null_fill_state = conn.execute("SELECT game_time IS NOT NULL,home_team_id,away_team_id,home_team_abbr,away_team_abbr FROM mlb.game_info WHERE game_id=900902").fetchone()
    _record(results, "game_info_null_fill_official_date_refresh_and_replay", "PASS" if null_fill == 1 and all(null_fill_state) and first == 1 and replay == 1 and date_refresh == 1 and game_state_values == ("2026-09-24","R",147,"NYY") else "FAIL", writes=[null_fill,first,replay,date_refresh], observed=list(game_state_values or ()), null_fill_row=list(null_fill_state or ()))

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
            "prop_features_precomputed": "fixed derived-feature EXECUTE only; no direct table DML",
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
            "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE],)
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
        conn.execute("INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, opponent, is_home, position, at_bats, hits, total_bases, singles, is_starter) VALUES (101,900101,'2026-09-23','NYY','BOS',true,'LF',3,2,2,2,false)")
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
                 has_function_privilege(%s,'mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text)','EXECUTE')""",
            (WRITER_ROLE,) * 10,
        ).fetchone()
        expected_grants = (False, False, False, False, False, True, True, True, True, True)
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
        _run_adjacent_writer_tests(conn, report["results"])

        report["phase"] = "migration_rollback"
        conn.execute("RESET ROLE")
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
            "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE],)
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
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([WRITER_ROLE, CORRECTION_ROLE],)
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
