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
from typing import Any, Callable
from urllib.parse import unquote, urlsplit

import psycopg

from backend.mlb.scripts import insert_mlb_stat_derived as writer


EXPECTED_PROJECT_REF = "vbnzmvmhuwvpkijxeuyq"
WRITER_ROLE = "proppadia_mlb_fixture_writer_20261016"
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


def _expect_writer_error(
    conn: psycopg.Connection,
    name: str,
    operation: Callable[[], Any],
    results: list[dict[str, Any]],
) -> None:
    conn.execute("SAVEPOINT expected_writer_error")
    try:
        operation()
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT expected_writer_error")
        conn.execute("RELEASE SAVEPOINT expected_writer_error")
        _record(
            results,
            name,
            "BLOCKED" if exc.sqlstate == "42501" else "FAIL",
            sqlstate=exc.sqlstate,
            exception=type(exc).__name__,
            column=getattr(getattr(exc, "diag", None), "column_name", None),
        )
        return
    conn.execute("ROLLBACK TO SAVEPOINT expected_writer_error")
    conn.execute("RELEASE SAVEPOINT expected_writer_error")
    _record(results, name, "FAIL", detail="writer unexpectedly succeeded without protected UPDATE grants")


def _expect_allowed_and_rollback(
    conn: psycopg.Connection,
    name: str,
    sql: str,
    results: list[dict[str, Any]],
) -> None:
    conn.execute("SAVEPOINT expected_allowed_probe")
    try:
        conn.execute(sql)
    except psycopg.Error as exc:
        conn.execute("ROLLBACK TO SAVEPOINT expected_allowed_probe")
        conn.execute("RELEASE SAVEPOINT expected_allowed_probe")
        _record(results, name, "FAIL", sqlstate=exc.sqlstate, exception=type(exc).__name__)
        return
    conn.execute("ROLLBACK TO SAVEPOINT expected_allowed_probe")
    conn.execute("RELEASE SAVEPOINT expected_allowed_probe")
    _record(results, name, "PASS", detail="operation permitted by the temporary test grant; probe rolled back")


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
    conn.execute(f"CREATE ROLE {WRITER_ROLE} NOLOGIN NOINHERIT NOCREATEROLE NOCREATEDB")
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
    conn.execute(
        f"""CREATE TABLE mlb.player_derived_stats (
            player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date NOT NULL,
            team text, is_home boolean, {metric_columns}, updated_at timestamptz,
            PRIMARY KEY(player_id, game_id), UNIQUE(player_id, game_date)
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
            streak_count integer, rolling_result_avg_7 numeric,
            UNIQUE(player_id, game_id, prop_type, prop_source)
        )
        """
    )
    conn.execute(f"GRANT USAGE ON SCHEMA mlb TO {WRITER_ROLE}")
    conn.execute(
        f"GRANT SELECT ON mlb.player_stats, mlb.player_derived_stats, mlb.model_training_props TO {WRITER_ROLE}"
    )
    conn.execute(
        f"GRANT INSERT ON mlb.player_stats, mlb.player_derived_stats, mlb.model_training_props TO {WRITER_ROLE}"
    )
    # Permit derived feature recomputation, while keeping source observations
    # and established training outcomes outside the ordinary UPDATE grant.
    derived_updates = ["game_id", "game_date", "team", "is_home", "updated_at"]
    derived_updates.extend(f"{prefix}_{metric}" for prefix in ("d7", "d15", "d30") for metric in writer.ROLLING_METRICS)
    conn.execute(
        f"GRANT UPDATE ({', '.join(derived_updates)}) ON mlb.player_derived_stats TO {WRITER_ROLE}"
    )
    conn.execute(
        f"GRANT UPDATE (game_date, team, opponent, is_home, position, is_starter) ON mlb.player_stats TO {WRITER_ROLE}"
    )
    conn.execute(
        f"GRANT UPDATE (team, opponent, team_id, opponent_team_id, opponent_encoded, is_home, updated_at, game_date, game_time, game_day_of_week, time_of_day_bucket, streak_type, streak_count, rolling_result_avg_7) ON mlb.model_training_props TO {WRITER_ROLE}"
    )


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
    conn.execute(
        "INSERT INTO mlb.player_stats (player_id, game_id, game_date, at_bats, hits) VALUES (103,900103,'2026-09-23',2,1)"
    )
    _record(results, "plain_source_insert_with_restricted_grant", "PASS")
    conn.execute(
        "INSERT INTO mlb.model_training_props (id, game_id, player_id, prop_type, prop_source) VALUES ('00000000-0000-0000-0000-000000900104',900104,453286,'hits','fixture')"
    )
    _record(results, "plain_training_insert_with_restricted_grant", "PASS")
    _expect_denied(conn, "source_populated_update_denied", "UPDATE mlb.player_stats SET hits = 99 WHERE player_id = 101", results)
    _expect_denied(conn, "training_outcome_update_denied", "UPDATE mlb.model_training_props SET outcome = 'loss' WHERE player_id = 453286", results)
    _expect_denied(conn, "delete_denied", "DELETE FROM mlb.player_stats WHERE player_id = 101", results)
    _expect_denied(conn, "truncate_denied", "TRUNCATE mlb.player_stats", results)
    _expect_denied(conn, "correction_role_switch_denied", f"SET ROLE {CORRECTION_ROLE}", results)
    _expect_denied(conn, "role_grant_escalation_denied", f"GRANT {CORRECTION_ROLE} TO {WRITER_ROLE}", results)

    source_candidate = _source_row(101, 900101)
    _expect_writer_error(
        conn,
        "actual_player_stats_upsert_under_restricted_grants",
        lambda: writer._upsert_player_stats_row(conn, source_candidate),
        results,
    )
    training_candidate = _training_row(900101)
    _expect_writer_error(
        conn,
        "actual_training_upsert_under_restricted_grants",
        lambda: writer._upsert_training_row(conn, training_candidate),
        results,
    )

    # Derived data is explicitly recomputable. The allowed derived UPDATE
    # path is exercised and repeated, then checked against an identity collision.
    first = writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    second = writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    _record(results, "derived_recompute_and_replay", "PASS" if first > 0 and second == 0 else "FAIL", first=first, replay=second)
    conn.execute(
        "INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, is_home) VALUES (102, 900102, '2026-09-23', 'NYY', true)"
    )
    conn.execute(
        "INSERT INTO mlb.player_derived_stats (player_id, game_id, game_date, team, is_home) VALUES (102, 900099, '2026-09-23', 'NYY', true)"
    )
    try:
        writer._refresh_player_derived_stats(conn, "2026-09-23", "2026-09-23")
    except writer.LegacyDerivedIdentityCollision as exc:
        _record(results, "derived_identity_collision_preserved", "PASS", category=type(exc).__name__)
    else:
        _record(results, "derived_identity_collision_preserved", "FAIL", detail="collision was not rejected")


def _run_application_logic_with_fixture_only_update(conn: psycopg.Connection, results: list[dict[str, Any]]) -> None:
    conn.execute("RESET ROLE")
    conn.execute(f"GRANT UPDATE ON mlb.player_stats TO {WRITER_ROLE}")
    conn.execute(f"GRANT UPDATE ON mlb.model_training_props TO {WRITER_ROLE}")
    conn.execute(f"SET ROLE {WRITER_ROLE}")

    inserted = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    replay = writer._upsert_player_stats_row(conn, _source_row(201, 900201, hits=2))
    _record(results, "player_stats_insert_and_identical_replay_with_update_grant", "PASS" if inserted == 1 and replay == 0 else "FAIL", inserted=int(inserted), replay=int(replay))

    conn.execute("RESET ROLE")
    conn.execute(
        "INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, opponent, is_home, position, hits, total_bases) VALUES (202, 900202, '2026-09-23', 'NYY', 'BOS', true, 'LF', NULL, NULL)"
    )
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    fill = writer._upsert_player_stats_row(conn, _source_row(202, 900202, hits=2))
    conflict = writer._upsert_player_stats_row(conn, _source_row(202, 900202, hits=3))
    values = conn.execute("SELECT hits, total_bases FROM mlb.player_stats WHERE player_id=202 AND game_id=900202").fetchone()
    _record(
        results,
        "player_stats_null_fill_and_conflict_preservation_with_update_grant",
        "PASS" if fill == 1 and conflict.conflicts and values == (2, 2) else "FAIL",
        fill=int(fill), conflicting_fields=list(conflict.conflicts[0]["conflicting_fields"]) if conflict.conflicts else [],
        canonical_values=list(values) if values else None,
    )
    _expect_allowed_and_rollback(conn, "source_direct_overwrite_bypasses_application_guard_with_broad_grant", "UPDATE mlb.player_stats SET hits = 99 WHERE player_id=202 AND game_id=900202", results)

    mtp_insert = writer._upsert_training_row(conn, _training_row(900301))
    mtp_replay = writer._upsert_training_row(conn, _training_row(900301))
    mtp_conflict = writer._upsert_training_row(conn, _training_row(900301, outcome="loss"))
    result_tuple = conn.execute("SELECT prop_value, line, over_under, outcome, status, was_correct FROM mlb.model_training_props WHERE game_id=900301").fetchone()
    _record(
        results,
        "training_outcome_insert_replay_and_conflict_preservation_with_update_grant",
        "PASS" if mtp_insert == 1 and mtp_replay == 0 and mtp_conflict.conflicts and result_tuple == (1, 0.5, "over", "win", "resolved", True) else "FAIL",
        inserted=int(mtp_insert), replay=int(mtp_replay), conflict=bool(mtp_conflict.conflicts),
        canonical_result=list(result_tuple) if result_tuple else None,
    )
    _expect_allowed_and_rollback(conn, "training_outcome_direct_overwrite_bypasses_application_guard_with_broad_grant", "UPDATE mlb.model_training_props SET outcome = 'loss' WHERE game_id=900301", results)

    conn.execute("RESET ROLE")
    conn.execute(f"REVOKE UPDATE ON mlb.player_stats, mlb.model_training_props FROM {WRITER_ROLE}")
    conn.execute(
        f"GRANT UPDATE (game_date, team, opponent, is_home, position, is_starter) ON mlb.player_stats TO {WRITER_ROLE}"
    )
    conn.execute(
        f"GRANT UPDATE (team, opponent, team_id, opponent_team_id, opponent_encoded, is_home, updated_at, game_date, game_time, game_day_of_week, time_of_day_bucket, streak_type, streak_count, rolling_result_avg_7) ON mlb.model_training_props TO {WRITER_ROLE}"
    )
    conn.execute(f"SET ROLE {WRITER_ROLE}")
    _expect_denied(conn, "source_populated_update_denied_after_protected_columns_revoked", "UPDATE mlb.player_stats SET hits = 99 WHERE player_id=202 AND game_id=900202", results)
    _expect_denied(conn, "training_outcome_update_denied_after_result_columns_revoked", "UPDATE mlb.model_training_props SET outcome = 'loss' WHERE game_id=900301", results)


def run() -> dict[str, Any]:
    dsn = os.environ.get("NHL_PP_TOI_TEST_DSN")
    parsed = urlsplit(dsn or "")
    username = unquote(parsed.username or "")
    reference_match = bool(
        dsn
        and (
            EXPECTED_PROJECT_REF in (parsed.hostname or "")
            or username.endswith("." + EXPECTED_PROJECT_REF)
            or username.endswith(EXPECTED_PROJECT_REF)
        )
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
            "player_stats": "SELECT + INSERT + UPDATE metadata only; no UPDATE to official stat columns",
            "player_derived_stats": "SELECT + INSERT + UPDATE derived columns; no DELETE/TRUNCATE",
            "model_training_props": "SELECT + INSERT + UPDATE metadata only; no UPDATE to protected result tuple",
            "correction_role": "NOLOGIN; no membership granted to writer",
        },
        "conflict_update_privilege_requirements": {
            "player_stats": list(PLAYER_STATS_FIELDS),
            "model_training_props": [
                "team", "opponent", "team_id", "opponent_team_id", "opponent_encoded", "is_home",
                *PROTECTED_RESULT_FIELDS, "updated_at", "game_date", "game_time", "game_day_of_week",
                "time_of_day_bucket", "streak_type", "streak_count",
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
        _create_fixture(conn)
        conn.execute("INSERT INTO mlb.player_stats (player_id, game_id, game_date, team, opponent, is_home, position, at_bats, hits, total_bases, singles, is_starter) VALUES (101,900101,'2026-09-23','NYY','BOS',true,'LF',3,2,2,2,false)")

        _run_role_tests(conn, report["results"])
        _run_application_logic_with_fixture_only_update(conn, report["results"])

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
        report.update({
            "fixture": "FAIL",
            "status": "BLOCKED",
            "sanitized_exception": type(exc).__name__,
            "sqlstate": getattr(exc, "sqlstate", None),
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
