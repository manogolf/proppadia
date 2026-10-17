"""Rollback-only PostgreSQL proof for restricted MLB today-MV refresh."""

from __future__ import annotations

import json
import os
import statistics
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit

import psycopg
from psycopg import errors


PROJECT_REF = "vbnzmvmhuwvpkijxeuyq"
FIXTURE_ROLE = "proppadia_mlb_ingest"
FUNCTION = "mlb.refresh_today_workspace_views()"
ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/mlb/sql/migrations/20261017_mlb_today_workspace_restricted_refresh.sql"
VIEWS = (
    "today_market_snapshot",
    "today_market_timing_signal",
    "today_player_context",
    "today_workspace_mlb",
)


def _dsn() -> str:
    dsn = os.environ.get("NHL_PP_TOI_TEST_DSN", "")
    parsed = urlsplit(dsn)
    host = (parsed.hostname or "").lower()
    user = unquote(parsed.username or "")
    direct = host == f"db.{PROJECT_REF}.supabase.co"
    pooler = host.endswith(".pooler.supabase.com") and user == f"postgres.{PROJECT_REF}"
    if not (direct or pooler):
        raise RuntimeError("NHL_PP_TOI_TEST_DSN is not the independently allowlisted preview")
    return dsn


def _expect_denied(conn: psycopg.Connection, label: str, sql: str) -> str:
    try:
        with conn.transaction():
            conn.execute(sql)
    except psycopg.Error as exc:
        if exc.sqlstate != "42501":
            raise AssertionError(f"{label}: expected 42501, got {exc.sqlstate}") from exc
        return exc.sqlstate
    raise AssertionError(f"{label}: operation unexpectedly succeeded")


def _create_fixture(conn: psycopg.Connection) -> None:
    conn.execute(f"CREATE ROLE {FIXTURE_ROLE} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT")
    conn.execute(f"GRANT {FIXTURE_ROLE} TO postgres")
    conn.execute("CREATE SCHEMA mlb")
    conn.execute(f"GRANT USAGE ON SCHEMA mlb TO {FIXTURE_ROLE}")
    conn.execute("CREATE TABLE mlb.refresh_fixture_source (lane text PRIMARY KEY, value integer NOT NULL)")
    conn.execute("INSERT INTO mlb.refresh_fixture_source VALUES ('snapshot',1),('timing',1),('player',1)")

    definitions = {
        "today_market_snapshot": "SELECT value FROM mlb.refresh_fixture_source WHERE lane='snapshot'",
        "today_market_timing_signal": "SELECT value FROM mlb.refresh_fixture_source WHERE lane='timing'",
        "today_player_context": "SELECT value FROM mlb.refresh_fixture_source WHERE lane='player'",
        "today_workspace_mlb": (
            "SELECT s.value AS id, s.value+t.value+p.value AS combined "
            "FROM mlb.today_market_snapshot s "
            "CROSS JOIN mlb.today_market_timing_signal t "
            "CROSS JOIN mlb.today_player_context p"
        ),
    }
    for view_name, query in definitions.items():
        conn.execute(f"CREATE MATERIALIZED VIEW mlb.{view_name} AS {query}")
        key = "id" if view_name == "today_workspace_mlb" else "value"
        conn.execute(f"CREATE UNIQUE INDEX {view_name}_fixture_key ON mlb.{view_name} ({key})")

    conn.execute(MIGRATION.read_text(), prepare=False)


def _cleanup_fixture(dsn: str) -> tuple[bool, bool]:
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        with conn.transaction():
            conn.execute("DROP SCHEMA IF EXISTS mlb CASCADE")
            conn.execute(f"REVOKE {FIXTURE_ROLE} FROM postgres")
            conn.execute(f"DROP ROLE IF EXISTS {FIXTURE_ROLE}")
        residue = conn.execute(
            """SELECT to_regnamespace('mlb') IS NULL,
                      NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)""",
            (FIXTURE_ROLE,),
        ).fetchone()
        return residue
    finally:
        conn.close()


def _lock_probe(dsn: str, *, concurrent: bool) -> dict[str, object]:
    refresher = psycopg.connect(dsn, connect_timeout=10)
    try:
        if concurrent:
            refresher.execute(f"SELECT {FUNCTION}")
        else:
            for view_name in VIEWS:
                refresher.execute(f"REFRESH MATERIALIZED VIEW mlb.{view_name}")

        lock_mode = refresher.execute(
            """SELECT mode FROM pg_locks
               WHERE pid=pg_backend_pid()
                 AND relation='mlb.today_market_snapshot'::regclass
                 AND granted ORDER BY mode"""
        ).fetchall()
        reader = psycopg.connect(dsn, connect_timeout=10)
        reader.execute("SET lock_timeout='150ms'")
        try:
            reader.execute("SELECT count(*) FROM mlb.today_market_snapshot").fetchone()
            read_status = "PASS"
        except errors.LockNotAvailable:
            read_status = "BLOCKED_BY_REFRESH_LOCK"
            reader.rollback()
        finally:
            reader.close()
    finally:
        refresher.rollback()
        refresher.close()
    expected = "PASS" if concurrent else "BLOCKED_BY_REFRESH_LOCK"
    if read_status != expected:
        raise AssertionError(f"reader result for concurrent={concurrent}: {read_status}")
    return {"held_lock_modes": [row[0] for row in lock_mode], "second_session_select": read_status}


def run() -> dict[str, object]:
    dsn = _dsn()
    report: dict[str, object] = {
        "preview_project_ref": PROJECT_REF,
        "fixture": "NOT_STARTED",
        "cleanup": "NOT_STARTED",
        "assertions": [],
        "refresh_timing_seconds_fixture_only": {},
    }
    fixture_committed = False
    conn: psycopg.Connection | None = None
    try:
        conn = psycopg.connect(dsn, connect_timeout=10)
        try:
            db, session_user, current_user, version, can_create_roles = conn.execute(
                """SELECT current_database(), session_user, current_user, version(),
                          (SELECT rolcreaterole FROM pg_roles WHERE rolname=current_user)"""
            ).fetchone()
            if db != "postgres" or session_user != "postgres" or current_user != "postgres":
                raise RuntimeError("preview connection identity differs from verified preview administrator")
            if "PostgreSQL 15." not in version:
                raise RuntimeError("preview server is not PostgreSQL 15")
            if not can_create_roles:
                raise RuntimeError("preview administrator cannot create the transaction-scoped fixture role")
            report.update({"database": db, "session_user": session_user, "server_version": version.split(",")[0]})

            baseline = conn.execute(
                """SELECT to_regnamespace('mlb') IS NULL,
                          NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)""",
                (FIXTURE_ROLE,),
            ).fetchone()
            if baseline != (True, True):
                raise RuntimeError("preview fixture baseline is not empty; stopped without changes")

            _create_fixture(conn)
            conn.commit()
            fixture_committed = True
            report["fixture"] = "CREATED_TEMPORARILY_IN_PREVIEW"
            with conn.transaction():
                function_row = conn.execute(
                    """SELECT pg_get_userbyid(p.proowner), p.prosecdef, p.proconfig,
                              has_function_privilege(%s,p.oid,'EXECUTE'),
                              EXISTS (SELECT 1 FROM aclexplode(p.proacl) acl
                                      WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE'),
                              pg_get_function_identity_arguments(p.oid)
                       FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                       WHERE n.nspname='mlb' AND p.proname='refresh_today_workspace_views'""",
                    (FIXTURE_ROLE,),
                ).fetchone()
                owner, security_definer, config, allowed, public_exec, args = function_row
                assert owner == "postgres" and security_definer and config == ["search_path=pg_catalog, pg_temp"]
                assert allowed and not public_exec and args == ""
                report["assertions"].append("owner_security_definer_search_path_and_execute_acl:PASS")

                conn.execute("UPDATE mlb.refresh_fixture_source SET value=2")
                conn.execute(f"SET ROLE {FIXTURE_ROLE}")
                conn.execute(f"SELECT {FUNCTION}")
                report["assertions"].append("restricted_role_executes_fixed_concurrent_refresh:PASS")
                report["assertions"].append(
                    "direct_refresh_denied_sqlstate="
                    + _expect_denied(conn, "direct refresh", "REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.today_market_snapshot")
                )
                report["assertions"].append(
                    "view_definition_change_denied_sqlstate="
                    + _expect_denied(conn, "alter matview", "ALTER MATERIALIZED VIEW mlb.today_market_snapshot RENAME TO forbidden")
                )
                report["assertions"].append(
                    "function_replacement_denied_sqlstate="
                    + _expect_denied(
                        conn,
                        "replace function",
                        f"CREATE OR REPLACE FUNCTION {FUNCTION} RETURNS void LANGUAGE sql AS 'SELECT'",
                    )
                )
                conn.execute("RESET ROLE")
                assert conn.execute("SELECT value FROM mlb.today_market_snapshot").fetchone() == (2,)
                assert conn.execute("SELECT value FROM mlb.today_market_timing_signal").fetchone() == (2,)
                assert conn.execute("SELECT value FROM mlb.today_player_context").fetchone() == (2,)
                assert conn.execute("SELECT combined FROM mlb.today_workspace_mlb").fetchone() == (6,)
                report["assertions"].append("four_views_refreshed_in_dependency_order:PASS")

                conn.execute("CREATE MATERIALIZED VIEW mlb.fixture_arbitrary_target AS SELECT 1 AS value")
                conn.execute("CREATE UNIQUE INDEX fixture_arbitrary_target_key ON mlb.fixture_arbitrary_target(value)")
                conn.execute(f"SET ROLE {FIXTURE_ROLE}")
                report["assertions"].append(
                    "arbitrary_target_direct_refresh_denied_sqlstate="
                    + _expect_denied(
                        conn,
                        "arbitrary target refresh",
                        "REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.fixture_arbitrary_target",
                    )
                )
                report["assertions"].append(
                    "caller_cannot_supply_target_sqlstate="
                    + str(
                        _call_denied(
                            conn,
                            f"SELECT mlb.refresh_today_workspace_views('mlb.fixture_arbitrary_target')",
                        )
                    )
                )
                conn.execute("RESET ROLE")

                before_failure = conn.execute(
                    "SELECT (SELECT value FROM mlb.today_market_snapshot), (SELECT value FROM mlb.today_market_timing_signal), (SELECT value FROM mlb.today_player_context)"
                ).fetchone()
                conn.execute("UPDATE mlb.refresh_fixture_source SET value=3")
                conn.execute("DROP MATERIALIZED VIEW mlb.today_workspace_mlb")
                conn.execute(f"SET ROLE {FIXTURE_ROLE}")
                failure_state = "NO_ERROR"
                try:
                    with conn.transaction():
                        conn.execute(f"SELECT {FUNCTION}")
                except psycopg.Error as exc:
                    failure_state = exc.sqlstate or "UNKNOWN"
                conn.execute("RESET ROLE")
                after_failure = conn.execute(
                    "SELECT (SELECT value FROM mlb.today_market_snapshot), (SELECT value FROM mlb.today_market_timing_signal), (SELECT value FROM mlb.today_player_context)"
                ).fetchone()
                assert failure_state == "42P01" and before_failure == after_failure == (2, 2, 2)
                conn.execute(
                    """CREATE MATERIALIZED VIEW mlb.today_workspace_mlb AS
                       SELECT s.value AS id, s.value+t.value+p.value AS combined
                       FROM mlb.today_market_snapshot s
                       CROSS JOIN mlb.today_market_timing_signal t
                       CROSS JOIN mlb.today_player_context p"""
                )
                conn.execute("CREATE UNIQUE INDEX today_workspace_mlb_fixture_key ON mlb.today_workspace_mlb(id)")
                report["assertions"].append("failure_rolls_back_prior_refreshes_sqlstate_42P01:PASS")

                timings: dict[str, list[float]] = {"ordinary": [], "concurrent": []}
                for _ in range(3):
                    started = time.perf_counter()
                    for view_name in VIEWS:
                        conn.execute(f"REFRESH MATERIALIZED VIEW mlb.{view_name}")
                    timings["ordinary"].append(time.perf_counter() - started)
                    started = time.perf_counter()
                    conn.execute(f"SELECT {FUNCTION}")
                    timings["concurrent"].append(time.perf_counter() - started)
                report["refresh_timing_seconds_fixture_only"] = {
                    name: {"samples": values, "median": statistics.median(values)}
                    for name, values in timings.items()
                }

            ordinary_lock = _lock_probe(dsn, concurrent=False)
            report["ordinary_refresh_lock"] = ordinary_lock
            concurrent_lock = _lock_probe(dsn, concurrent=True)
            report["concurrent_refresh_lock"] = concurrent_lock
            report["assertions"].append("reader_lock_behavior_matches_refresh_mode:PASS")
        finally:
            conn.rollback()
            conn.close()
            conn = None

        residue = _cleanup_fixture(dsn)
        fixture_committed = False
        assert residue == (True, True), f"fixture cleanup residue: {residue}"
        report["cleanup"] = "PASS_NO_SCHEMA_OR_ROLE_REMAINS"
        report["status"] = "PASS"
        return report
    except Exception as exc:
        try:
            if conn is not None:
                conn.rollback()
                conn.close()
                conn = None
            if fixture_committed:
                residue = _cleanup_fixture(dsn)
            else:
                check = psycopg.connect(dsn, connect_timeout=10)
                try:
                    residue = check.execute(
                        """SELECT to_regnamespace('mlb') IS NULL,
                                  NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)""",
                        (FIXTURE_ROLE,),
                    ).fetchone()
                finally:
                    check.close()
            report["cleanup"] = (
                "PASS_NO_SCHEMA_OR_ROLE_REMAINS" if residue == (True, True)
                else f"FAIL_RESIDUE_{residue}"
            )
        except Exception as cleanup_exc:
            report["cleanup"] = f"UNVERIFIED_{type(cleanup_exc).__name__}"
        report.update({"status": "FAIL", "error_type": type(exc).__name__, "error": str(exc)})
        raise RuntimeError(json.dumps(report, sort_keys=True)) from exc


def _call_denied(conn: psycopg.Connection, sql: str) -> str:
    try:
        with conn.transaction():
            conn.execute(sql)
    except psycopg.Error as exc:
        if exc.sqlstate not in {"42883", "42501"}:
            raise AssertionError(f"caller target parameter: unexpected SQLSTATE {exc.sqlstate}") from exc
        return exc.sqlstate
    raise AssertionError("caller unexpectedly supplied a target relation")


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, sort_keys=True))
