"""Shared psycopg helpers for repository query modules."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg
    import psycopg.rows
except Exception:  # pragma: no cover - environment-dependent import
    psycopg = None  # type: ignore

from backend.supabase.supabase_utils import get_database_url


MLB_INGEST_DATABASE_URL_ENV = "MLB_INGEST_DATABASE_URL"
MLB_INGEST_OPT_IN_ENV = "MLB_USE_RESTRICTED_INGEST"
MLB_INGEST_LOGIN = "proppadia_mlb_ingest_login"
MLB_INGEST_ROLE = "proppadia_mlb_ingest"
MLB_RESEARCH_DATABASE_URL_ENV = "MLB_RESEARCH_DATABASE_URL"
MLB_RESEARCH_LOGIN = "proppadia_mlb_research_login"
MLB_RESEARCH_ROLE = "proppadia_mlb_research_ro"

_MLB_INGEST_FUNCTIONS = (
    "safe_upsert_player_stats",
    "safe_fill_player_stats_at_bats",
    "safe_upsert_model_training_props",
    "safe_fill_player_stats_pa",
    "safe_upsert_player_roster",
    "safe_insert_player_id_if_absent",
    "safe_mark_player_roster_inactive",
    "safe_upsert_game_info",
    "safe_upsert_prop_features_precomputed",
    "safe_fill_prediction_backfill",
)

_MLB_INGEST_PROTECTED_RELATIONS = (
    "player_stats",
    "model_training_props",
    "player_derived_stats",
    "player_ids",
    "game_info",
    "prop_features_precomputed",
    "player_streak_profiles",
)


def scalar_int_result(row: Any, column: str = "result_count") -> int:
    """Read one integer result from psycopg mapping or tuple rows.

    Fixed PostgreSQL function calls should alias their scalar output to
    ``result_count``. Missing rows or mapping keys are contract failures rather
    than silent zero-write results.
    """
    if row is None:
        raise RuntimeError("database scalar result returned no row")
    if isinstance(row, Mapping):
        if column not in row:
            raise RuntimeError(
                f"database scalar result is missing expected column {column!r}"
            )
        value = row[column]
    else:
        try:
            value = row[0]
        except (IndexError, KeyError, TypeError):
            raise RuntimeError("database scalar result has an invalid row shape") from None
    return int(value or 0)


def _sqlstate(exc: BaseException) -> Optional[str]:
    state = getattr(exc, "sqlstate", None)
    if state:
        return str(state)
    diag = getattr(exc, "diag", None)
    state = getattr(diag, "sqlstate", None)
    return str(state) if state else None


def _restricted_connection_url() -> Optional[str]:
    """Return the canary URL only after an explicit per-process opt-in."""
    opt_in = os.environ.get(MLB_INGEST_OPT_IN_ENV)
    if opt_in is None:
        return None
    if opt_in != "1":
        raise RuntimeError("MLB restricted connection opt-in must be exactly 1 or unset")
    url = os.environ.get(MLB_INGEST_DATABASE_URL_ENV, "").strip()
    if not url:
        raise RuntimeError("MLB restricted connection opt-in is set but its URL is missing")
    return url


def mlb_ingest_restricted_mode_enabled() -> bool:
    """Whether this process explicitly opted into the restricted writer route."""
    return _restricted_connection_url() is not None


def verify_mlb_ingest_production_target(url: str) -> None:
    """Pin the canary override to the production project before connecting."""
    # Reuse the repository's already reviewed production-reference parser and
    # pin rather than duplicating either value in the shared DB helper.
    from backend.mlb.scripts.create_mlb_schema_acl_backup import (
        EXPECTED_PROJECT_REF,
        project_ref,
    )

    if project_ref(url) != EXPECTED_PROJECT_REF:
        raise RuntimeError("restricted MLB database project reference mismatch")


def configure_mlb_ingest_connection(
    conn, *, require_streak_read: bool = False
) -> Dict[str, str]:
    """Assume and verify the restricted role on this exact writer connection.

    The caller must close the connection if this raises. No diagnostic includes
    a DSN, SQL parameter, or server-provided error message.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET ROLE {MLB_INGEST_ROLE}")
            cur.execute(
                """
                WITH expected_functions(name) AS (
                    SELECT unnest(%s::text[])
                ), function_check AS (
                    SELECT count(*) = %s
                       AND coalesce(bool_and(
                           p.prosecdef
                           AND pg_get_userbyid(p.proowner) = 'postgres'
                           AND has_function_privilege(current_user, p.oid, 'EXECUTE')
                       ), false) AS ok
                    FROM expected_functions f
                    LEFT JOIN pg_proc p
                      ON p.pronamespace = 'mlb'::regnamespace
                     AND p.proname = f.name
                ), membership_check AS (
                    SELECT count(*) = 1
                       AND coalesce(bool_and(
                           granted.rolname = %s AND NOT m.admin_option
                       ), false) AS ok
                    FROM pg_auth_members m
                    JOIN pg_roles member ON member.oid = m.member
                    JOIN pg_roles granted ON granted.oid = m.roleid
                    WHERE member.rolname = session_user
                ), destructive_check AS (
                    SELECT count(*) = %s
                       AND coalesce(bool_and(
                           NOT has_table_privilege(current_user, c.oid, 'DELETE')
                           AND NOT has_table_privilege(current_user, c.oid, 'TRUNCATE')
                       ), false) AS ok
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'mlb' AND c.relname = ANY(%s::text[])
                ), metadata_check AS (
                    SELECT NOT has_table_privilege(current_user, 'mlb.model_training_props', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'prop_value', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'line', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'over_under', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'outcome', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'status', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'result', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'was_correct', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'predicted_outcome', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'confidence_score', 'UPDATE')
                       AND NOT has_column_privilege(current_user, 'mlb.model_training_props', 'corrected_result', 'UPDATE')
                       AND NOT EXISTS (
                           SELECT 1 FROM pg_attribute a
                           WHERE a.attrelid = 'mlb.model_training_props'::regclass
                             AND a.attnum > 0 AND NOT a.attisdropped
                             AND has_column_privilege(current_user, a.attrelid, a.attname, 'UPDATE')
                             AND a.attname <> ALL(ARRAY[
                                 'team','opponent','team_id','opponent_team_id','opponent_encoded',
                                 'is_home','updated_at','game_date','game_time','game_day_of_week',
                                 'time_of_day_bucket','streak_type','streak_count',
                                 'rolling_result_avg_7','game_type'
                             ]::name[])
                       )
                       AS ok
                ), adjacent_write_check AS (
                    SELECT count(*) = 3
                       AND coalesce(bool_and(
                        NOT has_any_column_privilege(current_user, c.oid, 'INSERT')
                        AND NOT has_any_column_privilege(current_user, c.oid, 'UPDATE')
                    ), false) AS ok
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'mlb'
                      AND c.relname IN ('player_ids','game_info','prop_features_precomputed')
                ), streak_read_check AS (
                    SELECT count(*) = 6
                       AND coalesce(bool_and(
                           has_column_privilege(current_user, 'mlb.player_streak_profiles', column_name, 'SELECT')
                       ), false)
                       AND NOT has_any_column_privilege(current_user, 'mlb.player_streak_profiles', 'INSERT')
                       AND NOT has_any_column_privilege(current_user, 'mlb.player_streak_profiles', 'UPDATE')
                       AND NOT has_table_privilege(current_user, 'mlb.player_streak_profiles', 'DELETE')
                       AND NOT has_table_privilege(current_user, 'mlb.player_streak_profiles', 'TRUNCATE')
                       AND EXISTS (
                           SELECT 1 FROM pg_policies
                           WHERE schemaname = 'mlb'
                             AND tablename = 'player_streak_profiles'
                             AND cmd = 'SELECT'
                             AND roles @> ARRAY['proppadia_mlb_ingest']::name[]
                       ) AS ok
                    FROM unnest(ARRAY[
                        'player_id','prop_type','prop_source','last_game_date','streak_type','streak_count'
                    ]::name[]) AS required(column_name)
                ), login_check AS (
                    SELECT coalesce(bool_and(
                        rolcanlogin AND NOT rolinherit AND NOT rolsuper
                        AND NOT rolcreatedb AND NOT rolcreaterole
                        AND NOT rolreplication AND NOT rolbypassrls
                    ), false) AS ok
                    FROM pg_roles WHERE rolname = session_user
                ), group_check AS (
                    SELECT coalesce(bool_and(
                        NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper
                        AND NOT rolcreatedb AND NOT rolcreaterole
                        AND NOT rolreplication AND NOT rolbypassrls
                    ), false) AS ok
                    FROM pg_roles WHERE rolname = current_user
                ), group_isolation_check AS (
                    SELECT NOT EXISTS (
                        SELECT 1 FROM pg_auth_members m
                        WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)
                    ) AS ok
                )
                SELECT session_user::text AS session_user,
                       current_user::text AS current_user,
                       (SELECT ok FROM login_check) AS login_ok,
                       (SELECT ok FROM group_check) AS group_ok,
                       (SELECT ok FROM group_isolation_check) AS group_isolated,
                       (SELECT ok FROM membership_check) AS membership_ok,
                       has_schema_privilege(current_user, 'mlb', 'USAGE') AS schema_usage,
                       NOT has_schema_privilege(current_user, 'mlb', 'CREATE') AS schema_create_denied,
                       (SELECT ok FROM function_check) AS functions_ok,
                       NOT has_any_column_privilege(current_user, 'mlb.player_stats', 'UPDATE') AS source_update_denied,
                       NOT has_any_column_privilege(current_user, 'mlb.player_stats', 'INSERT') AS source_insert_denied,
                       NOT has_any_column_privilege(current_user, 'mlb.model_training_props', 'INSERT') AS training_insert_denied,
                       (SELECT ok FROM metadata_check) AS outcomes_protected,
                       (SELECT ok FROM destructive_check) AS destructive_denied,
                       (SELECT ok FROM adjacent_write_check) AS adjacent_direct_writes_denied,
                       (NOT %s OR (SELECT ok FROM streak_read_check)) AS streak_read_contract
                """,
                (
                    list(_MLB_INGEST_FUNCTIONS),
                    len(_MLB_INGEST_FUNCTIONS),
                    MLB_INGEST_ROLE,
                    len(_MLB_INGEST_PROTECTED_RELATIONS),
                    list(_MLB_INGEST_PROTECTED_RELATIONS),
                    bool(require_streak_read),
                ),
            )
            row = cur.fetchone()
        if isinstance(row, dict):
            required_keys = (
                "session_user", "current_user", "login_ok", "group_ok",
                "group_isolated", "membership_ok", "schema_usage", "schema_create_denied", "functions_ok",
                "source_update_denied", "source_insert_denied",
                "training_insert_denied", "outcomes_protected", "destructive_denied",
                "adjacent_direct_writes_denied", "streak_read_contract",
            )
            missing = [key for key in required_keys if key not in row]
            if missing:
                raise RuntimeError(
                    "restricted MLB role assertion is missing result fields: "
                    + ", ".join(missing)
                )
            values = [row[key] for key in required_keys]
        else:
            values = list(row or ())
        if len(values) != 16:
            raise RuntimeError("restricted MLB role assertion returned an invalid result")
        session_user, current_user, *checks = values
        if session_user != MLB_INGEST_LOGIN or current_user != MLB_INGEST_ROLE or not all(checks):
            raise RuntimeError("restricted MLB role identity or permission assertion failed")
        return {"session_user": str(session_user), "current_user": str(current_user)}
    except RuntimeError:
        raise
    except Exception as exc:
        state = _sqlstate(exc)
        suffix = f" (SQLSTATE {state})" if state else f" ({type(exc).__name__})"
        raise RuntimeError(f"restricted MLB role activation or verification failed{suffix}") from None


def mlb_ingest_session_identity(conn) -> Dict[str, str]:
    """Return receipt-safe role identity from the connection used for writes."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT session_user::text, current_user::text")
            row = cur.fetchone()
        if isinstance(row, dict):
            session_user = row.get("session_user")
            current_user = row.get("current_user")
        else:
            session_user, current_user = tuple(row or ())
        if session_user != MLB_INGEST_LOGIN or current_user != MLB_INGEST_ROLE:
            raise RuntimeError("restricted MLB role identity assertion failed")
        return {"session_user": str(session_user), "current_user": str(current_user)}
    except RuntimeError:
        raise
    except Exception as exc:
        state = _sqlstate(exc)
        suffix = f" (SQLSTATE {state})" if state else f" ({type(exc).__name__})"
        raise RuntimeError(f"restricted MLB role identity read failed{suffix}") from None


def _db_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _connect(*, require_streak_read: bool = False):
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    restricted_url = _restricted_connection_url()
    if restricted_url is not None:
        verify_mlb_ingest_production_target(restricted_url)
    url = restricted_url or _db_url()
    # Prefer MLB schema for unqualified MLB table names after public-view cleanup.
    try:
        conn = psycopg.connect(
            url,
            row_factory=psycopg.rows.dict_row,
            prepare_threshold=None,
            options="-c search_path=mlb,public",
        )
    except Exception as exc:
        if restricted_url is None:
            raise
        state = _sqlstate(exc)
        suffix = f" (SQLSTATE {state})" if state else f" ({type(exc).__name__})"
        raise RuntimeError(f"restricted MLB database connection failed{suffix}") from None
    if restricted_url is not None:
        try:
            configure_mlb_ingest_connection(conn, require_streak_read=require_streak_read)
        except Exception:
            conn.close()
            raise
    return conn


def pg_connect(*, require_streak_read: bool = False):
    return _connect(require_streak_read=require_streak_read)


def pg_fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall() or [])


def pg_fetchall_mlb_research_readonly(
    sql: str, params: Sequence[Any] = ()
) -> List[Dict[str, Any]]:
    """Use the process opt-in research route, retaining the legacy fallback."""
    research_url = os.environ.get(MLB_RESEARCH_DATABASE_URL_ENV, "").strip()
    if not research_url:
        return pg_fetchall(sql, params)
    if psycopg is None:
        raise RuntimeError("psycopg is required for the MLB research connection")
    conn = psycopg.connect(
        research_url,
        row_factory=psycopg.rows.dict_row,
        prepare_threshold=None,
        options="-c search_path=mlb,public",
    )
    try:
        conn.execute("BEGIN READ ONLY")
        identity = conn.execute(
            "SELECT session_user::text AS session_user, current_user::text AS current_user, "
            "pg_has_role(current_user, %s, 'USAGE') AS role_inherited",
            (MLB_RESEARCH_ROLE,),
        ).fetchone()
        if (
            identity["session_user"] != MLB_RESEARCH_LOGIN
            or identity["current_user"] != MLB_RESEARCH_LOGIN
            or not identity["role_inherited"]
        ):
            raise RuntimeError("MLB research connection identity or inherited role mismatch")
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])
    finally:
        conn.close()


def pg_fetchone(sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def pg_execute(sql: str, params: Sequence[Any] = ()) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
