"""Explicit, process opt-in read-only connections for MLB research consumers."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event, text

MLB_RESEARCH_DATABASE_URL_ENV = "MLB_RESEARCH_DATABASE_URL"
MLB_RESEARCH_LOGIN = "proppadia_mlb_research_login"
MLB_RESEARCH_ROLE = "proppadia_mlb_research_ro"


def research_database_url(fallback_url: str | None = None) -> str:
    """Return the opt-in research URL or the caller's unchanged current route."""
    research_url = os.environ.get(MLB_RESEARCH_DATABASE_URL_ENV, "").strip()
    if research_url:
        return research_url
    if fallback_url:
        return fallback_url
    raise RuntimeError(f"{MLB_RESEARCH_DATABASE_URL_ENV} is not configured")


def _assert_research_identity(connection) -> None:
    identity = connection.execute(
        text(
            """SELECT session_user::text AS session_user,
                      current_user::text AS current_user,
                      pg_has_role(current_user, :role_name, 'USAGE') AS role_inherited"""
        ),
        {"role_name": MLB_RESEARCH_ROLE},
    ).mappings().one()
    if (
        identity["session_user"] != MLB_RESEARCH_LOGIN
        or identity["current_user"] != MLB_RESEARCH_LOGIN
        or not identity["role_inherited"]
    ):
        raise RuntimeError("MLB research connection identity or inherited role mismatch")


def configure_mlb_research_dbapi_session(conn) -> None:
    """Make an opted-in DB-API session read-only and verify its research identity.

    Works with psycopg2 and psycopg default tuple rows. Closes the connection
    before raising so callers never keep an unverified session.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            cur.execute(
                "SELECT session_user::text, current_user::text, "
                "pg_has_role(current_user, %s, 'USAGE')",
                (MLB_RESEARCH_ROLE,),
            )
            session_user, current_user, role_inherited = cur.fetchone()
        conn.commit()
    except Exception:
        conn.close()
        raise
    if (
        session_user != MLB_RESEARCH_LOGIN
        or current_user != MLB_RESEARCH_LOGIN
        or not role_inherited
    ):
        conn.close()
        raise RuntimeError("MLB research connection identity or inherited role mismatch")


def create_mlb_research_readonly_engine(fallback_url: str | None = None, **kwargs):
    """Create an engine whose opted-in transactions are read-only and identity checked."""
    research_url = os.environ.get(MLB_RESEARCH_DATABASE_URL_ENV, "").strip()
    url = research_url or fallback_url
    if not url:
        raise RuntimeError(f"{MLB_RESEARCH_DATABASE_URL_ENV} is not configured")
    engine = create_engine(url, **kwargs)
    if research_url:
        @event.listens_for(engine, "begin")
        def _research_transaction_begin(connection) -> None:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            _assert_research_identity(connection)
    return engine


@contextmanager
def mlb_research_readonly_connection(fallback_url: str | None = None) -> Iterator[object]:
    """Yield a psycopg connection with a checked read-only transaction when opted in."""
    research_url = os.environ.get(MLB_RESEARCH_DATABASE_URL_ENV, "").strip()
    url = research_url or fallback_url
    if not url:
        raise RuntimeError(f"{MLB_RESEARCH_DATABASE_URL_ENV} is not configured")
    try:
        import psycopg
        import psycopg.rows
    except ImportError as exc:  # pragma: no cover - depends on runtime environment
        raise RuntimeError("psycopg is required for the MLB research connection") from exc
    conn = psycopg.connect(
        url,
        row_factory=psycopg.rows.dict_row,
        prepare_threshold=None,
        options="-c search_path=mlb,public",
    )
    try:
        if research_url:
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
        yield conn
    finally:
        conn.close()
