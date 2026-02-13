"""Shared psycopg helpers for repository query modules."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg
    import psycopg.rows
except Exception:  # pragma: no cover - environment-dependent import
    psycopg = None  # type: ignore

from backend.supabase.supabase_utils import get_database_url


def _db_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _connect():
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    return psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=None)


def pg_fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall() or [])


def pg_fetchone(sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def pg_execute(sql: str, params: Sequence[Any] = ()) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
