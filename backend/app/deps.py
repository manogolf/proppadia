# backend/app/deps.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any

try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore

# Single source of truth for DB URL
from backend.supabase.supabase_utils import get_database_url  # type: ignore


def pg_connect():
    """
    Return a psycopg connection with prepared statements disabled
    (avoids GH Actions pooler 'DuplicatePreparedStatement' issues).
    """
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return psycopg.connect(url, prepare_threshold=0)


def pg_fetchone(sql: str, params: tuple = ()) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Execute read-only SQL and return a single row as dict.
    (ok, row, err) — row is None when no rows.
    """
    if psycopg is None:
        return False, None, "psycopg not installed"
    url = get_database_url()
    if not url:
        return False, None, "DATABASE_URL/SUPABASE_DB_URL not set"
    try:
        with psycopg.connect(url, prepare_threshold=0) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return True, None, None
            cols = [d[0] for d in cur.description]
            return True, dict(zip(cols, row)), None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"
