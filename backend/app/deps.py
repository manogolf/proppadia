# backend/app/deps.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any

from backend.shared.db import pg_connect as shared_pg_connect
from backend.shared.db import pg_fetchone as shared_pg_fetchone


def pg_connect():
    """
    Return a psycopg connection with prepared statements disabled
    (avoids GH Actions pooler 'DuplicatePreparedStatement' issues).
    """
    return shared_pg_connect()


def pg_fetchone(sql: str, params: tuple = ()) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Execute read-only SQL and return a single row as dict.
    (ok, row, err) — row is None when no rows.
    """
    try:
        row = shared_pg_fetchone(sql, params)
        return True, row, None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"
