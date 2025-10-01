# backend/app/deps.py
from __future__ import annotations
import os
from typing import Tuple, Dict, Any

# If you use bootstrap_env to load .env / normalize DB URL, keep this
try:
    import bootstrap_env  # noqa: F401
except Exception:
    pass

try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore


def _env_db_url() -> str | None:
    """Return DB URL with safe defaults for Supabase; None if not set."""
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        return None
    if "?sslmode=" not in db and "&sslmode=" not in db:
        db += ("&" if "?" in db else "?") + "sslmode=require"
    if "?gssencmode=" not in db and "&gssencmode=" not in db:
        db += ("&" if "?" in db else "?") + "gssencmode=disable"
    return db


def ping_db(timeout: float = 3.0) -> Tuple[bool, str]:
    """Lightweight DB health check used by /health."""
    if psycopg is None:
        return False, "psycopg not installed"
    db = _env_db_url()
    if not db:
        return False, "Missing SUPABASE_DB_URL/DATABASE_URL"
    try:
        with psycopg.connect(db, connect_timeout=int(timeout)) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def env_summary() -> Dict[str, Any]:
    """Tiny snapshot for /health so you can see what’s configured."""
    return {
        "has_SUPABASE_DB_URL": bool(os.getenv("SUPABASE_DB_URL")),
        "has_DATABASE_URL": bool(os.getenv("DATABASE_URL")),
        "has_SUPABASE_URL": bool(os.getenv("SUPABASE_URL")),
        "has_SUPABASE_ANON_KEY": bool(os.getenv("SUPABASE_ANON_KEY")),
    }

# --- minimal DB helpers used by routers ---------------------------------

def _require_db_url() -> str:
    db = _env_db_url()
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL/DATABASE_URL")
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    return db

def pg_fetchone(sql: str, params=None):
    """Execute a query and return the first row (or None)."""
    db = _require_db_url()
    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()

def pg_fetchall(sql: str, params=None):
    """Execute a query and return all rows as a list of tuples."""
    db = _require_db_url()
    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def pg_execute(sql: str, params=None) -> int:
    """Execute a write (INSERT/UPDATE/DELETE); returns affected rowcount."""
    db = _require_db_url()
    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()
        return cur.rowcount

# Back-compat alias used by older routers
def _db_url() -> str:
    return _env_db_url()
