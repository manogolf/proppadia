# backend/supabase/supabase_utils.py
from __future__ import annotations

import os
from typing import Optional, Tuple, Dict, Any

from pathlib import Path
from dotenv import load_dotenv

# Load the repo-root .env (…/backend/supabase -> …/.. -> repo root)
ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ROOT_ENV, override=False)

# Optional: load .env locally (ignored on CI)
try:
    if not os.getenv("GITHUB_ACTIONS"):
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
except Exception:
    pass

# ---- Resolve environment ----
def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 2 and s[0] in "'\"" and s[-1] == s[0]:
        s = s[1:-1].strip()
    return s or None

# URLs/keys (support both backend_* and vite_* names)
SUPABASE_URL: Optional[str] = _clean(
    os.getenv("SUPABASE_URL") or os.getenv("VITE_SUPABASE_URL")
)
# prefer service role for servers; fall back to publishable/anon for read-only
SUPABASE_KEY: Optional[str] = _clean(
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_PUBLISHABLE_KEY")
    or os.getenv("VITE_SUPABASE_PUBLISHABLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("VITE_SUPABASE_ANON_KEY")
)

# DB URL for psycopg (optional)
SUPABASE_DB_URL: Optional[str] = _clean(os.getenv("SUPABASE_DB_URL"))
DATABASE_URL: Optional[str] = _clean(os.getenv("DATABASE_URL") or SUPABASE_DB_URL)

# ---- Supabase client (exported symbol: supabase) ----
supabase = None
_client_error: Optional[str] = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        from supabase import create_client, Client  # type: ignore

        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)  # exported below
    else:
        _client_error = "Missing SUPABASE_URL or auth key (service/publishable/anon)."
except Exception as e:
    _client_error = f"{type(e).__name__}: {e}"
    supabase = None

def get_supabase_client():
    """
    Return a live Supabase client or raise with a clear message.
    """
    if supabase is None:
        raise RuntimeError(_client_error or "Supabase client unavailable")
    return supabase

def get_database_url() -> Optional[str]:
    """
    Single place to read the DB URL your code should use for psycopg connections.
    """
    return DATABASE_URL

def env_summary() -> Dict[str, Any]:
    """
    Lightweight diagnosis payload for /api/diag/supabase.
    """
    return {
        "SUPABASE_URL_set": bool(SUPABASE_URL),
        "SERVICE_ROLE_set": bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
        "PUBLISHABLE_set": bool(
            os.getenv("SUPABASE_PUBLISHABLE_KEY") or os.getenv("VITE_SUPABASE_PUBLISHABLE_KEY")
        ),
        "ANON_set": bool(os.getenv("SUPABASE_ANON_KEY") or os.getenv("VITE_SUPABASE_ANON_KEY")),
        "DB_URL_set": bool(get_database_url()),
        "client_ok": supabase is not None,
        "client_error": _client_error,
        "module": "backend/supabase/supabase_utils.py",
    }

# Optional: direct DB ping via psycopg (does not require public tables)
def ping_db() -> Tuple[bool, Optional[str]]:
    try:
        import psycopg  # type: ignore
    except Exception:
        return False, "psycopg not installed"

    url = get_database_url()
    if not url:
        return False, "DATABASE_URL not set"

    try:
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

__all__ = [
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "SUPABASE_DB_URL",
    "DATABASE_URL",
    "supabase",
    "get_supabase_client",
    "get_database_url",
    "env_summary",
    "ping_db",
]
