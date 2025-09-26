from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

# ---- paths so we can import backend/scripts/shared/supabase_utils.py ----
APP_DIR = Path(__file__).resolve().parent         # backend/app
BACKEND_DIR = APP_DIR.parent                      # backend/
ROOT_SCRIPTS = BACKEND_DIR / "scripts"
if ROOT_SCRIPTS.exists() and str(ROOT_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(ROOT_SCRIPTS))

# ---- robust import of the centralized Supabase helper ----
try:
    # package style: backend/scripts/shared/__init__.py exists
    from shared.supabase_utils import supabase  # type: ignore
except ModuleNotFoundError:
    # module style: import the file directly
    SHARED_DIR = ROOT_SCRIPTS / "shared"
    if SHARED_DIR.exists() and str(SHARED_DIR) not in sys.path:
        sys.path.insert(0, str(SHARED_DIR))
    import importlib
    supabase = importlib.import_module("supabase_utils").supabase  # type: ignore

# ---- optional direct Postgres access for non-public schemas (e.g., nhl.*) ----
try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore

def _db_url() -> Optional[str]:
    # accept any common env var name; Render already has DATABASE_URL
    return (
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_POSTGRES_URL")
    )

def pg_fetchone(sql: str, params: tuple = ()) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Execute read-only SQL and return a single row as dict.
    Requires psycopg and a DB URL env var.
    """
    if psycopg is None:
        return False, None, "psycopg not installed"
    url = _db_url()
    if not url:
        return False, None, "SUPABASE_DB_URL/DATABASE_URL not set"
    try:
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if not row:
                    return True, None, None
                cols = [d[0] for d in cur.description]
                return True, dict(zip(cols, row)), None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"

def env_summary() -> dict:
    return {
        "SUPABASE_URL_set": bool(os.getenv("SUPABASE_URL")),
        "SUPABASE_ANON_KEY_set": bool(os.getenv("SUPABASE_ANON_KEY")),
        "SUPABASE_SERVICE_ROLE_KEY_set": bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
        "SUPABASE_DB_URL_set": bool(_db_url()),
        "helper": "backend/scripts/shared/supabase_utils.py",
    }

def ping_db() -> Tuple[bool, Optional[str]]:
    """Lightweight readiness check via a public table; adjust if needed."""
    try:
        supabase.table("player_props").select("id").limit(1).execute()
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
