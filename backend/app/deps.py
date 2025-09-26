from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from shared.supabase_utils import supabase  # type: ignore
from fastapi import APIRouter
from backend.app.deps import pg_fetchone  # absolute import avoids relative import issues in editors
try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore

router = APIRouter(tags=["nhl"])


APP_DIR = Path(__file__).resolve().parent         # backend/app
BACKEND_DIR = APP_DIR.parent                      # backend/
ROOT_SCRIPTS = BACKEND_DIR / "scripts"
if ROOT_SCRIPTS.exists():
    sys.path.insert(0, str(ROOT_SCRIPTS))

def _db_url() -> str | None:
    # Use whichever of these you’ve set (you already have DATABASE_URL on Render)
    return (
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_POSTGRES_URL")
    )

def env_summary() -> dict:
    return {
        "SUPABASE_URL_set": bool(os.getenv("SUPABASE_URL")),
        "SUPABASE_ANON_KEY_set": bool(os.getenv("SUPABASE_ANON_KEY")),
        "SUPABASE_SERVICE_ROLE_KEY_set": bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
        "SUPABASE_DB_URL_set": bool(_db_url()), 
        "helper": "backend/scripts/shared/supabase_utils.py",
    }

def ping_db() -> Tuple[bool, Optional[str]]:
    """Lightweight DB touch; adjust table to any known universal table."""
    try:
        supabase.table("player_props").select("id").limit(1).execute()
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"    

def pg_fetchone(sql: str, params: tuple = ()) -> tuple[bool, dict[str, Any] | None, str | None]:
    """
    Execute read-only SQL and return one row as a dict.
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
    
        # add anywhere in the file (e.g., after /nhl/ping)
@router.get("/nhl/ping")
def ping_nhl():
    return {"sport": "nhl", "ok": True}

@router.get("/nhl/ping-db")
def nhl_ping_db():
    sql = """
        SELECT player_id, game_id, team_id, opponent_id, is_home, game_date
        FROM nhl.training_features_nhl_sog_v2
        ORDER BY game_date DESC
        LIMIT 1
    """
    ok, row, err = pg_fetchone(sql)
    if ok:
        return {"ok": True, "source": "postgres", "table": "nhl.training_features_nhl_sog_v2", "sample": row}
    return {"ok": False, "error": err}


