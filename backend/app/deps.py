from __future__ import annotations
import os, sys
from pathlib import Path
from typing import Tuple, Optional

APP_DIR = Path(__file__).resolve().parent         # backend/app
BACKEND_DIR = APP_DIR.parent                      # backend/
ROOT_SCRIPTS = BACKEND_DIR / "scripts"
if ROOT_SCRIPTS.exists():
    sys.path.insert(0, str(ROOT_SCRIPTS))

# Import the centralized, root-level helper
from shared.supabase_utils import supabase  # type: ignore

def env_summary() -> dict:
    return {
        "SUPABASE_URL_set": bool(os.getenv("SUPABASE_URL")),
        "SUPABASE_ANON_KEY_set": bool(os.getenv("SUPABASE_ANON_KEY")),
        "SUPABASE_SERVICE_ROLE_KEY_set": bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
        "helper": "backend/scripts/shared/supabase_utils.py",
    }

def ping_db() -> Tuple[bool, Optional[str]]:
    """Lightweight DB touch; adjust table to any known universal table."""
    try:
        supabase.table("player_props").select("id").limit(1).execute()
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
