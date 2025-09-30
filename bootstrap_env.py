from pathlib import Path
import os

def _normalize_and_mirror(dsn: str) -> str:
    if dsn and "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    if dsn and "gssencmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "gssencmode=disable"
    if dsn:
        os.environ.setdefault("DATABASE_URL", dsn)
        os.environ.setdefault("SUPABASE_DB_URL", dsn)
    return dsn

def _load_env_upwards():
    # If already set, normalize/mirror and bail
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if dsn:
        _normalize_and_mirror(dsn)
        return
    # Try python-dotenv, searching upward from this file
    try:
        from dotenv import load_dotenv  # pip install python-dotenv
    except Exception:
        return
    here = Path(__file__).resolve()
    for p in (here.parent, *here.parents):
        env = p / ".env"
        if env.exists():
            load_dotenv(env, override=False)
            break
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or ""
    _normalize_and_mirror(dsn)

_load_env_upwards()
