# backend/scripts/shared/supabase_utils.py
from __future__ import annotations
import os
from functools import lru_cache

# Optional for local dev; harmless in CI/Prod
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _resolve_key() -> str | None:
    """
    Accept any of the common env var names you already use in GH Actions / Render.
    """
    return (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    )

def _load_env() -> tuple[str, str]:
    url = os.getenv("SUPABASE_URL")
    key = _resolve_key()
    if not url or not key:
        raise RuntimeError(
            "Missing Supabase env. Set SUPABASE_URL and SUPABASE_KEY "
            "(or SUPABASE_SERVICE_ROLE / SUPABASE_SERVICE_ROLE_KEY)."
        )
    return url, key

@lru_cache(maxsize=1)
def get_supabase():
    """
    Lazily create a single Supabase client. Raises only when first used.
    """
    from supabase import create_client  # import here for clearer errors
    url, key = _load_env()
    return create_client(url, key)

class _SupabaseProxy:
    """
    Proxy that defers client creation until first attribute access.
    This lets you write `supabase.from_(...).select(...).execute()` safely,
    even if envs weren’t present at import time.
    """
    __slots__ = ("_client",)

    def __init__(self):
        self._client = None

    def _ensure(self):
        if self._client is None:
            self._client = get_supabase()

    def __getattr__(self, name: str):
        self._ensure()
        return getattr(self._client, name)

    def __call__(self):
        # Optional: allow `supabase()` to return the real client.
        self._ensure()
        return self._client

def table(name: str):
    """Convenience helper: table('foo').select(...).execute()"""
    return get_supabase().from_(name)

# What most code will import
supabase = _SupabaseProxy()

__all__ = ["get_supabase", "supabase", "table"]
