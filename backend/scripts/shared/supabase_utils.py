from __future__ import annotations
import os
from typing import Optional
try:
    from supabase import create_client, Client
except ImportError as e:
    raise RuntimeError(
        "Missing 'supabase' package in the current interpreter. "
        "Activate your venv and `pip install supabase==2.*`."
    ) from e

SUPABASE_URL: Optional[str] = os.getenv("SUPABASE_URL")
# Prefer service role if available; fall back to anon for read-only usage
SUPABASE_KEY: Optional[str] = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_URL and/or SUPABASE_*_KEY in environment."
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
