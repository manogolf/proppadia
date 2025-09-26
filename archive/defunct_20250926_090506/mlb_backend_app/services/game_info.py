# backend/app/services/game_info.py
from __future__ import annotations
from typing import Optional, Dict, Any
from scripts.shared.supabase_utils import supabase
from scripts.shared.mlb_api_v2 import get_game_time_et

def get_game_info(game_id: int) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase.from_("game_info")
            .select("game_id, game_time")
            .eq("game_id", int(game_id))
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        return rows[0] if rows else None
    except Exception:
        return None

def ensure_game_info(game) -> None:
    """
    Idempotent: if the row exists, return.
    Insert ONLY the columns your current schema has: (game_id, game_time).
    """
    if supabase is None:
        return
    try:
        gid = int(game.game_id)
    except Exception:
        return

    # Already present?
    if get_game_info(gid):
        return

    # Derive game_time (ET ISO) if not provided
    game_time = getattr(game, "game_time", None) or get_game_time_et(gid)

    row = {"game_id": gid}
    if game_time:
        row["game_time"] = game_time  # your table uses timestamp without tz

    try:
        supabase.from_("game_info").upsert(row, on_conflict="game_id").execute()
    except Exception:
        # best effort
        pass
