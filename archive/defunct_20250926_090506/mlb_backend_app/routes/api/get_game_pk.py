# backend/app/routes/api/get_game_pk.py
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from scripts.shared.supabase_utils import get_supabase
from scripts.shared.prop_utils import get_latest_team_for_player  # returns (abbr, team_id)

router = APIRouter()

@router.get("/getGamePk")
async def get_game_pk(
    player_id: int = Query(..., description="MLB player_id"),
    date: str = Query(..., description="Game date in YYYY-MM-DD (ET)"),
    tag: Optional[str] = Query(None, description="feature set tag (default env FEATURE_SET_TAG or v1)"),
):
    """
    Resolve game_id (game_pk) for a player and date.

    Tries:
      1) precomputed rows (prop_features_precomputed)
      2) fallback via player's latest team -> game_info on that date
    """
    supa = get_supabase()
    tag = tag or os.getenv("FEATURE_SET_TAG", "v1")

    # 1) Fast path: any precomputed row for that player/date/tag
    try:
        r = (
            supa.from_("prop_features_precomputed")
            .select("game_id")
            .eq("player_id", str(player_id))
            .eq("game_date", date)
            .eq("feature_set_tag", tag)
            .limit(1)
            .execute()
        )
        rows = getattr(r, "data", []) or []
        if rows and rows[0].get("game_id"):
            return {"game_id": int(rows[0]["game_id"]), "source": "precomputed"}
    except Exception:
        # fall through to fallback
        pass

    # 2) Fallback: use player's latest team, then find the team's game in game_info
    team_id = None
    try:
        _, team_id = get_latest_team_for_player(int(player_id))
    except Exception:
        team_id = None

    if team_id:
        try:
            r2 = (
                supa.from_("game_info")
                .select("game_id,home_team_id,away_team_id")
                .eq("game_date", date)
                .or_(f"home_team_id.eq.{team_id},away_team_id.eq.{team_id}")
                .order("game_id", desc=True)
                .limit(1)
                .execute()
            )
            rows2 = getattr(r2, "data", []) or []
            if rows2 and rows2[0].get("game_id"):
                return {
                    "game_id": int(rows2[0]["game_id"]),
                    "source": "game_info",
                    "team_id": int(team_id),
                }
        except Exception:
            pass

    # nothing found
    raise HTTPException(
        status_code=404,
        detail=f"No game found for player_id={player_id} on {date} (tag={tag}).",
    )
