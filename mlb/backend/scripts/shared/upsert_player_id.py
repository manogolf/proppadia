# File: backend/scripts/shared/upsert_player_id.py

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

async def upsert_player_id(player_name: str, team: str) -> str:
    """
    Resolve and upsert player_id for a given player_name and team from Supabase.
    """
    try:
        # Step 1: Try player_ids
        result = (
            supabase.table("player_ids")
            .select("player_id")
            .eq("player_name", player_name)
            .eq("team", team)
            .maybe_single()
            .execute()
        )

        if result.data and result.data.get("player_id"):
            return result.data["player_id"]

        # Step 2: Fallback to model_training_props
        fallback = (
            supabase.table("model_training_props")
            .select("player_id")
            .eq("player_name", player_name)
            .eq("team", team)
            .order("game_date", desc=True)
            .limit(1)
            .maybe_single()
            .execute()
        )

        player_id = fallback.data.get("player_id") if fallback.data else None
        if not player_id:
            raise ValueError(f"Unable to find player_id for {player_name} ({team})")

        # Step 3: Upsert into player_ids
        supabase.table("player_ids").upsert(
            {
                "player_name": player_name,
                "team": team,
                "player_id": player_id,
            },
            on_conflict="player_id",
        ).execute()

        return player_id

    except Exception as e:
        raise RuntimeError(f"upsert_player_id failed: {e}")
