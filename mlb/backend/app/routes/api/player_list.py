from fastapi import APIRouter, HTTPException
from scripts.shared.supabase_utils import supabase

router = APIRouter()


from fastapi import APIRouter, HTTPException
from scripts.shared.supabase_utils import supabase

router = APIRouter()

@router.get("/players")
async def get_players():
    try:
        response = (
    supabase
    .from_("player_props")
    .select("player_id, player_name, team")
    .neq("player_id", None)
    .neq("player_name", None)
    .neq("team", None)
    .limit(5000)
    .execute()
)


        if response.data is None:
            raise HTTPException(status_code=500, detail="Failed to fetch player list")

        seen = set()
        deduped = []
        for row in response.data:
            key = (row["player_id"], row["player_name"], row["team"])
            if key not in seen:
                deduped.append({
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                    "team": row["team"]
                })
                seen.add(key)

        return deduped

    except Exception as e:
        print(f"❌ Exception in /players: {e}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")



