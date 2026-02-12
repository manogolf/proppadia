from fastapi import APIRouter
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE_KEY"]
)

router = APIRouter()

@router.get("/player-profile/{player_id}")
async def get_player_profile(player_id: str):
    # Pull recent props (last 14 games)
    props_res = supabase.table("player_props") \
        .select("*") \
        .eq("player_id", player_id) \
        .order("game_date", desc=True) \
        .limit(14) \
        .execute()

    props = props_res.data or []

    # Pull model training props for streak + accuracy
    training_res = supabase.table("model_training_props") \
        .select("*") \
        .eq("player_id", player_id) \
        .order("game_date", desc=True) \
        .limit(30) \
        .execute()

    training = training_res.data or []

    # Extract player info from any recent prop
    basic_info = next((p for p in props if p.get("player_name")), {})

    # Rolling averages (example: hits, total_bases)
    def rolling_avg(field):
        vals = [float(p.get(field, 0)) for p in training if p.get(field) is not None]
        return round(sum(vals) / len(vals), 2) if vals else 0

    rolling_averages = {
        "hits": rolling_avg("hits"),
        "total_bases": rolling_avg("total_bases"),
        "strikeouts": rolling_avg("strikeouts_batting"),
    }

    # Simple streak logic
    def count_streak(outcome_type):
        count = 0
        for row in training:
            if row.get("outcome") == outcome_type:
                count += 1
            else:
                break
        return count

    streaks = {
        "hit_streak": count_streak("win"),
        "loss_streak": count_streak("loss"),
        "win_streak": count_streak("win")  # Placeholder for true win streak logic
    }

    # Accuracy
    total = [t for t in training if t.get("predicted_outcome") and t.get("was_correct") is not None]
    correct = [t for t in total if t["was_correct"] == True]

    recent = total[:7]
    recent_correct = [t for t in recent if t["was_correct"] == True]

    model_accuracy = {
        "lifetime": {
            "props_evaluated": len(total),
            "correct": len(correct),
            "accuracy": round(len(correct) / len(total), 3) if total else None
        },
        "last_7": {
            "props_evaluated": len(recent),
            "correct": len(recent_correct),
            "accuracy": round(len(recent_correct) / len(recent), 3) if recent else None
        }
    }

    return {
        "player_id": player_id,
        "player_name": basic_info.get("player_name"),
        "team": basic_info.get("team"),
        "position": basic_info.get("position"),
        "recent_props": props,
        "streaks": streaks,
        "rolling_averages": rolling_averages,
        "model_accuracy": model_accuracy
    }
