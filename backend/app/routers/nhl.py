from fastapi import APIRouter
from ..deps import supabase  # centralized root helper

router = APIRouter(tags=["nhl"])

@router.get("/nhl/ping")
def ping_nhl():
    return {"sport": "nhl", "ok": True}

@router.get("/nhl/ping-db")
def ping_nhl_db():
    """
    Touches nhl.training_features_nhl_sog_v2 and returns a tiny sample.
    Uses schema('nhl') explicitly; falls back to default schema if needed.
    """
    # Prefer explicit schema usage
    try:
        res = (
            supabase
            .schema("nhl")
            .table("training_features_nhl_sog_v2")
            .select("player_id,game_id,team_id,opponent_id,is_home,game_date", count="exact")
            .limit(1)
            .execute()
        )
        data = getattr(res, "data", []) or []
        count = getattr(res, "count", None)
        return {"ok": True, "schema": "nhl", "table": "training_features_nhl_sog_v2", "count": count, "sample": data[:1]}
    except Exception as e_schema:
        # Fallback in case the client/schema call style differs
        try:
            res2 = (
                supabase
                .table("training_features_nhl_sog_v2")
                .select("player_id,game_id,team_id,opponent_id,is_home,game_date", count="exact")
                .limit(1)
                .execute()
            )
            data2 = getattr(res2, "data", []) or []
            count2 = getattr(res2, "count", None)
            return {"ok": True, "schema": "default", "table": "training_features_nhl_sog_v2", "count": count2, "sample": data2[:1]}
        except Exception as e_default:
            return {
                "ok": False,
                "error": {
                    "schema_try": f"{type(e_schema).__name__}: {e_schema}",
                    "default_try": f"{type(e_default).__name__}: {e_default}",
                },
            }
