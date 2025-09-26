from scripts.shared.supabase_utils import supabase
from fastapi import APIRouter

router = APIRouter()

@router.get("/api/user-vs-model-accuracy")
async def get_user_vs_model_accuracy():
    result = supabase.from_("user_vs_model_accuracy_view").select("*").execute()

    if hasattr(result, "error") and result.error:
        raise Exception(f"Supabase error: {result.error.message}")

    raw = result.data or []
    
    # Optional: Normalize prop_type using display-friendly names from shared propUtils if needed on frontend
    return raw
