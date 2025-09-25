# backend/app/routes/api/model_metrics.py
from fastapi import APIRouter
from scripts.shared.supabase_utils import supabase


router = APIRouter()

@router.get("/api/model-metrics")
async def get_model_accuracy():
    result = supabase.rpc("get_model_accuracy_metrics").execute()
    return result.data if result.data else []
