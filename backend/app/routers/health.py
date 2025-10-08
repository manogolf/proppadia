# backend/app/routers/health.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/health", summary="Health")
def health():
    return {"ok": True}
