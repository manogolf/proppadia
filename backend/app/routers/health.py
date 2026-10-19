# backend/app/routers/health.py
from fastapi import APIRouter

from backend.shared.db.pg import api_database_route_state

router = APIRouter()

@router.get("/health", summary="Health")
def health():
    # Route name only (API_LOGIN / LEGACY_ALIASES); never a URL or login value.
    return {"ok": True, "db_route": api_database_route_state()}
