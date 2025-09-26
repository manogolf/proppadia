from fastapi import APIRouter
from ..deps import ping_db, env_summary

router = APIRouter(tags=["health"])

@router.get("/health/live")
def live():
    return {"ok": True}

@router.get("/health/ready")
def ready():
    db_ok, err = ping_db()
    return {"ok": db_ok, "db_ok": db_ok, "error": err, "env": env_summary()}
