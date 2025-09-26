from fastapi import APIRouter
from ..deps import ping_db, env_summary
import os

router = APIRouter(tags=["health"])

@router.get("/health/live")
def live():
    return {"ok": True}

@router.get("/health/ready")
def ready():
    db_ok, err = ping_db()
    return {"ok": db_ok, "db_ok": db_ok, "error": err, "env": env_summary()}

@router.get("/status")
def status():
    sha = (
        os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("GIT_COMMIT")
        or os.getenv("COMMIT_SHA")
        or os.getenv("VERCEL_GIT_COMMIT_SHA")
        or "unknown"
    )
    service = os.getenv("RENDER_SERVICE_NAME") or "proppadia-backend"
    db_ok, _ = ping_db()
    return {
        "ok": True,
        "service": service,
        "git_sha": sha[:7] if sha and sha != "unknown" else "unknown",
        "db_ok": db_ok,
        "env": env_summary(),
    }

