from fastapi import APIRouter

router = APIRouter(tags=["health"])

@router.get("/health/live")
def live():
    return {"ok": True}

@router.get("/health/ready")
def ready():
    # Add lightweight checks later (e.g., DB ping)
    return {"ok": True}
