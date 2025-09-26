from fastapi import APIRouter

router = APIRouter(tags=["nhl"])

@router.get("/nhl/ping")
def ping_nhl():
    return {"sport": "nhl", "ok": True}
