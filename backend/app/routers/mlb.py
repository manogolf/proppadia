from fastapi import APIRouter

router = APIRouter(tags=["mlb"])

@router.get("/mlb/ping")
def ping_mlb():
    return {"sport": "mlb", "ok": True}
