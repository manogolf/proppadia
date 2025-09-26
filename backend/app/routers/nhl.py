from fastapi import APIRouter
from backend.app.deps import pg_fetchone

router = APIRouter(tags=["nhl"])

@router.get("/nhl/ping")
def ping_nhl():
    return {"sport": "nhl", "ok": True}

@router.get("/nhl/ping-db")
def nhl_ping_db():
    sql = """
        SELECT player_id, game_id, team_id, opponent_id, is_home, game_date
        FROM nhl.training_features_nhl_sog_v2
        ORDER BY game_date DESC
        LIMIT 1
    """
    ok, row, err = pg_fetchone(sql)
    if ok:
        return {
            "ok": True,
            "source": "postgres",
            "table": "nhl.training_features_nhl_sog_v2",
            "sample": row,
        }
    return {"ok": False, "error": err}
