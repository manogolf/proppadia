#  backend/app/routes/api/games.py

from fastapi import APIRouter, Query
from datetime import date
from app.services.games import enrich_game_context  # implement using your current logic

router = APIRouter()

@router.get("/games/context")
def games_context(team_id: int = Query(..., ge=1),
                  for_date: str | None = None):
    ctx = enrich_game_context(team_id, for_date)  # returns {game_id, is_home, opponent, opponent_encoded, game_time, ...}
    return {"ok": True, "data": ctx}
