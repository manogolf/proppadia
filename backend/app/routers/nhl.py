# backend/app/routers/nhl.py
from __future__ import annotations

from typing import Optional, Dict, Any

from fastapi import APIRouter, Query
import httpx

from backend.app.deps import pg_fetchone
from backend.domains.nhl.repository import (
    fetch_games_today,
    fetch_props_today,
    fetch_saves,
    fetch_sog,
)

router = APIRouter(prefix="/api/nhl", tags=["nhl"])


@router.get("/gamecenter/{game_id}/landing", summary="NHL GameCenter landing (proxy)")
async def nhl_gamecenter_landing(game_id: int):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/landing"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers={"User-Agent": "proppadia/1.0"})
        r.raise_for_status()
        return {"ok": True, "game_id": game_id, "data": r.json()}
    except Exception as e:
        return {"ok": False, "game_id": game_id, "error": str(e)}


@router.get("/ping", summary="Ping Nhl")
def ping_nhl():
    return {"sport": "nhl", "ok": True}


@router.get("/ping-db", summary="Nhl Ping Db")
def nhl_ping_db():
    ok, row, err = pg_fetchone("SELECT 1 AS ok")
    return {"ok": bool(row), "err": err}


@router.get(
    "/games/today",
    summary="Nhl Games Today",
    description="Return today's NHL games with team names/abbrs (schema: nhl.games + nhl.teams).",
)
def nhl_games_today(
    date: Optional[str] = Query(
        None, description="YYYY-MM-DD (defaults to today in America/New_York)"
    ),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    return fetch_games_today(date, limit, offset)


@router.get(
    "/props/today",
    summary="Nhl Props Today",
    description="Return a small page of predictions for today's games.",
)
def nhl_props_today(
    date: Optional[str] = Query(
        None, description="YYYY-MM-DD (defaults to today in America/New_York)"
    ),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return fetch_props_today(date, limit, offset)


# --- SOG (wide) ---
@router.get("/sog", summary="Skater SOG predictions (wide)")
def sog(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return fetch_sog(date, limit, offset)


# --- Saves (wide) ---
@router.get("/saves", summary="Goalie Saves predictions (wide)")
def saves(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return fetch_saves(date, limit, offset)
