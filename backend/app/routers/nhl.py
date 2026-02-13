# backend/app/routers/nhl.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Query

from backend.app.schemas.nhl import (
    NhlDateRowsResponse,
    NhlDbPingResponse,
    NhlErrorResponse,
    NhlGamecenterLandingResponse,
    NhlPingResponse,
)
from backend.app.services.nhl import fetch_gamecenter_landing
from backend.app.services.shared import ping_db, sport_ping
from backend.domains.nhl.repository import (
    fetch_games_today,
    fetch_props_today,
    fetch_saves,
    fetch_sog,
)

router = APIRouter(prefix="/api/nhl", tags=["nhl"])


@router.get("/gamecenter/{game_id}/landing", summary="NHL GameCenter landing (proxy)")
async def nhl_gamecenter_landing(game_id: int) -> NhlGamecenterLandingResponse:
    return await fetch_gamecenter_landing(game_id)


@router.get("/ping", summary="Ping Nhl", response_model=NhlPingResponse)
def ping_nhl():
    return sport_ping("nhl")


@router.get("/ping-db", summary="Nhl Ping Db", response_model=NhlDbPingResponse)
def nhl_ping_db():
    return ping_db()


@router.get(
    "/games/today",
    summary="Nhl Games Today",
    description="Return today's NHL games with team names/abbrs (schema: nhl.games + nhl.teams).",
    response_model=NhlDateRowsResponse,
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
    response_model=NhlDateRowsResponse,
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
@router.get("/sog", summary="Skater SOG predictions (wide)", response_model=Union[List[Dict[str, Any]], NhlErrorResponse])
def sog(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return fetch_sog(date, limit, offset)


# --- Saves (wide) ---
@router.get("/saves", summary="Goalie Saves predictions (wide)", response_model=Union[List[Dict[str, Any]], NhlErrorResponse])
def saves(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return fetch_saves(date, limit, offset)
