# backend/app/routers/nhl.py
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException, Query

from backend.app.schemas.nhl import (
    NhlAddPropRequest,
    NhlAddPropResponse,
    NhlDateRowsResponse,
    NhlDbPingResponse,
    NhlErrorResponse,
    NhlGamecenterLandingResponse,
    NhlPingResponse,
    NhlPropHistoryResponse,
)
from backend.app.services.nhl import fetch_gamecenter_landing
from backend.app.services.nhl.slate_meta_service import get_nhl_slate_meta
from backend.app.services.nhl.prop_submission_service import add_prop, get_prop_history
from backend.app.services.shared import ping_db, sport_ping
from backend.domains.nhl.repository import (
    fetch_games_today,
    fetch_players_directory,
    fetch_props_today,
    fetch_saves,
    fetch_sog,
)

router = APIRouter(prefix="/api/nhl", tags=["nhl"])


def _model_to_dict(body):
    if hasattr(body, "model_dump"):
        return body.model_dump(exclude_none=True)  # pydantic v2
    return body.dict(exclude_none=True)  # pydantic v1


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
    "/slate/meta",
    summary="NHL slate metadata",
    description="Backend-owned NHL slate health metadata for games/props/sog/saves.",
)
def nhl_slate_meta(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in America/New_York)"),
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    try:
        return get_nhl_slate_meta(date=date, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


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


@router.get(
    "/players",
    summary="NHL players directory",
    description="Cumulative NHL players grouped by team context (not limited to today's slate).",
)
def nhl_players_directory(
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    include_inactive: bool = Query(False),
):
    return fetch_players_directory(limit, offset, include_inactive)


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


@router.post(
    "/props/add",
    summary="Persist user-added NHL prop",
    response_model=NhlAddPropResponse,
    response_model_exclude_none=True,
)
def add_prop_endpoint(body: NhlAddPropRequest):
    try:
        return add_prop(_model_to_dict(body))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get(
    "/props/history",
    summary="Read NHL prop history rows",
    response_model=NhlPropHistoryResponse,
    response_model_exclude_none=True,
)
def props_history_endpoint(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user_id: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    prop_source: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="pending|win|loss|push|resolved|dnp"),
):
    for label, raw in (("from_date", from_date), ("to_date", to_date)):
        if not raw:
            continue
        try:
            date.fromisoformat(raw)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"{label} must be YYYY-MM-DD") from e

    try:
        return get_prop_history(
            {
                "limit": limit,
                "offset": offset,
                "user_id": user_id,
                "from_date": from_date,
                "to_date": to_date,
                "prop_source": prop_source,
                "status": status,
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e
