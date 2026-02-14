from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from backend.app.schemas.mlb import (
    AddPropRequest,
    AddPropResponse,
    GamesContextResponse,
    ModelMetricRow,
    ModelWeeklyMetricRow,
    PingResponse,
    PlayerListItem,
    PlayerProfileResponse,
    PlayersSearchResponse,
    PredictRequest,
    PredictResponse,
    PropHistoryResponse,
    PreparePropRequest,
    PreparePropResponse,
    ResolvePlayerResponse,
    UserVsModelMetricRow,
    UserVsModelWeeklyMetricRow,
    model_to_dict,
)
from backend.app.services.mlb.game_context_service import get_game_context
from backend.app.services.mlb.metrics_service import (
    fetch_model_metrics,
    fetch_model_metrics_weekly,
    fetch_user_vs_model_metrics,
    fetch_user_vs_model_metrics_weekly,
)
from backend.app.services.mlb.market_odds_service import (
    fetch_mlb_market_odds,
    get_market_cache_status,
    get_supported_market_map,
)
from backend.app.services.mlb.prop_submission_service import (
    add_prop,
    get_prop_history,
    predict_prepared_prop,
    prepare_prop_submission,
)
from backend.app.services.mlb.schedule_service import fetch_schedule
from backend.app.services.mlb.standings_service import fetch_standings
from backend.app.services.mlb.player_service import (
    list_players,
    lookup_player,
    player_profile,
    resolve_player,
    search_players,
)
from backend.app.services.shared import ping_db, sport_ping

router = APIRouter(tags=["mlb"])
ET = ZoneInfo("America/New_York")


@router.get("/mlb/ping", response_model=PingResponse)
def ping_mlb():
    return sport_ping("mlb")


@router.get("/mlb/ping-db", summary="MLB DB connectivity check")
def ping_mlb_db():
    return ping_db()


@router.get("/mlb/market-odds", summary="MLB market odds lookup (OddsAPI)")
def mlb_market_odds(
    player_name: str = Query(..., description="Player full name"),
    prop_type: str = Query(..., description="MLB prop type key"),
    game_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today ET)"),
    over_under: str = Query("over", description="over|under"),
    line: Optional[float] = Query(None, description="Prop line value"),
):
    target_date = game_date or datetime.now(ET).date().isoformat()
    try:
        return fetch_mlb_market_odds(
            player_name=player_name,
            prop_type=prop_type,
            game_date=target_date,
            over_under=over_under,
            line=line,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get("/mlb/market-supported-props", summary="MLB prop types with OddsAPI market coverage")
def mlb_market_supported_props():
    mapping = get_supported_market_map()
    rows = [{"prop_type": k, "market_key": v} for k, v in sorted(mapping.items())]
    return {"ok": True, "count": len(rows), "rows": rows}


@router.get("/mlb/market-cache-status", summary="MLB OddsAPI cache status (no upstream call)")
def mlb_market_cache_status():
    return get_market_cache_status()


@router.get("/mlb/schedule", summary="MLB schedule proxy (backend-owned)")
def mlb_schedule(
    date_str: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD (defaults to today ET)"),
):
    target_date = date_str
    if target_date:
        try:
            date.fromisoformat(target_date)
        except ValueError as e:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from e
    else:
        target_date = datetime.now(ET).date().isoformat()

    try:
        payload = fetch_schedule(game_date=target_date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}") from e
    return payload


@router.get("/mlb/standings", summary="MLB standings proxy (backend-owned)")
def mlb_standings(
    season: Optional[int] = Query(None, description="Season year (defaults to today ET year)"),
    league_id: str = Query("103,104", description="Comma-separated MLB league ids"),
):
    target_season = int(season) if season else datetime.now(ET).year
    try:
        payload = fetch_standings(season=target_season, league_ids=league_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}") from e
    return payload


@router.get(
    "/players/resolve",
    summary="Resolve MLB player",
    response_model=ResolvePlayerResponse,
    response_model_exclude_none=True,
)
def players_resolve(
    name: Optional[str] = Query(None, description="Player name"),
    player_name: Optional[str] = Query(None, description="Alias for name"),
    player_id: Optional[int] = Query(None, description="Known player id"),
    team_abbr: Optional[str] = Query(None, description="Team abbreviation"),
):
    query_name = (name or player_name or "").strip() or None
    if player_id is None and not query_name:
        raise HTTPException(status_code=400, detail="Provide player_id or name/player_name")

    try:
        result = resolve_player(
            player_id=player_id,
            name=query_name,
            team_abbr=team_abbr,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    if not result:
        return {
            "ok": True,
            "found": False,
            "player_id": None,
            "player_name": query_name,
            "team_abbr": (team_abbr or "").upper() or None,
        }

    return {"ok": True, "found": True, **result}


@router.get(
    "/games/context",
    summary="Resolve MLB game context by team/date",
    response_model=GamesContextResponse,
    response_model_exclude_none=True,
)
def games_context(
    team_id: int = Query(..., description="MLB team id"),
    for_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today ET)"),
):
    target_date = for_date
    if target_date:
        try:
            date.fromisoformat(target_date)
        except ValueError as e:
            raise HTTPException(status_code=400, detail="for_date must be YYYY-MM-DD") from e
    else:
        target_date = datetime.now(ET).date().isoformat()

    try:
        context = get_game_context(team_id=team_id, game_date=target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    if not context:
        return {
            "ok": True,
            "found": False,
            "team_id": team_id,
            "for_date": target_date,
        }

    return {"ok": True, "found": True, **context}


@router.get(
    "/players/lookup",
    summary="Lookup MLB player by id",
    response_model=ResolvePlayerResponse,
    response_model_exclude_none=True,
)
def players_lookup(
    player_id: int = Query(..., description="MLB player id"),
):
    try:
        row = lookup_player(player_id=player_id)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    if not row:
        return {"ok": True, "found": False, "player_id": player_id}
    return {"ok": True, "found": True, **row}


@router.get(
    "/players/search",
    summary="Search MLB players",
    response_model=PlayersSearchResponse,
    response_model_exclude_none=True,
)
def players_search(
    q: str = Query(..., description="Name search query"),
    limit: int = Query(10, ge=1, le=100),
):
    try:
        rows = search_players(q=q, limit=limit)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    return {"ok": True, "count": len(rows), "rows": rows}


@router.get(
    "/players",
    summary="List MLB players",
    response_model=List[PlayerListItem],
    response_model_exclude_none=True,
)
def players_list(
    limit: int = Query(2000, ge=1, le=5000),
):
    try:
        rows = list_players(limit=limit)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e
    return rows


@router.get(
    "/player-profile/{player_id}",
    summary="MLB player profile",
    response_model=PlayerProfileResponse,
    response_model_exclude_none=True,
)
def get_player_profile(player_id: int):
    try:
        payload = player_profile(player_id=player_id)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e
    return payload


@router.post(
    "/prepareProp",
    summary="Prepare MLB prop features",
    response_model=PreparePropResponse,
    response_model_exclude_none=True,
)
def prepare_prop_endpoint(body: PreparePropRequest):
    try:
        return prepare_prop_submission(model_to_dict(body))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.post(
    "/predict",
    summary="Predict MLB prop",
    response_model=PredictResponse,
    response_model_exclude_none=True,
)
def predict_prop_endpoint(body: PredictRequest):
    try:
        return predict_prepared_prop(model_to_dict(body))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.post(
    "/props/add",
    summary="Persist user-added MLB prop from commit token",
    response_model=AddPropResponse,
    response_model_exclude_none=True,
)
def add_prop_endpoint(body: AddPropRequest):
    try:
        return add_prop(model_to_dict(body))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get(
    "/props/history",
    summary="Read MLB prop history rows",
    response_model=PropHistoryResponse,
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


@router.get(
    "/model-metrics",
    summary="Model accuracy by prop type",
    response_model=List[ModelMetricRow],
    response_model_exclude_none=True,
)
def model_metrics():
    try:
        return fetch_model_metrics()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get(
    "/user-vs-model-accuracy",
    summary="User vs model accuracy by prop type",
    response_model=List[UserVsModelMetricRow],
    response_model_exclude_none=True,
)
def user_vs_model_metrics():
    try:
        return fetch_user_vs_model_metrics()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get(
    "/user-vs-model-accuracy-weekly",
    summary="Weekly user vs model accuracy",
    response_model=List[UserVsModelWeeklyMetricRow],
    response_model_exclude_none=True,
)
def user_vs_model_metrics_weekly():
    try:
        return fetch_user_vs_model_metrics_weekly()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e


@router.get(
    "/model-accuracy-weekly",
    summary="Weekly model accuracy by prop type",
    response_model=List[ModelWeeklyMetricRow],
    response_model_exclude_none=True,
)
def model_metrics_weekly():
    try:
        return fetch_model_metrics_weekly()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e
