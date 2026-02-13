from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.app.services.mlb.game_context_service import get_game_context
from backend.app.services.mlb.metrics_service import (
    fetch_model_metrics,
    fetch_model_metrics_weekly,
    fetch_user_vs_model_metrics,
    fetch_user_vs_model_metrics_weekly,
)
from backend.app.services.mlb.prop_submission_service import (
    add_prop,
    predict_prepared_prop,
    prepare_prop_submission,
)
from backend.app.services.mlb.player_service import (
    list_players,
    lookup_player,
    player_profile,
    resolve_player,
    search_players,
)

router = APIRouter(tags=["mlb"])
ET = ZoneInfo("America/New_York")


class PingResponse(BaseModel):
    sport: str
    ok: bool


class PlayerIdentity(BaseModel):
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team_abbr: Optional[str] = None
    team_id: Optional[int] = None
    source: Optional[str] = None
    matched_on: Optional[str] = None


class ResolvePlayerResponse(BaseModel):
    ok: bool
    found: bool
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team_abbr: Optional[str] = None
    team_id: Optional[int] = None
    source: Optional[str] = None
    matched_on: Optional[str] = None


class GamesContextResponse(BaseModel):
    ok: bool
    found: bool
    team_id: int
    team_abbr: Optional[str] = None
    for_date: str
    game_id: Optional[int] = None
    game_time: Optional[str] = None
    is_home: Optional[bool] = None
    opponent_team_id: Optional[int] = None
    opponent: Optional[str] = None
    opponent_encoded: Optional[int] = None
    game_day_of_week: Optional[int] = None
    time_of_day_bucket: Optional[str] = None
    starting_pitcher_id: Optional[int] = None


class PlayersSearchResponse(BaseModel):
    ok: bool
    count: int
    rows: List[PlayerIdentity]


class PlayerListItem(BaseModel):
    player_id: int
    player_name: Optional[str] = None
    team: Optional[str] = None


class PlayerProfileInfo(BaseModel):
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team: Optional[str] = None
    team_id: Optional[int] = None


class PlayerProfileResponse(BaseModel):
    player_info: PlayerProfileInfo
    streaks: List[Dict[str, Any]]
    recent_props: List[Dict[str, Any]]
    stat_derived: List[Dict[str, Any]]
    training_summary: List[Dict[str, Any]]
    season_stats: Dict[str, Any]
    career_stats: Dict[str, Any]


class PreparePropRequest(BaseModel):
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team_id: Optional[int] = None
    team_abbr: Optional[str] = None
    game_date: Optional[str] = None
    prop_type: str
    prop_value: float
    over_under: str
    line_diff: Optional[float] = None
    rolling_result_avg_7: Optional[float] = None
    hit_streak: Optional[float] = None
    win_streak: Optional[float] = None


class PredictRequest(BaseModel):
    prop_type: str
    features: Dict[str, Any]


class AddPropRequest(BaseModel):
    prop_source: Optional[str] = "user_added"
    commit_token: str


class PreparePropResponse(BaseModel):
    ok: bool
    features: Dict[str, Any]
    warnings: Optional[List[str]] = None


class PredictResponse(BaseModel):
    prop_type: str
    probability: float
    probability_over: float
    probability_under: float
    recommendation: str
    predicted_outcome: str
    commit_token: str
    model: str
    model_meta: Optional[Dict[str, Any]] = None


class AddPropResponse(BaseModel):
    ok: bool
    saved: bool
    duplicate: bool
    id: Optional[str] = None


class ModelMetricRow(BaseModel):
    prop_type: str
    total: int
    correct: int


class UserVsModelMetricRow(BaseModel):
    prop_type: str
    total: int
    user_total: int
    user_correct: int
    model_total: int
    model_correct: int


class UserVsModelWeeklyMetricRow(UserVsModelMetricRow):
    week_start: date


class ModelWeeklyMetricRow(ModelMetricRow):
    week_start: date
    accuracy: Optional[float] = None


def _model_to_dict(body: BaseModel) -> Dict[str, Any]:
    if hasattr(body, "model_dump"):
        return body.model_dump(exclude_none=True)  # pydantic v2
    return body.dict(exclude_none=True)  # pydantic v1


@router.get("/mlb/ping", response_model=PingResponse)
def ping_mlb():
    return {"sport": "mlb", "ok": True}


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
        return prepare_prop_submission(_model_to_dict(body))
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
        return predict_prepared_prop(_model_to_dict(body))
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
        return add_prop(_model_to_dict(body))
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
