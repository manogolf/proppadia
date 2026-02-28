from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class NhlPingResponse(BaseModel):
    sport: str
    ok: bool


class NhlDbPingResponse(BaseModel):
    ok: bool
    err: Optional[str] = None


class NhlGamecenterLandingResponse(BaseModel):
    ok: bool
    game_id: int
    data: Optional[Any] = None
    error: Optional[str] = None


class NhlErrorResponse(BaseModel):
    ok: bool
    error: str


class NhlDateRowsResponse(BaseModel):
    ok: bool
    date: Optional[str] = None
    count: Optional[int] = None
    rows: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


class NhlWideRowsResponse(BaseModel):
    ok: bool
    error: str


class NhlAddPropRequest(BaseModel):
    player_id: int
    player_name: Optional[str] = None
    team: Optional[str] = None
    team_id: Optional[int] = None
    game_id: int
    game_date: Optional[str] = None
    prop_type: str
    prop_value: float
    over_under: Optional[str] = "over"
    probability: Optional[float] = 0.5
    prop_source: Optional[str] = "nhl_user_added"
    user_id: Optional[str] = None


class NhlAddPropResponse(BaseModel):
    ok: bool
    saved: bool
    duplicate: bool
    id: Optional[str] = None


class NhlPropHistoryRow(BaseModel):
    id: str
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team: Optional[str] = None
    team_id: Optional[int] = None
    game_id: Optional[int] = None
    game_date: Optional[str] = None
    prop_type: Optional[str] = None
    prop_value: Optional[float] = None
    over_under: Optional[str] = None
    status: Optional[str] = None
    outcome: Optional[str] = None
    prop_source: Optional[str] = None
    confidence_score: Optional[float] = None
    predicted_outcome: Optional[str] = None
    prediction_timestamp: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    user_id: Optional[str] = None


class NhlPropHistoryResponse(BaseModel):
    ok: bool
    count: int
    total: int
    limit: int
    offset: int
    rows: List[NhlPropHistoryRow]


class NhlPlayerProfileInfo(BaseModel):
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team: Optional[str] = None
    team_id: Optional[int] = None


class NhlPlayerProfileResponse(BaseModel):
    player_info: NhlPlayerProfileInfo
    streaks: List[Dict[str, Any]]
    recent_props: List[Dict[str, Any]]
    stat_derived: List[Dict[str, Any]]
    training_summary: List[Dict[str, Any]]
    season_stats: Dict[str, Any]
    career_stats: Dict[str, Any]
