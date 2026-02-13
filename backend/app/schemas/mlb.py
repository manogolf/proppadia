from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


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
    last_prop_date: Optional[date] = None


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


def model_to_dict(body: BaseModel) -> Dict[str, Any]:
    if hasattr(body, "model_dump"):
        return body.model_dump(exclude_none=True)  # pydantic v2
    return body.dict(exclude_none=True)  # pydantic v1
