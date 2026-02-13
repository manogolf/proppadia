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
