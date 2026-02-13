"""Shared ping helpers for sport routers."""

from __future__ import annotations

from typing import Dict


def sport_ping(sport: str) -> Dict[str, object]:
    return {"sport": str(sport), "ok": True}
