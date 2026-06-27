from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MarketIdentityInput:
    date: str = ""
    game_id: Any = None
    player_id: Any = None
    player_name: str = ""
    team: str = ""
    opponent: str = ""
    prop_type: str = ""
    side: str = ""
    line: Any = None


@dataclass(frozen=True)
class MarketIdentityResult:
    canonical_market_key: str
    fallback_market_key: str
    identity_status: str
    identity_confidence: float
    identity_method: str
    fallback_used: bool
    ambiguity_reason: str = ""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _line_text(value: Any) -> str:
    try:
        if value in ("", None):
            return ""
        return f"{float(value):g}"
    except Exception:
        return _clean(value)


def _id_text(value: Any) -> str:
    try:
        if value in ("", None):
            return ""
        return str(int(float(value)))
    except Exception:
        return _clean(value)


def resolve_market_identity(value: MarketIdentityInput) -> MarketIdentityResult:
    game_id = _id_text(value.game_id)
    player_id = _id_text(value.player_id)
    prop_type = _clean(value.prop_type).lower()
    side = _clean(value.side).lower()
    line = _line_text(value.line)
    if game_id and player_id and prop_type and side and line:
        key = "|".join([_clean(value.date), game_id, player_id, prop_type, side, line])
        return MarketIdentityResult(key, "", "resolved_by_id", 1.0, "game_id_player_id_market", False)
    fallback_parts = [
        _clean(value.date),
        _clean(value.player_name).lower(),
        _clean(value.team).upper(),
        _clean(value.opponent).upper(),
        prop_type,
        side,
        line,
    ]
    fallback_key = "|".join(fallback_parts)
    if all(fallback_parts):
        return MarketIdentityResult("", fallback_key, "resolved_by_name_fallback", 0.55, "date_name_team_market", True)
    missing = []
    for label, item in (
        ("game_id", game_id),
        ("player_id", player_id),
        ("prop_type", prop_type),
        ("side", side),
        ("line", line),
    ):
        if not item:
            missing.append(label)
    return MarketIdentityResult("", fallback_key, "unresolved", 0.0, "insufficient_market_identity", True, "missing_" + ",".join(missing))
