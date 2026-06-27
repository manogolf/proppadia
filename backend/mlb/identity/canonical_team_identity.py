from __future__ import annotations

from dataclasses import dataclass
from typing import Any


TEAM_ALIASES = {
    "ARI": "ARI",
    "AZ": "ARI",
    "ATH": "ATH",
    "OAK": "ATH",
    "CHC": "CHC",
    "CUBS": "CHC",
    "CHW": "CWS",
    "CWS": "CWS",
    "SOX": "CWS",
    "KC": "KC",
    "KCR": "KC",
    "LAD": "LAD",
    "LA": "LAD",
    "SD": "SD",
    "SDP": "SD",
    "SF": "SF",
    "SFG": "SF",
    "TB": "TB",
    "TBR": "TB",
    "WSH": "WSH",
    "WSN": "WSH",
    "NYY": "NYY",
    "NYA": "NYY",
    "NYM": "NYM",
    "NYN": "NYM",
}

KNOWN_TEAMS = {
    "ARI",
    "ATL",
    "BAL",
    "BOS",
    "CHC",
    "CIN",
    "CLE",
    "COL",
    "CWS",
    "DET",
    "HOU",
    "KC",
    "LAA",
    "LAD",
    "MIA",
    "MIL",
    "MIN",
    "NYM",
    "NYY",
    "ATH",
    "PHI",
    "PIT",
    "SD",
    "SEA",
    "SF",
    "STL",
    "TB",
    "TEX",
    "TOR",
    "WSH",
}


@dataclass(frozen=True)
class TeamIdentityResult:
    canonical_team: str
    identity_status: str
    identity_confidence: float
    identity_method: str
    fallback_used: bool
    ambiguity_reason: str = ""


def _clean(value: Any) -> str:
    return str(value or "").strip().upper()


def canonical_team_code(value: Any) -> TeamIdentityResult:
    raw = _clean(value)
    if not raw:
        return TeamIdentityResult("", "unresolved", 0.0, "missing_team", False, "missing_team")
    team = TEAM_ALIASES.get(raw, raw)
    if team in KNOWN_TEAMS:
        method = "alias_map" if team != raw else "canonical_code"
        return TeamIdentityResult(team, "resolved_by_id", 1.0 if method == "canonical_code" else 0.95, method, method == "alias_map")
    return TeamIdentityResult(raw, "unresolved", 0.0, "unknown_team_code", True, "unknown_team_code")
