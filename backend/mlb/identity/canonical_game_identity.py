from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.mlb.identity.canonical_team_identity import canonical_team_code


@dataclass(frozen=True)
class GameIdentityInput:
    date: str = ""
    game_id: Any = None
    event_id: str = ""
    home_team: str = ""
    away_team: str = ""
    team: str = ""
    opponent: str = ""


@dataclass(frozen=True)
class GameIdentityResult:
    canonical_game_id: str
    canonical_home_team: str
    canonical_away_team: str
    canonical_game_key: str
    provider_event_id: str
    identity_status: str
    identity_confidence: float
    identity_method: str
    fallback_used: bool
    ambiguity_reason: str = ""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _int_text(value: Any) -> str:
    try:
        if value in ("", None):
            return ""
        return str(int(float(value)))
    except Exception:
        return _clean(value)


class GameIdentityResolver:
    """Resolve MLB game identity from canonical IDs or team/date aliases."""

    def resolve(self, value: GameIdentityInput) -> GameIdentityResult:
        game_id = _int_text(value.game_id)
        event_id = _clean(value.event_id)
        home = canonical_team_code(value.home_team).canonical_team
        away = canonical_team_code(value.away_team).canonical_team
        if not home or not away:
            team = canonical_team_code(value.team).canonical_team
            opponent = canonical_team_code(value.opponent).canonical_team
            if team and opponent:
                teams = sorted([team, opponent])
                away = teams[0]
                home = teams[1]
        game_key = self.game_key(value.date, home, away, game_id=game_id, event_id=event_id)
        if game_id:
            return GameIdentityResult(game_id, home, away, game_key, event_id, "resolved_by_id", 1.0, "game_id", False)
        if event_id and home and away:
            return GameIdentityResult("", home, away, game_key, event_id, "resolved_by_provider_id", 0.75, "event_id_plus_teams", True)
        if home and away and value.date:
            return GameIdentityResult("", home, away, game_key, event_id, "resolved_by_game", 0.65, "date_team_game_key", True)
        reason = "missing_game_id_and_team_context"
        if event_id:
            reason = "provider_event_unmapped"
        return GameIdentityResult("", home, away, game_key, event_id, "unresolved", 0.0, "missing_context", True, reason)

    @staticmethod
    def game_key(date: str, home_team: str, away_team: str, game_id: str = "", event_id: str = "") -> str:
        if game_id:
            return f"mlb_game:{game_id}"
        if date and home_team and away_team:
            teams = "-".join(sorted([home_team, away_team]))
            return f"{date}:{teams}"
        if event_id:
            return f"provider_event:{event_id}"
        return ""
