from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

from backend.mlb.identity.canonical_team_identity import canonical_team_code


@dataclass(frozen=True)
class PlayerIdentityInput:
    player_id: Any = None
    player_name: str = ""
    normalized_player_name: str = ""
    team: str = ""
    opponent: str = ""
    game_id: Any = None
    event_id: str = ""


@dataclass(frozen=True)
class PlayerIdentityResult:
    canonical_player_id: str
    canonical_player_name: str
    identity_status: str
    identity_confidence: float
    identity_method: str
    fallback_used: bool
    ambiguity_reason: str = ""


def normalize_player_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9 ]+", " ", text).lower()
    return " ".join(text.split())


def _int_text(value: Any) -> str:
    try:
        if value in ("", None):
            return ""
        return str(int(float(value)))
    except Exception:
        return str(value or "").strip()


class PlayerIdentityResolver:
    """Resolve player identity using ID first, then contextual alias fallback.

    The optional reference rows let callers build a shared resolver from local
    player mapping tables or same-date slate artifacts without embedding custom
    resolver logic in every script.
    """

    def __init__(self, reference_rows: Iterable[dict[str, Any]] | None = None) -> None:
        self.by_id: dict[str, dict[str, Any]] = {}
        self.by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.by_name_team: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        if reference_rows:
            for row in reference_rows:
                self.add_reference(row)

    def add_reference(self, row: dict[str, Any]) -> None:
        player_id = _int_text(row.get("player_id") or row.get("mlb_player_id"))
        name = str(row.get("player_name") or row.get("name") or row.get("canonical_player_name") or "").strip()
        norm_name = normalize_player_name(row.get("normalized_player_name") or name)
        team = canonical_team_code(row.get("team") or row.get("team_code") or row.get("pitcher_team")).canonical_team
        ref = {"player_id": player_id, "player_name": name, "normalized_player_name": norm_name, "team": team}
        if player_id:
            self.by_id[player_id] = ref
        if norm_name:
            self.by_name[norm_name].append(ref)
            if team:
                self.by_name_team[(norm_name, team)].append(ref)

    def resolve(self, value: PlayerIdentityInput) -> PlayerIdentityResult:
        player_id = _int_text(value.player_id)
        if player_id:
            ref = self.by_id.get(player_id, {})
            return PlayerIdentityResult(
                player_id,
                str(ref.get("player_name") or value.player_name or "").strip(),
                "resolved_by_id",
                1.0,
                "player_id",
                False,
            )
        norm_name = normalize_player_name(value.normalized_player_name or value.player_name)
        team = canonical_team_code(value.team).canonical_team
        if not norm_name:
            return PlayerIdentityResult("", "", "unresolved", 0.0, "missing_player_identity", False, "missing_player_name_and_id")
        if team:
            matches = self.by_name_team.get((norm_name, team), [])
            unique = self._unique(matches)
            if len(unique) == 1:
                row = unique[0]
                return PlayerIdentityResult(
                    row["player_id"],
                    str(row.get("player_name") or value.player_name or "").strip(),
                    "resolved_by_name_fallback",
                    0.82,
                    "normalized_name_plus_team",
                    True,
                )
            if len(unique) > 1:
                return PlayerIdentityResult("", value.player_name, "ambiguous", 0.0, "normalized_name_plus_team", True, "multiple_players_same_name_team")
        matches = self.by_name.get(norm_name, [])
        unique = self._unique(matches)
        if len(unique) == 1:
            row = unique[0]
            return PlayerIdentityResult(
                row["player_id"],
                str(row.get("player_name") or value.player_name or "").strip(),
                "resolved_by_name_fallback",
                0.65,
                "normalized_name_only",
                True,
            )
        if len(unique) > 1:
            return PlayerIdentityResult("", value.player_name, "ambiguous", 0.0, "normalized_name_only", True, "multiple_players_same_name")
        return PlayerIdentityResult("", value.player_name, "unresolved", 0.0, "normalized_name_unmatched", True, "name_not_found")

    @staticmethod
    def _unique(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in rows:
            key = str(row.get("player_id") or "")
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out
