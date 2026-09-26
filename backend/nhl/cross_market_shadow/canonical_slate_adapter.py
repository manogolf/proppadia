"""Adapt the baseline NHL canonical slate to the cross-market schedule schema."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from backend.nhl.daily_capture import CanonicalGame


SCHEDULE_COLUMNS = [
    "canonical_season", "slate_date", "game_id", "game_date",
    "scheduled_start_time_utc", "home_team_id", "home_team", "away_team_id",
    "away_team", "game_status", "game_type_code",
]
_GAME_TYPES = {1, 2, 3}


def _utc(value: str) -> str:
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("CANONICAL_START_MUST_INCLUDE_TIMEZONE")
    return stamp.astimezone(timezone.utc).isoformat()


def _raw_games(payload: Mapping[str, Any]) -> dict[int, Mapping[str, Any]]:
    values: list[Mapping[str, Any]] = []
    weeks = payload.get("gameWeek")
    if isinstance(weeks, list):
        for day in weeks:
            if isinstance(day, Mapping) and isinstance(day.get("games"), list):
                values.extend(g for g in day["games"] if isinstance(g, Mapping))
    elif isinstance(payload.get("games"), list):
        values.extend(g for g in payload["games"] if isinstance(g, Mapping))
    result: dict[int, Mapping[str, Any]] = {}
    for game in values:
        raw_id = game.get("id") or game.get("gamePk") or game.get("gameId")
        if raw_id is None:
            raise ValueError("RAW_CANONICAL_GAME_ID_MISSING")
        game_id = int(raw_id)
        if game_id in result:
            raise ValueError(f"DUPLICATE_RAW_CANONICAL_GAME:{game_id}")
        result[game_id] = game
    return result


def adapt_canonical_slate(
    canonical_games: Sequence[CanonicalGame],
    raw_schedule_payload: Mapping[str, Any],
    *, slate_date: str,
    canonical_season: int,
) -> pd.DataFrame:
    """Project validated baseline game identities into the cross-market schema.

    The canonical game objects remain authoritative for identity and orientation.
    The corresponding raw baseline records supply only game type and game state,
    fields not carried on the shared ``CanonicalGame`` model.
    """
    if not slate_date or int(canonical_season) <= 0:
        raise ValueError("CANONICAL_SLATE_BINDING_REQUIRED")
    ids = [int(game.game_id) for game in canonical_games]
    if len(ids) != len(set(ids)):
        raise ValueError("DUPLICATE_CANONICAL_GAME_ID")
    raw_by_id = _raw_games(raw_schedule_payload)
    if not set(ids).issubset(raw_by_id):
        raise ValueError("CANONICAL_RAW_GAME_SET_MISMATCH")

    rows = []
    for game in canonical_games:
        raw = raw_by_id[int(game.game_id)]
        home, away = raw.get("homeTeam") or {}, raw.get("awayTeam") or {}
        try:
            home_id, away_id = int(game.home_team_id), int(game.away_team_id)
            game_type = int(raw["gameType"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"CANONICAL_IDENTITY_OR_GAME_TYPE_MISSING:{game.game_id}") from None
        if home_id <= 0 or away_id <= 0 or not game.home_team or not game.away_team:
            raise ValueError(f"CANONICAL_TEAM_IDENTITY_MISSING:{game.game_id}")
        if game_type not in _GAME_TYPES:
            raise ValueError(f"UNKNOWN_CANONICAL_GAME_TYPE:{game.game_id}")
        start = _utc(game.start_time_utc)
        raw_start = raw.get("startTimeUTC") or raw.get("gameDate")
        if (home.get("id") is None or away.get("id") is None
                or int(home["id"]) != home_id or int(away["id"]) != away_id
                or str(home.get("abbrev") or home.get("triCode") or "") != game.home_team
                or str(away.get("abbrev") or away.get("triCode") or "") != game.away_team
                or raw_start is None or _utc(str(raw_start)) != start):
            raise ValueError(f"CANONICAL_RAW_IDENTITY_MISMATCH:{game.game_id}")
        raw_state = str(raw.get("gameState") or "").strip().upper()
        if not raw_state:
            raise ValueError(f"CANONICAL_GAME_STATE_MISSING:{game.game_id}")
        # FUT is NHL's explicit not-started state; preserve all other states.
        status = "SCHEDULED" if raw_state == "FUT" else raw_state
        rows.append({
            "canonical_season": int(canonical_season), "slate_date": slate_date,
            "game_id": int(game.game_id), "game_date": slate_date,
            "scheduled_start_time_utc": start,
            "home_team_id": home_id, "home_team": game.home_team,
            "away_team_id": away_id, "away_team": game.away_team,
            "game_status": status, "game_type_code": game_type,
        })
    return pd.DataFrame(rows, columns=SCHEDULE_COLUMNS)
