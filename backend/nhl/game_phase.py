"""Canonical NHL game type and evaluation phase semantics."""
from __future__ import annotations


GAME_TYPE_PHASE = {
    1: "PRESEASON",
    2: "REGULAR_SEASON",
    3: "POSTSEASON",
}


def phase_for_game_type(game_type_code: object) -> str:
    """Return phase from the official NHL game type; unknown values fail closed."""
    try:
        return GAME_TYPE_PHASE[int(game_type_code)]
    except (KeyError, TypeError, ValueError):
        return "UNKNOWN_GAME_TYPE"


def regular_season_evaluation_eligible(game_type_code: object) -> bool:
    return phase_for_game_type(game_type_code) == "REGULAR_SEASON"


# ---- Season naming (NHL Backlog P2.25) ----
# A season is named by its start year. A game's season is the first four
# digits of its game ID (2026020141 is season 2026); a date belongs to the
# season that starts that year from July onward, so January-June 2027 is 2026.
AUTHORIZED_SEASONS = frozenset({2026})
"""Seasons the production lanes may run. Adding a season is a deliberate
rollover decision (which models carry over and requalify); until then a new
season fails loudly with NHL_SEASON_NOT_AUTHORIZED instead of finding nothing."""


def season_for_date(value: object) -> int:
    """Season of a YYYY-MM-DD date (string or date): July onward is that year's."""
    text = str(value)[:10]
    year, month = int(text[:4]), int(text[5:7])
    if not 1 <= month <= 12 or text[4] != "-" or text[7] != "-":
        raise ValueError(f"NHL_SEASON_DATE_INVALID:{value}")
    return year if month >= 7 else year - 1


def season_for_game_id(game_id: object) -> int:
    """Season encoded in an official NHL game ID (its first four digits)."""
    text = str(int(game_id))
    if len(text) != 10:
        raise ValueError(f"NHL_GAME_ID_INVALID:{game_id}")
    return int(text[:4])


def require_authorized_season(season: object) -> int:
    """Return the season if production lanes are authorized for it, else raise."""
    value = int(season)
    if value not in AUTHORIZED_SEASONS:
        raise RuntimeError(f"NHL_SEASON_NOT_AUTHORIZED:{value}")
    return value


def authorized_season_for_date(value: object) -> int:
    """The date's season, required to be authorized."""
    return require_authorized_season(season_for_date(value))
