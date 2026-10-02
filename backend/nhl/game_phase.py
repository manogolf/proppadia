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
