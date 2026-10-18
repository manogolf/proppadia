"""Inactive proposal for the source-observed postseason delayed-start tuple.

This module is intentionally not imported by RuntimeScheduleAuthority. It is a
review fixture for the narrow tuple extension and stays inert until a separate
authorized change wires it into the runtime allowlist.
"""
from __future__ import annotations

from typing import Any, Mapping

from backend.mlb.season_transition.contract_v1 import (
    PhaseContractError,
    classify_schedule_game,
)


class ProposedScheduleStatusError(RuntimeError):
    pass


EXISTING_ACCEPTED_TUPLES = frozenset({
    ("Preview", "S", "S"),
    ("Preview", "P", "P"),
    ("Live", "I", "I"),
    ("Live", "P", "PW"),
    ("Final", "F", "F"),
    ("Final", "O", "O"),
})
DELAYED_START_TUPLE = ("Preview", "P", "PI", "Delayed Start")


def validate_proposed_new_postseason_status(
    game: Mapping[str, Any], *, expected_game_pk: int
) -> None:
    """Apply the proposed policy to a complete exact-game schedule record."""
    try:
        game_pk = int(game.get("gamePk"))
    except (TypeError, ValueError):
        raise ProposedScheduleStatusError("GAME_PK_MISSING") from None
    if game_pk != int(expected_game_pk):
        raise ProposedScheduleStatusError("GAME_PK_CONFLICT")
    try:
        classification = classify_schedule_game(game, season=2026)
    except PhaseContractError as exc:
        raise ProposedScheduleStatusError(f"IDENTITY_CONFLICT:{exc}") from None
    if classification.season != 2026 or classification.phase != "POSTSEASON":
        raise ProposedScheduleStatusError("POSTSEASON_IDENTITY_CONFLICT")
    if not str(game.get("seriesDescription") or "").strip():
        raise ProposedScheduleStatusError("POSTSEASON_ROUND_MISSING")
    nested = ((game.get("gameData") or {}).get("game") or {})
    if nested.get("pk") not in (None, ""):
        try:
            if int(nested["pk"]) != game_pk:
                raise ProposedScheduleStatusError("GAME_PK_CONFLICT")
        except (TypeError, ValueError):
            raise ProposedScheduleStatusError("GAME_PK_CONFLICT") from None
    if nested.get("type") not in (None, "", game.get("gameType")):
        raise ProposedScheduleStatusError("GAME_TYPE_CONFLICT")
    if nested.get("season") not in (None, ""):
        try:
            if int(nested["season"]) != int(game.get("season")):
                raise ProposedScheduleStatusError("SEASON_CONFLICT")
        except (TypeError, ValueError):
            raise ProposedScheduleStatusError("SEASON_CONFLICT") from None

    status = game.get("status")
    if not isinstance(status, dict):
        raise ProposedScheduleStatusError("STATUS_MISSING")
    abstract = str(status.get("abstractGameState") or "")
    coded = str(status.get("codedGameState") or "")
    detailed = str(status.get("detailedState") or "")
    status_code = str(status.get("statusCode") or "")
    tuple3 = (abstract, coded, status_code)
    abstract_code = str(status.get("abstractGameCode") or "")
    expected_abstract_code = {"Preview": "P", "Live": "L", "Final": "F"}.get(abstract)
    if abstract_code and abstract_code != expected_abstract_code:
        raise ProposedScheduleStatusError("STATUS_CONFLICT")
    if (abstract, coded, status_code, detailed) == DELAYED_START_TUPLE:
        if abstract_code != "P":
            raise ProposedScheduleStatusError("STATUS_CONFLICT")
        return
    if detailed and tuple3 in EXISTING_ACCEPTED_TUPLES:
        return
    raise ProposedScheduleStatusError("STATUS_CONFLICT")
