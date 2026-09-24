"""Playable-terminal and rescheduled-identity contract for MLB Moneyline V1.

This module is deliberately independent of network and database code.  It
classifies the complete StatsAPI status tuple and reconciles repeated schedule
appearances by exact ``gamePk``.  Scores never participate in finality.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable


CONTRACT_VERSION = "MLB_MONEYLINE_PLAYABLE_TERMINAL_V1"
PLAYABLE_TERMINAL = "PLAYABLE_TERMINAL"
NONPLAYABLE_TERMINAL = "NONPLAYABLE_TERMINAL"
NONFINAL = "NONFINAL"
STATUS_CONFLICT = "STATUS_CONFLICT"
UNKNOWN_STATUS = "UNKNOWN_STATUS"

_ACCEPTED = {
    ("Final", "Final", "F", "F"),
    ("Final", "Game Over", "O", "O"),
}
_NONPLAYABLE_DETAILS = {
    "postponed", "cancelled", "canceled", "suspended", "delayed",
    "scheduled", "pre-game", "pregame", "warmup", "in progress",
}
_NONPLAYABLE_CODES = {"D", "DR", "C", "CR", "S", "P", "I", "IR", "W"}
_ACTIVE_DETAILS = {"scheduled", "pre-game", "pregame", "warmup", "in progress"}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def status_fields(value: dict[str, Any]) -> dict[str, str]:
    """Return the four authoritative status fields without supplying defaults."""
    status = value.get("status") if isinstance(value.get("status"), dict) else value
    return {
        "abstract_game_state": _text(status.get("abstractGameState")),
        "detailed_state": _text(status.get("detailedState")),
        "coded_game_state": _text(status.get("codedGameState")),
        "status_code": _text(status.get("statusCode")),
    }


@dataclass(frozen=True)
class PlayableTerminalDecision:
    classification: str
    reason: str
    fields: dict[str, str]

    @property
    def accepted(self) -> bool:
        return self.classification == PLAYABLE_TERMINAL

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "classification": self.classification,
            "reason": self.reason,
            **self.fields,
        }


def classify_playable_terminal(value: dict[str, Any]) -> PlayableTerminalDecision:
    """Classify one complete StatsAPI status tuple, failing closed on ambiguity."""
    fields = status_fields(value)
    abstract = fields["abstract_game_state"]
    detailed = fields["detailed_state"]
    coded = fields["coded_game_state"]
    status_code = fields["status_code"]
    exact = (abstract, detailed, coded, status_code)

    # Explicitly non-playable detail/code always wins over broad abstract Final.
    if detailed.casefold() in _NONPLAYABLE_DETAILS or coded.upper() in _NONPLAYABLE_CODES \
            or status_code.upper() in _NONPLAYABLE_CODES:
        classification = NONFINAL if detailed.casefold() in _ACTIVE_DETAILS else NONPLAYABLE_TERMINAL
        return PlayableTerminalDecision(
            classification, "EXPLICIT_NONPLAYABLE_STATUS_OVERRIDES_ABSTRACT_STATE", fields,
        )
    if exact in _ACCEPTED:
        return PlayableTerminalDecision(
            PLAYABLE_TERMINAL, "EXPLICIT_PLAYABLE_TERMINAL_COMBINATION", fields,
        )
    if not all(exact):
        return PlayableTerminalDecision(
            UNKNOWN_STATUS, "INCOMPLETE_AUTHORITATIVE_STATUS_TUPLE", fields,
        )
    if abstract == "Final" or detailed in {"Final", "Game Over"} or coded in {"F", "O"} \
            or status_code in {"F", "O"}:
        return PlayableTerminalDecision(
            STATUS_CONFLICT, "CONFLICTING_TERMINAL_STATUS_FIELDS", fields,
        )
    return PlayableTerminalDecision(UNKNOWN_STATUS, "UNRECOGNIZED_STATUS_COMBINATION", fields)


def _relation_fields(game: dict[str, Any]) -> dict[str, str]:
    return {
        "reschedule_date": _text(game.get("rescheduleDate")),
        "rescheduled_from": _text(game.get("rescheduledFrom")),
        "rescheduled_game_date": _text(game.get("rescheduledGameDate")),
        "resume_date": _text(game.get("resumeDate")),
        "resumed_from": _text(game.get("resumedFrom")),
        "description": _text(game.get("description")),
    }


def _teams(game: dict[str, Any]) -> tuple[int, ...]:
    result: list[int] = []
    for side in ("away", "home"):
        value = (((game.get("teams") or {}).get(side) or {}).get("team") or {}).get("id")
        try:
            result.append(int(value))
        except (TypeError, ValueError):
            pass
    return tuple(result)


@dataclass(frozen=True)
class ScheduleAppearance:
    source_order: int
    game_pk: int
    game_date_utc: str
    official_date: str
    game_number: int
    double_header: str
    team_ids: tuple[int, ...]
    status: PlayableTerminalDecision
    relations: dict[str, str]
    raw: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_order": self.source_order,
            "game_pk": None if self.game_pk <= 0 else self.game_pk,
            "game_date_utc": self.game_date_utc,
            "official_date": self.official_date,
            "game_number": self.game_number,
            "double_header": self.double_header,
            "team_ids": list(self.team_ids),
            "status": self.status.as_dict(),
            "relations": dict(self.relations),
        }


@dataclass(frozen=True)
class ScheduleGameDecision:
    game_pk: int
    appearances: tuple[ScheduleAppearance, ...]
    selected: ScheduleAppearance | None
    decision: str
    reason: str
    dependency_team_ids: tuple[int, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "game_pk": None if self.game_pk <= 0 else self.game_pk,
            "decision": self.decision,
            "reason": self.reason,
            "dependency_team_ids": list(self.dependency_team_ids),
            "selected_source_order": None if self.selected is None else self.selected.source_order,
            "appearances": [item.as_dict() for item in self.appearances],
        }


def schedule_appearances(payload: dict[str, Any]) -> tuple[ScheduleAppearance, ...]:
    result: list[ScheduleAppearance] = []
    order = 0
    for block in payload.get("dates") or []:
        for game in block.get("games") or []:
            order += 1
            try:
                game_pk = int(game["gamePk"])
            except (KeyError, TypeError, ValueError):
                # Negative source order is an internal unique key only. The
                # receipt emits null and the reconciliation forces global
                # fail-closed dependency handling; identity is never inferred.
                game_pk = -order
            try:
                game_number = int(game.get("gameNumber") or 1)
            except (TypeError, ValueError):
                game_number = 1
            result.append(ScheduleAppearance(
                source_order=order,
                game_pk=game_pk,
                game_date_utc=_text(game.get("gameDate")),
                official_date=_text(game.get("officialDate")),
                game_number=game_number,
                double_header=_text(game.get("doubleHeader")),
                team_ids=_teams(game),
                status=(
                    classify_playable_terminal(game) if game_pk > 0 else
                    PlayableTerminalDecision(
                        UNKNOWN_STATUS, "EXACT_GAME_PK_MISSING", status_fields(game),
                    )
                ),
                relations=_relation_fields(game),
                raw=game,
            ))
    return tuple(result)


def _same_identity(left: ScheduleAppearance, right: ScheduleAppearance) -> bool:
    return (
        left.game_pk == right.game_pk
        and left.game_date_utc == right.game_date_utc
        and left.official_date == right.official_date
        and left.game_number == right.game_number
        and left.team_ids == right.team_ids
    )


def _related(left: ScheduleAppearance, right: ScheduleAppearance) -> bool:
    left_dates = {left.game_date_utc, left.relations["reschedule_date"], left.relations["resume_date"]}
    right_origins = {right.relations["rescheduled_from"], right.relations["resumed_from"]}
    right_dates = {right.game_date_utc, right.relations["reschedule_date"], right.relations["resume_date"]}
    left_origins = {left.relations["rescheduled_from"], left.relations["resumed_from"]}
    left_dates.discard(""); right_origins.discard(""); right_dates.discard(""); left_origins.discard("")
    return bool(left_dates & right_origins or right_dates & left_origins
                or left.relations["reschedule_date"] == right.game_date_utc != ""
                or right.relations["reschedule_date"] == left.game_date_utc != "")


def reconcile_schedule_by_game_pk(payload: dict[str, Any]) -> tuple[ScheduleGameDecision, ...]:
    """Reconcile all appearances, selecting at most one playable candidate per gamePk."""
    grouped: dict[int, list[ScheduleAppearance]] = {}
    for appearance in schedule_appearances(payload):
        grouped.setdefault(appearance.game_pk, []).append(appearance)
    decisions: list[ScheduleGameDecision] = []
    for game_pk in sorted(grouped):
        items = tuple(grouped[game_pk])
        teams = tuple(sorted({team for item in items for team in item.team_ids}))
        if game_pk <= 0:
            decisions.append(ScheduleGameDecision(
                game_pk, items, None, "QUARANTINED", "EXACT_GAME_PK_MISSING", (),
            ))
            continue
        playable = [item for item in items if item.status.accepted]
        uncertain = [item for item in items if item.status.classification in {STATUS_CONFLICT, UNKNOWN_STATUS}]
        suspended = [item for item in items if item.status.fields["detailed_state"].casefold() == "suspended"]

        if playable:
            selected = playable[-1]
            incompatible = [item for item in items
                            if item is not selected
                            if not _same_identity(item, selected) and not _related(item, selected)]
            if incompatible or uncertain:
                decisions.append(ScheduleGameDecision(
                    game_pk, items, None, "QUARANTINED",
                    "CONFLICTING_REPEATED_SCHEDULE_APPEARANCES", teams,
                ))
            else:
                decisions.append(ScheduleGameDecision(
                    game_pk, items, selected, "FETCH_PLAYABLE_FINAL",
                    "ONE_EXACT_GAME_PK_PLAYABLE_TERMINAL_CANDIDATE", (),
                ))
            continue
        if uncertain:
            decisions.append(ScheduleGameDecision(
                game_pk, items, None, "QUARANTINED", "UNKNOWN_OR_CONFLICTING_STATUS", teams,
            ))
        elif suspended:
            decisions.append(ScheduleGameDecision(
                game_pk, items, None, "QUARANTINED", "SUSPENDED_GAME_OUTCOME_UNRESOLVED", teams,
            ))
        else:
            # Scheduled, pregame, live, postponed, and cancelled appearances do
            # not attach outcomes. A known makeup remains the same exact gamePk
            # and can become eligible only in a later playable-terminal response.
            decisions.append(ScheduleGameDecision(
                game_pk, items, None, "REJECTED_NONPLAYABLE", "NO_PLAYABLE_TERMINAL_APPEARANCE", (),
            ))
    return tuple(decisions)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def receipt_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def dependency_blocked_current_games(
    current_schedule: dict[str, Any], dependency_team_ids: Iterable[int], *, block_all: bool = False,
) -> set[int]:
    blocked_teams = {int(value) for value in dependency_team_ids}
    blocked: set[int] = set()
    for appearance in schedule_appearances(current_schedule):
        if appearance.game_pk > 0 and (block_all or blocked_teams.intersection(appearance.team_ids)):
            blocked.add(appearance.game_pk)
    return blocked
