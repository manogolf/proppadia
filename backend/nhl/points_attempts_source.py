"""Validated shot-attempt counts derived from official NHL Gamecenter PBP.

NHL boxscores commonly omit skater shot attempts.  PBP attempts are goals,
shots on goal, missed shots, and blocked shots, attributed to the shooter.
This module returns counts only when the final PBP event set reconciles to the
official boxscore's player-level shots on goal; otherwise callers must retain
the attempt value as unknown.
"""
from __future__ import annotations

from collections import Counter
from typing import Any


def preserve_existing_attempts_conflict_assignment(
    target_relation: str = "nhl.skater_game_logs_raw",
) -> str:
    """Return the raw-table UPSERT assignment that never replaces known attempts.

    Raw shot-attempt values may have independent official-PBP verification. A
    routine stage promotion may fill an unknown (NULL) raw value, but it must
    not replace a populated value, including a verified zero. Corrections to a
    populated value belong to the guarded official-source recovery path.
    """
    relation_parts = target_relation.split(".")
    if len(relation_parts) not in (1, 2) or not all(
        part.isidentifier() for part in relation_parts
    ):
        raise ValueError("INVALID_ATTEMPTS_TARGET_RELATION")
    return (
        "shot_attempts = COALESCE("
        f"{target_relation}.shot_attempts, EXCLUDED.shot_attempts)"
    )


def replace_staged_attempts_conflict_assignment() -> str:
    """Treat each stage write as a fresh source snapshot, including NULLs."""
    return "shot_attempts = EXCLUDED.shot_attempts"


def compare_reconciled_attempts(
    *, game_id: int, player_id: int, raw_attempts: int | None,
    official_attempts: int | None, raw_row_present: bool,
) -> dict[str, Any]:
    """Compare a reconciled PBP observation with raw state without mutation.

    A non-NULL raw value is not presumed verified. Unknown official input is
    explicitly classified as unavailable and never creates a discrepancy.
    """
    if official_attempts is None:
        status = "OFFICIAL_SOURCE_UNKNOWN"
    elif raw_attempts is None:
        status = "NULL_FILL_CANDIDATE"
    elif int(raw_attempts) == int(official_attempts):
        status = "MATCHES_RECONCILED_OFFICIAL_SOURCE"
    else:
        status = "CONFLICT_PRESERVE_RAW_PENDING_CORRECTION"
    return {
        "game_id": int(game_id),
        "player_id": int(player_id),
        "raw_row_present": bool(raw_row_present),
        "raw_attempts": None if raw_attempts is None else int(raw_attempts),
        "official_attempts": (
            None if official_attempts is None else int(official_attempts)
        ),
        "status": status,
    }


ATTEMPT_TYPES = frozenset({"goal", "shot-on-goal", "missed-shot", "blocked-shot"})
ON_GOAL_TYPES = frozenset({"goal", "shot-on-goal"})


def _boxscore_skaters(boxscore: dict[str, Any]) -> tuple[dict[int, tuple[int, int]], set[int]]:
    player_rows: dict[int, tuple[int, int]] = {}
    team_ids: set[int] = set()
    stats = boxscore.get("playerByGameStats") or {}
    for side in ("homeTeam", "awayTeam"):
        team_id = (boxscore.get(side) or {}).get("id")
        if team_id is None:
            raise ValueError("ATTEMPTS_BOXSCORE_TEAM_ID_MISSING")
        team_id = int(team_id)
        if team_id in team_ids:
            raise ValueError("ATTEMPTS_BOXSCORE_TEAM_ID_DUPLICATE")
        team_ids.add(team_id)
        team_stats = stats.get(side)
        if not isinstance(team_stats, dict):
            raise ValueError("ATTEMPTS_BOXSCORE_SKATER_SECTION_MISSING")
        defense = team_stats.get("defense") or []
        defensemen = team_stats.get("defensemen") or []
        if defense and defensemen:
            raise ValueError("ATTEMPTS_BOXSCORE_DEFENSE_SCHEMA_CONFLICT")
        for rows in (team_stats.get("forwards") or [], defense or defensemen):
            if not isinstance(rows, list):
                raise ValueError("ATTEMPTS_BOXSCORE_SKATER_SECTION_INVALID")
            for row in rows:
                raw_id, raw_sog = row.get("playerId"), row.get("sog")
                if raw_id is None or raw_sog is None:
                    raise ValueError("ATTEMPTS_BOXSCORE_SKATER_ID_OR_SOG_MISSING")
                player_id, sog = int(raw_id), int(raw_sog)
                if player_id in player_rows or sog < 0:
                    raise ValueError("ATTEMPTS_BOXSCORE_SKATER_ID_OR_SOG_INVALID")
                player_rows[player_id] = (team_id, sog)
    if not player_rows:
        raise ValueError("ATTEMPTS_BOXSCORE_SKATERS_EMPTY")
    return player_rows, team_ids


def derive_attempts_from_final_pbp(
    boxscore: dict[str, Any], play_by_play: dict[str, Any], *, game_id: int,
) -> dict[int, int]:
    """Return official per-player attempt counts or raise when source is incomplete.

    Missing, malformed, duplicate, or cross-team events are not interpreted as
    zero.  A returned zero means the player's complete final-game PBP contains
    no attempt event.
    """
    expected_game_id = int(game_id)
    if int(boxscore.get("id") or -1) != expected_game_id:
        raise ValueError("ATTEMPTS_BOXSCORE_GAME_ID_MISMATCH")
    if int(play_by_play.get("id") or -1) != expected_game_id:
        raise ValueError("ATTEMPTS_PBP_GAME_ID_MISMATCH")
    if play_by_play.get("gameState") not in {"OFF", "FINAL"}:
        raise ValueError("ATTEMPTS_PBP_GAME_NOT_FINAL")

    players, box_teams = _boxscore_skaters(boxscore)
    pbp_teams = {
        int((play_by_play.get(side) or {}).get("id") or -1)
        for side in ("homeTeam", "awayTeam")
    }
    if pbp_teams != box_teams:
        raise ValueError("ATTEMPTS_PBP_TEAM_ID_MISMATCH")
    plays = play_by_play.get("plays")
    if not isinstance(plays, list) or not plays:
        raise ValueError("ATTEMPTS_PBP_PLAYS_MISSING")

    attempts: Counter[int] = Counter()
    on_goal: Counter[int] = Counter()
    event_ids: set[int] = set()
    attempt_event_count = 0
    for play in plays:
        kind = play.get("typeDescKey")
        if kind not in ATTEMPT_TYPES:
            continue
        attempt_event_count += 1
        details = play.get("details") or {}
        raw_player = details.get("scoringPlayerId") if kind == "goal" else details.get("shootingPlayerId")
        raw_team = details.get("eventOwnerTeamId")
        raw_event_id = play.get("eventId")
        if raw_player is None or raw_team is None or raw_event_id is None:
            raise ValueError("ATTEMPTS_PBP_ATTEMPT_EVENT_IDENTITY_MISSING")
        player_id, team_id, event_id = int(raw_player), int(raw_team), int(raw_event_id)
        if event_id in event_ids:
            raise ValueError("ATTEMPTS_PBP_DUPLICATE_EVENT_ID")
        event_ids.add(event_id)
        if player_id not in players or players[player_id][0] != team_id:
            raise ValueError("ATTEMPTS_PBP_SHOOTER_IDENTITY_INVALID")
        attempts[player_id] += 1
        if kind in ON_GOAL_TYPES:
            on_goal[player_id] += 1

    if attempt_event_count == 0:
        raise ValueError("ATTEMPTS_PBP_NO_ATTEMPT_EVENTS")
    if any(on_goal.get(player_id, 0) != sog for player_id, (_team, sog) in players.items()):
        raise ValueError("ATTEMPTS_PBP_PLAYER_SOG_RECONCILIATION_FAILED")
    return {player_id: attempts.get(player_id, 0) for player_id in players}
