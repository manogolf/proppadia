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

import psycopg


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


def capture_stage_attempt_conflicts(
    dsn: str, *, slate_date: str, stage: str,
) -> dict[str, Any]:
    """Read exact-key staged attempts that conflict with populated raw values."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT raw.game_id::bigint, raw.player_id::bigint,
                   raw.shot_attempts::integer, stage.shot_attempts::integer
              FROM nhl.skater_game_logs_raw AS raw
              JOIN nhl.import_skater_logs_stage AS stage
                ON stage.game_id=raw.game_id AND stage.player_id=raw.player_id
             WHERE raw.game_date=%s::date AND stage.game_date=%s::date
               AND raw.shot_attempts IS NOT NULL
               AND stage.shot_attempts IS NOT NULL
               AND raw.shot_attempts IS DISTINCT FROM stage.shot_attempts
             ORDER BY raw.game_id, raw.player_id
            """,
            (slate_date, slate_date),
        )
        conflicts = [
            {
                "game_id": int(game_id),
                "player_id": int(player_id),
                "existing_shot_attempts": int(existing),
                "incoming_shot_attempts": int(incoming),
                "status": "CONFLICT_PRESERVED_EXISTING_VALUE",
            }
            for game_id, player_id, existing, incoming in cursor.fetchall()
        ]
    return {
        "schema_version": "NHL_SHOT_ATTEMPTS_SOURCE_CONFLICTS_V1",
        "stage": stage,
        "slate_date": slate_date,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
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


def _boxscore_goalies(boxscore: dict[str, Any]) -> dict[int, int]:
    """Goalie id -> team id; a goalie's attempt is not a skater attempt."""
    goalies: dict[int, int] = {}
    stats = boxscore.get("playerByGameStats") or {}
    for side in ("homeTeam", "awayTeam"):
        team_id = int((boxscore.get(side) or {}).get("id") or -1)
        for row in (stats.get(side) or {}).get("goalies") or []:
            if row.get("playerId") is not None:
                goalies[int(row["playerId"])] = team_id
    return goalies


def reconcile_attempts_per_skater(
    boxscore: dict[str, Any], play_by_play: dict[str, Any], *, game_id: int,
) -> dict[str, Any]:
    """Per-skater official attempt counts from final PBP (P2.37).

    Game-level identity, finality and event-id problems still raise. Within a
    valid game each skater is accepted only when his PBP shots on goal equal
    the boxscore; a mismatched skater stays unknown. A goalie's attempt is
    skipped. An attempt whose shooter is not a skater or goalie of the owning
    team cannot be assigned, so every skater of that team stays unknown.
    Returns ``{"attempts": {player: count}, "unresolved": {player: reason},
    "skipped_goalie_events": n}``.
    """
    expected_game_id = int(game_id)
    if int(boxscore.get("id") or -1) != expected_game_id:
        raise ValueError("ATTEMPTS_BOXSCORE_GAME_ID_MISMATCH")
    if int(play_by_play.get("id") or -1) != expected_game_id:
        raise ValueError("ATTEMPTS_PBP_GAME_ID_MISMATCH")
    if play_by_play.get("gameState") not in {"OFF", "FINAL"}:
        raise ValueError("ATTEMPTS_PBP_GAME_NOT_FINAL")

    players, box_teams = _boxscore_skaters(boxscore)
    goalies = _boxscore_goalies(boxscore)
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
    unassigned_teams: set[int] = set()
    attempt_event_count = skipped_goalie_events = 0
    for play in plays:
        kind = play.get("typeDescKey")
        if kind not in ATTEMPT_TYPES:
            continue
        attempt_event_count += 1
        details = play.get("details") or {}
        raw_player = details.get("scoringPlayerId") if kind == "goal" else details.get("shootingPlayerId")
        raw_team = details.get("eventOwnerTeamId")
        raw_event_id = play.get("eventId")
        if raw_team is None or raw_event_id is None:
            raise ValueError("ATTEMPTS_PBP_ATTEMPT_EVENT_IDENTITY_MISSING")
        team_id, event_id = int(raw_team), int(raw_event_id)
        if team_id not in box_teams:
            raise ValueError("ATTEMPTS_PBP_EVENT_TEAM_INVALID")
        if event_id in event_ids:
            raise ValueError("ATTEMPTS_PBP_DUPLICATE_EVENT_ID")
        event_ids.add(event_id)
        player_id = None if raw_player is None else int(raw_player)
        if player_id is not None and goalies.get(player_id) == team_id:
            skipped_goalie_events += 1
            continue
        if player_id is None or player_id not in players or players[player_id][0] != team_id:
            unassigned_teams.add(team_id)
            continue
        attempts[player_id] += 1
        if kind in ON_GOAL_TYPES:
            on_goal[player_id] += 1

    if attempt_event_count == 0:
        raise ValueError("ATTEMPTS_PBP_NO_ATTEMPT_EVENTS")
    accepted: dict[int, int] = {}
    unresolved: dict[int, str] = {}
    for player_id, (team_id, sog) in players.items():
        if team_id in unassigned_teams:
            unresolved[player_id] = "ATTEMPTS_PBP_TEAM_ATTEMPT_UNASSIGNED"
        elif on_goal.get(player_id, 0) != sog:
            unresolved[player_id] = "ATTEMPTS_PBP_PLAYER_SOG_RECONCILIATION_FAILED"
        else:
            accepted[player_id] = attempts.get(player_id, 0)
    if not accepted:
        raise ValueError("ATTEMPTS_PBP_NO_RECONCILED_SKATERS")
    return {"attempts": accepted, "unresolved": unresolved,
            "skipped_goalie_events": skipped_goalie_events}


def derive_attempts_from_final_pbp(
    boxscore: dict[str, Any], play_by_play: dict[str, Any], *, game_id: int,
) -> dict[int, int]:
    """Return official attempt counts for every skater that reconciles.

    Skaters whose PBP shots on goal differ from the boxscore are left out, so
    callers keep them unknown; they are never interpreted as zero. A returned
    zero means the skater reconciled and has no attempt event.
    """
    return reconcile_attempts_per_skater(boxscore, play_by_play, game_id=game_id)["attempts"]
