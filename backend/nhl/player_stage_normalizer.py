"""Deterministic, fail-closed normalization for the NHL player stage."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


PLAYER_STAGE_FIELDS = (
    "player_id",
    "team_id",
    "first_name",
    "last_name",
    "position",
    "shoots_catches",
    "active",
)

_MERGED_FIELDS = PLAYER_STAGE_FIELDS[1:]
_POSITION_MAP = {
    "G": "G", "GOALIE": "G",
    "D": "D", "LD": "D", "RD": "D", "DEF": "D",
    "DEFENSE": "D", "DEFENCE": "D",
    "C": "F", "L": "F", "R": "F", "LW": "F", "RW": "F",
    "F": "F", "W": "F", "CENTER": "F", "LEFT WING": "F",
    "RIGHT WING": "F", "FORWARD": "F",
}


class PlayerStageConflict(RuntimeError):
    """A player key has contradictory protected source values."""

    def __init__(self, *, conflicts: list[dict[str, Any]], counts: dict[str, int]):
        self.conflicts = conflicts
        self.counts = counts
        detail = ";".join(
            f"{item['player_id']}:{','.join(item['fields'])}"
            for item in conflicts
        )
        super().__init__(f"PLAYER_STAGE_PROTECTED_CONFLICT:{detail}")


def normalize_position(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return _POSITION_MAP.get(text)


def _nullable_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"PLAYER_STAGE_{field.upper()}_NOT_TEXT")
    value = value.strip()
    return value or None


def _normalize_row(source: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[Any, ...], list[str]]:
    try:
        player_id = int(source["player_id"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("PLAYER_STAGE_PLAYER_ID_INVALID") from error

    team_value = source.get("team_id")
    try:
        team_id = None if team_value is None else int(team_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"PLAYER_STAGE_TEAM_ID_INVALID:{player_id}") from error

    first_name = _nullable_text(source.get("first_name"), field="first_name")
    if first_name and first_name.casefold() in {"player", "unknown"}:
        first_name = None
    last_name = _nullable_text(source.get("last_name"), field="last_name")
    if last_name and last_name.isdigit():
        last_name = None

    shoots = _nullable_text(source.get("shoots_catches"), field="shoots_catches")
    if shoots is not None:
        shoots = shoots.upper()
        if shoots not in {"L", "R"}:
            raise ValueError(f"PLAYER_STAGE_SHOOTS_CATCHES_INVALID:{player_id}")

    active_value = source.get("active")
    if active_value is None:
        active = None
    elif isinstance(active_value, bool):
        active = active_value
    elif active_value in {0, 1}:
        active = bool(active_value)
    else:
        raise ValueError(f"PLAYER_STAGE_ACTIVE_INVALID:{player_id}")

    provider = source.get("provider", "nhl")
    provider = str(provider).strip().lower() if provider is not None else "nhl"
    provider_external = source.get("provider_player_id", player_id)
    try:
        provider_external = int(provider_external)
    except (TypeError, ValueError):
        provider_external = str(provider_external)

    identity_conflicts = []
    if provider != "nhl":
        identity_conflicts.append("provider")
    if provider_external != player_id:
        identity_conflicts.append("provider_player_id")

    row = {
        "player_id": player_id,
        "team_id": team_id,
        "first_name": first_name,
        "last_name": last_name,
        "position": normalize_position(source.get("position")),
        "shoots_catches": shoots,
        "active": active,
    }
    logical_signature = tuple(row[field] for field in PLAYER_STAGE_FIELDS) + (
        provider, provider_external,
    )
    return row, logical_signature, identity_conflicts


def normalize_player_stage_rows(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Return one deterministic player-dimension row per NHL ``player_id``.

    Missing nullable values may complement one another. Two different non-null
    values for any protected field are a conflict. The function examines the
    complete batch and raises before callers perform any database operation.
    """
    grouped: dict[int, list[tuple[dict[str, Any], tuple[Any, ...], list[str]]]] = {}
    source_count = 0
    for source in rows:
        normalized = _normalize_row(source)
        grouped.setdefault(normalized[0]["player_id"], []).append(normalized)
        source_count += 1

    counts = {
        "source_rows": source_count,
        "unique_player_ids": len(grouped),
        "duplicate_source_rows": source_count - len(grouped),
        "exact_rows_collapsed": 0,
        "complementary_groups_merged": 0,
        "conflicting_groups_rejected": 0,
    }
    output: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []

    for player_id in sorted(grouped):
        group = grouped[player_id]
        counts["exact_rows_collapsed"] += len(group) - len({item[1] for item in group})
        conflict_fields = set()
        for _, _, identity_conflicts in group:
            conflict_fields.update(identity_conflicts)

        merged = {"player_id": player_id}
        for field in _MERGED_FIELDS:
            values = {item[0][field] for item in group if item[0][field] is not None}
            if len(values) > 1:
                conflict_fields.add(field)
            elif values:
                merged[field] = next(iter(values))
            else:
                merged[field] = None

        providers = {
            (item[1][-2], item[1][-1])
            for item in group
        }
        if len(providers) > 1:
            conflict_fields.add("provider_identity")

        if conflict_fields:
            conflicts.append({
                "player_id": player_id,
                "fields": sorted(conflict_fields),
            })
            continue

        if len({item[1] for item in group}) > 1:
            counts["complementary_groups_merged"] += 1

        # Match the destination defaults only after nullable values are merged.
        merged["position"] = merged["position"] or "F"
        merged["active"] = True if merged["active"] is None else merged["active"]
        output.append(merged)

    counts["conflicting_groups_rejected"] = len(conflicts)
    if conflicts:
        raise PlayerStageConflict(conflicts=conflicts, counts=counts)
    return output, counts
