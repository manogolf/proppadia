"""Canonical MLB game-phase records and exact-game join interface.

All helpers are pure and offline.  They consume already-retained StatsAPI
schedule/feed payloads and never infer phase from a date.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from backend.mlb.season_transition.contract_v1 import (
    PhaseContractError,
    classify_schedule_game,
)


RELATIONSHIP_FIELDS = (
    "rescheduledFrom",
    "rescheduledFromDate",
    "rescheduleDate",
    "rescheduleGameDate",
    "resumeDate",
    "resumeGameDate",
    "resumedFrom",
    "resumedFromDate",
)

PHASE_STORAGE_COLUMNS = (
    "source_season",
    "source_game_type",
    "season_phase",
    "postseason_round",
    "season_name",
    "source_round",
    "schedule_relationships",
)

PHASE_CORE_FIELDS = (
    "source_season",
    "source_game_type",
    "season_phase",
    "postseason_round",
    "season_name",
)

CLEANROOM_GAME_BASE_COLUMNS = (
    "game_pk",
    "slate_date",
    "official_game_date",
    "home_team_mlb_id",
    "away_team_mlb_id",
    "scheduled_start_utc",
    "game_status",
    "source",
    "source_observed_at_utc",
    "ingested_at_utc",
    "source_payload_sha256",
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def game_pk_from_payload(game: Mapping[str, Any]) -> int:
    value = game.get("gamePk")
    if value in (None, ""):
        value = (((game.get("gameData") or {}).get("game") or {}).get("pk"))
    try:
        return int(value)
    except (TypeError, ValueError):
        raise PhaseContractError("AUTHORITATIVE_GAME_PK_MISSING") from None


def schedule_relationships(game: Mapping[str, Any]) -> dict[str, Any]:
    """Retain relationship fields exactly when the source supplied the key."""
    return {field: game.get(field) for field in RELATIONSHIP_FIELDS if field in game}


def canonical_phase_record(
    game: Mapping[str, Any],
    *,
    source_sha256: str,
    source_path: str = "",
    season: int | None = None,
) -> dict[str, Any]:
    classification = classify_schedule_game(game, season=season)
    return {
        "game_pk": game_pk_from_payload(game),
        "source_season": classification.season,
        "source_game_type": classification.raw_game_type,
        "season_phase": classification.phase,
        "postseason_round": classification.postseason_round,
        "season_name": classification.season_name,
        "source_round": classification.source_round,
        "schedule_relationships": schedule_relationships(game),
        "game_type_source_sha256": _clean(source_sha256),
        "game_type_source_path": _clean(source_path),
        "phase_decision": classification.decision,
    }


def cleanroom_game_insert_sql(table_columns: Iterable[str]) -> str:
    """Return a named-column insert compatible before or after migration.

    A partially applied phase schema is rejected.  The legacy schema continues
    to operate until the governed migration is explicitly activated.
    """
    available = set(table_columns)
    phase_available = available.intersection(PHASE_STORAGE_COLUMNS)
    if phase_available and phase_available != set(PHASE_STORAGE_COLUMNS):
        missing = sorted(set(PHASE_STORAGE_COLUMNS) - phase_available)
        raise RuntimeError(f"CANONICAL_PHASE_SCHEMA_PARTIAL:{','.join(missing)}")
    columns = list(CLEANROOM_GAME_BASE_COLUMNS)
    if phase_available:
        columns.extend(PHASE_STORAGE_COLUMNS)
    names = ",\n                    ".join(columns)
    values = ",\n                    ".join(
        f"%({name})s::jsonb" if name == "schedule_relationships" else f"%({name})s"
        for name in columns
    )
    return f"""
                INSERT INTO mlb_cleanroom_v1.games (
                    {names}
                ) VALUES (
                    {values}
                ) ON CONFLICT DO NOTHING
            """


@dataclass(frozen=True)
class CanonicalGamePhase:
    game_pk: int
    source_season: int
    source_game_type: str
    season_phase: str | None
    postseason_round: str | None
    season_name: str | None


class CanonicalGamePhaseIndex:
    """One unambiguous phase record per exact MLB gamePk."""

    def __init__(self) -> None:
        self._records: dict[int, dict[str, Any]] = {}
        self.consistent_duplicate_count = 0

    def add(self, record: Mapping[str, Any]) -> None:
        game_pk = int(record["game_pk"])
        incoming = dict(record)
        prior = self._records.get(game_pk)
        if prior is None:
            incoming["source_hashes"] = sorted(
                {value for value in [incoming.get("game_type_source_sha256")] if value}
            )
            incoming["source_paths"] = sorted(
                {value for value in [incoming.get("game_type_source_path")] if value}
            )
            self._records[game_pk] = incoming
            return
        conflicts = [field for field in PHASE_CORE_FIELDS if prior.get(field) != incoming.get(field)]
        if conflicts:
            raise PhaseContractError(
                f"DUPLICATE_GAME_PK_PHASE_CONFLICT:{game_pk}:{','.join(conflicts)}"
            )
        self.consistent_duplicate_count += 1
        relationships = dict(prior.get("schedule_relationships") or {})
        for key, value in (incoming.get("schedule_relationships") or {}).items():
            if key in relationships and relationships[key] not in (None, value) and value is not None:
                raise PhaseContractError(
                    f"DUPLICATE_GAME_PK_RELATIONSHIP_CONFLICT:{game_pk}:{key}"
                )
            if value is not None or key not in relationships:
                relationships[key] = value
        prior["schedule_relationships"] = relationships
        prior["source_hashes"] = sorted(
            set(prior.get("source_hashes") or [])
            | {value for value in [incoming.get("game_type_source_sha256")] if value}
        )
        prior["source_paths"] = sorted(
            set(prior.get("source_paths") or [])
            | {value for value in [incoming.get("game_type_source_path")] if value}
        )

    def records(self) -> list[dict[str, Any]]:
        return [dict(self._records[key]) for key in sorted(self._records)]

    def phase_for_game_pk(self, game_pk: int) -> CanonicalGamePhase:
        record = self._records.get(int(game_pk))
        if record is None:
            raise PhaseContractError(f"CANONICAL_GAME_PHASE_MISSING:{int(game_pk)}")
        if record.get("season_phase") is None:
            raise PhaseContractError(f"CANONICAL_GAME_PHASE_EXCLUDED_SPECIAL:{int(game_pk)}")
        return CanonicalGamePhase(
            game_pk=int(game_pk),
            source_season=int(record["source_season"]),
            source_game_type=str(record["source_game_type"]),
            season_phase=str(record["season_phase"]),
            postseason_round=record.get("postseason_round"),
            season_name=record.get("season_name"),
        )


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
