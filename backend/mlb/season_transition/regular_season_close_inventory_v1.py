"""Authoritative, source-hashed 2026 MLB regular-season close inventory.

This module is deliberately file-only.  It derives membership exclusively
from the verified game-phase authority and derives disposition exclusively
from the immutable StatsAPI schedule responses already named by that
authority.  It never calls a database or network service and it has no close
or publication side effect.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.season_transition.game_phase_authority_v1 import (
    DEFAULT_SOURCE_MANIFEST_PATH,
    EXPECTED_PHASE_COUNTS,
    EXPECTED_PROPOSAL_COUNT,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_TYPE_COUNTS,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
    REPO_ROOT,
)


CONTRACT_NAME = "MLB_2026_AUTHORITATIVE_REGULAR_SEASON_CLOSE_INVENTORY_V1"
INVENTORY_FILENAME = "authoritative_regular_season_games.jsonl"
MANIFEST_FILENAME = "close_inventory_manifest.json"
EXPECTED_TOTAL_CLASSIFIED = 2919
EXPECTED_REGULAR_SEASON = 2430
EXPECTED_PRESEASON = 489
PACKAGE_RELATIVE_PATH = (
    "docs/contracts/mlb_2026_authoritative_regular_season_close_inventory_v1"
)
DEFAULT_PACKAGE_PATH = REPO_ROOT / PACKAGE_RELATIVE_PATH
DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH = (
    DEFAULT_PACKAGE_PATH / "disposition_source_supplement_manifest.jsonl"
)

EXPECTED_CLOSE_INVENTORY_MANIFEST_SHA256 = (
    "f8bb9008e0ff4a0d8f9dc23e12f5a6e799476de58b699814e0466cd2458e756b"
)

DISPOSITIONS = frozenset(
    {
        "FINAL",
        "AUTHORITATIVELY_CANCELLED",
        "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED",
        "SUSPENDED_RESUMED_IDENTITY_RESOLVED",
        "SCHEDULED_NOT_FINAL",
        "UNRESOLVED_IDENTITY_OR_STATUS",
    }
)
CLOSE_COMPLETE_DISPOSITIONS = frozenset(
    {
        "FINAL",
        "AUTHORITATIVELY_CANCELLED",
        "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED",
        "SUSPENDED_RESUMED_IDENTITY_RESOLVED",
    }
)
FINAL_DETAILED_STATES = frozenset({"Final", "Completed Early"})
CANCELLED_DETAILED_STATES = frozenset({"Cancelled", "Canceled"})
KNOWN_NONTERMINAL_DETAILED_STATES = frozenset(
    {
        "Scheduled",
        "Pre-Game",
        "Warmup",
        "In Progress",
        "Delayed Start",
        "Delayed",
        "Manager challenge",
        "Game Over",
        "Postponed",
        "Suspended",
    }
)
RELATIONSHIP_FIELDS = (
    "rescheduleDate",
    "rescheduleGameDate",
    "rescheduledFrom",
    "rescheduledFromDate",
    "resumeDate",
    "resumeGameDate",
    "resumedFrom",
    "resumedFromDate",
)
STATUS_FIELDS = (
    "abstractGameState",
    "codedGameState",
    "detailedState",
    "statusCode",
    "reason",
)


class CloseInventoryError(RuntimeError):
    """Fail-closed inventory or package validation error."""


@dataclass(frozen=True)
class SourceObservation:
    source_path: str
    source_sha256: str
    game: Mapping[str, Any]


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise CloseInventoryError(f"CLOSE_INVENTORY_EVIDENCE_MISSING:{path}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_EVIDENCE_MALFORMED:{path}:{line_number}"
            ) from exc
        if not isinstance(row, dict):
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_EVIDENCE_MALFORMED:{path}:{line_number}"
            )
        rows.append(row)
    return rows


def _safe_repo_path(relative: str, *, root: Path) -> Path:
    if not relative or Path(relative).is_absolute():
        raise CloseInventoryError(f"CLOSE_INVENTORY_SOURCE_PATH_INVALID:{relative}")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        raise CloseInventoryError(f"CLOSE_INVENTORY_SOURCE_PATH_INVALID:{relative}") from None
    return resolved


def _status(game: Mapping[str, Any]) -> dict[str, Any]:
    value = game.get("status")
    if not isinstance(value, Mapping):
        return {field: None for field in STATUS_FIELDS}
    return {field: value.get(field) for field in STATUS_FIELDS}


def _source_ref(observation: SourceObservation) -> dict[str, str]:
    return {
        "path": observation.source_path,
        "sha256": observation.source_sha256,
    }


def _observation_sort_key(observation: SourceObservation) -> tuple[str, bytes]:
    return observation.source_path, canonical_json_bytes(observation.game)


def _score_outcome(game: Mapping[str, Any]) -> dict[str, Any] | None:
    teams = game.get("teams")
    if not isinstance(teams, Mapping):
        return None
    away = teams.get("away")
    home = teams.get("home")
    if not isinstance(away, Mapping) or not isinstance(home, Mapping):
        return None
    away_team = away.get("team") if isinstance(away.get("team"), Mapping) else {}
    home_team = home.get("team") if isinstance(home.get("team"), Mapping) else {}
    away_score = away.get("score")
    home_score = home.get("score")
    if isinstance(away_score, bool) or not isinstance(away_score, int):
        return None
    if isinstance(home_score, bool) or not isinstance(home_score, int):
        return None
    winner = (
        "AWAY"
        if away_score > home_score
        else "HOME"
        if home_score > away_score
        else "TIE"
    )
    return {
        "away_team_id": away_team.get("id"),
        "home_team_id": home_team.get("id"),
        "away_score": away_score,
        "home_score": home_score,
        "winner": winner,
    }


def _relationship_identity(
    game_pk: int,
    relationships: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "identity_semantics": "SAME_EXACT_GAME_PK_ACROSS_SCHEDULE_TRANSITIONS",
        "game_pk": game_pk,
        "original": (
            {
                "game_pk": game_pk,
                "scheduled_start": relationships.get("rescheduledFrom")
                or relationships.get("resumedFrom"),
                "scheduled_date": relationships.get("rescheduledFromDate")
                or relationships.get("resumedFromDate"),
            }
            if any(
                field in relationships
                for field in ("rescheduledFrom", "rescheduledFromDate", "resumedFrom", "resumedFromDate")
            )
            else None
        ),
        "replacement": (
            {
                "game_pk": game_pk,
                "scheduled_start": relationships.get("rescheduleDate"),
                "scheduled_date": relationships.get("rescheduleGameDate"),
            }
            if any(field in relationships for field in ("rescheduleDate", "rescheduleGameDate"))
            else None
        ),
        "resumed": (
            {
                "game_pk": game_pk,
                "scheduled_start": relationships.get("resumeDate"),
                "scheduled_date": relationships.get("resumeGameDate"),
            }
            if any(field in relationships for field in ("resumeDate", "resumeGameDate"))
            else None
        ),
        "related_game_pks": [],
        "raw_relationships": dict(sorted(relationships.items())),
    }


def classify_authoritative_game(
    authority_record: GamePhaseAuthorityRecord,
    observations: Sequence[SourceObservation],
) -> dict[str, Any]:
    """Build one deterministic disposition row without date inference."""

    game_pk = authority_record.game_pk
    problems: list[str] = []
    ordered = sorted(observations, key=_observation_sort_key)
    if not ordered:
        problems.append("NO_RETAINED_STATUS_OBSERVATION")

    raw_types = {str(obs.game.get("gameType") or "") for obs in ordered}
    if raw_types != {authority_record.source_game_type}:
        problems.append("SOURCE_GAME_TYPE_MISSING_OR_CONFLICTING")
    seasons: set[int] = set()
    for obs in ordered:
        try:
            seasons.add(int(obs.game.get("season")))
        except (TypeError, ValueError):
            problems.append("SOURCE_SEASON_MISSING_OR_INVALID")
    if seasons != {authority_record.source_season}:
        problems.append("SOURCE_SEASON_MISSING_OR_CONFLICTING")

    status_groups: defaultdict[bytes, list[SourceObservation]] = defaultdict(list)
    unknown_status = False
    for obs in ordered:
        status = _status(obs.game)
        if any(status[field] in (None, "") for field in STATUS_FIELDS[:-1]):
            problems.append("STATUS_FIELDS_MISSING")
        detailed = status["detailedState"]
        if detailed not in (
            FINAL_DETAILED_STATES
            | CANCELLED_DETAILED_STATES
            | KNOWN_NONTERMINAL_DETAILED_STATES
        ):
            unknown_status = True
        status_groups[canonical_json_bytes(status)].append(obs)
    if unknown_status:
        problems.append("UNKNOWN_AUTHORITATIVE_STATUS")

    final_observations = [
        obs for obs in ordered if _status(obs.game)["detailedState"] in FINAL_DETAILED_STATES
    ]
    cancelled_observations = [
        obs
        for obs in ordered
        if _status(obs.game)["detailedState"] in CANCELLED_DETAILED_STATES
    ]
    if final_observations and cancelled_observations:
        problems.append("CONFLICTING_TERMINAL_STATUS")

    final_outcomes: dict[bytes, dict[str, Any]] = {}
    for obs in final_observations:
        outcome = _score_outcome(obs.game)
        if outcome is None:
            problems.append("FINAL_SCORE_OR_OUTCOME_MISSING")
        else:
            final_outcomes[canonical_json_bytes(outcome)] = outcome
    if len(final_outcomes) > 1:
        problems.append("CONFLICTING_FINAL_SCORE_OR_OUTCOME")

    observed_relationship_values: defaultdict[str, set[bytes]] = defaultdict(set)
    relationship_values: dict[str, Any] = dict(authority_record.schedule_relationships)
    for obs in ordered:
        for field in RELATIONSHIP_FIELDS:
            if field in obs.game:
                observed_relationship_values[field].add(canonical_json_bytes(obs.game[field]))
                relationship_values[field] = obs.game[field]
    if any(len(values) > 1 for values in observed_relationship_values.values()):
        problems.append("CONFLICTING_RELATIONSHIP_EVIDENCE")

    has_reschedule = any(field.startswith("resched") for field in relationship_values)
    has_resume = any(field.startswith("resume") for field in relationship_values)
    required_reschedule = {
        "rescheduleDate",
        "rescheduleGameDate",
        "rescheduledFrom",
        "rescheduledFromDate",
    }
    required_resume = {"resumeDate", "resumeGameDate", "resumedFrom", "resumedFromDate"}
    if has_reschedule and not required_reschedule.issubset(relationship_values):
        problems.append("RESCHEDULE_IDENTITY_INCOMPLETE")
    if has_resume and not required_resume.issubset(relationship_values):
        problems.append("RESUMED_IDENTITY_INCOMPLETE")
    if has_reschedule and has_resume:
        problems.append("CONFLICTING_RELATIONSHIP_MODES")

    if problems:
        disposition = "UNRESOLVED_IDENTITY_OR_STATUS"
        reason = ";".join(sorted(set(problems)))
    elif final_observations and has_resume:
        disposition = "SUSPENDED_RESUMED_IDENTITY_RESOLVED"
        reason = "RETAINED_FINAL_OUTCOME_AND_COMPLETE_RESUME_IDENTITY"
    elif final_observations and has_reschedule:
        disposition = "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED"
        reason = "RETAINED_FINAL_OUTCOME_AND_COMPLETE_RESCHEDULE_IDENTITY"
    elif final_observations:
        disposition = "FINAL"
        reason = "RETAINED_AUTHORITATIVE_FINAL_OUTCOME"
    elif cancelled_observations:
        disposition = "AUTHORITATIVELY_CANCELLED"
        reason = "RETAINED_AUTHORITATIVE_CANCELLATION"
    else:
        disposition = "SCHEDULED_NOT_FINAL"
        reason = "NO_RETAINED_AUTHORITATIVE_TERMINAL_STATUS"

    status_rank = {
        "Postponed": 0,
        "Scheduled": 1,
        "Delayed Start": 2,
        "Pre-Game": 3,
        "Warmup": 4,
        "Delayed": 5,
        "In Progress": 6,
        "Manager challenge": 7,
        "Game Over": 8,
        "Cancelled": 9,
        "Canceled": 9,
        "Final": 10,
        "Completed Early": 10,
    }
    representative = max(
        ordered,
        key=lambda obs: (
            status_rank.get(str(_status(obs.game)["detailedState"]), -1),
            _observation_sort_key(obs),
        ),
        default=None,
    )
    statuses = []
    for encoded in sorted(status_groups):
        evidence = status_groups[encoded]
        statuses.append(
            {
                "status": json.loads(encoded),
                "observation_count": len(evidence),
                "source_artifacts": [_source_ref(obs) for obs in evidence],
            }
        )
    scheduled_starts = sorted(
        {str(obs.game.get("gameDate")) for obs in ordered if obs.game.get("gameDate")}
    )
    official_dates = sorted(
        {str(obs.game.get("officialDate")) for obs in ordered if obs.game.get("officialDate")}
    )
    return {
        "game_pk": game_pk,
        "source_season": authority_record.source_season,
        "authoritative_raw_game_type": authority_record.source_game_type,
        "normalized_phase": authority_record.season_phase,
        "scheduled_start": representative.game.get("gameDate") if representative else None,
        "observed_scheduled_starts": scheduled_starts,
        "observed_official_dates": official_dates,
        "authoritative_status": _status(representative.game) if representative else None,
        "authoritative_status_evidence": statuses,
        "final_outcome": next(iter(final_outcomes.values()), None),
        "relationship_identities": _relationship_identity(game_pk, relationship_values),
        "phase_source_artifact": {
            "path": authority_record.primary_source_path,
            "sha256": authority_record.primary_source_sha256,
        },
        "disposition_source_artifact": _source_ref(representative) if representative else None,
        "retained_source_artifact_count": len({obs.source_path for obs in ordered}),
        "retained_status_observation_count": len(ordered),
        "close_disposition": disposition,
        "disposition_reason": reason,
    }


def _load_verified_observations(
    regular_records: Sequence[GamePhaseAuthorityRecord],
    *,
    source_manifest_paths: Sequence[Path],
    root: Path,
) -> tuple[dict[int, list[SourceObservation]], dict[str, Any]]:
    regular_game_pks = {record.game_pk for record in regular_records}
    observations: defaultdict[int, list[SourceObservation]] = defaultdict(list)
    manifest_rows: list[dict[str, Any]] = []
    manifest_hashes: dict[str, str] = {}
    for source_manifest_path in source_manifest_paths:
        try:
            manifest_hash = file_sha256(source_manifest_path)
        except OSError as exc:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_MANIFEST_MISSING:{source_manifest_path}"
            ) from exc
        manifest_hashes[str(source_manifest_path.relative_to(root))] = manifest_hash
        manifest_rows.extend(_read_jsonl(source_manifest_path))
    seen_paths: set[str] = set()
    total_schedule_rows = 0
    for manifest_row in manifest_rows:
        source_path = str(manifest_row.get("source_path") or "")
        source_sha256 = str(manifest_row.get("source_sha256") or "")
        if source_path in seen_paths:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_PATH_DUPLICATE:{source_path}"
            )
        seen_paths.add(source_path)
        if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_HASH_INVALID:{source_path}"
            )
        path = _safe_repo_path(source_path, root=root)
        try:
            source_bytes = int(manifest_row.get("source_bytes"))
            schedule_game_rows = int(manifest_row.get("schedule_game_rows"))
        except (TypeError, ValueError):
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_MANIFEST_ROW_INVALID:{source_path}"
            ) from None
        try:
            source_matches = (
                path.stat().st_size == source_bytes
                and file_sha256(path) == source_sha256
            )
        except OSError as exc:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_MISSING:{source_path}"
            ) from exc
        if not source_matches:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_HASH_MISMATCH:{source_path}"
            )
        try:
            payload = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_MALFORMED:{source_path}"
            ) from exc
        observed_schedule_rows = 0
        for date_row in payload.get("dates") or []:
            if not isinstance(date_row, Mapping):
                continue
            for game in date_row.get("games") or []:
                if not isinstance(game, Mapping):
                    continue
                observed_schedule_rows += 1
                game_pk = game.get("gamePk")
                if game_pk in regular_game_pks:
                    observations[int(game_pk)].append(
                        SourceObservation(source_path, source_sha256, game)
                    )
        if observed_schedule_rows != schedule_game_rows:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_SOURCE_ROW_COUNT_MISMATCH:{source_path}"
            )
        total_schedule_rows += observed_schedule_rows
    return dict(observations), {
        "manifest_hashes": dict(sorted(manifest_hashes.items())),
        "source_file_count": len(seen_paths),
        "source_observation_count": total_schedule_rows,
        "source_population_sha256": canonical_sha256(
            [
                {
                    "source_path": str(row["source_path"]),
                    "source_sha256": str(row["source_sha256"]),
                    "source_bytes": int(row["source_bytes"]),
                    "schedule_game_rows": int(row["schedule_game_rows"]),
                }
                for row in sorted(manifest_rows, key=lambda row: str(row["source_path"]))
            ]
        ),
    }


def build_authoritative_inventory(
    *,
    authority: HashedProposalAuthority | None = None,
    root: Path = REPO_ROOT,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build all 2,430 rows after verifying the frozen phase authority."""

    authority = authority or HashedProposalAuthority(root=root)
    metadata = authority.metadata
    if (
        metadata.proposal_count != EXPECTED_TOTAL_CLASSIFIED
        or dict(metadata.phase_counts)
        != {"PRESEASON": EXPECTED_PRESEASON, "REGULAR_SEASON": EXPECTED_REGULAR_SEASON}
        or metadata.missing_count
        or metadata.unknown_count
        or metadata.conflicting_count
        or metadata.duplicate_identity_count
    ):
        raise CloseInventoryError("CLOSE_INVENTORY_PHASE_AUTHORITY_COUNTS_INVALID")
    regular_records = [
        record for record in authority.records if record.season_phase == "REGULAR_SEASON"
    ]
    if len(regular_records) != EXPECTED_REGULAR_SEASON:
        raise CloseInventoryError("CLOSE_INVENTORY_REGULAR_POPULATION_INVALID")
    observations, disposition_sources = _load_verified_observations(
        regular_records,
        source_manifest_paths=(
            DEFAULT_SOURCE_MANIFEST_PATH,
            DEFAULT_DISPOSITION_SUPPLEMENT_MANIFEST_PATH,
        ),
        root=root,
    )
    rows = [
        classify_authoritative_game(record, observations.get(record.game_pk, []))
        for record in regular_records
    ]
    dispositions = Counter(row["close_disposition"] for row in rows)
    summary = {
        "contract_name": CONTRACT_NAME,
        "season": 2026,
        "authority": metadata.to_dict(),
        "population_counts": {
            "total_classified_game_pks": metadata.proposal_count,
            "regular_season_game_pks": len(rows),
            "preseason_game_pks": metadata.phase_counts["PRESEASON"],
            "missing": metadata.missing_count,
            "unknown": metadata.unknown_count,
            "conflicting": metadata.conflicting_count,
            "duplicate_identities": metadata.duplicate_identity_count,
        },
        "disposition_counts": {
            disposition: dispositions.get(disposition, 0)
            for disposition in sorted(DISPOSITIONS)
        },
        "scheduled_not_final_game_pks": [
            row["game_pk"]
            for row in rows
            if row["close_disposition"] == "SCHEDULED_NOT_FINAL"
        ],
        "unresolved_game_pks": [
            row["game_pk"]
            for row in rows
            if row["close_disposition"] == "UNRESOLVED_IDENTITY_OR_STATUS"
        ],
        "relationship_game_pks": [
            row["game_pk"]
            for row in rows
            if row["relationship_identities"]["raw_relationships"]
        ],
        "disposition_sources": disposition_sources,
    }
    return rows, summary


def manifest_body_sha256(manifest: Mapping[str, Any]) -> str:
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    return canonical_sha256(body)


def make_inventory_manifest(
    rows: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]
) -> dict[str, Any]:
    jsonl = b"".join(canonical_json_bytes(row) + b"\n" for row in rows)
    manifest: dict[str, Any] = {
        "contract_name": CONTRACT_NAME,
        "season": 2026,
        "determinism": "SOURCE_HASH_BOUND_NO_WALL_CLOCK_FIELDS",
        "inventory_path": INVENTORY_FILENAME,
        "inventory_sha256": hashlib.sha256(jsonl).hexdigest(),
        "inventory_row_count": len(rows),
        "game_pk_population_sha256": canonical_sha256(
            [int(row["game_pk"]) for row in rows]
        ),
        "authority_proposal_sha256": summary["authority"]["proposal_sha256"],
        "authority_records_sha256": summary["authority"]["authority_records_sha256"],
        "source_manifest_sha256": summary["authority"]["source_manifest_sha256"],
        "source_file_count": summary["authority"]["source_file_count"],
        "source_observation_count": summary["authority"]["source_observation_count"],
        "disposition_source_manifest_hashes": summary["disposition_sources"][
            "manifest_hashes"
        ],
        "disposition_source_file_count": summary["disposition_sources"][
            "source_file_count"
        ],
        "disposition_source_observation_count": summary["disposition_sources"][
            "source_observation_count"
        ],
        "disposition_source_population_sha256": summary["disposition_sources"][
            "source_population_sha256"
        ],
        "population_counts": summary["population_counts"],
        "disposition_counts": summary["disposition_counts"],
        "scheduled_not_final_game_pks": summary["scheduled_not_final_game_pks"],
        "unresolved_game_pks": summary["unresolved_game_pks"],
        "relationship_game_pks": summary["relationship_game_pks"],
    }
    manifest["manifest_sha256"] = manifest_body_sha256(manifest)
    return manifest


def write_inventory_core(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    *,
    package_path: Path = DEFAULT_PACKAGE_PATH,
) -> dict[str, Any]:
    """Write only deterministic review evidence; never a close package."""

    package_path.mkdir(parents=True, exist_ok=True)
    inventory_path = package_path / INVENTORY_FILENAME
    inventory_path.write_bytes(
        b"".join(canonical_json_bytes(row) + b"\n" for row in rows)
    )
    manifest = make_inventory_manifest(rows, summary)
    (package_path / MANIFEST_FILENAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    relationship_rows = [
        row for row in rows if row["relationship_identities"]["raw_relationships"]
    ]
    (package_path / "relationship_ledger.jsonl").write_bytes(
        b"".join(canonical_json_bytes(row) + b"\n" for row in relationship_rows)
    )
    for filename, values in (
        ("scheduled_not_final_game_pks.json", summary["scheduled_not_final_game_pks"]),
        ("unresolved_game_pks.json", summary["unresolved_game_pks"]),
    ):
        (package_path / filename).write_text(
            json.dumps(values, indent=2) + "\n", encoding="utf-8"
        )
    return manifest


def validate_inventory_rows(
    rows: Sequence[Mapping[str, Any]],
    authority_records: Iterable[GamePhaseAuthorityRecord],
    *,
    expected_count: int,
) -> dict[str, Any]:
    expected = {
        record.game_pk: record
        for record in authority_records
        if record.season_phase == "REGULAR_SEASON"
    }
    seen: set[int] = set()
    duplicates: list[int] = []
    invalid: list[str] = []
    dispositions: Counter[str] = Counter()
    for row in rows:
        try:
            game_pk = int(row.get("game_pk"))
        except (TypeError, ValueError):
            invalid.append("MISSING_GAME_PK")
            continue
        if game_pk in seen:
            duplicates.append(game_pk)
        seen.add(game_pk)
        record = expected.get(game_pk)
        if record is None:
            invalid.append(f"{game_pk}:EXTRA_OR_NONREGULAR_GAME_PK")
            continue
        if (
            row.get("authoritative_raw_game_type") != record.source_game_type
            or row.get("normalized_phase") != "REGULAR_SEASON"
        ):
            invalid.append(f"{game_pk}:PHASE_AUTHORITY_CONFLICT")
        disposition = str(row.get("close_disposition") or "")
        dispositions[disposition] += 1
        if disposition not in DISPOSITIONS:
            invalid.append(f"{game_pk}:DISPOSITION_INVALID")
        relationships = row.get("relationship_identities")
        if not isinstance(relationships, Mapping) or relationships.get("game_pk") != game_pk:
            invalid.append(f"{game_pk}:RELATIONSHIP_IDENTITY_INVALID")
        if not isinstance(row.get("phase_source_artifact"), Mapping):
            invalid.append(f"{game_pk}:PHASE_SOURCE_MISSING")
        if row.get("disposition_source_artifact") is None:
            invalid.append(f"{game_pk}:STATUS_SOURCE_MISSING")
    omitted = sorted(set(expected) - seen)
    extras = sorted(seen - set(expected))
    scheduled = sorted(
        int(row["game_pk"])
        for row in rows
        if row.get("close_disposition") == "SCHEDULED_NOT_FINAL"
    )
    unresolved = sorted(
        int(row["game_pk"])
        for row in rows
        if row.get("close_disposition") == "UNRESOLVED_IDENTITY_OR_STATUS"
    )
    integrity_passed = (
        len(rows) == expected_count
        and len(expected) == expected_count
        and not duplicates
        and not omitted
        and not extras
        and not invalid
    )
    return {
        "integrity_passed": integrity_passed,
        "close_ready": integrity_passed and not scheduled and not unresolved,
        "row_count": len(rows),
        "expected_count": expected_count,
        "duplicate_game_pks": sorted(set(duplicates)),
        "omitted_game_pks": omitted,
        "extra_or_nonregular_game_pks": extras,
        "invalid_rows": invalid,
        "disposition_counts": dict(sorted(dispositions.items())),
        "scheduled_not_final_game_pks": scheduled,
        "unresolved_game_pks": unresolved,
    }


def validate_close_inventory_package(
    *,
    package_path: Path = DEFAULT_PACKAGE_PATH,
    expected_manifest_sha256: str = EXPECTED_CLOSE_INVENTORY_MANIFEST_SHA256,
    authority: HashedProposalAuthority | None = None,
) -> dict[str, Any]:
    """Validate the one canonical package and report readiness without writes."""

    manifest_path = package_path / MANIFEST_FILENAME
    inventory_path = package_path / INVENTORY_FILENAME
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CloseInventoryError("CLOSE_INVENTORY_MANIFEST_MISSING_OR_MALFORMED") from exc
    if not isinstance(manifest, dict):
        raise CloseInventoryError("CLOSE_INVENTORY_MANIFEST_MALFORMED")
    actual_manifest_sha = manifest_body_sha256(manifest)
    embedded_manifest_sha = manifest.get("manifest_sha256")
    if actual_manifest_sha != embedded_manifest_sha:
        raise CloseInventoryError("CLOSE_INVENTORY_MANIFEST_HASH_MISMATCH")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_manifest_sha256):
        raise CloseInventoryError("CLOSE_INVENTORY_PINNED_MANIFEST_HASH_INVALID")
    if actual_manifest_sha != expected_manifest_sha256:
        raise CloseInventoryError("CLOSE_INVENTORY_PINNED_MANIFEST_HASH_MISMATCH")
    if manifest.get("contract_name") != CONTRACT_NAME or manifest.get("season") != 2026:
        raise CloseInventoryError("CLOSE_INVENTORY_CONTRACT_IDENTITY_MISMATCH")
    if manifest.get("inventory_path") != INVENTORY_FILENAME:
        raise CloseInventoryError("CLOSE_INVENTORY_PATH_IDENTITY_MISMATCH")
    if file_sha256(inventory_path) != manifest.get("inventory_sha256"):
        raise CloseInventoryError("CLOSE_INVENTORY_FILE_HASH_MISMATCH")
    rows = _read_jsonl(inventory_path)
    authority = authority or HashedProposalAuthority()
    metadata = authority.metadata
    rebuilt_rows, rebuilt_summary = build_authoritative_inventory(authority=authority)
    if rows != rebuilt_rows:
        raise CloseInventoryError("CLOSE_INVENTORY_REBUILD_MISMATCH")
    expected_authority_values = {
        "authority_proposal_sha256": metadata.proposal_sha256,
        "authority_records_sha256": metadata.authority_records_sha256,
        "source_manifest_sha256": metadata.source_manifest_sha256,
        "source_file_count": metadata.source_file_count,
        "source_observation_count": metadata.source_observation_count,
    }
    for field, expected_value in expected_authority_values.items():
        if manifest.get(field) != expected_value:
            raise CloseInventoryError(f"CLOSE_INVENTORY_AUTHORITY_BINDING_MISMATCH:{field}")
    expected_disposition_source_values = {
        "disposition_source_manifest_hashes": rebuilt_summary[
            "disposition_sources"
        ]["manifest_hashes"],
        "disposition_source_file_count": rebuilt_summary["disposition_sources"][
            "source_file_count"
        ],
        "disposition_source_observation_count": rebuilt_summary[
            "disposition_sources"
        ]["source_observation_count"],
        "disposition_source_population_sha256": rebuilt_summary[
            "disposition_sources"
        ]["source_population_sha256"],
    }
    for field, expected_value in expected_disposition_source_values.items():
        if manifest.get(field) != expected_value:
            raise CloseInventoryError(
                f"CLOSE_INVENTORY_DISPOSITION_SOURCE_BINDING_MISMATCH:{field}"
            )
    report = validate_inventory_rows(
        rows,
        authority.records,
        expected_count=EXPECTED_REGULAR_SEASON,
    )
    if canonical_sha256([row.get("game_pk") for row in rows]) != manifest.get(
        "game_pk_population_sha256"
    ):
        raise CloseInventoryError("CLOSE_INVENTORY_POPULATION_HASH_MISMATCH")
    if manifest.get("inventory_row_count") != len(rows):
        raise CloseInventoryError("CLOSE_INVENTORY_ROW_COUNT_MISMATCH")
    if manifest.get("population_counts") != {
        "total_classified_game_pks": EXPECTED_PROPOSAL_COUNT,
        "regular_season_game_pks": EXPECTED_REGULAR_SEASON,
        "preseason_game_pks": EXPECTED_PHASE_COUNTS["PRESEASON"],
        "missing": 0,
        "unknown": 0,
        "conflicting": 0,
        "duplicate_identities": 0,
    }:
        raise CloseInventoryError("CLOSE_INVENTORY_POPULATION_COUNTS_MISMATCH")
    if manifest.get("disposition_counts") != {
        disposition: report["disposition_counts"].get(disposition, 0)
        for disposition in sorted(DISPOSITIONS)
    }:
        raise CloseInventoryError("CLOSE_INVENTORY_DISPOSITION_COUNTS_MISMATCH")
    if manifest.get("scheduled_not_final_game_pks") != report[
        "scheduled_not_final_game_pks"
    ]:
        raise CloseInventoryError("CLOSE_INVENTORY_SCHEDULED_LEDGER_MISMATCH")
    if manifest.get("unresolved_game_pks") != report["unresolved_game_pks"]:
        raise CloseInventoryError("CLOSE_INVENTORY_UNRESOLVED_LEDGER_MISMATCH")
    report.update(
        {
            "contract_name": CONTRACT_NAME,
            "manifest_sha256": actual_manifest_sha,
            "authority_proposal_sha256": metadata.proposal_sha256,
            "authority_records_sha256": metadata.authority_records_sha256,
            "source_manifest_sha256": metadata.source_manifest_sha256,
            "decision": (
                "REGULAR_SEASON_CLOSE_READY"
                if report["close_ready"]
                else "REGULAR_SEASON_CLOSE_BLOCKED"
            ),
            "check_only": True,
            "close_package_created": False,
        }
    )
    return report


def authority_expectations() -> dict[str, Any]:
    """Expose the frozen counts used by contract validators."""

    return {
        "proposal_count": EXPECTED_PROPOSAL_COUNT,
        "type_counts": EXPECTED_TYPE_COUNTS,
        "phase_counts": EXPECTED_PHASE_COUNTS,
        "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
    }
