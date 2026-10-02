#!/usr/bin/env python3
"""Build an immutable child MLB phase-authority snapshot entirely offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from backend.mlb.season_transition.canonical_phase_v1 import canonical_phase_record
from backend.mlb.season_transition.contract_v1 import PhaseContractError
from backend.mlb.season_transition.game_phase_authority_v1 import (
    GamePhaseAuthorityError,
    VersionedFileAuthority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    BUILDER_CONTRACT_NAME,
    BUILDER_VERSION,
    REPO_ROOT,
    SNAPSHOT_CONTRACT_NAME,
    SNAPSHOT_SCHEMA_VERSION,
    SnapshotDescriptorError,
    canonical_json_bytes,
    sha256_file,
    verify_descriptor_chain,
)


SOURCE_SET_CONTRACT_NAME = "MLB_2026_PHASE_AUTHORITY_EXPLICIT_SOURCE_SET_V1"
SOURCE_SET_SCHEMA_VERSION = 1
NO_NEW_AUTHORITY_EVIDENCE = "NO_NEW_AUTHORITY_EVIDENCE"
CANDIDATE_READY = "CANDIDATE_SNAPSHOT_READY_NOT_ACTIVATED"
PROPOSAL_NAME = "canonical_game_phase_full_snapshot.jsonl"
SOURCE_MANIFEST_NAME = "retained_source_manifest.jsonl"
DESCRIPTOR_NAME = "descriptor.json"
VALIDATION_NAME = "validation_report.json"
MANIFEST_NAME = "sha256_manifest.txt"
CLASSIFICATION_FIELDS = (
    "game_pk",
    "source_season",
    "source_game_type",
    "season_phase",
    "postseason_round",
    "season_name",
    "source_round",
    "schedule_relationships",
)
IMMUTABLE_EXISTING_FIELDS = CLASSIFICATION_FIELDS[1:]


class SnapshotBuildError(RuntimeError):
    pass


def _canonical_line(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotBuildError(f"SOURCE_SET_MISSING_OR_MALFORMED:{path}") from exc
    if not isinstance(value, dict):
        raise SnapshotBuildError("SOURCE_SET_OBJECT_REQUIRED")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SnapshotBuildError(f"RETAINED_EVIDENCE_MISSING:{path}") from exc
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SnapshotBuildError(f"JSONL_MALFORMED:{path}:{number}") from exc
        if not isinstance(row, dict):
            raise SnapshotBuildError(f"JSONL_OBJECT_REQUIRED:{path}:{number}")
        rows.append(row)
    return rows


def _repo_path(value: Any, *, root: Path) -> tuple[str, Path]:
    text = str(value or "")
    if not text or Path(text).is_absolute():
        raise SnapshotBuildError(f"SOURCE_PATH_INVALID:{text}")
    path = (root / text).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        raise SnapshotBuildError(f"SOURCE_PATH_OUTSIDE_REPOSITORY:{text}") from None
    return text, path


def _schedule_games(payload: Any, source_path: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
        raise SnapshotBuildError(f"STATSAPI_SCHEDULE_DATES_REQUIRED:{source_path}")
    games: list[dict[str, Any]] = []
    for block in payload["dates"]:
        if not isinstance(block, dict) or not isinstance(block.get("games", []), list):
            raise SnapshotBuildError(f"STATSAPI_SCHEDULE_GAMES_INVALID:{source_path}")
        for game in block.get("games", []):
            if not isinstance(game, dict):
                raise SnapshotBuildError(f"STATSAPI_SCHEDULE_GAME_INVALID:{source_path}")
            try:
                game_pk = int(game.get("gamePk"))
            except (TypeError, ValueError):
                raise SnapshotBuildError(f"AUTHORITATIVE_GAME_PK_MISSING:{source_path}") from None
            if game_pk <= 0:
                raise SnapshotBuildError(f"AUTHORITATIVE_GAME_PK_INVALID:{game_pk}")
            nested_pk = ((game.get("gameData") or {}).get("game") or {}).get("pk")
            if nested_pk not in (None, ""):
                try:
                    if int(nested_pk) != game_pk:
                        raise SnapshotBuildError(f"SCHEDULE_GAME_PK_CONFLICT:{game_pk}")
                except (TypeError, ValueError):
                    raise SnapshotBuildError(f"SCHEDULE_GAME_PK_CONFLICT:{game_pk}") from None
            games.append(game)
    return games


def _scheduled_start(game: Mapping[str, Any], game_pk: int) -> str:
    value = str(game.get("gameDate") or "")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULED_START_MISSING:{game_pk}") from None
    if parsed.tzinfo is None or parsed.year != 2026:
        raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULED_START_INVALID:{game_pk}")
    return value


def _source_set(
    source_set_path: Path, *, root: Path
) -> tuple[Any, list[tuple[dict[str, Any], Path]]]:
    source_set = _read_json(source_set_path)
    if source_set.get("contract_name") != SOURCE_SET_CONTRACT_NAME:
        raise SnapshotBuildError("SOURCE_SET_CONTRACT_INVALID")
    if source_set.get("schema_version") != SOURCE_SET_SCHEMA_VERSION:
        raise SnapshotBuildError("SOURCE_SET_SCHEMA_UNSUPPORTED")
    if not str(source_set.get("source_set_id") or ""):
        raise SnapshotBuildError("SOURCE_SET_ID_MISSING")
    descriptor_text, descriptor_path = _repo_path(
        source_set.get("parent_descriptor_path"), root=root
    )
    parent_hash = str(source_set.get("parent_descriptor_sha256") or "")
    if re.fullmatch(r"[0-9a-f]{64}", parent_hash) is None:
        raise SnapshotBuildError("SOURCE_SET_PARENT_HASH_INVALID")
    try:
        parent = verify_descriptor_chain(
            descriptor_path,
            expected_sha256=parent_hash,
            root=root,
            allow_candidate=True,
        )
    except SnapshotDescriptorError as exc:
        raise SnapshotBuildError(f"SOURCE_SET_PARENT_INVALID:{exc}") from exc
    rows = source_set.get("sources")
    if not isinstance(rows, list) or not rows:
        raise SnapshotBuildError("SOURCE_SET_EMPTY")
    selected: list[tuple[dict[str, Any], Path]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SnapshotBuildError("SOURCE_SET_ROW_INVALID")
        source_text, source_path = _repo_path(row.get("source_path"), root=root)
        if source_text in seen:
            raise SnapshotBuildError(f"SOURCE_SET_DUPLICATE_PATH:{source_text}")
        seen.add(source_text)
        expected_hash = str(row.get("source_sha256") or "")
        if re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None:
            raise SnapshotBuildError(f"SOURCE_SET_HASH_INVALID:{source_text}")
        try:
            expected_bytes = int(row.get("source_bytes"))
        except (TypeError, ValueError):
            raise SnapshotBuildError(f"SOURCE_SET_BYTES_INVALID:{source_text}") from None
        if not source_path.is_file():
            raise SnapshotBuildError(f"RETAINED_EVIDENCE_MISSING:{source_text}")
        if source_path.stat().st_size != expected_bytes or sha256_file(source_path) != expected_hash:
            raise SnapshotBuildError(f"RETAINED_EVIDENCE_HASH_MISMATCH:{source_text}")
        selected.append((dict(row), source_path))
    selected.sort(key=lambda item: str(item[0]["source_path"]))
    return parent, selected


def _same_existing(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> None:
    game_pk = int(existing["game_pk"])
    for field in IMMUTABLE_EXISTING_FIELDS:
        if field == "schedule_relationships":
            prior = dict(existing.get(field) or {})
            for key, value in dict(incoming.get(field) or {}).items():
                if key in prior and prior[key] not in (None, value) and value is not None:
                    raise SnapshotBuildError(
                        f"EXISTING_GAME_RELATIONSHIP_CONFLICT:{game_pk}:{key}"
                    )
            continue
        if existing.get(field) != incoming.get(field):
            raise SnapshotBuildError(f"EXISTING_GAME_MUTATION_REJECTED:{game_pk}:{field}")


def _merge_new(
    current: dict[str, Any], incoming: Mapping[str, Any], source_path: str, source_hash: str
) -> None:
    game_pk = int(current["game_pk"])
    for field in IMMUTABLE_EXISTING_FIELDS:
        if field == "schedule_relationships":
            relationships = dict(current.get(field) or {})
            for key, value in dict(incoming.get(field) or {}).items():
                if key in relationships and relationships[key] not in (None, value) and value is not None:
                    raise SnapshotBuildError(f"NEW_GAME_RELATIONSHIP_CONFLICT:{game_pk}:{key}")
                if value is not None or key not in relationships:
                    relationships[key] = value
            current[field] = relationships
            continue
        if current.get(field) != incoming.get(field):
            raise SnapshotBuildError(f"NEW_GAME_IDENTITY_CONFLICT:{game_pk}:{field}")
    if current.get("scheduled_start_utc") != incoming.get("scheduled_start_utc"):
        raise SnapshotBuildError(f"NEW_GAME_SCHEDULED_START_CONFLICT:{game_pk}")
    current["source_paths"] = sorted(set(current["source_paths"]) | {source_path})
    current["source_hashes"] = sorted(set(current["source_hashes"]) | {source_hash})


def _classification_hash(rows: Iterable[Mapping[str, Any]]) -> str:
    values = [{key: row.get(key) for key in CLASSIFICATION_FIELDS} for row in rows]
    return hashlib.sha256(canonical_json_bytes(values)).hexdigest()


def _write_atomic(path: Path, body: bytes) -> None:
    path.write_bytes(body)


def build_candidate_snapshot(
    *, source_set_path: Path, output_dir: Path, root: Path = REPO_ROOT
) -> dict[str, Any]:
    """Build and validate one candidate; never changes active selection."""

    root = root.resolve()
    output_dir = output_dir.resolve()
    try:
        output_dir.relative_to(root)
    except ValueError:
        raise SnapshotBuildError("OUTPUT_DIRECTORY_OUTSIDE_REPOSITORY") from None
    if output_dir.exists():
        raise SnapshotBuildError("OUTPUT_DIRECTORY_ALREADY_EXISTS")
    parent, selected = _source_set(source_set_path.resolve(), root=root)
    parent_rows = _read_jsonl(parent.proposal_path)
    parent_by_game = {int(row["game_pk"]): row for row in parent_rows}
    if len(parent_by_game) != len(parent_rows):
        raise SnapshotBuildError("PARENT_DUPLICATE_GAME_PK")
    parent_source_rows = _read_jsonl(parent.source_manifest_path)
    parent_sources = {str(row["source_path"]): row for row in parent_source_rows}
    if len(parent_sources) != len(parent_source_rows):
        raise SnapshotBuildError("PARENT_DUPLICATE_SOURCE_PATH")

    new_records: dict[int, dict[str, Any]] = {}
    appended_sources: list[dict[str, Any]] = []
    consistent_existing_observations = 0
    repeated_new_observations = 0
    for source_row, source_path in selected:
        source_text = str(source_row["source_path"])
        source_hash = str(source_row["source_sha256"])
        prior_source = parent_sources.get(source_text)
        if prior_source is not None:
            if (
                prior_source.get("source_sha256") != source_hash
                or int(prior_source.get("source_bytes")) != int(source_row["source_bytes"])
            ):
                raise SnapshotBuildError(f"PARENT_SOURCE_PATH_CONFLICT:{source_text}")
            continue
        try:
            payload = json.loads(source_path.read_bytes())
        except json.JSONDecodeError as exc:
            raise SnapshotBuildError(f"STATSAPI_SOURCE_MALFORMED:{source_text}") from exc
        games = _schedule_games(payload, source_text)
        schedule_game_row_count = len(games)
        requested_pks = source_row.get("selected_game_pks")
        if requested_pks is not None:
            try:
                selected_pks = [int(value) for value in requested_pks]
            except (TypeError, ValueError):
                raise SnapshotBuildError(f"SOURCE_SET_GAME_PK_SELECTION_INVALID:{source_text}") from None
            if not selected_pks or len(selected_pks) != len(set(selected_pks)):
                raise SnapshotBuildError(f"SOURCE_SET_GAME_PK_SELECTION_INVALID:{source_text}")
            matches = [game for game in games if int(game.get("gamePk") or 0) in set(selected_pks)]
            counts = Counter(int(game.get("gamePk") or 0) for game in matches)
            if set(counts) != set(selected_pks) or any(count != 1 for count in counts.values()):
                raise SnapshotBuildError(f"SOURCE_SET_SELECTED_GAME_PK_NOT_UNIQUE_OR_MISSING:{source_text}")
            games = matches
        manifest_row = {
            "schedule_game_rows": schedule_game_row_count,
            "selected_game_pks": sorted(selected_pks) if requested_pks is not None else None,
            "source_bytes": int(source_row["source_bytes"]),
            "source_path": source_text,
            "source_sha256": source_hash,
        }
        appended_sources.append(manifest_row)
        for game in games:
            try:
                record = canonical_phase_record(
                    game, source_sha256=source_hash, source_path=source_text
                )
            except PhaseContractError as exc:
                raise SnapshotBuildError(f"AUTHORITATIVE_CLASSIFICATION_REJECTED:{exc}") from exc
            game_pk = int(record["game_pk"])
            if int(record["source_season"]) != int(parent.data["supported_season"]):
                raise SnapshotBuildError(f"AUTHORITATIVE_SEASON_CONFLICT:{game_pk}")
            record["scheduled_start_utc"] = _scheduled_start(game, game_pk)
            if record.get("season_phase") is None:
                raise SnapshotBuildError(f"NEW_AUTHORITY_SPECIAL_EXCLUDED:{game_pk}")
            existing = parent_by_game.get(game_pk)
            if existing is not None:
                _same_existing(existing, record)
                consistent_existing_observations += 1
                continue
            # Newly appended identities must carry a complete ordinary schedule
            # status and explicit game type/season. Never infer these fields.
            if requested_pks is not None:
                status = game.get("status")
                if not isinstance(status, dict) or any(
                    not str(status.get(field) or "").strip()
                    for field in ("abstractGameState", "codedGameState", "detailedState", "statusCode")
                ):
                    raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULE_STATUS_INCOMPLETE:{game_pk}")
                if str(status.get("codedGameState")) != str(status.get("statusCode")):
                    raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULE_STATUS_CONFLICT:{game_pk}")
                abstract, coded = str(status.get("abstractGameState")), str(status.get("codedGameState"))
                if (abstract == "Final") != (coded == "F"):
                    raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULE_STATUS_CONFLICT:{game_pk}")
            if not str(game.get("gameType") or "").strip() or game.get("season") in (None, ""):
                raise SnapshotBuildError(f"AUTHORITATIVE_SCHEDULE_IDENTITY_INCOMPLETE:{game_pk}")
            if record.get("season_phase") == "POSTSEASON" and not str(
                record.get("source_round") or ""
            ).strip():
                raise SnapshotBuildError(f"AUTHORITATIVE_POSTSEASON_ROUND_MISSING:{game_pk}")
            current = new_records.get(game_pk)
            if current is None:
                record["source_paths"] = [source_text]
                record["source_hashes"] = [source_hash]
                new_records[game_pk] = record
            else:
                _merge_new(current, record, source_text, source_hash)
                repeated_new_observations += 1

    if not new_records:
        return {
            "contract_name": BUILDER_CONTRACT_NAME,
            "builder_version": BUILDER_VERSION,
            "decision": NO_NEW_AUTHORITY_EVIDENCE,
            "candidate_created": False,
            "network_requests": 0,
            "database_requests": 0,
            "selected_source_count": len(selected),
            "consistent_existing_observations": consistent_existing_observations,
            "output_directory_created": False,
        }

    new_rows = [new_records[key] for key in sorted(new_records)]
    full_rows = parent_rows + new_rows
    full_source_rows = parent_source_rows + sorted(
        appended_sources, key=lambda row: row["source_path"]
    )
    if len({int(row["game_pk"]) for row in full_rows}) != len(full_rows):
        raise SnapshotBuildError("CANDIDATE_DUPLICATE_GAME_PK")
    if len({str(row["source_path"]) for row in full_source_rows}) != len(full_source_rows):
        raise SnapshotBuildError("CANDIDATE_DUPLICATE_SOURCE_PATH")

    parent_proposal_bytes = parent.proposal_path.read_bytes()
    parent_source_bytes = parent.source_manifest_path.read_bytes()
    if not parent_proposal_bytes.endswith(b"\n") or not parent_source_bytes.endswith(b"\n"):
        raise SnapshotBuildError("PARENT_BYTES_NOT_APPEND_SAFE")
    proposal_bytes = parent_proposal_bytes + b"".join(_canonical_line(row) for row in new_rows)
    source_manifest_bytes = parent_source_bytes + b"".join(
        _canonical_line(row)
        for row in sorted(appended_sources, key=lambda row: row["source_path"])
    )
    proposal_sha = hashlib.sha256(proposal_bytes).hexdigest()
    source_manifest_sha = hashlib.sha256(source_manifest_bytes).hexdigest()
    type_counts = dict(sorted(Counter(str(row["source_game_type"]) for row in full_rows).items()))
    phase_counts = dict(
        sorted(Counter(str(row["season_phase"]) for row in full_rows).items())
    )
    game_pks = [int(row["game_pk"]) for row in full_rows]
    scheduled_dates = [
        datetime.fromisoformat(str(row["scheduled_start_utc"]).replace("Z", "+00:00"))
        .date()
        .isoformat()
        for row in new_rows
    ]
    scheduled_from = min(
        str(parent.data["scheduled_date_from"]), min(scheduled_dates)
    )
    scheduled_through = max(
        str(parent.data["scheduled_date_through"]), max(scheduled_dates)
    )
    snapshot_id = f"MLB_2026_GAME_PHASE_AUTHORITY_{proposal_sha[:16].upper()}"
    parent_rel = os.path.relpath(parent.path, output_dir)
    descriptor = {
        "builder_contract": BUILDER_CONTRACT_NAME,
        "builder_version": BUILDER_VERSION,
        "classification_sha256": _classification_hash(full_rows),
        "contract_name": SNAPSHOT_CONTRACT_NAME,
        "deterministic_source_set_sha256": hashlib.sha256(
            canonical_json_bytes(full_source_rows)
        ).hexdigest(),
        "distinct_game_pk_count": len(set(game_pks)),
        "game_pk_population_sha256": hashlib.sha256(
            canonical_json_bytes(game_pks)
        ).hexdigest(),
        "normalized_phase_counts": phase_counts,
        "parent_descriptor_path": parent_rel,
        "parent_descriptor_sha256": parent.sha256,
        "parent_snapshot_id": str(parent.data["snapshot_id"]),
        "proposal_path": PROPOSAL_NAME,
        "proposal_sha256": proposal_sha,
        "raw_game_type_counts": type_counts,
        "root_snapshot_id": str(parent.data["root_snapshot_id"]),
        "row_count": len(full_rows),
        "scheduled_date_from": scheduled_from,
        "scheduled_date_through": scheduled_through,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "snapshot_status": "CANDIDATE",
        "source_file_count": len(full_source_rows),
        "source_manifest_path": SOURCE_MANIFEST_NAME,
        "source_manifest_sha256": source_manifest_sha,
        "source_observation_count": sum(
            int(row["schedule_game_rows"]) for row in full_source_rows
        ),
        "supported_season": 2026,
        "v1_authority_records_sha256": str(
            parent.data["v1_authority_records_sha256"]
        ),
        "v1_proposal_sha256": str(parent.data["v1_proposal_sha256"]),
        "v1_source_manifest_sha256": str(
            parent.data["v1_source_manifest_sha256"]
        ),
    }
    descriptor_bytes = _canonical_line(descriptor)
    descriptor_sha = hashlib.sha256(descriptor_bytes).hexdigest()
    report = {
        "builder_contract": BUILDER_CONTRACT_NAME,
        "builder_version": BUILDER_VERSION,
        "candidate_created": True,
        "candidate_descriptor_sha256": descriptor_sha,
        "decision": CANDIDATE_READY,
        "existing_parent_rows_byte_identical": True,
        "new_game_pk_count": len(new_rows),
        "new_game_pks": [int(row["game_pk"]) for row in new_rows],
        "new_normalized_phase_counts": dict(
            sorted(Counter(str(row["season_phase"]) for row in new_rows).items())
        ),
        "new_raw_game_type_counts": dict(
            sorted(Counter(str(row["source_game_type"]) for row in new_rows).items())
        ),
        "consistent_existing_observations": consistent_existing_observations,
        "repeated_new_observations": repeated_new_observations,
        "network_requests": 0,
        "database_requests": 0,
        "active_selection_modified": False,
        "synthetic_evidence_is_operational_proof": False,
    }
    report_bytes = _canonical_line(report)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        _write_atomic(temp_dir / PROPOSAL_NAME, proposal_bytes)
        _write_atomic(temp_dir / SOURCE_MANIFEST_NAME, source_manifest_bytes)
        _write_atomic(temp_dir / DESCRIPTOR_NAME, descriptor_bytes)
        _write_atomic(temp_dir / VALIDATION_NAME, report_bytes)
        manifest_paths = (
            temp_dir / PROPOSAL_NAME,
            temp_dir / SOURCE_MANIFEST_NAME,
            temp_dir / DESCRIPTOR_NAME,
            temp_dir / VALIDATION_NAME,
        )
        manifest = b"".join(
            f"{sha256_file(path)}  {path.name}\n".encode("utf-8")
            for path in manifest_paths
        )
        _write_atomic(temp_dir / MANIFEST_NAME, manifest)
        try:
            VersionedFileAuthority(
                descriptor_path=temp_dir / DESCRIPTOR_NAME,
                expected_descriptor_sha256=descriptor_sha,
                root=root,
                allow_candidate=True,
            )
        except GamePhaseAuthorityError as exc:
            raise SnapshotBuildError(f"CANDIDATE_LOADER_VALIDATION_FAILED:{exc}") from exc
        os.replace(temp_dir, output_dir)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-set", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = build_candidate_snapshot(
            source_set_path=args.source_set,
            output_dir=args.output_dir,
        )
    except SnapshotBuildError as exc:
        print(json.dumps({"decision": "CANDIDATE_BUILD_BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["decision"] in {NO_NEW_AUTHORITY_EVIDENCE, CANDIDATE_READY} else 2


if __name__ == "__main__":
    raise SystemExit(main())
