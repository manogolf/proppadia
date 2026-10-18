#!/usr/bin/env python3
"""Prepare and validate the inactive, governed V5 ALCS authority child."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.mlb.scripts.build_mlb_versioned_file_phase_authority_v1 import (
    CANDIDATE_READY,
    build_candidate_snapshot,
)
from backend.mlb.season_transition.game_phase_authority_v1 import VersionedFileAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH,
    REPO_ROOT,
    canonical_json_bytes,
    sha256_file,
    verify_descriptor_chain,
)


PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_postseason_alcs_authority_v5"
SOURCE_SET = PACKAGE / "source_set.json"
SNAPSHOT = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v5"
DESCRIPTOR = SNAPSHOT / "descriptor.json"
MANIFEST = SNAPSHOT / "sha256_manifest.txt"
PROPOSAL = SNAPSHOT / "canonical_game_phase_full_snapshot.jsonl"
SOURCE_MANIFEST = SNAPSHOT / "retained_source_manifest.jsonl"
BUILD_REPORT = SNAPSHOT / "validation_report.json"
PROMOTION_RECORD = PACKAGE / "inactive_promotion_record.json"
PROPOSED_SELECTION = PACKAGE / "proposed_active_selection.json"
PROPOSED_SELECTION_SHA = PACKAGE / "proposed_active_selection.sha256"
V4_DESCRIPTOR = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v4/descriptor.json"
V4_DESCRIPTOR_SHA = "7828f1bdd35fa9fa763d8e7100a4215d4fa5fb928d13497535f27872bdd47a77"
ACTIVE_SELECTION_SHA = "1a3a1ea4e5935c247ef0ac96e06595727233c7086d704543b73cd548693a1d57"
EXPECTED_ADDITIONS = (849816, 849817)
EXPECTED_ROUND = "AL Championship Series"
RESCHEDULED_FROM = "2026-10-17T00:00:00Z"


class PackageValidationError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise PackageValidationError(f"JSON_OBJECT_REQUIRED:{path}")
    return value


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _verify_evidence(row: Mapping[str, Any]) -> bytes:
    relative = str(row.get("source_path") or "")
    path = (REPO_ROOT / relative).resolve()
    try:
        path.relative_to(REPO_ROOT.resolve())
    except ValueError:
        raise PackageValidationError(f"EVIDENCE_PATH_OUTSIDE_REPOSITORY:{relative}") from None
    raw = path.read_bytes()
    if len(raw) != int(row["source_bytes"]):
        raise PackageValidationError(f"EVIDENCE_SIZE_MISMATCH:{relative}")
    if hashlib.sha256(raw).hexdigest() != str(row["source_sha256"]):
        raise PackageValidationError(f"EVIDENCE_HASH_MISMATCH:{relative}")
    return raw


def _schedule_games(raw: bytes) -> list[dict[str, Any]]:
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
        raise PackageValidationError("SCHEDULE_DATES_INVALID")
    return [game for day in payload["dates"] for game in day.get("games", [])]


def _source_selection(data: Mapping[str, Any]) -> bytes:
    return canonical_json_bytes(data) + b"\n"


def prepare_inactive_governed_child() -> dict[str, Any]:
    """Build once, mark the new descriptor governed, and leave V4 selected."""
    if sha256_file(ACTIVE_SELECTION_PATH) != ACTIVE_SELECTION_SHA:
        raise PackageValidationError("ACTIVE_SELECTION_NOT_EXPECTED_V4")
    if sha256_file(V4_DESCRIPTOR) != V4_DESCRIPTOR_SHA:
        raise PackageValidationError("V4_DESCRIPTOR_HASH_MISMATCH")
    if SNAPSHOT.exists() or PROMOTION_RECORD.exists() or PROPOSED_SELECTION.exists():
        raise PackageValidationError("V5_OUTPUT_ALREADY_EXISTS")

    build = build_candidate_snapshot(source_set_path=SOURCE_SET, output_dir=SNAPSHOT)
    if build.get("decision") != CANDIDATE_READY or tuple(build.get("new_game_pks", [])) != EXPECTED_ADDITIONS:
        raise PackageValidationError("V5_BUILD_ADDITIONS_MISMATCH")
    candidate_descriptor_sha = sha256_file(DESCRIPTOR)
    descriptor = _read_json(DESCRIPTOR)
    if descriptor.get("snapshot_status") != "CANDIDATE":
        raise PackageValidationError("V5_BUILD_NOT_CANDIDATE")
    descriptor["snapshot_status"] = "GOVERNED"
    descriptor_bytes = canonical_json_bytes(descriptor) + b"\n"
    DESCRIPTOR.write_bytes(descriptor_bytes)

    names = (PROPOSAL.name, SOURCE_MANIFEST.name, DESCRIPTOR.name, BUILD_REPORT.name)
    MANIFEST.write_bytes(b"".join(
        f"{sha256_file(SNAPSHOT / name)}  {name}\n".encode("utf-8") for name in names
    ))

    proposed_selection = {
        "contract_name": "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1",
        "descriptor_path": "backend/mlb/season_transition/authority_snapshots/v5/descriptor.json",
        "descriptor_sha256": hashlib.sha256(descriptor_bytes).hexdigest(),
        "schema_version": 1,
        "selection_status": "GOVERNED_ACTIVE",
    }
    proposed_selection_bytes = _source_selection(proposed_selection)
    promotion = {
        "contract_name": "MLB_2026_INACTIVE_GOVERNED_PHASE_AUTHORITY_PROMOTION_V1",
        "active_selection_sha256_before_and_after": ACTIVE_SELECTION_SHA,
        "candidate_descriptor_sha256": candidate_descriptor_sha,
        "governed_descriptor_sha256": hashlib.sha256(descriptor_bytes).hexdigest(),
        "parent_descriptor_sha256": V4_DESCRIPTOR_SHA,
        "proposal_sha256": sha256_file(PROPOSAL),
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST),
        "source_set_sha256": sha256_file(SOURCE_SET),
        "new_game_pks": list(EXPECTED_ADDITIONS),
        "selection_changed": False,
        "network_requests": 0,
        "database_requests": 0,
    }
    PROMOTION_RECORD.write_bytes(canonical_json_bytes(promotion) + b"\n")
    PROPOSED_SELECTION.write_bytes(proposed_selection_bytes)
    PROPOSED_SELECTION_SHA.write_text(
        hashlib.sha256(proposed_selection_bytes).hexdigest() + "  proposed_active_selection.json\n",
        encoding="utf-8",
    )
    return {"decision": "GOVERNED_CHILD_PREPARED_INACTIVE", **promotion}


def validate_package() -> dict[str, Any]:
    """Read-only verification of the governed child and all pinned evidence."""
    if sha256_file(ACTIVE_SELECTION_PATH) != ACTIVE_SELECTION_SHA:
        raise PackageValidationError("ACTIVE_SELECTION_CHANGED")
    if sha256_file(V4_DESCRIPTOR) != V4_DESCRIPTOR_SHA:
        raise PackageValidationError("V4_DESCRIPTOR_CHANGED")
    source_set = _read_json(SOURCE_SET)
    promotion = _read_json(PROMOTION_RECORD)
    if promotion.get("source_set_sha256") != sha256_file(SOURCE_SET):
        raise PackageValidationError("SOURCE_SET_HASH_MISMATCH")

    descriptor_sha = sha256_file(DESCRIPTOR)
    verified = verify_descriptor_chain(
        DESCRIPTOR, expected_sha256=descriptor_sha, root=REPO_ROOT, allow_candidate=False
    )
    if verified.parent is None or verified.parent.sha256 != V4_DESCRIPTOR_SHA:
        raise PackageValidationError("V5_PARENT_NOT_ACTIVE_V4")
    if verified.data.get("snapshot_status") != "GOVERNED":
        raise PackageValidationError("V5_NOT_GOVERNED")
    if promotion.get("governed_descriptor_sha256") != descriptor_sha:
        raise PackageValidationError("PROMOTION_DESCRIPTOR_HASH_MISMATCH")
    if promotion.get("candidate_descriptor_sha256") == descriptor_sha:
        raise PackageValidationError("PROMOTION_HASHES_NOT_DISTINCT")
    candidate_descriptor = dict(verified.data)
    candidate_descriptor["snapshot_status"] = "CANDIDATE"
    reconstructed_candidate_sha = hashlib.sha256(
        canonical_json_bytes(candidate_descriptor) + b"\n"
    ).hexdigest()
    if reconstructed_candidate_sha != promotion.get("candidate_descriptor_sha256"):
        raise PackageValidationError("CANDIDATE_TO_GOVERNED_TRANSITION_MISMATCH")

    parent_proposal = verified.parent.proposal_path.read_bytes()
    proposal_bytes = PROPOSAL.read_bytes()
    if not proposal_bytes.startswith(parent_proposal):
        raise PackageValidationError("V4_PROPOSAL_PREFIX_CHANGED")
    proposal_rows = _jsonl(PROPOSAL)
    parent_rows = _jsonl(verified.parent.proposal_path)
    additions = proposal_rows[len(parent_rows):]
    if [int(row["game_pk"]) for row in additions] != list(EXPECTED_ADDITIONS):
        raise PackageValidationError("V5_EXACT_ADDITIONS_MISMATCH")
    by_pk = {int(row["game_pk"]): row for row in additions}
    for pk, row in by_pk.items():
        if (row.get("source_season") != 2026 or row.get("source_game_type") != "L"
                or row.get("season_phase") != "POSTSEASON"
                or row.get("postseason_round") != "LEAGUE_CHAMPIONSHIP_SERIES"
                or row.get("source_round") != EXPECTED_ROUND):
            raise PackageValidationError(f"V5_CLASSIFICATION_MISMATCH:{pk}")
    if by_pk[849817].get("schedule_relationships", {}).get("rescheduledFrom") != RESCHEDULED_FROM:
        raise PackageValidationError("V5_RESCHEDULE_RELATIONSHIP_MISSING")
    if by_pk[849816].get("schedule_relationships"):
        raise PackageValidationError("V5_GAME_849816_UNEXPECTED_RELATIONSHIP")

    source_rows = source_set.get("sources")
    if not isinstance(source_rows, list) or len(source_rows) != 3 or {
        tuple(row["selected_game_pks"]) for row in source_rows
    } != {
        (849816,), (849817,)
    }:
        raise PackageValidationError("V5_SOURCE_SET_SELECTION_MISMATCH")
    selected_hashes: dict[int, set[str]] = {849816: set(), 849817: set()}
    expected_starts = {849816: "2026-10-18T00:08:00Z", 849817: "2026-10-18T21:08:00Z"}
    for source in source_rows:
        raw = _verify_evidence(source)
        games = _schedule_games(raw)
        pk = int(source["selected_game_pks"][0])
        matches = [game for game in games if int(game.get("gamePk") or 0) == pk]
        if len(matches) != 1:
            raise PackageValidationError(f"SELECTED_SOURCE_GAME_NOT_UNIQUE:{pk}")
        game = matches[0]
        if (str(game.get("gameType")) != "L" or int(game.get("season")) != 2026
                or game.get("seriesDescription") != EXPECTED_ROUND):
            raise PackageValidationError(f"SELECTED_SOURCE_IDENTITY_MISMATCH:{pk}")
        if str(game.get("gameDate") or "") != expected_starts[pk]:
            raise PackageValidationError(f"SELECTED_SOURCE_START_MISMATCH:{pk}")
        if pk == 849817 and game.get("rescheduledFrom") != RESCHEDULED_FROM:
            raise PackageValidationError("SELECTED_RESCHEDULE_RELATIONSHIP_MISMATCH")
        selected_hashes[pk].add(str(source["source_sha256"]))
    for pk in EXPECTED_ADDITIONS:
        if set(by_pk[pk].get("source_hashes") or []) != selected_hashes[pk]:
            raise PackageValidationError(f"V5_PROPOSAL_SOURCE_HASH_MISMATCH:{pk}")

    corroborating = source_set.get("corroborating_schedule_evidence") or []
    corroborating_games: dict[str, list[dict[str, Any]]] = {}
    for evidence in corroborating:
        games = _schedule_games(_verify_evidence(evidence))
        expected = {int(value) for value in evidence.get("game_pks") or []}
        observed = {int(game.get("gamePk") or 0) for game in games}
        if not expected.issubset(observed):
            raise PackageValidationError("CORROBORATING_GAME_MISSING")
        corroborating_games[str(evidence["source_sha256"])] = games
    delayed_sha = "50e2a6d70ec32797a9cfacbda05fd549a290d49c6d753e905751cdfb7800554c"
    delayed = [game for game in corroborating_games[delayed_sha]
               if int(game.get("gamePk") or 0) == 849817]
    if len(delayed) != 1:
        raise PackageValidationError("DELAYED_START_SOURCE_GAME_INVALID")
    status = delayed[0].get("status") or {}
    if (status.get("abstractGameState"), status.get("abstractGameCode"),
            status.get("codedGameState"), status.get("statusCode"),
            status.get("detailedState")) != (
            "Preview", "P", "P", "PI", "Delayed Start"):
        raise PackageValidationError("DELAYED_START_SOURCE_STATUS_MISMATCH")

    selections = source_set.get("selection_evidence") or []
    if len(selections) != 3:
        raise PackageValidationError("SELECTION_EVIDENCE_COUNT_MISMATCH")
    pinned_schedule_sources = {
        str(row["source_sha256"]): str(row["source_path"])
        for row in [*(source_set.get("sources") or []), *corroborating]
    }
    for evidence in selections:
        receipt = json.loads(_verify_evidence(evidence))
        schedule = receipt.get("history_schedule") or {}
        if schedule.get("source_sha256") != evidence.get("schedule_sha256"):
            raise PackageValidationError("SELECTION_SCHEDULE_HASH_BINDING_MISMATCH")
        pinned_schedule_path = pinned_schedule_sources.get(str(evidence.get("schedule_sha256")))
        if not pinned_schedule_path:
            raise PackageValidationError("SELECTION_SCHEDULE_SOURCE_NOT_PINNED")
        if schedule.get("source_path") != pinned_schedule_path:
            raise PackageValidationError("SELECTION_SCHEDULE_PATH_BINDING_MISMATCH")
        decision_rows = {int(row["game_pk"]): row for row in receipt.get("decisions", [])}
        if not {849816, 849817}.issubset(decision_rows):
            raise PackageValidationError("SELECTION_GAME_DECISION_MISSING")
        appearances = decision_rows[849817].get("appearances") or []
        if not any(
            (item.get("relations") or {}).get("rescheduled_from") == RESCHEDULED_FROM
            for item in appearances
        ):
            raise PackageValidationError("SELECTION_RESCHEDULE_LINK_MISSING")

    proposed_bytes = PROPOSED_SELECTION.read_bytes()
    proposed = json.loads(proposed_bytes)
    if (proposed.get("contract_name") != "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1"
            or proposed.get("descriptor_path") != "backend/mlb/season_transition/authority_snapshots/v5/descriptor.json"
            or proposed.get("schema_version") != 1
            or proposed.get("selection_status") != "GOVERNED_ACTIVE"
            or proposed.get("descriptor_sha256") != descriptor_sha):
        raise PackageValidationError("PROPOSED_SELECTION_DESCRIPTOR_MISMATCH")
    if hashlib.sha256(proposed_bytes).hexdigest() != PROPOSED_SELECTION_SHA.read_text().split()[0]:
        raise PackageValidationError("PROPOSED_SELECTION_FILE_HASH_MISMATCH")
    if promotion.get("active_selection_sha256_before_and_after") != ACTIVE_SELECTION_SHA:
        raise PackageValidationError("ACTIVE_SELECTION_COMMITMENT_MISMATCH")

    manifest_lines = MANIFEST.read_text(encoding="utf-8").splitlines()
    expected_manifest = [
        f"{sha256_file(SNAPSHOT / name)}  {name}"
        for name in (PROPOSAL.name, SOURCE_MANIFEST.name, DESCRIPTOR.name, BUILD_REPORT.name)
    ]
    if manifest_lines != expected_manifest:
        raise PackageValidationError("V5_SNAPSHOT_MANIFEST_MISMATCH")
    build_report = _read_json(BUILD_REPORT)
    if build_report.get("decision") != CANDIDATE_READY or build_report.get("active_selection_modified") is not False:
        raise PackageValidationError("V5_BUILD_REPORT_MISMATCH")
    if build_report.get("candidate_descriptor_sha256") != promotion.get("candidate_descriptor_sha256"):
        raise PackageValidationError("V5_BUILD_PROMOTION_BINDING_MISMATCH")
    if VersionedFileAuthority(
        descriptor_path=DESCRIPTOR, expected_descriptor_sha256=descriptor_sha, root=REPO_ROOT
    ).metadata.proposal_count != len(proposal_rows):
        raise PackageValidationError("V5_LOADER_COUNT_MISMATCH")
    return {
        "decision": "GOVERNED_CHILD_VALID_INACTIVE",
        "active_selection_sha256": sha256_file(ACTIVE_SELECTION_PATH),
        "parent_descriptor_sha256": V4_DESCRIPTOR_SHA,
        "descriptor_sha256": descriptor_sha,
        "proposal_sha256": sha256_file(PROPOSAL),
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST),
        "new_game_pks": list(EXPECTED_ADDITIONS),
        "row_count": len(proposal_rows),
        "postseason_count": verified.data["normalized_phase_counts"]["POSTSEASON"],
        "scheduled_date_through": verified.data["scheduled_date_through"],
        "selection_changed": False,
        "network_requests": 0,
        "database_requests": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prepare", action="store_true")
    group.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    try:
        result = prepare_inactive_governed_child() if args.prepare else validate_package()
    except Exception as exc:
        print(json.dumps({"decision": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
