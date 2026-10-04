#!/usr/bin/env python3
"""Validate, promote, activate, or roll back the retained Oct 3–4 V4 child."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import tempfile
from pathlib import Path

from backend.mlb.public_game_predictions.phase_gating_v1 import classify_moneyline_row
from backend.mlb.season_transition.game_phase_authority_v1 import VersionedFileAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH, REPO_ROOT, canonical_json_bytes, sha256_file,
    verify_descriptor_chain,
)
from backend.mlb.season_transition.regular_season_close_inventory_v2 import validate_close_readiness_package

V3_SHA = "1ddcb28828215704f4e75ad810e744853443c9b7ed7c8c42fb67a587a99fcb1e"
PARENT_V3 = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v3/descriptor.json"
SNAPSHOT = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v4"
DESCRIPTOR = SNAPSHOT / "descriptor.json"
MANIFEST = SNAPSHOT / "sha256_manifest.txt"
PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_postseason_schedule_authority_v4"
SOURCE_SET = PACKAGE / "source_set.json"
ROLLBACK = PACKAGE / "activation_rollback.json"
CLOSE = REPO_ROOT / "artifacts/operational/mlb/season_close/2026/regular_season_close.json"
CLOSE_SHA = "fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3"
PKS = (849823, 849825, 849828, 849829, 849830, 849835)
ROUNDS = {pk: "NL Division Series" for pk in (849823, 849825, 849828, 849830)}
ROUNDS.update({pk: "AL Division Series" for pk in (849829, 849835)})


class ActivationError(RuntimeError):
    pass


def _atomic_replace(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        os.replace(temp, path)
        dfd = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        temp.unlink(missing_ok=True)


def _expected_manifest() -> bytes:
    names = ("canonical_game_phase_full_snapshot.jsonl", "retained_source_manifest.jsonl",
             "descriptor.json", "validation_report.json")
    return b"".join(f"{sha256_file(SNAPSHOT / name)}  {name}\n".encode() for name in names)


def validate_child(*, allow_candidate: bool = True) -> dict:
    if MANIFEST.read_bytes() != _expected_manifest():
        raise ActivationError("V4_MANIFEST_MISMATCH")
    if sha256_file(CLOSE) != CLOSE_SHA:
        raise ActivationError("REGULAR_SEASON_CLOSE_HASH_CHANGED")
    v3 = verify_descriptor_chain(PARENT_V3, expected_sha256=V3_SHA, root=REPO_ROOT)
    child = verify_descriptor_chain(DESCRIPTOR, expected_sha256=sha256_file(DESCRIPTOR),
                                    root=REPO_ROOT, allow_candidate=allow_candidate)
    if child.parent is None or child.parent.sha256 != v3.sha256:
        raise ActivationError("V4_PARENT_NOT_V3")
    parent_bytes = v3.proposal_path.read_bytes()
    raw = child.proposal_path.read_bytes()
    if not raw.startswith(parent_bytes):
        raise ActivationError("V3_PARENT_PROPOSAL_BYTES_CHANGED")
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    prior = [json.loads(line) for line in parent_bytes.splitlines() if line.strip()]
    additions = rows[len(prior):]
    if tuple(sorted(int(row["game_pk"]) for row in additions)) != PKS:
        raise ActivationError("V4_EXACT_GAME_PK_SET_MISMATCH")
    for row in additions:
        pk = int(row["game_pk"])
        if (row.get("source_season") != 2026 or row.get("source_game_type") != "D"
                or row.get("season_phase") != "POSTSEASON"
                or row.get("postseason_round") != "DIVISION_SERIES"
                or row.get("source_round") != ROUNDS[pk]
                or not row.get("source_hashes")):
            raise ActivationError(f"V4_CLASSIFICATION_MISMATCH:{pk}")
    source_set = json.loads(SOURCE_SET.read_text())
    actual_hashes = {str(row["source_sha256"]) for row in source_set["sources"]}
    if actual_hashes != {str(h) for row in additions for h in row["source_hashes"]}:
        raise ActivationError("V4_SOURCE_SET_BINDING_MISMATCH")
    source_rows = [json.loads(line) for line in child.source_manifest_path.read_text().splitlines() if line.strip()]
    selected = [row for row in source_rows if row.get("source_sha256") in actual_hashes]
    selected_pks = {int(pk) for row in selected for pk in row.get("selected_game_pks") or []}
    if selected_pks != set(PKS) or len(selected) != 2:
        raise ActivationError("V4_SOURCE_MANIFEST_SELECTION_MISMATCH")
    authority = VersionedFileAuthority(descriptor_path=DESCRIPTOR,
                                       expected_descriptor_sha256=sha256_file(DESCRIPTOR),
                                       allow_candidate=allow_candidate)
    for pk in PKS:
        decision = classify_moneyline_row({"game_id": pk, "game_date": "2026-10-04"}, authority=authority)
        if decision.evaluation_partition != "POSTSEASON":
            raise ActivationError(f"V4_MONEYLINE_EXACT_GAME_GATE_FAILED:{pk}")
    if authority.metadata.phase_counts.get("REGULAR_SEASON") != 2430:
        raise ActivationError("REGULAR_SEASON_POPULATION_CHANGED")
    close = validate_close_readiness_package()
    if (not close.get("integrity_passed") or not close.get("close_ready")
            or close.get("population_counts", {}).get("regular_season_game_pks") != 2430):
        raise ActivationError("REGULAR_SEASON_CLOSE_VALIDATION_FAILED")
    return {"descriptor_sha256": sha256_file(DESCRIPTOR),
            "proposal_sha256": sha256_file(child.proposal_path),
            "source_manifest_sha256": sha256_file(child.source_manifest_path),
            "row_count": len(rows), "parent_row_count": len(prior), "new_game_pks": list(PKS),
            "supported_through": child.data["scheduled_date_through"],
            "snapshot_status": child.data["snapshot_status"], "close_sha256": CLOSE_SHA,
            "close_readiness": close["decision"]}


def promote() -> dict:
    result = validate_child(allow_candidate=True)
    if result["snapshot_status"] != "CANDIDATE" or (PACKAGE / "promotion_record.json").exists():
        raise ActivationError("PROMOTION_STATE_INVALID_OR_ALREADY_RECORDED")
    data = json.loads(DESCRIPTOR.read_text())
    candidate_hash = result["descriptor_sha256"]
    data["snapshot_status"] = "GOVERNED"
    _atomic_replace(DESCRIPTOR, canonical_json_bytes(data) + b"\n")
    _atomic_replace(MANIFEST, _expected_manifest())
    result = validate_child(allow_candidate=False)
    record = {"contract_name": "MLB_2026_POSTSEASON_SCHEDULE_AUTHORITY_PROMOTION_V1",
              "candidate_descriptor_sha256": candidate_hash,
              "governed_descriptor_sha256": result["descriptor_sha256"],
              "proposal_sha256": result["proposal_sha256"],
              "source_manifest_sha256": result["source_manifest_sha256"]}
    fd = os.open(PACKAGE / "promotion_record.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream: stream.write(canonical_json_bytes(record) + b"\n")
    return record


def activate() -> dict:
    child = validate_child(allow_candidate=False)
    prior = ACTIVE_SELECTION_PATH.read_bytes()
    active = json.loads(prior)
    if active.get("descriptor_sha256") != V3_SHA:
        raise ActivationError("ACTIVE_SELECTION_NOT_PINNED_V3")
    selected = canonical_json_bytes({
        "contract_name": "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1",
        "descriptor_path": str(DESCRIPTOR.relative_to(REPO_ROOT)), "descriptor_sha256": child["descriptor_sha256"],
        "schema_version": 1, "selection_status": "GOVERNED_ACTIVE",
    }) + b"\n"
    rollback = {"contract_name": "MLB_2026_PHASE_AUTHORITY_SELECTION_ROLLBACK_V1",
                "prior_selection_base64": base64.b64encode(prior).decode(),
                "prior_selection_sha256": hashlib.sha256(prior).hexdigest(),
                "child_selection_base64": base64.b64encode(selected).decode(),
                "child_selection_sha256": hashlib.sha256(selected).hexdigest(),
                "child_descriptor_sha256": child["descriptor_sha256"],
                "rollback_mode": "COMPARE_AND_SWAP_ONLY"}
    fd = os.open(ROLLBACK, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream: stream.write(canonical_json_bytes(rollback) + b"\n")
    if ACTIVE_SELECTION_PATH.read_bytes() != prior:
        raise ActivationError("ACTIVE_SELECTION_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(ACTIVE_SELECTION_PATH, selected)
    return child


def rollback() -> dict:
    record = json.loads(ROLLBACK.read_text())
    prior = base64.b64decode(record["prior_selection_base64"])
    child = base64.b64decode(record["child_selection_base64"])
    current = ACTIVE_SELECTION_PATH.read_bytes()
    if current != child or hashlib.sha256(current).hexdigest() != record["child_selection_sha256"]:
        raise ActivationError("ROLLBACK_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(ACTIVE_SELECTION_PATH, prior)
    return {"status": "ROLLED_BACK", "restored_sha256": hashlib.sha256(prior).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group(required=True)
    for name in ("validate", "promote", "activate", "rollback"):
        actions.add_argument(f"--{name}", action="store_true")
    args = parser.parse_args()
    try:
        result = (promote() if args.promote else activate() if args.activate else
                  rollback() if args.rollback else validate_child())
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
