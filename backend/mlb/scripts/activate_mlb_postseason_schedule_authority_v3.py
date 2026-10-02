#!/usr/bin/env python3
"""Validate/promote/activate the exact-game postseason schedule extension V3."""
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

PKS = (849841, 849842, 849844, 849846, 849848)
ROUNDS = {849841: "NL Wild Card Series", 849842: "NL Wild Card Series",
          849844: "NL Wild Card Series", 849846: "AL Wild Card Series",
          849848: "AL Wild Card Series"}
SOURCE_SHA = "25c70d96c0f7b52c46ae06082e10d2ebdd1ac2148be79f56de540670b9244fb0"
PARENT_SHA = "cde49ebcaf789db1f0a33e512886d64ef1832c356f536ac554e56fd68a16a807"
CLOSE = REPO_ROOT / "artifacts/operational/mlb/season_close/2026/regular_season_close.json"
CLOSE_SHA = "fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3"
PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_postseason_schedule_authority_ingestion_v1"
SNAPSHOT = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v3"
DESCRIPTOR = SNAPSHOT / "descriptor.json"
MANIFEST = SNAPSHOT / "sha256_manifest.txt"
ROLLBACK = PACKAGE / "activation_rollback.json"


class IngestionAuthorityError(RuntimeError):
    pass


def _atomic_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.replace(temp, path)
        dfd = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        temp.unlink(missing_ok=True)


def _manifest() -> bytes:
    names = ("canonical_game_phase_full_snapshot.jsonl", "retained_source_manifest.jsonl",
             "descriptor.json", "validation_report.json")
    return b"".join(f"{sha256_file(SNAPSHOT / n)}  {n}\n".encode() for n in names)


def validate_child(*, allow_candidate: bool) -> dict:
    if MANIFEST.read_bytes() != _manifest():
        raise IngestionAuthorityError("CHILD_MANIFEST_MISMATCH")
    if sha256_file(CLOSE) != CLOSE_SHA:
        raise IngestionAuthorityError("REGULAR_SEASON_CLOSE_HASH_CHANGED")
    if sha256_file(REPO_ROOT / "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-10-01/20261001T123006472388Z_2026-08-05_2026-10-01_25c70d96c0f7b52c46ae06082e10d2ebdd1ac2148be79f56de540670b9244fb0.json") != SOURCE_SHA:
        raise IngestionAuthorityError("RETAINED_SCHEDULE_HASH_MISMATCH")
    child = verify_descriptor_chain(DESCRIPTOR, expected_sha256=sha256_file(DESCRIPTOR),
                                    root=REPO_ROOT, allow_candidate=allow_candidate)
    if child.parent is None or child.parent.sha256 != PARENT_SHA:
        raise IngestionAuthorityError("PARENT_DESCRIPTOR_MISMATCH")
    parent_bytes = child.parent.proposal_path.read_bytes()
    raw = child.proposal_path.read_bytes()
    if not raw.startswith(parent_bytes):
        raise IngestionAuthorityError("PARENT_PROPOSAL_BYTES_CHANGED")
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    parent_rows = [json.loads(line) for line in parent_bytes.splitlines() if line.strip()]
    additions = rows[len(parent_rows):]
    if tuple(sorted(int(row["game_pk"]) for row in additions)) != PKS:
        raise IngestionAuthorityError("APPENDED_GAME_PK_SET_MISMATCH")
    for row in additions:
        pk = int(row["game_pk"])
        if (row.get("source_season") != 2026 or row.get("source_game_type") != "F"
                or row.get("season_phase") != "POSTSEASON"
                or row.get("postseason_round") != "WILD_CARD"
                or row.get("source_round") != ROUNDS[pk]
                or row.get("game_type_source_sha256") != SOURCE_SHA):
            raise IngestionAuthorityError(f"APPENDED_CLASSIFICATION_MISMATCH:{pk}")
    source_rows = [json.loads(line) for line in child.source_manifest_path.read_text().splitlines() if line.strip()]
    selected = [row for row in source_rows if row.get("source_sha256") == SOURCE_SHA]
    if (len(selected) != 1 or selected[0].get("selected_game_pks") != list(PKS)
            or selected[0].get("source_bytes") != 4431941
            or selected[0].get("schedule_game_rows") != 735):
        raise IngestionAuthorityError("SOURCE_MANIFEST_BINDING_MISMATCH")
    authority = VersionedFileAuthority(descriptor_path=DESCRIPTOR,
                                       expected_descriptor_sha256=sha256_file(DESCRIPTOR),
                                       allow_candidate=allow_candidate)
    authority.require_supported_window("2026-09-30", "2026-10-02")
    for pk in PKS:
        decision = classify_moneyline_row({"game_id": pk, "game_date": "2026-10-01"}, authority=authority)
        if decision.evaluation_partition != "POSTSEASON":
            raise IngestionAuthorityError(f"MONEYLINE_PHASE_GATE_FAILED:{pk}")
    if authority.metadata.phase_counts.get("REGULAR_SEASON") != 2430:
        raise IngestionAuthorityError("REGULAR_SEASON_POPULATION_CHANGED")
    close_readiness = validate_close_readiness_package()
    if (not close_readiness.get("integrity_passed") or not close_readiness.get("close_ready")
            or close_readiness.get("population_counts", {}).get("regular_season_game_pks") != 2430):
        raise IngestionAuthorityError("REGULAR_SEASON_CLOSE_VALIDATION_FAILED")
    return {"descriptor_sha256": sha256_file(DESCRIPTOR), "proposal_sha256": sha256_file(child.proposal_path),
            "source_manifest_sha256": sha256_file(child.source_manifest_path), "row_count": len(rows),
            "parent_row_count": len(parent_rows), "new_game_pks": list(PKS),
            "snapshot_status": child.data["snapshot_status"], "close_sha256": CLOSE_SHA,
            "close_readiness": close_readiness["decision"]}


def promote() -> dict:
    result = validate_child(allow_candidate=True)
    if result["snapshot_status"] != "CANDIDATE" or (PACKAGE / "promotion_record.json").exists():
        raise IngestionAuthorityError("PROMOTION_STATE_INVALID_OR_ALREADY_RECORDED")
    data = json.loads(DESCRIPTOR.read_text())
    candidate_sha = result["descriptor_sha256"]
    data["snapshot_status"] = "GOVERNED"
    _atomic_replace(DESCRIPTOR, canonical_json_bytes(data) + b"\n")
    _atomic_replace(MANIFEST, _manifest())
    result = validate_child(allow_candidate=False)
    record = {"contract_name": "MLB_2026_POSTSEASON_SCHEDULE_AUTHORITY_PROMOTION_V1",
              "candidate_descriptor_sha256": candidate_sha,
              "governed_descriptor_sha256": result["descriptor_sha256"],
              "proposal_sha256": result["proposal_sha256"],
              "source_manifest_sha256": result["source_manifest_sha256"]}
    fd = os.open(PACKAGE / "promotion_record.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream: stream.write(canonical_json_bytes(record) + b"\n")
    return record


def activate() -> dict:
    child = validate_child(allow_candidate=False)
    prior = ACTIVE_SELECTION_PATH.read_bytes()
    prior_obj = json.loads(prior)
    if prior_obj.get("descriptor_sha256") != PARENT_SHA:
        raise IngestionAuthorityError("ACTIVE_SELECTION_NOT_PINNED_V2")
    selection = {"contract_name": "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1",
                 "descriptor_path": str(DESCRIPTOR.relative_to(REPO_ROOT)),
                 "descriptor_sha256": child["descriptor_sha256"], "schema_version": 1,
                 "selection_status": "GOVERNED_ACTIVE"}
    selected = canonical_json_bytes(selection) + b"\n"
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
        raise IngestionAuthorityError("ACTIVE_SELECTION_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(ACTIVE_SELECTION_PATH, selected)
    return child


def rollback() -> dict:
    evidence = json.loads(ROLLBACK.read_text())
    prior = base64.b64decode(evidence["prior_selection_base64"])
    child = base64.b64decode(evidence["child_selection_base64"])
    current = ACTIVE_SELECTION_PATH.read_bytes()
    if current != child or hashlib.sha256(current).hexdigest() != evidence["child_selection_sha256"]:
        raise IngestionAuthorityError("ROLLBACK_COMPARE_AND_SWAP_MISMATCH")
    _atomic_replace(ACTIVE_SELECTION_PATH, prior)
    return {"status": "ROLLED_BACK", "restored_sha256": hashlib.sha256(prior).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    for name in ("validate", "promote", "activate", "rollback"): action.add_argument(f"--{name}", action="store_true")
    args = parser.parse_args()
    try:
        result = (promote() if args.promote else activate() if args.activate else
                  rollback() if args.rollback else validate_child(allow_candidate=True))
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True)); return 2
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
