#!/usr/bin/env python3
"""Validate, promote, and compare-and-swap activate the Sep 29 authority child.

This is an offline file-only operation. Promotion and activation are explicit
subcommands; rollback is a compare-and-swap against the exact child selection.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_V1_DESCRIPTOR_SHA256, VersionedFileAuthority, load_v1_authority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH, REPO_ROOT, SnapshotDescriptorError,
    canonical_json_bytes, sha256_file, verify_descriptor_chain,
)

PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_postseason_authority_extension_v1"
SNAPSHOT = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v2"
DESCRIPTOR = SNAPSHOT / "descriptor.json"
MANIFEST = SNAPSHOT / "sha256_manifest.txt"
SOURCE = REPO_ROOT / (
    "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-09-29/"
    "20260929T233006658540Z_2026-08-05_2026-09-29_"
    "fca8218b214cbe3bf6b4d9d58fbf81cfa29bb9a384afed5f9f468c9958bb8b3e.json"
)
SOURCE_SHA256 = "fca8218b214cbe3bf6b4d9d58fbf81cfa29bb9a384afed5f9f468c9958bb8b3e"
EXPECTED_GAME_PKS = (849843, 849845, 849849, 849851)
EXPECTED_ROUNDS = {
    849843: "NL Wild Card Series", 849845: "NL Wild Card Series",
    849849: "AL Wild Card Series", 849851: "AL Wild Card Series",
}
CLOSE = REPO_ROOT / "artifacts/operational/mlb/season_close/2026/regular_season_close.json"
CLOSE_SHA256 = "fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3"
ROLLBACK = PACKAGE / "activation_rollback.json"
SELECTION_SCHEMA = "MLB_2026_ACTIVE_FILE_PHASE_AUTHORITY_SELECTION_V1"
EXPECTED_ACTIVE_SHA256 = hashlib.sha256(ACTIVE_SELECTION_PATH.read_bytes()).hexdigest()


class ActivationError(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o644)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        dirfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    finally:
        temp.unlink(missing_ok=True)


def _compare_and_swap(path: Path, expected: bytes, replacement: bytes, error_code: str) -> None:
    if not Path(path).is_file() or Path(path).read_bytes() != expected:
        raise ActivationError(error_code)
    _atomic_replace(Path(path), replacement)


def _manifest_bytes() -> bytes:
    names = ("canonical_game_phase_full_snapshot.jsonl", "retained_source_manifest.jsonl",
             "descriptor.json", "validation_report.json")
    return b"".join(
        f"{sha256_file(SNAPSHOT / name)}  {name}\n".encode()
        for name in names
    )


def _verify_child_manifest() -> None:
    if MANIFEST.read_bytes() != _manifest_bytes():
        raise ActivationError("CHILD_SHA256_MANIFEST_MISMATCH")


def validate_child(*, allow_candidate: bool) -> dict[str, Any]:
    _verify_child_manifest()
    if sha256_file(SOURCE) != SOURCE_SHA256:
        raise ActivationError("RETAINED_SCHEDULE_HASH_MISMATCH")
    if sha256_file(CLOSE) != CLOSE_SHA256:
        raise ActivationError("REGULAR_SEASON_CLOSE_HASH_CHANGED")
    data = json.loads(DESCRIPTOR.read_text(encoding="utf-8"))
    descriptor_hash = sha256_file(DESCRIPTOR)
    verified = verify_descriptor_chain(
        DESCRIPTOR, expected_sha256=descriptor_hash,
        root=REPO_ROOT, allow_candidate=allow_candidate,
    )
    if verified.parent is None or verified.parent.sha256 != EXPECTED_V1_DESCRIPTOR_SHA256:
        raise ActivationError("CHILD_PARENT_NOT_PINNED_V1")
    parent_bytes = verified.parent.proposal_path.read_bytes()
    proposal_bytes = verified.proposal_path.read_bytes()
    if not proposal_bytes.startswith(parent_bytes):
        raise ActivationError("V1_PARENT_PROPOSAL_BYTES_CHANGED")
    rows = [json.loads(line) for line in proposal_bytes.splitlines() if line.strip()]
    parent_rows = [json.loads(line) for line in parent_bytes.splitlines() if line.strip()]
    child_rows = rows[len(parent_rows):]
    if tuple(sorted(int(row["game_pk"]) for row in child_rows)) != EXPECTED_GAME_PKS:
        raise ActivationError("CHILD_GAME_PK_SET_MISMATCH")
    if len(rows) != len(parent_rows) + len(EXPECTED_GAME_PKS):
        raise ActivationError("CHILD_POPULATION_CHANGED")
    for row in child_rows:
        game_pk = int(row["game_pk"])
        if (row.get("source_game_type") != "F" or row.get("source_season") != 2026
                or row.get("season_phase") != "POSTSEASON"
                or row.get("postseason_round") != "WILD_CARD"
                or row.get("source_round") != EXPECTED_ROUNDS[game_pk]
                or row.get("game_type_source_sha256") != SOURCE_SHA256
                or row.get("game_type_source_path") != str(SOURCE.relative_to(REPO_ROOT))):
            raise ActivationError(f"CHILD_CLASSIFICATION_MISMATCH:{game_pk}")
    if any(row.get("season_phase") == "REGULAR_SEASON" for row in child_rows):
        raise ActivationError("REGULAR_SEASON_ROW_APPENDED")
    source_rows = [json.loads(line) for line in verified.source_manifest_path.read_text().splitlines() if line.strip()]
    source_row = next((row for row in source_rows if row.get("source_sha256") == SOURCE_SHA256), None)
    if not source_row or source_row.get("source_bytes") != SOURCE.stat().st_size or source_row.get("schedule_game_rows") != 730:
        raise ActivationError("RETAINED_SOURCE_MANIFEST_MISMATCH")
    if not verified.proposal_path.read_bytes().startswith(parent_bytes):
        raise ActivationError("V1_PROPOSAL_PREFIX_MISMATCH")
    return {"descriptor_sha256": descriptor_hash, "rows": len(rows),
            "new_game_pks": list(EXPECTED_GAME_PKS), "parent_rows": len(parent_rows),
            "proposal_sha256": sha256_file(verified.proposal_path),
            "source_manifest_sha256": sha256_file(verified.source_manifest_path),
            "snapshot_status": data["snapshot_status"]}


def promote() -> dict[str, Any]:
    result = validate_child(allow_candidate=True)
    if result["snapshot_status"] != "CANDIDATE":
        raise ActivationError("CHILD_NOT_CANDIDATE")
    target = PACKAGE / "promotion_record.json"
    if target.exists():
        raise ActivationError("PROMOTION_RECORD_ALREADY_EXISTS")
    desc = json.loads(DESCRIPTOR.read_text(encoding="utf-8"))
    desc["snapshot_status"] = "GOVERNED"
    raw = canonical_json_bytes(desc) + b"\n"
    _atomic_replace(DESCRIPTOR, raw)
    _atomic_replace(MANIFEST, _manifest_bytes())
    result = validate_child(allow_candidate=False)
    record = {"contract_name": "MLB_2026_POSTSEASON_AUTHORITY_EXTENSION_PROMOTION_V1",
              "candidate_descriptor_sha256": hashlib.sha256(
                  (canonical_json_bytes({**desc, "snapshot_status": "CANDIDATE"}) + b"\n")).hexdigest(),
              "governed_descriptor_sha256": result["descriptor_sha256"],
              "proposal_sha256": result["proposal_sha256"],
              "source_manifest_sha256": result["source_manifest_sha256"],
              "proposal_and_source_manifest_bytes_unchanged": True}
    target.write_bytes(canonical_json_bytes(record) + b"\n")
    return record


def activate() -> dict[str, Any]:
    child = validate_child(allow_candidate=False)
    prior = ACTIVE_SELECTION_PATH.read_bytes()
    if _sha(prior) != EXPECTED_ACTIVE_SHA256:
        raise ActivationError("ACTIVE_SELECTION_COMPARE_AND_SWAP_MISMATCH")
    prior_obj = json.loads(prior)
    if prior_obj.get("descriptor_sha256") != EXPECTED_V1_DESCRIPTOR_SHA256:
        raise ActivationError("ACTIVE_SELECTION_NOT_EXPECTED_V1")
    selection = {"contract_name": SELECTION_SCHEMA,
                 "descriptor_path": str(DESCRIPTOR.relative_to(REPO_ROOT)),
                 "descriptor_sha256": child["descriptor_sha256"],
                 "schema_version": 1, "selection_status": "GOVERNED_ACTIVE"}
    selected = canonical_json_bytes(selection) + b"\n"
    rollback = {"contract_name": "MLB_2026_PHASE_AUTHORITY_SELECTION_ROLLBACK_V1",
                "prior_selection_base64": base64.b64encode(prior).decode("ascii"),
                "prior_selection_sha256": _sha(prior),
                "child_selection_base64": base64.b64encode(selected).decode("ascii"),
                "child_selection_sha256": _sha(selected),
                "child_descriptor_sha256": child["descriptor_sha256"],
                "rollback_mode": "COMPARE_AND_SWAP_ONLY"}
    fd = os.open(ROLLBACK, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream:
        stream.write(canonical_json_bytes(rollback) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    _compare_and_swap(ACTIVE_SELECTION_PATH, prior, selected,
                      "ACTIVE_SELECTION_CHANGED_BEFORE_REPLACE")
    return {"active_selection_sha256": _sha(selected), "descriptor_sha256": child["descriptor_sha256"],
            "rollback_record": str(ROLLBACK.relative_to(REPO_ROOT)),
            "prior_selection_sha256": _sha(prior)}


def rollback() -> dict[str, str]:
    record = json.loads(ROLLBACK.read_text(encoding="utf-8"))
    current = ACTIVE_SELECTION_PATH.read_bytes()
    child = base64.b64decode(record["child_selection_base64"], validate=True)
    prior = base64.b64decode(record["prior_selection_base64"], validate=True)
    if _sha(child) != record["child_selection_sha256"] or _sha(prior) != record["prior_selection_sha256"]:
        raise ActivationError("ROLLBACK_RECORD_HASH_INVALID")
    _compare_and_swap(ACTIVE_SELECTION_PATH, child, prior,
                      "ROLLBACK_ACTIVE_SELECTION_COMPARE_AND_SWAP_MISMATCH")
    return {"restored_selection_sha256": _sha(prior), "child_selection_sha256": _sha(child)}


def validate_active() -> dict[str, Any]:
    from backend.mlb.public_game_predictions.phase_gating_v1 import classify_moneyline_row
    from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority
    from backend.mlb.season_transition.regular_season_close_inventory_v2 import validate_close_readiness_package

    child = validate_child(allow_candidate=False)
    active = HashedProposalAuthority()
    if active.metadata.snapshot_descriptor_sha256 != child["descriptor_sha256"]:
        raise ActivationError("ACTIVE_DESCRIPTOR_DOES_NOT_MATCH_REVIEWED_CHILD")
    active.require_supported_window("2026-09-29", "2026-09-29")
    decisions = [classify_moneyline_row(
        {"game_id": game_pk, "game_date": "2026-09-29"}, authority=active
    ) for game_pk in EXPECTED_GAME_PKS]
    if any(item.evaluation_partition != "POSTSEASON" for item in decisions):
        raise ActivationError("MONEYLINE_POSTSEASON_COMPATIBILITY_FAILED")
    readiness = validate_close_readiness_package()
    if not readiness["integrity_passed"] or not readiness["close_ready"]:
        raise ActivationError("REGULAR_SEASON_CLOSE_READINESS_CHANGED")
    v1 = load_v1_authority()
    if v1.metadata.phase_counts.get("REGULAR_SEASON") != 2430:
        raise ActivationError("V1_REGULAR_SEASON_POPULATION_CHANGED")
    result = {"status": "VALIDATED", "active_descriptor_sha256": child["descriptor_sha256"],
            "moneyline_postseason_games": len(decisions),
            "regular_season_game_count": 2430,
            "close_artifact_sha256": sha256_file(CLOSE),
            "close_readiness": readiness["decision"]}
    validation_path = PACKAGE / "activation_validation.json"
    validation_path.write_bytes(canonical_json_bytes({
        "contract_name": "MLB_2026_POSTSEASON_AUTHORITY_EXTENSION_VALIDATION_V1",
        **result,
        "source_schedule_sha256": SOURCE_SHA256,
        "new_game_pks": list(EXPECTED_GAME_PKS),
        "parent_rows_preserved_byte_for_byte": child["parent_rows"] == 2919,
        "child_manifest_verified": True,
    }) + b"\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--promote", action="store_true")
    actions.add_argument("--activate", action="store_true")
    actions.add_argument("--rollback", action="store_true")
    actions.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    try:
        result = (promote() if args.promote else activate() if args.activate
                  else rollback() if args.rollback else validate_active())
    except (ActivationError, SnapshotDescriptorError, OSError, ValueError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
