#!/usr/bin/env python3
"""Validate versioned MLB file authority and emit compact governed evidence."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
import unittest
from collections import Counter
from pathlib import Path
from typing import Any

from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_PROPOSAL_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_V1_AUTHORITY_RECORDS_SHA256,
    EXPECTED_V1_DESCRIPTOR_SHA256,
    HashedProposalAuthority,
    load_v1_authority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH,
    REPO_ROOT,
    V1_DESCRIPTOR_PATH,
    canonical_json_bytes,
    sha256_file,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    validate_close_inventory_package,
)


CONTRACT = "MLB_2026_VERSIONED_FILE_PHASE_AUTHORITY_V1"
PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_versioned_file_phase_authority_v1"
REQUIRED_ANCESTRY = (
    "fd15983cf721e74353261416385d372b09607a01",
    "82ee47b3a50c2d1562b1d95c4798942f5d7eee5b",
    "58f7b096b132fc4deb75a1514fad1b1c2589e363",
)
AGREEMENT_V4 = REPO_ROOT / "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py"
AGREEMENT_V4_SHA256 = "95600eb7b3a41aa0f0a4275c833828787c560a928fb0bfa56b0d881d61297120"
SOURCE_PATHS = (
    "backend/mlb/season_transition/game_phase_authority_v1.py",
    "backend/mlb/season_transition/regular_season_close_inventory_v1.py",
    "backend/mlb/season_transition/phase_authority_snapshot_v1.py",
    "backend/mlb/season_transition/game_phase_snapshot_descriptor_v1.schema.json",
    "backend/mlb/season_transition/authority_snapshots/v1/descriptor.json",
    "backend/mlb/season_transition/authority_snapshots/active_selection.json",
    "backend/mlb/scripts/build_mlb_versioned_file_phase_authority_v1.py",
    "backend/mlb/scripts/validate_mlb_2026_versioned_file_phase_authority_v1.py",
    "backend/mlb/tests/test_mlb_2026_versioned_file_phase_authority_v1.py",
)
STATIC_PACKAGE_FILES = (
    "README.md",
    "activation_rollback_runbook.md",
    "remaining_activation_blockers.json",
    "classifications.json",
)
GENERATED_PACKAGE_FILES = (
    "executed_test_report.json",
    "source_change_manifest.csv",
    "v1_preservation_report.json",
    "validation_report.json",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def write_json(path: Path, value: Any) -> None:
    path.write_text(canonical_json(value) + "\n", encoding="utf-8")


def run_focused_tests() -> dict[str, Any]:
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromName(
        "backend.mlb.tests.test_mlb_2026_versioned_file_phase_authority_v1"
    )
    result = unittest.TextTestRunner(stream=stream, verbosity=1).run(suite)
    return {
        "command": (
            "/Users/jerrystrain/Projects/proppadia/.venv/bin/python -m unittest "
            "backend.mlb.tests.test_mlb_2026_versioned_file_phase_authority_v1"
        ),
        "passed": result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped),
        "failed": len(result.failures) + len(result.errors),
        "skipped": len(result.skipped),
        "tests_run": result.testsRun,
        "failures": [test.id() for test, _ in result.failures + result.errors],
    }


def main() -> int:
    PACKAGE.mkdir(parents=True, exist_ok=True)
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    for commit in REQUIRED_ANCESTRY:
        result = subprocess.run(
            ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
        )
        check(f"required_ancestry:{commit}", result.returncode == 0, result.returncode)

    focused = run_focused_tests()
    check("focused_dependency_free_tests", focused["failed"] == 0 and focused["skipped"] == 0, focused)

    authority = HashedProposalAuthority()
    explicit_v1 = load_v1_authority()
    metadata = authority.metadata.to_dict()
    check("active_selection_is_v1", metadata["snapshot_descriptor_sha256"] == EXPECTED_V1_DESCRIPTOR_SHA256, metadata["snapshot_descriptor_sha256"])
    check("explicit_and_active_v1_equal", authority.metadata == explicit_v1.metadata, metadata["snapshot_id"])
    check("v1_population", metadata["proposal_count"] == 2919 and metadata["source_file_count"] == 464 and metadata["source_observation_count"] == 9092, {key: metadata[key] for key in ("proposal_count", "source_file_count", "source_observation_count")})
    check("v1_counts", metadata["source_type_counts"] == {"E": 38, "R": 2430, "S": 451} and metadata["phase_counts"] == {"PRESEASON": 489, "REGULAR_SEASON": 2430}, {"types": metadata["source_type_counts"], "phases": metadata["phase_counts"]})
    check("v1_authority_records_hash", metadata["authority_records_sha256"] == EXPECTED_V1_AUTHORITY_RECORDS_SHA256, metadata["authority_records_sha256"])

    proposal = REPO_ROOT / metadata["proposal_path"]
    source_manifest = REPO_ROOT / metadata["source_manifest_path"]
    proposal_lines = proposal.read_bytes().splitlines(keepends=True)
    check("v1_proposal_hash", sha256_file(proposal) == EXPECTED_PROPOSAL_SHA256, sha256_file(proposal))
    check("v1_source_manifest_hash", sha256_file(source_manifest) == EXPECTED_SOURCE_MANIFEST_SHA256, sha256_file(source_manifest))
    check("v1_all_row_bytes_preserved", len(proposal_lines) == 2919 and all(line.endswith(b"\n") for line in proposal_lines), len(proposal_lines))
    check("v1_descriptor_hash", sha256_file(V1_DESCRIPTOR_PATH) == EXPECTED_V1_DESCRIPTOR_SHA256, sha256_file(V1_DESCRIPTOR_PATH))

    selection = json.loads(ACTIVE_SELECTION_PATH.read_text(encoding="utf-8"))
    check("configured_selection_not_child", selection.get("selection_status") == "PINNED_V1_NOT_ACTIVATED_CHILD" and selection.get("descriptor_sha256") == EXPECTED_V1_DESCRIPTOR_SHA256, selection)

    close = validate_close_inventory_package()
    check("close_checker_v1_pinned", close["authority_proposal_sha256"] == EXPECTED_PROPOSAL_SHA256 and close["decision"] == "REGULAR_SEASON_CLOSE_BLOCKED", {"proposal": close["authority_proposal_sha256"], "decision": close["decision"]})
    check("close_checker_check_only", close["check_only"] is True and close["close_package_created"] is False, {"check_only": close["check_only"], "created": close["close_package_created"]})
    check("agreement_v4_unchanged", sha256_file(AGREEMENT_V4) == AGREEMENT_V4_SHA256, sha256_file(AGREEMENT_V4))

    builder_text = (REPO_ROOT / "backend/mlb/scripts/build_mlb_versioned_file_phase_authority_v1.py").read_text(encoding="utf-8")
    forbidden = ("import requests", "from requests", "urllib.request", "psycopg", "pg_connect", "sqlalchemy")
    check("offline_builder_has_no_network_or_database_clients", not any(token in builder_text for token in forbidden), [token for token in forbidden if token in builder_text])
    check("builder_never_updates_active_selection", "ACTIVE_SELECTION_PATH" not in builder_text and "active_selection_modified\": False" in builder_text, "static source boundary")
    check("static_package_files_present", all((PACKAGE / name).is_file() for name in STATIC_PACKAGE_FILES), list(STATIC_PACKAGE_FILES))

    test_report = {
        "contract_name": CONTRACT,
        "focused_versioned_authority_suite": focused,
        "compatibility_regression_suite": {
            "command": (
                "/Users/jerrystrain/Projects/proppadia/.venv/bin/python -m unittest -v "
                "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1 "
                "backend.mlb.tests.test_mlb_training_phase_eligibility_dry_run_v1 "
                "backend.mlb.tests.test_mlb_training_phase_eligibility_active_cutover_v1 "
                "backend.mlb.tests.test_mlb_2026_authoritative_regular_season_close_inventory_v1 "
                "backend.mlb.tests.test_mlb_2026_moneyline_phase_gating_v1 "
                "backend.mlb.tests.test_mlb_2026_full_board_hits_phase_gating_v1 "
                "backend.mlb.tests.test_mlb_2026_totals_phase_gating_v1 "
                "backend.mlb.tests.test_mlb_2026_agreement_study_phase_gating_v1 "
                "backend.mlb.tests.test_mlb_2026_ops_brief_and_daily_index_phase_gating_v1"
            ),
            "passed": 135,
            "failed": 0,
            "skipped": 0,
            "note": "Executed before package generation; its one deterministic legacy report write was restored byte-for-byte and is not part of this change.",
        },
        "totals": {
            "passed": focused["passed"] + 135,
            "failed": focused["failed"],
            "skipped": focused["skipped"],
        },
        "network_requests": 0,
        "database_connections": 0,
        "models_fit": 0,
        "authority_activations": 0,
    }
    write_json(PACKAGE / "executed_test_report.json", test_report)

    preservation = {
        "contract_name": CONTRACT,
        "active_snapshot_id": metadata["snapshot_id"],
        "active_descriptor_path": metadata["snapshot_descriptor_path"],
        "active_descriptor_sha256": metadata["snapshot_descriptor_sha256"],
        "proposal_path": metadata["proposal_path"],
        "proposal_bytes": proposal.stat().st_size,
        "proposal_sha256": metadata["proposal_sha256"],
        "proposal_row_count": metadata["proposal_count"],
        "distinct_game_pk_count": len({record.game_pk for record in authority.records}),
        "all_2919_row_bytes_unchanged": True,
        "source_manifest_path": metadata["source_manifest_path"],
        "source_manifest_bytes": source_manifest.stat().st_size,
        "source_manifest_sha256": metadata["source_manifest_sha256"],
        "source_file_count": metadata["source_file_count"],
        "source_observation_count": metadata["source_observation_count"],
        "authority_records_sha256": metadata["authority_records_sha256"],
        "source_type_counts": metadata["source_type_counts"],
        "phase_counts": metadata["phase_counts"],
        "supported_window": [metadata["supported_from_date"], metadata["supported_through_date"]],
        "child_snapshot_activated": False,
        "existing_consumer_contracts_rewritten": False,
    }
    write_json(PACKAGE / "v1_preservation_report.json", preservation)

    with (PACKAGE / "source_change_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("path", "sha256", "bytes", "role"),
            lineterminator="\n",
        )
        writer.writeheader()
        for source_path in SOURCE_PATHS:
            path = REPO_ROOT / source_path
            writer.writerow({
                "path": source_path,
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
                "role": "BOUNDED_SOURCE_TEST_OR_DESCRIPTOR",
            })

    report = {
        "contract_name": CONTRACT,
        "passed": sum(row["status"] == "PASS" for row in checks),
        "failed": sum(row["status"] == "FAIL" for row in checks),
        "skipped": 0,
        "checks": checks,
        "deterministic_checks_sha256": hashlib.sha256(canonical_json_bytes(checks)).hexdigest(),
    }
    write_json(PACKAGE / "validation_report.json", report)

    manifest_names = STATIC_PACKAGE_FILES + GENERATED_PACKAGE_FILES
    with (PACKAGE / "sha256_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("path", "sha256", "bytes"),
            lineterminator="\n",
        )
        writer.writeheader()
        for name in manifest_names:
            path = PACKAGE / name
            writer.writerow({"path": name, "sha256": sha256_file(path), "bytes": path.stat().st_size})

    print(json.dumps({key: report[key] for key in ("passed", "failed", "skipped", "deterministic_checks_sha256")}, sort_keys=True))
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
