#!/usr/bin/env python3
"""Validate the executed MLB canonical-phase coverage/test gate offline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.mlb.scripts.run_mlb_canonical_phase_stdlib_tests_v1 import execute_target


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_COVERAGE_AND_TEST_GATE_V1"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PACKAGE = ROOT / "docs/contracts/mlb_2026_canonical_phase_coverage_and_test_gate_v1"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(package: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    stored_tests = read_json(package / "executed_dependency_free_test_report.json")
    executed_tests = execute_target()
    check("actual_assertions_reexecuted", executed_tests == stored_tests, executed_tests["gate_status"])
    check(
        "test_counts_exact",
        (
            stored_tests["intended_scenarios"] == 25
            and stored_tests["executed_scenarios"] == 25
            and stored_tests["passed"] == 25
            and stored_tests["failed"] == 0
            and stored_tests["skipped"] == 0
            and stored_tests["unexecuted"] == 0
        ),
        {
            key: stored_tests[key]
            for key in ("intended_scenarios", "executed_scenarios", "passed", "failed", "skipped", "unexecuted")
        },
    )
    check(
        "validator_reconciliation",
        stored_tests["existing_validator_check_count"] == 13
        and stored_tests["existing_validator_status"] == "PASS"
        and not stored_tests["missing_validator_mappings"]
        and not stored_tests["missing_test_functions"],
        "13 checks fully mapped",
    )

    population_dir = package / "population_reconciliation"
    reconciliation = read_json(population_dir / "canonical_population_reconciliation.json")
    summary = read_json(population_dir / "coverage_gate_summary.json")
    missing = read_jsonl(population_dir / "missing_game_pk_ledger.jsonl")
    conflicts = read_jsonl(population_dir / "conflict_ledger.jsonl")
    source_manifest = read_jsonl(population_dir / "authoritative_schedule_source_manifest.jsonl")
    universe = reconciliation["canonical_universe"]
    authoritative = reconciliation["authoritative_schedule_summary"]
    check(
        "canonical_population_counts",
        universe["distinct_game_pk_count"] == 2901
        and universe["authoritatively_classified_count"] == 2430
        and universe["missing_authoritative_type_count"] == 471,
        universe,
    )
    check(
        "authoritative_schedule_counts",
        authoritative["source_file_count"] == 463
        and authoritative["observation_count"] == 8602
        and authoritative["distinct_game_pk_count"] == 2430
        and authoritative["source_game_type_counts"] == {"R": 2430}
        and authoritative["season_phase_counts"] == {"REGULAR_SEASON": 2430}
        and authoritative["special_event_count"] == 0,
        authoritative,
    )
    missing_ids = [int(row["game_pk"]) for row in missing]
    check(
        "missing_ledger_exact",
        len(missing_ids) == 471
        and len(set(missing_ids)) == 471
        and missing_ids == universe["missing_authoritative_type_game_pks"],
        {"rows": len(missing_ids), "unique": len(set(missing_ids))},
    )
    check("conflict_ledger_empty", conflicts == [], {"rows": len(conflicts)})
    check(
        "proposal_is_subset",
        reconciliation["offline_proposal_is_retained_file_subset"] is True
        and next(
            row for row in reconciliation["populations"]
            if row["source"] == "offline_phase_proposal_97"
        )["distinct_game_pk_count"] == 97,
        "97 < 2430",
    )

    source_hash_failures = []
    for row in source_manifest:
        path = ROOT / row["source_path"]
        if not path.is_file() or sha256(path) != row["source_sha256"]:
            source_hash_failures.append(row["source_path"])
    check(
        "retained_schedule_source_hashes",
        len(source_manifest) == 463 and not source_hash_failures,
        {"manifest_rows": len(source_manifest), "failures": source_hash_failures},
    )

    database_snapshot = package / "database_population_snapshot.json"
    check(
        "database_snapshot_identity",
        reconciliation["database_snapshot_sha256"] == sha256(database_snapshot)
        and read_json(database_snapshot)["database_transaction"] == "READ_ONLY",
        reconciliation["database_snapshot_sha256"],
    )
    acquisition = read_json(population_dir / "source_completion_acquisition_proposal.json")
    check(
        "source_completion_is_unexecuted_and_free",
        acquisition["status"] == "PROPOSED_NOT_EXECUTED"
        and acquisition["execution_authorized"] is False
        and acquisition["expected_request_count"] == 1
        and acquisition["expected_paid_credit_count"] == 0
        and acquisition["expected_target_game_pk_count"] == 471,
        acquisition,
    )
    recommendation = read_json(population_dir / "activation_recommendation.json")
    check(
        "activation_gate_blocked",
        recommendation["recommendation"] == "CANONICAL_PHASE_ACTIVATION_GATE_BLOCKED"
        and summary["gate_status"] == "BLOCKED"
        and universe["fully_classified"] is False,
        recommendation,
    )

    failed = [row["check"] for row in checks if row["status"] != "PASS"]
    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_checks": failed,
        "database_writes": 0,
        "network_requests": 0,
        "checks": checks,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, default=DEFAULT_PACKAGE)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate(args.package.resolve())
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
