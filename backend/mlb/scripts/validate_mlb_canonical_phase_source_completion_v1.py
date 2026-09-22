#!/usr/bin/env python3
"""Validate the MLB canonical-phase source completion entirely offline."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

from backend.mlb.scripts.run_mlb_canonical_phase_stdlib_tests_v1 import execute_target
from backend.mlb.scripts.validate_mlb_canonical_phase_coverage_and_test_gate_v1 import (
    validate as validate_coverage,
)
from backend.mlb.season_transition.canonical_phase_v1 import (
    CanonicalGamePhaseIndex,
    canonical_phase_record,
)


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_SOURCE_COMPLETION_V1"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PACKAGE = ROOT / "docs/contracts/mlb_2026_canonical_phase_source_completion_v1"
PRIOR_GATE = ROOT / "docs/contracts/mlb_2026_canonical_phase_coverage_and_test_gate_v1"
EXPECTED_URL = (
    "https://statsapi.mlb.com/api/v1/schedule?"
    "sportId=1&startDate=2026-02-20&endDate=2026-03-25"
)
EXPECTED_RAW_PATH = Path(
    "backend/mlb/data/external/statsapi/raw/2026/"
    "schedule_2026-02-20_2026-03-25.json"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _manifest_failures(package: Path) -> list[str]:
    failures: list[str] = []
    manifest = package / "sha256_manifest.txt"
    for line in manifest.read_text().splitlines():
        expected, relative = line.split("  ", 1)
        path = (package / relative).resolve()
        if not path.is_file() or sha256(path) != expected:
            failures.append(relative)
    return failures


def validate(package: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    receipt = read_json(package / "acquisition_receipt.json")
    check(
        "single_free_request_receipt",
        receipt["contract_name"] == CONTRACT_NAME
        and receipt["status"] == "SUCCESS"
        and receipt["request_count"] == 1
        and receipt["retry_count"] == 0
        and receipt["paid_provider_request_count"] == 0
        and receipt["paid_credit_count"] == 0
        and receipt["http_status"] == 200
        and receipt["request_url"] == EXPECTED_URL
        and receipt["response_url"] == EXPECTED_URL
        and receipt["raw_response_path"] == str(EXPECTED_RAW_PATH)
        and receipt["atomic_write"] is True
        and receipt["overwrite_performed"] is False,
        receipt,
    )

    raw_path = ROOT / EXPECTED_RAW_PATH
    raw = raw_path.read_bytes()
    check(
        "immutable_raw_response_identity",
        len(raw) == receipt["raw_response_byte_count"] == 642447
        and sha256(raw_path) == receipt["raw_response_sha256"]
        == "3ca8bff9e4361676ad3b34c5166f23f4d180d809083557ad8498062a9febd091",
        {"path": str(EXPECTED_RAW_PATH), "bytes": len(raw), "sha256": sha256(raw_path)},
    )

    proposal_path = ROOT / receipt["proposal_path"]
    proposal = read_json(proposal_path)
    check(
        "authorized_proposal_exact",
        sha256(proposal_path) == receipt["proposal_sha256"]
        and proposal["expected_request_count"] == 1
        and proposal["expected_paid_credit_count"] == 0
        and proposal["parameters"]
        == {"sportId": 1, "startDate": "2026-02-20", "endDate": "2026-03-25"}
        and proposal["storage_path"] == str(EXPECTED_RAW_PATH),
        {"proposal_sha256": sha256(proposal_path), "parameters": proposal["parameters"]},
    )

    payload = json.loads(raw)
    games = [game for block in payload["dates"] for game in block.get("games", [])]
    index = CanonicalGamePhaseIndex()
    records: dict[int, dict[str, Any]] = {}
    for game in games:
        record = canonical_phase_record(
            game,
            source_sha256=receipt["raw_response_sha256"],
            source_path=str(EXPECTED_RAW_PATH),
        )
        index.add(record)
        records[int(record["game_pk"])] = record
    response_ids = set(records)
    type_counts = Counter(str(row["source_game_type"]) for row in records.values())
    phase_counts = Counter(str(row["season_phase"]) for row in records.values())
    check(
        "authoritative_response_contract_classification",
        len(games) == 490
        and len(records) == 490
        and index.consistent_duplicate_count == 0
        and type_counts == {"E": 38, "R": 1, "S": 451}
        and phase_counts == {"PRESEASON": 489, "REGULAR_SEASON": 1}
        and {int(row["source_season"]) for row in records.values()} == {2026},
        {"observations": len(games), "distinct": len(records), "types": type_counts, "phases": phase_counts},
    )

    prior_missing = {
        int(row["game_pk"])
        for row in read_jsonl(PRIOR_GATE / "population_reconciliation/missing_game_pk_ledger.jsonl")
    }
    resolved = prior_missing & response_ids
    outside = sorted(response_ids - prior_missing)
    outside_ledger = read_jsonl(package / "returned_outside_prior_missing_ledger.jsonl")
    check(
        "prior_471_fully_resolved",
        len(prior_missing) == len(resolved) == 471 and not (prior_missing - response_ids),
        {"prior_missing": len(prior_missing), "resolved": len(resolved), "unresolved": len(prior_missing - response_ids)},
    )
    check(
        "outside_prior_missing_ledger_exact",
        len(outside) == 19
        and [int(row["game_pk"]) for row in outside_ledger] == outside
        and sum(records[game_pk]["season_phase"] == "PRESEASON" for game_pk in outside) == 18
        and sum(records[game_pk]["season_phase"] == "REGULAR_SEASON" for game_pk in outside) == 1,
        {"count": len(outside), "game_pks": outside},
    )

    reconciliation = read_json(package / "source_completion_reconciliation.json")
    check(
        "source_completion_reconciliation_ready",
        reconciliation["status"] == "READY"
        and reconciliation["recommendation"] == "CANONICAL_PHASE_SOURCE_COMPLETION_READY"
        and reconciliation["resolved_prior_missing_game_pk_count"] == 471
        and reconciliation["unresolved_prior_missing_game_pk_count"] == 0
        and all(value == 0 for value in reconciliation["violations"].values()),
        reconciliation["violations"],
    )

    proposal_dir = package / "canonical_backfill_proposal"
    backfill_rows = read_jsonl(proposal_dir / "canonical_game_phase_backfill_proposal.jsonl")
    backfill_types = Counter(str(row["source_game_type"]) for row in backfill_rows)
    backfill_phases = Counter(str(row["season_phase"]) for row in backfill_rows)
    check(
        "canonical_backfill_proposal_complete",
        len(backfill_rows) == len({int(row["game_pk"]) for row in backfill_rows}) == 2919
        and backfill_types == {"E": 38, "R": 2430, "S": 451}
        and backfill_phases == {"PRESEASON": 489, "REGULAR_SEASON": 2430},
        {"rows": len(backfill_rows), "types": backfill_types, "phases": backfill_phases},
    )
    inner_manifest_failures = []
    for line in (proposal_dir / "sha256_manifest.txt").read_text().splitlines():
        expected, relative = line.split("  ", 1)
        if sha256(proposal_dir / relative) != expected:
            inner_manifest_failures.append(relative)
    check("backfill_inner_manifest", not inner_manifest_failures, inner_manifest_failures)

    coverage = validate_coverage(package)
    check(
        "coverage_gate_validation",
        coverage["status"] == "PASS"
        and coverage["check_count"] == 12
        and not coverage["failed_checks"],
        {"status": coverage["status"], "checks": coverage["check_count"], "failed": coverage["failed_checks"]},
    )

    stored_tests = read_json(package / "executed_dependency_free_test_report.json")
    executed_tests = execute_target()
    check(
        "dependency_free_assertions_reexecuted",
        stored_tests == executed_tests
        and executed_tests["passed"] == 25
        and executed_tests["failed"] == 0
        and executed_tests["skipped"] == 0
        and executed_tests["unexecuted"] == 0
        and executed_tests["existing_validator_check_count"] == 13
        and executed_tests["existing_validator_status"] == "PASS",
        {key: executed_tests[key] for key in ("passed", "failed", "skipped", "unexecuted", "gate_status")},
    )

    population_dir = package / "population_reconciliation"
    missing = read_jsonl(population_dir / "missing_game_pk_ledger.jsonl")
    conflicts = read_jsonl(population_dir / "conflict_ledger.jsonl")
    population = read_json(population_dir / "canonical_population_reconciliation.json")
    check(
        "zero_remaining_classification_violations",
        not missing
        and not conflicts
        and population["canonical_universe"]
        == {
            "authoritatively_classified_count": 2919,
            "distinct_game_pk_count": 2919,
            "fully_classified": True,
            "missing_authoritative_type_count": 0,
            "missing_authoritative_type_game_pks": [],
        },
        {"missing": len(missing), "conflicts": len(conflicts), "universe": population["canonical_universe"]},
    )

    acquisition_source = ROOT / "backend/mlb/scripts/acquire_mlb_canonical_phase_source_completion_v1.py"
    tree = ast.parse(acquisition_source.read_text())
    opener_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "opener"
        and node.func.attr == "open"
    ]
    check(
        "one_shot_no_retry_implementation",
        len(opener_calls) == 1 and receipt["request_count"] == 1 and receipt["retry_count"] == 0,
        {"opener_call_sites": len(opener_calls), "request_count": receipt["request_count"], "retry_count": receipt["retry_count"]},
    )

    manifest_failures = _manifest_failures(package)
    check("package_sha256_manifest", not manifest_failures, manifest_failures)

    failed = [row["check"] for row in checks if row["status"] != "PASS"]
    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "passed_check_count": len(checks) - len(failed),
        "failed_checks": failed,
        "network_requests_during_validation": 0,
        "database_writes": 0,
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
