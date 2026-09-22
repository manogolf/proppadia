#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free validation for the authoritative close inventory V1."""

from __future__ import annotations

import argparse
import io
import json
import unittest
from pathlib import Path
from typing import Any

from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    CONTRACT_NAME,
    DEFAULT_PACKAGE_PATH,
    INVENTORY_FILENAME,
    MANIFEST_FILENAME,
    build_authoritative_inventory,
    canonical_json_bytes,
    make_inventory_manifest,
    validate_close_inventory_package,
)


ROOT = Path(__file__).resolve().parents[3]
TEST_MODULE = (
    "backend.mlb.tests."
    "test_mlb_2026_authoritative_regular_season_close_inventory_v1"
)
EXPECTED_TEST_COUNT = 18


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.scenarios: list[dict[str, str]] = []

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.scenarios.append({"scenario": test.id(), "status": "PASSED"})

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.scenarios.append(
            {"scenario": test.id(), "status": "SKIPPED", "reason": reason}
        )

    def _record_failure(self, test: unittest.case.TestCase, err: Any) -> None:
        self.scenarios.append(
            {
                "scenario": test.id(),
                "status": "FAILED",
                "error": self._exc_info_to_string(err, test),
            }
        )

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._record_failure(test, err)

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._record_failure(test, err)


def _check(checks: list[dict[str, Any]], name: str, passed: bool, detail: Any) -> None:
    checks.append(
        {
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "detail": detail,
        }
    )


def run_validation() -> dict[str, Any]:
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromName(TEST_MODULE)
    test_count = suite.countTestCases()
    result: RecordingResult = unittest.TextTestRunner(
        stream=stream,
        verbosity=1,
        resultclass=RecordingResult,
    ).run(suite)  # type: ignore[assignment]
    failed = len(result.failures) + len(result.errors)
    skipped = len(result.skipped)
    passed = result.testsRun - failed - skipped
    tests = {
        "runner": "PYTHON_STANDARD_LIBRARY_UNITTEST",
        "module": TEST_MODULE,
        "intended": EXPECTED_TEST_COUNT,
        "discovered": test_count,
        "executed": result.testsRun,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "unexecuted": max(EXPECTED_TEST_COUNT - result.testsRun, 0),
        "successful": result.wasSuccessful(),
        "scenarios": sorted(result.scenarios, key=lambda row: row["scenario"]),
    }

    rows, summary = build_authoritative_inventory()
    rebuilt_manifest = make_inventory_manifest(rows, summary)
    package_manifest = json.loads(
        (DEFAULT_PACKAGE_PATH / MANIFEST_FILENAME).read_text(encoding="utf-8")
    )
    rebuilt_inventory = b"".join(
        canonical_json_bytes(row) + b"\n" for row in rows
    )
    stored_inventory = (DEFAULT_PACKAGE_PATH / INVENTORY_FILENAME).read_bytes()
    close_report = validate_close_inventory_package()

    command_text = (
        ROOT / "backend/mlb/scripts/prepare_mlb_2026_regular_season_close_v1.py"
    ).read_text(encoding="utf-8")
    makefile_text = (ROOT / "Makefile").read_text(encoding="utf-8")
    checks: list[dict[str, Any]] = []
    _check(checks, "tests_discovered", test_count == EXPECTED_TEST_COUNT, test_count)
    _check(
        checks,
        "tests_all_executed",
        tests["executed"] == EXPECTED_TEST_COUNT and tests["unexecuted"] == 0,
        tests,
    )
    _check(
        checks,
        "tests_all_passed",
        tests["successful"] and failed == 0 and skipped == 0,
        {"passed": passed, "failed": failed, "skipped": skipped},
    )
    _check(checks, "deterministic_inventory_bytes", rebuilt_inventory == stored_inventory, len(stored_inventory))
    _check(checks, "deterministic_manifest", rebuilt_manifest == package_manifest, rebuilt_manifest["manifest_sha256"])
    _check(checks, "authority_population_2919", summary["population_counts"]["total_classified_game_pks"] == 2919, summary["population_counts"])
    _check(checks, "regular_population_2430", len(rows) == 2430, len(rows))
    _check(checks, "preseason_population_489", summary["population_counts"]["preseason_game_pks"] == 489, summary["population_counts"]["preseason_game_pks"])
    _check(
        checks,
        "authority_violation_counts_zero",
        all(summary["population_counts"][field] == 0 for field in ("missing", "unknown", "conflicting", "duplicate_identities")),
        summary["population_counts"],
    )
    _check(
        checks,
        "inventory_integrity",
        close_report["integrity_passed"],
        {
            "rows": close_report["row_count"],
            "omitted": len(close_report["omitted_game_pks"]),
            "extra_or_nonregular": len(
                close_report["extra_or_nonregular_game_pks"]
            ),
            "duplicates": len(close_report["duplicate_game_pks"]),
            "invalid": len(close_report["invalid_rows"]),
        },
    )
    _check(
        checks,
        "premature_close_blocked",
        not close_report["close_ready"]
        and close_report["decision"] == "REGULAR_SEASON_CLOSE_BLOCKED"
        and len(close_report["scheduled_not_final_game_pks"]) == 88,
        {
            "decision": close_report["decision"],
            "scheduled_not_final": len(
                close_report["scheduled_not_final_game_pks"]
            ),
        },
    )
    _check(
        checks,
        "exact_corrected_disposition_counts",
        summary["disposition_counts"]
        == {
            "AUTHORITATIVELY_CANCELLED": 0,
            "FINAL": 2316,
            "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED": 25,
            "SCHEDULED_NOT_FINAL": 88,
            "SUSPENDED_RESUMED_IDENTITY_RESOLVED": 1,
            "UNRESOLVED_IDENTITY_OR_STATUS": 0,
        },
        summary["disposition_counts"],
    )
    temporal_coverage = summary["temporal_coverage"]
    _check(
        checks,
        "exact_348_local_terminal_recoveries",
        len(temporal_coverage["locally_recovered_game_pks"]) == 348,
        len(temporal_coverage["locally_recovered_game_pks"]),
    )
    _check(
        checks,
        "exact_88_temporal_remainder",
        len(temporal_coverage["current_date_game_pks"]) == 16
        and len(temporal_coverage["future_game_pks"]) == 72
        and sorted(
            temporal_coverage["current_date_game_pks"]
            + temporal_coverage["future_game_pks"]
        )
        == summary["scheduled_not_final_game_pks"],
        {
            "current_date": len(temporal_coverage["current_date_game_pks"]),
            "future": len(temporal_coverage["future_game_pks"]),
        },
    )
    prohibited_command_fragments = (
        "--inventory",
        "--execute-close",
        "--authorization-token",
        "--output-dir",
        "_write_package",
        "AUTHORIZATION_TOKEN",
    )
    _check(
        checks,
        "no_arbitrary_inventory_or_close_execution_interface",
        not any(fragment in command_text for fragment in prohibited_command_fragments)
        and "MLB_CLOSE_INVENTORY" not in makefile_text,
        list(prohibited_command_fragments),
    )
    _check(
        checks,
        "check_only_no_close_package",
        close_report["check_only"] and not close_report["close_package_created"],
        {
            "check_only": close_report["check_only"],
            "close_package_created": close_report["close_package_created"],
        },
    )
    passed_validation = all(check["status"] == "PASS" for check in checks)
    return {
        "contract_name": CONTRACT_NAME,
        "validation_passed": passed_validation,
        "check_count": len(checks),
        "failed_checks": [
            check["check"] for check in checks if check["status"] == "FAIL"
        ],
        "checks": checks,
        "tests": tests,
        "population_counts": summary["population_counts"],
        "disposition_counts": summary["disposition_counts"],
        "scheduled_not_final_count": len(summary["scheduled_not_final_game_pks"]),
        "unresolved_count": len(summary["unresolved_game_pks"]),
        "close_readiness": close_report["decision"],
        "manifest_sha256": close_report["manifest_sha256"],
        "network_requests": 0,
        "database_connections": 0,
        "regular_season_close_executions": 0,
        "close_packages_created": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = run_validation()
    if args.write_report:
        (DEFAULT_PACKAGE_PATH / "validation_report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["validation_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
