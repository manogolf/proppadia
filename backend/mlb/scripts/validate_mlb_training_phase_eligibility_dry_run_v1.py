#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free validator for the training phase eligibility dry run."""

from __future__ import annotations

import argparse
import csv
import io
import json
import unittest
from collections import Counter
from pathlib import Path
from typing import Any

from backend.mlb.scripts.build_mlb_training_phase_eligibility_dry_run_v1 import (
    EVIDENCE_DIR,
    EXPECTED_COUNTS,
)
from backend.mlb.season_transition.training_phase_eligibility_v1 import (
    BLOCKED_ABSENT_AUTHORITY,
    BLOCKED_AUTHORITY_HASH_MISMATCH,
    BLOCKED_CONFLICTING_TYPE,
    BLOCKED_DUPLICATE_AUTHORITY,
    BLOCKED_MISSING_GAME_PK,
    BLOCKED_SPECIAL_TYPE,
    BLOCKED_STALE_AUTHORITY,
    BLOCKED_UNKNOWN_TYPE,
    EXCLUDED_POSTSEASON,
    EXCLUDED_PRESEASON,
)


CONTRACT_NAME = "MLB_2026_TRAINING_PHASE_ELIGIBILITY_DRY_RUN_V1"
TEST_MODULE = "backend.mlb.tests.test_mlb_training_phase_eligibility_dry_run_v1"
ROOT = Path(__file__).resolve().parents[3]


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.scenarios: list[dict[str, str]] = []

    @staticmethod
    def _name(test: unittest.case.TestCase) -> str:
        return test.id().rsplit(".", 1)[-1]

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.scenarios.append({"scenario": self._name(test), "status": "PASSED"})

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.scenarios.append(
            {"scenario": self._name(test), "status": "SKIPPED", "reason": reason}
        )

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self.scenarios.append(
            {"scenario": self._name(test), "status": "FAILED", "error": self._exc_info_to_string(err, test)}
        )

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addError(test, err)
        self.scenarios.append(
            {"scenario": self._name(test), "status": "FAILED", "error": self._exc_info_to_string(err, test)}
        )


def execute_tests() -> dict[str, Any]:
    suite = unittest.defaultTestLoader.loadTestsFromName(TEST_MODULE)
    stream = io.StringIO()
    runner = unittest.TextTestRunner(
        stream=stream,
        verbosity=1,
        resultclass=RecordingResult,
    )
    result = runner.run(suite)
    passed = sum(row["status"] == "PASSED" for row in result.scenarios)
    failed = sum(row["status"] == "FAILED" for row in result.scenarios)
    skipped = sum(row["status"] == "SKIPPED" for row in result.scenarios)
    intended = 15
    return {
        "runner": "PYTHON_STDLIB_UNITTEST_ACTUAL_ASSERTION_EXECUTION",
        "module": TEST_MODULE,
        "intended": intended,
        "executed": int(result.testsRun),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "unexecuted": intended - int(result.testsRun),
        "successful": bool(result.wasSuccessful() and result.testsRun == intended),
        "scenarios": sorted(result.scenarios, key=lambda row: row["scenario"]),
    }


def validate() -> dict[str, Any]:
    tests = execute_tests()
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append(
            {"check": name, "status": "PASS" if passed else "FAIL", "detail": detail}
        )

    check("dependency_free_tests", tests["successful"], tests)
    report_path = EVIDENCE_DIR / "dry_run_report.json"
    ledger_path = EVIDENCE_DIR / "excluded_game_pks.csv"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    gate = report["gate_report"]
    check(
        "exact_frozen_counts",
        gate["input_row_count"] == EXPECTED_COUNTS["input_rows"]
        and gate["input_distinct_game_pk_count"] == EXPECTED_COUNTS["input_game_pks"]
        and gate["admitted_row_count"] == EXPECTED_COUNTS["admitted_rows"]
        and gate["admitted_distinct_game_pk_count"] == EXPECTED_COUNTS["admitted_game_pks"]
        and gate["decision_row_counts"][EXCLUDED_PRESEASON]
        == EXPECTED_COUNTS["excluded_preseason_rows"]
        and len(gate["decision_game_pks"][EXCLUDED_PRESEASON])
        == EXPECTED_COUNTS["excluded_preseason_game_pks"],
        EXPECTED_COUNTS,
    )
    blocked_codes = (
        BLOCKED_MISSING_GAME_PK,
        BLOCKED_ABSENT_AUTHORITY,
        BLOCKED_SPECIAL_TYPE,
        BLOCKED_UNKNOWN_TYPE,
        BLOCKED_CONFLICTING_TYPE,
        BLOCKED_DUPLICATE_AUTHORITY,
        BLOCKED_AUTHORITY_HASH_MISMATCH,
        BLOCKED_STALE_AUTHORITY,
    )
    check(
        "zero_unresolved_authority",
        all(gate["decision_row_counts"][code] == 0 for code in blocked_codes)
        and gate["decision_row_counts"][EXCLUDED_POSTSEASON] == 0,
        {code: gate["decision_row_counts"][code] for code in blocked_codes},
    )
    check(
        "membership_only_hash_invariance",
        report["invariance"]["all_retained_hashes_identical"]
        and all(
            item["before_sha256"] == item["after_sha256"]
            for item in report["invariance"]["hashes"].values()
        ),
        report["invariance"]["hashes"],
    )
    check(
        "stable_order_and_no_date_default",
        gate["stable_input_order_preserved"]
        and gate["calendar_inference"] is False
        and gate["missing_type_default"] is False,
        {
            "stable_input_order_preserved": gate["stable_input_order_preserved"],
            "calendar_inference": gate["calendar_inference"],
            "missing_type_default": gate["missing_type_default"],
        },
    )
    safety = report["safety"]
    zero_safety_keys = (
        "database_connections",
        "network_requests",
        "paid_requests",
        "model_fit_calls",
        "model_train_calls",
        "model_score_calls",
        "model_artifact_writes",
        "prediction_writes",
        "dataset_writes",
        "operational_artifact_writes",
    )
    check(
        "zero_fit_training_and_operational_writes",
        all(safety[key] == 0 for key in zero_safety_keys),
        {key: safety[key] for key in zero_safety_keys},
    )
    with ledger_path.open(newline="", encoding="utf-8") as handle:
        exclusions = list(csv.DictReader(handle))
    type_counts = Counter(row["authoritative_game_type"] for row in exclusions)
    check(
        "exact_exclusion_ledger",
        len(exclusions) == 471
        and len({int(row["game_pk"]) for row in exclusions}) == 471
        and sum(int(row["row_count"]) for row in exclusions) == 141_162
        and {row["authoritative_phase"] for row in exclusions} == {"PRESEASON"}
        and type_counts == Counter({"S": 440, "E": 31}),
        {"rows": len(exclusions), "row_sum": sum(int(row["row_count"]) for row in exclusions), "type_counts": dict(type_counts)},
    )
    check(
        "ordinary_selector_not_activated",
        report["ordinary_trainer_selector_activated"] is False
        and report["active_selector_cutover_authorized"] is False,
        {
            "ordinary_trainer_selector_activated": report["ordinary_trainer_selector_activated"],
            "active_selector_cutover_authorized": report["active_selector_cutover_authorized"],
        },
    )
    helper_text = (
        ROOT / "backend/mlb/season_transition/training_phase_eligibility_v1.py"
    ).read_text(encoding="utf-8")
    check(
        "shared_helper_reuses_authority_interface",
        "CanonicalGamePhaseAuthority" in helper_text
        and "normalize_source_game_type" not in helper_text
        and "game_date" not in helper_text
        and "datetime" not in helper_text,
        "no independent type map or calendar classification",
    )
    failed = [row for row in checks if row["status"] != "PASS"]
    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if not failed else "FAIL",
        "checks_passed": len(checks) - len(failed),
        "checks_failed": len(failed),
        "tests": tests,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=EVIDENCE_DIR / "test_report.json")
    args = parser.parse_args()
    if args.output.resolve().parent != EVIDENCE_DIR.resolve():
        raise RuntimeError("VALIDATION_EVIDENCE_DESTINATION_NOT_GOVERNED")
    report = validate()
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
