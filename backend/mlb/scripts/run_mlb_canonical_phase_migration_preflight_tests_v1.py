#!/usr/bin/env python3
"""Execute migration-preflight unittest assertions and emit a JSON report."""

from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1"
TARGET_MODULE = "backend.mlb.tests.test_mlb_canonical_phase_migration_activation_preflight_v1"


class RecordingResult(unittest.TestResult):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[dict[str, str]] = []

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.results.append({"scenario": test.id(), "status": "PASSED"})

    def addFailure(self, test: unittest.case.TestCase, err: tuple[type[BaseException], BaseException, object]) -> None:
        super().addFailure(test, err)
        self.results.append(
            {"scenario": test.id(), "status": "FAILED", "error": self._exc_info_to_string(err, test)}
        )

    def addError(self, test: unittest.case.TestCase, err: tuple[type[BaseException], BaseException, object]) -> None:
        super().addError(test, err)
        self.results.append(
            {"scenario": test.id(), "status": "FAILED", "error": self._exc_info_to_string(err, test)}
        )

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.results.append({"scenario": test.id(), "status": "SKIPPED", "reason": reason})


def execute() -> dict[str, object]:
    suite = unittest.defaultTestLoader.loadTestsFromName(TARGET_MODULE)
    intended = suite.countTestCases()
    result = RecordingResult()
    suite.run(result)
    failed = len(result.failures) + len(result.errors)
    skipped = len(result.skipped)
    passed = result.testsRun - failed - skipped
    return {
        "contract_name": CONTRACT_NAME,
        "target_module": TARGET_MODULE,
        "runner": "PYTHON_STANDARD_LIBRARY_UNITTEST_ACTUAL_ASSERTIONS",
        "intended_scenarios": intended,
        "executed_scenarios": result.testsRun,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "unexecuted": intended - result.testsRun,
        "gate_status": (
            "PASS"
            if result.testsRun == intended and failed == 0 and skipped == 0
            else "FAIL"
        ),
        "results": result.results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = execute()
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["gate_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
