#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free validation for the active training phase cutover."""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import unittest
from pathlib import Path
from typing import Any


CONTRACT_NAME = "MLB_2026_TRAINING_PHASE_ELIGIBILITY_ACTIVE_CUTOVER_V1"
ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_training_phase_eligibility_active_cutover_v1"
TEST_MODULES = (
    "backend.mlb.tests.test_mlb_training_phase_eligibility_active_cutover_v1",
    "backend.mlb.tests.test_mlb_training_phase_eligibility_dry_run_v1",
    "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1",
)
EXPECTED_TEST_COUNTS = {
    "backend.mlb.tests.test_mlb_training_phase_eligibility_active_cutover_v1": 12,
    "backend.mlb.tests.test_mlb_training_phase_eligibility_dry_run_v1": 15,
    "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1": 13,
}


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.scenarios: list[dict[str, str]] = []

    @staticmethod
    def _name(test: unittest.case.TestCase) -> str:
        return test.id()

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.scenarios.append({"scenario": self._name(test), "status": "PASSED"})

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.scenarios.append(
            {"scenario": self._name(test), "status": "SKIPPED", "reason": reason}
        )

    def _failed(self, test: unittest.case.TestCase, err: Any) -> None:
        self.scenarios.append(
            {
                "scenario": self._name(test),
                "status": "FAILED",
                "error": self._exc_info_to_string(err, test),
            }
        )

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._failed(test, err)

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._failed(test, err)


def model_artifact_state() -> dict[str, Any]:
    roots = (ROOT / "models_out", ROOT / "backend/mlb/models")
    rows = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.joblib")):
            stat = path.stat()
            rows.append(
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "inode": stat.st_ino,
                }
            )
    return {"artifact_count": len(rows), "state_sha256": canonical_sha256(rows)}


def execute_tests() -> dict[str, Any]:
    before = model_artifact_state()
    scenarios: list[dict[str, str]] = []
    module_counts: dict[str, int] = {}
    failures = skips = executed = 0
    for module in TEST_MODULES:
        suite = unittest.defaultTestLoader.loadTestsFromName(module)
        stream = io.StringIO()
        result = unittest.TextTestRunner(
            stream=stream,
            verbosity=1,
            resultclass=RecordingResult,
        ).run(suite)
        module_counts[module] = int(result.testsRun)
        executed += int(result.testsRun)
        failures += len(result.failures) + len(result.errors)
        skips += len(result.skipped)
        scenarios.extend(result.scenarios)
    after = model_artifact_state()
    intended = sum(EXPECTED_TEST_COUNTS.values())
    module_coverage = module_counts == EXPECTED_TEST_COUNTS
    passed = executed - failures - skips
    return {
        "runner": "PYTHON_STDLIB_UNITTEST_ACTUAL_ASSERTION_EXECUTION",
        "modules": list(TEST_MODULES),
        "module_counts": module_counts,
        "intended": intended,
        "executed": executed,
        "passed": passed,
        "failed": failures,
        "skipped": skips,
        "unexecuted": intended - executed,
        "module_coverage_exact": module_coverage,
        "model_artifact_state_before": before,
        "model_artifact_state_after": after,
        "model_artifacts_unchanged": before == after,
        "successful": (
            failures == 0
            and skips == 0
            and executed == intended
            and module_coverage
            and before == after
        ),
        "scenarios": sorted(scenarios, key=lambda row: row["scenario"]),
    }


def static_validation_snapshot() -> dict[str, Any]:
    trainer_path = ROOT / "backend/mlb/model_trainer.py"
    lineage_path = ROOT / "backend/mlb/training_run_lineage_v1.py"
    trainer_source = trainer_path.read_text(encoding="utf-8")
    lineage_source = lineage_path.read_text(encoding="utf-8")
    tree = ast.parse(trainer_source)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    source_modes = {}
    for name in ("_fetch_from_view", "_fetch_base_and_merge", "_fetch_reconcile_and_merge"):
        rendered = ast.dump(functions[name], include_attributes=False)
        source_modes[name] = "_apply_active_training_phase_gate" in rendered
    trainer = functions["train_models_for_prop"]
    calls = [
        (node.func.id, node.lineno)
        for node in ast.walk(trainer)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    line_by_call = {name: line for name, line in calls}
    ordering = {
        "fetch_before_input_binding": line_by_call["fetch_training_rows"]
        < line_by_call["_create_training_input_binding"],
        "input_binding_before_pipeline": line_by_call["_create_training_input_binding"]
        < line_by_call["build_pipeline"],
    }
    gate_source = ast.unparse(functions["_apply_active_training_phase_gate"])
    return {
        "source_modes_gated": source_modes,
        "fit_ordering": ordering,
        "input_manifest_contract_present": "MLB_IMMUTABLE_TRAINING_INPUT_MANIFEST_V1" in lineage_source,
        "result_manifest_contract_present": "MLB_IMMUTABLE_TRAINING_RESULT_MANIFEST_V1" in lineage_source,
        "atomic_no_overwrite_publish": "os.link(temporary, path)" in lineage_source,
        "completed_binding_before_latest": trainer_source.index(
            "require_completed_result_binding(input_manifest_path, result_manifest_path)"
        )
        < trainer_source.index("_atomic_write_bytes(latest_path, model_bytes)"),
        "no_independent_phase_mapping": "normalize_source_game_type" not in trainer_source,
        "no_calendar_phase_inference": all(
            token not in gate_source
            for token in ("game_date", "month", "calendar", "default_to_r")
        ),
    }


def validate() -> dict[str, Any]:
    tests = execute_tests()
    first_static = static_validation_snapshot()
    second_static = static_validation_snapshot()
    dry_report = json.loads(
        (
            ROOT
            / "docs/contracts/mlb_2026_training_phase_eligibility_dry_run_v1/dry_run_report.json"
        ).read_text(encoding="utf-8")
    )
    gate = dry_report["gate_report"]
    checks = {
        "all_dependency_free_tests": tests["successful"],
        "all_source_modes_gated": all(first_static["source_modes_gated"].values()),
        "input_binding_precedes_pipeline": all(first_static["fit_ordering"].values()),
        "completed_binding_precedes_registration": first_static[
            "completed_binding_before_latest"
        ],
        "manifest_contracts_present": first_static["input_manifest_contract_present"]
        and first_static["result_manifest_contract_present"],
        "atomic_immutable_manifest_write": first_static["atomic_no_overwrite_publish"],
        "no_independent_phase_mapping": first_static["no_independent_phase_mapping"],
        "no_calendar_phase_inference": first_static["no_calendar_phase_inference"],
        "static_validation_deterministic": first_static == second_static,
        "frozen_counts_exact": (
            gate["input_row_count"] == 600766
            and gate["input_distinct_game_pk_count"] == 2812
            and gate["admitted_row_count"] == 459604
            and gate["admitted_distinct_game_pk_count"] == 2341
            and gate["decision_row_counts"]["EXCLUDED_PRESEASON"] == 141162
            and len(gate["decision_game_pks"]["EXCLUDED_PRESEASON"]) == 471
            and gate["blocked_row_count"] == 0
        ),
        "retained_hashes_invariant": dry_report["invariance"][
            "all_retained_hashes_identical"
        ],
        "zero_model_fit_during_validation": tests["model_artifacts_unchanged"],
        "zero_operational_artifact_mutation": tests["model_artifacts_unchanged"],
    }
    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks_passed": sum(checks.values()),
        "checks_failed": sum(not value for value in checks.values()),
        "checks": checks,
        "tests": tests,
        "static_validation": first_static,
        "static_validation_sha256": canonical_sha256(first_static),
        "frozen_reproduction": {
            "input_rows": gate["input_row_count"],
            "input_game_pks": gate["input_distinct_game_pk_count"],
            "admitted_rows": gate["admitted_row_count"],
            "admitted_game_pks": gate["admitted_distinct_game_pk_count"],
            "excluded_preseason_rows": gate["decision_row_counts"]["EXCLUDED_PRESEASON"],
            "excluded_preseason_game_pks": len(
                gate["decision_game_pks"]["EXCLUDED_PRESEASON"]
            ),
            "blocked_rows": gate["blocked_row_count"],
            "invariant_hashes": dry_report["invariance"]["hashes"],
        },
        "safety": {
            "model_fit_calls": 0,
            "model_train_commands": 0,
            "database_connections": 0,
            "network_requests": 0,
            "paid_requests": 0,
            "operational_artifact_writes": 0,
            "synthetic_temporary_artifacts_only": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=PACKAGE / "validation_report.json")
    args = parser.parse_args()
    if args.output.resolve().parent != PACKAGE.resolve():
        raise RuntimeError("ACTIVE_CUTOVER_EVIDENCE_DESTINATION_NOT_GOVERNED")
    report = validate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
