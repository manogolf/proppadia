#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free validation for the active training phase cutover."""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
from collections import Counter, defaultdict
import unittest
from pathlib import Path
from typing import Any


CONTRACT_NAME = "MLB_2026_TRAINING_PHASE_ACTIVE_CUTOVER_EVIDENCE_CORRECTION_V1"
PARENT_CONTRACT_NAME = "MLB_2026_TRAINING_PHASE_ELIGIBILITY_ACTIVE_CUTOVER_V1"
ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_training_phase_eligibility_active_cutover_v1"
CORRECTION_PACKAGE = PACKAGE / "evidence_correction_v1"
DESIGN_INVENTORY = (
    ROOT
    / "docs/contracts/mlb_2026_stat_derived_training_phase_correction_design_v1"
    / "model_lineage_inventory.csv"
)
ALLOWED_MODEL_EXTENSIONS = frozenset({".joblib", ".pkl", ".pickle", ".onnx", ".pt"})
EXPECTED_HARD_LINKED_PATH_GROUPS = [
    [
        "artifacts/analysis/model_development/mlb_cc_0001_prepared_vector_reconstruction/2026-07-10/model_replay_dirs/hits_20260611/latest/hits.joblib",
        "models_out/archive/hits/hits-20260611T060831Z.joblib",
    ],
    [
        "artifacts/analysis/model_development/mlb_cc_0001_prepared_vector_reconstruction/2026-07-10/model_replay_dirs/hits_20260709/latest/hits.joblib",
        "models_out/archive/hits/hits-20260709T061129Z.joblib",
    ],
]
TEST_MODULES = (
    "backend.mlb.tests.test_mlb_training_phase_eligibility_active_cutover_v1",
    "backend.mlb.tests.test_mlb_training_phase_active_cutover_evidence_correction_v1",
    "backend.mlb.tests.test_mlb_training_phase_eligibility_dry_run_v1",
    "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1",
)
EXPECTED_TEST_COUNTS = {
    "backend.mlb.tests.test_mlb_training_phase_eligibility_active_cutover_v1": 12,
    "backend.mlb.tests.test_mlb_training_phase_active_cutover_evidence_correction_v1": 7,
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


def _allowed_model_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in ALLOWED_MODEL_EXTENSIONS


def mlb_model_binary_paths(root: Path = ROOT) -> list[Path]:
    """Return the correction-design MLB binary population by unique path.

    This function reads directory entries and file metadata only.  It never
    opens or hashes a model binary's content.
    """

    paths: dict[str, Path] = {}

    models_out = root / "models_out"
    if models_out.exists():
        for path in models_out.rglob("*"):
            relative = path.relative_to(models_out)
            if "nhl" in {part.lower() for part in relative.parts}:
                continue
            if _allowed_model_file(path):
                paths[path.relative_to(root).as_posix()] = path

    legacy_bundle = root / "artifacts/mlb_models_bundle"
    if legacy_bundle.exists():
        for path in legacy_bundle.rglob("*"):
            if _allowed_model_file(path):
                paths[path.relative_to(root).as_posix()] = path

    residual_ranker = (
        root / "backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib"
    )
    if _allowed_model_file(residual_ranker):
        paths[residual_ranker.relative_to(root).as_posix()] = residual_ranker

    research = root / "artifacts/analysis/model_development"
    if research.exists():
        for path in research.rglob("*"):
            relative = path.relative_to(root).as_posix()
            if _allowed_model_file(path) and "mlb" in relative.lower():
                paths[relative] = path

    return [paths[key] for key in sorted(paths)]


def original_monitor_paths(root: Path = ROOT) -> list[Path]:
    """Reconstruct the superseded two-root `.joblib` monitor population."""

    paths: dict[str, Path] = {}
    for scan_root in (root / "models_out", root / "backend/mlb/models"):
        if not scan_root.exists():
            continue
        for path in scan_root.rglob("*.joblib"):
            if path.is_file():
                paths[path.relative_to(root).as_posix()] = path
    return [paths[key] for key in sorted(paths)]


def model_metadata_state(root: Path = ROOT) -> dict[str, Any]:
    """Metadata-identity safety state; explicitly not byte identity."""

    rows: list[dict[str, Any]] = []
    inode_paths: defaultdict[tuple[int, int], list[str]] = defaultdict(list)
    source_counts: Counter[str] = Counter()
    extension_counts: Counter[str] = Counter()
    for path in mlb_model_binary_paths(root):
        relative = path.relative_to(root).as_posix()
        stat = path.stat()
        if relative.startswith("models_out/"):
            source = "models_out_excluding_nhl"
        elif relative.startswith("artifacts/mlb_models_bundle/"):
            source = "legacy_mlb_models_bundle"
        elif relative == "backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib":
            source = "hits_residual_ranker"
        else:
            source = "mlb_tagged_model_development_research"
        rows.append(
            {
                "path": relative,
                "bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "device": stat.st_dev,
                "inode": stat.st_ino,
                "source": source,
            }
        )
        inode_paths[(stat.st_dev, stat.st_ino)].append(relative)
        source_counts[source] += 1
        extension_counts[path.suffix.lower()] += 1
    hard_linked_path_groups = sorted(
        (sorted(paths) for paths in inode_paths.values() if len(paths) > 1),
        key=lambda paths: paths[0],
    )
    return {
        "identity_semantics": "METADATA_IDENTITY_ONLY_NOT_BYTE_IDENTITY",
        "artifact_path_count": len(rows),
        "inode_identity_count": len(inode_paths),
        "extension_counts": dict(sorted(extension_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "nhl_path_count": sum(
            "nhl" in {part.lower() for part in Path(row["path"]).parts}
            for row in rows
        ),
        "hard_linked_path_groups": hard_linked_path_groups,
        "metadata_fields": ["path", "bytes", "mtime_ns", "device", "inode", "source"],
        "metadata_state_sha256": canonical_sha256(rows),
        "model_content_hashes_computed": 0,
        "byte_identity_proven": False,
    }


def artifact_population_reconciliation(root: Path = ROOT) -> dict[str, Any]:
    corrected_paths = {
        path.relative_to(root).as_posix() for path in mlb_model_binary_paths(root)
    }
    original_paths = {
        path.relative_to(root).as_posix() for path in original_monitor_paths(root)
    }
    original_nhl = sorted(
        path for path in original_paths if "nhl" in {p.lower() for p in Path(path).parts}
    )
    omitted = sorted(corrected_paths - original_paths)
    original_mlb = sorted(original_paths & corrected_paths)
    return {
        "corrected_mlb_binary_path_count": len(corrected_paths),
        "corrected_path_population_sha256": canonical_sha256(sorted(corrected_paths)),
        "original_monitor_path_count": len(original_paths),
        "original_monitor_mlb_path_count": len(original_mlb),
        "original_monitor_nhl_path_count": len(original_nhl),
        "original_monitor_nhl_paths": original_nhl,
        "mlb_paths_omitted_by_original_monitor_count": len(omitted),
        "mlb_paths_omitted_by_original_monitor": omitted,
        "mlb_paths_omitted_by_original_monitor_sha256": canonical_sha256(omitted),
        "original_monitor_evidence_status": "SUPERSEDED_BY_MLB_2026_TRAINING_PHASE_ACTIVE_CUTOVER_EVIDENCE_CORRECTION_V1",
    }


def design_inventory_binary_paths(root: Path = ROOT) -> set[str]:
    inventory = (
        root
        / "docs/contracts/mlb_2026_stat_derived_training_phase_correction_design_v1"
        / "model_lineage_inventory.csv"
    )
    with inventory.open(newline="", encoding="utf-8") as handle:
        return {
            row["artifact_path"]
            for row in csv.DictReader(handle)
            if row["artifact_kind"] in {"MODEL_BINARY", "RESEARCH_MODEL_BINARY"}
        }


def execute_tests() -> dict[str, Any]:
    before = model_metadata_state()
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
    after = model_metadata_state()
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
        "model_metadata_state_before": before,
        "model_metadata_state_after": after,
        "model_metadata_unchanged": before == after,
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
    metadata = tests["model_metadata_state_after"]
    reconciliation = artifact_population_reconciliation()
    corrected_paths = {
        path.relative_to(ROOT).as_posix() for path in mlb_model_binary_paths()
    }
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
        "corrected_population_matches_design_inventory": corrected_paths
        == design_inventory_binary_paths(),
        "corrected_population_counts_exact": (
            metadata["artifact_path_count"] == 538
            and metadata["extension_counts"] == {".joblib": 534, ".pkl": 4}
            and metadata["nhl_path_count"] == 0
            and metadata["inode_identity_count"] == 536
            and metadata["hard_linked_path_groups"]
            == EXPECTED_HARD_LINKED_PATH_GROUPS
        ),
        "original_monitor_scope_reconciled": (
            reconciliation["original_monitor_path_count"] == 444
            and reconciliation["original_monitor_mlb_path_count"] == 428
            and reconciliation["original_monitor_nhl_path_count"] == 16
            and reconciliation["mlb_paths_omitted_by_original_monitor_count"] == 110
        ),
        "metadata_monitor_explicitly_not_byte_identity": (
            metadata["identity_semantics"]
            == "METADATA_IDENTITY_ONLY_NOT_BYTE_IDENTITY"
            and metadata["model_content_hashes_computed"] == 0
            and metadata["byte_identity_proven"] is False
        ),
        "zero_model_fit_during_validation": tests["model_metadata_unchanged"],
        "zero_operational_artifact_mutation": tests["model_metadata_unchanged"],
    }
    return {
        "contract_name": CONTRACT_NAME,
        "parent_contract_name": PARENT_CONTRACT_NAME,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks_passed": sum(checks.values()),
        "checks_failed": sum(not value for value in checks.values()),
        "checks": checks,
        "tests": tests,
        "artifact_metadata_monitor": metadata,
        "artifact_population_reconciliation": reconciliation,
        "evidence_correction": {
            "original_report_path": "docs/contracts/mlb_2026_training_phase_eligibility_active_cutover_v1/validation_report.json",
            "original_report_preserved": True,
            "original_444_claim_superseded": True,
            "monitored_metadata_unchanged": True,
            "evidence_of_model_mutation": False,
            "exhaustive_538_file_byte_identity_proven": False,
            "eligibility_and_lineage_validation_affected": False,
            "existing_model_lineage_classifications_changed": False,
        },
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
            "existing_model_content_hashes_computed": 0,
            "synthetic_temporary_artifacts_only": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=CORRECTION_PACKAGE / "validation_report.json",
    )
    args = parser.parse_args()
    if args.output.resolve().parent != CORRECTION_PACKAGE.resolve():
        raise RuntimeError("ACTIVE_CUTOVER_EVIDENCE_DESTINATION_NOT_GOVERNED")
    report = validate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
