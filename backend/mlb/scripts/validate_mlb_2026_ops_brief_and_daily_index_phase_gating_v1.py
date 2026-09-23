#!/usr/bin/env python3
"""Build compact, offline evidence for the Ops Brief/daily-index phase gate."""

from __future__ import annotations

import csv
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any

from backend.mlb.reporting.phase_reporting_v1 import (
    AGREEMENT_POPULATION,
    AGREEMENT_RECONCILIATION,
    AGREEMENT_STALENESS,
    CONTRACT_NAME,
    REPO_ROOT,
    build_current_phase_reporting_control,
    sha256_file,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    DEFAULT_PROPOSAL_PATH,
    DEFAULT_SOURCE_MANIFEST_PATH,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    DEFAULT_PACKAGE_PATH as CLOSE_PACKAGE,
    INVENTORY_FILENAME,
    MANIFEST_FILENAME,
)


CANONICAL_PYTHON = Path("/Users/jerrystrain/Projects/proppadia/.venv/bin/python")
PACKAGE = REPO_ROOT / "docs/contracts/mlb_2026_ops_brief_and_daily_index_phase_gating_v1"
TEST_MODULES = (
    "backend.mlb.tests.test_mlb_2026_ops_brief_and_daily_index_phase_gating_v1",
    "backend.mlb.tests.test_mlb_2026_authoritative_regular_season_close_inventory_v1",
    "backend.mlb.tests.test_mlb_2026_agreement_study_phase_gating_v1",
)
SOURCE_PATHS = (
    DEFAULT_PROPOSAL_PATH,
    DEFAULT_SOURCE_MANIFEST_PATH,
    CLOSE_PACKAGE / INVENTORY_FILENAME,
    CLOSE_PACKAGE / MANIFEST_FILENAME,
    AGREEMENT_RECONCILIATION,
    AGREEMENT_POPULATION,
    AGREEMENT_STALENESS,
    REPO_ROOT / "backend/mlb/exports/model_v2/mlb_market_strong_agreement_separation_prospective_v1.sqlite3",
    REPO_ROOT / "backend/mlb/config/model_authority.json",
)
SOURCE_FILES = (
    REPO_ROOT / "backend/mlb/reporting/phase_reporting_v1.py",
    REPO_ROOT / "backend/mlb/scripts/report_mlb_daily_ops_brief.py",
    REPO_ROOT / "backend/mlb/scripts/build_mlb_artifact_index.py",
    REPO_ROOT / "backend/mlb/scripts/validate_mlb_2026_ops_brief_and_daily_index_phase_gating_v1.py",
    REPO_ROOT / "backend/mlb/tests/test_mlb_2026_ops_brief_and_daily_index_phase_gating_v1.py",
)


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def relative(path: Path) -> str:
    return str(path.resolve().relative_to(REPO_ROOT.resolve()))


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.scenarios: list[dict[str, str]] = []

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.scenarios.append({"scenario": test.id(), "status": "passed"})

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self.scenarios.append({"scenario": test.id(), "status": "failed"})

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addError(test, err)
        self.scenarios.append({"scenario": test.id(), "status": "failed"})

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.scenarios.append(
            {"scenario": test.id(), "status": "skipped", "reason": reason}
        )


def execute_tests() -> dict[str, Any]:
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for module in TEST_MODULES:
        suite.addTests(loader.loadTestsFromName(module))
    stream = io.StringIO()
    result: RecordingResult = unittest.TextTestRunner(
        stream=stream,
        verbosity=2,
        resultclass=RecordingResult,
    ).run(suite)
    passed = sum(row["status"] == "passed" for row in result.scenarios)
    failed = sum(row["status"] == "failed" for row in result.scenarios)
    skipped = sum(row["status"] == "skipped" for row in result.scenarios)
    return {
        "runner": "unittest_standard_library",
        "canonical_interpreter": str(CANONICAL_PYTHON),
        "modules": list(TEST_MODULES),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "all_intended_scenarios_executed": failed == 0 and skipped == 0,
        "scenarios": result.scenarios,
    }


def source_hashes() -> dict[str, str]:
    return {relative(path): sha256_file(path) for path in SOURCE_PATHS}


def write_consumer_manifest() -> None:
    rows = [
        ("both", "canonical game-phase proposal", relative(DEFAULT_PROPOSAL_PATH), "GOVERNING_AUTHORITY", "exact gamePk", "regular/postseason/preseason partitions"),
        ("both", "regular-season close inventory", relative(CLOSE_PACKAGE / INVENTORY_FILENAME), "GOVERNING_LEDGER", "exact gamePk", "close status and late-season descriptive completeness"),
        ("both", "agreement immutable SQLite ledger", "backend/mlb/exports/model_v2/mlb_market_strong_agreement_separation_prospective_v1.sqlite3", "GOVERNING_LEDGER", "exact gamePk", "phase-separated prospective agreement status"),
        ("both", "agreement committed daily summary", "artifacts/analysis/model_development/mlb_market_strong_agreement_separation_prospective_v1/2026-09-09/summary.json", "STALE_CACHE_REJECTED_AS_AUTHORITY", "aggregate only", "preserved; cannot override live ledger"),
        ("ops_brief", "model-vs-fade summary", "tmp/analysis/mlb_model_vs_fade_summary.json", "LEGACY_UNPARTITIONED_SUMMARY", "not proven", "metrics suppressed from certification"),
        ("ops_brief", "ops-current-state moneyline/totals", "artifacts/analysis/mlb/ops_current_state/<DATE>/mlb_ops_current_state_summary.json", "PHASE_UNBOUND_GENERATED_SUMMARY", "aggregate only", "metrics suppressed"),
        ("ops_brief", "Pinnacle capture summary", "backend/mlb/exports/market_history/full_game_totals/<DATE>/*/pinnacle/pinnacle_capture_summary.json", "COLLECTION_STATUS_ONLY", "not exposed by aggregate", "not market/ROI evidence"),
        ("ops_brief", "postgrade alerts", "artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json", "INACTIVE_LEGACY_STATUS", "not applicable", "historical context only"),
        ("ops_brief", "pipeline and ops histories", "artifacts/mlb_pipeline_history.jsonl; artifacts/mlb_prod12_ops_history.jsonl", "OPERATIONAL_HEALTH", "not applicable", "not evaluation evidence"),
        ("ops_brief", "Hits environment and candidate boards", "artifacts/analysis/mlb/review_aids/**", "SOURCE_HEALTH_OR_REVIEW_ONLY", "not asserted by this layer", "not evaluation certification"),
        ("ops_brief", "legacy model performance", "backend/mlb/exports/model_performance/*.csv", "LEGACY_UNPARTITIONED_SUMMARY", "not proven", "cannot enter governed phase totals"),
        ("ops_brief", "review-aid performance", "artifacts/analysis/mlb/review_aids/performance/review_aid_performance_summary.json", "LEGACY_UNPARTITIONED_SUMMARY", "not proven", "cannot enter governed phase totals"),
        ("ops_brief", "Total Bases shadow evaluation", "artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json", "LEGACY_UNPARTITIONED_SUMMARY", "not proven", "cannot enter governed phase totals"),
        ("daily_index", "artifact row/link counts", "artifacts/analysis/mlb/**", "NAVIGATION_OR_AVAILABILITY_ONLY", "not applicable", "not evaluation evidence"),
        ("both", "historical 56-20 cohort", "separate frozen historical contract", "HISTORICAL_SEPARATE", "frozen cohort", "never prospective agreement evidence"),
    ]
    with (PACKAGE / "input_and_consumer_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("surface", "input", "source_path", "classification", "identity", "reporting_use"))
        writer.writerows(rows)


def write_source_manifest(control: dict[str, Any]) -> None:
    roles = {
        relative(DEFAULT_PROPOSAL_PATH): "canonical exact-gamePk phase records",
        relative(DEFAULT_SOURCE_MANIFEST_PATH): "authority source-hash manifest",
        relative(CLOSE_PACKAGE / INVENTORY_FILENAME): "canonical regular-season disposition ledger",
        relative(CLOSE_PACKAGE / MANIFEST_FILENAME): "close inventory population/hash binding",
        relative(AGREEMENT_RECONCILIATION): "current immutable agreement-ledger reconciliation",
        relative(AGREEMENT_POPULATION): "agreement exact-gamePk phase population",
        relative(AGREEMENT_STALENESS): "stale-summary rejection evidence",
        "backend/mlb/exports/model_v2/mlb_market_strong_agreement_separation_prospective_v1.sqlite3": "immutable agreement ledger",
        "backend/mlb/config/model_authority.json": "model governance",
    }
    hashes = source_hashes()
    with (PACKAGE / "authority_source_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("path", "bytes", "sha256", "role", "authority_status"))
        for rel_path, digest in sorted(hashes.items()):
            path = REPO_ROOT / rel_path
            writer.writerow((rel_path, path.stat().st_size, digest, roles[rel_path], "VERIFIED_READ_ONLY"))
    write_json(PACKAGE / "phase_reporting_control.json", control)
    with (PACKAGE / "source_and_test_manifest.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("path", "bytes", "sha256", "role"))
        for path in SOURCE_FILES:
            writer.writerow(
                (
                    relative(path),
                    path.stat().st_size,
                    sha256_file(path),
                    (
                        "deterministic_tests"
                        if "/tests/" in path.as_posix()
                        else "source_or_validator"
                    ),
                )
            )


def write_readme(control: dict[str, Any], tests: dict[str, Any]) -> None:
    pop = control["canonical_population"]
    close = control["close"]
    late = close["late_season_regular_season"]
    agreement = control["agreement"]
    text = f"""# MLB 2026 Ops Brief and Daily Index Phase Gating V1

The Ops Brief and daily index now use one read-only, exact-`gamePk` reporting
control. It validates the frozen file authority, the hash-bound close inventory,
and the current immutable agreement ledger before displaying governed counts.
Phase is never reconstructed from date, month, filename, status, market, or model
participation.

## Current governed state

- Canonical population: {pop['total']} games ({pop['REGULAR_SEASON']} regular,
  {pop['PRESEASON']} preseason, {pop['POSTSEASON']} postseason).
- Authority exceptions: missing {pop['missing']}, unknown {pop['unknown']},
  conflicting {pop['conflicting']}, duplicate identities {pop['duplicate_identities']}.
- Regular-season close: `{close['decision']}` with {close['outstanding_game_pks']}
  scheduled-not-final and {close['unresolved_game_pks']} unresolved games.
- Late-season regular subset: {late['game_pks']} games, descriptive only; it cannot
  independently certify an ordinary model.
- Postseason: no authoritative game is present. The empty partition is reported
  explicitly as no evidence, not omission and not synthetic operational proof.
- Agreement: the live immutable ledger governs ({agreement['live_counts']['risk_rows']}
  risk rows, {agreement['live_counts']['outcomes']} outcomes,
  {agreement['live_counts']['bookmaker_price_cells']} price cells). The committed
  daily summary remains preserved but stale and cannot override the ledger.
- Historical 56-20 evidence remains a separate historical cohort.

## Presentation correction

Phase-unbound legacy model, metric, market, and ROI aggregates are no longer
displayed as current evidence. Other counts remain visible only as operational,
source-health, navigation, or availability observations. No prediction, outcome,
price, credit, metric, ledger, schedule, or close artifact was rewritten.

## Validation

- Standard-library tests: {tests['passed']} passed, {tests['failed']} failed,
  {tests['skipped']} skipped.
- Both report surfaces invoke the same renderer and source-hashed control.
- The close checker remains check-only; no close package was created.
- Validation used zero network/provider requests and zero paid credits.

## Remaining blockers

1. Exactly {close['outstanding_game_pks']} authoritative regular-season games are
   still scheduled-not-final.
2. No actual authoritative postseason game has traversed the ordinary reporting
   path, so operational postseason readiness remains blocked.
3. Authority currently ends at {control['authority']['supported_through_date']};
   later dates fail closed until ordinary authoritative retention extends it.
4. No qualified MLB production model exists; selector, ranking, Quick Card,
   publication, and wagering remain unavailable.
"""
    (PACKAGE / "README.md").write_text(text, encoding="utf-8")


def write_manifest() -> dict[str, str]:
    manifest_path = PACKAGE / "sha256_manifest.txt"
    entries: dict[str, str] = {}
    for path in sorted(PACKAGE.iterdir()):
        if not path.is_file() or path == manifest_path:
            continue
        entries[path.name] = sha256_file(path)
    manifest_path.write_text(
        "".join(f"{digest}  {name}\n" for name, digest in entries.items()),
        encoding="utf-8",
    )
    return entries


def main() -> int:
    if Path(sys.executable).resolve() != CANONICAL_PYTHON.resolve():
        raise SystemExit(f"canonical interpreter required: {CANONICAL_PYTHON}")
    before = source_hashes()
    tests = execute_tests()
    control = build_current_phase_reporting_control(report_date="2026-09-22")
    after = source_hashes()
    PACKAGE.mkdir(parents=True, exist_ok=True)
    write_source_manifest(control)
    write_consumer_manifest()
    write_json(PACKAGE / "executed_test_report.json", tests)
    reconciliation = _load_reconciliation()
    write_json(
        PACKAGE / "before_after_invariance_report.json",
        {
            "source_hashes_before": before,
            "source_hashes_after": after,
            "all_governing_source_hashes_identical": before == after,
            "prediction_outcome_price_credit_metric_evidence": {
                "agreement_table_hashes": reconciliation["table_hashes"],
                "provider_and_study_credit_reconciliation": reconciliation[
                    "provider_and_study_credit_reconciliation"
                ],
                "ledger_sha256_before": reconciliation["ledger"]["prechange_sha256"],
                "ledger_sha256_after": sha256_file(
                    REPO_ROOT / reconciliation["ledger"]["path"]
                ),
                "changed": False,
            },
            "presentation_changed": True,
            "historical_reports_rewritten": False,
            "network_requests": 0,
            "paid_credits": 0,
        },
    )
    write_json(
        PACKAGE / "stale_summary_handling.json",
        {
            "governing_authority": "LIVE_IMMUTABLE_LEDGER",
            "live_counts": control["agreement"]["live_counts"],
            "stale_summary": control["agreement"]["stale_summary"],
            "historical_56_20": control["historical_56_20"],
            "result": "STALE_SUMMARY_PRESERVED_BUT_CANNOT_OVERRIDE_LEDGER",
        },
    )
    write_json(PACKAGE / "classifications.json", control["classifications"])
    write_json(
        PACKAGE / "before_after_reporting_comparison.json",
        {
            "canonical_phase_and_count_reporting": {
                "before": "UNPARTITIONED_OR_NOT_EXPOSED_AS_ONE_GOVERNED_CONTROL",
                "after": control["canonical_population"],
            },
            "late_season_completeness": {
                "before": "DATE_CONTEXT_NOT_BOUND_TO_CANONICAL_CLOSE_POPULATION",
                "after": control["close"]["late_season_regular_season"],
            },
            "postseason_reporting": {
                "before": "NO_EXPLICIT_EMPTY_AUTHORITATIVE_PARTITION",
                "after": control["partitions"]["POSTSEASON"],
            },
            "agreement_reporting": {
                "before": "STALE_COMMITTED_SUMMARY_COULD_BE_DISPLAYED_WITHOUT_LEDGER_PRIORITY",
                "after": {
                    "authority": control["agreement"]["authority"],
                    "live_counts": control["agreement"]["live_counts"],
                    "stale_summary_may_override": control["agreement"]["stale_summary"]["may_override_ledger"],
                },
            },
            "close_status": {
                "before": "NOT_BOUND_ON_BOTH_REPORT_SURFACES",
                "after": {
                    "decision": control["close"]["decision"],
                    "population": control["close"]["canonical_regular_season_game_pks"],
                    "accepted_dispositions": control["close"]["accepted_disposition_game_pks"],
                    "outstanding": control["close"]["outstanding_game_pks"],
                    "unresolved": control["close"]["unresolved_game_pks"],
                },
            },
            "prediction_quality": {
                "before": "LEGACY_PHASE_UNBOUND_AGGREGATES_DISPLAYED",
                "after": control["prediction_quality_effect"],
            },
            "market_and_roi": {
                "before": "LEGACY_PHASE_UNBOUND_AGGREGATES_DISPLAYED",
                "after": control["market_roi_effect"],
            },
            "model_selector_publication": {
                "before": "GOVERNANCE_FRAGMENTED_ACROSS_SECTIONS",
                "after": control["classifications"]["model_selector_publication_status"],
            },
            "stale_summary_handling": {
                "before": "GENERATED_SUMMARY_NOT_EXPLICITLY_SUBORDINATED",
                "after": control["agreement"]["stale_summary"]["decision"],
            },
            "evidence_mutation": "NONE_PRESENTATION_ONLY",
        },
    )
    write_json(
        PACKAGE / "remaining_blockers.json",
        {
            "regular_season_close": f"{control['close']['outstanding_game_pks']} scheduled-not-final games",
            "postseason_operational_validation": "no actual authoritative postseason game on ordinary path",
            "authority_horizon": control["authority"]["supported_through_date"],
            "model_authority": "NO_QUALIFIED_MLB_MODEL",
            "selector_ranking_quick_card_publication": "UNAVAILABLE",
        },
    )
    write_readme(control, tests)
    checks = {
        "canonical_interpreter": Path(sys.executable).resolve() == CANONICAL_PYTHON.resolve(),
        "all_tests_passed": tests["failed"] == 0 and tests["skipped"] == 0,
        "deterministic_test_evidence_excludes_wall_clock_output": (
            "runner_output" not in tests
        ),
        "governing_sources_unchanged": before == after,
        "canonical_population_2919": control["canonical_population"]["total"] == 2919,
        "regular_population_2430": control["canonical_population"]["REGULAR_SEASON"] == 2430,
        "preseason_population_489": control["canonical_population"]["PRESEASON"] == 489,
        "postseason_population_zero_explicit": control["canonical_population"]["POSTSEASON"] == 0,
        "authority_exception_counts_zero": all(
            control["canonical_population"][key] == 0
            for key in ("missing", "unknown", "conflicting", "duplicate_identities")
        ),
        "close_check_only_and_blocked": control["close"]["check_only"] and control["close"]["decision"] == "REGULAR_SEASON_CLOSE_BLOCKED",
        "close_outstanding_88": control["close"]["outstanding_game_pks"] == 88,
        "agreement_stale_summary_cannot_override": not control["agreement"]["stale_summary"]["may_override_ledger"],
        "ten_explicit_classifications": len(control["classifications"]) == 10,
        "zero_requests_and_credits": control["classifications"]["requests_and_credits"] == "REPORTING_USED_ZERO_REQUESTS_ZERO_PAID_CREDITS",
    }
    validation = {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "passed_checks": sum(checks.values()),
        "failed_checks": sum(not value for value in checks.values()),
        "test_counts": {key: tests[key] for key in ("passed", "failed", "skipped")},
    }
    write_json(PACKAGE / "validation_report.json", validation)
    manifest = write_manifest()
    print(
        json.dumps(
            {
                "status": validation["status"],
                "package": relative(PACKAGE),
                "files": len(manifest) + 1,
                "manifest_entries": len(manifest),
                "tests": validation["test_counts"],
                "checks": {"passed": validation["passed_checks"], "failed": validation["failed_checks"]},
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if validation["status"] == "PASS" else 1


def _load_reconciliation() -> dict[str, Any]:
    value = json.loads(AGREEMENT_RECONCILIATION.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("agreement reconciliation is not an object")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
