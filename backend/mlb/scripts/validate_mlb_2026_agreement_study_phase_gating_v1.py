#!/usr/bin/env python3
"""Offline validator and compact evidence builder for agreement phase gating."""
from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import io
import json
import sqlite3
import unittest
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from backend.mlb.markets import agreement_phase_gating_v1 as gate
from backend.mlb.scripts import capture_mlb_market_strong_agreement_live_v4 as capture
from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/contracts/mlb_2026_agreement_study_phase_gating_v1"
PRECHANGE_LEDGER_SHA256 = "0ef31f5dee181d8707a7d963b329186955d7984ceb0f72a233df0356b27e6855"
TEST_MODULES = (
    "backend.mlb.tests.test_mlb_2026_agreement_study_phase_gating_v1",
    "backend.mlb.tests.test_mlb_market_strong_agreement_separation_prospective_v1",
    "backend.mlb.tests.test_mlb_market_strong_agreement_live_capture_v4",
    "backend.mlb.tests.test_mlb_rolling_integrity_run_receipt",
    "backend.mlb.tests.test_mlb_market_strong_agreement_separation_feasibility_v2",
    "backend.mlb.tests.test_mlb_market_strong_agreement_separation_live_timing_v3",
    "backend.mlb.tests.test_validate_mlb_market_strong_agreement_separation_prospective_v1",
)
GOVERNED_SOURCE_FILES = (
    "backend/mlb/markets/agreement_phase_gating_v1.py",
    "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py",
    "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py",
    "backend/mlb/scripts/write_mlb_rolling_integrity_receipt.py",
    "backend/mlb/scripts/validate_mlb_2026_agreement_study_phase_gating_v1.py",
    "backend/mlb/tests/test_mlb_2026_agreement_study_phase_gating_v1.py",
    "backend/mlb/tests/test_mlb_market_strong_agreement_separation_prospective_v1.py",
    "backend/mlb/tests/test_mlb_market_strong_agreement_live_capture_v4.py",
    "backend/mlb/tests/test_mlb_rolling_integrity_run_receipt.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(rows: list[dict[str, Any]]) -> str:
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def read_table(connection: sqlite3.Connection, table: str, order: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")]


def execute_tests() -> dict[str, Any]:
    class RecordingResult(unittest.TextTestResult):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.records: list[dict[str, str]] = []

        def addSuccess(self, test):
            super().addSuccess(test)
            self.records.append({"scenario": test.id(), "status": "PASSED"})

        def addFailure(self, test, err):
            super().addFailure(test, err)
            self.records.append({"scenario": test.id(), "status": "FAILED"})

        def addError(self, test, err):
            super().addError(test, err)
            self.records.append({"scenario": test.id(), "status": "FAILED"})

        def addSkip(self, test, reason):
            super().addSkip(test, reason)
            self.records.append({"scenario": test.id(), "status": "SKIPPED"})

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for module in TEST_MODULES:
        suite.addTests(loader.loadTestsFromName(module))
    stream = io.StringIO()
    result = unittest.TextTestRunner(
        stream=stream, verbosity=1, resultclass=RecordingResult).run(suite)
    failed = len(result.failures) + len(result.errors)
    return {
        "passed": result.testsRun - failed - len(result.skipped),
        "failed": failed,
        "skipped": len(result.skipped),
        "executed": result.testsRun,
        "intended": suite.countTestCases(),
        "canonical_interpreter": "/Users/jerrystrain/Projects/proppadia/.venv/bin/python",
        "results": sorted(result.records, key=lambda value: value["scenario"]),
        "successful": result.wasSuccessful(),
    }


def retained_reconciliation() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    ledger = study.LEDGER
    before = sha256(ledger)
    authority = gate.verified_agreement_phase_authority()
    uri = ledger.resolve().as_uri() + "?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        predictions = read_table(connection, "predictions", "game_date,game_id")
        risk = read_table(connection, "risk_set", "game_date,game_key")
        prices = read_table(connection, "bookmaker_prices", "game_key,bookmaker_key")
        outcomes = read_table(connection, "outcomes", "game_key")
        provenance = read_table(connection, "capture_provenance_v4", "game_key")
        claims = read_table(connection, "live_capture_claims_v4", "game_date,capture_mode")
        events = read_table(connection, "live_capture_events_v4", "recorded_at_utc,event_id")

    decision_by_game: dict[int, gate.AgreementPhaseDecision] = {}
    violations: list[dict[str, Any]] = []
    for source, rows in (("predictions", predictions), ("risk_set", risk)):
        for row in rows:
            try:
                decision = gate.classify_agreement_row(row, authority=authority)
                prior = decision_by_game.setdefault(decision.game_pk, decision)
                if prior != decision:
                    violations.append({"source": source, "gamePk": decision.game_pk,
                                       "code": "CROSS_SOURCE_PHASE_CONFLICT"})
            except gate.AgreementPhaseGateError as exc:
                violations.append({"source": source, "gamePk": row.get("game_id"),
                                   "code": exc.code, "detail": exc.detail})

    risk_by_key = {row["game_key"]: row for row in risk}
    aggregates: dict[int, dict[str, Any]] = {}
    for row in predictions:
        game_pk = int(row["game_id"])
        decision = decision_by_game[game_pk]
        aggregates[game_pk] = {
            "gamePk": game_pk, "game_date": row["game_date"],
            "source_game_type": decision.source_game_type,
            "normalized_phase": decision.normalized_phase,
            "postseason_round": decision.postseason_round or "",
            "prediction_rows": 1, "risk_rows": 0, "eligible_risk_rows": 0,
            "outcome_rows": 0, "bookmaker_price_rows": 0, "provenance_rows": 0,
        }
    for row in risk:
        target = aggregates[int(row["game_id"])]
        target["risk_rows"] += 1
        target["eligible_risk_rows"] += int(row["risk_set_eligible"])
    for row in outcomes:
        risk_row = risk_by_key.get(row["game_key"])
        if risk_row is None:
            violations.append({"source": "outcomes", "gamePk": None,
                               "code": "OUTCOME_WITHOUT_EXACT_RISK_IDENTITY",
                               "detail": row["game_key"]})
        else:
            aggregates[int(risk_row["game_id"])]["outcome_rows"] += 1
    for source, rows, field in (
        ("bookmaker_prices", prices, "bookmaker_price_rows"),
        ("capture_provenance_v4", provenance, "provenance_rows"),
    ):
        for row in rows:
            risk_row = risk_by_key.get(row["game_key"])
            if risk_row is None:
                violations.append({"source": source, "gamePk": None,
                                   "code": "CHILD_WITHOUT_EXACT_RISK_IDENTITY",
                                   "detail": row["game_key"]})
            else:
                aggregates[int(risk_row["game_id"])][field] += 1

    phase_counts = Counter(value["normalized_phase"] for value in aggregates.values())
    risk_phase_counts = Counter(
        decision_by_game[int(row["game_id"])].normalized_phase for row in risk)
    outcome_phase_counts = Counter(
        decision_by_game[int(risk_by_key[row["game_key"]]["game_id"])].normalized_phase
        for row in outcomes
    )
    summary = json.loads((study.OUT / "summary.json").read_text())
    raw_files = sorted(capture.RUNTIME.rglob("*.json"))
    raw_manifest = [{"path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size,
                     "sha256": sha256(path)} for path in raw_files]
    after = sha256(ledger)
    reconciliation = {
        "authority": authority.metadata.to_dict(),
        "ledger": {"path": str(ledger.relative_to(ROOT)), "bytes": ledger.stat().st_size,
                   "sha256_before_validation": before, "sha256_after_validation": after,
                   "prechange_sha256": PRECHANGE_LEDGER_SHA256,
                   "metadata_and_content_unchanged_during_validation": before == after,
                   "matches_prechange_sha256": before == PRECHANGE_LEDGER_SHA256},
        "live_counts": {
            "predictions": len(predictions), "distinct_prediction_gamePks": len(aggregates),
            "risk_rows": len(risk), "distinct_risk_gamePks": len({r["game_id"] for r in risk}),
            "eligible_risk_rows": sum(int(r["risk_set_eligible"]) for r in risk),
            "bookmaker_price_cells": len(prices), "outcomes": len(outcomes),
            "provenance_rows": len(provenance), "claims": len(claims), "events": len(events)},
        "phase_counts": {"prediction_gamePks": dict(sorted(phase_counts.items())),
                         "risk_rows": dict(sorted(risk_phase_counts.items())),
                         "outcomes": dict(sorted(outcome_phase_counts.items()))},
        "violations": violations,
        "historical_56_20_included": False,
        "current_separation_classification": "SEPARATION_EVIDENCE_INSUFFICIENT",
        "provider_and_study_credit_reconciliation": {
            "routine_claims": sum(row["capture_mode"] == "LIVE" for row in claims),
            "recovery_claims": sum(row["capture_mode"] == "HISTORICAL_RECOVERY" for row in claims),
            "routine_observed_study_credits": sum(
                int(row["x_requests_last"] or 0) for row in events
                if row["status"] == "SUCCESS_RESPONSE_PRESERVED" and row["capture_mode"] == "LIVE"),
            "recovery_observed_study_credits": sum(
                int(row["x_requests_last"] or 0) for row in events
                if row["status"] == "SUCCESS_RESPONSE_PRESERVED"
                and row["capture_mode"] == "HISTORICAL_RECOVERY"),
            "pre_request_fail_closed_events": sum(
                row["status"] == "LIVE_TIMING_FAIL_CLOSED" for row in events),
        },
        "retained_raw_source_files": len(raw_manifest),
        "retained_raw_source_manifest_sha256": stable_hash(raw_manifest),
        "table_hashes": {
            "predictions": stable_hash(predictions), "risk_set": stable_hash(risk),
            "bookmaker_prices": stable_hash(prices), "outcomes": stable_hash(outcomes),
            "capture_provenance_v4": stable_hash(provenance),
            "live_capture_claims_v4": stable_hash(claims),
            "live_capture_events_v4": stable_hash(events),
        },
    }
    staleness = {
        "committed_summary_path": str((study.OUT / "summary.json").relative_to(ROOT)),
        "committed_summary_counts": {
            "risk_rows": summary["risk_rows"],
            "bookmaker_price_cells": summary["bookmaker_price_cells"],
            "effective_outcome_count": summary["effective_outcome_count"]},
        "live_immutable_ledger_counts": {
            "risk_rows": len(risk), "bookmaker_price_cells": len(prices),
            "effective_outcome_count": len(outcomes)},
        "deltas_live_minus_summary": {
            "risk_rows": len(risk) - summary["risk_rows"],
            "bookmaker_price_cells": len(prices) - summary["bookmaker_price_cells"],
            "effective_outcome_count": len(outcomes) - summary["effective_outcome_count"]},
        "decision": "COMMITTED_DAILY_SUMMARY_STALE_DO_NOT_REWRITE",
        "classification_impact": "NONE_EVIDENCE_GATE_REMAINS_UNMET",
        "acquisition_accounting_is_not_interchangeable": {
            "committed_v1_historical_plan_dates": summary["planned_request_dates"],
            "committed_v1_historical_planned_credits": summary["planned_expected_credit_cost"],
            "live_v4_routine_claims": len(claims),
            "live_v4_observed_credits": reconciliation[
                "provider_and_study_credit_reconciliation"]["routine_observed_study_credits"],
            "interpretation": ("V1 historical expected-cost planning and V4 live observed usage are "
                               "different controls and are reported separately"),
        },
    }
    return reconciliation, [aggregates[key] for key in sorted(aggregates)], staleness


def consumer_manifest() -> list[dict[str, str]]:
    return [
        {"consumer": "V1 prediction observation loader", "role": "immutable prediction input",
         "phase_behavior": "observation preserved; exact gamePk gate required before acquisition",
         "source": "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py"},
        {"consumer": "V1 historical response reconciliation", "role": "market attachment/risk rows",
         "phase_behavior": "explicit phase snapshot; provider-only events rejected",
         "source": "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py"},
        {"consumer": "V4 routine/recovery capture", "role": "claims, raw response, market attachment",
         "phase_behavior": "gate before ledger writes, credential read, or provider request",
         "source": "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py"},
        {"consumer": "V1 outcome grader", "role": "official outcome attachment",
         "phase_behavior": "explicit exact-gamePk regular or postseason cohort",
         "source": "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py"},
        {"consumer": "V1 metrics/report", "role": "proper score, calibration, economics, separation",
         "phase_behavior": "regular and postseason reports are mutually exclusive",
         "source": "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py"},
        {"consumer": "V4 claim accounting", "role": "routine/recovery usage and study credit",
         "phase_behavior": "phase joined transiently by prediction gamePk; mixed phase date fails closed",
         "source": "backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py"},
    ]


def validate(output: Path, *, run_tests: bool = True) -> dict[str, Any]:
    reconciliation, population, staleness = retained_reconciliation()
    tests = execute_tests() if run_tests else {"passed": 0, "failed": 0, "skipped": 0,
                                               "executed": 0, "successful": True}
    bounded_source = "\n".join((inspect.getsource(gate), inspect.getsource(study),
                                 inspect.getsource(capture)))
    forbidden = ('or "R"', "or 'R'", 'fillna("R")', "fillna('R')",
                 "def late_season_regime", "POSTSEASON_CALENDAR_WINDOW")
    checks = {
        "authority_population_healthy": all(
            getattr(gate.verified_agreement_phase_authority().metadata, name) == 0
            for name in ("missing_count", "unknown_count", "conflicting_count",
                         "duplicate_identity_count")),
        "retained_prediction_population_exact": reconciliation["live_counts"]["predictions"] == 158,
        "retained_risk_population_exact": reconciliation["live_counts"]["risk_rows"] == 155,
        "retained_price_population_exact": reconciliation["live_counts"]["bookmaker_price_cells"] == 1550,
        "retained_outcome_population_exact": reconciliation["live_counts"]["outcomes"] == 35,
        "retained_rows_all_authoritative_regular": (
            reconciliation["phase_counts"]["prediction_gamePks"] == {"REGULAR_SEASON": 158}
            and reconciliation["phase_counts"]["risk_rows"] == {"REGULAR_SEASON": 155}
            and reconciliation["phase_counts"]["outcomes"] == {"REGULAR_SEASON": 35}),
        "zero_retained_authority_violations": not reconciliation["violations"],
        "ledger_unchanged_during_validation": reconciliation["ledger"]["metadata_and_content_unchanged_during_validation"],
        "ledger_matches_prechange_hash": reconciliation["ledger"]["matches_prechange_sha256"],
        "routine_recovery_claims_separate": (
            reconciliation["provider_and_study_credit_reconciliation"]["routine_claims"] == 11
            and reconciliation["provider_and_study_credit_reconciliation"]["recovery_claims"] == 0),
        "provider_usage_and_study_credits_separate": (
            reconciliation["provider_and_study_credit_reconciliation"]["routine_observed_study_credits"] == 11),
        "historical_56_20_separate": reconciliation["historical_56_20_included"] is False,
        "current_classification_preserved": (
            reconciliation["current_separation_classification"] == "SEPARATION_EVIDENCE_INSUFFICIENT"),
        "stale_summary_identified_not_rewritten": staleness["decision"] == "COMMITTED_DAILY_SUMMARY_STALE_DO_NOT_REWRITE",
        "no_missing_type_or_calendar_phase_fallback": not any(token in bounded_source for token in forbidden),
        "postseason_default_is_never_implicit": (
            inspect.signature(study.report).parameters["evaluation_phase"].default == "REGULAR_SEASON"
            and inspect.signature(capture.execute_capture).parameters["evaluation_phase"].default == "REGULAR_SEASON"),
        "all_dependency_free_tests_pass": tests["successful"] and tests["executed"] > 0,
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    result = {"contract": "MLB_2026_AGREEMENT_STUDY_PHASE_GATING_V1",
              "status": "PASS" if not failed else "FAIL", "checks": checks,
              "failed_checks": failed, "tests": tests, "network_requests": 0,
              "database_connections": 0, "operational_writes": 0}
    if failed:
        raise RuntimeError("Agreement phase-gating validation failed: " + ", ".join(failed))
    output.mkdir(parents=True, exist_ok=True)
    (output / "consumer_manifest.json").write_text(
        json.dumps(consumer_manifest(), indent=2, sort_keys=True) + "\n")
    (output / "retained_reconciliation.json").write_text(
        json.dumps(reconciliation, indent=2, sort_keys=True) + "\n")
    with (output / "retained_gamepk_population.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(population[0]) if population else ["gamePk"],
            lineterminator="\n")
        writer.writeheader(); writer.writerows(population)
    (output / "summary_staleness_report.json").write_text(
        json.dumps(staleness, indent=2, sort_keys=True) + "\n")
    (output / "validation_report.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n")
    (output / "executed_test_report.json").write_text(
        json.dumps(tests, indent=2, sort_keys=True) + "\n")
    (output / "before_after_invariance_report.json").write_text(json.dumps({
        "ledger_sha256_before": reconciliation["ledger"]["prechange_sha256"],
        "ledger_sha256_after": reconciliation["ledger"]["sha256_after_validation"],
        "byte_identical": reconciliation["ledger"]["matches_prechange_sha256"],
        "table_hashes_after": reconciliation["table_hashes"],
        "membership_effect": "NONE_RETAINED_POPULATION_ALL_AUTHORITATIVE_REGULAR_SEASON",
        "prediction_probability_price_outcome_effect": "NONE_IMMUTABLE_LEDGER_UNCHANGED",
    }, indent=2, sort_keys=True) + "\n")
    with (output / "affected_row_ledger.csv").open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("source", "gamePk", "code", "detail"))
        for violation in reconciliation["violations"]:
            writer.writerow((violation.get("source", ""), violation.get("gamePk", ""),
                             violation.get("code", ""), violation.get("detail", "")))
    with (output / "source_artifact_manifest.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=("path", "bytes", "sha256"), lineterminator="\n")
        writer.writeheader()
        for relative in GOVERNED_SOURCE_FILES:
            path = ROOT / relative
            writer.writerow({"path": relative, "bytes": path.stat().st_size, "sha256": sha256(path)})
    return result


def write_readme(output: Path) -> None:
    text = """# MLB 2026 agreement-study phase gating V1

The prospective ten-book agreement study now joins the shared canonical phase
authority by exact `gamePk` before capture, grading, or evaluation membership.
Regular-season and postseason-shadow evidence are disjoint. Preseason, special,
missing, stale, conflicting, and duplicate authority fail closed. Phase labels
remain transient; immutable ledgers were not rewritten.

The historical 56-20 cohort remains excluded. The live immutable ledger has
158 prediction gamePks, 155 risk rows, 1,550 price cells, and 35 outcomes; all
resolve to authoritative regular season. The committed daily summary is stale
at 124 risk rows and 1,240 price cells and is preserved as historical evidence.
Its classification remains `SEPARATION_EVIDENCE_INSUFFICIENT`.

No prediction, probability, threshold, bookmaker selection, price, outcome,
claim rule, model, selector, publication state, or acquisition authorization
changed. Postseason code paths are synthetic-test ready, but operational
readiness is blocked until the authority includes an actual postseason game
and that game passes through the ordinary path. Late-season regular evidence is
descriptive only and cannot independently certify the study.
"""
    (output / "README.md").write_text(text)


def write_remaining_blockers(output: Path) -> None:
    payload = {
        "postseason_operational_readiness": "BLOCKED_NO_ACTUAL_AUTHORITATIVE_POSTSEASON_OBSERVATION",
        "current_separation_classification": "SEPARATION_EVIDENCE_INSUFFICIENT",
        "late_season_cohort": "DESCRIPTIVE_ONLY_NOT_AN_INDEPENDENT_CERTIFICATION_COHORT",
        "stale_committed_summary": "PRESERVED_NOT_REWRITTEN; LIVE_LEDGER_IS_AUTHORITATIVE",
        "ordinary_capture_horizon": "V4 remains frozen through 2026-09-27; any postseason operation requires separate authorization and fresh authority",
        "smallest_next_action": "Allow ordinary retained-source collection; after the first authoritative postseason game, run the no-network preflight and synthetic-equivalent validator before any separately authorized capture.",
    }
    (output / "remaining_blockers.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_classifications(output: Path) -> None:
    payload = {
        "prospective_regular_season_study_integrity": "READY_EXACT_GAME_PK_AUTHORITY",
        "historical_56_20_separation": "PRESERVED_EXCLUDED_FROM_PROSPECTIVE_EVIDENCE",
        "late_season_cohort_status": "DESCRIPTIVE_ONLY_NOT_INDEPENDENTLY_CERTIFYING",
        "postseason_code_readiness": "READY_SYNTHETIC_FIXTURES_ONLY",
        "postseason_operational_readiness": "BLOCKED_PENDING_ACTUAL_AUTHORITATIVE_POSTSEASON_GAME_ON_ORDINARY_PATH",
        "study_credit_integrity": "READY_PHASE_AND_CAPTURE_MODE_SEPARATED_NO_RULE_CHANGE",
        "current_separation_classification": "SEPARATION_EVIDENCE_INSUFFICIENT",
        "prediction_quality_effect": "NONE_MEMBERSHIP_AND_REPORTING_ONLY",
        "market_comparison_effect": "NONE_RETAINED_MARKET_ROWS_AND_PRICES_UNCHANGED",
        "remaining_blockers": [
            "actual authoritative postseason game has not passed through the ordinary path",
            "current frozen authority supports only through 2026-09-27 and contains no postseason rows",
            "V4 ordinary capture horizon remains frozen through 2026-09-27",
            "committed daily summary is stale relative to the append-only ledger and remains preserved",
        ],
    }
    (output / "classifications.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_manifest(output: Path) -> None:
    files = sorted(path for path in output.rglob("*")
                   if path.is_file() and path.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(
        f"{sha256(path)}  {path.relative_to(output)}\n" for path in files))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=CONTRACT)
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    result = validate(output, run_tests=not args.skip_tests)
    write_readme(output)
    write_remaining_blockers(output)
    write_classifications(output)
    write_manifest(output)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
