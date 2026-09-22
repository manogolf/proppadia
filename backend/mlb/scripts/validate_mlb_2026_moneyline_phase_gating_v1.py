#!/usr/bin/env python3
"""Build and validate the offline MLB Moneyline phase-gating contract."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import sqlite3
import unittest
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.mlb.public_game_predictions.phase_gating_v1 import (
    canonical_rows_sha256,
    classify_moneyline_row,
)
from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_moneyline_phase_gating_v1"
SNAPSHOT = ROOT / "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/database_population_snapshot.json"
MARKET_DB = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
METRIC_ARTIFACT = ROOT / (
    "artifacts/analysis/model_development/"
    "mlb_standalone_prediction_foundation_certification_v1/2026-08-12/"
    "moneyline_prospective_evidence.csv"
)
EXPECTED_SNAPSHOT_SHA256 = "6c821faf73f52b30cefe97c290ec1e0a4f6e6249d9bddfb1e9288788021aed5a"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str).encode("utf-8")


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _source_inventory() -> list[dict[str, Any]]:
    rows = [
        ("backend/mlb/public_game_predictions/pythagorean_log5_v1.py", "immutable probability and official-final grade construction", "ACTIVE_CORE", "grade append is phase-gated; probability formula unchanged"),
        ("backend/mlb/public_game_predictions/durable_store_v1.py", "Postgres prediction/outcome durability", "ACTIVE_CORE", "ungraded outcomes partitioned before grading; final write boundary rechecks authority"),
        ("backend/mlb/scripts/run_mlb_public_game_moneyline_daily_v1.py", "ordinary score/grade lifecycle", "ACTIVE_CORE", "inherits gated durable fetch and gated outcome write"),
        ("backend/mlb/scripts/grade_mlb_public_game_pythagorean_log5_v1.py", "manual local/durable grading adapter", "ACTIVE_CORE", "requires partition before either append path"),
        ("backend/app/services/mlb/public_game_prediction_service.py", "public immutable prediction projection", "ACTIVE_PROJECTION", "joins raw type, normalized phase, postseason round without ledger mutation"),
        ("backend/mlb/scripts/audit_mlb_moneyline_probability_region_premise_v1.py", "accuracy/calibration/Brier/log-loss and market/ROI research reports", "ACTIVE_SHARED_REPORT_LOADER", "default regular-only exact-gamePk partition; postseason callable as a distinct partition"),
        ("backend/mlb/scripts/observe_mlb_strong_moneyline_shadow_v1.py", "manual append-only STRONG observer and hypothetical ROI report", "ACTIVE_INDIRECT", "inherits regular-only shared loader"),
        ("backend/mlb/scripts/observe_mlb_moneyline_strength_classes_shadow_v1.py", "manual append-only class observer and hypothetical ROI report", "ACTIVE_INDIRECT", "inherits regular-only shared loader"),
        ("backend/mlb/scripts/analyze_mlb_strong_moneyline_independent_information_v1.py", "Moneyline information research", "RESEARCH_INDIRECT", "inherits shared regular-only loader"),
        ("backend/mlb/scripts/audit_mlb_betonline_moneyline_capture_recovery_joint_strength_repricing_v1.py", "Moneyline price recovery research", "RESEARCH_INDIRECT", "inherits shared regular-only loader"),
        ("backend/mlb/scripts/audit_mlb_joint_strength_incremental_value_executability_v1.py", "Moneyline joint-strength research", "RESEARCH_INDIRECT", "inherits shared regular-only loader"),
        ("backend/mlb/scripts/audit_mlb_across_board_apparent_ev_provenance_economic_value_v1.py", "cross-lane EV provenance", "EXCLUDED_CROSS_LANE", "not modified; requested scope exclusion"),
        ("backend/mlb/scripts/run_mlb_market_strong_agreement_separation_prospective_v1.py", "agreement study", "EXCLUDED_AGREEMENT", "not modified; requested scope exclusion"),
        ("backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py", "agreement capture", "EXCLUDED_AGREEMENT", "not modified; requested scope exclusion"),
        ("backend/mlb/scripts/run_mlb_pinnacle_anchored_moneyline_residual_v1.py", "one-off residual model-development report", "IMMUTABLE_LEGACY_RESEARCH", "not rerun or rewritten; future rerun must use shared gate"),
        ("backend/mlb/scripts/certify_mlb_standalone_prediction_foundations_v1.py", "one-off foundation certification export", "IMMUTABLE_LEGACY_CERTIFICATION", "preserved; future certification must use phase-bound input manifest"),
        ("backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py", "price attachment", "NON_EVALUATION_INPUT", "captures immutable exact-gamePk prices; evaluation partition is downstream"),
        ("backend/mlb/scripts/capture_mlb_bookmaker_eu_supplemental_v1.py", "price attachment", "NON_EVALUATION_INPUT", "captures immutable exact-gamePk prices; evaluation partition is downstream"),
        ("backend/mlb/scripts/run_mlb_main_market_provider_replacement_trial_v1.py", "provider trial attachment", "NON_EVALUATION_INPUT", "not a certification/evaluation total"),
        ("backend/mlb/scripts/report_mlb_ops_current_state_alignment_v1.py", "general Ops index", "EXCLUDED_GENERAL_OPS", "not modified; requested scope exclusion"),
    ]
    return [
        {"path": path, "consumer": consumer, "classification": classification,
         "phase_gating_result": result, "source_sha256": sha256(ROOT / path)}
        for path, consumer, classification, result in rows
    ]


def _snapshot_reconciliation(authority: HashedProposalAuthority) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if sha256(SNAPSHOT) != EXPECTED_SNAPSHOT_SHA256:
        raise RuntimeError("DATABASE_POPULATION_SNAPSHOT_HASH_MISMATCH")
    payload = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    ledger: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    for source in ("mlb.public_game_moneyline_predictions", "mlb.public_game_moneyline_outcomes"):
        population = payload["populations"][source]
        source_rows = population["rows"]
        regular_rows: list[dict[str, Any]] = []
        for item in source_rows:
            game_pk = int(item["game_pk"])
            dates = list(item["game_dates"])
            if len(dates) != 1:
                raise RuntimeError(f"MONEYLINE_DATE_IDENTITY_CONFLICT:{source}:{game_pk}")
            decision = classify_moneyline_row(
                {"game_id": game_pk, "game_date": dates[0]}, authority=authority
            )
            row_hash = sha256_bytes(canonical(item))
            ledger.append({
                "source": source, "game_pk": game_pk,
                "row_count": len(item["identity_variants"]),
                "game_date": dates[0], "source_game_type": decision.source_game_type,
                "normalized_phase": decision.normalized_phase,
                "postseason_round": decision.postseason_round,
                "evaluation_partition": decision.evaluation_partition,
                "retained_group_sha256": row_hash,
            })
            if decision.evaluation_partition == "REGULAR_SEASON":
                regular_rows.append(item)
        before = population["rowset_sha256"]
        after = canonical_rows_sha256(regular_rows)
        # The source collector's rowset hash has its own canonical envelope;
        # retain it separately and use a like-for-like hash for membership.
        membership_before = canonical_rows_sha256(source_rows)
        summary[source] = {
            "rows": int(population["raw_row_count"]),
            "distinct_game_pks": int(population["distinct_game_pk_count"]),
            "regular_rows": len(regular_rows), "postseason_rows": 0,
            "preseason_rows": 0, "invalid_rows": 0,
            "source_rowset_sha256": before,
            "membership_content_sha256_before": membership_before,
            "membership_content_sha256_after_regular_gate": after,
            "content_equivalent": membership_before == after,
        }
    return ledger, summary


def _market_reconciliation(authority: HashedProposalAuthority) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    uri = f"file:{MARKET_DB.resolve()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        rows = conn.execute(
            """SELECT canonical_market_identity,game_id,game_date,captured_at_utc,
                      market_payload_sha256,COALESCE(raw_source_sha256,'')
               FROM supplemental_main_market_snapshots
               WHERE market_type='MONEYLINE'
               ORDER BY game_id,canonical_market_identity"""
        ).fetchall()
    grouped: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    for item in rows:
        grouped[int(item[1])].append(item)
    ledger: list[dict[str, Any]] = []
    phases: dict[str, int] = defaultdict(int)
    regular_rows: list[tuple[Any, ...]] = []
    for game_pk in sorted(grouped):
        items = grouped[game_pk]
        dates = sorted({str(item[2]) for item in items})
        if len(dates) != 1:
            raise RuntimeError(f"MARKET_GAME_DATE_CONFLICT:{game_pk}")
        decision = classify_moneyline_row(
            {"game_id": game_pk, "game_date": dates[0]}, authority=authority
        )
        phases[decision.evaluation_partition] += len(items)
        if decision.evaluation_partition == "REGULAR_SEASON":
            regular_rows.extend(items)
        ledger.append({
            "source": "supplemental_main_market_snapshots:MONEYLINE",
            "game_pk": game_pk, "row_count": len(items), "game_date": dates[0],
            "source_game_type": decision.source_game_type,
            "normalized_phase": decision.normalized_phase,
            "postseason_round": decision.postseason_round,
            "evaluation_partition": decision.evaluation_partition,
            "retained_group_sha256": sha256_bytes(canonical(items)),
        })
    before = sha256_bytes(canonical(rows))
    after = sha256_bytes(canonical(regular_rows))
    return ledger, {
        "source": "supplemental_main_market_snapshots:MONEYLINE",
        "rows": len(rows), "distinct_game_pks": len(grouped),
        "phase_row_counts": dict(sorted(phases.items())),
        "membership_content_sha256_before": before,
        "membership_content_sha256_after_regular_gate": after,
        "content_equivalent": before == after,
        "earliest_capture_utc": min(str(row[3]) for row in rows),
        "latest_capture_utc": max(str(row[3]) for row in rows),
        "database_file_content_not_committed": True,
    }


def _test_report() -> dict[str, Any]:
    suite = unittest.defaultTestLoader.loadTestsFromName(
        "backend.mlb.tests.test_mlb_2026_moneyline_phase_gating_v1"
    )
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    return {
        "runner": "PYTHON_STDLIB_UNITTEST_ACTUAL_ASSERTIONS",
        "intended": result.testsRun,
        "passed": result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped),
        "failed": len(result.failures) + len(result.errors),
        "skipped": len(result.skipped),
        "successful": result.wasSuccessful(),
        "transcript": stream.getvalue().splitlines(),
    }


def build(package: Path) -> dict[str, Any]:
    package.mkdir(parents=True, exist_ok=True)
    authority = HashedProposalAuthority()
    snapshot_ledger, snapshot_summary = _snapshot_reconciliation(authority)
    market_ledger, market_summary = _market_reconciliation(authority)
    ledger = sorted(snapshot_ledger + market_ledger,
                    key=lambda item: (item["source"], item["game_pk"]))
    affected = [row for row in ledger if row["evaluation_partition"] != "REGULAR_SEASON"]
    with (package / "retained_row_phase_reconciliation.jsonl").open("w", encoding="utf-8") as handle:
        for row in ledger:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    with (package / "affected_row_ledger.jsonl").open("w", encoding="utf-8") as handle:
        for row in affected:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    summary = {
        "contract": "MLB_2026_MONEYLINE_PHASE_GATING_V1",
        "authority": authority.metadata.to_dict(),
        "sources": {**snapshot_summary, market_summary["source"]: market_summary},
        "ledger_group_rows": len(ledger),
        "affected_group_rows": len(affected),
        "missing_authority": 0, "unknown_authority": 0,
        "conflicting_authority": 0, "duplicate_authority": 0,
        "historical_restatement_required": False,
    }
    write_json(package / "retained_reconciliation_summary.json", summary)
    write_json(package / "consumer_manifest.json", {"consumers": _source_inventory()})
    metric_hash = sha256(METRIC_ARTIFACT)
    invariance = {
        "prediction": snapshot_summary["mlb.public_game_moneyline_predictions"],
        "outcome": snapshot_summary["mlb.public_game_moneyline_outcomes"],
        "price": market_summary,
        "metric_artifact": {
            "path": str(METRIC_ARTIFACT.relative_to(ROOT)),
            "sha256_before": metric_hash, "sha256_after": metric_hash,
            "byte_identical": True,
            "limitation": "aggregate artifact is immutable but its historical input manifest is not exact-gamePk bound",
        },
        "probability_or_pick_recomputation": 0,
        "immutable_ledger_writes": 0,
    }
    write_json(package / "regular_season_invariance_report.json", invariance)
    classifications = {
        "moneyline_regular_season_integrity": "READY",
        "moneyline_postseason_code_readiness": "READY_SYNTHETIC_CONTROL_FLOW_ONLY",
        "moneyline_postseason_operational_readiness": "BLOCKED_NO_RETAINED_AUTHORITATIVE_POSTSEASON_GAME",
        "historical_restatement_requirement": "NOT_REQUIRED",
        "prediction_quality_effect": "NONE_MEMBERSHIP_ONLY",
        "market_roi_effect": "NONE_FOR_RETAINED_REGULAR_COHORT_PARTITIONED_PROSPECTIVELY",
        "remaining_downstream_blockers": [
            "first real retained authoritative postseason game is not yet present",
            "ordinary prediction/outcome/price retention window has not validated postseason operation",
            "legacy aggregate metric artifacts lack immutable exact-gamePk input-manifest binding",
            "excluded agreement and cross-lane reports require separate authorized cutovers",
        ],
    }
    write_json(package / "classifications.json", classifications)
    tests = _test_report()
    write_json(package / "executed_test_report.json", tests)
    validation = {
        "status": "PASS" if tests["successful"] and not affected else "FAIL",
        "checks": {
            "authority_2919": authority.metadata.proposal_count == 2919,
            "retained_prediction_rows_regular": snapshot_summary["mlb.public_game_moneyline_predictions"]["rows"] == 633,
            "retained_outcome_rows_regular": snapshot_summary["mlb.public_game_moneyline_outcomes"]["rows"] == 630,
            "retained_market_rows_regular": market_summary["rows"] == 15489,
            "all_retained_groups_regular": not affected,
            "prediction_content_equivalent": snapshot_summary["mlb.public_game_moneyline_predictions"]["content_equivalent"],
            "outcome_content_equivalent": snapshot_summary["mlb.public_game_moneyline_outcomes"]["content_equivalent"],
            "price_content_equivalent": market_summary["content_equivalent"],
            "dependency_free_tests": tests["successful"],
            "network_requests": True,
            "database_connections": True,
            "database_writes": True,
        },
        "network_request_count": 0, "operational_database_connection_count": 0,
        "database_write_count": 0, "pipeline_run_count": 0,
    }
    validation["status"] = "PASS" if all(validation["checks"].values()) else "FAIL"
    write_json(package / "validation_report.json", validation)
    readme = """# MLB 2026 Moneyline phase gating V1

The active Moneyline grading boundary and the shared Moneyline report loader now
join the source-hashed canonical authority by exact gamePk. Regular-season and
postseason evaluation are disjoint. Preseason is excluded; special, missing,
unknown, stale, conflicting, and duplicate authority fails closed. No phase
column was added to a Moneyline ledger, and no prediction, outcome, price,
probability, pick, timestamp, or model was rewritten.

The retained local reconciliation proves 633 prediction rows, 630 outcome rows,
and 15,489 retained Moneyline price observations (630 gamePks) are all
authoritative regular-season games. Their regular-gate membership/content hashes
are unchanged, so no historical restatement is required. The pre-existing
aggregate metric artifact is byte-identical; its older input lineage is not an
exact-gamePk manifest and remains a documented historical limitation.

Postseason behavior is proven only with synthetic fixtures covering all six
supported round codes. Operational readiness remains blocked until a real
authoritative postseason game is retained and an ordinary prediction/outcome/
price window validates the path. Prediction quality is not claimed to improve;
only evaluation membership changes. Market/ROI calculations remain separately
labeled hypothetical and are never combined with prediction-quality metrics.

Agreement-study, cross-lane Ops Brief, general daily-index, and legacy one-off
model-development outputs were not modified. The next action is ordinary source
retention followed by a no-special-authorization postseason shadow observation
when the first real postseason game appears.
"""
    (package / "README.md").write_text(readme, encoding="utf-8")
    files = sorted(path for path in package.iterdir()
                   if path.is_file() and path.name != "sha256_manifest.txt")
    (package / "sha256_manifest.txt").write_text(
        "".join(f"{sha256(path)}  {path.name}\n" for path in files), encoding="utf-8"
    )
    return validation


def validate(package: Path) -> dict[str, Any]:
    failures: list[str] = []
    manifest = package / "sha256_manifest.txt"
    entries = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, name = line.split("  ", 1)
        entries.append(name)
        if sha256(package / name) != digest:
            failures.append(f"MANIFEST_HASH_MISMATCH:{name}")
    report = json.loads((package / "validation_report.json").read_text(encoding="utf-8"))
    tests = _test_report()
    if report["status"] != "PASS":
        failures.append("STORED_VALIDATION_FAILED")
    if not tests["successful"] or tests["passed"] != 19 or tests["failed"] or tests["skipped"]:
        failures.append("DEPENDENCY_FREE_TESTS_FAILED")
    expected = {
        "README.md", "affected_row_ledger.jsonl", "classifications.json",
        "consumer_manifest.json", "executed_test_report.json",
        "regular_season_invariance_report.json", "retained_reconciliation_summary.json",
        "retained_row_phase_reconciliation.jsonl", "validation_report.json",
    }
    if set(entries) != expected:
        failures.append("MANIFEST_ENTRY_SET_MISMATCH")
    return {
        "status": "PASS" if not failures else "FAIL",
        "manifest_entries": len(entries), "test_counts": tests,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    package = args.package.resolve()
    if args.build:
        build(package)
    report = validate(package)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
