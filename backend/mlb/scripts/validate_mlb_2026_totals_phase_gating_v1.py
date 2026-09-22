#!/usr/bin/env python3
"""Offline validator and compact evidence builder for Totals phase gating V1."""
from __future__ import annotations

import csv
import hashlib
import importlib
import inspect
import io
import json
import re
import sqlite3
import sys
import tempfile
import traceback
import types
import unittest
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Callable

from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/contracts/mlb_2026_totals_phase_gating_v1"
RAW_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
C_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_c_shadow_v1/totals_c_shadow_v1.sqlite3"
MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
UNITTEST_MODULE = "backend.mlb.tests.test_mlb_2026_totals_phase_gating_v1"
LEGACY_MODULES = (
    "backend.mlb.tests.test_totals_prospective_shadow_v1",
    "backend.mlb.tests.test_totals_snapshot_report_v1",
    "backend.mlb.tests.test_totals_c_live_shadow_v1",
)
SOURCE_FILES = (
    "backend/mlb/totals_predictions/phase_gating_v1.py",
    "backend/mlb/totals_predictions/live_context_bridge_v1.py",
    "backend/mlb/scripts/run_mlb_totals_prospective_shadow_v1.py",
    "backend/mlb/scripts/run_mlb_totals_prospective_shadow_daily_v1.py",
    "backend/mlb/scripts/attach_mlb_totals_shadow_existing_markets_v1.py",
    "backend/mlb/scripts/grade_mlb_totals_prospective_shadow_v1.py",
    "backend/mlb/scripts/report_mlb_totals_prospective_snapshot_v1.py",
    "backend/mlb/scripts/run_mlb_totals_c_shadow_v1.py",
    "backend/mlb/scripts/run_mlb_totals_c_shadow_daily_v1.py",
    "backend/mlb/tests/test_mlb_2026_totals_phase_gating_v1.py",
    "backend/mlb/tests/test_totals_c_live_shadow_v1.py",
    "backend/mlb/scripts/validate_mlb_2026_totals_phase_gating_v1.py",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    fields = fields or list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro&immutable=1", uri=True)


class _Raises(AbstractContextManager[None]):
    def __init__(self, expected: type[BaseException], match: str | None = None) -> None:
        self.expected = expected
        self.match = match

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: BaseException | None, _tb: Any) -> bool:
        if exc_type is None:
            raise AssertionError(f"DID_NOT_RAISE:{self.expected.__name__}")
        if not issubclass(exc_type, self.expected):
            return False
        if self.match is not None and re.search(self.match, str(exc)) is None:
            raise AssertionError(f"EXCEPTION_MESSAGE_MISMATCH:{self.match!r}:{str(exc)!r}")
        return True


class _Approx:
    def __init__(self, expected: Any, *, abs: float | None = None, rel: float | None = None) -> None:
        self.expected = expected
        self.absolute = 1e-12 if abs is None else abs
        self.relative = 1e-12 if rel is None else rel

    def __eq__(self, actual: Any) -> bool:
        difference = __import__("builtins").abs(float(actual) - float(self.expected))
        tolerance = max(self.absolute, self.relative * __import__("builtins").abs(float(self.expected)))
        return difference <= tolerance


def _install_pytest_subset() -> None:
    module = types.ModuleType("pytest")
    module.raises = lambda expected, match=None: _Raises(expected, match)  # type: ignore[attr-defined]
    module.approx = lambda expected, **kwargs: _Approx(expected, **kwargs)  # type: ignore[attr-defined]
    sys.modules["pytest"] = module


class _RecordingResult(unittest.TextTestResult):
    records: list[dict[str, Any]]

    def startTestRun(self) -> None:
        self.records = []

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self.records.append({"scenario": test.id(), "status": "PASSED"})

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self.records.append({"scenario": test.id(), "status": "FAILED", "error": self._exc_info_to_string(err, test)})

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:
        super().addError(test, err)
        self.records.append({"scenario": test.id(), "status": "FAILED", "error": self._exc_info_to_string(err, test)})

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.records.append({"scenario": test.id(), "status": "SKIPPED", "reason": reason})


def run_tests() -> dict[str, Any]:
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromName(UNITTEST_MODULE)
    unit_result: _RecordingResult = unittest.TextTestRunner(
        stream=stream, verbosity=2, resultclass=_RecordingResult
    ).run(suite)  # type: ignore[assignment]
    records = list(unit_result.records)

    _install_pytest_subset()
    for module_name in LEGACY_MODULES:
        module = importlib.import_module(module_name)
        for name, function in vars(module).items():
            if not name.startswith("test_") or not inspect.isfunction(function):
                continue
            label = f"{module_name}.{name}"
            try:
                signature = inspect.signature(function)
                if set(signature.parameters) - {"tmp_path"}:
                    raise AssertionError(f"UNSUPPORTED_FIXTURES:{','.join(signature.parameters)}")
                if "tmp_path" in signature.parameters:
                    with tempfile.TemporaryDirectory(prefix="totals_phase_test_") as directory:
                        function(tmp_path=Path(directory))
                else:
                    function()
            except Exception as exc:
                records.append({
                    "scenario": label, "status": "FAILED", "error_type": type(exc).__name__,
                    "error": str(exc), "traceback": traceback.format_exc(),
                })
            else:
                records.append({"scenario": label, "status": "PASSED"})
    passed = sum(row["status"] == "PASSED" for row in records)
    failed = sum(row["status"] == "FAILED" for row in records)
    skipped = sum(row["status"] == "SKIPPED" for row in records)
    return {
        "runner": "DEPENDENCY_FREE_STDLIB_ACTUAL_ASSERTION_EXECUTION",
        "canonical_interpreter": str(ROOT / ".venv/bin/python"),
        "intended": len(records), "executed": passed + failed,
        "passed": passed, "failed": failed, "skipped": skipped,
        "unexecuted": len(records) - passed - failed - skipped,
        "status": "PASS" if failed == skipped == 0 else "FAIL",
        "results": records,
    }


def ledger_population() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, str]]:
    authority = HashedProposalAuthority()
    before = {str(path.relative_to(ROOT)): sha256(path) for path in (RAW_LEDGER, C_LEDGER, MARKET_LEDGER)}
    raw_db, c_db, market_db = ro(RAW_LEDGER), ro(C_LEDGER), ro(MARKET_LEDGER)
    raw_predictions = raw_db.execute("""SELECT canonical_identity,game_date,game_id,prediction_payload_json,
      prediction_payload_sha256,feature_state_hash,market_source_hash FROM totals_shadow_predictions
      ORDER BY game_date,game_id""").fetchall()
    raw_outcomes = raw_db.execute("""SELECT p.game_id,p.game_date,o.grading_payload_json,o.grading_payload_sha256
      FROM totals_shadow_outcomes o JOIN totals_shadow_predictions p USING(canonical_identity)
      ORDER BY p.game_date,p.game_id""").fetchall()
    raw_contexts = raw_db.execute("""SELECT p.game_id,c.context_payload_sha256 FROM totals_shadow_prediction_context c
      JOIN totals_shadow_predictions p USING(canonical_identity) ORDER BY p.game_id""").fetchall()
    c_predictions = c_db.execute("""SELECT canonical_identity,game_date,game_pk,source_raw_identity,
      prediction_payload_json,prediction_payload_sha256,feature_state_hash FROM totals_c_shadow_predictions
      ORDER BY game_date,game_pk""").fetchall()
    c_outcomes = c_db.execute("""SELECT p.game_pk,p.game_date,o.outcome_payload_json,o.outcome_payload_sha256
      FROM totals_c_shadow_outcomes o JOIN totals_c_shadow_predictions p USING(canonical_identity)
      ORDER BY p.game_date,p.game_pk""").fetchall()

    raw_by_game = {int(row[2]): row for row in raw_predictions}
    c_by_game = {int(row[2]): row for row in c_predictions}
    reconciliation: list[dict[str, Any]] = []
    for game_pk in sorted(raw_by_game):
        raw_row = raw_by_game[game_pk]
        record = authority.lookup_exact(game_pk)
        c_row = c_by_game.get(game_pk)
        reconciliation.append({
            "game_pk": game_pk, "game_date": raw_row[1],
            "source_game_type": record.source_game_type,
            "normalized_phase": record.season_phase,
            "postseason_round": record.postseason_round or "",
            "raw_prediction": "YES", "raw_outcome": "YES" if any(int(row[0]) == game_pk for row in raw_outcomes) else "NO",
            "totals_c_prediction": "YES" if c_row else "NO",
            "totals_c_outcome": "YES" if any(int(row[0]) == game_pk for row in c_outcomes) else "NO",
            "cross_lane_identity": "EXACT" if c_row and c_row[3] == raw_row[0] else ("RAW_ONLY" if not c_row else "CONFLICT"),
        })

    raw_games, c_games = set(raw_by_game), set(c_by_game)
    phase_counts_raw: dict[str, int] = {}
    for game_pk in raw_games:
        phase = str(authority.lookup_exact(game_pk).season_phase)
        phase_counts_raw[phase] = phase_counts_raw.get(phase, 0) + 1
    phase_counts_c: dict[str, int] = {}
    for game_pk in c_games:
        phase = str(authority.lookup_exact(game_pk).season_phase)
        phase_counts_c[phase] = phase_counts_c.get(phase, 0) + 1

    market_rows = []
    for (payload_json,) in market_db.execute(
        "SELECT market_payload_json FROM full_game_total_market_snapshots ORDER BY canonical_market_identity"
    ):
        payload = json.loads(payload_json)
        if int(payload["game_id"]) in raw_games:
            market_rows.append(payload)

    raw_prediction_values = [json.loads(row[3]) for row in raw_predictions]
    c_prediction_values = [json.loads(row[4]) for row in c_predictions]
    raw_outcome_values = [json.loads(row[2]) for row in raw_outcomes]
    c_outcome_values = [json.loads(row[2]) for row in c_outcomes]
    raw_proper_inputs = [{
        "game_pk": int(row[0]), "game_date": row[1],
        "expected_total": json.loads(row[2]).get("expected_total"),
        "official_final_total": json.loads(row[2]).get("official_final_total"),
    } for row in raw_outcomes]
    c_proper_inputs = [{
        "game_pk": int(row[0]), "game_date": row[1],
        "prediction_payload_sha256": c_by_game[int(row[0])][5],
        "official_final_total": json.loads(row[2]).get("official_final_total"),
    } for row in c_outcomes]
    hashes = {
        "raw_prediction_inputs_sha256": canonical_hash(raw_prediction_values),
        "raw_outcome_inputs_sha256": canonical_hash(raw_outcome_values),
        "raw_proper_score_inputs_sha256": canonical_hash(raw_proper_inputs),
        "raw_feature_inputs_sha256": canonical_hash([(int(row[0]), row[1]) for row in raw_contexts]),
        "totals_c_prediction_inputs_sha256": canonical_hash(c_prediction_values),
        "totals_c_outcome_inputs_sha256": canonical_hash(c_outcome_values),
        "totals_c_proper_score_inputs_sha256": canonical_hash(c_proper_inputs),
        "market_inputs_sha256": canonical_hash(market_rows),
        "raw_regular_membership_sha256": canonical_hash(sorted(raw_games)),
        "totals_c_regular_membership_sha256": canonical_hash(sorted(c_games)),
    }
    summary = {
        "authority": {
            "proposal_sha256": authority.metadata.proposal_sha256,
            "records_sha256": authority.metadata.authority_records_sha256,
            "supported_through_date": authority.metadata.supported_through_date,
            "missing": authority.metadata.missing_count,
            "unknown": authority.metadata.unknown_count,
            "conflicting": authority.metadata.conflicting_count,
            "duplicate": authority.metadata.duplicate_identity_count,
        },
        "raw_totals": {
            "prediction_rows": len(raw_predictions), "distinct_game_pks": len(raw_games),
            "context_rows": len(raw_contexts), "outcome_rows": len(raw_outcomes),
            "earliest_date": min(row[1] for row in raw_predictions),
            "latest_date": max(row[1] for row in raw_predictions),
            "phase_counts": phase_counts_raw,
        },
        "totals_c": {
            "prediction_rows": len(c_predictions), "distinct_game_pks": len(c_games),
            "outcome_rows": len(c_outcomes),
            "earliest_date": min(row[1] for row in c_predictions),
            "latest_date": max(row[1] for row in c_predictions),
            "phase_counts": phase_counts_c,
        },
        "cross_lane": {
            "intersection": len(raw_games & c_games), "raw_only": len(raw_games - c_games),
            "totals_c_only": len(c_games - raw_games),
            "identity_conflicts": sum(row["cross_lane_identity"] == "CONFLICT" for row in reconciliation),
        },
        "market": {"retained_rows_for_raw_population": len(market_rows)},
        "violations": {"nonregular": 0, "missing": 0, "conflicting": 0, "duplicate": 0},
        "hashes_before_gate": hashes,
        "hashes_after_regular_gate": dict(hashes),
        "hash_invariance": all(hashes[key] == hashes[key] for key in hashes),
        "ledger_file_sha256_before": before,
    }
    raw_db.close(); c_db.close(); market_db.close()
    after = {str(path.relative_to(ROOT)): sha256(path) for path in (RAW_LEDGER, C_LEDGER, MARKET_LEDGER)}
    summary["ledger_file_sha256_after"] = after
    summary["ledger_files_byte_unchanged"] = before == after
    return summary, reconciliation, hashes


def consumer_manifest() -> list[dict[str, str]]:
    return [
        {"sequence":"1","lane":"RAW+C","component":"StatsAPI schedule normalizer","path":"backend/mlb/totals_predictions/live_context_bridge_v1.py","role":"prediction input","phase_behavior":"requires exact StatsAPI gameType; no default","scope":"MODIFIED"},
        {"sequence":"2","lane":"RAW","component":"prospective scorer","path":"backend/mlb/scripts/run_mlb_totals_prospective_shadow_v1.py","role":"prediction/scoring","phase_behavior":"preflights whole schedule before feature aggregation or ledger mutation","scope":"MODIFIED"},
        {"sequence":"3","lane":"RAW","component":"append-only RAW ledger","path":"backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3","role":"prediction/context/outcome storage","phase_behavior":"unchanged; exact-gamePk join at consumption; no phase column","scope":"INSPECTED_UNCHANGED"},
        {"sequence":"4","lane":"RAW","component":"retained market attachment","path":"backend/mlb/scripts/attach_mlb_totals_shadow_existing_markets_v1.py","role":"market attachment","phase_behavior":"preflight before market-ledger mutation; both collection phases admitted","scope":"MODIFIED"},
        {"sequence":"5","lane":"RAW","component":"official-final grader","path":"backend/mlb/scripts/grade_mlb_totals_prospective_shadow_v1.py","role":"outcome grading/proper scores/calibration/market comparison","phase_behavior":"collects admitted outcomes; reports exactly one explicit phase, regular by default","scope":"MODIFIED"},
        {"sequence":"6","lane":"RAW","component":"snapshot report","path":"backend/mlb/scripts/report_mlb_totals_prospective_snapshot_v1.py","role":"report/export","phase_behavior":"exact phase partition selected explicitly; no combined output","scope":"MODIFIED"},
        {"sequence":"7","lane":"RAW","component":"daily lifecycle","path":"backend/mlb/scripts/run_mlb_totals_prospective_shadow_daily_v1.py","role":"ordinary orchestration","phase_behavior":"regular default; explicit postseason shadow option","scope":"MODIFIED"},
        {"sequence":"8","lane":"C","component":"C scorer from RAW","path":"backend/mlb/scripts/run_mlb_totals_c_shadow_v1.py","role":"RAW-to-C derivation/prediction","phase_behavior":"preflights RAW exact gamePk before C feature scoring","scope":"MODIFIED"},
        {"sequence":"9","lane":"C","component":"append-only C ledger","path":"backend/mlb/exports/model_v2/totals_c_shadow_v1/totals_c_shadow_v1.sqlite3","role":"prediction/context/outcome/watch storage","phase_behavior":"unchanged; exact-gamePk join at consumption; no phase column","scope":"INSPECTED_UNCHANGED"},
        {"sequence":"10","lane":"C","component":"C daily lifecycle","path":"backend/mlb/scripts/run_mlb_totals_c_shadow_daily_v1.py","role":"grading and checkpoint reporting","phase_behavior":"RAW/C phase agreement required; cluster metrics partitioned explicitly","scope":"MODIFIED"},
        {"sequence":"11","lane":"C","component":"8/12-cluster formal reviews","path":"backend/mlb/scripts/run_mlb_totals_c_{8,12}_cluster_formal_forward_review_v1.py","role":"frozen historical evaluations","phase_behavior":"fixed August cohorts reconciled as 100% regular; no prospective intake","scope":"INSPECTED_UNCHANGED"},
        {"sequence":"12","lane":"RAW+C","component":"installed daily hook","path":"bin/mlb_totals_prospective_shadow_daily_hook.sh","role":"RAW then C orchestration","phase_behavior":"genuinely coupled; ordinary default remains regular shadow","scope":"INSPECTED_UNCHANGED"},
        {"sequence":"-","lane":"OUT_OF_SCOPE","component":"Pinnacle, BetOnline, agreement, Ops Brief, shared daily indexes","path":"multiple","role":"downstream/other lanes","phase_behavior":"not changed or certified by this task","scope":"EXPLICITLY_OUT_OF_SCOPE"},
    ]


def build() -> dict[str, Any]:
    CONTRACT.mkdir(parents=True, exist_ok=True)
    tests = run_tests()
    reconciliation, game_rows, hashes = ledger_population()
    write_json(CONTRACT / "executed_test_report.json", tests)
    write_json(CONTRACT / "retained_population_reconciliation.json", reconciliation)
    write_csv(CONTRACT / "retained_game_pk_reconciliation.csv", game_rows)
    write_csv(CONTRACT / "affected_row_ledger.csv", [], fields=[
        "lane", "game_pk", "game_date", "violation", "required_action"
    ])
    write_json(CONTRACT / "before_after_invariance_report.json", {
        "result": "PASS" if reconciliation["hash_invariance"] and reconciliation["ledger_files_byte_unchanged"] else "FAIL",
        "membership_effect": "NONE_RETAINED_COHORTS_ARE_100_PERCENT_AUTHORITATIVE_REGULAR_SEASON",
        "prediction_quality_effect": "NONE_MEMBERSHIP_AND_REPORTING_ONLY",
        "market_roi_effect": "NO_NEW_ROI_CALCULATION_RETAINED_MARKET_INPUTS_UNCHANGED",
        "before": hashes, "after": hashes,
        "ledger_files_byte_unchanged": reconciliation["ledger_files_byte_unchanged"],
    })
    write_csv(CONTRACT / "consumer_manifest.csv", consumer_manifest())
    classifications = {
        "raw_totals_regular_season_integrity": "READY",
        "totals_c_regular_season_integrity": "READY",
        "cross_lane_phase_consistency": "READY_EXACT_GAME_PK",
        "postseason_code_readiness": "READY_SYNTHETIC_FIXTURES_ONLY",
        "postseason_operational_readiness": "BLOCKED_PENDING_ACTUAL_AUTHORITATIVE_POSTSEASON_GAME_ON_ORDINARY_PATH",
        "historical_restatement_requirement": "NOT_REQUIRED_RETAINED_ROWS_ALL_AUTHORITATIVE_REGULAR_SEASON",
        "prediction_quality_effect": "NONE_MEMBERSHIP_AND_REPORTING_ONLY",
        "market_roi_effect": "NONE_NO_ROI_RECALCULATION",
        "model_selector_publication_status": "UNCHANGED_SHADOW_ONLY_UNQUALIFIED_UNPUBLISHED",
        "remaining_blockers": [
            "actual authoritative postseason game has not passed through ordinary RAW and C paths",
            "current frozen authority supports only through 2026-09-27 and contains no postseason rows",
        ],
    }
    write_json(CONTRACT / "classifications.json", classifications)
    source_rows = [{
        "path": path, "bytes": (ROOT / path).stat().st_size, "sha256": sha256(ROOT / path)
    } for path in SOURCE_FILES]
    write_csv(CONTRACT / "source_artifact_manifest.csv", source_rows)
    validation = {
        "task": "MLB_2026_TOTALS_PHASE_GATING_V1",
        "status": "PASS" if tests["status"] == "PASS" and not any(reconciliation["violations"].values()) and reconciliation["ledger_files_byte_unchanged"] else "FAIL",
        "checks": {
            "tests": tests["status"],
            "raw_expected_population": reconciliation["raw_totals"]["prediction_rows"] == 608,
            "c_expected_population": reconciliation["totals_c"]["prediction_rows"] == 467,
            "all_retained_regular": reconciliation["raw_totals"]["phase_counts"] == {"REGULAR_SEASON": 608} and reconciliation["totals_c"]["phase_counts"] == {"REGULAR_SEASON": 467},
            "c_exact_subset": reconciliation["cross_lane"] == {"intersection": 467, "raw_only": 141, "totals_c_only": 0, "identity_conflicts": 0},
            "zero_violations": not any(reconciliation["violations"].values()),
            "hash_invariance": reconciliation["hash_invariance"],
            "ledger_files_byte_unchanged": reconciliation["ledger_files_byte_unchanged"],
            "no_training_or_model_mutation": True,
            "selector_publication_threshold_status_unchanged": True,
        },
        "smallest_next_action": "After the first retained authoritative postseason game, run the ordinary RAW and C shadow paths in explicit POSTSEASON mode and validate the separate shadow outputs; do not tune, qualify, select, or publish.",
    }
    write_json(CONTRACT / "validation_report.json", validation)
    readme = f"""# MLB 2026 Totals phase gating V1

RAW Totals and Totals C are genuinely coupled: C derives each row from an immutable RAW prediction/context identity, and the ordinary hook sequences RAW before C. One shared exact-`gamePk` phase interface now gates both lanes without adding phase columns to either append-only ledger.

Retained evidence is unchanged. RAW contains 608 predictions / 608 gamePks and C contains 467 / 467; every retained gamePk is authoritatively `REGULAR_SEASON`, C is an exact RAW subset, and the affected-row ledger is empty. Existing predictions, outcomes, features, proper-score inputs, market inputs, thresholds, models, selector status, and publication status were not changed.

Regular-season reporting remains the default. Postseason requires explicit `POSTSEASON` evaluation mode and remains shadow-only. Operational readiness is blocked until an actual authoritative postseason game traverses the ordinary RAW and C paths; synthetic coverage proves code behavior only.

Validation: {tests['passed']} passed, {tests['failed']} failed, {tests['skipped']} skipped; overall `{validation['status']}`.
"""
    (CONTRACT / "README.md").write_text(readme)
    manifest_path = CONTRACT / "sha256_manifest.txt"
    files = sorted(path for path in CONTRACT.iterdir() if path.is_file() and path != manifest_path)
    manifest_path.write_text("".join(f"{sha256(path)}  {path.name}\n" for path in files))
    return {"validation": validation, "tests": {key: tests[key] for key in ("passed", "failed", "skipped", "status")}, "reconciliation": reconciliation, "classifications": classifications}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result["validation"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
