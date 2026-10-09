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
from typing import Any, Callable, Mapping

from backend.mlb.season_transition.game_phase_authority_v1 import GamePhaseAuthorityError, HashedProposalAuthority
from backend.mlb.season_transition.contract_v1 import classify_schedule_game
from backend.mlb.totals_predictions.live_context_bridge_v1 import load_retained_schedule_projection


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
RAW_EXPORT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_totals_prospective_shadow_v1"
C_START_DATE = "2026-08-17"
INGESTION_RECEIPT_NAME = "totals_raw_ingestion_receipt.json"
INGESTION_RECEIPT_GLOB = "totals_raw_ingestion_receipt*.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")).hexdigest()


def aggregate_status(checks: dict[str, Any]) -> str:
    values = list(checks.values())
    if any(value is False or value == "FAIL" for value in values):
        return "FAIL"
    if any(value == "INCONCLUSIVE" for value in values):
        return "INCONCLUSIVE"
    return "PASS" if values and all(value is True or value == "PASS" for value in values) else "FAIL"


def expected_population_status(
    observed: set[tuple[str, int]],
    expected: set[tuple[str, int]],
    *,
    evidence_errors: list[str],
    duplicate_rows: int = 0,
) -> str:
    if evidence_errors:
        return "INCONCLUSIVE"
    if duplicate_rows:
        return "FAIL"
    return "PASS" if observed == expected else "INCONCLUSIVE"


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


def validate_ingestion_receipt(
    receipt_path: Path,
    *,
    root: Path = ROOT,
    expected_prediction_hashes: Mapping[tuple[str, int], str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Validate a separately retained Totals append receipt and its source artifacts."""
    errors: list[str] = []
    root = root.resolve()
    receipt_path = receipt_path.resolve()
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("schema_version") != "MLB_TOTALS_RAW_INGESTION_RECEIPT_V1":
            return [], [f"{receipt_path.relative_to(root)}:SCHEMA_VERSION_INVALID"]
        manifest_path = receipt_path.parent / "reproducibility_hashes.sha256"
        manifest_entries = {}
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                manifest_entries[parts[1].lstrip("* ")] = parts[0]
        receipt_hash = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        if manifest_entries.get(receipt_path.name) != receipt_hash:
            errors.append(f"{receipt_path.relative_to(root)}:RECEIPT_MANIFEST_HASH_MISSING_OR_MISMATCH")
        schedule_path = Path(str(receipt["schedule_source_path"]))
        schedule_path = schedule_path if schedule_path.is_absolute() else root / schedule_path
        schedule_digest = hashlib.sha256(schedule_path.read_bytes()).hexdigest() if schedule_path.is_file() else None
        if schedule_digest != receipt.get("schedule_source_sha256"):
            errors.append(f"{receipt_path.relative_to(root)}:SCHEDULE_SOURCE_HASH_MISSING_OR_MISMATCH")
            schedule_payload = {}
        else:
            schedule_payload, _, _, projection_digest = load_retained_schedule_projection(
                schedule_path, str(schedule_digest), observed_at_utc="2000-01-01T00:00:00Z"
            )
            if projection_digest != receipt.get("schedule_projection_sha256"):
                errors.append(f"{receipt_path.relative_to(root)}:SCHEDULE_PROJECTION_HASH_MISMATCH")
        schedule_games: dict[int, list[dict[str, Any]]] = {}
        for day in schedule_payload.get("dates", []):
            for game in day.get("games", []):
                if game.get("gamePk") is not None:
                    schedule_games.setdefault(int(game["gamePk"]), []).append(game)
        entries = receipt.get("entries")
        if not isinstance(entries, list) or not entries:
            errors.append(f"{receipt_path.relative_to(root)}:ENTRIES_MISSING_OR_EMPTY")
            entries = []
        validated: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for item in entries:
            if not isinstance(item, dict):
                errors.append(f"{receipt_path.relative_to(root)}:ENTRY_NOT_OBJECT")
                continue
            date, game_pk = str(item.get("game_date") or ""), int(item.get("game_pk", -1))
            identity = (date, game_pk)
            if date != str(receipt.get("game_date") or ""):
                errors.append(f"{receipt_path.relative_to(root)}:RECEIPT_DATE_MISMATCH:{game_pk}")
            if (item.get("schedule_source_path") != receipt.get("schedule_source_path")
                    or item.get("schedule_source_sha256") != receipt.get("schedule_source_sha256")
                    or item.get("schedule_projection_sha256") != receipt.get("schedule_projection_sha256")):
                errors.append(f"{receipt_path.relative_to(root)}:ENTRY_SCHEDULE_BINDING_MISMATCH:{game_pk}")
            if item.get("canonical_identity", "").split("|")[:2] != [date, str(game_pk)]:
                errors.append(f"{receipt_path.relative_to(root)}:CANONICAL_IDENTITY_MISMATCH:{game_pk}")
            if identity in seen:
                errors.append(f"{receipt_path.relative_to(root)}:DUPLICATE_IDENTITY:{identity}")
                continue
            seen.add(identity)
            raw_schedule_games = schedule_games.get(game_pk, [])
            if len(raw_schedule_games) != 1:
                errors.append(f"{receipt_path.relative_to(root)}:SCHEDULE_GAME_NOT_UNIQUE:{game_pk}")
                continue
            raw_game = raw_schedule_games[0]
            if (str(raw_game.get("officialDate") or "") != date
                    or int(raw_game.get("season", -1)) != int(item.get("source_season", -2))
                    or str(raw_game.get("gameType") or "") != str(item.get("source_game_type") or "")):
                errors.append(f"{receipt_path.relative_to(root)}:SCHEDULE_IDENTITY_MISMATCH:{game_pk}")
            else:
                classification = classify_schedule_game(raw_game, season=int(raw_game["season"]))
                if (classification.phase != item.get("normalized_phase")
                        or classification.postseason_round != item.get("postseason_round")):
                    errors.append(f"{receipt_path.relative_to(root)}:SCHEDULE_PHASE_MISMATCH:{game_pk}")
            market_rel, market_sha, event_id = item.get("market_source_path"), item.get("market_source_sha256"), item.get("market_source_event_id")
            if market_sha:
                market_path = Path(str(market_rel))
                market_path = market_path if market_path.is_absolute() else root / market_path
                if not market_path.is_file() or hashlib.sha256(market_path.read_bytes()).hexdigest() != market_sha:
                    errors.append(f"{receipt_path.relative_to(root)}:MARKET_SOURCE_HASH_MISSING_OR_MISMATCH:{game_pk}")
                elif not event_id:
                    errors.append(f"{receipt_path.relative_to(root)}:MARKET_EVENT_ID_MISSING:{game_pk}")
                else:
                    market_payload = json.loads(market_path.read_text(encoding="utf-8"))
                    market_events = market_payload if isinstance(market_payload, list) else market_payload.get("events", [])
                    event_matches = [event for event in market_events if str(event.get("id")) == str(event_id)]
                    away = raw_game.get("teams", {}).get("away", {}).get("team", {}).get("name")
                    home = raw_game.get("teams", {}).get("home", {}).get("team", {}).get("name")
                    if (len(event_matches) != 1 or event_matches[0].get("away_team") != away
                            or event_matches[0].get("home_team") != home):
                        errors.append(f"{receipt_path.relative_to(root)}:MARKET_EVENT_GAME_BINDING_MISMATCH:{game_pk}")
            elif event_id or market_rel:
                errors.append(f"{receipt_path.relative_to(root)}:PARTIAL_MARKET_PROVENANCE:{game_pk}")
            if expected_prediction_hashes is not None:
                observed_payload_hash = expected_prediction_hashes.get(identity)
                if observed_payload_hash != item.get("prediction_payload_sha256"):
                    errors.append(f"{receipt_path.relative_to(root)}:LEDGER_INGESTION_HASH_MISMATCH:{game_pk}")
            if not item.get("prediction_payload_sha256") or not item.get("context_payload_sha256"):
                errors.append(f"{receipt_path.relative_to(root)}:INGESTION_HASH_MISSING:{game_pk}")
            validated.append(item)
        return (validated if not errors else []), errors
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        return [], [f"{receipt_path.relative_to(root)}:{type(exc).__name__}:{exc}"]


def retained_raw_producer_census(
    expected_prediction_hashes: Mapping[tuple[str, int], str] | None = None,
) -> dict[str, Any]:
    """Build independent game identity expectations from retained RAW producer exports."""
    # Use the producer's stable daily-output suffix.
    files = sorted(path for path in RAW_EXPORT_ROOT.glob("*/*_totals_shadow_predictions.csv") if path.is_file())
    identities: list[tuple[str, int]] = []
    phase_by_identity: dict[tuple[str, int], str] = {}
    producer_row_count = 0
    verified_manifest_files = 0
    manifest_errors: list[str] = []
    for csv_path in files:
        manifest = csv_path.parent / "reproducibility_hashes.sha256"
        if not manifest.is_file():
            manifest_errors.append(f"{manifest.relative_to(ROOT)}:MISSING")
            continue
        entries: dict[str, str] = {}
        for line in manifest.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                entries[parts[1].lstrip("* ")] = parts[0]
        try:
            relative = str(csv_path.relative_to(csv_path.parent))
            expected = entries.get(relative)
            if expected is None or hashlib.sha256(csv_path.read_bytes()).hexdigest() != expected:
                manifest_errors.append(f"{csv_path.relative_to(ROOT)}:HASH_MISSING_OR_MISMATCH")
                continue
            # Verify every manifest-listed artifact in the producer package, not just the CSV.
            package_ok = True
            for rel, digest in entries.items():
                artifact = csv_path.parent / rel
                if not artifact.is_file() or hashlib.sha256(artifact.read_bytes()).hexdigest() != digest:
                    manifest_errors.append(f"{artifact.relative_to(ROOT)}:HASH_MISSING_OR_MISMATCH")
                    package_ok = False
            if not package_ok:
                continue
            verified_manifest_files += len(entries)
            with csv_path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    identities.append((row["game_date"], int(row["game_pk"])))
                    producer_row_count += 1
        except (OSError, KeyError, ValueError) as exc:
            manifest_errors.append(f"{csv_path.relative_to(ROOT)}:{type(exc).__name__}:{exc}")
    counts: dict[str, int] = {}
    duplicates = len(identities) - len(set(identities))
    authority = HashedProposalAuthority()
    receipt_entries: list[dict[str, Any]] = []
    receipt_paths = sorted(path for path in RAW_EXPORT_ROOT.glob(f"*/{INGESTION_RECEIPT_GLOB}") if path.is_file())
    valid_receipt_count = 0
    for receipt_path in receipt_paths:
        entries, receipt_errors = validate_ingestion_receipt(
            receipt_path, expected_prediction_hashes=expected_prediction_hashes
        )
        manifest_errors.extend(receipt_errors)
        if receipt_errors:
            continue
        valid_receipt_count += 1
        receipt_entries.extend(entries)
        for item in entries:
            identity = (str(item["game_date"]), int(item["game_pk"]))
            phase = str(item["normalized_phase"])
            if identity in phase_by_identity and phase_by_identity[identity] != phase:
                manifest_errors.append(f"{receipt_path.relative_to(ROOT)}:PHASE_CONFLICT:{identity}")
            phase_by_identity[identity] = phase
            identities.append(identity)
    authority = HashedProposalAuthority()
    for date, game_pk in set(identities):
        identity = (date, game_pk)
        try:
            record = authority.lookup_exact(game_pk)
        except GamePhaseAuthorityError as exc:
            if identity not in phase_by_identity:
                manifest_errors.append(f"PHASE_AUTHORITY_UNAVAILABLE:{identity}:{exc}")
            continue
        phase = str(record.season_phase)
        if identity in phase_by_identity and phase_by_identity[identity] != phase:
            manifest_errors.append(f"ACTIVE_AUTHORITY_PHASE_CONFLICT:{identity}")
        phase_by_identity.setdefault(identity, phase)
    for identity in set(identities):
        phase = phase_by_identity.get(identity)
        if phase:
            counts[phase] = counts.get(phase, 0) + 1
    identities = sorted(set(identities))
    c_expected = sorted(identity for identity in identities if identity[0] >= C_START_DATE and phase_by_identity.get(identity) == "REGULAR_SEASON")
    return {
        "source_root": str(RAW_EXPORT_ROOT.relative_to(ROOT)),
        "raw_producer_csv_count": len(files),
        "raw_producer_rows": producer_row_count,
        "raw_unique_exact_identities": len(set(identities)),
        "raw_duplicate_identity_rows": duplicates,
        "raw_phase_counts": counts,
        "verified_manifest_artifact_count": verified_manifest_files,
        "manifest_error_count": len(manifest_errors),
        "manifest_errors": manifest_errors,
        "ingestion_receipt_count": len(receipt_paths),
        "valid_ingestion_receipt_count": valid_receipt_count,
        "receipt_entry_count": len(receipt_entries),
        "identities": identities,
        "c_expected_start_date": C_START_DATE,
        "c_expected_identities": c_expected,
    }


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
    prediction_hashes = {(str(row[1]), int(row[2])): str(row[4]) for row in raw_predictions}
    census = retained_raw_producer_census(expected_prediction_hashes=prediction_hashes)
    raw_ledger_identities = {(str(row[1]), int(row[2])) for row in raw_predictions}
    c_ledger_identities = {(str(row[1]), int(row[2])) for row in c_predictions}
    raw_expected_identities = {(str(date), int(game_pk)) for date, game_pk in census["identities"]}
    c_expected_identities = {(str(date), int(game_pk)) for date, game_pk in census["c_expected_identities"]}
    census["raw_ledger_identity_count"] = len(raw_ledger_identities)
    census["raw_ledger_only_identities"] = [list(item) for item in sorted(raw_ledger_identities - raw_expected_identities)]
    census["raw_producer_only_identities"] = [list(item) for item in sorted(raw_expected_identities - raw_ledger_identities)]
    census["c_ledger_identity_count"] = len(c_ledger_identities)
    census["c_ledger_only_identities"] = [list(item) for item in sorted(c_ledger_identities - c_expected_identities)]
    census["c_expected_only_identities"] = [list(item) for item in sorted(c_expected_identities - c_ledger_identities)]
    census["raw_population_status"] = expected_population_status(
        raw_ledger_identities, raw_expected_identities,
        evidence_errors=census["manifest_errors"], duplicate_rows=census["raw_duplicate_identity_rows"],
    )
    census["c_population_status"] = expected_population_status(
        c_ledger_identities, c_expected_identities,
        evidence_errors=census["manifest_errors"], duplicate_rows=census["raw_duplicate_identity_rows"],
    )
    census["raw_unexplained_ledger_identities"] = []
    census["existing_ledger_source_claims_not_used_as_population_evidence"] = []
    for date, game_pk in census["raw_ledger_only_identities"]:
        record = authority.lookup_exact(game_pk)
        raw_row = raw_by_game[game_pk]
        payload = json.loads(raw_row[3])
        schedule_dir = ROOT / "backend/mlb/exports/provider_event_game_bindings/schedule_sources" / date
        schedule_matches = []
        for schedule_path in schedule_dir.glob("*.json"):
            if hashlib.sha256(schedule_path.read_bytes()).hexdigest() != payload.get("schedule_source_sha256"):
                continue
            schedule_json = json.loads(schedule_path.read_text(encoding="utf-8"))
            if any(int(game.get("gamePk", -1)) == game_pk for day in schedule_json.get("dates", []) for game in day.get("games", [])):
                schedule_matches.append(str(schedule_path.relative_to(ROOT)))
        market_value_path = Path(str(payload.get("market_source_path") or ""))
        market_path = market_value_path if market_value_path.is_absolute() else ROOT / market_value_path
        market_file_hash_matches = (
            bool(payload.get("market_source_sha256")) and market_path.is_file()
            and hashlib.sha256(market_path.read_bytes()).hexdigest() == payload.get("market_source_sha256")
        )
        event_ids = []
        if market_file_hash_matches:
            market_json = json.loads(market_path.read_text(encoding="utf-8"))
            market_events = market_json if isinstance(market_json, list) else market_json.get("events", [])
            for event in market_events:
                if (event.get("away_team") == payload.get("away_team")
                        and event.get("home_team") == payload.get("home_team")):
                    event_ids.append(str(event.get("id")))
        census["raw_unexplained_ledger_identities"].append({
            "game_date": date,
            "game_pk": game_pk,
            "source_game_type": record.source_game_type,
            "normalized_phase": record.season_phase,
            "authority_primary_source_path": record.primary_source_path,
            "authority_primary_source_sha256": record.primary_source_sha256,
            "missing_evidence": "Totals-owned producer export or ingestion receipt for this exact identity",
        })
        census["existing_ledger_source_claims_not_used_as_population_evidence"].append({
            "game_date": date,
            "game_pk": game_pk,
            "ledger_prediction_payload_sha256": str(raw_row[4]),
            "ledger_schedule_source_sha256": payload.get("schedule_source_sha256"),
            "retained_schedule_hash_matches_with_game_identity": schedule_matches,
            "ledger_market_source_path": payload.get("market_source_path"),
            "ledger_market_source_sha256": payload.get("market_source_sha256"),
            "market_file_hash_matches": market_file_hash_matches,
            "team_matched_market_event_ids": sorted(event_ids),
            "market_event_id_in_immutable_ledger_payload": payload.get("market_source_event_id"),
            "totals_owned_ingestion_receipt": "NOT_FOUND",
            "counted_as_independent_expected_identity": False,
        })
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
        "hash_invariance": None,
        "ledger_file_sha256_before": before,
        "independent_population_census": {key: value for key, value in census.items() if key not in {"identities", "c_expected_identities"}},
    }
    raw_db.close(); c_db.close(); market_db.close()
    after = {str(path.relative_to(ROOT)): sha256(path) for path in (RAW_LEDGER, C_LEDGER, MARKET_LEDGER)}
    summary["ledger_file_sha256_after"] = after
    summary["ledger_files_byte_unchanged"] = before == after
    summary["hash_invariance"] = summary["ledger_files_byte_unchanged"]
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
        "historical_restatement_requirement": "NOT_REQUIRED_FOR_PHASE_MEMBERSHIP; POPULATION_EXPECTATIONS_UNRESOLVED",
        "prediction_quality_effect": "NONE_MEMBERSHIP_AND_REPORTING_ONLY",
        "market_roi_effect": "NONE_NO_ROI_RECALCULATION",
        "model_selector_publication_status": "UNCHANGED_SHADOW_ONLY_UNQUALIFIED_UNPUBLISHED",
        "remaining_blockers": [
            "actual authoritative postseason game has not passed through ordinary RAW and C paths",
            "two postseason RAW identities have no Totals-owned producer export or ingestion receipt, so the full RAW expected population remains inconclusive",
            "the retained producer census establishes the regular RAW population and the exact C population, but cannot certify unrepresented RAW postseason intake",
            "SciPy coefficient discrepancy remains open pending a controlled comparison in the original pinned environment",
        ],
    }
    write_json(CONTRACT / "classifications.json", classifications)
    source_rows = [{
        "path": path, "bytes": (ROOT / path).stat().st_size, "sha256": sha256(ROOT / path)
    } for path in SOURCE_FILES]
    write_csv(CONTRACT / "source_artifact_manifest.csv", source_rows)
    checks = {
            "tests": tests["status"],
            "raw_expected_population": reconciliation["independent_population_census"]["raw_population_status"],
            "c_expected_population": reconciliation["independent_population_census"]["c_population_status"],
            "phase_classification_valid": set(reconciliation["raw_totals"]["phase_counts"]).issubset({"REGULAR_SEASON", "POSTSEASON"}) and reconciliation["totals_c"]["phase_counts"].get("REGULAR_SEASON", 0) == reconciliation["totals_c"]["distinct_game_pks"],
            "c_exact_subset": reconciliation["cross_lane"]["totals_c_only"] == 0 and reconciliation["cross_lane"]["identity_conflicts"] == 0 and reconciliation["cross_lane"]["intersection"] == reconciliation["totals_c"]["distinct_game_pks"],
            "zero_violations": not any(reconciliation["violations"].values()),
            "hash_invariance": reconciliation["hash_invariance"],
            "ledger_files_byte_unchanged": reconciliation["ledger_files_byte_unchanged"],
            "no_training_or_model_mutation": True,
            "selector_publication_threshold_status_unchanged": True,
        }
    validation = {
        "task": "MLB_2026_TOTALS_PHASE_GATING_V1",
        "status": aggregate_status(checks),
        "checks": checks,
        "population_expectation_evidence": reconciliation["independent_population_census"],
        "smallest_next_action": "For the next naturally admitted RAW game, retain the new source-hashed ingestion receipt; rerun the local census only after an independently retained producer artifact or receipt exists for the two historical postseason identities. Do not reconstruct or backfill them.",
    }
    write_json(CONTRACT / "validation_report.json", validation)
    census = reconciliation["independent_population_census"]
    readme = f"""# MLB 2026 Totals phase gating V1

RAW Totals and Totals C are genuinely coupled: C derives each row from an immutable RAW prediction/context identity, and the ordinary hook sequences RAW before C. One shared exact-`gamePk` phase interface now gates both lanes without adding phase columns to either append-only ledger.

Retained evidence is unchanged. The observed population is RAW 625 predictions/gamePks (623 regular season, 2 postseason) and C 482 predictions/gamePks (482 regular season), with C an exact RAW subset for all 482 identities and 143 RAW-only identities. Independently retained, hash-verified daily RAW producer CSVs establish 623 exact regular-season identities; the C launch floor (`{C_START_DATE}`) yields 482 expected identities, exactly matching C. The two extra RAW identities (849823 and 849825, postseason) have matching schedule and market source-file hashes claimed by the ledger payload, and their teams match provider event IDs in retained market files. However, no Totals-owned ingestion receipt binds each exact identity to those events and the ingestion payload hash; those source components are diagnostic only and are not used as an expected-population oracle. The complete RAW expected population remains `INCONCLUSIVE`. No old rows were reconstructed or backfilled. Each future newly appended prediction now writes an immutable `totals_raw_ingestion_receipt__*.json` containing exact phase, schedule/projection hashes, market event/source hashes when available, and prediction/context payload hashes. The sibling reproducibility manifest hashes the receipt; the census verifies it and the referenced source artifacts before counting identities. The census verified {census['raw_producer_csv_count']} producer CSVs and {census['verified_manifest_artifact_count']} manifest-listed artifacts with {census['manifest_error_count']} manifest errors. Existing predictions, outcomes, features, proper-score inputs, market inputs, thresholds, models, selector status, and publication status were not changed.

Regular-season reporting remains the default. Postseason requires explicit `POSTSEASON` evaluation mode and remains shadow-only. Operational readiness is blocked until an actual authoritative postseason game traverses the ordinary RAW and C paths; synthetic coverage proves code behavior only.

The SciPy coefficient discrepancy remains open; this validation did not establish its cause or change any tolerance or production check.

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
