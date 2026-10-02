import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.daily_capture import sha256_file
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


SLATE = "2026-10-02"


def write_package_file(directory: Path, name: str, value: dict) -> None:
    (directory / name).write_text(json.dumps(value, sort_keys=True) + "\n")


def finalize_package(directory: Path) -> None:
    files = sorted(path for path in directory.iterdir() if path.is_file())
    (directory / "SHA256SUMS").write_text(
        "".join(f"{sha256_file(path)}  {path.name}\n" for path in files)
    )


def make_daily_package(root: Path, run_id: str, *, slate: str = SLATE,
                       classification: str = "READY", incomplete: bool = False,
                       missing_prediction: str | None = None,
                       completed: str = "2026-10-02T15:00:00Z") -> Path:
    package = root / f"run_id={run_id}"
    package.mkdir(parents=True)
    odds = root / f"odds-{run_id}"
    odds.mkdir()
    write_package_file(odds, "observation.json", {"status": "CAPTURED_NONEMPTY"})
    finalize_package(odds)
    reconciliation = root / f"reconciliation-{run_id}"
    reconciliation.mkdir()
    write_package_file(reconciliation, "RUN_COMPLETE.json", {"status": "COMPLETE"})
    write_package_file(reconciliation, "outcomes.json", {"status": "FINAL"})
    finalize_package(reconciliation)
    lanes = {
        "shared_prerequisites": {"status": "COMPLETE", "outputs": []},
        "roster": {"status": "COMPLETE", "outputs": []},
    }
    for lane, filename in (("legacy_sog", "sog_predictions_wide_calibrated.csv"),
                           ("points", "points_predictions.csv"),
                           ("saves", "saves_predictions.csv")):
        outputs = []
        if filename != missing_prediction:
            artifact = root / run_id / filename
            artifact.parent.mkdir(exist_ok=True)
            artifact.write_text("player_id,game_id,game_date\n1,2,2026-10-02\n")
            row = {"path": str(artifact), "bytes": artifact.stat().st_size,
                   "sha256": sha256_file(artifact)}
            if lane != "legacy_sog":
                row.update(parent_daily_run_id=run_id, canonical_game_set_hash="gamehash",
                           canonical_game_count=1)
            outputs.append(row)
        lanes[lane] = {"status": "COMPLETE", "outputs": outputs}
    receipt = {
        "schema_version": "NHL_COMPREHENSIVE_DAILY_RUN_RECEIPT_V3",
        "parent_daily_run_id": run_id, "slate_date": slate,
        "operational_timezone": "America/New_York", "final_classification": classification,
        "canonical_game_ids": [2], "canonical_game_set_hash": "gamehash", "lanes": lanes,
        "prior_day_learning": {
            "official_outcomes_status": "FINAL",
            "reconciliation_status": "CREATED",
            "reconciliation_package": str(reconciliation),
            "moneyline_grade_status": "COMPLETE",
            "puck_line_grade_status": "COMPLETE",
        },
        "odds_observation": {"classification": "CAPTURED_NONEMPTY", "path": str(odds),
                             "manifest_sha256": sha256_file(odds / "SHA256SUMS")},
    }
    write_package_file(package, "parent_receipt.json", receipt)
    if not incomplete:
        write_package_file(package, "RUN_COMPLETE.json", {
            "schema_version": receipt["schema_version"],
            "parent_daily_run_id": run_id,
            "final_classification": classification,
            "completed_at_utc": completed,
        })
        finalize_package(package)
    return package


class CrossMarketDailyReadinessTest(unittest.TestCase):
    def test_current_day_ready_receipt_passes_and_newest_valid_selected(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            make_daily_package(root, "old", completed="2026-10-02T14:00:00Z")
            make_daily_package(root, "nhldaily_20261002T145032406591Z_be94e137",
                               completed="2026-10-02T18:54:15Z")
            ok, reason = capture.morning_capture_allowed(SLATE, daily_run_root=root)
        self.assertTrue(ok)
        self.assertEqual(reason, "DAILY_READY_RECEIPT:nhldaily_20261002T145032406591Z_be94e137")

    def test_failed_blocking_receipt_fails(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "failed", classification="FAILED_BLOCKING")
            self.assertFalse(capture.morning_capture_allowed(
                SLATE, daily_run_root=Path(raw))[0])

    def test_prior_day_ready_receipt_does_not_satisfy_today(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "yesterday", slate="2026-10-01")
            self.assertFalse(capture.morning_capture_allowed(
                SLATE, daily_run_root=Path(raw))[0])

    def test_current_day_partial_receipt_fails_integrity(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "partial", incomplete=True)
            self.assertFalse(capture.morning_capture_allowed(
                SLATE, daily_run_root=Path(raw))[0])

    def test_et_date_is_authoritative_when_pt_date_differs(self):
        # 04:30 UTC is still Oct 1 in Los Angeles but Oct 2 in New York.
        now = datetime(2026, 10, 2, 4, 30, tzinfo=timezone.utc)
        self.assertEqual(now.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat(),
                         "2026-10-01")
        self.assertEqual(capture.resolve_slate_date("today", now), SLATE)

    def test_legitimate_operator_origin_is_not_a_readiness_gate(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "operator")
            with patch.dict("os.environ", {
                    "PROPPADIA_INVOCATION_ORIGIN": "UNDECLARED"}):
                self.assertTrue(capture.morning_capture_allowed(
                    SLATE, daily_run_root=Path(raw))[0])

    def test_noop_does_not_create_paid_claim_or_advance_auto_phase(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            status = capture.record_morning_not_ready(root, SLATE, "fixture")
            payload = json.loads(status.read_text())
            self.assertEqual(payload["phase"], None)
            self.assertEqual(payload["live_calls"], 0)
            self.assertIsNone(capture.prior_capture_suppression(
                "AUTO", existing=False, prior_claim=False, prior_paid=False, force=False))
            self.assertFalse(list(root.glob("paid_attempt_claims/*/*.claim.json")))

    def test_auto_allowed_eight_hours_early_and_timing_is_diagnostic(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            make_daily_package(root, "ready")
            now = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)
            schedule = pd.DataFrame({"scheduled_start_time_utc": ["2026-10-02T23:00:00Z"]})
            result = capture.auto_capture_readiness_decision(
                SLATE, schedule, now, daily_run_root=root)
        self.assertEqual(result["decision"], "LIVE_CAPTURE_ALLOWED")
        self.assertEqual(result["daily_readiness"], "PASS")
        self.assertEqual(result["prior_day_learning"], "PASS")
        self.assertEqual(result["requested_phase"], "AUTO")
        self.assertEqual(result["capture_window"], "NONBLOCKING")
        self.assertEqual(result["resolved_phase"], "MIDDAY")
        self.assertEqual(result["minutes_until_first_start"], 480.0)
        self.assertEqual(result["first_start_time"], "2026-10-02T23:00:00Z")

    def test_no_canonical_games_remains_blocked(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "ready")
            result = capture.auto_capture_readiness_decision(
                SLATE, pd.DataFrame(columns=["scheduled_start_time_utc"]),
                datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc),
                daily_run_root=Path(raw))
        self.assertEqual(result["decision"], "BLOCKED")
        self.assertEqual(result["gate_reason"], "NO_CANONICAL_2026_GAMES")

    def test_prior_day_reconciliation_manifest_integrity_remains_required(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            package = make_daily_package(root, "ready")
            receipt = json.loads((package / "parent_receipt.json").read_text())
            reconciliation = Path(receipt["prior_day_learning"]["reconciliation_package"])
            (reconciliation / "outcomes.json").write_text("tampered\n")
            schedule = pd.DataFrame({
                "scheduled_start_time_utc": ["2026-10-02T23:00:00Z"]})
            result = capture.auto_capture_readiness_decision(
                SLATE, schedule, datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc),
                daily_run_root=root)
        self.assertEqual(result["decision"], "BLOCKED")
        self.assertEqual(result["prior_day_learning"], "BLOCKED")

    def test_auto_first_capture_then_refresh_is_the_later_observation(self):
        schedule = pd.DataFrame({"scheduled_start_time_utc": ["2026-10-02T23:00:00Z"]})
        now = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)
        first_phase, _ = capture.phase_for(schedule, now, "AUTO", False)
        self.assertEqual(first_phase, "MIDDAY")
        self.assertEqual(capture.prior_capture_suppression(
            first_phase, existing=True, prior_claim=False, prior_paid=False, force=False),
            "NOOP_ALREADY_CAPTURED")
        refresh_phase, _ = capture.phase_for(schedule, now, "REFRESH", False)
        self.assertEqual(refresh_phase, "REFRESH")
        self.assertIsNone(capture.prior_capture_suppression(
            refresh_phase, existing=True, prior_claim=True, prior_paid=True, force=False))

    def test_missing_prediction_artifact_blocks(self):
        with tempfile.TemporaryDirectory() as raw:
            make_daily_package(Path(raw), "missing", missing_prediction="points_predictions.csv")
            self.assertFalse(capture.morning_capture_allowed(
                SLATE, daily_run_root=Path(raw))[0])


if __name__ == "__main__":
    unittest.main()
