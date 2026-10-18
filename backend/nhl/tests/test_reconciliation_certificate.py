from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.reconciliation_certificate import (
    RosterObservationNotReady,
    require_prior_day_certificate,
    require_slate_roster_observation,
    verify_reconciliation_certificate,
)


def _write_package(root: Path, *, slate: str = "2026-10-17",
                   final: bool = True, outcome_date: str | None = None) -> Path:
    package = root / slate / "reconciliation=test"
    package.mkdir(parents=True)
    outcome_date = outcome_date or slate
    (package / "canonical_admitted_slate.csv").write_text(
        "canonical_season,slate_date,game_id\n2026," + slate + ",2026020001\n"
    )
    (package / "canonical_game_outcomes.csv").write_text(
        "canonical_season,slate_date,game_id,official_final,"
        "official_final_home_goals,official_final_away_goals,outcome_source,"
        "outcome_source_timestamp_utc\n2026," + outcome_date + ",2026020001,"
        + ("True" if final else "False") + ",2,1,NHL_OFFICIAL_GAMECENTER,2026-10-18T01:00:00Z\n"
    )
    for name in ("graded_moneyline.csv", "graded_puck_line.csv"):
        (package / name).write_text("game_id\n2026020001\n")
    (package / "summary.json").write_text(json.dumps({
        "status": "COMPLETE", "slate_date": slate, "games": 1,
    }))
    (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
    names = sorted(path.name for path in package.iterdir()
                   if path.name != "RUN_COMPLETE.json")
    (package / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256((package / name).read_bytes()).hexdigest()}  {name}\n"
        for name in names
    ))
    return package


class ReconciliationCertificateTests(unittest.TestCase):
    def test_complete_final_exact_game_set_is_certified(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = _write_package(root)
            result = verify_reconciliation_certificate(package, "2026-10-17")
        self.assertEqual(result["status"], "CERTIFIED_FINAL")
        self.assertEqual(result["game_ids"], [2026020001])

    def test_not_final_outcome_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            package = _write_package(Path(temp), final=False)
            with self.assertRaisesRegex(RuntimeError, "OUTCOMES_NOT_FINAL"):
                verify_reconciliation_certificate(package, "2026-10-17")

    def test_wrong_date_outcome_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            package = _write_package(Path(temp), outcome_date="2026-10-16")
            with self.assertRaisesRegex(RuntimeError, "SLATE_MISMATCH"):
                verify_reconciliation_certificate(package, "2026-10-17")

    def test_hash_corruption_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            package = _write_package(Path(temp))
            with (package / "canonical_game_outcomes.csv").open("a") as output:
                output.write("corruption\n")
            with self.assertRaisesRegex(RuntimeError, "HASH_MISMATCH"):
                verify_reconciliation_certificate(package, "2026-10-17")

    def test_partial_game_coverage_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            package = _write_package(Path(temp))
            (package / "canonical_admitted_slate.csv").write_text(
                "canonical_season,slate_date,game_id\n2026,2026-10-17,2026020001\n"
                "2026,2026-10-17,2026020002\n"
            )
            manifest_names = [line.split("  ", 1)[1]
                              for line in (package / "SHA256SUMS").read_text().splitlines()]
            (package / "SHA256SUMS").write_text("".join(
                f"{hashlib.sha256((package / name).read_bytes()).hexdigest()}  {name}\n"
                for name in manifest_names
            ))
            with self.assertRaisesRegex(RuntimeError, "GAME_COVERAGE_MISMATCH"):
                verify_reconciliation_certificate(package, "2026-10-17")

    def test_missing_package_and_nonempty_schedule_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "CERTIFICATE_REQUIRED"):
                require_prior_day_certificate("2026-10-17", reconciliation_root=Path(temp))

    def test_verified_empty_official_schedule_allows_no_prior_games(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            day = root / "slates" / "2026-10-17"
            day.mkdir(parents=True)
            raw = day / "raw_schedule_response.json"
            raw.write_text(json.dumps({"gameWeek": [{"date": "2026-10-17", "games": []}]}))
            (day / "slate_health.json").write_text(json.dumps({
                "slate_date": "2026-10-17", "completion_status": "VALID_EMPTY_SLATE",
                "downstream_ready": True,
                "source": "https://api-web.nhle.com/v1/schedule/2026-10-17",
                "raw_source_hash": hashlib.sha256(raw.read_bytes()).hexdigest(),
                "normalized_game_count": 0,
            }))
            result = require_prior_day_certificate(
                "2026-10-17", reconciliation_root=root / "reconciliation",
                slate_root=root / "slates",
            )
        self.assertEqual(result["status"], "VERIFIED_EMPTY_SCHEDULE")

    def test_current_slate_roster_requires_verified_immutable_observation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            slate = "2026-10-18"
            roster_root = root / "roster"
            observation = roster_root / "season=2026" / f"slate_date={slate}" / "observation=run1"
            observation.mkdir(parents=True)
            (observation / "observation_summary.json").write_text(json.dumps({"phase": "EARLY"}))
            verified = {"path": str(observation), "manifest_sha256": "roster-manifest",
                        "observation_timestamp_utc": "2026-10-18T14:00:00Z"}
            with patch(
                "backend.nhl.daily_capture.load_canonical_slate",
                return_value=[type("Game", (), {"game_id": 2026020001})()],
            ), patch(
                "backend.nhl.daily_orchestration.verify_roster_observation_reuse",
                return_value=verified,
            ) as verify:
                result = require_slate_roster_observation(
                    slate, slate_root=root / "slates", roster_root=roster_root)
        verify.assert_called_once()
        self.assertEqual(result["status"], "VERIFIED_CURRENT_SLATE_ROSTER")
        self.assertEqual(result["package_manifest_sha256"], "roster-manifest")

    def test_current_slate_roster_missing_blocks_nonempty_slate(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "backend.nhl.daily_capture.load_canonical_slate",
            return_value=[type("Game", (), {"game_id": 2026020001})()],
        ):
            with self.assertRaisesRegex(RosterObservationNotReady,
                                        "ROSTER_OBSERVATION_MISSING"):
                require_slate_roster_observation(
                    "2026-10-18", slate_root=Path(temp) / "slates",
                    roster_root=Path(temp) / "roster")

    def test_corrupt_roster_evidence_is_not_classified_as_not_ready(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "backend.nhl.daily_capture.load_canonical_slate",
            return_value=[type("Game", (), {"game_id": 2026020001})()],
        ), patch(
            "backend.nhl.daily_orchestration.verify_roster_observation_reuse",
            side_effect=RuntimeError("ROSTER_OBSERVATION_HASH_MISMATCH"),
        ):
            root = Path(temp) / "roster" / "season=2026" / "slate_date=2026-10-18"
            package = root / "observation=run1"
            package.mkdir(parents=True)
            (package / "observation_summary.json").write_text(json.dumps({"phase": "EARLY"}))
            with self.assertRaisesRegex(RuntimeError, "ROSTER_OBSERVATION_INVALID") as caught:
                require_slate_roster_observation(
                    "2026-10-18", slate_root=Path(temp) / "slates",
                    roster_root=Path(temp) / "roster")
            self.assertNotIsInstance(caught.exception, RosterObservationNotReady)


if __name__ == "__main__":
    unittest.main()
