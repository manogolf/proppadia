"""The phase restatement re-grades participation beside the sealed package."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl import postgame_learning
from backend.nhl.postgame_learning import (
    PHASE_RESTATEMENT_CONTRACT, _restate_sog_participation, _write_phase_restatement,
)


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-18"
           / "reconciliation=a44cf217c8e17bcb688e")


class SogRestatementGateTests(unittest.TestCase):
    def test_a_changed_resolved_grade_is_refused(self):
        frame = pd.DataFrame({
            "canonical_season": [2026], "slate_date": ["2026-10-18"], "game_id": [1],
            "player_id": [10], "line": [1.5], "selected_side": ["OVER"],
            "official_final": [True], "official_sog": [3], "participation_status": ["APPEARED"],
            "outcome_source": ["x"], "outcome_source_timestamp_utc": ["2026-10-19T00:00:00Z"],
            "grading_state": ["LOSS"], "grading_timestamp_utc": ["2026-10-19T01:00:00Z"],
        })
        skaters = pd.DataFrame({
            "canonical_season": [2026, 2026], "slate_date": ["2026-10-18"] * 2,
            "game_id": [1, 1], "player_id": [10, 20], "team_id": [100, 200],
            "official_final": [True, True], "official_sog": [3, 1],
            "participation_state": ["PARTICIPATED"] * 2, "outcome_source": ["x"] * 2,
            "outcome_source_timestamp_utc": ["2026-10-19T00:00:00Z"] * 2,
        })
        with self.assertRaisesRegex(RuntimeError, "SOG_RESTATEMENT_CHANGED_A_RESOLVED_GRADE"):
            _restate_sog_participation(frame, skaters)


@unittest.skipUnless(PACKAGE.is_dir(), "retained 2026-10-18 reconciliation package is a local artifact")
class October18ParticipationRestatementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="nhl_restatement_")
        cls.destination = _write_phase_restatement(PACKAGE, Path(cls.temp.name))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def read(self, lane):
        return pd.read_csv(self.destination / f"graded_{lane}.csv")

    def test_sog_absences_become_non_appearances_and_settled_rows_are_unchanged(self):
        restated = self.read("sog").grading_state.value_counts().to_dict()
        self.assertEqual(restated, {"WIN": 972, "LOSS": 267, "NO_APPEARANCE_UNGRADED": 348})

    def test_resilient_sog_grades_skaters_who_appeared(self):
        restated = self.read("resilient_sog").grading_state.value_counts().to_dict()
        self.assertEqual(restated, {"WIN": 972, "LOSS": 267, "NO_APPEARANCE_UNGRADED": 348})

    def test_points_absences_become_non_participants(self):
        for lane in ("points", "resilient_points"):
            statuses = self.read(lane).grading_status.value_counts().to_dict()
            self.assertEqual(statuses.get("NONPARTICIPANT_UNGRADED"), 87, lane)
            self.assertEqual(statuses.get("SETTLED"), 213, lane)
            self.assertNotIn("PARTICIPATION_STATUS_UNRESOLVED", statuses)

    def test_lineage_binds_the_sealed_inputs_and_records_changes(self):
        lineage = json.loads((self.destination / "lineage.json").read_text())
        self.assertEqual(lineage["contract_version"], PHASE_RESTATEMENT_CONTRACT)
        self.assertEqual(lineage["source_package"], str(PACKAGE))
        self.assertIn("canonical_skater_outcomes.csv", lineage["source_hashes"])
        self.assertEqual(lineage["grade_changes"]["sog"], 348)
        self.assertEqual(lineage["grade_changes"]["resilient_sog"], 1587)
        self.assertEqual(lineage["grade_changes"]["points"], 87)
        self.assertTrue((self.destination / "SHA256SUMS").is_file())

    def test_rewrite_is_idempotent_and_never_touches_the_package(self):
        before = (PACKAGE / "SHA256SUMS").read_bytes()
        with patch.object(postgame_learning.pd.DataFrame, "to_csv",
                          side_effect=AssertionError("EXISTING_RESTATEMENT_REWRITTEN")):
            again = _write_phase_restatement(PACKAGE, Path(self.temp.name))
        self.assertEqual(again, self.destination)
        self.assertEqual((PACKAGE / "SHA256SUMS").read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
