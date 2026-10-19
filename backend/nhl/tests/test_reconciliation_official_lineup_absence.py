"""Grade the retained 2026-10-18 package through the operational grading path.

Both official box scores list 18 dressed skaters per team. Twenty predicted
skaters (all ACTIVE_ROSTER_UNCONFIRMED at prediction time) appear in neither
box score: they did not dress and are non-participants, not unresolved.
"""
import json
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.postgame_reconcile.core import (
    _official_lineup_absences,
    grade_operational_sources,
)


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-18"
           / "reconciliation=a44cf217c8e17bcb688e")


class OfficialLineupAbsenceEvidenceTests(unittest.TestCase):
    """Absence is non-participation only when the official lineup is complete."""

    def skaters(self, *, final=True):
        return pd.DataFrame({
            "game_id": [1, 1, 2], "player_id": [10, 20, 30], "team_id": [100, 200, 300],
            "official_final": [final] * 3,
        })

    def test_absent_from_both_teams_box_score_is_reported(self):
        predictions = pd.DataFrame({"game_id": [1, 1], "player_id": [10, 11]})
        absent = _official_lineup_absences(predictions, self.skaters())
        self.assertEqual(absent.to_dict("records"), [{"game_id": 1, "player_id": 11}])

    def test_one_sided_or_nonfinal_box_score_is_not_evidence(self):
        predictions = pd.DataFrame({"game_id": [2, 1], "player_id": [31, 11]})
        self.assertTrue(_official_lineup_absences(predictions.iloc[:1], self.skaters()).empty)
        self.assertTrue(_official_lineup_absences(predictions, self.skaters(final=False)).empty)
        self.assertTrue(_official_lineup_absences(
            predictions, self.skaters().drop(columns=["team_id", "official_final"])).empty)


@unittest.skipUnless(PACKAGE.is_dir(), "retained 2026-10-18 reconciliation package is a local artifact")
class October18OfficialLineupAbsenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        binding = json.loads((PACKAGE / "source_bindings.json").read_text())
        if not all(Path(binding[lane]["run"]).is_dir() for lane in ("sog", "points", "saves")):
            raise unittest.SkipTest("retained 2026-10-18 prediction cards are local artifacts")
        cls.grades = grade_operational_sources(
            binding,
            pd.read_csv(PACKAGE / "canonical_admitted_slate.csv"),
            pd.read_csv(PACKAGE / "canonical_game_outcomes.csv"),
            pd.read_csv(PACKAGE / "canonical_skater_outcomes.csv"),
            pd.read_csv(PACKAGE / "canonical_goalie_outcomes.csv"),
            "2026-10-19T04:05:00+00:00",
        )

    def test_sog_players_absent_from_official_box_scores_are_nonparticipants(self):
        states = self.grades["sog"].grading_state.value_counts().to_dict()
        self.assertEqual(states.get("WIN"), 972)
        self.assertEqual(states.get("LOSS"), 267)
        self.assertEqual(states.get("NO_APPEARANCE_UNGRADED"), 348)
        self.assertNotIn("UNRESOLVED_UNGRADED", states)

    def test_resilient_sog_grades_skaters_who_appeared(self):
        # The resilient population grades the same immutable card as the
        # main path, so its record and absences must match it exactly.
        states = self.grades["resilient_sog"].grading_state.value_counts().to_dict()
        self.assertEqual(states.get("WIN"), 972)
        self.assertEqual(states.get("LOSS"), 267)
        self.assertEqual(states.get("NO_APPEARANCE_UNGRADED"), 348)
        self.assertNotIn("UNRESOLVED_UNGRADED", states)

    def test_points_players_absent_from_official_box_scores_are_nonparticipants(self):
        statuses = self.grades["points"].grading_status.value_counts().to_dict()
        self.assertEqual(statuses.get("SETTLED"), 213)
        self.assertEqual(statuses.get("NONPARTICIPANT_UNGRADED"), 87)
        self.assertNotIn("PARTICIPATION_STATUS_UNRESOLVED", statuses)


if __name__ == "__main__":
    unittest.main()
