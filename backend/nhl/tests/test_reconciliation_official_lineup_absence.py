"""Grade the retained 2026-10-18 package through the operational grading path.

Both official box scores list 18 dressed skaters per team. Twenty predicted
skaters (all ACTIVE_ROSTER_UNCONFIRMED at prediction time) appear in neither
box score: they did not dress and are non-participants, not unresolved.
"""
import json
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.postgame_reconcile.core import grade_operational_sources


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-18"
           / "reconciliation=a44cf217c8e17bcb688e")


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

    def test_resilient_sog_grades_skaters_who_appeared(self):
        # The resilient population grades the same immutable card as the
        # main path, so its settled record must match it exactly.
        states = self.grades["resilient_sog"].grading_state.value_counts().to_dict()
        self.assertEqual(states.get("WIN"), 972)
        self.assertEqual(states.get("LOSS"), 267)


if __name__ == "__main__":
    unittest.main()
