"""Goalie starter = the boxscore's starter flag, not most ice time (P2.41)."""
import unittest

import pandas as pd

from backend.nhl.postgame_reconcile.core import build_outcomes
from backend.nhl.postgame_reconcile.staging_sync import _authoritative_goalie_contract

GID = 2026020500


def box(with_flags=True):
    def goalie(pid, toi, starter):
        row = {"playerId": pid, "toi": toi, "saves": 10, "shotsAgainst": 11}
        if with_flags:
            row["starter"] = starter
        return row
    skater = {"playerId": 1, "goals": 0, "assists": 0, "points": 0, "sog": 1, "toi": "15:00"}
    return {"playerByGameStats": {
        # Home starter pulled after 20 minutes; the reliever played more.
        "homeTeam": {"forwards": [skater], "defense": [], "goalies": [goalie(30, "20:00", True), goalie(31, "40:00", False)]},
        "awayTeam": {"forwards": [{**skater, "playerId": 2}], "defense": [], "goalies": [goalie(40, "60:00", True)]},
    }}


GAME = pd.DataFrame([{"game_id": GID, "slate_date": "2026-11-01", "game_type_code": 2, "home_team_id_canonical": 1,
                      "away_team_id_canonical": 2, "home_score": 3, "away_score": 2}])


class OutcomeStarterTests(unittest.TestCase):
    def test_pulled_starter_keeps_the_start(self):
        _, _, goalies = build_outcomes(GAME, {GID: box()}, "2026-11-02T08:00:00Z")
        g = goalies.set_index("goalie_id")
        self.assertTrue(g.loc[30, "actual_start_flag"])
        self.assertFalse(g.loc[31, "actual_start_flag"])
        self.assertEqual(g.loc[31, "goalie_participation_state"], "RELIEF_APPEARANCE")
        self.assertTrue((goalies.starter_identity_method == "OFFICIAL_BOXSCORE_STARTER_FLAG").all())

    def test_without_flags_most_ice_time_decides_as_before(self):
        _, _, goalies = build_outcomes(GAME, {GID: box(with_flags=False)}, "2026-11-02T08:00:00Z")
        g = goalies.set_index("goalie_id")
        self.assertTrue(g.loc[31, "actual_start_flag"])
        self.assertTrue((goalies.starter_identity_method == "UNIQUE_MAX_OFFICIAL_TOI_POSTGAME").all())


class StagingStarterTests(unittest.TestCase):
    def rows(self, flags):
        out = [{"game_id": GID, "player_id": 30, "team_id": 1, "toi_minutes": 20.0},
               {"game_id": GID, "player_id": 31, "team_id": 1, "toi_minutes": 40.0},
               {"game_id": GID, "player_id": 40, "team_id": 2, "toi_minutes": 60.0}]
        if flags:
            for row, flag in zip(out, (True, False, True)):
                row["starter"] = flag
        return out

    def test_flags_select_the_pulled_starter(self):
        _, starters, _ = _authoritative_goalie_contract(self.rows(True), game_ids=[GID])
        self.assertEqual(starters, [(GID, 30), (GID, 40)])

    def test_retained_evidence_without_flags_still_verifies(self):
        _, starters, _ = _authoritative_goalie_contract(self.rows(False), game_ids=[GID])
        self.assertEqual(starters, [(GID, 31), (GID, 40)])

    def test_two_flagged_starters_fail_closed(self):
        rows = self.rows(True)
        rows[1]["starter"] = True
        with self.assertRaisesRegex(RuntimeError, "STARTER_NOT_UNIQUE"):
            _authoritative_goalie_contract(rows, game_ids=[GID])


if __name__ == "__main__":
    unittest.main()
