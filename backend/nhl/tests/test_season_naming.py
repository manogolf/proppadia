"""One season rule and one authorized-season list for production lanes (P2.25)."""
import tempfile
import unittest
from pathlib import Path

from backend.nhl.game_phase import (
    AUTHORIZED_SEASONS, authorized_season_for_date, require_authorized_season,
    season_for_date, season_for_game_id)


class SeasonNamingTests(unittest.TestCase):
    def test_date_rule_july_onward_is_the_new_season(self):
        self.assertEqual(2026, season_for_date("2026-10-22"))
        self.assertEqual(2026, season_for_date("2027-04-25"))
        self.assertEqual(2026, season_for_date("2027-06-30"))
        self.assertEqual(2027, season_for_date("2027-07-01"))
        self.assertEqual(2025, season_for_date("2026-06-30"))
        with self.assertRaises(ValueError):
            season_for_date("2026-13-01")

    def test_game_id_carries_its_season(self):
        self.assertEqual(2026, season_for_game_id(2026020141))
        self.assertEqual(2026, season_for_game_id("2026030111"))
        with self.assertRaises(ValueError):
            season_for_game_id(141)

    def test_date_and_game_id_rules_agree_for_this_season(self):
        for game_id, slate in ((2026010002, "2026-09-19"), (2026020141, "2026-10-20"),
                               (2026021300, "2027-04-15"), (2026030411, "2027-06-20")):
            self.assertEqual(season_for_game_id(game_id), season_for_date(slate))

    def test_only_2026_is_authorized_and_others_fail_loudly(self):
        self.assertEqual(frozenset({2026}), AUTHORIZED_SEASONS)
        self.assertEqual(2026, authorized_season_for_date("2027-05-01"))
        with self.assertRaisesRegex(RuntimeError, "NHL_SEASON_NOT_AUTHORIZED:2027"):
            authorized_season_for_date("2027-10-12")
        with self.assertRaisesRegex(RuntimeError, "NHL_SEASON_NOT_AUTHORIZED:2025"):
            require_authorized_season(2025)

    def test_new_season_slate_fails_loudly_instead_of_finding_nothing(self):
        from backend.nhl.postgame_reconcile.core import _cross_market_final_runs
        from backend.nhl.cross_market_shadow.core import daily_status
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "NHL_SEASON_NOT_AUTHORIZED:2027"):
                _cross_market_final_runs(Path(temp), "2027-10-12")
            with self.assertRaisesRegex(RuntimeError, "NHL_SEASON_NOT_AUTHORIZED:2027"):
                daily_status(Path(temp), "2027-10-12")

    def test_this_season_paths_are_unchanged(self):
        from backend.nhl.postgame_reconcile.core import _cross_market_final_runs
        with tempfile.TemporaryDirectory() as temp:
            run = (Path(temp) / "cross_market_shadow" / "season=2026" / "slate_date=2027-04-25"
                   / "run_type=FINAL_PREGAME" / "state=abc")
            run.mkdir(parents=True)
            self.assertEqual([run], _cross_market_final_runs(Path(temp), "2027-04-25"))


if __name__ == "__main__":
    unittest.main()
