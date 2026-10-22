"""The daily history seeding skips the certified prior day's void games."""
import unittest
import unittest.mock

from backend.nhl.cli import prior_day_voided_env
from backend.nhl.official_request_journal import ENV_VOIDED_GAME_IDS, skip_voided_games


class PriorDayVoidedEnvTests(unittest.TestCase):
    def test_certified_void_games_are_passed_to_children(self):
        learning = {"certificate": {"status": "CERTIFIED_FINAL", "slate_date": "2026-10-21",
                                    "voided_game_ids": [2026020155, 2026020150]}}
        env = prior_day_voided_env(learning, "2026-10-21")
        self.assertEqual({ENV_VOIDED_GAME_IDS: "2026020155,2026020150"}, env)
        with unittest.mock.patch.dict("os.environ", env):
            self.assertEqual([2026020151], skip_voided_games([2026020150, 2026020151, 2026020155]))

    def test_no_void_games_or_other_date_passes_nothing(self):
        certified = {"certificate": {"status": "CERTIFIED_FINAL", "slate_date": "2026-10-21",
                                     "game_ids": [2026020151]}}
        self.assertEqual({}, prior_day_voided_env(certified, "2026-10-21"))
        voided = {"certificate": {"slate_date": "2026-10-20", "voided_game_ids": [2026020140]}}
        self.assertEqual({}, prior_day_voided_env(voided, "2026-10-21"))
        self.assertEqual({}, prior_day_voided_env({}, "2026-10-21"))


if __name__ == "__main__":
    unittest.main()
