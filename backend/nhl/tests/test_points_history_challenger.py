import importlib.util
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "backend/nhl/scripts/score_nhl_points_history_challenger.py"
spec = importlib.util.spec_from_file_location("points_history_challenger", SCRIPT)
challenger = importlib.util.module_from_spec(spec)
spec.loader.exec_module(challenger)


class PointsHistoryChallengerTests(unittest.TestCase):
    def _csv(self, root, name, rows):
        path = root / name
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_same_identity_and_nonhistory_features_are_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common = [{"player_id": 1, "game_id": 100, "is_home": 1, "team_d10_sf_per_game": 30.0, "d5_sog_per60": 2.0}]
            a = self._csv(root, "a.csv", common)
            b = self._csv(root, "b.csv", [{**common[0], "d5_sog_per60": 4.0}])
            c = self._csv(root, "c.csv", common)
            frames = challenger.load_features({"cross_season": a, "legacy_120_day": b, "current_season_only": c})
            self.assertEqual(frames["legacy_120_day"].loc[0, "d5_sog_per60"], 4.0)
            bad = self._csv(root, "bad.csv", [{**common[0], "team_d10_sf_per_game": 31.0}])
            with self.assertRaisesRegex(ValueError, "NON_PLAYER_HISTORY_FEATURE_DIFFERS"):
                challenger.load_features({"cross_season": a, "legacy_120_day": bad, "current_season_only": c})

    def test_mismatched_game_player_population_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = self._csv(root, "a.csv", [{"player_id": 1, "game_id": 100, "d5_sog_per60": 2.0}])
            b = self._csv(root, "b.csv", [{"player_id": 2, "game_id": 100, "d5_sog_per60": 4.0}])
            c = self._csv(root, "c.csv", [{"player_id": 1, "game_id": 100, "d5_sog_per60": 2.0}])
            with self.assertRaisesRegex(ValueError, "ARM_IDENTITY_MISMATCH"):
                challenger.load_features({"cross_season": a, "legacy_120_day": b, "current_season_only": c})


if __name__ == "__main__":
    unittest.main()
