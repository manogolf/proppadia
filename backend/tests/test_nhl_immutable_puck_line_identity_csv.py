import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.cross_market_shadow.core import (
    PARAMETER_PATH as MONEYLINE_PARAMETER_PATH,
    PUCK_PARAMETER_PATH,
    digest_value,
    moneyline_substantive_identity,
    sha256,
)
from backend.nhl.postgame_reconcile.core import (
    _read_immutable_puck_predictions,
    _verify_cross_market_identities,
)


class ImmutablePuckLineIdentityCsvTests(unittest.TestCase):
    def test_round_trip_float_read_preserves_substantive_identity(self):
        features = {
            "diff_std_goal_diff_pg": -0.38095238095238093,
            "diff_r10_goal_diff_pg": -0.38095238095238093,
            "diff_std_shot_diff_pg": -2.904761904761905,
            "diff_days_rest": 0.0,
            "home_back_to_back": 0.0,
            "away_back_to_back": 0.0,
        }
        common = {
            "canonical_season": 2026,
            "slate_date": "2026-10-15",
            "game_id": 2026020109,
            "scheduled_start_time_utc": "2026-10-15T23:00:00+00:00",
            "home_team": "OTT",
            "away_team": "NYI",
            **features,
        }
        puck_identity = {
            "canonical_season": common["canonical_season"],
            "game_id": common["game_id"],
            "scheduled_start_time_utc": common["scheduled_start_time_utc"],
            "home_team": common["home_team"],
            "away_team": common["away_team"],
            "features": features,
            "control_artifact_sha256": sha256(PUCK_PARAMETER_PATH),
        }
        puck = pd.DataFrame([{
            **common,
            "substantive_prediction_sha256": digest_value(puck_identity),
        }])
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "puck.csv"
            puck.to_csv(path, index=False)
            default_value = pd.read_csv(path).iloc[0]["diff_std_goal_diff_pg"]
            parsed = _read_immutable_puck_predictions(path)

        self.assertNotEqual(default_value, features["diff_std_goal_diff_pg"])
        self.assertEqual(
            parsed.iloc[0]["diff_std_goal_diff_pg"],
            features["diff_std_goal_diff_pg"],
        )

        moneyline = pd.DataFrame([{
            **common,
            "substantive_prediction_sha256": moneyline_substantive_identity(
                canonical_season=common["canonical_season"],
                game_id=common["game_id"],
                scheduled_start_time_utc=common["scheduled_start_time_utc"],
                home_team=common["home_team"],
                away_team=common["away_team"],
                features=features,
                parameter_sha256=sha256(MONEYLINE_PARAMETER_PATH),
            ),
        }])
        _verify_cross_market_identities(moneyline, parsed)


if __name__ == "__main__":
    unittest.main()
