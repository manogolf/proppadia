from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

from backend.mlb.shared.ubo5_tb15_production_route import route_rows


class Ubo5Tb15DecommissionTest(unittest.TestCase):
    def setUp(self):
        self.rows = pd.DataFrame([{
            "slate_date": "2026-07-29", "game_pk": 1, "batter_mlb_id": 2,
            "prop_type": "total_bases", "line": 1.5,
            "production_prob_over": .41,
            "counterfactual_incumbent_probability": .41,
        }])

    def test_old_enable_flag_cannot_reactivate_or_load_artifact(self):
        with patch("joblib.load") as load:
            got = route_rows(
                self.rows, artifact=Path("/does/not/exist.joblib"), enabled=True
            ).iloc[0]
        load.assert_not_called()
        self.assertFalse(got.route_eligibility)
        self.assertFalse(got.ubo5_route_attempted)
        self.assertFalse(got.ubo5_artifact_loaded)
        self.assertEqual(got.ubo5_route_status, "DECOMMISSIONED_NOT_EVALUATED")
        self.assertEqual(got.exclusion_reason, "UBO5_DECOMMISSIONED")

    def test_incumbent_is_active_and_not_copied_to_ubo5(self):
        got = route_rows(
            self.rows, artifact=Path("/does/not/exist.joblib"), enabled=True
        ).iloc[0]
        self.assertEqual(got.model_source, "INCUMBENT")
        self.assertEqual(got.active_model_source, "INCUMBENT")
        self.assertEqual(got.active_probability, .41)
        self.assertTrue(pd.isna(got.ubo5_probability_over))


if __name__ == "__main__":
    unittest.main()
