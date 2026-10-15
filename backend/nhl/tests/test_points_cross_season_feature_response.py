import unittest

import numpy as np
import pandas as pd

from backend.nhl.scripts import build_nhl_points_cross_season_feature_response as audit


class PointsCrossSeasonFeatureResponseTests(unittest.TestCase):
    def test_fixed_game_maturity_bands(self):
        got = audit.game_band(pd.Series([0, 1, 2, 3, 5, 6, 10, 11]))
        self.assertEqual(list(got.astype(str)), ["0", "1-2", "1-2", "3-5", "3-5", "6-10", "6-10", "11+"])

    def test_expected_count_bins_are_fixed_and_cover_boundaries(self):
        got = pd.cut(pd.Series([.1, .2, .3, .4, 1.0, 1.2]), audit.COUNT_EDGES,
                     labels=audit.COUNT_LABELS, right=False, include_lowest=True)
        self.assertEqual(list(got.astype(str)), ["<.20", ".20-.30", ".30-.40", ".40-.50", "1.00+", "1.00+"])

    def test_residual_and_brier_metrics(self):
        frame = pd.DataFrame({"realized_points":[0, 1], "expected_points":[.2, .4], "prob_over_0_5":[.1, .7]})
        result = audit.metrics(frame)
        self.assertAlmostEqual(result["residual_realized_minus_predicted"], .2)
        self.assertAlmostEqual(result["brier_0_5"], .05)

    def test_matching_pair_selection_uses_only_pregame_covariates(self):
        features = ["current_season_games_prior", "player_points_last10", "season_ppg_prior",
                    "mean_toi_last10", "mean_pp_toi_last10", "d10_sog_per60",
                    "team_d10_sf_per_game", "last10_team_sog_share"]
        base = {c: [v] for c, v in zip(features, [2., .5, .5, 15., 1., 3., 28., .05])}
        h = pd.DataFrame(base); h["game_id"] = [1]; h["player_id"] = [10]
        h["expected_points"] = [.4]; h["realized_points"] = [0.]; h["prob_over_0_5"] = [.3]
        d = pd.DataFrame(base); d["game_id"] = [2]; d["player_id"] = [11]
        d["expected_points"] = [.4]; d["realized_points"] = [1.]; d["prob_over_0_5"] = [.3]
        first, *_ = audit.match_common_support(h, d)
        h["realized_points"] = [99.]; d["realized_points"] = [-99.]
        second, *_ = audit.match_common_support(h, d)
        self.assertEqual(first["pairs"], second["pairs"])
        self.assertTrue(first["summary"]["outcome_blind"])


if __name__ == "__main__":
    unittest.main()
