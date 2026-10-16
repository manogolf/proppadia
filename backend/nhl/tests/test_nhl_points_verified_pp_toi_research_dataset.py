import unittest

import pandas as pd

from backend.nhl.scripts.build_nhl_points_verified_pp_toi_research_dataset import (
    make_strata,
    outcome_class,
    qualify_replay_rows,
)


class VerifiedPpToiResearchDatasetTests(unittest.TestCase):
    def test_qualification_requires_nonempty_fully_verified_history(self):
        replay = pd.DataFrame({
            "status": ["REPLAYED"] * 4 + ["TARGET_RAW_IDENTITY_MISSING"],
            "prior_player_games_in_120d": [10, 10, 0, 2, 10],
            "verified_reconstruction_rows_in_window": [10, 9, 0, 2, 10],
        })
        self.assertEqual(qualify_replay_rows(replay).tolist(), [True, False, False, True, False])

    def test_outcome_classes_keep_three_plus_together(self):
        self.assertEqual(outcome_class(pd.Series([0, 1, 2, 3, 6])).tolist(), ["0", "1", "2", "3+", "3+"])

    def test_strata_roll_up_each_population_without_losing_rows(self):
        population = pd.DataFrame({
            "canonical_season": [2023, 2023, 2024],
            "game_date": ["2023-10-10", "2023-10-11", "2024-10-10"],
            "realized_points": [0, 1, 3],
            "prior_player_games_in_120d": [1, 5, 10],
            "player_lifetime_games_prior": [0, 25, 120],
            "current_raw_replay_matches_frozen_feature": [True, False, True],
        })
        cohort = population.iloc[[0, 2]].copy()
        result = make_strata(population, cohort)
        sums = result.groupby(["population", "dimension"]).row_count.sum()
        for dimension in result.dimension.unique():
            self.assertEqual(sums["FROZEN_TRAINING_POPULATION", dimension], len(population))
            self.assertEqual(sums["FULLY_VERIFIED_COHORT", dimension], len(cohort))
        outcome = result[(result.population == "FROZEN_TRAINING_POPULATION") & (result.dimension == "outcome_class")]
        self.assertEqual(outcome.row_count.sum(), len(population))
        self.assertIn("3+", outcome.stratum.tolist())


if __name__ == "__main__":
    unittest.main()
