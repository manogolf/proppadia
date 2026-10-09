import unittest
import numpy as np

from backend.nhl.scripts.build_nhl_points_leader_validation import (
    fit_alpha, nb_probs, poisson_probs, season_split,
)


class PointsLeaderValidationTests(unittest.TestCase):
  def test_poisson_and_nb_thresholds_are_coherent_and_monotonic_in_mean(self):
    means = np.array([0.05, 0.2, 0.8, 1.5, 3.0])
    for probabilities in (poisson_probs(means), nb_probs(means, 0.15)):
      self.assertTrue(np.all(probabilities[:, 0] >= probabilities[:, 1]))
      self.assertTrue(np.all(probabilities[:, 1] >= probabilities[:, 2]))
      self.assertTrue(np.all(np.diff(probabilities, axis=0) >= 0))


  def test_nb_dispersion_estimator_returns_finite_nonnegative_value(self):
    y = np.array([0, 0, 0, 1, 1, 2, 3, 4, 0, 1] * 20)
    mu = np.full_like(y, 0.5, dtype=float)
    alpha = fit_alpha(y, mu)
    self.assertTrue(0 <= alpha <= 2)
    self.assertTrue(np.isfinite(alpha))


  def test_nb_probability_layer_converges_to_poisson_at_zero_dispersion(self):
    means = np.array([0.1, 0.5, 1.2])
    self.assertTrue(np.allclose(nb_probs(means, 1e-11), poisson_probs(means), atol=1e-10))

  def test_evaluation_season_split_excludes_all_same_and_future_season_rows(self):
    import pandas as pd
    from backend.nhl.scripts.build_nhl_points_architecture_bakeoff import ARMS
    frame = pd.DataFrame({"canonical_season":[2023,2024,2025,2025],
      "history_contract":[ARMS[2]]*4,"game_id":[1,2,3,4],"player_id":[10,10,10,11]})
    train, test = season_split(frame, 2025)
    self.assertEqual(train.canonical_season.unique().tolist(), [2023,2024])
    self.assertEqual(len(test), 2)

  def test_restoration_manifest_target_binding_and_strict_prior_contract(self):
    import json
    from pathlib import Path
    import pandas as pd
    root = Path("artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3")
    manifest = json.loads((root / "validation_input_manifest.json").read_text())
    from backend.nhl.scripts.build_nhl_points_leader_validation import sha256, validate_restored_targets
    self.assertEqual(sha256(root / "validation_input_2023_2025.csv.gz"), manifest["validation_input_sha256"])
    frame = pd.read_csv(root / "validation_input_2023_2025.csv.gz")
    validate_restored_targets(frame, root / "canonical_player_game_outcomes.csv", root / "SHA256SUMS")
    self.assertIn("strict earlier-date", manifest["feature_semantics"])
    self.assertIn("same-day excluded", manifest["feature_semantics"])

  def test_poisson_threshold_coherence_has_no_crossings(self):
    means = np.linspace(0.01, 5, 1000)
    probabilities = poisson_probs(means)
    crossings = (probabilities[:,0] < probabilities[:,1]) | (probabilities[:,1] < probabilities[:,2])
    self.assertEqual(int(crossings.sum()), 0)

  def test_feature_builder_excludes_same_day_outcomes(self):
    import pandas as pd
    from backend.nhl.scripts.build_nhl_points_architecture_bakeoff import build_frame, ARMS
    src = pd.DataFrame({
      "season":[2025,2025,2025],"game_date":["2025-10-07","2025-10-07","2025-10-08"],
      "start_time_utc":["2025-10-07T23:00:00Z","2025-10-07T23:00:00Z","2025-10-08T23:00:00Z"],
      "game_id":[1,2,3],"player_id":[9,9,9],"team_id":[1,1,1],
      "realized_goals":[1,0,0],"realized_assists":[0,1,0],"realized_points":[1,1,0],
      "is_home":[1,0,1],"toi_minutes":[10,10,10],"pp_toi_minutes":[1,1,1],
      "shots_on_goal":[2,1,3],"shot_attempts":[4,2,5]})
    features = build_frame(src)
    literal = features[features.history_contract == ARMS[0]].sort_values("game_id")
    self.assertEqual(literal.player_history_games.tolist(), [0,0,2])
    self.assertEqual(literal.loc[literal.game_id == 3,"player_points_sum_last10"].iloc[0], 2)


if __name__ == "__main__":
  unittest.main()
