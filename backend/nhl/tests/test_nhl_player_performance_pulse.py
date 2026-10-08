import importlib.util
import json
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "backend/nhl/scripts/extend_nhl_player_performance_pulse.py"
spec = importlib.util.spec_from_file_location("pulse_extension", SCRIPT)
pulse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pulse)


class PlayerPerformancePulseTests(unittest.TestCase):
    def _transition(self, player, game_a, game_b, pred_a, pred_b, lane="sog"):
        return {
            "lane": lane,
            "player_id": player,
            "player_name": f"P{player}",
            "game_n_id": game_a,
            "game_n_date": "2026-10-01",
            "game_n1_id": game_b,
            "game_n1_date": "2026-10-03",
            "realized_stat": 2,
            "prediction_prior": json.dumps(pred_a),
            "prediction_current": json.dumps(pred_b),
            "prediction_artifact_identity_prior": f"pred-{game_a}.csv",
            "prediction_artifact_sha256_prior": f"hash-{game_a}",
            "prediction_artifact_identity_current": f"pred-{game_b}.csv",
            "prediction_artifact_sha256_current": f"hash-{game_b}",
            "model_family_version": "phoenix_v2",
        }

    def test_same_player_same_line_only_and_probability_deltas(self):
        transitions = pd.DataFrame([
            self._transition(10, 1, 2, [{"line": 1.5, "prob_over": .2}, {"line": 2.5, "prob_over": .1}], [{"line": 1.5, "prob_over": .35}, {"line": 3.5, "prob_over": .04}]),
            self._transition(11, 3, 4, [{"line": 1.5, "prob_over": .8}], [{"line": 1.5, "prob_over": .7}]),
        ])
        result = pulse.prediction_transitions(transitions)
        self.assertEqual(len(result), 2)
        row = result[(result.player_id == 10)].iloc[0]
        self.assertEqual(row.line, 1.5)
        self.assertAlmostEqual(row.signed_probability_change, .15)
        self.assertAlmostEqual(row.absolute_probability_change, .15)
        self.assertEqual(row.model_version_comparability_status, "UNPROVEN_FITTED_ARTIFACT_HASH_NOT_BOUND_TO_HISTORICAL_RECEIPTS")
        self.assertNotIn(2.5, result.line.tolist())
        self.assertNotIn(3.5, result.line.tolist())
        self.assertNotIn(11, result[result.player_id == 10].player_id.tolist())

    def test_prediction_transition_is_deterministic(self):
        transitions = pd.DataFrame([self._transition(10, 1, 2, [{"line": 1.5, "prob_over": .2}], [{"line": 1.5, "prob_over": .3}])])
        pd.testing.assert_frame_equal(pulse.prediction_transitions(transitions), pulse.prediction_transitions(transitions))

    def test_missing_model_identity_is_not_silently_compared(self):
        row = self._transition(10, 1, 2, [{"line": 1.5, "prob_over": .2}], [{"line": 1.5, "prob_over": .3}])
        row["model_family_version"] = None
        self.assertTrue(pulse.prediction_transitions(pd.DataFrame([row])).empty)

    def test_repeated_divergence_requires_two_consecutive_transitions(self):
        self.assertEqual(pulse.consecutive_runs([True, True, False, True], minimum=2), [(0, 2)])
        self.assertEqual(pulse.consecutive_runs([True, False, True], minimum=2), [])

    def test_unchanged_dynamic_state_is_not_promoted_to_failure(self):
        transitions = pd.DataFrame([{
            "lane": "sog", "player_id": 10, "game_n_id": 1, "game_n1_id": 2, "realized_stat": 0,
        }])
        movements = pd.DataFrame([{
            "lane": "sog", "player_id": 10, "game_n_id": 1, "game_n1_id": 2,
            "feature": "d10_sog_per60", "prior_value": 3.0, "current_value": 3.0, "signed_change": 0.0,
        }])
        result = pulse.stale_state_pulse(transitions, movements)
        self.assertEqual(result.iloc[0].status, "UNRESOLVED_HISTORY_LIMITATION")
        self.assertFalse(result.iloc[0].confirmed_stale_failure)

    def test_historical_unresolved_rolling_comparison_is_not_a_failure(self):
        # The revision carries base unresolved counts as unresolved evidence;
        # it never maps them into stale-state failures.
        self.assertEqual(pulse.DIRECT_FEATURES["sog"][0], "d5_sog_per60")
        self.assertFalse(pulse.stale_state_pulse(pd.DataFrame(columns=["lane", "player_id", "game_n_id", "game_n1_id"]), pd.DataFrame(columns=["lane", "player_id", "game_n_id", "game_n1_id", "feature", "signed_change"])).get("confirmed_stale_failure", pd.Series(dtype=bool)).any())

    def test_window_entering_and_displaced_game_ids(self):
        partial = pulse.window_roll_forward([11, 12], 13, 3)
        self.assertEqual(partial["current_ids"], [11, 12, 13])
        self.assertEqual(partial["window_count_after"], 3)
        self.assertIsNone(partial["displaced_game_id"])
        full = pulse.window_roll_forward([10, 11, 12], 13, 3)
        self.assertEqual(full["current_ids"], [11, 12, 13])
        self.assertEqual(full["entering_game_id"], 13)
        self.assertEqual(full["displaced_game_id"], 10)

    def test_lane_lines_are_not_cross_compared(self):
        transitions = pd.DataFrame([self._transition(20, 1, 2, [{"line": 0.5, "prob_over": .2}], [{"line": 1.5, "prob_over": .3}], lane="points")])
        self.assertTrue(pulse.prediction_transitions(transitions).empty)


if __name__ == "__main__":
    unittest.main()
