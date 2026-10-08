import importlib.util
import io
import json
import tempfile
import unittest
import warnings
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import numpy as np


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "backend/nhl/scripts/extend_nhl_player_performance_pulse.py"
spec = importlib.util.spec_from_file_location("pulse_extension", SCRIPT)
pulse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pulse)


class PlayerPerformancePulseTests(unittest.TestCase):
    def test_player_name_selection_skips_missing_and_blank_values(self):
        select = pulse.pulse.first_valid_player_name
        self.assertEqual(select("Current Name", "Prior Name"), "Current Name")
        self.assertEqual(select(np.nan, "Prior Name"), "Prior Name")
        self.assertEqual(select(None, "Prior Name"), "Prior Name")
        self.assertEqual(select("   ", "Prior Name"), "Prior Name")
        self.assertEqual(select(pd.NA, " ", "Retained Official Name"), "Retained Official Name")
        self.assertIsNone(select(np.nan, None, "  "))
        self.assertIsNone(select(np.nan))

    def test_official_receipt_bound_roster_is_identity_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            roster_dir = Path(temp) / "observation"
            roster_dir.mkdir()
            snapshot = roster_dir / "roster_snapshot.jsonl"
            snapshot.write_text(json.dumps({
                "game_id": 20, "player_id": 200, "first_name": "Official",
                "last_name": "Player", "official_source_identity": "NHL_API_ROSTER",
            }) + "\n")
            sums = roster_dir / "SHA256SUMS"
            sums.write_text(f"{pulse.pulse.sha256(snapshot)}  roster_snapshot.jsonl\n")
            receipt = {"roster_observation": {
                "path": str(roster_dir), "manifest_sha256": pulse.pulse.sha256(sums),
            }}
            names = pulse.pulse.roster_identity_names([(Path("receipt.json"), receipt)])
            self.assertEqual(names[(20, 200)], "Official Player")
            self.assertEqual(pulse.pulse.first_valid_player_name(np.nan, None, names[(20, 200)]), "Official Player")

    def test_transition_and_exemplar_carry_resolved_name_without_changing_analytics(self):
        prior = pd.Series({
            "player_id": 100, "player_name": "Authoritative Name", "game_id": 1,
            "slate_date": "2026-10-01", "team_id": 2, "outcome_value": 1,
            "feature_values": {"d10_sog_per60": 4.0}, "prediction_ladder": '[{"line": 1.5, "prob_over": 0.4}]',
        })
        current = pd.Series({
            "player_name": np.nan, "game_id": 2, "slate_date": "2026-10-03", "team_id": 2,
            "feature_values": {"d10_sog_per60": 5.0}, "prediction_ladder": '[{"line": 1.5, "prob_over": 0.6}]',
        })
        transition = pulse._transition_from_states("sog", prior, current)
        self.assertEqual(transition["player_name"], "Authoritative Name")
        self.assertEqual((transition["game_n_id"], transition["game_n1_id"], transition["realized_stat"]), (1, 2, 1))
        self.assertEqual(json.loads(transition["feature_state_prior"]), {"d10_sog_per60": 4.0})
        self.assertEqual(json.loads(transition["feature_state_current"]), {"d10_sog_per60": 5.0})
        transitions = pd.DataFrame([transition])
        exemplar = pulse.exemplars(transitions, pd.DataFrame(), pd.DataFrame())["sog"]["improving_state"]
        self.assertEqual(exemplar["player_name"], "Authoritative Name")
        self.assertEqual(exemplar["player_id"], 100)
        self.assertIsNone(pulse.pulse.normalize_json_value(pulse.pulse.first_valid_player_name(np.nan)))
        self.assertEqual(json.loads(pulse.pulse.strict_json_dumps({"player_name": np.nan}))["player_name"], None)

    def test_strict_json_normalizes_missing_and_nonfinite_values(self):
        value = {
            "python_nan": float("nan"),
            "numpy_nan": np.float64("nan"),
            "positive_infinity": float("inf"),
            "negative_infinity": np.float64("-inf"),
            "finite_float": np.float64(1.25),
            "integer": np.int64(4),
            "boolean": np.bool_(True),
            "nested": [pd.NA, {"value": -2.5, "missing": pd.NaT}],
            "none": None,
        }
        encoded = pulse.pulse.strict_json_dumps(value, sort_keys=True)
        parsed = json.loads(encoded, parse_constant=lambda token: self.fail(f"invalid JSON token: {token}"))
        self.assertEqual(parsed, {
            "python_nan": None,
            "numpy_nan": None,
            "positive_infinity": None,
            "negative_infinity": None,
            "finite_float": 1.25,
            "integer": 4,
            "boolean": True,
            "nested": [None, {"value": -2.5, "missing": None}],
            "none": None,
        })
        self.assertNotIn("NaN", encoded)
        self.assertNotIn("Infinity", encoded)
        self.assertEqual(pulse.pulse.strict_json_dumps(1.25), "1.25")

    def test_shared_json_writer_emits_strict_json(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "summary.json"
            pulse.pulse.write_json(target, {"unavailable": np.float64("nan")}, indent=2)
            contents = target.read_text()
            self.assertEqual(json.loads(contents), {"unavailable": None})
            self.assertNotIn("NaN", contents)
            self.assertNotIn("Infinity", contents)

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

    def test_movement_frame_concat_preserves_schema_without_futurewarning(self):
        old_mov = pd.DataFrame({
            "lane": ["sog"],
            "player_id": [10],
            "standardized_movement": [0.25],
            "feature": ["d10_sog_per60"],
            "signed_change": [1.0],
        })

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            unchanged = pulse.combine_movement_frames(old_mov, [])
        pd.testing.assert_frame_equal(unchanged, old_mov)

        new_mov = [{
            "lane": "points",
            "player_id": 11,
            "standardized_movement": None,
            "feature": "is_home",
            "signed_change": 0.0,
        }]
        with warnings.catch_warnings(record=True) as caught_nonempty:
            warnings.simplefilter("always")
            combined = pulse.combine_movement_frames(old_mov, new_mov)

        self.assertEqual(list(combined.columns), list(old_mov.columns))
        self.assertEqual(combined.lane.tolist(), ["sog", "points"])
        self.assertEqual(combined.player_id.tolist(), [10, 11])
        self.assertEqual(combined.feature.tolist(), ["d10_sog_per60", "is_home"])
        self.assertTrue(pd.isna(combined.loc[1, "standardized_movement"]))
        self.assertEqual(combined.standardized_movement.dtype, old_mov.standardized_movement.dtype)
        warning_text = "DataFrame concatenation with empty or all-NA entries"
        for caught_group in (caught, caught_nonempty):
            self.assertFalse(any(
                issubclass(item.category, FutureWarning) and warning_text in str(item.message)
                for item in caught_group
            ))

    def test_cli_reports_package_paths_and_refuses_existing_revision(self):
        movement_columns = [
            "lane", "player_id", "game_n_id", "game_n1_id", "feature",
            "prior_value", "current_value", "signed_change", "absolute_change",
            "percent_change", "standardized_movement", "movement_status",
        ]
        transitions = pd.DataFrame(columns=["lane", "game_n1_date"])
        predictions = pd.DataFrame(columns=["lane", "absolute_probability_change"])
        empty_movements = pd.DataFrame(columns=movement_columns)
        audit = {"missing_artifacts": 0, "hash_mismatches": 0, "eligible_state_rows": 0, "new_transition_rows": 7}

        with tempfile.TemporaryDirectory() as temp:
            temp_root = Path(temp)
            base = temp_root / "base"
            base.mkdir()
            out = temp_root / "revision"
            (base / "player_state_transitions.csv").write_text("lane,player_id,game_n_id,game_n1_id,game_n1_date\n")
            (base / "feature_movements.csv").write_text(",".join(movement_columns) + "\n")
            (base / "rolling_state_audit.csv").write_text("status\n")
            (base / "independent_rolling_checks.csv").write_text("status\n")
            (base / "summary.json").write_text("{}\n")

            args = ["extend_nhl_player_performance_pulse.py", "--as-of-date", "2026-10-08", "--base-package", str(base), "--output-dir", str(out)]
            output = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(patch.object(pulse, "ROOT", temp_root))
                stack.enter_context(patch.object(pulse, "_load_transitions", return_value=(transitions, audit)))
                stack.enter_context(patch.object(pulse, "prediction_transitions", return_value=predictions))
                stack.enter_context(patch.object(
                    pulse, "movement_and_classes",
                    side_effect=lambda _transitions, _predictions, movements: (movements, pd.DataFrame(columns=["lane"]), {}),
                ))
                stack.enter_context(patch.object(pulse, "feature_prediction_relationships", return_value=pd.DataFrame()))
                stack.enter_context(patch.object(pulse, "divergence_pulse", return_value=pd.DataFrame()))
                stack.enter_context(patch.object(pulse, "stale_state_pulse", return_value=pd.DataFrame(columns=["status"])))
                stack.enter_context(patch.object(pulse, "prospective_window_lineage", return_value=pd.DataFrame()))
                stack.enter_context(patch.object(pulse, "exemplars", return_value={}))
                stack.enter_context(patch("sys.argv", args))
                with redirect_stdout(output):
                    self.assertEqual(pulse.main(), 0)

                report = output.getvalue()
                self.assertIn("NHL PLAYER PERFORMANCE PULSE: COMPLETE", report)
                self.assertIn("As-of: 2026-10-08", report)
                self.assertIn("New transitions: 7", report)
                self.assertIn("Classification: DYNAMIC_FEATURE_EVOLUTION_VERIFIED_WITH_WARNINGS", report)
                self.assertIn(f"Package: {out}", report)
                self.assertIn(f"Summary: {out / 'summary.json'}", report)
                self.assertIn(f"Report: {out / 'README.md'}", report)
                self.assertTrue((out / "summary.json").is_file())
                self.assertTrue((out / "README.md").is_file())
                summary_text = (out / "summary.json").read_text()
                json.loads(summary_text, parse_constant=lambda token: self.fail(f"invalid JSON token: {token}"))
                self.assertNotIn("NaN", summary_text)
                self.assertNotIn("Infinity", summary_text)

                summary_before = (out / "summary.json").read_bytes()
                refusal_output = io.StringIO()
                with redirect_stdout(refusal_output), self.assertRaises(SystemExit):
                    pulse.main()
                self.assertEqual(refusal_output.getvalue(), "")
                self.assertEqual((out / "summary.json").read_bytes(), summary_before)

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
