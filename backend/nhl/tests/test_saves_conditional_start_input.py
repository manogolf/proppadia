from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.prediction_lineage import prepare_scoring_input
from backend.nhl.scripts.score_nhl_props import main as score_props, prepare_X, prob_over_poisson


class SavesConditionalStartInputTests(unittest.TestCase):
    def _prepare(self, root: Path, *, constant_features=None) -> pd.DataFrame:
        source = root / "source.csv"
        output = root / "scoring_input.csv"
        pd.DataFrame([
            {"player_id": 11, "game_id": 2026020001, "game_date": "2026-10-01",
             "start_prob": None, "d5_saves_per60": 20.0,
             "starter_provenance": "UNCONFIRMED"},
            {"player_id": 12, "game_id": 2026020001, "game_date": "2026-10-01",
             "start_prob": None, "d5_saves_per60": None,
             "starter_provenance": "UNCONFIRMED"},
        ]).to_csv(source, index=False)
        prepare_scoring_input(
            source_path=source, output_path=output,
            canonical_games=[{
                "game_id": 2026020001,
                "start_time_utc": "2026-10-02T02:00:00Z",
                "home_team_id": 1, "away_team_id": 2,
            }],
            slate="2026-10-01", parent_daily_run_id="test-run",
            feature_input_cutoff_utc="2026-10-01T20:00:00Z",
            expected_game_set_hash=canonical_game_set_hash([2026020001]),
            constant_feature_values=constant_features,
        )
        return pd.read_csv(output)

    def test_admitted_saves_rows_receive_constant_conditional_start_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._prepare(Path(tmp), constant_features={"start_prob": 1.0})
        self.assertEqual(len(result), 2)
        self.assertEqual(result.player_id.tolist(), [11, 12])
        self.assertEqual(result.start_prob.tolist(), [1.0, 1.0])
        self.assertFalse(result.start_prob.eq(0.0).any())

    def test_no_null_start_probability_reaches_generic_scorer_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._prepare(Path(tmp), constant_features={"start_prob": 1.0})
        self.assertFalse(result.start_prob.isna().any())
        self.assertTrue(result.start_prob.eq(1.0).all())

    def test_contract_assignment_does_not_add_rows_or_admit_ineligible_goalies(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._prepare(Path(tmp), constant_features={"start_prob": 1.0})
        self.assertEqual(set(result.player_id), {11, 12})
        self.assertNotIn(13, set(result.player_id))

    def test_other_missing_history_and_starter_provenance_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._prepare(Path(tmp), constant_features={"start_prob": 1.0})
        self.assertTrue(pd.isna(result.loc[result.player_id.eq(12), "d5_saves_per60"]).all())
        self.assertEqual(result.starter_provenance.tolist(), ["UNCONFIRMED", "UNCONFIRMED"])

    def test_continuous_missing_history_keeps_median_imputation(self):
        from backend.nhl.scripts.score_nhl_props import prepare_X

        frame = pd.DataFrame({"recent_saves": [20.0, None, 30.0]})
        prepared = prepare_X(frame, ["recent_saves"], ["recent_saves"])
        self.assertTrue(np.isfinite(prepared.recent_saves).all())
        self.assertEqual(prepared.recent_saves.tolist(), [20.1, 25.0, 29.9])

    def test_scored_conditional_probabilities_are_open_interval_model_tails(self):
        repo = Path(__file__).resolve().parents[3]
        feature_meta_path = repo / "backend/nhl/features/feature_metadata_nhl.json"
        metadata = json.loads(feature_meta_path.read_text())["goalie_saves"]
        model_dir = repo / "backend/nhl/models/latest/goalie_saves"
        artifact = json.loads((model_dir / "MODEL_ARTIFACT.json").read_text())
        model = artifact["sklearn_poisson"]
        rows = []
        for player_id, is_home, b2b, recent in ((11, 1, "f", 20.0), (12, 0, "t", None)):
            row = {feature: 0.0 for feature in metadata}
            row.update({
                "player_id": player_id, "game_id": 2026020001,
                "game_date": "2026-10-01", "is_home": is_home,
                "rest_days": 2 if player_id == 11 else 4, "b2b_flag": b2b,
                "start_prob": 1.0, "d5_saves_per60": recent,
                "d10_saves_per60": 22.0, "d20_saves_per60": 23.0,
                "d5_shots_faced_per60": 25.0, "season_save_pct": 0.91,
                "opponent_id": 2,
            })
            rows.append(row)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path, output_path = root / "input.csv", root / "predictions.csv"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            argv = [
                "score_nhl_props.py", "--model-dir", str(model_dir),
                "--csv", str(input_path), "--feature-json", str(feature_meta_path),
                "--feature-key", "goalie_saves", "--line", "18.5,24.5,30.5",
                "--out", str(output_path),
            ]
            with patch("sys.argv", argv):
                score_props()
            output = pd.read_csv(output_path)
            probability_columns = [c for c in output if c.startswith("p_over_")]
            probabilities = output[probability_columns].to_numpy(float)
            self.assertTrue(((probabilities > 0.0) & (probabilities < 1.0)).all())
            x = prepare_X(pd.DataFrame(rows), metadata, model["feature_order"])
            mu = np.exp(x.values @ np.asarray(model["coef"], dtype=float) + model["intercept"])
            expected = np.clip(prob_over_poisson(mu, 18.5), 1e-6, 1 - 1e-6)
            np.testing.assert_allclose(output.p_over_18_5, expected, rtol=0, atol=1e-14)

    def test_default_scoring_input_behavior_for_other_lanes_is_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._prepare(Path(tmp))
        self.assertTrue(result.start_prob.isna().all())
        self.assertTrue(pd.isna(result.loc[result.player_id.eq(12), "d5_saves_per60"]).all())

    def test_missing_contract_feature_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.csv"
            pd.DataFrame([{
                "player_id": 11, "game_id": 2026020001, "game_date": "2026-10-01",
            }]).to_csv(source, index=False)
            with self.assertRaisesRegex(RuntimeError, "SCORING_INPUT_CONTRACT_FEATURE_MISSING:start_prob"):
                prepare_scoring_input(
                    source_path=source, output_path=root / "output.csv",
                    canonical_games=[{
                        "game_id": 2026020001,
                        "start_time_utc": "2026-10-02T02:00:00Z",
                        "home_team_id": 1, "away_team_id": 2,
                    }],
                    slate="2026-10-01", parent_daily_run_id="test-run",
                    feature_input_cutoff_utc="2026-10-01T20:00:00Z",
                    expected_game_set_hash=canonical_game_set_hash([2026020001]),
                    constant_feature_values={"start_prob": 1.0},
                )


if __name__ == "__main__":
    unittest.main()
