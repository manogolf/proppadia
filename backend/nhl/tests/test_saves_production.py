"""Production Saves selection (P2.41 cutover): refit live, Phoenix V2 one-step rollback."""
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from backend.nhl import saves_production as sp
from backend.nhl import saves_refit_model as srm


def wide(n=2):
    frame = pd.DataFrame({"player_id": [10 + i for i in range(n)], "game_id": [2026020500] * n,
                          "expected_saves": [20.0 + 4 * i for i in range(n)]})
    model = {"variance_ratio": 1.97, "lines": list(srm.LINES)}
    return pd.concat([frame, srm.prob_over(model, frame.expected_saves.to_numpy())], axis=1)


class SelectionTests(unittest.TestCase):
    def test_refit_is_production_and_phoenix_is_the_rollback(self):
        self.assertEqual(sp.PRODUCTION, sp.SHOTS_X_SAVE_RATE_V1)
        self.assertEqual(sp.labels()["model"], srm.MODEL_ID)
        with mock.patch.object(sp, "PRODUCTION", sp.PHOENIX_V2):
            self.assertEqual(sp.labels()["model_family"], "phoenix")

    def test_long_rows_have_no_calibration(self):
        rows = sp.long_predictions(wide(), srm.LINES, 1.0)
        self.assertEqual(len(rows), 2 * len(srm.LINES))
        self.assertTrue((rows.raw_prob_over == rows.prob_over).all())
        self.assertTrue(np.allclose(rows.prob_over + rows.prob_under, 1))
        for _, g in rows.groupby(["game_id", "goalie_id"]):
            self.assertTrue((np.diff(g.sort_values("line").prob_over.to_numpy()) <= 0).all())

    @unittest.skipUnless(sp.CONTRACT_PATH.is_file(), "local frozen Saves refit not present")
    def test_frozen_refit_loads_with_its_identity(self):
        model, contract = sp.load_refit()
        self.assertEqual(model["calibration_layer"], "NONE")
        self.assertTrue(contract["model_identity_sha256"].startswith("14998530"))
        self.assertEqual(contract["reproducibility"]["status"], "PASS")


class DailyLaneTests(unittest.TestCase):
    def test_refit_lane_writes_the_production_layout_with_lineage(self):
        from backend.nhl.scripts import score_nhl_saves_with_lineage as lane
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[3] / "tmp") as temp:
            temp = Path(temp)
            source = pd.DataFrame({"player_id": [10, 11], "game_id": [2026020500] * 2, "team_id": [1, 2],
                                   "home_team_id": [1, 1], "away_team_id": [2, 2], "game_date": ["2026-11-02"] * 2,
                                   "game_start_utc": ["2026-11-02T23:00:00Z"] * 2, "parent_daily_run_id": ["r"] * 2,
                                   "feature_input_cutoff_utc": ["2026-11-02T15:00:00Z"] * 2,
                                   "canonical_game_set_hash": ["h"] * 2})
            source.to_csv(temp / "in.csv", index=False)
            out = temp / "saves_predictions.csv"
            argv = ["x", "--model-dir", "unused", "--csv", str(temp / "in.csv"), "--feature-json", "unused",
                    "--feature-key", "goalie_saves", "--line", ",".join(str(x) for x in srm.LINES), "--out", str(out)]
            contract = {"model_id": srm.MODEL_ID, "model_identity_sha256": "f" * 64}
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(sp, "load_refit", return_value=({}, contract)), \
                    mock.patch.object(sp, "score_refit", return_value=(wide(), {"history_snapshot_utc": "t"})), \
                    mock.patch.object(sp, "components", return_value=[("scorer_selection", Path(sp.__file__))]), \
                    mock.patch("backend.nhl.database_access.database_url", return_value="postgresql://unused"):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    lane.main()
            bound = pd.read_csv(out)
            self.assertEqual(list(bound.columns[:2]), ["player_id", "game_id"])
            self.assertIn("p_over_24_5", bound.columns)
            self.assertIn("parent_daily_run_id", bound.columns)
            evidence = json.loads(buffer.getvalue().split("NHL_CHILD_SUMMARY_JSON=")[1])["fitted_model_evidence"]
            self.assertEqual(evidence["model_family"], "saves_refit")


if __name__ == "__main__":
    unittest.main()
