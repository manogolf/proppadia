from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.cross_market_shadow.core import PUCK_PARAMETER_PATH, build_puck_line_predictions, build_v2_predictions


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "artifacts/analysis/model_development/nhl_2026_mainline_shadow_activation_and_sog_lineage_closure_v1/2026-09-15"


class MainlineShadowActivationAndSogClosureTest(unittest.TestCase):
    def test_puck_runtime_is_exact_frozen_artifact(self):
        parent = ROOT / "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/fitted_research_artifact.json"
        self.assertEqual(PUCK_PARAMETER_PATH.read_bytes(), parent.read_bytes())

    def test_puck_probability_coherence_and_replay(self):
        schedule = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": "2026-09-19", "game_id": 2026010001,
            "game_date": "2026-09-19", "scheduled_start_time_utc": "2026-09-19T23:00:00Z",
            "home_team_id": 10, "home_team": "MTL", "away_team_id": 9, "away_team": "OTT",
            "game_status": "SCHEDULED", "game_type_code": 1,
        }])
        history = pd.DataFrame(columns=list(schedule.columns) + ["final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots"])
        ml, _ = build_v2_predictions(schedule, history, "2026-09-19T18:00:00Z")
        first = build_puck_line_predictions(ml)
        second = build_puck_line_predictions(ml)
        self.assertAlmostEqual(float(first.probability_sum.iloc[0]), 1.0, places=14)
        self.assertAlmostEqual(float(first.home_minus_1_5_cover_probability.iloc[0] + first.away_plus_1_5_cover_probability.iloc[0]), 1.0, places=14)
        self.assertAlmostEqual(float(first.away_minus_1_5_cover_probability.iloc[0] + first.home_plus_1_5_cover_probability.iloc[0]), 1.0, places=14)
        self.assertEqual(first.substantive_prediction_sha256.tolist(), second.substantive_prediction_sha256.tolist())

    def test_closed_package_team_uniqueness_and_parity(self):
        backcast = pd.read_parquet(PACKAGE / "sog_repaired_backcast_team_complete.parquet")
        coverage = json.loads((PACKAGE / "coverage_and_parity.json").read_text())
        self.assertEqual(int(backcast.team_id.notna().sum()), 19840)
        self.assertTrue(backcast.groupby("game_id").team_id.nunique().eq(2).all())
        self.assertEqual(coverage["original_before_digest"], coverage["original_after_digest"])
        self.assertEqual(coverage["frozen_probability_parity_rows"], 13389)

    def test_all_74_dispositions_are_pregame_and_non_outcome(self):
        rows = pd.read_csv(PACKAGE / "sog_74_team_side_dispositions.csv")
        self.assertEqual(len(rows), 74)
        self.assertEqual(rows[["game_id", "player_id"]].drop_duplicates().shape[0], 74)
        self.assertTrue(rows.current_game_participation_used.eq(False).all())
        self.assertTrue(rows.current_game_box_score_used.eq(False).all())
        self.assertTrue(rows.disposition.eq("RESOLVED_DETERMINISTIC_TEAM_SIDE").all())

    def test_decision_and_manifest(self):
        decision = json.loads((PACKAGE / "decision.json").read_text())
        self.assertEqual(decision["NHL_SOG_REPAIR_SENSITIVITY"], "CONCLUSION_UNCHANGED")
        self.assertEqual(decision["NHL_SOG_TEAM_SIDE_ROWS_RESOLVED"], "74 / 74")
        for line in (PACKAGE / "SHA256SUMS").read_text().splitlines():
            expected, name = line.split("  ", 1)
            import hashlib
            self.assertEqual(hashlib.sha256((PACKAGE / name).read_bytes()).hexdigest(), expected)


if __name__ == "__main__":
    unittest.main()
