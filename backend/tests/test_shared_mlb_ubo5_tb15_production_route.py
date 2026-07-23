from pathlib import Path
import unittest

import pandas as pd

from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES
from backend.mlb.shared.ubo5_tb15_production_route import route_rows

ROOT = Path(__file__).resolve().parents[2]
ART = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib"
LIVE = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/resume_02_unplayed_candidate_adapter/live_scorer_input.csv"


class Ubo5Tb15RouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = pd.read_csv(LIVE).iloc[[0]].copy()
        cls.row["batter_identity_certified"] = True
        cls.row["identity_ambiguous"] = False

    def route(self, frame=None, **kw):
        return route_rows(frame if frame is not None else self.row, artifact=ART, enabled=True,
                          now_utc="2026-07-23T18:30:00Z", **kw)

    def test_exact_eligible_row_routes(self):
        got = self.route().iloc[0]
        self.assertTrue(got.route_eligibility)
        self.assertEqual(got.model_source, "UBO5_TB15_ESTABLISHED")
        self.assertNotEqual(got.active_probability, got.existing_production_probability)

    def test_disabled_is_exact_fallback(self):
        got = route_rows(self.row, artifact=ART, enabled=False, now_utc="2026-07-23T18:30:00Z").iloc[0]
        self.assertFalse(got.route_eligibility)
        self.assertEqual(got.active_probability, got.existing_production_probability)

    def test_fail_closed_defects(self):
        defects = {
            "sparse": ("strict_prior_pa", 99),
            "line": ("line", .5),
            "lineup": ("starter_certification", "PROJECTED"),
            "identity": ("identity_ambiguous", True),
            "post_start": ("prediction_timestamp_utc", "2026-07-23T20:00:00Z"),
        }
        for name, (column, value) in defects.items():
            with self.subTest(name=name):
                frame = self.row.copy(); frame[column] = value
                got = self.route(frame).iloc[0]
                self.assertFalse(got.route_eligibility)
                self.assertEqual(got.active_probability, got.existing_production_probability)
        frame = self.row.copy(); frame[FEATURES[0]] = None
        self.assertFalse(self.route(frame).iloc[0].route_eligibility)
        self.assertFalse(self.route(expected_artifact_sha256="bad").iloc[0].route_eligibility)

    def test_frozen_indicator_backed_nulls_route(self):
        frame = self.row.copy()
        for feature in ("p_hit_suppression", "p_k_rate", "p_prior_dates", "matchup_k", "matchup_hit"):
            frame[feature] = None
        got = self.route(frame).iloc[0]
        self.assertTrue(got.route_eligibility)
        self.assertEqual(got.feature_completeness_status, "COMPLETE_WITH_MODEL_SUPPORTED_NULLS")


if __name__ == "__main__":
    unittest.main()
