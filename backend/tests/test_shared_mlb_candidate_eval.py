import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.scripts import mlb_candidate_eval as cand


def _baseline_payload() -> dict:
    return {
        "overall": {"window_mode": "games", "window_value": 30, "total": 1000, "accuracy_pct": 52.0},
        "by_prop": [
            {"prop_type": "hits", "total": 500, "accuracy_pct": 53.0},
            {"prop_type": "total_bases", "total": 500, "accuracy_pct": 51.0},
        ],
    }


class TestSharedMlbCandidateEval(unittest.TestCase):
    def test_evaluate_promote(self):
        result = cand.evaluate_candidate(
            baseline_payload=_baseline_payload(),
            candidate_payload={
                "overall": {"total": 1100, "accuracy_pct": 53.0},
                "by_prop": [
                    {"prop_type": "hits", "accuracy_pct": 53.8},
                    {"prop_type": "total_bases", "accuracy_pct": 51.2},
                ],
            },
            required_props=["hits", "total_bases"],
            min_overall_lift_pct=0.25,
            max_prop_drop_pct=0.5,
            min_candidate_total=1000,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"], "promote")
        self.assertEqual(result["failures"], [])

    def test_evaluate_hold_on_prop_degradation(self):
        result = cand.evaluate_candidate(
            baseline_payload=_baseline_payload(),
            candidate_payload={
                "overall": {"total": 1100, "accuracy_pct": 53.0},
                "by_prop": [
                    {"prop_type": "hits", "accuracy_pct": 51.9},
                    {"prop_type": "total_bases", "accuracy_pct": 51.2},
                ],
            },
            required_props=["hits", "total_bases"],
            min_overall_lift_pct=0.25,
            max_prop_drop_pct=0.5,
            min_candidate_total=1000,
        )
        self.assertFalse(result["ok"])
        self.assertIn("required_prop_stability", result["failures"])
        self.assertEqual(result["recommendation"], "hold")

    def test_main_uses_baseline_file_and_returns_nonzero_when_lift_too_low(self):
        with tempfile.TemporaryDirectory() as td:
            baseline_path = Path(td) / "mlb_quality_games_30_120.json"
            baseline_path.write_text(json.dumps(_baseline_payload()), encoding="utf-8")
            out = StringIO()
            with patch.object(
                cand.analyze_mlb_prediction_quality,
                "collect_quality",
                return_value={
                    "overall": {"total": 1200, "accuracy_pct": 52.1},
                    "by_prop": [
                        {"prop_type": "hits", "accuracy_pct": 53.0},
                        {"prop_type": "total_bases", "accuracy_pct": 51.1},
                    ],
                },
            ), redirect_stdout(out):
                rc = cand.main(["--baseline-path", str(baseline_path), "--min-overall-lift-pct", "0.5"])
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("overall_lift", payload["failures"])


if __name__ == "__main__":
    unittest.main()
