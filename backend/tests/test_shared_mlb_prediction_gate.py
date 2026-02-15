import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import mlb_prediction_gate as gate


class TestSharedMlbPredictionGate(unittest.TestCase):
    def test_gate_passes_when_operability_and_quality_pass(self):
        out = StringIO()
        with patch.object(
            gate,
            "collect_probe",
            return_value={"ok": True, "predict_success": 5},
        ), patch.object(
            gate,
            "collect_quality",
            return_value={"overall": {"total": 100, "accuracy_pct": 55.0}},
        ), patch.object(
            gate,
            "InProcessClient",
            return_value=object(),
        ), redirect_stdout(out):
            rc = gate.main(
                [
                    "--sample-size",
                    "5",
                    "--require-min-success",
                    "1",
                    "--quality-min-total",
                    "1",
                    "--quality-min-accuracy",
                    "50",
                ]
            )
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["quality"]["ok"])

    def test_gate_fails_when_quality_below_threshold(self):
        out = StringIO()
        with patch.object(
            gate,
            "collect_probe",
            return_value={"ok": True, "predict_success": 5},
        ), patch.object(
            gate,
            "collect_quality",
            return_value={"overall": {"total": 100, "accuracy_pct": 49.9}},
        ), patch.object(
            gate,
            "InProcessClient",
            return_value=object(),
        ), redirect_stdout(out):
            rc = gate.main(
                [
                    "--quality-min-total",
                    "1",
                    "--quality-min-accuracy",
                    "50",
                ]
            )
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertFalse(payload["quality"]["ok"])

    def test_gate_passes_games_mode_window_to_quality(self):
        out = StringIO()
        with patch.object(
            gate,
            "collect_probe",
            return_value={"ok": True, "predict_success": 5},
        ), patch.object(
            gate,
            "collect_quality",
            return_value={"overall": {"total": 100, "accuracy_pct": 55.0}},
        ) as quality_mock, patch.object(
            gate,
            "InProcessClient",
            return_value=object(),
        ), redirect_stdout(out):
            rc = gate.main(
                [
                    "--quality-window-mode",
                    "games",
                    "--quality-games-back",
                    "45",
                    "--quality-min-total",
                    "1",
                    "--quality-min-accuracy",
                    "50",
                ]
            )
        self.assertEqual(rc, 0)
        quality_mock.assert_called_once_with("games", 45)


if __name__ == "__main__":
    unittest.main()
