"""SOG shadows retired 2026-11-16 (NHL Backlog P2.59): lanes close as RETIRED and score nothing."""
import unittest
from datetime import datetime, timezone
from unittest import mock

from backend.nhl import cli
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


class FakeRecorder:
    def __init__(self):
        self.finished = []

    def finish_lane(self, name, **kwargs):
        self.finished.append((name, kwargs))


class RetirementTests(unittest.TestCase):
    def test_the_five_formula_family_lanes_close_as_retired(self):
        self.assertEqual(cli.RETIRED_SOG_SHADOW_LANES, {
            "sog_fixed_blend_shadows", "sog_d10_d20_divergence_shadow", "sog_d10_structural_correction_shadow",
            "sog_shrunk_rate_k180_shadow", "sog_poisson_formula_shadow"})
        for lane in cli.RETIRED_SOG_SHADOW_LANES:
            rec = FakeRecorder()
            self.assertTrue(cli._retired_sog_shadow(rec, lane))
            self.assertEqual(rec.finished[0][1]["status"], "RETIRED")
        rec = FakeRecorder()
        self.assertFalse(cli._retired_sog_shadow(rec, "legacy_sog"))
        self.assertEqual(rec.finished, [])

    def test_sog_prediction_only_observer_is_not_called(self):
        with mock.patch.object(capture, "observe_independent_prediction_only", side_effect=RuntimeError("x")), \
                mock.patch.object(capture, "observe_sog_prediction_only") as sog:
            out = capture.observe_prediction_only_lanes("2026-11-17", "AUTO", "postgresql://fixture",
                                                        datetime(2026, 11, 17, tzinfo=timezone.utc))
        sog.assert_not_called()
        self.assertEqual(out["SOG"]["status"], "RETIRED")
        self.assertEqual(out["POINTS"]["status"], "FAILED_BEFORE_RECEIPT")


if __name__ == "__main__":
    unittest.main()
