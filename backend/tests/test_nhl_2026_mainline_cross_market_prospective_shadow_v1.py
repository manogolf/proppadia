import tempfile
import unittest
from pathlib import Path

from backend.nhl.scripts.activate_nhl_2026_mainline_cross_market_prospective_shadow_v1 import run_validation


class CrossMarketShadowTest(unittest.TestCase):
    def test_local_rehearsal(self):
        with tempfile.TemporaryDirectory(prefix="nhl_cross_market_test_") as raw:
            checks, summary = run_validation(Path(raw))
        self.assertEqual(summary["failures"], 0)
        self.assertTrue(all(row["status"] == "PASS" for row in checks))
        self.assertEqual(summary["live_api_calls"], 0)


if __name__ == "__main__":
    unittest.main()
