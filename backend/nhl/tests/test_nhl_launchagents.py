import plistlib
import unittest
from pathlib import Path

TEMPLATES = Path(__file__).resolve().parents[1] / "launchagents"


class CrossMarketLaunchAgentTests(unittest.TestCase):
    def test_cross_market_job_keeps_the_mac_awake(self):
        # A brief overnight wake once left its reconciliation hung with the
        # lock held, blocking the 04:30 and 05:45 runs (2026-11-10).
        path = TEMPLATES / "com.proppadia.nhl.mainline-cross-market-shadow.plist"
        args = plistlib.loads(path.read_bytes())["ProgramArguments"]
        self.assertEqual(args[:2], ["/usr/bin/caffeinate", "-i"])
        self.assertTrue(args[3].endswith("run_nhl_mainline_cross_market_capture_warn_only.py"))

    def test_install_script_reloads_the_cross_market_job(self):
        script = (Path(__file__).resolve().parents[3] / "bin/nhl_install_schedule.sh").read_text()
        self.assertIn('reload com.proppadia.nhl.mainline-cross-market-shadow "$templates/', script)


if __name__ == "__main__":
    unittest.main()
