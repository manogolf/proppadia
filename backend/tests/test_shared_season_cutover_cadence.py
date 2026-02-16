import json
import unittest
from contextlib import redirect_stdout
from io import StringIO

from backend.scripts import season_cutover_cadence as scc


class TestSharedSeasonCutoverCadence(unittest.TestCase):
    def test_build_plan_shape(self):
        payload = scc.build_plan(
            timezone="America/New_York",
            market_every_hours=8,
            roster_hour_local=9,
            stat_hour_local=11,
            ops_hour_local=12,
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["pre_cutover_gate"], "make season-cutover-ready")
        self.assertEqual(len(payload["lanes"]), 4)
        self.assertEqual(payload["lanes"][0]["cron"], "0 */8 * * *")

    def test_main_json_output(self):
        out = StringIO()
        with redirect_stdout(out):
            rc = scc.main(["--json", "--market-every-hours", "6"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["lanes"][0]["cron"], "0 */6 * * *")


if __name__ == "__main__":
    unittest.main()
