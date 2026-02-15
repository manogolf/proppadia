import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import mlb_readiness_snapshot as readiness


class TestSharedMlbReadinessSnapshot(unittest.TestCase):
    def test_main_pass_with_fresh_roster_and_stat_volume(self):
        def _fetchone(sql, params=()):
            if "MAX(game_date)" in sql:
                return {"n": 25, "latest_game_date": "2025-08-15"}
            self.fail(f"Unexpected SQL in test stub: {sql}")

        out = StringIO()
        with patch.object(readiness, "pg_fetchone", side_effect=_fetchone), patch.object(
            readiness,
            "get_roster_freshness",
            return_value={"ok": True, "status": "pass", "total_players": 1200, "active_players": 850},
        ), redirect_stdout(out):
            rc = readiness.main()
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["checks"]["stat_derived"]["status"], "pass")
        self.assertEqual(payload["checks"]["roster"]["status"], "pass")

    def test_main_fail_when_stale_and_zero_stat_rows(self):
        def _fetchone(sql, params=()):
            if "MAX(game_date)" in sql:
                return {"n": 0, "latest_game_date": None}
            self.fail(f"Unexpected SQL in test stub: {sql}")

        out = StringIO()
        with patch.object(readiness, "pg_fetchone", side_effect=_fetchone), patch.object(
            readiness,
            "get_roster_freshness",
            return_value={"ok": False, "status": "fail", "total_players": 0, "active_players": 0},
        ), redirect_stdout(out):
            rc = readiness.main()
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "fail")
        self.assertEqual(payload["checks"]["stat_derived"]["status"], "pass")
        self.assertEqual(payload["checks"]["roster"]["status"], "fail")


if __name__ == "__main__":
    unittest.main()
