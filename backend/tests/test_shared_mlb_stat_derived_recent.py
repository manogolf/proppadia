import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import validate_mlb_stat_derived_recent as validator


class TestSharedMlbStatDerivedRecent(unittest.TestCase):
    def test_json_pass_includes_latest_game_date(self):
        out = StringIO()
        with patch.object(
            validator,
            "pg_fetchone",
            return_value={"n": 42, "latest_game_date": "2025-08-15"},
        ):
            with redirect_stdout(out):
                rc = validator.main(["--days", "30", "--require-min", "1", "--json"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "pass")
        self.assertEqual(payload["count"], 42)
        self.assertEqual(payload["latest_game_date"], "2025-08-15")

    def test_json_fail_when_below_required_minimum(self):
        out = StringIO()
        with patch.object(
            validator,
            "pg_fetchone",
            return_value={"n": 0, "latest_game_date": None},
        ):
            with redirect_stdout(out):
                rc = validator.main(["--days", "30", "--require-min", "1", "--json"])
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["status"], "fail")
        self.assertEqual(payload["count"], 0)
        self.assertIsNone(payload["latest_game_date"])


if __name__ == "__main__":
    unittest.main()
