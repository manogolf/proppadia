import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import analyze_nhl_prediction_quality as quality


class TestSharedNhlPredictionQuality(unittest.TestCase):
    def test_common_cte_escapes_like_percent(self):
        self.assertIn("LIKE 'nhl_%%'", quality.COMMON_CTE)

    def test_pass_with_sufficient_total(self):
        side_effects = [
            [{"total": 20, "correct": 12}],
            [{"prop_type": "sog", "total": 10, "correct": 6}],
            [{"prop_source": "nhl_sog", "total": 20, "correct": 12}],
        ]
        out = StringIO()
        with patch.object(quality, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = quality.main(["--from-date", "2025-12-01", "--to-date", "2025-12-31", "--min-total", "5"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["overall"]["total"], 20)

    def test_fail_with_bad_date(self):
        out = StringIO()
        with redirect_stdout(out):
            rc = quality.main(["--from-date", "12/01/2025", "--to-date", "2025-12-31"])
        self.assertEqual(rc, 2)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("YYYY-MM-DD", payload["error"])


if __name__ == "__main__":
    unittest.main()
