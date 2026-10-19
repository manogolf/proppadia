"""Points feature exporters keep unknown shot attempts distinct from zero."""
import re
import unittest
from pathlib import Path


SQL = Path(__file__).resolve().parents[1] / "sql"
EXPORTERS = ("export_points.sql", "export_points_history_challenger.sql")


class PointsAttemptMissingnessSqlTests(unittest.TestCase):
    def test_raw_attempts_are_never_coalesced_to_zero(self):
        for name in EXPORTERS:
            with self.subTest(exporter=name):
                text = (SQL / name).read_text()
                self.assertNotRegex(text, r"COALESCE\(\s*l\.shot_attempts\s*,\s*0\s*\)")

    def test_attempt_window_sums_skip_unknown_rows(self):
        for name in EXPORTERS:
            with self.subTest(exporter=name):
                text = re.sub(r"\s+", " ", (SQL / name).read_text())
                self.assertNotRegex(text, r"THEN shot_attempts ELSE 0 END")
                self.assertRegex(
                    text, r"AND shot_attempts IS NOT NULL THEN shot_attempts ?\* ?60\.0 ?/ ?toi_minutes")


if __name__ == "__main__":
    unittest.main()
