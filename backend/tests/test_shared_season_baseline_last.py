import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from backend.scripts import season_baseline_last as sbl


class TestSharedSeasonBaselineLast(unittest.TestCase):
    def test_build_payload_fails_when_missing(self):
        with TemporaryDirectory() as td:
            payload = sbl.build_payload(Path(td))
            self.assertFalse(payload["ok"])
            self.assertFalse(payload["latest"]["mlb"]["exists"])
            self.assertFalse(payload["latest"]["nhl"]["exists"])

    def test_build_payload_reads_latest_files(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            mlb = root / "mlb_quality_games_30_120.json"
            nhl = root / "nhl_quality_2025-12-01_2025-12-31.json"
            mlb.write_text('{"status":"pass","overall":{"total":123,"accuracy_pct":56.7}}', encoding="utf-8")
            nhl.write_text('{"status":"pass","overall":{"total":77,"accuracy_pct":51.2}}', encoding="utf-8")

            payload = sbl.build_payload(root)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["latest"]["mlb"]["overall_total"], 123)
            self.assertEqual(payload["latest"]["nhl"]["overall_total"], 77)


if __name__ == "__main__":
    unittest.main()
