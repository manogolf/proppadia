import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from backend.scripts import check_season_baseline_artifacts as cba


class TestSharedSeasonBaselineArtifacts(unittest.TestCase):
    def test_build_payload_fails_when_missing_files(self):
        with TemporaryDirectory() as td:
            payload = cba.build_payload(Path(td))
            self.assertFalse(payload["ok"])
            self.assertIn("missing_mlb_baseline", payload["errors"])
            self.assertIn("missing_nhl_baseline", payload["errors"])

    def test_build_payload_passes_when_files_exist(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            (root / "mlb_quality_games_30_120.json").write_text("{}", encoding="utf-8")
            (root / "nhl_quality_2025-12-01_2025-12-31.json").write_text("{}", encoding="utf-8")
            payload = cba.build_payload(root)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["errors"], [])

    def test_build_payload_enforces_max_age(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            mlb = root / "mlb_quality_games_30_120.json"
            nhl = root / "nhl_quality_2025-12-01_2025-12-31.json"
            mlb.write_text("{}", encoding="utf-8")
            nhl.write_text("{}", encoding="utf-8")
            with patch.object(cba, "_age_hours", return_value=50.0):
                payload = cba.build_payload(root, max_age_hours=24)
            self.assertFalse(payload["ok"])
            self.assertIn("mlb_baseline_stale", payload["errors"])
            self.assertIn("nhl_baseline_stale", payload["errors"])


if __name__ == "__main__":
    unittest.main()
