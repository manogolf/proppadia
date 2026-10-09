import csv
import hashlib
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_1_5_deep_analysis/2026-10-03_through_2026-10-08"


class DeepSog15PackageTest(unittest.TestCase):
    def rows(self, name):
        with (OUT / name).open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def test_census_and_daily_reconcile(self):
        summary = json.loads((OUT / "summary.json").read_text())
        rows = self.rows("analysis_rows.csv")
        settled = [r for r in rows if r["grade_status"] == "SETTLED"]
        self.assertEqual(len(rows), summary["population"])
        self.assertEqual(len(settled), summary["settled"])
        keys = [(r["slate_date"], r["game_id"], r["player_id"]) for r in rows]
        self.assertEqual(len(keys), len(set(keys)))
        daily = self.rows("daily_1_5_performance.csv")
        self.assertEqual(sum(int(r["settled"]) for r in daily), len(settled))
        self.assertEqual(sum(int(r["unresolved"]) for r in daily), summary["unresolved"])
        self.assertEqual(sum(int(r["over_calls"]) + int(r["under_calls"]) for r in daily), len(settled))

    def test_postgame_counterfactuals_are_labeled(self):
        rows = self.rows("rate_exposure_decomposition.csv")
        self.assertTrue(rows)
        self.assertTrue(all(r["counterfactuals_postgame_invalid_for_prediction"] == "True" for r in rows))

    def test_shadow_arms_are_exact_common_row_summaries(self):
        rows = self.rows("shadow_1_5_common_rows.csv")
        self.assertEqual({r["arm"] for r in rows}, {
            "A_PRIOR_SEASON_CARRY_FORWARD", "B_PRIOR_SEASON_RECENCY_WEIGHTED",
            "C_MULTISEASON_SHRUNK_PLAYER", "D_PLAYER_ROLE_HIERARCHICAL",
            "F_CURRENT_PRESEASON_UPDATE", "G_COLD_START_TO_CURRENT_SEASON_BLEND",
        })
        self.assertTrue(all(int(r["common_rows"]) > 0 for r in rows))

    def test_package_checksums_cover_all_deliverables(self):
        manifest = {}
        for line in (OUT / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ", 1)
            manifest[name] = digest
        for name, digest in manifest.items():
            self.assertEqual(hashlib.sha256((OUT / name).read_bytes()).hexdigest(), digest)
        self.assertIn("unresolved_rows.csv", manifest)


if __name__ == "__main__":
    unittest.main()
