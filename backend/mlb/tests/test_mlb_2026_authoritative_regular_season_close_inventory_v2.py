from __future__ import annotations

import unittest

from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    build_inventory, validate_package,
)


class RegularSeasonCloseInventoryV2Tests(unittest.TestCase):
    def test_population_and_exact_authority(self) -> None:
        rows, summary = build_inventory()
        self.assertEqual(len(rows), 2430)
        self.assertEqual(summary["population_counts"]["preseason_game_pks"], 489)
        self.assertTrue(all(row["authoritative_raw_game_type"] == "R" for row in rows))
        self.assertEqual(len({row["game_pk"] for row in rows}), 2430)

    def test_reconciles_all_prior_blockers_without_calendar_inference(self) -> None:
        _, summary = build_inventory()
        recon = summary["reconciliation"]
        self.assertEqual(recon["prior_blockers"], 88)
        self.assertEqual(recon["exact_gamepk_live_feed_ids"], 72)
        self.assertEqual(recon["accepted_terminal_from_blockers"], 69)
        self.assertEqual(recon["unresolved"], 19)
        self.assertNotIn(824785, summary["unresolved_game_pks"])
        self.assertEqual(len(summary["unresolved_game_pks"]), 19)

    def test_versioned_package_rebuild_is_valid(self) -> None:
        report = validate_package()
        self.assertTrue(report["integrity_passed"], report["checks"])
        self.assertEqual(report["population_counts"]["regular_season_game_pks"], 2430)


if __name__ == "__main__":
    unittest.main()
