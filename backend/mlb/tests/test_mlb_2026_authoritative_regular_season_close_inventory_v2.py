from __future__ import annotations

import json
import unittest

from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    EVIDENCE, PACKAGE, RETAINED_RELATIONSHIP_FEED_SHA256,
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
        self.assertEqual(recon["accepted_terminal_from_blockers"], 87)
        self.assertEqual(recon["unresolved"], 1)
        self.assertNotIn(824785, summary["unresolved_game_pks"])
        self.assertEqual(summary["unresolved_game_pks"], [823490])

    def test_exact_date_conflict_cases_use_terminal_feed_played_date(self) -> None:
        rows, _ = build_inventory()
        by_pk = {row["game_pk"]: row for row in rows}
        expected = {
            823489: ("2026-09-25", (110, 147), (3, 6)),
            824703: ("2026-09-25", (112, 111), (3, 4)),
            824705: ("2026-09-27", (112, 111), (6, 2)),
        }
        for game_pk, (played_date, teams, scores) in expected.items():
            row = by_pk[game_pk]
            self.assertEqual(row["close_disposition"], "FINAL")
            self.assertEqual(row["played_official_date"], played_date)
            self.assertIn(played_date, row["observed_official_dates"])
            outcome = row["final_outcome"]
            self.assertEqual((outcome["away_team_id"], outcome["home_team_id"]), teams)
            self.assertEqual((outcome["away_score"], outcome["home_score"]), scores)
            self.assertEqual(
                row["disposition_source_artifact"]["source_kind"],
                "STATSAPI_LIVE_GAME_FEED",
            )
            self.assertEqual(len(row["disposition_source_artifact"]["sha256"]), 64)

    def test_schedule_history_closes_fifteen_feed_gaps_only(self) -> None:
        rows, _ = build_inventory()
        by_pk = {row["game_pk"]: row for row in rows}
        prior_feed_gaps = {
            822841, 823086, 823168, 823327, 823410, 823490, 823492, 823894,
            824060, 824223, 824301, 824625, 824710, 824784, 824868, 824951,
        }
        for game_pk in prior_feed_gaps - {823490}:
            self.assertEqual(by_pk[game_pk]["close_disposition"], "FINAL")
            self.assertEqual(
                by_pk[game_pk]["disposition_source_artifact"]["source_kind"],
                "STATSAPI_SCHEDULE_RESPONSE",
            )
        self.assertEqual(by_pk[823490]["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS")
        self.assertIsNone(by_pk[823490]["final_outcome"])

    def test_824785_relationship_feed_remains_hash_pinned(self) -> None:
        records = [json.loads(line) for line in (PACKAGE / EVIDENCE).read_text().splitlines()]
        self.assertTrue(any(
            row["game_pk"] == 824785
            and row["sha256"] == RETAINED_RELATIONSHIP_FEED_SHA256
            for row in records
        ))

    def test_versioned_package_rebuild_is_valid(self) -> None:
        report = validate_package()
        self.assertTrue(report["integrity_passed"], report["checks"])
        self.assertEqual(report["population_counts"]["regular_season_game_pks"], 2430)


if __name__ == "__main__":
    unittest.main()
