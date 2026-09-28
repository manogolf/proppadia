from __future__ import annotations

import json
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    EVIDENCE, PACKAGE, RETAINED_RELATIONSHIP_FEED_SHA256,
    SOURCE_COMPLETION_RECEIPT,
    _apply_v2_rain_cancellation_interpretation,
    build_inventory, validate_close_readiness_package, validate_package,
)
from backend.mlb.scripts import prepare_mlb_2026_regular_season_close_v1 as close_command
from backend.mlb.season_transition import regular_season_close_inventory_v1 as v1
from backend.mlb.season_transition.game_phase_authority_v1 import load_v1_authority


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
        self.assertEqual(recon["exact_gamepk_live_feed_ids"], 73)
        self.assertEqual(recon["accepted_terminal_from_blockers"], 88)
        self.assertEqual(recon["unresolved"], 0)
        self.assertNotIn(824785, summary["unresolved_game_pks"])
        self.assertEqual(summary["unresolved_game_pks"], [])

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
        self.assertEqual(by_pk[823490]["close_disposition"], "AUTHORITATIVELY_CANCELLED")
        self.assertNotIn("final_outcome", by_pk[823490])
        self.assertNotIn("played_official_date", by_pk[823490])

    def test_823490_rain_cancellation_is_accepted_but_not_played(self) -> None:
        rows, _ = build_inventory()
        row = next(row for row in rows if row["game_pk"] == 823490)
        self.assertEqual(row["close_disposition"], "AUTHORITATIVELY_CANCELLED")
        self.assertNotIn("final_outcome", row)
        self.assertNotIn("played_official_date", row)
        interpretation = row["v2_contract_interpretation"]
        self.assertTrue(any(
            item["raw_status"]["statusCode"] == "CR"
            and item["raw_status"]["detailedState"] == "Cancelled: Rain"
            and item["source_sha256"]
            == "33df6c93eded854287295ffb3fc5b716e6c57e5444d3224d52a205f497ada82b"
            for item in interpretation
        ))
        status_evidence = row["authoritative_status_evidence"]
        self.assertTrue(any(
            artifact["sha256"]
            == "33df6c93eded854287295ffb3fc5b716e6c57e5444d3224d52a205f497ada82b"
            for group in status_evidence
            for artifact in group["source_artifacts"]
        ))
        receipt = json.loads((PACKAGE / SOURCE_COMPLETION_RECEIPT).read_text())
        self.assertEqual(receipt["http_status"], 200)
        self.assertEqual(receipt["request"]["request_count"], 1)
        self.assertEqual(receipt["validation"]["status_code"], "CR")
        self.assertEqual(receipt["validation"]["disposition"], "AUTHORITATIVELY_CANCELLED")

    def test_unknown_cancellation_evidence_stays_unresolved(self) -> None:
        rows, _ = build_inventory()
        row = next(row for row in rows if row["game_pk"] == 823490)
        artifact = next(
            artifact
            for group in row["authoritative_status_evidence"]
            for artifact in group["source_artifacts"]
            if artifact["sha256"]
            == "33df6c93eded854287295ffb3fc5b716e6c57e5444d3224d52a205f497ada82b"
        )
        source = v1.SourceObservation(
            artifact["path"], artifact["sha256"],
            v1._normalize_live_feed_game(json.loads((v1.REPO_ROOT / artifact["path"]).read_text())),
            "STATSAPI_LIVE_GAME_FEED",
        )
        changed = dict(source.game)
        changed["status"] = {**changed["status"], "reason": "Weather"}
        unknown = v1.SourceObservation(
            source.source_path, source.source_sha256, changed,
            source.source_kind, source.observation_timestamp_utc)
        adapted, _ = _apply_v2_rain_cancellation_interpretation(823490, [unknown])
        authority = next(r for r in load_v1_authority(root=v1.REPO_ROOT).records
                         if r.game_pk == 823490)
        classified = v1.classify_authoritative_game(authority, adapted)
        self.assertEqual(classified["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS")
        self.assertIn("UNKNOWN_AUTHORITATIVE_STATUS", classified["disposition_reason"])

    def test_conflicting_final_and_rain_cancellation_stays_unresolved(self) -> None:
        rows, _ = build_inventory()
        row = next(row for row in rows if row["game_pk"] == 823490)
        artifact = next(
            artifact
            for group in row["authoritative_status_evidence"]
            for artifact in group["source_artifacts"]
            if artifact["sha256"]
            == "33df6c93eded854287295ffb3fc5b716e6c57e5444d3224d52a205f497ada82b"
        )
        source = v1.SourceObservation(
            artifact["path"], artifact["sha256"],
            v1._normalize_live_feed_game(json.loads((v1.REPO_ROOT / artifact["path"]).read_text())),
            "STATSAPI_LIVE_GAME_FEED",
        )
        final_game = dict(source.game)
        final_game["status"] = {
            "abstractGameState": "Final", "codedGameState": "F",
            "statusCode": "F", "detailedState": "Final",
        }
        final_game["teams"] = {
            "away": {"team": {"id": 110}, "score": 2},
            "home": {"team": {"id": 147}, "score": 1},
        }
        conflicting = v1.SourceObservation(
            "synthetic_conflicting_terminal", "0" * 64, final_game,
            "STATSAPI_LIVE_GAME_FEED",
        )
        adapted, _ = _apply_v2_rain_cancellation_interpretation(
            823490, [source, conflicting])
        authority = next(r for r in load_v1_authority(root=v1.REPO_ROOT).records
                         if r.game_pk == 823490)
        classified = v1.classify_authoritative_game(authority, adapted)
        self.assertEqual(classified["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS")
        self.assertIn("CONFLICTING_TERMINAL_STATUS", classified["disposition_reason"])

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

    def test_current_close_checker_uses_pinned_v2_and_reports_ready_check_only(self) -> None:
        with patch("sys.argv", ["prepare_mlb_2026_regular_season_close_v1"]), redirect_stdout(io.StringIO()):
            self.assertEqual(close_command.main(), 0)
        report = validate_close_readiness_package()
        self.assertEqual(report["decision"], "REGULAR_SEASON_CLOSE_READY")
        self.assertTrue(report["close_ready"])
        self.assertTrue(report["check_only"])
        self.assertFalse(report["close_package_created"])
        self.assertEqual(report["close_blocker_game_pks"], [])
        self.assertTrue(report["823490_cancellation_nonplayed_check"])
        self.assertEqual(report["population_counts"]["regular_season_game_pks"], 2430)


if __name__ == "__main__":
    unittest.main()
