from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.scripts.attach_mlb_hits05_full_board_markets_v1 import attach_date
from backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 import (
    MODEL_PATH,
    score_fixture,
    score_prepared_features,
    sha256_file,
    verified_model_bundle,
)
from backend.mlb.scripts.validate_mlb_hits05_full_board_shadow_v1 import validate


class Hits05FullBoardShadowV1Test(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.ledger = self.root / "shadow.sqlite3"
        self.fixture = self.root / "fixture.json"
        self.fixture.write_text(json.dumps({"rows": [{
            "slate_date": "2099-01-02",
            "game_id": 990001,
            "player_id": 660001,
            "player_name": "Process Fixture Hitter",
            "team": "NYY",
            "opponent": "BOS",
            "scheduled_start_utc": "2099-01-02T20:00:00Z",
            "prepared_features": {},
        }]}))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _score_fixture(self) -> dict:
        return score_fixture(
            self.fixture,
            capture_time=datetime(2099, 1, 2, 12, tzinfo=timezone.utc),
            ledger_path=self.ledger,
            run_tag="fixture_process_only",
        )

    def test_exact_model_binding_and_no_market_features(self) -> None:
        bundle = verified_model_bundle()
        self.assertEqual(sha256_file(MODEL_PATH), ledger.MODEL_HASH)
        columns = bundle["meta"]["input_columns"]
        self.assertEqual(len(columns), 73)
        self.assertFalse(any("market" in name or "odds" in name or "price" in name for name in columns))

    def test_direct_score_is_deterministic(self) -> None:
        first = score_prepared_features({})
        second = score_prepared_features({})
        self.assertEqual(first["probability_over"], second["probability_over"])
        self.assertEqual(first["model_input_vector"], second["model_input_vector"])

    def test_process_fixture_is_pregame_and_not_prospective(self) -> None:
        result = self._score_fixture()
        self.assertEqual(result["run_status"], "PASS_PROCESS_ONLY_NOT_PROSPECTIVE_EVIDENCE")
        connection = ledger.connect_ledger(self.ledger)
        payload = json.loads(connection.execute("SELECT prediction_payload_json FROM hits05_full_board_predictions").fetchone()[0])
        self.assertEqual(payload["evidence_mode"], "PROCESS_ONLY_REPLAY")
        self.assertEqual(payload["outcomes_accessed_during_scoring"], 0)
        self.assertFalse(payload["market_observation_required_for_admission"])

    def test_poststart_fixture_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "FIXTURE_NOT_STRICT_PREGAME"):
            score_fixture(
                self.fixture,
                capture_time=datetime(2099, 1, 3, tzinfo=timezone.utc),
                ledger_path=self.ledger,
                run_tag="late_fixture",
            )

    def test_prediction_and_outcome_are_immutable(self) -> None:
        self._score_fixture()
        connection = ledger.connect_ledger(self.ledger)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "APPEND_ONLY"):
            connection.execute("UPDATE hits05_full_board_predictions SET probability_over=0.1")
        identity = connection.execute("SELECT canonical_identity FROM hits05_full_board_predictions").fetchone()[0]
        payload = {
            "slate_date": "2099-01-02", "game_id": 990001, "player_id": 660001,
            "actual_hits": 1, "appearance_status": "APPEARANCE_RESOLVED",
            "outcome_status": "CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT",
            "grading_timestamp_utc": "2099-01-03T12:00:00Z", "grading_source": "FIXTURE",
            "grading_source_sha256": "a" * 64,
        }
        self.assertEqual(ledger.append_outcome(connection, identity, payload), "APPENDED_NEW")
        changed = {**payload, "actual_hits": 0}
        self.assertEqual(ledger.append_outcome(connection, identity, changed), "EXISTING_OUTCOME_CONFLICT_PRESERVED")

    def test_market_attaches_after_prediction_without_changing_population(self) -> None:
        self._score_fixture()
        lineage = self.root / "prediction_lineage_ledger.csv"
        identity = {"game_date": "2099-01-02", "game_id": 990001, "player_id": 660001, "prop_type": "hits", "line": 0.5}
        row = {
            "canonical_row_identity": json.dumps(identity), "lineage_status": "LINEAGE_CERTIFIED",
            "odds_snapshot_timestamp": "2099-01-02T19:40:00Z", "scheduled_game_start": "2099-01-02T20:00:00Z",
            "price_over_american": "-120", "price_under_american": "+100", "bookmaker_key": "betonlineag",
            "market_provider_origin_family": "fixture",
        }
        with lineage.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row))
            writer.writeheader(); writer.writerow(row)
        before = ledger.counts(ledger.connect_ledger(self.ledger))["prediction_rows"]
        result = attach_date("2099-01-02", self.ledger, lineage)
        repeated = attach_date("2099-01-02", self.ledger, lineage)
        after = ledger.counts(ledger.connect_ledger(self.ledger))["prediction_rows"]
        self.assertEqual(result["observations_added"], 1)
        self.assertEqual(repeated["observations_existing"], 1)
        self.assertEqual(ledger.counts(ledger.connect_ledger(self.ledger))["market_observation_rows"], 1)
        self.assertEqual(before, after)

    def test_validator_passes_clean_process_ledger(self) -> None:
        self._score_fixture()
        result = validate(self.ledger, None)
        self.assertEqual(result["status"], "PASS", result)


if __name__ == "__main__":
    unittest.main()
