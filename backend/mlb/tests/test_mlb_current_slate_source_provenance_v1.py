from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.app.services.mlb import market_odds_service
from backend.mlb.scripts import build_mlb_predictions_wide as wide_builder
from backend.mlb.scripts import run_mlb_public_game_moneyline_daily_v1 as moneyline
from backend.mlb.shared.current_slate_source_provenance_v1 import (
    filtering_boundary,
    input_status,
    required_input,
    retain_bytes,
    write_receipt,
)


class CurrentSlateSourceProvenanceTests(unittest.TestCase):
    def _capture_mock_odds_response(self, payload: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, object]]:
        raw = json.dumps(payload, separators=(",", ":")).encode()

        class Response:
            status_code = 200
            content = raw

            @staticmethod
            def raise_for_status() -> None:
                return None

            @staticmethod
            def json() -> list[dict[str, object]]:
                return payload

        game_date = "2099-10-09"
        market_odds_service._snapshot_cache.pop(game_date, None)
        market_odds_service._snapshot_source_evidence.pop(game_date, None)
        with patch.dict("os.environ", {"ODDS_API_KEY": "offline-test-key"}), \
             patch.object(market_odds_service, "_markets_query", return_value="batter_hits"), \
             patch.object(market_odds_service, "_experimental_market_keys", return_value=[]), \
             patch.object(market_odds_service.requests, "get", return_value=Response()):
            events = market_odds_service._fetch_market_snapshot(game_date=game_date)
        evidence = market_odds_service.get_market_snapshot_source_evidence(game_date=game_date)
        market_odds_service._snapshot_cache.pop(game_date, None)
        market_odds_service._snapshot_source_evidence.pop(game_date, None)
        assert evidence is not None
        return events, evidence

    def test_valid_empty_source_is_retained_and_hash_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            raw = b'{"dates":[{"date":"2026-10-09","games":[]}]}'
            source = retain_bytes(Path(directory) / "schedule.json", raw)
            self.assertEqual("VALID_EMPTY", input_status(0))
            self.assertEqual(hashlib.sha256(raw).hexdigest(), source["sha256"])
            self.assertEqual(raw, Path(source["path"]).read_bytes())

    def test_missing_input_is_not_misclassified_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                required_input(Path(directory) / "missing.json")

    def test_odds_valid_empty_retains_exact_response_and_zero_counts(self) -> None:
        events, evidence = self._capture_mock_odds_response([])
        self.assertEqual([], events)
        self.assertEqual(0, evidence["raw_event_count"])
        self.assertEqual(0, evidence["slate_event_count"])
        self.assertEqual(b"[]", evidence["responses"][0]["raw_bytes"])

    def test_wide_snapshot_writer_retains_explicit_empty_event_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            out_path = Path(directory) / "odds.json"
            metadata = wide_builder._write_odds_snapshot_json(
                out_path=out_path, slate_date="2099-10-09", events=[]
            )
            payload = json.loads(Path(metadata["tagged_path"]).read_text())
            self.assertEqual(0, payload["event_count"])
            self.assertEqual([], payload["events"])

    def test_moneyline_current_schedule_receipt_distinguishes_valid_empty(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                raw = b'{"dates":[]}'
                source = moneyline._retain_current_schedule(
                    raw, run_identity="local_daily_test", slate_date="2099-10-09",
                    source_identity="fixture://statsapi/schedule?date=2099-10-09",
                    acquired_at_utc="2099-10-09T12:00:00Z",
                )
                moneyline._CURRENT_SLATE_EVIDENCE.clear()
                moneyline._CURRENT_SLATE_EVIDENCE.update({
                    "run_identity": "local_daily_test", "requested_slate_date": "2099-10-09",
                    "acquired_at_utc": "2099-10-09T12:00:00Z", "current_schedule": source,
                    "schedule_status": "VALID_EMPTY",
                })
                receipt = moneyline._write_current_slate_receipt(
                    status="VALID_EMPTY", counts={"schedule_games": 0, "scored_rows": 0, "admitted_rows": 0},
                )
                payload = json.loads(Path(receipt["path"]).read_text())
                self.assertEqual("VALID_EMPTY", payload["schedule_status"])
                self.assertEqual(0, payload["counts"]["schedule_games"])
                self.assertEqual(source["sha256"], payload["current_schedule"]["sha256"])
                self.assertIn("local_daily_test.json", payload["moneyline_attempt_receipt_path"])
            finally:
                moneyline._CURRENT_SLATE_EVIDENCE.clear()
                moneyline._ATTEMPT_CONTEXT.clear()
                os.chdir(original_cwd)

    def test_odds_current_slate_filter_rejection_is_counted(self) -> None:
        events, evidence = self._capture_mock_odds_response([
            {"id": "on-date", "commence_time": "2099-10-09T18:00:00Z"},
            {"id": "other-date", "commence_time": "2099-10-10T18:00:00Z"},
        ])
        self.assertEqual(["on-date"], [event["id"] for event in events])
        self.assertEqual(2, evidence["raw_event_count"])
        self.assertEqual(1, evidence["slate_event_count"])

    def test_filtering_boundary_accounts_for_dropped_rows_and_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            boundary = filtering_boundary(5, 2)
            self.assertEqual({"input_rows": 5, "output_rows": 2, "rejected_rows": 3}, boundary)
            with self.assertRaises(ValueError):
                filtering_boundary(2, 3)
            payload = {"schema_version": "MLB_CURRENT_SLATE_SOURCE_PROVENANCE_V1",
                       "counts": {"events": 5, "offers": 2, "event_offer_boundary": boundary}}
            result = write_receipt(Path(directory) / "receipt.json", payload)
            stored = Path(result["path"]).read_bytes()
            self.assertEqual(hashlib.sha256(stored).hexdigest(), result["sha256"])
            self.assertEqual(3, json.loads(stored)["counts"]["event_offer_boundary"]["rejected_rows"])


if __name__ == "__main__":
    unittest.main()
