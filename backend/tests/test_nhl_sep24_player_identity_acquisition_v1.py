from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.official_request_journal import (
    canonical_game_set_hash,
    verify_player_identity_response_run,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    DEFAULT_OUTPUT,
    ROOT,
    SEP24_PLAYER_IDENTITY_GAME_HASH,
    SEP24_PLAYER_IDENTITY_GAME_IDS,
    SEP24_PLAYER_IDENTITY_ID,
    SEP24_PLAYER_IDENTITY_SLATE,
    _receipt,
    player_identity_acquisition,
)


class _Response:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, body: bytes, status: int = 200):
        self.content = body
        self.status_code = status


class Sep24PlayerIdentityAcquisitionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nhl_identity_acq_", dir=ROOT)
        self.output_root = Path(self.temp.name) / "postgame_reconciliation"
        self.source_binding = {"game_ids": SEP24_PLAYER_IDENTITY_GAME_IDS.copy()}
        self.body = json.dumps({
            "playerId": SEP24_PLAYER_IDENTITY_ID,
            "firstName": {"default": "Test"},
            "lastName": {"default": "Player"},
        }).encode()
        for key in (
            "NHL_OFFICIAL_REQUEST_JOURNAL_REQUIRED", "NHL_RECONCILIATION_RUN_ID",
            "NHL_OFFICIAL_REQUEST_JOURNAL", "NHL_OFFICIAL_RESPONSE_CACHE",
            "NHL_REQUESTED_SLATE_DATE", "NHL_CANONICAL_GAME_SET_HASH",
            "NHL_CANONICAL_GAME_IDS", "NHL_AUTHORIZED_PLAYER_LOOKUP_IDS",
            "NHL_OFFICIAL_RESPONSE_SOURCE_CACHE", "NHL_OFFICIAL_RESPONSE_SOURCE_RUN_ID",
            "NHL_OFFICIAL_RESPONSE_SOURCE_JOURNAL_SHA256",
            "NHL_OFFICIAL_RESPONSE_SOURCE_LEDGER_JSON",
        ):
            os.environ.pop(key, None)

    def tearDown(self):
        self.temp.cleanup()

    def _mock_success(self):
        return patch(
            "backend.nhl.official_request_journal.requests.Session.get",
            return_value=_Response(self.body),
        )

    def test_exact_player_frozen_slate_receipt_and_hash_binding(self):
        with self._mock_success() as request, patch(
                "backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect",
                side_effect=AssertionError("DATABASE_ACCESS_FORBIDDEN")) as database:
            result = player_identity_acquisition(
                slate_date=SEP24_PLAYER_IDENTITY_SLATE,
                player_id=SEP24_PLAYER_IDENTITY_ID,
                source_binding=self.source_binding,
                output_root=self.output_root,
            )
        self.assertEqual(request.call_count, 1)
        self.assertEqual(database.call_count, 0)
        self.assertEqual(result["canonical_game_set_hash"], SEP24_PLAYER_IDENTITY_GAME_HASH)
        self.assertEqual(result["game_ids"], SEP24_PLAYER_IDENTITY_GAME_IDS)
        self.assertEqual(result["player_id"], SEP24_PLAYER_IDENTITY_ID)
        self.assertEqual(result["request_accounting"]["total_network_attempts"], 1)
        self.assertEqual(result["request_accounting"]["total_logical_requests"], 1)
        self.assertEqual(result["request_accounting"]["retries"], 0)
        self.assertEqual(result["database_writes"], 0)
        self.assertEqual(result["other_player_lookups"], 0)

        receipt_path = Path(result["receipt_path"])
        receipt = json.loads(receipt_path.read_text())
        self.assertEqual(receipt["contract_version"], "NHL_PLAYER_IDENTITY_ACQUISITION_V1")
        self.assertEqual(receipt["endpoint_identity"], {
            "slate_date": SEP24_PLAYER_IDENTITY_SLATE,
            "player_id": SEP24_PLAYER_IDENTITY_ID,
        })
        self.assertEqual(receipt["journal_sha256"], result["journal_sha256"])
        self.assertEqual(receipt["tree_fingerprint"], result["tree_fingerprint"])
        resolved = _receipt(
            result["run_id"], role="PLAYER_IDENTITY_RESPONSE_SOURCE",
            slate_date=SEP24_PLAYER_IDENTITY_SLATE, output_root=self.output_root)
        binding = verify_player_identity_response_run(
            self.output_root / "request_runs" / SEP24_PLAYER_IDENTITY_SLATE / result["run_id"],
            expected_run_id=result["run_id"], slate_date=SEP24_PLAYER_IDENTITY_SLATE,
            game_ids=SEP24_PLAYER_IDENTITY_GAME_IDS, expected_player_id=SEP24_PLAYER_IDENTITY_ID,
            repository_root=ROOT, expected_journal_sha256=resolved["journal_sha256"],
            expected_tree_fingerprint=resolved["tree_fingerprint"],
            expected_object_sha256=resolved["object_sha256"],
            expected_index_sha256=resolved["index_sha256"],
        )
        self.assertEqual(binding["response_set_sha256"], receipt["response_set_sha256"])
        self.assertEqual(binding["responses"][0]["object_sha256"], receipt["object_sha256"])

    def test_wrong_player_or_date_is_rejected_before_request(self):
        with self._mock_success() as request:
            for date_value, player in ((SEP24_PLAYER_IDENTITY_SLATE, 8481748),
                                       ("2026-09-23", SEP24_PLAYER_IDENTITY_ID)):
                with self.subTest(date=date_value, player=player):
                    with self.assertRaisesRegex(RuntimeError, "SCOPE_INVALID"):
                        player_identity_acquisition(
                            slate_date=date_value, player_id=player,
                            source_binding=self.source_binding, output_root=self.output_root)
        request.assert_not_called()

    def test_missing_or_extra_slate_ids_are_rejected_before_request(self):
        with self._mock_success() as request:
            for ids in (SEP24_PLAYER_IDENTITY_GAME_IDS[:-1],
                        SEP24_PLAYER_IDENTITY_GAME_IDS + [2026010048]):
                with self.subTest(count=len(ids)):
                    with self.assertRaisesRegex(RuntimeError, "SCOPE_INVALID"):
                        player_identity_acquisition(
                            slate_date=SEP24_PLAYER_IDENTITY_SLATE,
                            player_id=SEP24_PLAYER_IDENTITY_ID,
                            source_binding={"game_ids": ids}, output_root=self.output_root)
        request.assert_not_called()
        self.assertEqual(canonical_game_set_hash(SEP24_PLAYER_IDENTITY_GAME_IDS),
                         SEP24_PLAYER_IDENTITY_GAME_HASH)

    def test_wrong_response_identity_leaves_no_completed_receipt(self):
        wrong = json.dumps({"playerId": 8481748}).encode()
        with patch("backend.nhl.official_request_journal.requests.Session.get",
                   return_value=_Response(wrong)):
            with self.assertRaisesRegex(RuntimeError, "PLAYER_ID_MISMATCH"):
                player_identity_acquisition(
                    slate_date=SEP24_PLAYER_IDENTITY_SLATE,
                    player_id=SEP24_PLAYER_IDENTITY_ID,
                    source_binding=self.source_binding, output_root=self.output_root)
        receipts = list((self.output_root / "acquisition_receipts" /
                         SEP24_PLAYER_IDENTITY_SLATE).glob("*.json"))
        self.assertEqual(receipts, [])

    def test_failed_request_is_not_receipted_and_receipt_scope_tampering_fails(self):
        with patch("backend.nhl.official_request_journal.requests.Session.get",
                   return_value=_Response(b"{}", status=503)):
            with self.assertRaises(Exception):
                player_identity_acquisition(
                    slate_date=SEP24_PLAYER_IDENTITY_SLATE,
                    player_id=SEP24_PLAYER_IDENTITY_ID,
                    source_binding=self.source_binding, output_root=self.output_root)
        receipts = list((self.output_root / "acquisition_receipts" /
                         SEP24_PLAYER_IDENTITY_SLATE).glob("*.json"))
        self.assertEqual(receipts, [])

        with self._mock_success():
            result = player_identity_acquisition(
                slate_date=SEP24_PLAYER_IDENTITY_SLATE,
                player_id=SEP24_PLAYER_IDENTITY_ID,
                source_binding=self.source_binding, output_root=self.output_root)
        path = Path(result["receipt_path"])
        receipt = json.loads(path.read_text())
        receipt["player_id"] = 8481748
        path.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(RuntimeError, "RECEIPT_SCOPE_INVALID"):
            _receipt(result["run_id"], role="PLAYER_IDENTITY_RESPONSE_SOURCE",
                     slate_date=SEP24_PLAYER_IDENTITY_SLATE, output_root=self.output_root)


if __name__ == "__main__":
    unittest.main()
