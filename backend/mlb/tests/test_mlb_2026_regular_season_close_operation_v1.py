from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.season_transition import regular_season_close_inventory_v1 as v1
from backend.mlb.season_transition import regular_season_close_inventory_v2 as v2
from backend.mlb.season_transition import regular_season_close_operation_v1 as op


class RegularSeasonCloseOperationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.inputs, cls.rows = op.close_inputs()
        cls.authorization = {
            "schema": op.AUTH_SCHEMA, "authorizes_close": True,
            "authorization_id": "test-fixture-only", "authorized_by": "isolated-test",
            "input_binding": op.expected_authorization_binding(cls.inputs),
        }
        cls.test_inputs = {**cls.inputs, "inventory_sha256": "e" * 64}
        cls.test_authorization = {**cls.authorization,
                                 "input_binding": op.expected_authorization_binding(cls.test_inputs)}
        cls.test_rows = [{
            "game_pk": game_pk, "authoritative_raw_game_type": "R",
            "close_disposition": "AUTHORITATIVELY_CANCELLED" if game_pk == 823490 else "FINAL",
        } for game_pk in [*range(1, 2430), 823490]]

    def test_readiness_failure_blocks_input_preparation(self) -> None:
        with patch.object(op.v2, "validate_close_readiness_package", return_value={
            "integrity_passed": True, "close_ready": False,
        }):
            with self.assertRaisesRegex(v1.CloseInventoryError, "READINESS_NOT_PASSED"):
                op.close_inputs()

    def test_readiness_inputs_and_cancelled_game_semantics(self) -> None:
        self.assertEqual(self.inputs["population_size"], 2430)
        row = next(item for item in self.rows if item["game_pk"] == 823490)
        self.assertEqual(row["close_disposition"], "AUTHORITATIVELY_CANCELLED")
        self.assertNotIn("played_official_date", row)
        self.assertNotIn("final_outcome", row)

    def test_authorization_must_bind_exact_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "auth.json"
            path.write_text(json.dumps(self.authorization))
            self.assertEqual(op.validate_authorization(path, self.inputs)["authorization_id"], "test-fixture-only")
            bad = dict(self.authorization)
            bad["input_binding"] = {**bad["input_binding"], "inventory_sha256": "0" * 64}
            path.write_text(json.dumps(bad))
            with self.assertRaisesRegex(v1.CloseInventoryError, "INPUT_MISMATCH"):
                op.validate_authorization(path, self.inputs)

    def test_postseason_or_unresolved_rows_rejected(self) -> None:
        for change, expected in (
            ({"authoritative_raw_game_type": "S"}, "POSTSEASON"),
            ({"close_disposition": "UNRESOLVED_IDENTITY_OR_STATUS"}, "UNRESOLVED"),
        ):
            changed = [dict(row) for row in self.rows]
            changed[0].update(change)
            with self.assertRaisesRegex(v1.CloseInventoryError, expected):
                op.validate_close_rows(changed)
        changed = [dict(row) for row in self.rows if row["game_pk"] != 823490]
        changed.append({**next(row for row in self.rows if row["game_pk"] == 823490),
                        "played_official_date": "2026-09-28"})
        with self.assertRaisesRegex(v1.CloseInventoryError, "CANCELLATION_SEMANTICS"):
            op.validate_close_rows(changed)

    def test_atomic_publication_is_immutable_and_repeat_fails(self) -> None:
        document = op.close_document(self.test_inputs, self.test_rows,
                                     {**self.test_authorization, "_sha256": "d" * 64})
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "close.json"
            op.publish_immutable(target, document)
            self.assertEqual(json.loads(target.read_text()), document)
            self.assertEqual(op.validate_close_artifact(
                target, expected_inputs=self.test_inputs, expected_rows=self.test_rows), document)
            with self.assertRaisesRegex(v1.CloseInventoryError, "ALREADY_EXISTS"):
                op.publish_immutable(target, document)
            self.assertEqual(json.loads(target.read_text()), document)
            self.assertEqual(list(Path(temp).glob("*.tmp")), [])

    def test_execution_rejects_conflicting_authorization_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            auth = Path(temp) / "auth.json"
            auth.write_text(json.dumps(self.test_authorization))
            output = Path(temp) / "close.json"
            auth.write_text(json.dumps({**self.test_authorization, "authorizes_close": False}))
            with patch.object(op, "close_inputs", return_value=(self.test_inputs, self.test_rows)):
                with self.assertRaisesRegex(v1.CloseInventoryError, "NOT_EXPLICIT"):
                    op.execute_close(auth, output)
            self.assertFalse(output.exists())

    def test_close_artifact_hash_and_input_binding_fail_closed(self) -> None:
        doc = op.close_document(self.test_inputs, self.test_rows,
                                {**self.test_authorization, "_sha256": "d" * 64})
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "close.json"
            path.write_text(json.dumps(doc))
            damaged = dict(doc)
            damaged["game_count"] = 1
            path.write_text(json.dumps(damaged))
            with self.assertRaisesRegex(v1.CloseInventoryError, "ARTIFACT_HASH_MISMATCH"):
                op.validate_close_artifact(path, expected_inputs=self.test_inputs, expected_rows=self.test_rows)
            path.write_text(json.dumps(doc))
            with self.assertRaisesRegex(v1.CloseInventoryError, "ARTIFACT_INPUT_MISMATCH"):
                op.validate_close_artifact(
                    path, expected_inputs={**self.test_inputs, "inventory_sha256": "0" * 64},
                    expected_rows=self.test_rows)


if __name__ == "__main__":
    unittest.main()
