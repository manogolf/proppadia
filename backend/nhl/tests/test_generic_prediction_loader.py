from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.scripts import load_nhl_predictions_generic as loader


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "nhldaily_20261010T192919232680Z_b65172b4"
PREDICTIONS = ROOT / "backend/nhl/data/processed/daily_runs" / RUN_ID / "points_predictions.csv"
PREDICTION_SHA256 = "acff4810365c221872c0ac58d91179e8a57692daef60e7be6314732af7e76b8a"
BASE_FEATURE_HASH = (
    "NHL_POINTS_COUNT_HGB_V1:"
    "a53f43d5caea9a64ee8e6d48daf8072e0332b629998242639b2ba15be743e5c2"
)
MODEL_PARAMS = {
    "fitted_model_identity_sha256": "a53f43d5caea9a64ee8e6d48daf8072e0332b629998242639b2ba15be743e5c2",
    "model_artifact_sha256": "e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9",
    "feature_contract_sha256": "77fe5795448422802ad80a91c75a9818e3cd6a27622a7d8fc570d63974049566",
    "history_contract": "120_DAY_LEGACY_BOUND",
}


class FakeCursor:
    def __init__(self):
        self.params = None
        self.statement = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, params=None):
        self.statement = statement
        self.params = params

    def fetchall(self):
        return []


class FakeConnection:
    def __init__(self):
        self.test_cursor = FakeCursor()
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self.test_cursor

    def commit(self):
        self.committed = True


class GenericPredictionLoaderTests(unittest.TestCase):
    def run_hgb_loader(self, connection, *, path=PREDICTIONS, expected_sha=PREDICTION_SHA256,
                       run_id=RUN_ID, model_family="hist_gradient_boosting",
                       model_version="NHL_POINTS_COUNT_HGB_V1", prop="player_points",
                       feature_hash=BASE_FEATURE_HASH, params=MODEL_PARAMS):
        argv = [
            "load_nhl_predictions_generic.py", "--pred-csv", str(path), "--project", "nhl",
            "--prop", prop, "--model-family", model_family, "--model-version", model_version,
            "--feature-hash", feature_hash, "--model-params-json", json.dumps(params),
            "--expected-sha256", expected_sha,
        ]
        with patch.dict(os.environ, {"SUPABASE_DB_URL": "postgresql://test.invalid/test"}), \
             patch.object(sys, "argv", argv), \
             patch.object(loader.psycopg, "connect", return_value=connection) as connect:
            loader.main()
        return connect

    def test_retained_hgb_artifact_loads_and_binds_metadata_and_run(self):
        self.assertEqual(hashlib.sha256(PREDICTIONS.read_bytes()).hexdigest(), PREDICTION_SHA256)
        connection = FakeConnection()
        self.run_hgb_loader(connection)
        self.assertTrue(connection.committed)
        payload = json.loads(connection.test_cursor.params[0])
        self.assertEqual(len(payload), 1743)
        self.assertEqual(len({(r["game_id"], r["player_id"], r["line"]) for r in payload}), 1743)
        self.assertEqual({r["game_id"] for r in payload},
                         set(range(2026020072, 2026020084)))
        self.assertEqual({r["line"] for r in payload}, {0.5, 1.5, 2.5})
        self.assertTrue(all(r["model_family"] == "hist_gradient_boosting" for r in payload))
        self.assertTrue(all(r["model_version"] == "NHL_POINTS_COUNT_HGB_V1" for r in payload))
        self.assertTrue(all(r["feature_hash"] == f"{BASE_FEATURE_HASH}:run:{RUN_ID}" for r in payload))
        self.assertTrue(all(r["model_params"]["fitted_model_identity_sha256"]
                            == MODEL_PARAMS["fitted_model_identity_sha256"] for r in payload))
        self.assertTrue(all(r["model_params"]["parent_daily_run_id"] == RUN_ID for r in payload))
        self.assertIn("ON CONFLICT (prop, player_id, game_id, line, feature_hash)",
                      connection.test_cursor.statement)

    def test_expected_hash_mismatch_fails_before_database_connect(self):
        with patch.object(sys, "argv", [
            "loader", "--pred-csv", str(PREDICTIONS), "--project", "nhl",
            "--prop", "player_points", "--expected-sha256", "0" * 64,
        ]), patch.object(loader.psycopg, "connect") as connect:
            with self.assertRaisesRegex(SystemExit, "Prediction artifact hash mismatch"):
                loader.main()
        connect.assert_not_called()

    def test_run_scope_is_retry_stable_and_distinct_between_same_day_runs(self):
        first = loader.persistence_feature_hash(BASE_FEATURE_HASH, RUN_ID, "player_points")
        retry = loader.persistence_feature_hash(BASE_FEATURE_HASH, RUN_ID, "player_points")
        later = loader.persistence_feature_hash(
            BASE_FEATURE_HASH, RUN_ID + "_retry2", "player_points")
        self.assertEqual(first, retry)
        self.assertNotEqual(first, later)
        self.assertEqual(loader.persistence_feature_hash("phoenix_v2", RUN_ID, "goalie_saves"),
                         "phoenix_v2")

    def test_phoenix_points_and_saves_keep_model_family_and_run_context(self):
        for prop, family, version, feature in (
            ("player_points", "phoenix", "phoenix_v2", "phoenix_v2"),
            ("goalie_saves", "phoenix", "phoenix_v2", "phoenix_v2"),
        ):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "predictions.csv"
                with path.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=[
                        "game_id", "player_id", "line", "prob_over", "parent_daily_run_id",
                    ])
                    writer.writeheader()
                    writer.writerow({"game_id": 1, "player_id": 2, "line": 0.5,
                                     "prob_over": 0.4, "parent_daily_run_id": RUN_ID})
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                connection = FakeConnection()
                self.run_hgb_loader(connection, path=path, expected_sha=digest, prop=prop,
                                    model_family=family, model_version=version, feature_hash=feature)
                payload = json.loads(connection.test_cursor.params[0])
                self.assertEqual(payload[0]["model_family"], family)
                self.assertEqual(payload[0]["model_version"], version)
                expected_feature_hash = (
                    f"{feature}:run:{RUN_ID}" if prop == "player_points" else feature)
                self.assertEqual(payload[0]["feature_hash"], expected_feature_hash)

    def test_conflicting_duplicate_keys_in_input_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.csv"
            fields = ["game_id", "player_id", "line", "prob_over"]
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerow({"game_id": 1, "player_id": 2, "line": 0.5, "prob_over": 0.4})
                writer.writerow({"game_id": 1, "player_id": 2, "line": 0.5, "prob_over": 0.5})
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            connection = FakeConnection()
            with self.assertRaisesRegex(SystemExit, "Conflicting duplicate prediction key"):
                self.run_hgb_loader(connection, path=path, expected_sha=digest,
                                    run_id="", params={})
            self.assertFalse(connection.committed)


if __name__ == "__main__":
    unittest.main()
