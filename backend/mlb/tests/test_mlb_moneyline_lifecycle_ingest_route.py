"""Restricted ingest writer route for the MLB moneyline shadow lifecycle."""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[3]
FORWARD = ROOT / "backend/mlb/sql/migrations/20261019_mlb_moneyline_lifecycle_ingest_write.sql"
ROLLBACK = ROOT / "backend/mlb/sql/migrations/20261019_mlb_moneyline_lifecycle_ingest_write_rollback.sql"
STORE = ROOT / "backend/mlb/public_game_predictions/durable_store_v1.py"
HOOK = ROOT / "bin/mlb_public_game_moneyline_daily_hook.sh"


def _grants(sql: str, verb: str, privilege: str) -> dict[str, set[str]]:
    pattern = rf"{verb} {privilege} \((.*?)\)\s*ON ([\w.]+) (?:TO|FROM) proppadia_mlb_ingest;"
    return {
        relation: {column.strip() for column in columns.split(",")}
        for columns, relation in re.findall(pattern, sql, flags=re.DOTALL)
    }


def _store_inserts() -> dict[str, set[str]]:
    source = STORE.read_text(encoding="utf-8")
    return {
        relation: {column.strip() for column in columns.split(",")}
        for relation, columns in re.findall(r"INSERT INTO (mlb\.\w+)\s*\((.*?)\)\s*VALUES", source, flags=re.DOTALL)
    }


class LifecycleRouteTests(unittest.TestCase):
    def test_hook_runs_the_lifecycle_on_restricted_ingest(self) -> None:
        hook = HOOK.read_text(encoding="utf-8")
        self.assertIn(
            'MLB_USE_RESTRICTED_INGEST=1 MLB_RUN_IDENTITY="${run_identity}" .venv/bin/python -m '
            "backend.mlb.scripts.run_mlb_public_game_moneyline_daily_v1",
            hook,
        )
        self.assertEqual(hook.count("run_mlb_public_game_moneyline_daily_v1"), 1)

    def test_hook_publishes_the_phase_authority_chain(self) -> None:
        hook = HOOK.read_text(encoding="utf-8")
        self.assertIn(
            "MLB_PHASE_AUTHORITY_CHAIN_PUBLISH=1 MLB_USE_RESTRICTED_INGEST=1 MLB_RUN_IDENTITY=", hook
        )
        self.assertIn('"phase_chain_publication"', hook)

    def test_insert_grants_match_the_durable_store_inserts_exactly(self) -> None:
        sql = FORWARD.read_text(encoding="utf-8")
        self.assertEqual(_grants(sql, "GRANT", "INSERT"), _store_inserts())

    def test_select_grants_cover_reads_and_corrections_stay_read_only(self) -> None:
        sql = FORWARD.read_text(encoding="utf-8")
        selects = _grants(sql, "GRANT", "SELECT")
        inserts = _grants(sql, "GRANT", "INSERT")
        self.assertNotIn("mlb.public_game_official_final_corrections", inserts)
        self.assertEqual(len(selects["mlb.public_game_official_final_corrections"]), 10)
        # Primary-key columns for every ON CONFLICT target are readable.
        keys = {
            "mlb.public_game_official_finals": {"game_pk"},
            "mlb.public_game_team_state_snapshots": {"model_version", "prediction_cutoff_utc"},
            "mlb.public_game_moneyline_predictions": {"game_date", "game_id", "model_version", "prediction_snapshot_class"},
            "mlb.public_game_moneyline_outcomes": {"game_date", "game_id", "model_version", "prediction_snapshot_class"},
        }
        for relation, columns in keys.items():
            self.assertLessEqual(columns, selects[relation], relation)
        self.assertLessEqual({"prediction_payload", "admission_status", "payload_sha256"},
                             selects["mlb.public_game_moneyline_predictions"])
        self.assertIn("state_hash", selects["mlb.public_game_team_state_snapshots"])
        self.assertIn("payload_sha256", selects["mlb.public_game_moneyline_outcomes"])

    def test_no_mutating_privilege_and_rollback_matches(self) -> None:
        sql = FORWARD.read_text(encoding="utf-8")
        body = sql[: sql.index("-- Verification")]
        for forbidden in ("GRANT UPDATE", "GRANT DELETE", "GRANT TRUNCATE", "GRANT ALL", "GRANT USAGE",
                          "CREATE POLICY", "ALTER ROLE", "CREATE ROLE", "SECURITY DEFINER"):
            self.assertNotIn(forbidden, body.upper())
        self.assertIn("NOT APPLIED", sql)
        rollback = ROLLBACK.read_text(encoding="utf-8")
        for privilege in ("SELECT", "INSERT"):
            self.assertEqual(_grants(rollback, "REVOKE", privilege), _grants(sql, "GRANT", privilege))

    def test_verification_runs_without_set_role(self) -> None:
        verification = FORWARD.read_text(encoding="utf-8").split("-- Verification", 1)[1]
        self.assertNotIn("SET ROLE", verification.upper())
        self.assertIn("has_column_privilege('proppadia_mlb_ingest'", verification)
        self.assertIn("has_table_privilege('proppadia_mlb_ingest', rel, priv)", verification)


class DurableStoreConnectionTests(unittest.TestCase):
    def test_store_connections_use_the_ingest_login_under_the_opt_in(self) -> None:
        from backend.mlb.public_game_predictions import durable_store_v1 as store
        from backend.shared.db import pg

        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        env = {"MLB_USE_RESTRICTED_INGEST": "1", "MLB_INGEST_DATABASE_URL": "postgresql://ingest"}
        with patch.dict(os.environ, env, clear=True), \
             patch.object(pg.psycopg, "connect", return_value=conn) as connect, \
             patch.object(pg, "verify_mlb_ingest_production_target") as target, \
             patch.object(pg, "configure_mlb_ingest_connection") as configure:
            self.assertFalse(store.designated_snapshot_exists("2026-10-19", "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"))
        self.assertEqual(connect.call_args.args[0], "postgresql://ingest")
        target.assert_called_once_with("postgresql://ingest")
        configure.assert_called_once()


if __name__ == "__main__":
    unittest.main()
