"""Signed DB phase-authority chain: writer, verifier, hosted read, SQL contract.

Uses committed files and an in-memory connection only; no database or raw sources.
"""
from __future__ import annotations

import functools
import hashlib
import json
import re
import shutil
import tempfile
import unittest
import unittest.mock
from dataclasses import replace
from pathlib import Path

from backend.mlb.season_transition import phase_authority_db_chain_v1 as chain_module
from backend.mlb.season_transition.game_phase_authority_v1 import (
    REPO_ROOT,
    SOURCE_VERIFICATION_RAW_BYTES,
    GamePhaseAuthorityError,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.phase_authority_db_chain_v1 import (
    KEY_ENV,
    PhaseChainError,
    extend_public_authority,
    publish_runtime_records,
    signing_key_id,
)

KEY = bytes(range(32))
ENV = {KEY_ENV: KEY.hex()}
MIGRATION = REPO_ROOT / "backend/mlb/sql/migrations/20261020_mlb_phase_authority_db_chain.sql"
ROLLBACK = REPO_ROOT / "backend/mlb/sql/migrations/20261020_mlb_phase_authority_db_chain_rollback.sql"
SOURCE_SHA = "a" * 64


def world_series_record(pk: int = 849900, game_type: str = "W", round_label: str = "World Series"):
    return GamePhaseAuthorityRecord(
        game_pk=pk, source_season=2026, source_game_type=game_type, season_phase="POSTSEASON",
        postseason_round={"W": "WORLD_SERIES", "L": "LEAGUE_CHAMPIONSHIP_SERIES"}[game_type],
        season_name="MLB_2026_POSTSEASON", source_round=round_label, schedule_relationships={},
        primary_source_path="artifacts/ops/history.json", primary_source_sha256=SOURCE_SHA,
        source_paths=("artifacts/ops/history.json",), source_hashes=(SOURCE_SHA,),
        phase_decision="CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE",
        authority_status="AUTHORITATIVE_UNAMBIGUOUS", scheduled_start_utc="2026-10-24T00:08:00Z",
    )


class FullyVerifiedBase:
    """The committed snapshot, labelled as the local fully verified base."""

    source_verification = SOURCE_VERIFICATION_RAW_BYTES

    def __init__(self, base):
        self._base = base

    def __getattr__(self, name):
        return getattr(self._base, name)


class FakeRuntime:
    def __init__(self, base, records):
        self.base = base
        self.records = {record.game_pk: record for record in records}

    def is_runtime_exact(self, pk):
        return int(pk) in self.records

    def lookup_exact(self, pk):
        return self.records[int(pk)]


class FakeChainDatabase:
    """In-memory table plus the append function's checks from the migration."""

    def __init__(self):
        self.rows: list[dict] = []

    def connect(self):
        return FakeConnection(self)

    def append(self, chain_id, sequence, game_pk, prev, entry_sha, text, signature, key_id):
        entry = json.loads(text)
        if (hashlib.sha256(text.encode()).hexdigest() != entry_sha or entry["sequence"] != sequence
                or entry["prev_entry_sha256"] != prev or entry["record"]["game_pk"] != game_pk
                or entry["record"]["season_phase"] != "POSTSEASON"):
            raise RuntimeError("22023")
        if any(row["game_pk"] == game_pk for row in self.rows):
            raise RuntimeError("23505")
        expected_prev = self.rows[-1]["entry_sha256"] if self.rows else entry["anchor_descriptor_sha256"]
        if sequence != len(self.rows) + 1 or prev != expected_prev:
            raise RuntimeError("23514")
        self.rows.append({
            "chain_id": chain_id, "sequence": sequence, "game_pk": game_pk,
            "prev_entry_sha256": prev, "entry_sha256": entry_sha, "entry_text": text,
            "signature_hmac_sha256": signature, "signing_key_id": key_id,
        })
        return 1


class FakeConnection:
    def __init__(self, db):
        self.db = db
        self.result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self

    def execute(self, sql, params):
        if "append_phase_authority_chain_entry" in sql:
            self.result = [{"result_count": self.db.append(*params)}]
        else:
            self.result = [dict(row) for row in self.db.rows if row["chain_id"] == params[0]]

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None

    def commit(self):
        pass


class DbChainTests(unittest.TestCase):
    def setUp(self) -> None:
        (REPO_ROOT / "tmp").mkdir(exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix="db_chain_", dir=REPO_ROOT / "tmp"))
        self.addCleanup(shutil.rmtree, directory, True)
        self.config_path = directory / "phase_authority_db_chain.json"
        config = json.loads(chain_module.CONFIG_PATH.read_text())
        config["signing_key_id"] = signing_key_id(KEY)
        self.config_path.write_text(json.dumps(config))
        self.base = HashedProposalAuthority(verify_source_bytes=False)
        self.db = FakeChainDatabase()

    def publish(self, *records, **kwargs):
        runtime = FakeRuntime(FullyVerifiedBase(self.base), records)
        return publish_runtime_records(
            runtime, [record.game_pk for record in records], selection_receipt_sha256="b" * 64,
            connect=self.db.connect, environ=ENV, config_path=self.config_path, **kwargs)

    def extend(self, **kwargs):
        options = {"connect": self.db.connect, "environ": ENV, "config_path": self.config_path}
        options.update(kwargs)
        return extend_public_authority(self.base, **options)

    # Round trip -------------------------------------------------------------
    def test_published_game_is_served_by_verified_read(self) -> None:
        self.assertEqual(self.publish(world_series_record())["appended_game_pks"], [849900])
        self.assertEqual(self.publish(world_series_record(849901))["entry_count"], 2)
        authority, status = self.extend()
        self.assertEqual(status["status"], "DB_CHAIN_VERIFIED")
        self.assertEqual(status["extension_game_count"], 2)
        record = authority.lookup_exact(849900)
        self.assertEqual((record.season_phase, record.postseason_round), ("POSTSEASON", "WORLD_SERIES"))
        self.assertTrue(authority.is_runtime_exact(849900))
        self.assertEqual(authority.lookup_exact(849815).source_round, "AL Championship Series")
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            authority.lookup_exact(849999)
        self.assertEqual(caught.exception.code, "GAME_PHASE_ABSENT")
        self.assertEqual(self.db.rows[0]["prev_entry_sha256"],
                         json.loads(self.config_path.read_text())["anchor_descriptor_sha256"])

    def test_republication_is_idempotent_and_conflicts_fail(self) -> None:
        self.publish(world_series_record())
        self.assertEqual(self.publish(world_series_record())["status"], "ALREADY_PUBLISHED")
        with self.assertRaises(PhaseChainError) as caught:
            self.publish(world_series_record(849900, "L", "AL Championship Series"))
        self.assertEqual(caught.exception.code, "DB_CHAIN_EXISTING_GAME_CONFLICT")
        self.assertEqual(len(self.db.rows), 1)

    def test_games_already_in_committed_snapshot_are_not_published(self) -> None:
        committed = self.base.lookup_exact(849815)
        runtime = FakeRuntime(FullyVerifiedBase(self.base), [])
        result = publish_runtime_records(
            runtime, [committed.game_pk], selection_receipt_sha256="b" * 64,
            connect=self.db.connect, environ=ENV, config_path=self.config_path)
        self.assertEqual(result["status"], "NOTHING_TO_PUBLISH")

    # Tamper detection -------------------------------------------------------
    def assert_tamper(self, mutate, code: str) -> None:
        self.publish(world_series_record())
        self.publish(world_series_record(849901))
        self.publish(world_series_record(849902))
        mutate(self.db.rows)
        with self.assertRaises(PhaseChainError) as caught:
            self.extend()
        self.assertEqual(caught.exception.code, code)

    def test_edited_label_fails_hash(self) -> None:
        def mutate(rows):
            rows[1]["entry_text"] = rows[1]["entry_text"].replace("World Series", "AL Championship Series")
        self.assert_tamper(mutate, "DB_CHAIN_ENTRY_HASH_MISMATCH")

    def test_rehashed_edit_fails_signature(self) -> None:
        def mutate(rows):
            entry = json.loads(rows[2]["entry_text"])
            entry["record"]["source_round"] = "Forged Series"
            text = chain_module.canonical_json_bytes(entry).decode()
            rows[2].update(entry_text=text, entry_sha256=hashlib.sha256(text.encode()).hexdigest())
        self.assert_tamper(mutate, "DB_CHAIN_SIGNATURE_INVALID")

    def test_deleted_middle_entry_breaks_links(self) -> None:
        self.assert_tamper(lambda rows: rows.pop(1), "DB_CHAIN_LINK_INVALID")

    def test_reordered_entries_break_links(self) -> None:
        def mutate(rows):
            rows[1], rows[2] = rows[2], rows[1]
        self.assert_tamper(mutate, "DB_CHAIN_LINK_INVALID")

    def test_forged_entry_with_other_key_fails(self) -> None:
        other = FakeChainDatabase()
        self.db, original = other, self.db
        self.publish(world_series_record())
        self.db = original
        forged = dict(other.rows[0])
        forged["signature_hmac_sha256"] = "0" * 64
        self.db.rows.append(forged)
        with self.assertRaises(PhaseChainError) as caught:
            self.extend()
        self.assertEqual(caught.exception.code, "DB_CHAIN_SIGNATURE_INVALID")

    def test_signed_entry_conflicting_with_snapshot_fails(self) -> None:
        conflicting = world_series_record(849815)  # committed as ALCS
        with self.assertRaises(PhaseChainError):
            self.publish(conflicting)
        # A writer bug that slipped past the check still fails on read.
        runtime = FakeRuntime(FullyVerifiedBase(self.base), [conflicting])
        runtime.base = FullyVerifiedBase(_AbsentBase(self.base))
        publish_runtime_records(runtime, [849815], selection_receipt_sha256="b" * 64,
                                connect=self.db.connect, environ=ENV, config_path=self.config_path)
        with self.assertRaises(PhaseChainError) as caught:
            self.extend()
        self.assertEqual(caught.exception.code, "DB_CHAIN_BASE_CONFLICT")

    def test_regular_season_record_is_rejected(self) -> None:
        record = replace(world_series_record(), source_game_type="R", season_phase="REGULAR_SEASON",
                         postseason_round=None, season_name="MLB_2026_REGULAR_SEASON")
        with self.assertRaises(PhaseChainError) as caught:
            self.publish(record)
        self.assertEqual(caught.exception.code, "DB_CHAIN_RECORD_CLASSIFICATION_INVALID")

    def test_anchor_outside_active_lineage_fails(self) -> None:
        self.publish(world_series_record())
        config = json.loads(self.config_path.read_text())
        config["anchor_descriptor_sha256"] = "c" * 64
        self.config_path.write_text(json.dumps(config))
        with self.assertRaises(PhaseChainError) as caught:
            self.extend()
        self.assertEqual(caught.exception.code, "DB_CHAIN_ANCHOR_NOT_IN_ACTIVE_LINEAGE")

    # Configuration gaps withhold only ---------------------------------------
    def test_unconfigured_chain_never_touches_database(self) -> None:
        def refuse():
            raise AssertionError("database reached")
        authority, status = self.extend(connect=refuse, config_path=chain_module.CONFIG_PATH)
        self.assertIs(authority, self.base)
        self.assertEqual(status, {"status": "DB_CHAIN_NOT_CONFIGURED"})

    def test_missing_or_wrong_key_and_unreachable_table_withhold_only(self) -> None:
        self.assertEqual(self.extend(environ={})[1]["status"], "DB_CHAIN_KEY_UNAVAILABLE")
        self.assertEqual(self.extend(environ={KEY_ENV: "11" * 32})[1]["status"], "DB_CHAIN_KEY_ID_MISMATCH")

        def unreachable():
            raise ConnectionError("down")
        authority, status = self.extend(connect=unreachable)
        self.assertIs(authority, self.base)
        self.assertEqual(status["status"], "DB_CHAIN_UNAVAILABLE")

    # Writer guards ------------------------------------------------------------
    def test_writer_requires_full_verification_and_restricted_ingest(self) -> None:
        runtime = FakeRuntime(self.base, [world_series_record()])
        with self.assertRaises(PhaseChainError) as caught:
            publish_runtime_records(runtime, [849900], selection_receipt_sha256="b" * 64,
                                    connect=self.db.connect, environ=ENV, config_path=self.config_path)
        self.assertEqual(caught.exception.code, "DB_CHAIN_WRITER_REQUIRES_FULL_VERIFICATION")
        runtime.base = FullyVerifiedBase(self.base)
        # Without the ingest URL the writer refuses; it never falls back to another login.
        with self.assertRaises(PhaseChainError) as caught:
            publish_runtime_records(runtime, [849900], selection_receipt_sha256="b" * 64,
                                    environ=ENV, config_path=self.config_path)
        self.assertEqual(caught.exception.code, "DB_CHAIN_WRITER_REQUIRES_RESTRICTED_INGEST")

    def test_daily_hook_is_opt_in_and_non_fatal(self) -> None:
        from backend.mlb.scripts import run_mlb_public_game_moneyline_daily_v1 as daily

        with unittest.mock.patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop(daily.PHASE_AUTHORITY_CHAIN_PUBLISH_ENV, None)
            self.assertEqual(daily._publish_phase_authority_chain(None, [], None, write_durable=True),
                             {"status": "DISABLED"})
            os.environ[daily.PHASE_AUTHORITY_CHAIN_PUBLISH_ENV] = "1"
            runtime = FakeRuntime(FullyVerifiedBase(self.base), [world_series_record()])
            os.environ.pop(KEY_ENV, None)
            result = daily._publish_phase_authority_chain(
                runtime, [{"game_id": 849900}], {"sha256": "b" * 64}, write_durable=True)
        self.assertEqual(result["status"], "FAILED")

    # Hosted endpoint ----------------------------------------------------------
    def test_endpoint_serves_chain_game_and_fails_closed_on_tamper(self) -> None:
        from fastapi.testclient import TestClient

        from backend.app.api_server import app
        from backend.app.services.mlb import public_game_prediction_service as service
        from backend.mlb.tests.test_public_game_prediction_pythagorean_log5_v1 import _scored_game

        self.publish(world_series_record())
        row = {**_scored_game(849900, "2026-10-24T00:08:00Z"), "game_date": "2026-10-23"}
        extend = functools.partial(extend_public_authority, connect=self.db.connect, environ=ENV,
                                   config_path=self.config_path)
        with unittest.mock.patch.object(service, "extend_public_authority", extend), \
                unittest.mock.patch.object(service, "fetch_prediction_rows", lambda _date: [row]), \
                unittest.mock.patch.dict("os.environ", {"MLB_PUBLIC_GAME_PREDICTIONS_ENABLED": "1"}):
            client = TestClient(app)
            payload = client.get("/api/mlb/game-predictions", params={"game_date": "2026-10-23"}).json()
            self.assertEqual(payload["count"], 1)
            self.assertEqual(payload["rows"][0]["postseason_round"], "WORLD_SERIES")
            self.assertEqual(payload["phase_authority_verification"], "COMMITTED_SNAPSHOT_AND_SIGNED_DB_CHAIN")
            self.assertEqual(payload["phase_authority_extension"]["status"], "DB_CHAIN_VERIFIED")
            self.db.rows[0]["signature_hmac_sha256"] = "0" * 64
            payload = client.get("/api/mlb/game-predictions", params={"game_date": "2026-10-23"}).json()
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["unavailable_status"], "PHASE_AUTHORITY_EVIDENCE_UNAVAILABLE")
        self.assertEqual(payload["unavailable_code"], "DB_CHAIN_SIGNATURE_INVALID")


class _AbsentBase:
    """Base that hides one committed game, to simulate a buggy writer."""

    def __init__(self, base):
        self._base = base

    def __getattr__(self, name):
        return getattr(self._base, name)

    def lookup_exact(self, pk):
        if int(pk) == 849815:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=849815)
        return self._base.lookup_exact(pk)


class MigrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sql = MIGRATION.read_text()
        self.code = "\n".join(line for line in self.sql.splitlines() if not line.lstrip().startswith("--"))

    def test_writer_reaches_table_only_through_definer_function(self) -> None:
        self.assertIn("SECURITY DEFINER\nSET search_path = pg_catalog", self.code)
        grants = re.findall(r"GRANT\s+(\w+)\s+ON\s+(?:TABLE|FUNCTION)\s+([^\s(]+)", self.code)
        self.assertEqual(sorted(grants), [
            ("EXECUTE", "mlb.append_phase_authority_chain_entry"),
            ("SELECT", "mlb.phase_authority_chain_entries"),
        ])
        self.assertIn("TO proppadia_api, proppadia_mlb_ingest;", self.code)
        self.assertNotRegex(self.code, r"GRANT\s+(INSERT|UPDATE|DELETE|TRUNCATE|ALL)")
        self.assertIn("REVOKE ALL ON TABLE mlb.phase_authority_chain_entries FROM PUBLIC", self.code)

    def test_table_is_append_only_and_hash_checked(self) -> None:
        self.assertIn("BEFORE UPDATE OR DELETE ON mlb.phase_authority_chain_entries", self.code)
        self.assertIn("BEFORE TRUNCATE ON mlb.phase_authority_chain_entries", self.code)
        self.assertIn("ENABLE ROW LEVEL SECURITY", self.code)
        self.assertIn("pg_catalog.sha256(pg_catalog.convert_to(entry_text, 'UTF8'))", self.code)
        self.assertIn("p_prev_entry_sha256 IS DISTINCT FROM v_head.entry_sha256", self.code)

    def test_editor_verification_and_safe_rollback(self) -> None:
        files = sorted(MIGRATION.with_name(MIGRATION.stem + "_verify").glob("*.sql"))
        self.assertEqual(len(files), 6)
        for path in files:
            text = path.read_text()
            code = "\n".join(line for line in text.splitlines() if not line.startswith("--"))
            self.assertIn("-- Expect:", text)
            self.assertEqual(code.count(";"), 1, path.name)  # one query per file
            self.assertRegex(code.lstrip(), r"^SELECT\b")
        self.assertIn("has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT')",
                      files[0].read_text())
        self.assertIn("has_table_privilege", ROLLBACK.with_name(ROLLBACK.stem + "_verify.sql").read_text())
        self.assertNotIn("SET ROLE", self.code)
        rollback = ROLLBACK.read_text()
        self.assertIn("kept without grants", rollback)
        self.assertIn("IF EXISTS (SELECT 1 FROM mlb.phase_authority_chain_entries)", rollback)


if __name__ == "__main__":
    unittest.main()
