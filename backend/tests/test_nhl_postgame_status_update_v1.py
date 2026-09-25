from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from backend.nhl.official_request_journal import canonical_game_set_hash
from backend.nhl.postgame_reconcile.status_update import (
    FROZEN_GAME_IDS,
    FROZEN_GAME_SET_HASH,
    SLATE_DATE,
    update_frozen_statuses,
    validate_retained_bundle,
)


def schedule_payload(*, ids=None, state="FINAL", home_override=None,
                     start_override=None):
    ids = list(FROZEN_GAME_IDS) if ids is None else ids
    games = []
    for game_id in ids:
        i = int(game_id) - FROZEN_GAME_IDS[0]
        games.append({
            "id": int(game_id), "gameType": 1,
            "startTimeUTC": start_override or "2026-09-25T02:00:00Z",
            "gameState": state,
            "homeTeam": {"abbrev": (home_override if i == 0 and home_override else f"H{i:02d}")},
            "awayTeam": {"abbrev": f"A{i:02d}"},
        })
    return {"gameWeek": [{"date": SLATE_DATE, "games": games}]}


def write_bundle(root: Path, payload=None) -> tuple[Path, str]:
    bundle = root / f"fixture_bundle_20260925_040000Z_{len(list(root.iterdir())):02d}"
    bundle.mkdir()
    raw = json.dumps(payload or schedule_payload(), sort_keys=True,
                     separators=(",", ":")).encode()
    (bundle / "official_schedule.json").write_bytes(raw)
    manifest = {
        "contract_version": "NHL_POSTGAME_STATUS_BUNDLE_V1",
        "bundle_id": bundle.name,
        "slate_date": SLATE_DATE,
        "created_at_utc": "2026-09-25T04:01:00Z",
        "game_ids": list(FROZEN_GAME_IDS),
        "game_set_hash": FROZEN_GAME_SET_HASH,
        "sources": [{
            "role": "OFFICIAL_POSTGAME_SCHEDULE", "provider": "NHL",
            "url": "https://api-web.nhle.com/v1/schedule/2026-09-24",
            "http_status": 200, "redirect_count": 0,
            "fetched_at_utc": "2026-09-25T04:00:00Z",
            "path": "official_schedule.json",
            "sha256": hashlib.sha256(raw).hexdigest(),
        }],
    }
    manifest_bytes = json.dumps(manifest, sort_keys=True,
                                separators=(",", ":")).encode()
    (bundle / "bundle_manifest.json").write_bytes(manifest_bytes)
    return bundle, hashlib.sha256(manifest_bytes).hexdigest()


def canonical_rows():
    rows = []
    for index, game_id in enumerate(FROZEN_GAME_IDS):
        rows.append((game_id, SLATE_DATE, "2026-09-25T02:00:00Z", 2026,
                     1, f"H{index:02d}", f"A{index:02d}", "scheduled"))
    return rows


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1
        self.last_sql = ""

    def execute(self, sql, params=None):
        compact = " ".join(sql.split())
        self.connection.statements.append((compact, params))
        self.last_sql = compact
        if compact.startswith("SELECT"):
            selected = set(params[0])
            self.selected = [row for row in self.connection.rows if row[0] in selected]
            self.rowcount = len(self.selected)
            return
        if compact.startswith("UPDATE nhl.games SET status = %s WHERE game_id = %s"):
            self.connection.update_attempts += 1
            if self.connection.fail_on_update == self.connection.update_attempts:
                self.rowcount = 0
                return
            status, game_id = params
            for index, row in enumerate(self.connection.rows):
                if row[0] == game_id:
                    self.connection.rows[index] = (*row[:7], status)
                    self.rowcount = 1
                    return
            self.rowcount = 0
            return
        raise AssertionError(f"UNEXPECTED_SQL:{compact}")

    def fetchall(self):
        return list(getattr(self, "selected", self.connection.rows))

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows, fail_on_update=None):
        self.rows = list(rows)
        self.before = list(rows)
        self.fail_on_update = fail_on_update
        self.update_attempts = 0
        self.statements = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True
        self.rows = list(self.before)

    def close(self):
        self.closed = True


class NHLPostgameStatusUpdateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nhl_status_update_")
        self.root = Path(self.temp.name)
        self.bundle, self.manifest_hash = write_bundle(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def mutate_bundle(self, payload):
        self.bundle, self.manifest_hash = write_bundle(self.root, payload)

    def invoke(self, rows=None, fail_on_update=None):
        connection = FakeConnection(rows or canonical_rows(), fail_on_update)
        result = update_frozen_statuses(
            bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash,
            dsn="fixture-only", connect=lambda unused_dsn: connection,
        )
        return result, connection

    def test_exact_frozen_set_and_hash_validate_without_database(self):
        result = validate_retained_bundle(
            bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)
        self.assertEqual(result["game_ids"], list(FROZEN_GAME_IDS))
        self.assertEqual(result["game_set_hash"], FROZEN_GAME_SET_HASH)
        self.assertEqual(canonical_game_set_hash(result["game_ids"]), FROZEN_GAME_SET_HASH)
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(result["database_writes"], 0)

    def test_manifest_hash_and_source_hash_are_enforced(self):
        with self.assertRaisesRegex(ValueError, "BUNDLE_MANIFEST_HASH_MISMATCH"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256="0" * 64)
        (self.bundle / "official_schedule.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "OFFICIAL_SCHEDULE_HASH_MISMATCH"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

    def test_missing_game_fails_exact_set_validation(self):
        self.mutate_bundle(schedule_payload(ids=list(FROZEN_GAME_IDS[:-1])))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_MISSING_GAME_IDS"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

    def test_duplicate_missing_and_extra_game_ids_fail_closed(self):
        duplicated = list(FROZEN_GAME_IDS) + [FROZEN_GAME_IDS[0]]
        self.mutate_bundle(schedule_payload(ids=duplicated))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_DUPLICATE_GAME_ID"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

        self.mutate_bundle(schedule_payload(ids=list(FROZEN_GAME_IDS[:-1])))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_MISSING_GAME_IDS"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

        extra = list(FROZEN_GAME_IDS) + [2026010099]
        self.mutate_bundle(schedule_payload(ids=extra))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_EXTRA_GAME_ID"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

    def test_nonfinal_and_official_identity_mismatches_fail_closed(self):
        self.mutate_bundle(schedule_payload(state="FUT"))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_GAME_NOT_FINAL"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

        self.mutate_bundle(schedule_payload(home_override="BAD"))
        with self.assertRaisesRegex(ValueError, "CANONICAL_TEAM_MISMATCH"):
            self.invoke()

        self.mutate_bundle(schedule_payload(start_override="2026-09-26T02:00:00Z"))
        with self.assertRaisesRegex(ValueError, "OFFICIAL_GAME_DATE_MISMATCH"):
            validate_retained_bundle(bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash)

    def test_canonical_date_or_schedule_identity_mismatch_rolls_back(self):
        rows = canonical_rows()
        rows[0] = (rows[0][0], "2026-09-23", *rows[0][2:])
        connection = FakeConnection(rows)
        with self.assertRaisesRegex(ValueError, "CANONICAL_GAME_DATE_MISMATCH"):
            update_frozen_statuses(
                bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash,
                dsn="fixture-only", connect=lambda unused: connection)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)
        self.assertEqual(connection.rows, connection.before)
        self.assertEqual(connection.update_attempts, 0)

    def test_status_only_writes_and_preserves_unrelated_game(self):
        unrelated = (2026010999, "2026-09-24", "2026-09-25T03:00:00Z", 2026,
                     1, "XYZ", "ABC", "scheduled")
        connection = FakeConnection(canonical_rows() + [unrelated])
        result = update_frozen_statuses(
            bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash,
            dsn="fixture-only", connect=lambda unused: connection)
        self.assertEqual(result["updated_rows"], 11)
        self.assertTrue(connection.committed)
        updates = [(sql, params) for sql, params in connection.statements if sql.startswith("UPDATE")]
        self.assertEqual(len(updates), 11)
        self.assertTrue(all(sql == "UPDATE nhl.games SET status = %s WHERE game_id = %s"
                            for sql, unused in updates))
        self.assertEqual({params[1] for unused, params in updates}, set(FROZEN_GAME_IDS))
        self.assertEqual(connection.rows[-1], unrelated)
        self.assertTrue(all(row[7] == "final" for row in connection.rows[:11]))

    def test_atomic_rollback_on_partial_update_failure(self):
        connection = FakeConnection(canonical_rows(), fail_on_update=6)
        with self.assertRaisesRegex(RuntimeError, "STATUS_UPDATE_ROWCOUNT_MISMATCH"):
            update_frozen_statuses(
                bundle_dir=self.bundle, expected_manifest_sha256=self.manifest_hash,
                dsn="fixture-only", connect=lambda unused: connection)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)
        self.assertEqual(connection.rows, connection.before)

    def test_already_final_is_idempotent(self):
        rows = [(row[0], *row[1:7], "final") for row in canonical_rows()]
        result, connection = self.invoke(rows=rows)
        self.assertEqual(result["updated_rows"], 0)
        self.assertEqual(result["already_final_rows"], 11)
        self.assertEqual(connection.update_attempts, 0)
        self.assertTrue(connection.committed)


if __name__ == "__main__":
    unittest.main()
