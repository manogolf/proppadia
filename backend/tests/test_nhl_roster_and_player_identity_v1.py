from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# These legacy collector modules validate configuration at import time.  The
# tests never connect; sentinel values permit importing their pure helpers.
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://offline.invalid/test")
os.environ.setdefault("SLATE_DATE", "2026-09-20")

from backend.nhl.official_request_journal import (
    ENV_CACHE,
    ENV_GAME_HASH,
    ENV_GAME_IDS,
    ENV_JOURNAL,
    ENV_REQUIRED,
    ENV_RESPONSE_SOURCE_LEDGER,
    ENV_RUN_ID,
    ENV_SLATE,
    RequestContext,
    build_typed_response_source_ledger,
    canonical_game_set_hash,
    official_get,
    read_journal,
    verify_payload_identity,
    verify_preserved_response_run,
    verify_roster_response_run,
)
from backend.nhl.player_external_identity import (
    PlayerIdentityConflict,
    localized_text,
    resolve_player_external_identity,
)
from backend.nhl.scripts.refresh_players_and_roster_today import (
    _append_from_section,
    _validate_roster_names,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    REQUEST_RUN_RECEIPTS,
    local_conditional_lookup_inventory,
)
from backend.nhl.scripts.seed_skater_logs_for_date import resolve_skater_player_id


ROOT = Path(__file__).resolve().parents[2]
RUNS = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/"
        "request_runs/2026-09-20")
ORIGINAL = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
THIRD = "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9"
GAMES = list(range(2026010008, 2026010015))


class _Transaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.connection.lock.acquire()
        self.snapshot = list(self.connection.rows)
        return self

    def __exit__(self, kind, value, traceback):
        if kind is not None:
            self.connection.rows[:] = self.snapshot
        self.connection.lock.release()
        return False


class _Cursor:
    description = [SimpleNamespace(name="player_id"), SimpleNamespace(name="provider"),
                   SimpleNamespace(name="provider_player_id")]

    def __init__(self, connection):
        self.connection = connection
        self.selected = []

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, sql, values):
        if "INSERT INTO nhl.player_external_ids" in sql:
            if self.connection.fail_next_insert:
                self.connection.fail_next_insert = False
                raise RuntimeError("UNEXPECTED_SQL_FAILURE")
            player_id, provider, external = int(values[0]), str(values[1]), str(values[2])
            conflict = any(
                (row[0] == player_id and row[1] == provider)
                or (row[1] == provider and row[2] == external)
                for row in self.connection.rows
            )
            if not conflict:
                self.connection.rows.append((player_id, provider, external))
        elif "SELECT player_id, provider, provider_player_id" in sql:
            provider, player_id, external = str(values[0]), int(values[1]), str(values[2])
            self.selected = [row for row in self.connection.rows
                             if row[1] == provider and (row[0] == player_id or row[2] == external)]
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return list(self.selected)


class _Connection:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.lock = threading.RLock()
        self.fail_next_insert = False

    def transaction(self):
        return _Transaction(self)

    def cursor(self):
        return _Cursor(self)


def _bindings():
    authority_receipt = REQUEST_RUN_RECEIPTS[ORIGINAL]
    roster_receipt = REQUEST_RUN_RECEIPTS[THIRD]
    authority = verify_preserved_response_run(
        RUNS / ORIGINAL, expected_run_id=ORIGINAL, slate_date="2026-09-20",
        game_ids=GAMES, repository_root=ROOT,
        expected_journal_sha256=authority_receipt["journal_sha256"],
        expected_tree_fingerprint=authority_receipt["tree_fingerprint"])
    roster = verify_roster_response_run(
        RUNS / THIRD, expected_run_id=THIRD, slate_date="2026-09-20",
        game_ids=GAMES, expected_teams=roster_receipt["teams"], repository_root=ROOT,
        expected_journal_sha256=roster_receipt["journal_sha256"],
        expected_tree_fingerprint=roster_receipt["tree_fingerprint"],
        expected_response_set_sha256=roster_receipt["response_set_sha256"])
    return authority, roster


class RosterAndPlayerIdentityTest(unittest.TestCase):
    def test_plain_and_localized_names(self):
        self.assertEqual(localized_text("  Jack Hughes  "), "Jack Hughes")
        self.assertEqual(localized_text({"default": "  Jack Hughes  ", "fr": "Jack Hughes"}),
                         "Jack Hughes")
        self.assertIsNone(localized_text({"fr": "Nom"}))

    def test_player_landing_requires_exact_returned_identity(self):
        identity = {"slate_date": "2026-09-20", "player_id": 8484537}
        body = json.dumps({
            "playerId": 8484537,
            "firstName": {"default": "Ozzy"},
            "lastName": {"default": "Wiesblatt"},
        }).encode()
        verify_payload_identity("PLAYER_LANDING", identity, body)
        self.assertEqual(localized_text(json.loads(body)["firstName"]), "Ozzy")
        with self.assertRaisesRegex(RuntimeError, "PLAYER_ID_MISMATCH"):
            verify_payload_identity(
                "PLAYER_LANDING", identity,
                json.dumps({"playerId": 8482103}).encode())

    def test_all_548_roster_names_are_retained_without_landing_backfill(self):
        unused, roster = _bindings()
        players = []
        for claim in roster["responses"]:
            body = (Path(roster["source_cache"]) / "objects" /
                    f"{claim['object_sha256']}.json").read_bytes()
            payload = json.loads(body)
            _append_from_section(players, payload.get("forwards"), "F")
            _append_from_section(players, payload.get("defensemen") or payload.get("defense"), "D")
            _append_from_section(players, payload.get("goalies"), "G")
        self.assertEqual(len(players), 548)
        self.assertEqual(len({row["person"]["id"] for row in players}), 548)
        staged = [{"player_id": row["person"]["id"],
                   "first_name": row["firstName"], "last_name": row["lastName"]}
                  for row in players]
        with patch("backend.nhl.scripts.refresh_players_and_roster_today.fetch_player_name_strict",
                   side_effect=AssertionError("ARTIFICIAL_BACKFILL_FORBIDDEN")) as landing:
            _validate_roster_names(staged)
        landing.assert_not_called()

    def test_exact_identity_is_idempotent_and_both_conflicts_fail_closed(self):
        connection = _Connection([(8482103, "nhl", "8482103")])
        result = resolve_player_external_identity(
            connection, player_id=8482103, provider="nhl", provider_player_id="8482103")
        self.assertEqual(result.disposition, "EXACT_IDEMPOTENT_MAPPING")
        with self.assertRaisesRegex(PlayerIdentityConflict, "INTERNAL_PLAYER"):
            resolve_player_external_identity(
                connection, player_id=8482103, provider="nhl", provider_player_id="8484537")
        with self.assertRaisesRegex(PlayerIdentityConflict, "EXTERNAL_ID_PLAYER"):
            resolve_player_external_identity(
                connection, player_id=8484537, provider="nhl", provider_player_id="8482103")
        self.assertEqual(connection.rows, [(8482103, "nhl", "8482103")])
        # The savepoint rollback leaves the connection usable.
        resolve_player_external_identity(
            connection, player_id=8482103, provider="nhl", provider_player_id="8482103")

    def test_unexpected_sql_error_rolls_back_and_connection_remains_usable(self):
        connection = _Connection()
        connection.fail_next_insert = True
        with self.assertRaisesRegex(RuntimeError, "UNEXPECTED_SQL_FAILURE"):
            resolve_player_external_identity(
                connection, player_id=1, provider="nhl", provider_player_id="1")
        self.assertEqual(connection.rows, [])
        resolve_player_external_identity(
            connection, player_id=1, provider="nhl", provider_player_id="1")
        self.assertEqual(connection.rows, [(1, "nhl", "1")])

    def test_concurrent_identical_and_conflicting_inserts(self):
        connection = _Connection()
        errors = []

        def resolve(player_id, external):
            try:
                resolve_player_external_identity(
                    connection, player_id=player_id, provider="nhl",
                    provider_player_id=external)
            except Exception as error:
                errors.append(error)

        identical = [threading.Thread(target=resolve, args=(7, "7")) for _ in range(8)]
        for thread in identical:
            thread.start()
        for thread in identical:
            thread.join()
        self.assertEqual(connection.rows, [(7, "nhl", "7")])
        self.assertEqual(errors, [])

        conflict = threading.Thread(target=resolve, args=(8, "7"))
        conflict.start(); conflict.join()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], PlayerIdentityConflict)
        self.assertEqual(connection.rows, [(7, "nhl", "7")])

    def test_abbreviated_name_never_merges_distinct_provider_ids(self):
        pid, learned = resolve_skater_player_id(
            nhl_id=8484537, normalized_name="a. smith", external_ids={},
            roster_names={"alex smith": (8482103, 1)})
        self.assertIsNone(pid)
        self.assertFalse(learned)
        pid, learned = resolve_skater_player_id(
            nhl_id=8484537, normalized_name="a. smith", external_ids={},
            roster_names={"alex smith": (8482103, 1), "adam smith": (8484537, 1)})
        self.assertIsNone(pid)
        self.assertFalse(learned)

    def test_typed_sources_resolve_22_objects_and_only_one_conditional_lookup(self):
        authority, roster = _bindings()
        ledger = build_typed_response_source_ledger([authority, roster])
        inventory = local_conditional_lookup_inventory(authority, roster)
        self.assertEqual(ledger["declared_response_identities"], 22)
        self.assertEqual(inventory["localized_roster_names_retained"], 548)
        self.assertEqual(inventory["player_ids"], [8484537])
        self.assertEqual(roster["unpreserved_player_landing_responses"], 250)

        with tempfile.TemporaryDirectory(prefix="nhl_typed_sources_") as temp:
            temp = Path(temp)
            env = {
                ENV_REQUIRED: "1", ENV_RUN_ID: "typed_target",
                ENV_JOURNAL: str(temp / "journal.jsonl"), ENV_CACHE: str(temp / "cache"),
                ENV_SLATE: "2026-09-20", ENV_GAME_HASH: canonical_game_set_hash(GAMES),
                ENV_GAME_IDS: ",".join(map(str, GAMES)),
                ENV_RESPONSE_SOURCE_LEDGER: json.dumps(ledger),
            }
            with patch.dict(os.environ, env, clear=True), \
                 patch("backend.nhl.official_request_journal.requests.Session") as network:
                for source in (authority, roster):
                    for claim in source["responses"]:
                        official_get(
                            "unused", timeout=1, stage="FIXTURE",
                            endpoint_family=claim["endpoint_family"],
                            identity=claim["resource_identity"], reuse_preserved=True)
                with self.assertRaisesRegex(RuntimeError, "DECLARED_RESPONSE_REUSE_REQUIRED"):
                    official_get(
                        "https://api-web.nhle.com/v1/schedule/2026-09-20", timeout=1,
                        stage="FIXTURE", endpoint_family="SCHEDULE",
                        identity={"slate_date": "2026-09-20"})
                with self.assertRaisesRegex(RuntimeError, "NOT_DECLARED"):
                    official_get(
                        "unused", timeout=1, stage="FIXTURE",
                        endpoint_family="PLAYER_LANDING",
                        identity={"slate_date": "2026-09-20", "player_id": 8484537},
                        reuse_preserved=True)
                network.assert_not_called()
            rows = read_journal(Path(env[ENV_JOURNAL]))
            self.assertEqual(len(rows), 24)
            self.assertEqual(sum(row["event_kind"] == "PRESERVED_RESPONSE_REUSE"
                                 for row in rows), 22)
            self.assertEqual(rows[-1]["event_kind"], "REQUEST_REJECTED")

    def test_typed_sources_reject_overlap_wrong_family_and_tampered_claim(self):
        authority, roster = _bindings()
        overlap = copy.deepcopy(authority)
        overlap["responses"].append(copy.deepcopy(overlap["responses"][0]))
        with self.assertRaisesRegex(RuntimeError, "OVERLAP"):
            build_typed_response_source_ledger([overlap, roster])

        wrong = copy.deepcopy(authority)
        wrong["responses"][0]["endpoint_family"] = "ROSTER"
        with self.assertRaisesRegex(RuntimeError, "WRONG_FAMILY"):
            build_typed_response_source_ledger([wrong, roster])

        ledger = build_typed_response_source_ledger([authority, roster])
        ledger["sources"][0]["responses"][0]["object_sha256"] = "0" * 64
        with tempfile.TemporaryDirectory(prefix="nhl_tampered_claim_") as temp:
            temp = Path(temp)
            env = {
                ENV_REQUIRED: "1", ENV_RUN_ID: "tampered_target",
                ENV_JOURNAL: str(temp / "journal.jsonl"), ENV_CACHE: str(temp / "cache"),
                ENV_SLATE: "2026-09-20", ENV_GAME_HASH: canonical_game_set_hash(GAMES),
                ENV_GAME_IDS: ",".join(map(str, GAMES)),
                ENV_RESPONSE_SOURCE_LEDGER: json.dumps(ledger),
            }
            with patch.dict(os.environ, env, clear=True), \
                 patch("backend.nhl.official_request_journal.requests.Session") as network:
                RequestContext.from_env(required=True)
                with self.assertRaisesRegex(RuntimeError, "LEDGER_BINDING_MISMATCH"):
                    official_get(
                        "unused", timeout=1, stage="FIXTURE", endpoint_family="SCHEDULE",
                        identity={"slate_date": "2026-09-20"}, reuse_preserved=True)
                network.assert_not_called()


if __name__ == "__main__":
    unittest.main()
