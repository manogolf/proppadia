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
    verify_player_identity_response_run,
    verify_preserved_response_run,
    verify_roster_response_run,
)
from backend.nhl.player_external_identity import (
    PlayerIdentityConflict,
    is_abbreviated_player_name,
    localized_text,
    resolve_player_external_identity,
)
from backend.nhl.scripts.refresh_players_and_roster_today import (
    _append_from_section,
    _validate_roster_names,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    REQUEST_RUN_RECEIPTS,
    classify_database_identity_rows,
    local_conditional_lookup_inventory,
)
from backend.nhl.scripts.seed_goalie_logs_for_date import resolve_goalie_player_id
from backend.nhl.scripts.seed_skater_logs_for_date import resolve_skater_player_id


ROOT = Path(__file__).resolve().parents[2]
RUNS = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/"
        "request_runs/2026-09-20")
ORIGINAL = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
THIRD = "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9"
FOURTH = "nhlpostgame_20260920_20260922T181726727181Z_70f0280d"
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


def _player_binding():
    receipt = REQUEST_RUN_RECEIPTS[FOURTH]
    return verify_player_identity_response_run(
        RUNS / FOURTH, expected_run_id=FOURTH, slate_date="2026-09-20",
        game_ids=GAMES, expected_player_id=receipt["player_id"],
        repository_root=ROOT, expected_journal_sha256=receipt["journal_sha256"],
        expected_tree_fingerprint=receipt["tree_fingerprint"],
        expected_object_sha256=receipt["object_sha256"],
        expected_index_sha256=receipt["index_sha256"])


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

    def test_fourth_run_is_reusable_only_for_ethan_czata_identity(self):
        binding = _player_binding()
        self.assertEqual(binding["role"], "PLAYER_IDENTITY_RESPONSE_SOURCE")
        self.assertEqual(len(binding["responses"]), 1)
        claim = binding["responses"][0]
        self.assertEqual(claim["resource_identity"], {
            "slate_date": "2026-09-20", "player_id": 8485386})
        self.assertEqual(claim["object_sha256"],
                         "69733de66231bda93e5deb6c5d013aa3ce6a146835c1ae301ffdc2be477fb7b4")
        self.assertEqual(claim["index_sha256"],
                         "797f6100afd0c418f366c914e464ecb311768923f3b80fab48f18d55d33aa99f")

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

    def test_typed_sources_resolve_23_objects_and_local_coverage_is_unverified(self):
        authority, roster = _bindings()
        player = _player_binding()
        ledger = build_typed_response_source_ledger([authority, roster, player])
        inventory = local_conditional_lookup_inventory(authority, roster)
        self.assertEqual(ledger["declared_response_identities"], 23)
        self.assertEqual(inventory["localized_roster_names_retained"], 548)
        self.assertEqual(inventory["participating_nhl_ids"], 280)
        self.assertEqual(inventory["database_mapping_coverage"], "UNVERIFIED")
        self.assertNotIn("player_ids", inventory)
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
                for source in (authority, roster, player):
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
            self.assertEqual(len(rows), 25)
            self.assertEqual(sum(row["event_kind"] == "PRESERVED_RESPONSE_REUSE"
                                 for row in rows), 23)
            self.assertEqual(rows[-1]["event_kind"], "REQUEST_REJECTED")

    def test_audited_unicode_abbreviation_rule(self):
        authority, unused = _bindings()
        names = []
        for claim in authority["responses"]:
            if claim["endpoint_family"] != "BOXSCORE":
                continue
            payload = json.loads((Path(authority["source_cache"]) / "objects" /
                                  f"{claim['object_sha256']}.json").read_bytes())
            for side in ("homeTeam", "awayTeam"):
                team = (payload.get("playerByGameStats") or {}).get(side) or {}
                for section in ("forwards", "defense", "goalies"):
                    for row in team.get(section) or []:
                        names.append(row["name"]["default"])
        self.assertEqual(len(names), 280)
        self.assertTrue(all(is_abbreviated_player_name(name) for name in names))
        for name in ("E. Czata", "O. Wiesblatt", "J. O'Brien",
                     "J. Smith-Jones", "M. St. Louis"):
            self.assertTrue(is_abbreviated_player_name(name), name)
        for name in ("Ethan Czata", "J Czata"):
            self.assertFalse(is_abbreviated_player_name(name), name)

    def test_database_partition_fixture_and_lookup_authorization(self):
        authority, roster = _bindings()
        inventory = local_conditional_lookup_inventory(authority, roster)
        absent = set(inventory["roster_absent_ids"])
        preserved = {8485386}
        lookups = {8484537, 8485525, 8486221}
        numeric_proven = {
            8481681, 8481827, 8482167, 8483436, 8483442, 8483494,
            8483652, 8483684, 8484217, 8484219, 8484222, 8484433,
            8484760, 8485007, 8485010, 8485080, 8485375, 8485387,
            8485412, 8485563, 8485594, 8486044, 8486229,
        }
        goalie_binds = {8482472, 8482867, 8482949, 8484181,
                        8484391, 8484900, 8484996}
        absent_bind_candidates = sorted(
            absent - preserved - lookups - numeric_proven - set(inventory["goalie_ids"]))[:5]
        roster_present_bind_candidates = sorted(
            set(inventory["participant_ids"]) - absent - set(inventory["goalie_ids"]))[:8]
        deterministic = (set(absent_bind_candidates) | goalie_binds
                         | set(roster_present_bind_candidates))
        exact = (set(inventory["participant_ids"]) - deterministic - numeric_proven
                 - preserved - lookups)
        player_rows = [
            {"player_id": player_id, "full_name": f"Player {player_id}",
             "first_name": "Full", "last_name": f"Name-{player_id}",
             "team_id": None, "position": "G" if player_id in inventory["goalie_ids"] else "F"}
            for player_id in sorted(exact | deterministic)
        ] + [
            {"player_id": player_id, "full_name": "A. Player",
             "first_name": None, "last_name": None, "team_id": None, "position": "F"}
            for player_id in sorted(numeric_proven)
        ]
        external_rows = [
            {"player_id": player_id, "provider": "nhl",
             "provider_player_id": str(player_id)}
            for player_id in sorted(exact)
        ]
        result = classify_database_identity_rows(
            inventory=inventory, player_rows=player_rows, external_rows=external_rows,
            preserved_player_ids=preserved, authorized_lookup_ids=lookups,
            slate_date="2026-09-20")
        self.assertEqual(result["roster_absent_classification_counts"], {
            "exact_mapping": 22, "deterministic_same_number_bind": 5,
            "numeric_identity_proven_bind": 23,
            "preserved_response_resolution": 1, "new_official_lookup": 3,
            "conflict": 0,
        })
        self.assertEqual(result["classification"]["new_official_lookup"],
                         [8484537, 8485525, 8486221])
        self.assertEqual(result["classification"]["preserved_response_resolution"], [8485386])
        self.assertEqual(len(result["classification"]["exact_mapping"]), 233)
        self.assertEqual(len(result["classification"]["deterministic_same_number_bind"]), 20)
        self.assertEqual(result["classification"]["numeric_identity_proven_bind"],
                         sorted(numeric_proven))
        self.assertEqual(len(result["numeric_identity_provenance"]), 23)
        self.assertTrue(all(row["mapping"] == {
            "internal_player_id": row["player_id"], "provider": "nhl",
            "provider_external_id": str(row["player_id"]),
        } for row in result["numeric_identity_provenance"]))
        self.assertEqual(result["status"], "DATABASE_IDENTITY_PREFLIGHT_VALID")
        self.assertTrue(result["partition_complete_and_mutually_exclusive"])
        self.assertTrue(result["roster_absent_partition_complete"])
        self.assertEqual(sum(result["roster_absent_classification_counts"].values()), 54)
        self.assertEqual(len({player_id for values in result["roster_absent_partition"].values()
                              for player_id in values}), 54)
        deterministic_record = next(
            row for row in result["classification_records"]
            if row["nhl_id"] in deterministic)
        self.assertEqual(deterministic_record["authoritative_name_source"],
                         "FIRST_LAST_COLUMNS")
        connection = _Connection()
        for player_id in (result["classification"]["deterministic_same_number_bind"]
                          + result["classification"]["numeric_identity_proven_bind"]):
            resolve_player_external_identity(
                connection, player_id=player_id, provider="nhl",
                provider_player_id=player_id)
        self.assertEqual(len(connection.rows), 43)
        before = list(connection.rows)
        with self.assertRaisesRegex(PlayerIdentityConflict, "EXTERNAL_ID_PLAYER_CONFLICT"):
            resolve_player_external_identity(
                connection, player_id=9999999, provider="nhl",
                provider_player_id=result["classification"]
                ["deterministic_same_number_bind"][0])
        self.assertEqual(connection.rows, before)
        mismatch = classify_database_identity_rows(
            inventory=inventory, player_rows=player_rows,
            external_rows=external_rows, preserved_player_ids=preserved,
            authorized_lookup_ids={8484537}, slate_date="2026-09-20")
        self.assertEqual(mismatch["status"], "FAILED_CLOSED_DATABASE_PREFLIGHT")
        self.assertIn("AUTHORIZED_PLAYER_LOOKUP_SET_MISMATCH", mismatch["failure"])
        self.assertEqual(len(mismatch["classification_records"]), 280)
        self.assertEqual(sum(mismatch["roster_absent_classification_counts"].values()), 54)

        # Replaying zero, some, or all deterministic bindings changes only the
        # exact/bind counts.  It must never expand the authorized network set.
        bind_candidates = sorted(deterministic | numeric_proven)
        for completed in (0, len(bind_candidates) // 2, len(bind_candidates)):
            progressed_external = external_rows + [
                {"player_id": player_id, "provider": "nhl",
                 "provider_player_id": str(player_id)}
                for player_id in bind_candidates[:completed]
            ]
            progressed = classify_database_identity_rows(
                inventory=inventory, player_rows=player_rows,
                external_rows=progressed_external, preserved_player_ids=preserved,
                authorized_lookup_ids=lookups, slate_date="2026-09-20")
            self.assertEqual(progressed["status"], "DATABASE_IDENTITY_PREFLIGHT_VALID")
            self.assertEqual(progressed["classification"]["new_official_lookup"],
                             [8484537, 8485525, 8486221])
            self.assertEqual(progressed["classification"]["preserved_response_resolution"],
                             [8485386])
            self.assertEqual(len(progressed["classification"]["exact_mapping"]),
                             len(exact) + completed)
            self.assertEqual(
                len(progressed["classification"]["deterministic_same_number_bind"])
                + len(progressed["classification"]["numeric_identity_proven_bind"]),
                len(bind_candidates) - completed)
            self.assertEqual(progressed["goalie_partition_coverage"], 28)

        conflicted_external = external_rows + [{
            "player_id": 9999999, "provider": "nhl",
            "provider_player_id": str(bind_candidates[0]),
        }]
        conflict = classify_database_identity_rows(
            inventory=inventory, player_rows=player_rows,
            external_rows=conflicted_external, preserved_player_ids=preserved,
            authorized_lookup_ids=lookups, slate_date="2026-09-20")
        self.assertEqual(conflict["status"], "FAILED_CLOSED_DATABASE_PREFLIGHT")
        self.assertIn(bind_candidates[0], conflict["classification"]["conflict"])
        self.assertIn("DATABASE_IDENTITY_CONFLICT", conflict["failure"])

        internal_conflict_id = min(numeric_proven)
        internal_conflict = classify_database_identity_rows(
            inventory=inventory, player_rows=player_rows,
            external_rows=external_rows + [{
                "player_id": internal_conflict_id, "provider": "nhl",
                "provider_player_id": "9999999",
            }], preserved_player_ids=preserved,
            authorized_lookup_ids=lookups, slate_date="2026-09-20")
        self.assertIn(internal_conflict_id,
                      internal_conflict["classification"]["conflict"])
        self.assertIn("DATABASE_IDENTITY_CONFLICT", internal_conflict["failure"])

        # A same-number row does not bind from database state alone.
        database_only_inventory = copy.deepcopy(inventory)
        database_only_id = min(numeric_proven)
        database_only_inventory["official_numeric_identity_evidence"].pop(
            str(database_only_id))
        database_only = classify_database_identity_rows(
            inventory=database_only_inventory, player_rows=player_rows,
            external_rows=external_rows, preserved_player_ids=preserved,
            authorized_lookup_ids=lookups, slate_date="2026-09-20")
        self.assertIn(database_only_id,
                      database_only["classification"]["new_official_lookup"])
        original_name = next(row["full_name"] for row in player_rows
                             if row["player_id"] == database_only_id)
        self.assertEqual(original_name, "A. Player")

    def test_numeric_identity_provenance_fails_closed_on_tampering_or_wrong_slate(self):
        authority, roster = _bindings()
        cases = []
        changed_index = copy.deepcopy(authority)
        next(row for row in changed_index["responses"]
             if row["endpoint_family"] == "BOXSCORE")["index_sha256"] = "0" * 64
        cases.append((changed_index, "INDEX_CHANGED"))
        changed_object = copy.deepcopy(authority)
        next(row for row in changed_object["responses"]
             if row["endpoint_family"] == "BOXSCORE")["object_sha256"] = "0" * 64
        cases.append((changed_object, "INDEX_IDENTITY_MISMATCH"))
        changed_journal = copy.deepcopy(authority)
        changed_journal["source_journal_sha256"] = "0" * 64
        cases.append((changed_journal, "JOURNAL_CHANGED"))
        changed_tree = copy.deepcopy(authority)
        changed_tree["tree_fingerprint"] = "0" * 64
        cases.append((changed_tree, "TREE_CHANGED"))
        wrong_date = copy.deepcopy(authority)
        next(row for row in wrong_date["responses"]
             if row["endpoint_family"] == "BOXSCORE")["resource_identity"][
                 "slate_date"] = "2026-09-21"
        cases.append((wrong_date, "GAME_SET_MISMATCH"))
        wrong_game_set = copy.deepcopy(authority)
        wrong_game_set["canonical_game_set_hash"] = "0" * 64
        cases.append((wrong_game_set, "GAME_SET_MISMATCH"))
        for changed, failure in cases:
            with self.subTest(failure=failure), self.assertRaisesRegex(RuntimeError, failure):
                local_conditional_lookup_inventory(changed, roster)

    def test_goalie_resolution_is_exact_id_only_and_never_silent_alias(self):
        self.assertEqual(resolve_goalie_player_id(8485525, {8485525: 8485525}), 8485525)
        self.assertIsNone(resolve_goalie_player_id(8485525, {}))
        with self.assertRaisesRegex(RuntimeError, "NOT_SAME_NUMBER"):
            resolve_goalie_player_id(8485525, {8485525: 8482103})

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
