import copy
import inspect
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.official_request_journal import (
    request_run_tree_fingerprint,
    sha256_bytes,
    verify_preserved_response_run,
)
from backend.nhl.postgame_reconcile.core import (
    SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS,
    validate_request_lineage,
)
from backend.nhl.postgame_reconcile import staging_sync as sync
from backend.nhl.scripts.run_nhl_postgame_reconciliation import main as reconciliation_main


ROOT = Path(__file__).resolve().parents[2]
SLATE = "2026-09-20"
SOURCE_RUN = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
SOURCE_ROOT = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/"
               "request_runs/2026-09-20" / SOURCE_RUN)
JOURNAL_SHA = "2994ef2bd162c483eacc20b02cf83cd0938fb5f79fd76d208ce57981ed473cb7"
TREE_SHA = "c6e6402b2eba12a8efb30a60df607324cf8684361d357fb5340cfc9051fa7110"
GAME_IDS = list(range(2026010008, 2026010015))


def real_evidence():
    return sync.load_verified_authoritative_staging_set(
        SOURCE_ROOT, source_run_id=SOURCE_RUN, slate_date=SLATE,
        game_ids=GAME_IDS, repository_root=ROOT,
        expected_journal_sha256=JOURNAL_SHA,
        expected_tree_fingerprint=TREE_SHA)


def synthetic_evidence():
    game_ids = list(range(101, 108))
    skaters = []
    goalies = []
    goalie_rows = []
    starters = []
    goalie_team_membership = {}
    for game_id in game_ids:
        for index in range(36):
            skaters.append({
                "game_id": game_id, "player_id": game_id * 1000 + index,
                "game_date": SLATE, "team_id": 1 if index < 18 else 2,
                "opponent_id": 2 if index < 18 else 1, "is_home": index < 18,
                "shots_on_goal": 0, "shot_attempts": None, "toi_minutes": 10.0,
                "pp_toi_minutes": None, "goals": 0, "assists": 0, "blocks": 0,
                "official_identity_locator": f"fixture[{index}]",
            })
        game_goalies = [(game_id, game_id * 100 + index) for index in range(4)]
        goalies.extend(game_goalies)
        starters.extend([game_goalies[1], game_goalies[3]])
        teams = [game_id * 10 + 1, game_id * 10 + 2]
        goalie_team_membership[str(game_id)] = teams
        for index, (unused_game, player_id) in enumerate(game_goalies):
            goalie_rows.append({
                "game_id": game_id, "player_id": player_id,
                "team_id": teams[0] if index < 2 else teams[1],
                "toi_minutes": 10.0 if index % 2 == 0 else 50.0,
                "official_identity_locator": f"goalies[{index}].playerId",
            })
    identities = [(row["game_id"], row["player_id"]) for row in skaters]
    game_hash = sync.canonical_game_set_hash(game_ids)
    return {
        "contract_version": sync.CONTRACT, "slate_date": SLATE,
        "game_ids": game_ids, "canonical_game_set_sha256": game_hash,
        "authority_source_run_id": "fixture", "authority_source_journal_sha256": "a" * 64,
        "authority_source_tree_fingerprint": "b" * 64,
        "authority_response_set_sha256": "c" * 64,
        "skater_rows": skaters, "skater_identities": identities,
        "goalie_rows": goalie_rows, "goalie_identities": goalies,
        "starter_identities": starters,
        "goalie_team_membership": goalie_team_membership,
        "expected_identity_set_sha256": sync.identity_set_sha256(identities),
        "per_game": {str(game): {"skaters": 36, "goalies": 4} for game in game_ids},
        "exact_provider_id_resolution": True,
    }


class FakeConnection:
    def __init__(self, identities=None):
        self.original = set(identities or [])
        self.working = set(self.original)
        self.original_payload_version = "pre_correction"
        self.working_payload_version = self.original_payload_version
        self.committed = False
        self.rollback_count = 0
        self.closed = False

    def commit(self):
        self.original = set(self.working)
        self.original_payload_version = self.working_payload_version
        self.committed = True

    def rollback(self):
        self.working = set(self.original)
        self.working_payload_version = self.original_payload_version
        self.rollback_count += 1

    def close(self):
        self.closed = True


class CaptureCursor:
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, statement, params=None):
        self.statements.append(" ".join(str(statement).split()))


class CaptureConnection:
    def __init__(self):
        self.statements = []

    def cursor(self):
        return CaptureCursor(self.statements)


class NHLPostgameStagingSyncTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.authority = real_evidence()

    def test_official_j_slavin_is_exact_8481004_not_roster_8476958(self):
        identities = set(self.authority["skater_identities"])
        self.assertIn((2026010010, 8481004), identities)
        self.assertNotIn((2026010010, 8476958), identities)
        row = next(row for row in self.authority["skater_rows"]
                   if row["game_id"] == 2026010010 and row["player_id"] == 8481004)
        self.assertEqual(row["official_identity_locator"],
                         "$.playerByGameStats.awayTeam.forwards[0].playerId")
        roster = json.loads((ROOT / "artifacts/operational/nhl/postgame_reconciliation/"
                             "request_runs/2026-09-20/"
                             "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9/"
                             "preserved_responses/objects/"
                             "531ce7482f7a36fa65049d0106cb627980a3cf6e014d42a1688bbe7d590902f9.json").read_text())
        roster_ids = {int(row["id"]) for section in ("forwards", "defensemen", "goalies")
                      for row in roster.get(section) or []}
        self.assertIn(8476958, roster_ids)
        self.assertNotIn(8476958, {player_id for game_id, player_id in identities
                                  if game_id == 2026010010})

    def test_real_authority_shape_is_exact(self):
        self.assertEqual(len(self.authority["game_ids"]), 7)
        self.assertEqual(len(self.authority["skater_identities"]), 252)
        self.assertEqual(len(self.authority["goalie_identities"]), 28)
        self.assertEqual(len(self.authority["starter_identities"]), 14)
        self.assertEqual(len(self.authority["goalie_rows"]), 28)
        self.assertTrue(all(len(teams) == 2 for teams in
                            self.authority["goalie_team_membership"].values()))
        self.assertTrue(all(value == {"skaters": 36, "goalies": 4}
                            for value in self.authority["per_game"].values()))

    def test_one_stale_row_is_derived_from_set_difference_without_hard_coding(self):
        evidence = synthetic_evidence()
        arbitrary_extra = (evidence["game_ids"][2], 99999999)
        existing = list(evidence["skater_identities"]) + [arbitrary_extra]
        result = sync.diff_staging_identities(evidence, existing)
        self.assertEqual(result["extra_identities"], [arbitrary_extra])
        source = Path(sync.__file__).read_text()
        self.assertNotIn("8476958", source)
        self.assertNotIn("8481004", source)

    def test_missing_and_duplicate_identity_fail_closed(self):
        evidence = synthetic_evidence()
        missing = sync.diff_staging_identities(
            evidence, evidence["skater_identities"][:-1])
        self.assertEqual(len(missing["missing_identities"]), 1)
        with self.assertRaisesRegex(RuntimeError, "EXISTING_DUPLICATE"):
            sync.diff_staging_identities(
                evidence, list(evidence["skater_identities"]) +
                [tuple(evidence["skater_identities"][0])])

    def test_malformed_or_duplicate_official_identity_fails(self):
        binding = verify_preserved_response_run(
            SOURCE_ROOT, expected_run_id=SOURCE_RUN, slate_date=SLATE,
            game_ids=GAME_IDS, repository_root=ROOT,
            expected_journal_sha256=JOURNAL_SHA, expected_tree_fingerprint=TREE_SHA)
        with tempfile.TemporaryDirectory(prefix="nhl_stage_bad_object_") as tmp:
            cache = Path(tmp)
            (cache / "objects").mkdir()
            broken = json.loads(json.dumps(binding))
            broken["source_cache"] = str(cache)
            for claim in broken["responses"]:
                source = Path(binding["source_cache"]) / "objects" / f"{claim['object_sha256']}.json"
                body = source.read_bytes()
                if claim["endpoint_family"] == "BOXSCORE" and claim["resource_identity"]["game_id"] == GAME_IDS[0]:
                    payload = json.loads(body)
                    row = payload["playerByGameStats"]["homeTeam"]["forwards"][0]
                    payload["playerByGameStats"]["homeTeam"]["forwards"].append(dict(row))
                    body = json.dumps(payload, separators=(",", ":")).encode()
                    claim["object_sha256"] = sha256_bytes(body)
                    claim["response_bytes"] = len(body)
                (cache / "objects" / f"{claim['object_sha256']}.json").write_bytes(body)
            with self.assertRaisesRegex(RuntimeError, "SKATERS_PER_GAME|DUPLICATE"):
                sync.build_authoritative_staging_set(
                    broken, slate_date=SLATE, expected_game_ids=GAME_IDS)

    def test_tampered_source_tree_fails_before_database(self):
        with tempfile.TemporaryDirectory(prefix="nhl_stage_source_") as tmp:
            repository = Path(tmp)
            request_root = repository / "runs" / SOURCE_RUN
            shutil.copytree(SOURCE_ROOT, request_root)
            tree = request_run_tree_fingerprint(request_root, repository_root=repository)
            object_path = next((request_root / "preserved_responses/objects").glob("*.json"))
            object_path.write_bytes(object_path.read_bytes() + b"\n")
            with patch("backend.nhl.postgame_reconcile.staging_sync.psycopg.connect") as database:
                with self.assertRaisesRegex(RuntimeError, "TREE_FINGERPRINT_MISMATCH"):
                    sync.load_verified_authoritative_staging_set(
                        request_root, source_run_id=SOURCE_RUN, slate_date=SLATE,
                        game_ids=GAME_IDS, repository_root=repository,
                        expected_journal_sha256=JOURNAL_SHA,
                        expected_tree_fingerprint=tree)
            database.assert_not_called()

    def test_wrong_authorized_digest_fails_before_mutation(self):
        evidence = synthetic_evidence()
        connection = FakeConnection(evidence["skater_identities"])
        with patch.object(sync, "_begin_correction"), \
             patch.object(sync, "_fetch_canonical_game_ids", return_value=evidence["game_ids"]), \
             patch.object(sync, "_fetch_target_skater_identities",
                          return_value=list(connection.working)), \
             patch.object(sync, "_materialize_expected_rows") as materialize, \
             patch.object(sync, "_upsert_expected_rows") as upsert, \
             patch.object(sync, "_delete_extra_rows") as delete:
            with self.assertRaisesRegex(RuntimeError, "AUTHORIZED_EXTRA_SET_DIGEST_MISMATCH"):
                sync.synchronize_staging_set(
                    "offline", evidence, authorized_extra_digest="f" * 64,
                    connection_factory=lambda *args, **kwargs: connection)
        materialize.assert_not_called(); upsert.assert_not_called(); delete.assert_not_called()
        self.assertFalse(connection.committed)
        self.assertTrue(connection.closed)

    def _run_fake_sync(self, *, extra=None, fail=False, staged_goalies=None,
                       final_failure=False):
        evidence = synthetic_evidence()
        extra = extra or []
        connection = FakeConnection(list(evidence["skater_identities"]) + extra)
        difference = sync.diff_staging_identities(evidence, connection.working)

        def delete(*unused_args, **unused_kwargs):
            for identity in difference["extra_identities"]:
                connection.working.remove(tuple(identity))
            return difference["extra_identities"]

        def fetch(*unused_args, **unused_kwargs):
            return sorted(connection.working)

        def upsert(*unused_args, **unused_kwargs):
            connection.working_payload_version = "authoritative"
            return 252

        staged_goalies = (list(evidence["goalie_identities"])
                          if staged_goalies is None else staged_goalies)
        if final_failure:
            staged_goalies = list(evidence["goalie_identities"][:-1])
        with patch.object(sync, "_begin_correction"), \
             patch.object(sync, "_fetch_canonical_game_ids", return_value=evidence["game_ids"]), \
             patch.object(sync, "_fetch_target_skater_identities", side_effect=fetch), \
             patch.object(sync, "_materialize_expected_rows"), \
             patch.object(sync, "_upsert_expected_rows", side_effect=upsert), \
             patch.object(sync, "_delete_extra_rows", side_effect=delete), \
             patch.object(sync, "_fetch_goalie_stage_identities",
                          return_value=staged_goalies):
            injector = ((lambda: (_ for _ in ()).throw(RuntimeError("simulated")))
                        if fail else None)
            if fail or final_failure:
                expected_error = "simulated" if fail else "STAGING_IDENTITY_SET_MISMATCH"
                with self.assertRaisesRegex(RuntimeError, expected_error):
                    sync.synchronize_staging_set(
                        "offline", evidence,
                        authorized_extra_digest=difference["authorized_extra_set_digest"],
                        connection_factory=lambda *args, **kwargs: connection,
                        failure_injector=injector)
                return evidence, connection, None
            result = sync.synchronize_staging_set(
                "offline", evidence,
                authorized_extra_digest=difference["authorized_extra_set_digest"],
                connection_factory=lambda *args, **kwargs: connection)
            return evidence, connection, result

    def test_target_stale_row_removed_and_replay_deletes_zero(self):
        evidence, connection, result = self._run_fake_sync(extra=[(103, 99999999)])
        self.assertTrue(connection.committed)
        self.assertEqual(result["deleted_identities"], [(103, 99999999)])
        self.assertEqual(result["staging_equality"]["games"], 7)
        self.assertEqual(result["staging_equality"]["skater_appearances"], 252)
        self.assertEqual(result["staging_equality"]["goalie_appearances"], 28)
        self.assertEqual(result["staging_equality"]["confirmed_starters"], 14)
        unused, replay_connection, replay = self._run_fake_sync()
        self.assertTrue(replay_connection.committed)
        self.assertEqual(replay["deleted_rows"], 0)

    def test_mid_transaction_failure_rolls_back_complete_original_set(self):
        extra = (103, 99999999)
        evidence, connection, unused = self._run_fake_sync(extra=[extra], fail=True)
        self.assertFalse(connection.committed)
        self.assertEqual(connection.working,
                         set(evidence["skater_identities"]) | {extra})
        self.assertEqual(connection.working_payload_version, "pre_correction")
        self.assertGreaterEqual(connection.rollback_count, 1)

    def test_final_goalie_validation_failure_rolls_back_skater_changes(self):
        extra = (103, 99999999)
        evidence, connection, unused = self._run_fake_sync(
            extra=[extra], final_failure=True)
        self.assertFalse(connection.committed)
        self.assertEqual(connection.working,
                         set(evidence["skater_identities"]) | {extra})
        self.assertEqual(connection.working_payload_version, "pre_correction")
        self.assertGreaterEqual(connection.rollback_count, 1)

    def test_nullable_mixed_and_incorrect_staged_goalie_teams_are_non_authoritative(self):
        evidence = synthetic_evidence()
        identities = list(evidence["goalie_identities"])
        all_null = [(game_id, player_id, None) for game_id, player_id in identities]
        mixed = [(game_id, player_id, None if index % 2 else -999999)
                 for index, (game_id, player_id) in enumerate(identities)]
        for staged in (all_null, mixed):
            result = sync._validate_postwrite_staging(
                evidence, actual_skaters=list(evidence["skater_identities"]),
                staged_goalie_rows=staged)
            self.assertEqual(result["goalie_appearances"], 28)
            self.assertEqual(result["confirmed_starters"], 14)

    def test_staged_goalie_membership_differences_fail_closed(self):
        evidence = synthetic_evidence()
        identities = list(evidence["goalie_identities"])
        cases = [
            identities[:-1],
            identities + [(evidence["game_ids"][0], 99999999)],
            identities + [identities[0]],
        ]
        for staged in cases:
            with self.subTest(staged_count=len(staged)):
                with self.assertRaisesRegex(RuntimeError, "STAGING_IDENTITY_SET_MISMATCH"):
                    sync._validate_postwrite_staging(
                        evidence, actual_skaters=list(evidence["skater_identities"]),
                        staged_goalie_rows=staged)

    def test_official_goalie_membership_and_team_failures_are_closed(self):
        evidence = synthetic_evidence()
        missing_team = copy.deepcopy(evidence["goalie_rows"])
        missing_team[0]["team_id"] = None
        conflict = copy.deepcopy(evidence["goalie_rows"])
        conflicting_row = dict(conflict[0])
        conflicting_row["team_id"] = int(conflicting_row["team_id"]) + 1
        conflict.append(conflicting_row)
        incomplete = copy.deepcopy(evidence["goalie_rows"])
        first_game = evidence["game_ids"][0]
        first_team = incomplete[0]["team_id"]
        for row in incomplete:
            if row["game_id"] == first_game:
                row["team_id"] = first_team
        cases = [
            (missing_team, "TEAM_MEMBERSHIP_INVALID"),
            (conflict, "CONFLICTING_TEAM"),
            (incomplete, "TEAM_CARDINALITY"),
        ]
        for rows, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(RuntimeError, error):
                    sync._authoritative_goalie_contract(
                        rows, game_ids=evidence["game_ids"])

    def test_official_goalie_tied_or_unusable_toi_fails_closed(self):
        evidence = synthetic_evidence()
        tied = copy.deepcopy(evidence["goalie_rows"])
        tied[0]["toi_minutes"] = tied[1]["toi_minutes"]
        unusable = copy.deepcopy(evidence["goalie_rows"])
        unusable[0]["toi_minutes"] = None
        for rows, error in ((tied, "STARTER_NOT_UNIQUE"),
                            (unusable, "TOI_UNUSABLE")):
            with self.subTest(error=error):
                with self.assertRaisesRegex(RuntimeError, error):
                    sync._authoritative_goalie_contract(
                        rows, game_ids=evidence["game_ids"])

    def test_goalie_stage_query_cannot_convert_nullable_team_id(self):
        source = inspect.getsource(sync._fetch_goalie_stage_identities)
        self.assertNotIn("team_id", source)
        self.assertNotIn("toi_minutes", source)
        self.assertNotIn("int(team_id)", source)

    def test_scope_and_concurrency_guards_are_structural(self):
        source = inspect.getsource(sync._delete_extra_rows)
        self.assertIn("target.game_date=%s::date", source)
        self.assertIn("target.game_id=ANY(%s)", source)
        connection = CaptureConnection()
        sync._begin_correction(connection)
        statements = "\n".join(connection.statements)
        self.assertIn("ISOLATION LEVEL SERIALIZABLE", statements)
        self.assertIn("LOCK TABLE nhl.import_skater_logs_stage IN SHARE ROW EXCLUSIVE MODE",
                      statements)

    def test_read_only_preflight_rolls_back_and_reports_all_hashes(self):
        evidence = synthetic_evidence()
        extra = (103, 99999999)
        connection = FakeConnection(list(evidence["skater_identities"]) + [extra])
        with patch.object(sync, "_begin_read_only"), \
             patch.object(sync, "_fetch_canonical_game_ids", return_value=evidence["game_ids"]), \
             patch.object(sync, "_fetch_target_skater_identities",
                          return_value=sorted(connection.working)), \
             patch.object(sync, "_materialize_expected_rows") as mutation:
            result = sync.staging_set_preflight(
                "offline", evidence,
                connection_factory=lambda *args, **kwargs: connection)
        mutation.assert_not_called()
        self.assertFalse(connection.committed)
        self.assertTrue(connection.closed)
        self.assertEqual(result["extra_identities"], [[103, 99999999]]
                         if isinstance(result["extra_identities"][0], list)
                         else [(103, 99999999)])
        for key in ("existing_identity_set_sha256", "missing_identity_set_sha256",
                    "extra_identity_set_sha256", "authorized_extra_set_digest"):
            self.assertRegex(result[key], r"^[0-9a-f]{64}$")
        self.assertFalse(result["request_run_created"])
        self.assertEqual(result["database_writes"], 0)

    def test_correction_cli_cannot_continue_into_reconciliation(self):
        evidence = synthetic_evidence()
        result = {"status": "STAGING_SET_SYNCHRONIZED", "request_run_created": False}
        argv = ["run_nhl_postgame_reconciliation.py", SLATE, "--correct-staging-set",
                "--authority-response-source-run-id", SOURCE_RUN,
                "--authorized-extra-set-digest", "a" * 64]
        output = []
        with patch.object(sys, "argv", argv), \
             patch.dict(os.environ, {"SUPABASE_DB_URL": "postgresql://offline.invalid/test"}), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation."
                   "load_verified_authoritative_staging_set", return_value=evidence), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation."
                   "synchronize_staging_set", return_value=result), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation._run") as child, \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.official_get") as network, \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation."
                   "publish_reconciliation") as publish, \
             patch("builtins.print", side_effect=lambda value: output.append(value)):
            code = reconciliation_main()
        self.assertEqual(code, 0)
        child.assert_not_called(); network.assert_not_called(); publish.assert_not_called()
        self.assertEqual(json.loads(output[-1])["status"], "STAGING_SET_SYNCHRONIZED")

    def test_september_20_lineage_requires_fifth_failed_run(self):
        sources = [
            {"role": "AUTHORITY_RESPONSE_SOURCE", "source_run_id": "authority"},
            {"role": "ROSTER_RESPONSE_SOURCE", "source_run_id": "roster"},
            {"role": "PLAYER_IDENTITY_RESPONSE_SOURCE", "source_run_id": "player"},
        ]
        ancestors = [{"role": "FAILED_EXECUTION_ANCESTOR", "run_id": run_id}
                     for run_id in SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS]
        valid = {"contract_version": "NHL_POSTGAME_REQUEST_LINEAGE_V5",
                 "response_sources": sources, "failed_ancestors": ancestors}
        validate_request_lineage(valid, slate_date=SLATE)
        with self.assertRaisesRegex(RuntimeError, "ALL_FIVE"):
            validate_request_lineage(
                {**valid, "failed_ancestors": ancestors[:-1]}, slate_date=SLATE)
        self.assertEqual(len(SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS), 5)
        self.assertTrue(all("staging" not in run_id.lower()
                            for run_id in SEPTEMBER_20_REQUIRED_FAILED_ANCESTORS))


if __name__ == "__main__":
    unittest.main()
