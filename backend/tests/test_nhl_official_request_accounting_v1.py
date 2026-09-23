from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.official_request_journal import (
    ENV_CACHE,
    ENV_AUTHORIZED_PLAYER_LOOKUP_IDS,
    ENV_GAME_HASH,
    ENV_GAME_IDS,
    ENV_JOURNAL,
    ENV_REQUIRED,
    ENV_RUN_ID,
    ENV_SLATE,
    ENV_SOURCE_CACHE,
    ENV_SOURCE_JOURNAL_SHA256,
    ENV_SOURCE_RUN_ID,
    ROSTER_REDIRECT_POLICY,
    RequestContext,
    canonical_game_set_hash,
    official_get,
    official_season_id,
    read_journal,
    summarize_journal,
    verify_preserved_response_run,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    _binding_from_acquisition,
    authority_roster_acquisition,
)


SLATE = "2026-09-19"
GAMES = list(range(2026010001, 2026010008))


class FakeResponse:
    def __init__(self, payload, status=200, headers=None, content=None):
        self.content = json.dumps(payload).encode() if content is None else content
        self.status_code = status
        self.headers = headers or {}


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.headers = {}

    def get(self, url, **kwargs):
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self):
        return None


def schedule_payload():
    return {"gameWeek": [{"date": SLATE, "games": [
        {"id": gid, "gameState": "FINAL", "homeTeam": {"id": 1}, "awayTeam": {"id": 2}}
        for gid in GAMES
    ]}]}


class OfficialRequestAccountingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="nhl_request_journal_")
        self.root = Path(self.tmp.name)
        self.env = {
            ENV_REQUIRED: "1", ENV_RUN_ID: "fixture_run",
            ENV_JOURNAL: str(self.root / "journal.jsonl"),
            ENV_CACHE: str(self.root / "cache"), ENV_SLATE: SLATE,
            ENV_GAME_HASH: canonical_game_set_hash(GAMES),
            ENV_GAME_IDS: ",".join(map(str, GAMES)),
        }

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, outcomes, **kwargs):
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            return official_get(**kwargs)

    def test_parent_day_schedule_and_exact_authority_eight(self):
        outcomes = [FakeResponse(schedule_payload())] + [FakeResponse({"id": gid}) for gid in GAMES]
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            official_get("https://example.invalid/schedule", timeout=1, stage="AUTHORITY",
                         endpoint_family="SCHEDULE", identity={"slate_date": SLATE},
                         authority_boundary=True, preserve_response=True).raise_for_status()
            for gid in GAMES:
                official_get("https://example.invalid/box", timeout=1, stage="AUTHORITY",
                             endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": gid},
                             authority_boundary=True, preserve_response=True).raise_for_status()
        summary = summarize_journal(Path(self.env[ENV_JOURNAL]), run_id="fixture_run",
                                    expected_game_hash=self.env[ENV_GAME_HASH])
        self.assertEqual(summary["authority_boundary_logical_requests"], 8)
        self.assertEqual(summary["total_network_attempts"], 8)
        self.assertEqual(summary["successful_responses"], 8)

    def test_retry_success_exhausted_and_fallback_are_exact(self):
        outcomes = [FakeResponse({"data": []}, 503), FakeResponse({"data": []}, 200),
                    FakeResponse({"data": []}, 503), FakeResponse({"data": []}, 503),
                    FakeResponse({"data": []}, 200)]
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            official_get("https://example.invalid/shift", timeout=1, stage="SHIFT",
                         endpoint_family="SHIFT_CHART",
                         identity={"slate_date": SLATE, "game_id": GAMES[0]},
                         max_attempts=2, retry_statuses={503}).raise_for_status()
            failed = official_get("https://example.invalid/pbp", timeout=1, stage="PBP",
                                  endpoint_family="PLAY_BY_PLAY",
                                  identity={"slate_date": SLATE, "game_id": GAMES[1]},
                                  max_attempts=2, retry_statuses={503})
            with self.assertRaises(Exception):
                failed.raise_for_status()
            official_get("https://example.invalid/roster", timeout=1, stage="ROSTER",
                         endpoint_family="ROSTER",
                         identity={"slate_date": SLATE, "team": "AAA", "roster_variant": "2026"},
                         request_class="FALLBACK").raise_for_status()
        summary = summarize_journal(Path(self.env[ENV_JOURNAL]), run_id="fixture_run",
                                    expected_game_hash=self.env[ENV_GAME_HASH])
        self.assertEqual(summary["total_logical_requests"], 3)
        self.assertEqual(summary["total_network_attempts"], 5)
        self.assertEqual(summary["retries"], 2)
        self.assertEqual(summary["failed_attempts"], 3)
        self.assertEqual(summary["fallback_attempts"], 1)

    def test_player_landing_requires_explicit_id_before_transport(self):
        env = {**self.env, ENV_AUTHORIZED_PLAYER_LOOKUP_IDS: ""}
        with patch.dict(os.environ, env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session") as transport:
            with self.assertRaisesRegex(RuntimeError, "NOT_EXPLICITLY_AUTHORIZED"):
                official_get(
                    "https://api-web.nhle.com/v1/player/8480000/landing",
                    timeout=1, stage="IDENTITY", endpoint_family="PLAYER_LANDING",
                    identity={"slate_date": SLATE, "player_id": 8480000})
        transport.assert_not_called()
        rows = read_journal(Path(self.env[ENV_JOURNAL]))
        self.assertEqual(rows[-1]["event_kind"], "REQUEST_REJECTED")

    def test_eight_game_acquisition_only_topology_and_shared_typed_source(self):
        slate = "2026-09-21"
        games = list(range(2026010015, 2026010023))
        teams = ["BUF", "CBJ", "CHI", "COL", "DAL", "DET", "MIN", "MTL",
                 "NJD", "NYR", "OTT", "PHI", "PIT", "STL", "WPG", "WSH"]
        schedule = {"gameWeek": [{"date": slate, "games": [
            {"id": game_id, "gameState": "FINAL",
             "homeTeam": {"id": index * 2 + 1},
             "awayTeam": {"id": index * 2 + 2}}
            for index, game_id in enumerate(games)]}]}
        outcomes = [FakeResponse(schedule)]
        outcomes.extend(FakeResponse({"id": game_id}) for game_id in games)
        for team in teams:
            outcomes.extend([
                FakeResponse({}, 307, {
                    "Location": f"https://api-web.nhle.com/v1/roster/{team}/20262027"},
                    content=b""),
                FakeResponse({"forwards": [{"id": 8000000 + len(outcomes),
                                             "firstName": {"default": "A"},
                                             "lastName": {"default": "Player"}}]}),
            ])
        output = self.root / "postgame"
        source = {"game_ids": games, "team_codes": teams}
        with patch.dict(os.environ, {}, clear=True), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.ROOT", self.root), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect") as database, \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            receipt = authority_roster_acquisition(
                slate_date=slate, source_binding=source, output_root=output)
        database.assert_not_called()
        accounting = receipt["request_accounting"]
        self.assertEqual(accounting["total_logical_requests"], 25)
        self.assertEqual(accounting["total_network_attempts"], 41)
        self.assertEqual(accounting["successful_responses"], 25)
        self.assertEqual(accounting["allowed_redirects"], 16)
        self.assertEqual(accounting["cache_reuse_events"], 0)
        journal = read_journal(output / "request_runs" / slate / receipt["run_id"] /
                               "official_request_journal.jsonl")
        self.assertEqual({row["endpoint_family"] for row in journal},
                         {"SCHEDULE", "BOXSCORE", "ROSTER"})
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.ROOT", self.root):
            authority = _binding_from_acquisition(
                output_root=output, run_id=receipt["run_id"],
                role="AUTHORITY_RESPONSE_SOURCE", slate_date=slate,
                game_ids=games, teams=teams)
            roster = _binding_from_acquisition(
                output_root=output, run_id=receipt["run_id"],
                role="ROSTER_RESPONSE_SOURCE", slate_date=slate,
                game_ids=games, teams=teams)
        self.assertEqual(authority["source_run_id"], roster["source_run_id"])
        self.assertEqual(len(authority["responses"]), 9)
        self.assertEqual(len(roster["responses"]), 16)
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.ROOT", self.root):
            with self.assertRaises(RuntimeError):
                _binding_from_acquisition(
                    output_root=output, run_id=receipt["run_id"],
                    role="AUTHORITY_RESPONSE_SOURCE", slate_date=slate,
                    game_ids=games[:-1], teams=teams)
            with self.assertRaises(RuntimeError):
                _binding_from_acquisition(
                    output_root=output, run_id=receipt["run_id"],
                    role="AUTHORITY_RESPONSE_SOURCE", slate_date="2026-09-22",
                    game_ids=games, teams=teams)
        journal_path = (output / "request_runs" / slate / receipt["run_id"] /
                        "official_request_journal.jsonl")
        journal_path.write_text(journal_path.read_text().replace(
            '"response_bytes":', '"response_bytes": 1, "original_response_bytes":', 1))
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.ROOT", self.root):
            with self.assertRaisesRegex(RuntimeError, "RECEIPT_IDENTITY_MISMATCH"):
                _binding_from_acquisition(
                    output_root=output, run_id=receipt["run_id"],
                    role="AUTHORITY_RESPONSE_SOURCE", slate_date=slate,
                    game_ids=games, teams=teams)

    def test_partial_acquisition_retains_journal_without_receipt_or_retry(self):
        slate = "2026-09-21"
        games = [2026010015]
        teams = ["COL", "WPG"]
        outcomes = [FakeResponse({"gameWeek": [{"date": slate, "games": [
            {"id": games[0], "gameState": "FINAL",
             "homeTeam": {"id": 1}, "awayTeam": {"id": 2}}]}]}),
                    FakeResponse({"id": games[0]}), FakeResponse({}, 500)]
        output = self.root / "partial"
        with patch.dict(os.environ, {}, clear=True), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.ROOT", self.root), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            with self.assertRaises(Exception):
                authority_roster_acquisition(
                    slate_date=slate,
                    source_binding={"game_ids": games, "team_codes": teams},
                    output_root=output)
        runs = list((output / "request_runs" / slate).iterdir())
        self.assertEqual(len(runs), 1)
        journal = read_journal(runs[0] / "official_request_journal.jsonl")
        self.assertEqual(len(journal), 3)
        self.assertEqual(journal[-1]["attempt_number"], 1)
        self.assertFalse((output / "acquisition_receipts" / slate /
                          f"{runs[0].name}.json").exists())

    def test_roster_redirect_is_one_logical_operation_and_two_distinct_attempts(self):
        current = "https://api-web.nhle.com/v1/roster/NJD/current"
        destination = "https://api-web.nhle.com/v1/roster/NJD/20262027"
        outcomes = [
            FakeResponse({}, 307, {"Location": destination}, content=b""),
            FakeResponse({"forwards": [{"id": 1}]}),
        ]
        response = self.call(
            outcomes, url=current, timeout=1, stage="ROSTER_COLLECTION",
            endpoint_family="ROSTER",
            identity={"slate_date": SLATE, "team": "NJD", "roster_variant": "current"},
            preserve_response=True,
            redirect_policy={"policy": ROSTER_REDIRECT_POLICY, "team": "NJD",
                             "repository_season": 2026},
        )
        self.assertEqual(response.status_code, 200)
        rows = read_journal(Path(self.env[ENV_JOURNAL]))
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["logical_request_id"] for row in rows}, {rows[0]["logical_request_id"]})
        self.assertEqual([row["attempt_number"] for row in rows], [1, 2])
        self.assertEqual(rows[0]["final_disposition"], "ALLOWED_REDIRECT")
        self.assertEqual(rows[1]["attempt_reason"], "REDIRECT_FOLLOW")
        self.assertEqual(rows[0]["redirect_location"], destination)
        self.assertFalse(rows[0]["response_preserved"])
        self.assertTrue(rows[1]["response_preserved"])
        self.assertEqual(len(list((self.root / "cache/objects").iterdir())), 1)
        summary = summarize_journal(Path(self.env[ENV_JOURNAL]), run_id="fixture_run",
                                    expected_game_hash=self.env[ENV_GAME_HASH])
        self.assertEqual(summary["allowed_redirects"], 1)
        self.assertEqual(summary["total_network_attempts"], 2)
        self.assertEqual(summary["successful_responses"], 1)
        self.assertEqual(summary["failed_attempts"], 0)
        self.assertEqual(summary["retries"], 0)
        self.assertEqual(summary["fallback_attempts"], 0)

    def test_roster_redirect_allowlist_rejects_without_fallback_or_preservation(self):
        current = "https://api-web.nhle.com/v1/roster/NJD/current"
        rejected = {
            "HOST": "https://example.com/v1/roster/NJD/20262027",
            "DOWNGRADE": "http://api-web.nhle.com/v1/roster/NJD/20262027",
            "FOUR_DIGIT": "https://api-web.nhle.com/v1/roster/NJD/2026",
            "WRONG_SEASON": "https://api-web.nhle.com/v1/roster/NJD/20272028",
            "WRONG_TEAM": "https://api-web.nhle.com/v1/roster/NYR/20262027",
            "QUERY": "https://api-web.nhle.com/v1/roster/NJD/20262027?x=1",
            "FRAGMENT": "https://api-web.nhle.com/v1/roster/NJD/20262027#x",
            "CREDENTIALS": "https://user:pass@api-web.nhle.com/v1/roster/NJD/20262027",
            "NONSTANDARD_PORT": "https://api-web.nhle.com:444/v1/roster/NJD/20262027",
        }
        for label, location in rejected.items():
            with self.subTest(label=label):
                Path(self.env[ENV_JOURNAL]).unlink(missing_ok=True)
                with self.assertRaises(RuntimeError):
                    self.call(
                        [FakeResponse({}, 307, {"Location": location}, content=b"")],
                        url=current, timeout=1, stage="ROSTER_COLLECTION",
                        endpoint_family="ROSTER",
                        identity={"slate_date": SLATE, "team": "NJD",
                                  "roster_variant": "current"},
                        preserve_response=True,
                        redirect_policy={"policy": ROSTER_REDIRECT_POLICY, "team": "NJD",
                                         "repository_season": 2026},
                    )
                rows = read_journal(Path(self.env[ENV_JOURNAL]))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["final_disposition"], "REDIRECT_REJECTED")
                self.assertFalse(rows[0]["response_preserved"])
        self.assertEqual(official_season_id(2026), "20262027")
        roster_source = Path("backend/nhl/scripts/refresh_players_and_roster_today.py").read_text()
        self.assertIn("f\"{BASE}/roster/{tri}/{official_season}\"", roster_source)

    def test_relative_308_roster_redirect_is_normalized_and_accepted(self):
        response = self.call(
            [FakeResponse({}, 308, {"Location": "/v1/roster/NJD/20262027"}, content=b""),
             FakeResponse({"goalies": [{"id": 1}]})],
            url="https://api-web.nhle.com/v1/roster/NJD/current", timeout=1,
            stage="ROSTER_COLLECTION", endpoint_family="ROSTER",
            identity={"slate_date": SLATE, "team": "NJD", "roster_variant": "current"},
            redirect_policy={"policy": ROSTER_REDIRECT_POLICY, "team": "NJD",
                             "repository_season": 2026},
        )
        self.assertEqual(response.status_code, 200)
        rows = read_journal(Path(self.env[ENV_JOURNAL]))
        self.assertEqual(rows[0]["redirect_location"],
                         "https://api-web.nhle.com/v1/roster/NJD/20262027")
        self.assertEqual(rows[1]["request_target"]["path"], "/v1/roster/NJD/20262027")

    def test_roster_redirect_missing_malformed_loop_and_max_hop_fail_closed(self):
        current = "https://api-web.nhle.com/v1/roster/NJD/current"
        destination = "https://api-web.nhle.com/v1/roster/NJD/20262027"
        cases = [
            ([FakeResponse({}, 302,
                           {"Location": "https://api-web.nhle.com/v1/roster/NJD/20262027"},
                           content=b"")], "STATUS_NOT_ALLOWLISTED"),
            ([FakeResponse({}, 307, {}, content=b"")], "LOCATION_MISSING"),
            ([FakeResponse({}, 307, {"Location": "https://api-web.nhle.com:bad/x"}, content=b"")],
             "LOCATION_MALFORMED"),
            ([FakeResponse({}, 307, {"Location": destination}, content=b""),
              FakeResponse({}, 308, {"Location": current}, content=b"")], "REDIRECT_LOOP"),
            ([FakeResponse({}, 307, {"Location": destination}, content=b""),
              FakeResponse({}, 307,
                           {"Location": "https://api-web.nhle.com/v1/roster/NJD/20272028"},
                           content=b"")], "MAX_HOPS"),
        ]
        for outcomes, expected in cases:
            with self.subTest(expected=expected):
                Path(self.env[ENV_JOURNAL]).unlink(missing_ok=True)
                with self.assertRaisesRegex(RuntimeError, expected):
                    self.call(
                        outcomes, url=current, timeout=1, stage="ROSTER_COLLECTION",
                        endpoint_family="ROSTER",
                        identity={"slate_date": SLATE, "team": "NJD",
                                  "roster_variant": "current"},
                        preserve_response=True,
                        redirect_policy={"policy": ROSTER_REDIRECT_POLICY, "team": "NJD",
                                         "repository_season": 2026},
                    )
                rows = read_journal(Path(self.env[ENV_JOURNAL]))
                self.assertIn(len(rows), {1, 2})
                self.assertEqual(rows[-1]["final_disposition"], "REDIRECT_REJECTED")
                self.assertFalse((self.root / "cache/objects").exists())

    def test_preserved_reuse_and_hash_mismatch_fail_closed(self):
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession([FakeResponse({"id": GAMES[0]})])):
            official_get("https://example.invalid/box", timeout=1, stage="AUTHORITY",
                         endpoint_family="BOXSCORE",
                         identity={"slate_date": SLATE, "game_id": GAMES[0]},
                         preserve_response=True).raise_for_status()
            reused = official_get("unused", timeout=1, stage="SKATER",
                                  endpoint_family="BOXSCORE",
                                  identity={"slate_date": SLATE, "game_id": GAMES[0]},
                                  reuse_preserved=True)
            self.assertEqual(reused.json()["id"], GAMES[0])
            object_path = next((self.root / "cache/objects").iterdir())
            object_path.write_text(json.dumps({"id": GAMES[1]}))
            with self.assertRaisesRegex(RuntimeError, "HASH_MISMATCH"):
                official_get("unused", timeout=1, stage="GOALIE",
                             endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": GAMES[0]},
                             reuse_preserved=True)

    def test_explicit_cross_run_eight_response_reuse_has_zero_network_attempts(self):
        source_run = "failed_source_fixture"
        source_root = self.root / source_run
        source_env = dict(self.env)
        source_env.update({
            ENV_RUN_ID: source_run,
            ENV_JOURNAL: str(source_root / "official_request_journal.jsonl"),
            ENV_CACHE: str(source_root / "preserved_responses"),
        })
        outcomes = [FakeResponse(schedule_payload())] + [FakeResponse({"id": gid}) for gid in GAMES]
        with patch.dict(os.environ, source_env, clear=True), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            official_get("x", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, authority_boundary=True,
                         preserve_response=True)
            for gid in GAMES:
                official_get("x", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": gid},
                             authority_boundary=True, preserve_response=True)
        lineage = verify_preserved_response_run(
            source_root, expected_run_id=source_run, slate_date=SLATE, game_ids=GAMES)
        target_env = dict(self.env)
        target_env.update({
            ENV_RUN_ID: "target_fixture",
            ENV_JOURNAL: str(self.root / "target/journal.jsonl"),
            ENV_CACHE: str(self.root / "target/cache"),
            ENV_SOURCE_CACHE: lineage["source_cache"],
            ENV_SOURCE_RUN_ID: source_run,
            ENV_SOURCE_JOURNAL_SHA256: lineage["source_journal_sha256"],
        })
        with patch.dict(os.environ, target_env, clear=True), \
             patch("backend.nhl.official_request_journal.requests.Session") as transport:
            official_get("unused", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, authority_boundary=True,
                         reuse_preserved=True)
            for gid in GAMES:
                official_get("unused", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": gid},
                             authority_boundary=True, reuse_preserved=True)
            transport.assert_not_called()
        summary = summarize_journal(Path(target_env[ENV_JOURNAL]), run_id="target_fixture",
                                    expected_game_hash=target_env[ENV_GAME_HASH])
        self.assertEqual(summary["total_network_attempts"], 0)
        self.assertEqual(summary["cache_reuse_events"], 8)
        rows = read_journal(Path(target_env[ENV_JOURNAL]))
        self.assertTrue(all(row["cross_run_reuse"] for row in rows))
        self.assertTrue(all(row["source_run_id"] == source_run for row in rows))

    def test_original_september_20_authority_objects_reuse_without_network(self):
        repository = Path(__file__).resolve().parents[2]
        source_run = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
        game_ids = list(range(2026010008, 2026010015))
        source_root = (repository / "artifacts/operational/nhl/postgame_reconciliation/"
                       "request_runs/2026-09-20" / source_run)
        binding = verify_preserved_response_run(
            source_root, expected_run_id=source_run, slate_date="2026-09-20",
            game_ids=game_ids, repository_root=repository,
            expected_journal_sha256="2994ef2bd162c483eacc20b02cf83cd0938fb5f79fd76d208ce57981ed473cb7",
            expected_tree_fingerprint="c6e6402b2eba12a8efb30a60df607324cf8684361d357fb5340cfc9051fa7110")
        target = self.root / "real-source-target"
        target_env = {
            ENV_REQUIRED: "1", ENV_RUN_ID: "real_source_target",
            ENV_JOURNAL: str(target / "journal.jsonl"), ENV_CACHE: str(target / "cache"),
            ENV_SLATE: "2026-09-20", ENV_GAME_HASH: canonical_game_set_hash(game_ids),
            ENV_GAME_IDS: ",".join(map(str, game_ids)),
            ENV_SOURCE_CACHE: binding["source_cache"], ENV_SOURCE_RUN_ID: source_run,
            ENV_SOURCE_JOURNAL_SHA256: binding["source_journal_sha256"],
        }
        with patch.dict(os.environ, target_env, clear=True), \
             patch("backend.nhl.official_request_journal.requests.Session") as transport:
            official_get("unused", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": "2026-09-20"}, authority_boundary=True,
                         reuse_preserved=True)
            for game_id in game_ids:
                official_get("unused", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                             identity={"slate_date": "2026-09-20", "game_id": game_id},
                             authority_boundary=True, reuse_preserved=True)
            transport.assert_not_called()
        summary = summarize_journal(Path(target_env[ENV_JOURNAL]), run_id="real_source_target",
                                    expected_game_hash=target_env[ENV_GAME_HASH])
        self.assertEqual(summary["authority_boundary_logical_requests"], 8)
        self.assertEqual(summary["total_network_attempts"], 0)
        self.assertEqual(summary["cache_reuse_events"], 8)

    def test_tampered_cross_run_source_fails_without_network_fallback(self):
        source_run = "tampered_source_fixture"
        source_root = self.root / source_run
        source_env = dict(self.env)
        source_env.update({ENV_RUN_ID: source_run,
                           ENV_JOURNAL: str(source_root / "official_request_journal.jsonl"),
                           ENV_CACHE: str(source_root / "preserved_responses")})
        outcomes = [FakeResponse(schedule_payload())] + [FakeResponse({"id": gid}) for gid in GAMES]
        with patch.dict(os.environ, source_env, clear=True), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            official_get("x", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, authority_boundary=True,
                         preserve_response=True)
            for gid in GAMES:
                official_get("x", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": gid},
                             authority_boundary=True, preserve_response=True)
        next((source_root / "preserved_responses/objects").iterdir()).write_bytes(b"{}")
        with patch("backend.nhl.official_request_journal.requests.Session") as transport:
            with self.assertRaisesRegex(RuntimeError, "PRESERVED_SOURCE_(RESPONSE|JOURNAL_RESPONSE)_HASH_MISMATCH"):
                verify_preserved_response_run(
                    source_root, expected_run_id=source_run, slate_date=SLATE, game_ids=GAMES)
            transport.assert_not_called()

    def test_unrelated_game_and_missing_configuration_fail_before_network(self):
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session") as session:
            with self.assertRaisesRegex(RuntimeError, "UNRELATED_GAME_ID"):
                official_get("unused", timeout=1, stage="FIXTURE", endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": 999}, reuse_preserved=True)
            session.assert_not_called()
        broken = dict(self.env)
        broken.pop(ENV_JOURNAL)
        with patch.dict(os.environ, broken, clear=True):
            with self.assertRaisesRegex(RuntimeError, "CONFIG_INCOMPLETE"):
                RequestContext.from_env(required=True)
        bad_hash = dict(self.env); bad_hash[ENV_GAME_HASH] = "0" * 64
        with patch.dict(os.environ, bad_hash, clear=True):
            with self.assertRaisesRegex(RuntimeError, "GAME_SET_HASH_MISMATCH"):
                RequestContext.from_env(required=True)

    def test_child_process_concurrent_appends_and_redaction(self):
        code = (
            "from backend.nhl.official_request_journal import RequestContext;"
            "import os;"
            "RequestContext.from_env(required=True).append({"
            "'event_kind':'FIXTURE','timestamp_utc':'2026-09-20T00:00:00Z',"
            "'pid':os.getpid(),'caller_stage':'CHILD','endpoint_family':'FIXTURE',"
            "'resource_identity':{'game_id':2026010001},'final_disposition':'OK'})"
        )
        env = dict(os.environ); env.update(self.env); env["ODDS_API_KEY"] = "DO_NOT_PERSIST_SECRET"
        processes = [subprocess.Popen([sys.executable, "-c", code], cwd=Path.cwd(), env=env)
                     for _ in range(12)]
        self.assertTrue(all(process.wait() == 0 for process in processes))
        rows = read_journal(Path(self.env[ENV_JOURNAL]))
        self.assertEqual(len(rows), 12)
        text = Path(self.env[ENV_JOURNAL]).read_text()
        self.assertNotIn("DO_NOT_PERSIST_SECRET", text)
        self.assertNotIn("https://", text)
        self.assertEqual(Path(self.env[ENV_JOURNAL]).stat().st_mode & 0o777, 0o600)

    def test_seven_game_expected_plan_reconciles_exactly(self):
        outcomes = []
        # Eight authority responses, twelve unique team rosters, seven shift charts,
        # and seven play-by-play responses are the expected network operations.
        outcomes.extend([FakeResponse(schedule_payload())] + [FakeResponse({"id": gid}) for gid in GAMES])
        outcomes.extend(FakeResponse({"forwards": [{"id": 1}]}) for _ in range(12))
        outcomes.extend(FakeResponse({"data": []}) for _ in range(7))
        outcomes.extend(FakeResponse({"plays": []}) for _ in range(7))
        with patch.dict(os.environ, self.env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session",
                   side_effect=lambda: FakeSession(outcomes)):
            official_get("x", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, authority_boundary=True,
                         preserve_response=True)
            for gid in GAMES:
                official_get("x", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                             identity={"slate_date": SLATE, "game_id": gid},
                             authority_boundary=True, preserve_response=True)
            # Child-equivalent cache operations: schedule ingestion, goalie 7,
            # skater schedule, skater roster-alignment 7, skater stats 7.
            official_get("x", timeout=1, stage="SCHEDULE_INGESTION", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, reuse_preserved=True)
            for stage in ("GOALIE", "SKATER_ALIGNMENT", "SKATER_STATS"):
                for gid in GAMES:
                    official_get("x", timeout=1, stage=stage, endpoint_family="BOXSCORE",
                                 identity={"slate_date": SLATE, "game_id": gid}, reuse_preserved=True)
            official_get("x", timeout=1, stage="SKATER", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, reuse_preserved=True)
            for number in range(12):
                official_get("x", timeout=1, stage="ROSTER", endpoint_family="ROSTER",
                             identity={"slate_date": SLATE, "team": f"T{number:02d}",
                                       "roster_variant": "current"}, preserve_response=True)
            # Two repeated-team game uses consume the already preserved roster
            # bodies without another network attempt.
            for number in range(2):
                official_get("x", timeout=1, stage="ROSTER", endpoint_family="ROSTER",
                             identity={"slate_date": SLATE, "team": f"T{number:02d}",
                                       "roster_variant": "current"}, reuse_preserved=True)
            for gid in GAMES:
                official_get("x", timeout=1, stage="SHIFT", endpoint_family="SHIFT_CHART",
                             identity={"slate_date": SLATE, "game_id": gid})
                official_get("x", timeout=1, stage="PBP", endpoint_family="PLAY_BY_PLAY",
                             identity={"slate_date": SLATE, "game_id": gid})
        summary = summarize_journal(Path(self.env[ENV_JOURNAL]), run_id="fixture_run",
                                    expected_game_hash=self.env[ENV_GAME_HASH])
        self.assertEqual(summary["total_logical_requests"], 59)
        self.assertEqual(summary["total_network_attempts"], 34)
        self.assertEqual(summary["cache_reuse_events"], 25)
        self.assertEqual(summary["counts_by_endpoint_family"]["SHIFT_CHART"]["network_attempts"], 7)
        self.assertEqual(summary["counts_by_endpoint_family"]["PLAY_BY_PLAY"]["network_attempts"], 7)

    def test_governed_call_graph_has_no_direct_http_bypass(self):
        paths = [
            "backend/nhl/scripts/run_nhl_postgame_reconciliation.py",
            "backend/nhl/scripts/import_schedule_today.py",
            "backend/nhl/scripts/refresh_players_and_roster_today.py",
            "backend/nhl/scripts/seed_goalie_logs_for_date.py",
            "backend/nhl/scripts/seed_skater_logs_for_date.py",
            "backend/nhl/scripts/ingest_shiftcharts_for_date.py",
            "backend/nhl/scripts/backfill_game_manpower_segments.py",
            "backend/nhl/scripts/fill_pp_toi_minutes_for_date.py",
        ]
        forbidden = ("requests.get(", ".get(\"https://", "urlopen(")
        for raw in paths:
            text = Path(raw).read_text()
            with self.subTest(path=raw):
                self.assertIn("official_get", text)
                self.assertFalse(any(token in text for token in forbidden))


if __name__ == "__main__":
    unittest.main()
