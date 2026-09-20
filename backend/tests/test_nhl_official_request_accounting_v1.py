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
    ENV_GAME_HASH,
    ENV_GAME_IDS,
    ENV_JOURNAL,
    ENV_REQUIRED,
    ENV_RUN_ID,
    ENV_SLATE,
    RequestContext,
    canonical_game_set_hash,
    official_get,
    read_journal,
    summarize_journal,
)


SLATE = "2026-09-19"
GAMES = list(range(2026010001, 2026010008))


class FakeResponse:
    def __init__(self, payload, status=200):
        self.content = json.dumps(payload).encode()
        self.status_code = status


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
