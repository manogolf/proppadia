import json
import hashlib
import os
import shutil
import tempfile
import unittest
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.official_request_journal import (
    ENV_CACHE, ENV_GAME_HASH, ENV_GAME_IDS, ENV_JOURNAL, ENV_REQUIRED,
    ENV_RUN_ID, ENV_SLATE, canonical_game_set_hash, official_get,
    summarize_journal, verify_failed_execution_ancestor,
    verify_preserved_response_run,
)

from backend.nhl.postgame_reconcile.core import (
    publish_reconciliation,
    reconciliation_lock,
    resolve_operational_sources,
    validate_final_slate,
)
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    _run,
    fetch_official,
    main as reconciliation_main,
    official_games_for_slate,
)


SLATE = "2026-09-19"
START = "2026-09-19T23:00:00Z"
PRED = "2026-09-19T20:45:51Z"


def canonical():
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010001,
        "scheduled_start_time_utc": START, "home_team_id": 19, "away_team_id": 25,
        "home_team_code": "STL", "away_team_code": "DAL", "game_type_code": 1,
        "retained_game_state": "FINAL",
    }])


def official(state="FINAL", home=19):
    return pd.DataFrame([{
        "game_id": 2026010001, "game_state": state, "home_team_id": home,
        "away_team_id": 25, "home_score": 3, "away_score": 2,
    }])


def boxscore(saves=25):
    return {2026010001: {"playerByGameStats": {
        "homeTeam": {
            "forwards": [{"playerId": 101, "goals": 1, "assists": 1, "sog": 3, "toi": "18:00"}],
            "defense": [],
            "goalies": [{"playerId": 201, "saves": saves, "shotsAgainst": 27, "toi": "60:00"}],
        },
        "awayTeam": {
            "forwards": [{"playerId": 102, "goals": 0, "assists": 0, "sog": 1, "toi": "12:00"}],
            "defense": [],
            "goalies": [{"playerId": 202, "saves": 20, "shotsAgainst": 23, "toi": "60:00"}],
        },
    }}}


def schedule_game(game_id, *, state="FINAL", child_date=None, home=19, away=25):
    row = {
        "id": game_id, "gameState": state,
        "homeTeam": {"id": home, "score": 3},
        "awayTeam": {"id": away, "score": 2},
    }
    if child_date is not None:
        row["gameDate"] = child_date
    return row


def week_fixture(target_games=None):
    target_games = [schedule_game(2026010001)] if target_games is None else target_games
    return {"gameWeek": [
        {"date": "2026-09-18", "games": [schedule_game(2026010998)]},
        {"date": SLATE, "games": target_games},
        {"date": "2026-09-20", "games": [schedule_game(2026010999)]},
    ]}


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def prediction_tree(root: Path):
    main = root / "constructed/mainline/season=2026/slate_date=2026-09-19/run_type=MIDDAY/state=fixture"
    props = root / "constructed/independent_props"
    main.mkdir(parents=True)
    props.mkdir(parents=True)
    common = {
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010001,
        "prediction_creation_time_utc": PRED, "scheduled_start_time_utc": START,
        "game_type_code": 1,
    }
    pd.DataFrame([{**common, "model_favored_team": "STL", "v2_home_win_probability": .53}]).to_csv(main / "v2_immutable_predictions.csv", index=False)
    pd.DataFrame([{**common, "home_minus_1_5_cover_probability": .30}]).to_csv(main / "puck_line_v1_immutable_predictions.csv", index=False)
    pd.DataFrame([{"game_id": 2026010001, "player_id": 101, "line": .5, "prob_over": .4, "model": "points"}]).to_csv(props / "points_immutable_predictions.csv", index=False)
    pd.DataFrame([
        {"game_id": 2026010001, "goalie_id": 201, "line": 24.5, "prob_over": .5},
        {"game_id": 2026010001, "goalie_id": 202, "line": 24.5, "prob_over": .4},
    ]).to_csv(props / "saves_conditional_start_predictions.csv", index=False)
    (props / "lane_prediction_summary.json").write_text(json.dumps({"observation_timestamp_utc": PRED}))


class PostgameReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="nhl_postgame_fixture_")
        self.root = Path(self.tmp.name)
        self.pred = self.root / "pred"
        self.out = self.root / "out"
        prediction_tree(self.pred)

    def tearDown(self):
        self.tmp.cleanup()

    def publish(self, **overrides):
        kwargs = dict(canonical=canonical(), official=official(), boxscores=boxscore(),
                      slate_date=SLATE, prediction_root=self.pred, output_root=self.out,
                      observed_at="2026-09-20T04:00:00Z")
        kwargs.update(overrides)
        return publish_reconciliation(**kwargs)

    def test_all_final_valid_points_conditional_saves_and_absent_sog_prediction(self):
        with patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.dict(os.environ, {"ODDS_API_KEY": "fixture-secret"}):
            destination, status = self.publish()
        self.assertEqual(status, "COMPLETE_NEW_APPEND_ONLY")
        points = pd.read_csv(destination / "graded_points.csv")
        saves = pd.read_csv(destination / "graded_saves.csv")
        sog = pd.read_csv(destination / "graded_sog_outcomes_only.csv")
        self.assertEqual(points.official_points.iloc[0], 2)
        self.assertEqual(points.grading_status.iloc[0], "PRESEASON_NON_EVALUATION")
        self.assertEqual(saves.grading_status.value_counts().to_dict(), {
            "PRESEASON_NON_EVALUATION_CONDITIONAL_STARTER": 2,
        })
        self.assertTrue(sog.grading_status.eq("NO_SEPTEMBER_19_PREDICTION_GRADE").all())
        summary = json.loads((destination / "summary.json").read_text())
        self.assertEqual(summary["bookmaker_requests"], 0)
        self.assertEqual(summary["paid_credits"], 0)

    def test_unfinished_game_fails_before_collector(self):
        called = []
        with self.assertRaisesRegex(RuntimeError, "NOT_OFFICIAL_FINAL"):
            self.publish(official=official("LIVE"), collector=lambda: called.append(True))
        self.assertEqual(called, [])

    def test_identity_mismatch_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "HOME_IDENTITY_MISMATCH"):
            validate_final_slate(canonical(), official(home=99), SLATE)

    def test_idempotent_second_pass_inserts_zero_and_does_not_recollect(self):
        called = []
        first, state1 = self.publish(collector=lambda: called.append("first"))
        second, state2 = self.publish(collector=lambda: called.append("second"))
        self.assertEqual(first, second)
        self.assertEqual(state1, "COMPLETE_NEW_APPEND_ONLY")
        self.assertEqual(state2, "IDEMPOTENT_EXISTING_ZERO_INSERTS")
        self.assertEqual(called, ["first"])

    def test_conflicting_retained_outcome_fails_closed(self):
        self.publish()
        changed = official().copy()
        changed["home_score"] = 4
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_RETAINED_OUTCOME"):
            self.publish(official=changed)

    def test_lock_is_acquired_and_released_on_failure(self):
        with self.assertRaisesRegex(RuntimeError, "fixture"):
            with reconciliation_lock(self.out, SLATE):
                raise RuntimeError("fixture")
        with reconciliation_lock(self.out, SLATE):
            self.assertTrue(True)

    def test_parent_day_filters_seven_day_response_with_requested_date_in_middle(self):
        payload = {"gameWeek": [
            {"date": f"2026-09-{day:02d}", "games": [schedule_game(2026010000 + day)]}
            for day in range(16, 23)
        ]}
        result = official_games_for_slate(payload, SLATE)
        self.assertEqual(result.game_id.astype(int).tolist(), [2026010019])

    def test_parent_date_is_authoritative_when_child_date_absent_or_misleading(self):
        payload = week_fixture([
            schedule_game(2026010001),
            schedule_game(2026010002, child_date="2026-09-20"),
        ])
        result = official_games_for_slate(payload, SLATE)
        self.assertEqual(result.game_id.astype(int).tolist(), [2026010001, 2026010002])

    def test_duplicate_game_identity_fails_before_boxscore_requests(self):
        payload = week_fixture([schedule_game(2026010001), schedule_game(2026010001)])
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.requests.get",
                   return_value=Response(payload)) as request:
            with self.assertRaisesRegex(RuntimeError, "OFFICIAL_DUPLICATE_GAME_IDENTITY"):
                fetch_official(SLATE, {2026010001})
        self.assertEqual(request.call_count, 1)

    def test_missing_or_unexpected_game_fails_before_boxscore_requests(self):
        for games, expected in [([], {2026010001}),
                                ([schedule_game(2026010001), schedule_game(2026010002)], {2026010001})]:
            with self.subTest(games=len(games)):
                with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.requests.get",
                           return_value=Response(week_fixture(games))) as request:
                    with self.assertRaisesRegex(RuntimeError, "OFFICIAL_GAME_SET_MISMATCH_BEFORE_BOXSCORE_REQUESTS"):
                        fetch_official(SLATE, expected)
                self.assertEqual(request.call_count, 1)

    def test_exact_request_count_and_only_canonical_boxscores(self):
        payload = week_fixture([schedule_game(2026010001), schedule_game(2026010002)])
        urls = []
        def get(url, timeout):
            urls.append(url)
            return Response(payload if "/schedule/" in url else {"game": url})
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.requests.get", side_effect=get):
            official_frame, boxes, counts = fetch_official(SLATE, {2026010001, 2026010002})
        self.assertEqual(set(official_frame.game_id.astype(int)), {2026010001, 2026010002})
        self.assertEqual(set(boxes), {2026010001, 2026010002})
        self.assertEqual(counts, {
            "schedule_requests_expected": 1, "schedule_requests_actual": 1,
            "game_requests_expected": 2, "game_requests_actual": 2,
            "official_requests_expected": 3, "official_requests_actual": 3,
        })
        self.assertEqual(len(urls), 3)
        self.assertFalse(any("2026010998" in url or "2026010999" in url for url in urls))

    def test_unfinished_requested_game_is_rejected(self):
        frame = official_games_for_slate(week_fixture([schedule_game(2026010001, state="LIVE")]), SLATE)
        with self.assertRaisesRegex(RuntimeError, "ADMITTED_GAMES_NOT_OFFICIAL_FINAL"):
            validate_final_slate(canonical(), frame, SLATE)

    def test_child_collectors_receive_resolved_database_aliases(self):
        with patch.dict(os.environ, {"DATABASE_URL": "${SUPABASE_DB_URL}"}, clear=False), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.subprocess.run") as run:
            _run(["fixture-command"], SLATE, "postgresql://resolved.example/database")
        child = run.call_args.kwargs["env"]
        self.assertEqual(child["SUPABASE_DB_URL"], "postgresql://resolved.example/database")
        self.assertEqual(child["DATABASE_URL"], "postgresql://resolved.example/database")

    def test_completed_package_contains_exact_end_to_end_accounting(self):
        request_root = self.root / "requests"
        game_hash = canonical_game_set_hash([2026010001])
        env = {
            ENV_REQUIRED: "1", ENV_RUN_ID: "package_fixture",
            ENV_JOURNAL: str(request_root / "journal.jsonl"),
            ENV_CACHE: str(request_root / "cache"), ENV_SLATE: SLATE,
            ENV_GAME_HASH: game_hash, ENV_GAME_IDS: "2026010001",
        }
        schedule = week_fixture()
        responses = [Response(schedule), Response({"id": 2026010001})]
        for response in responses:
            response.content = json.dumps(response.payload).encode()
            response.status_code = 200
        class Session:
            headers = {}
            def get(self, url, **kwargs):
                return responses.pop(0)
            def close(self):
                return None
        with patch.dict(os.environ, env, clear=False), \
             patch("backend.nhl.official_request_journal.requests.Session", side_effect=Session):
            official_get("x", timeout=1, stage="AUTH", endpoint_family="SCHEDULE",
                         identity={"slate_date": SLATE}, authority_boundary=True,
                         preserve_response=True)
            official_get("x", timeout=1, stage="AUTH", endpoint_family="BOXSCORE",
                         identity={"slate_date": SLATE, "game_id": 2026010001},
                         authority_boundary=True, preserve_response=True)
            destination, state = self.publish(
                request_journal=Path(env[ENV_JOURNAL]),
                request_accounting_factory=lambda: summarize_journal(
                    Path(env[ENV_JOURNAL]), run_id="package_fixture", expected_game_hash=game_hash,
                ),
                request_lineage={
                    "contract_version": "NHL_POSTGAME_REQUEST_LINEAGE_V2",
                    "canonical_game_set_hash": game_hash,
                    "ancestors": [
                        {"role": "AUTHORITY_RESPONSE_SOURCE",
                         "source_run_id": "source_fixture"},
                        {"role": "FAILED_EXECUTION_ANCESTOR",
                         "run_id": "second_failed_fixture"},
                    ],
                },
            )
        self.assertEqual(state, "COMPLETE_NEW_APPEND_ONLY")
        accounting = json.loads((destination / "official_request_accounting.json").read_text())
        summary = json.loads((destination / "summary.json").read_text())
        self.assertEqual(accounting["total_logical_requests"], 2)
        self.assertEqual(summary["official_request_accounting"], accounting)
        self.assertIn("official_request_journal.jsonl", (destination / "SHA256SUMS").read_text())
        lineage = json.loads((destination / "request_lineage.json").read_text())
        self.assertEqual(lineage["completed_request_run"]["role"], "COMPLETED_EXECUTION")
        self.assertEqual(lineage["completed_request_run"]["run_id"], "package_fixture")
        self.assertEqual([row["role"] for row in lineage["ancestors"]],
                         ["AUTHORITY_RESPONSE_SOURCE", "FAILED_EXECUTION_ANCESTOR"])
        self.assertEqual(
            lineage["completed_request_run"]["journal_sha256"],
            hashlib.sha256(Path(env[ENV_JOURNAL]).read_bytes()).hexdigest())
        self.assertIn("request_lineage.json", (destination / "SHA256SUMS").read_text())

    def test_september_20_local_only_source_binding_is_exact(self):
        operational = Path(__file__).resolve().parents[2] / "artifacts/operational/nhl"
        with patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect") as database:
            binding = resolve_operational_sources(
                slate_date="2026-09-20", operational_root=operational)
        database.assert_not_called()
        self.assertEqual(binding["canonical_games"], 7)
        self.assertEqual(binding["cross_market"]["moneyline_rows"], 7)
        self.assertEqual(binding["cross_market"]["puck_line_rows"], 7)
        self.assertEqual(binding["sog"]["prediction_rows"], 5517)
        self.assertEqual(binding["sog"]["players"], 408)
        self.assertEqual(binding["sog"]["exclusions"], 45)
        self.assertEqual(binding["points"], {
            "status": "NO_PROSPECTIVE_POINTS_PREDICTIONS",
            "reason": "PROSPECTIVE_NOT_BEFORE_2026-09-21",
        })
        self.assertEqual(binding["saves"], {
            "status": "NO_PROSPECTIVE_SAVES_PREDICTIONS",
            "reason": "PROSPECTIVE_NOT_BEFORE_2026-09-21",
        })

    def test_corrupt_local_manifest_fails_before_database_or_official_request(self):
        operational = self.root / "operational"
        run = (operational / "cross_market_shadow/season=2026/slate_date=2026-09-20/"
               "run_type=FINAL_PREGAME/state=fixture")
        run.mkdir(parents=True)
        (run / "SHA256SUMS").write_text("0" * 64 + "  missing.csv\n")
        with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect") as database, \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.official_get") as network:
            with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_SOURCE_HASH_MISMATCH"):
                resolve_operational_sources(slate_date="2026-09-20", operational_root=operational)
        database.assert_not_called()
        network.assert_not_called()

    def test_september_20_execute_requires_explicit_source_run_before_database(self):
        with patch.object(sys, "argv", ["run_nhl_postgame_reconciliation.py",
                                        "2026-09-20", "--execute"]), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect") as database, \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.official_get") as network, \
             patch("builtins.print"):
            code = reconciliation_main()
        self.assertEqual(code, 5)
        database.assert_not_called()
        network.assert_not_called()

    def test_september_20_two_ancestor_preflight_is_fully_local(self):
        original = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
        second = "nhlpostgame_20260920_20260922T161724179739Z_cef0bc8b"
        output = []
        with patch.object(sys, "argv", ["run_nhl_postgame_reconciliation.py",
                                        "2026-09-20", "--local-input-preflight",
                                        "--reuse-request-run-id", original,
                                        "--lineage-request-run-id", second]), \
             patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.psycopg.connect") as database, \
             patch("builtins.print", side_effect=lambda value: output.append(value)):
            code = reconciliation_main()
        self.assertEqual(code, 0)
        database.assert_not_called()
        payload = json.loads(output[-1])
        self.assertEqual(payload["status"], "LOCAL_INPUTS_VALID")
        self.assertEqual(payload["source_binding"]["canonical_games"], 7)
        roles = [row["role"] for row in payload["request_lineage"]["ancestors"]]
        self.assertEqual(roles, ["AUTHORITY_RESPONSE_SOURCE", "FAILED_EXECUTION_ANCESTOR"])
        self.assertEqual(payload["request_lineage"]["ancestors"][1]["reuse_records"], 9)
        self.assertEqual(payload["database_requests"], 0)
        self.assertEqual(payload["external_requests"], 0)

    def test_failed_ancestor_alteration_cycle_and_game_set_mismatch_are_rejected(self):
        repository = Path(__file__).resolve().parents[2]
        runs = repository / "artifacts/operational/nhl/postgame_reconciliation/request_runs/2026-09-20"
        original = "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6"
        second = "nhlpostgame_20260920_20260922T161724179739Z_cef0bc8b"
        game_ids = list(range(2026010008, 2026010015))
        source = verify_preserved_response_run(
            runs / original, expected_run_id=original, slate_date="2026-09-20",
            game_ids=game_ids, repository_root=repository,
            expected_journal_sha256="2994ef2bd162c483eacc20b02cf83cd0938fb5f79fd76d208ce57981ed473cb7",
            expected_tree_fingerprint="c6e6402b2eba12a8efb30a60df607324cf8684361d357fb5340cfc9051fa7110")
        common = dict(
            expected_run_id=second, slate_date="2026-09-20", game_ids=game_ids,
            response_source=source, repository_root=repository,
            expected_journal_sha256="30625a8088ea6837e2293b9339a8b38787a044ba7d0b7033ab54965ec695459c",
            expected_tree_fingerprint="fe6a4cb817f3e291447b187af68749eee6a733a854886cb6420a90303a019b84")
        with self.assertRaisesRegex(RuntimeError, "GAME_SET_HASH_MISMATCH"):
            verify_failed_execution_ancestor(runs / second, **{**common, "game_ids": [999]})
        with self.assertRaisesRegex(RuntimeError, "REQUEST_LINEAGE_CYCLE"):
            verify_failed_execution_ancestor(
                runs / second, **{**common, "response_source": {**source, "source_run_id": second}})
        altered = self.root / second
        shutil.copytree(runs / second, altered)
        with (altered / "official_request_journal.jsonl").open("a") as handle:
            handle.write("\n")
        with self.assertRaisesRegex(RuntimeError, "JOURNAL_CHANGED"):
            verify_failed_execution_ancestor(altered, **common)

    def test_september_19_completed_package_identity_is_unchanged(self):
        repository = Path(__file__).resolve().parents[2]
        package = (repository / "artifacts/operational/nhl/postgame_reconciliation/2026-09-19/"
                   "reconciliation=75d3f2ec2d50f53ee792")
        complete = package / "RUN_COMPLETE.json"
        self.assertEqual(json.loads(complete.read_text())["substantive_identity"],
                         "75d3f2ec2d50f53ee79232954f6e5d0de1a97c4dc23d21994bfea0c247d0ce86")
        self.assertEqual(hashlib.sha256(complete.read_bytes()).hexdigest(),
                         "126a7d99365c8a7c070c07bae3da200ff986ccac44a8a825a6a0040a47d6dc08")

    def test_schedule_and_roster_database_writes_are_idempotent_upserts(self):
        schedule = Path("backend/nhl/scripts/import_schedule_today.py").read_text()
        roster = Path("backend/nhl/scripts/refresh_players_and_roster_today.py").read_text()
        self.assertIn("ON CONFLICT (team) DO UPDATE", schedule)
        self.assertIn("ON CONFLICT (game_id) DO UPDATE", schedule)
        self.assertIn("ON CONFLICT (game_id, team_id, player_id)", roster)


if __name__ == "__main__":
    unittest.main()
