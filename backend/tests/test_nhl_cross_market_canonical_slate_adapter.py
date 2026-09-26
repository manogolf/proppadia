import unittest
import tempfile
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.cross_market_shadow.canonical_slate_adapter import (
    SCHEDULE_COLUMNS,
    adapt_canonical_slate,
)
from backend.nhl.daily_capture import CanonicalGame
from backend.nhl.cross_market_shadow.official_outcomes import load_official_outcomes
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


def fixture(game_type=1, state="FUT", home_id=15, away_id=6):
    canonical = CanonicalGame(
        game_id=2026010049, start_time_utc="2026-09-25T23:00:00Z",
        home_team="WSH", away_team="BOS", home_team_id=home_id, away_team_id=away_id,
    )
    raw = {"gameWeek": [{"date": "2026-09-25", "games": [{
        "id": 2026010049, "startTimeUTC": "2026-09-25T23:00:00Z",
        "gameType": game_type, "gameState": state,
        "homeTeam": {"id": 15, "abbrev": "WSH"},
        "awayTeam": {"id": 6, "abbrev": "BOS"},
    }]}]}
    return canonical, raw


class CanonicalSlateAdapterTest(unittest.TestCase):
    def convert(self, game, raw):
        return adapt_canonical_slate([game], raw, slate_date="2026-09-25", canonical_season=2026)

    def test_scheduled_preseason_and_orientation(self):
        game, raw = fixture()
        result = self.convert(game, raw)
        self.assertEqual(list(result.columns), SCHEDULE_COLUMNS)
        row = result.iloc[0]
        self.assertEqual(row.game_status, "SCHEDULED")
        self.assertEqual(int(row.game_type_code), 1)
        self.assertEqual((row.home_team, row.away_team), ("WSH", "BOS"))
        self.assertEqual((int(row.home_team_id), int(row.away_team_id)), (15, 6))

    def test_non_future_state_is_not_collapsed(self):
        game, raw = fixture(state="LIVE")
        self.assertEqual(self.convert(game, raw).iloc[0].game_status, "LIVE")

    def test_empty_slate(self):
        result = adapt_canonical_slate([], {"gameWeek": []}, slate_date="2026-09-25", canonical_season=2026)
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), SCHEDULE_COLUMNS)

    def test_duplicate_canonical_game_rejected(self):
        game, raw = fixture()
        with self.assertRaisesRegex(ValueError, "DUPLICATE_CANONICAL_GAME_ID"):
            adapt_canonical_slate([game, game], raw, slate_date="2026-09-25", canonical_season=2026)

    def test_missing_identity_fields_rejected(self):
        game, raw = fixture(home_id=None)
        with self.assertRaisesRegex(ValueError, "CANONICAL_IDENTITY_OR_GAME_TYPE_MISSING"):
            self.convert(game, raw)

    def test_raw_team_orientation_mismatch_rejected(self):
        game, raw = fixture()
        raw["gameWeek"][0]["games"][0]["homeTeam"]["abbrev"] = "BOS"
        with self.assertRaisesRegex(ValueError, "CANONICAL_RAW_IDENTITY_MISMATCH"):
            self.convert(game, raw)

    def test_missing_raw_identity_rejected(self):
        game, raw = fixture()
        raw["gameWeek"][0]["games"][0]["awayTeam"].pop("id")
        with self.assertRaisesRegex(ValueError, "CANONICAL_RAW_IDENTITY_MISMATCH"):
            self.convert(game, raw)

    def test_exporter_uses_supplied_canonical_schedule_without_schedule_query(self):
        game, raw = fixture()
        schedule = self.convert(game, raw)
        with tempfile.TemporaryDirectory(prefix="nhl_canonical_schedule_adapter_") as tmp:
            with patch.object(capture.psycopg, "connect") as connect, \
                    patch.object(capture.pd, "read_sql_query", return_value=pd.DataFrame()) as read_sql:
                connect.return_value.__enter__.return_value = object()
                schedule_path, history_path, returned = capture.export_inputs(
                    "dsn-unused-by-mock", "2026-09-25", Path(tmp) / "inputs",
                    canonical_schedule=schedule,
                )
            self.assertEqual(read_sql.call_count, 1)
            self.assertIn("nhl.skater_game_logs_raw", read_sql.call_args.args[0])
            self.assertIn("g.game_type=2", read_sql.call_args.args[0])
            self.assertIn("g.start_time_utc < %s::timestamptz", read_sql.call_args.args[0])
            self.assertNotIn("l.goals", read_sql.call_args.args[0])
            self.assertNotIn("lower(g.status)='final'", read_sql.call_args.args[0])
            self.assertEqual(
                read_sql.call_args.kwargs["params"][0],
                pd.Timestamp("2026-09-25T23:00:00Z"),
            )
            self.assertNotIn("FROM nhl.games WHERE season=2026 AND game_date", read_sql.call_args.args[0])
            self.assertEqual(returned.to_dict("records"), schedule.to_dict("records"))
            self.assertTrue(schedule_path.exists())
            self.assertTrue(history_path.exists())


class CrossMarketScoreSourceContractTest(unittest.TestCase):
    def setUp(self):
        self.history = pd.DataFrame([{
            "canonical_season": 2026,
            "game_id": 2026020001,
            "home_team_id": 6,
            "away_team_id": 8,
            "scheduled_start_time_utc": "2026-09-24T02:00:00Z",
            "game_status": "OFF",
            "final_home_goals": 0,
            "final_away_goals": 2,
            "score_home_team_id": 6,
            "score_away_team_id": 8,
            "score_source": "OFFICIAL_NHL_FINAL_SCORE",
            "score_status": "QUALIFIED",
            "score_identity_qualified": True,
            "score_observed_at_utc": "2026-09-24T04:30:00Z",
        }])
        self.schedule = pd.DataFrame([{
            "scheduled_start_time_utc": "2026-09-25T23:00:00Z",
        }])

    def test_official_score_source_accepts_a_real_zero_goal_team(self):
        capture.validate_history_score_source(self.history, self.schedule)

    def test_skater_goal_aggregate_is_not_training_compatible_source(self):
        self.history.loc[0, "score_source"] = "SKATER_GAME_LOG_GOALS"
        with self.assertRaisesRegex(
            ValueError, "CROSS_MARKET_TRAINING_COMPATIBLE_FINAL_SCORE_SOURCE_REQUIRED"
        ):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_missing_official_score_fails_closed(self):
        self.history.loc[0, "final_home_goals"] = pd.NA
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_FINAL_SCORE_MISSING_OR_INVALID"):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_score_observed_at_or_after_target_fails_closed(self):
        self.history.loc[0, "score_observed_at_utc"] = "2026-09-25T23:00:00Z"
        with self.assertRaisesRegex(
            ValueError, "CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET"
        ):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_unfinished_game_state_fails_closed(self):
        self.history.loc[0, "game_status"] = "LIVE"
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_GAME_NOT_OFFICIAL_FINAL"):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_official_outcome_observed_before_target_is_accepted(self):
        capture.validate_history_score_source(self.history, self.schedule)

    def test_identity_or_home_away_orientation_mismatch_fails_closed(self):
        self.history.loc[0, "score_home_team_id"] = 8
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_FINAL_SCORE_HOME_IDENTITY_MISMATCH"):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_game_start_must_be_strictly_before_target(self):
        self.history.loc[0, "scheduled_start_time_utc"] = "2026-09-25T23:00:00Z"
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_FINAL_SCORE_NOT_STRICT_PRIOR"):
            capture.validate_history_score_source(self.history, self.schedule)

    def test_duplicate_outcome_evidence_fails_closed(self):
        duplicate = pd.concat([self.history, self.history], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_DUPLICATE_FINAL_SCORE_IDENTITY"):
            capture.validate_history_score_source(duplicate, self.schedule)

    def test_null_skater_goals_do_not_affect_official_team_score(self):
        canonical, raw = fixture()
        schedule = capture.adapt_canonical_slate(
            [canonical], raw, slate_date="2026-09-25", canonical_season=2026
        )
        history = pd.DataFrame([{
            "canonical_season": 2026,
            "game_id": 2026020001,
            "scheduled_start_time_utc": "2026-09-24T02:00:00Z",
            "home_team_id": 6,
            "away_team_id": 8,
            "home_team": "BOS",
            "away_team": "MTL",
            "game_status": "scheduled",
            "game_type_code": 2,
            "final_home_shots": 25,
            "final_away_shots": 28,
            "final_home_skater_rows": 18,
            "final_home_distinct_skater_players": 18,
            "final_away_skater_rows": 18,
            "final_away_distinct_skater_players": 18,
            "skater_goals": pd.NA,
        }])
        official = pd.DataFrame([{
            "canonical_season": 2026, "game_id": 2026020001, "game_type_code": 2,
            "scheduled_start_time_utc": "2026-09-24T02:00:00Z",
            "home_team_id": 6, "away_team_id": 8,
            "final_home_goals": 0, "final_away_goals": 2,
            "game_status": "OFF", "score_source": "OFFICIAL_NHL_FINAL_SCORE",
            "score_status": "QUALIFIED", "score_identity_qualified": True,
            "score_observed_at_utc": "2026-09-24T04:30:00Z",
            "outcome_artifact": "preserved/canonical_game_outcomes.csv",
            "outcome_artifact_sha256": "a" * 64,
        }])
        with tempfile.TemporaryDirectory(prefix="nhl_score_source_gate_") as tmp:
            input_dir = Path(tmp) / "inputs"
            with patch.object(capture.psycopg, "connect") as connect, \
                    patch.object(capture.pd, "read_sql_query", return_value=history), \
                    patch.object(capture, "load_official_outcomes", return_value=official):
                connect.return_value.__enter__.return_value = object()
                _, history_path, _ = capture.export_inputs(
                    "dsn-unused-by-mock", "2026-09-25", input_dir,
                    canonical_schedule=schedule,
                )
            exported = pd.read_csv(history_path)
            self.assertEqual(int(exported.iloc[0].final_home_goals), 0)
            self.assertEqual(int(exported.iloc[0].final_away_goals), 2)
            self.assertEqual(exported.iloc[0].score_source, "OFFICIAL_NHL_FINAL_SCORE")
            self.assertTrue(pd.isna(exported.iloc[0].skater_goals))
            self.assertIn("outcome_artifact_sha256", exported)

    def test_missing_official_outcome_evidence_fails_closed(self):
        canonical, raw = fixture()
        schedule = capture.adapt_canonical_slate(
            [canonical], raw, slate_date="2026-09-25", canonical_season=2026
        )
        history = pd.DataFrame([{
            "canonical_season": 2026, "game_id": 2026020001,
            "scheduled_start_time_utc": "2026-09-24T02:00:00Z",
            "home_team_id": 6, "away_team_id": 8, "home_team": "BOS", "away_team": "MTL",
            "game_status": "scheduled", "game_type_code": 2,
            "final_home_shots": 25, "final_away_shots": 28,
            "final_home_skater_rows": 18, "final_home_distinct_skater_players": 18,
            "final_away_skater_rows": 18, "final_away_distinct_skater_players": 18,
        }])
        with tempfile.TemporaryDirectory(prefix="nhl_missing_official_outcome_") as tmp:
            with patch.object(capture.psycopg, "connect") as connect, \
                    patch.object(capture.pd, "read_sql_query", return_value=history), \
                    patch.object(capture, "load_official_outcomes", return_value=pd.DataFrame()):
                connect.return_value.__enter__.return_value = object()
                with self.assertRaisesRegex(ValueError, "CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING"):
                    capture.export_inputs(
                        "dsn-unused-by-mock", "2026-09-25", Path(tmp) / "inputs",
                        canonical_schedule=schedule,
                    )

    def test_conflicting_score_team_identity_fails_closed(self):
        self.history.loc[0, "score_identity_qualified"] = False
        with self.assertRaisesRegex(ValueError, "CROSS_MARKET_FINAL_SCORE_IDENTITY_CONFLICT"):
            capture.validate_history_score_source(self.history, self.schedule)


class OfficialOutcomeArtifactTest(unittest.TestCase):
    game_id = 2026020001
    slate_date = "2026-09-24"

    def create_artifact(self, root: Path, *, state="OFF", home_id=6, away_id=8,
                        home_score=0, away_score=2, duplicate=False):
        package = root / self.slate_date / "reconciliation=fixture"
        package.mkdir(parents=True)
        run_id = "fixture_run"
        cache = root / "request_runs" / self.slate_date / run_id / "preserved_responses"
        (cache / "objects").mkdir(parents=True)
        (cache / "index").mkdir(parents=True)

        schedule_identity = {"slate_date": self.slate_date}
        schedule_payload = {"gameWeek": [{"date": self.slate_date, "games": [{
            "id": self.game_id, "gameDate": self.slate_date, "gameState": state,
            "homeTeam": {"id": home_id, "score": home_score},
            "awayTeam": {"id": away_id, "score": away_score},
        }]}]}
        requests = [("SCHEDULE", schedule_identity, schedule_payload, None)]
        requests.append(("BOXSCORE", {"slate_date": self.slate_date, "game_id": self.game_id},
                         {"id": self.game_id}, self.game_id))
        journal = []
        for family, identity, payload, gid in requests:
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            digest = hashlib.sha256(body).hexdigest()
            token = hashlib.sha256(json.dumps(
                {"endpoint_family": family, "identity": identity},
                sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest()
            (cache / "objects" / f"{digest}.json").write_bytes(body)
            (cache / "index" / f"{token}.json").write_text(json.dumps({
                "endpoint_family": family, "identity": identity,
                "response_sha256": digest, "object_name": f"{digest}.json",
            }))
            journal.append({
                "run_id": run_id, "endpoint_family": family,
                "resource_identity": identity, "final_disposition": "SUCCESS",
                "event_kind": "NETWORK_ATTEMPT", "authority_boundary": True,
                "http_status": 200,
                "response_preserved": True, "response_sha256": digest,
                "request_end_utc": "2026-09-24T03:15:00Z",
            })
        if duplicate:
            journal.append(dict(journal[-1]))
        (package / "official_request_journal.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in journal)
        )
        slate_row = {
            "canonical_season": 2026, "slate_date": self.slate_date,
            "game_id": self.game_id, "game_type_code": 2,
            "home_team_id": 6, "away_team_id": 8,
            "scheduled_start_time_utc": "2026-09-24T01:00:00Z",
        }
        outcomes_row = {
            "canonical_season": 2026, "slate_date": self.slate_date,
            "game_id": self.game_id, "game_type_code": 2,
            "home_team_id": 6, "away_team_id": 8,
            "official_final_home_goals": 0,
            "official_final_away_goals": 2, "official_final": True,
            "outcome_source": "NHL_OFFICIAL_GAMECENTER",
            "outcome_source_timestamp_utc": "2026-09-24T03:30:00Z",
            "outcome_conflict_status": "NO_CONFLICT",
        }
        if duplicate:
            outcome_rows = [outcomes_row, outcomes_row]
        else:
            outcome_rows = [outcomes_row]
        pd.DataFrame([slate_row]).to_csv(package / "canonical_admitted_slate.csv", index=False)
        pd.DataFrame(outcome_rows).to_csv(package / "canonical_game_outcomes.csv", index=False)
        (package / "summary.json").write_text(json.dumps({
            "status": "COMPLETE", "slate_date": self.slate_date,
            "substantive_identity": "fixture_identity",
        }))
        files = ["canonical_admitted_slate.csv", "canonical_game_outcomes.csv",
                 "official_request_journal.jsonl", "summary.json"]
        (package / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256((package / name).read_bytes()).hexdigest()}  {name}\n"
            for name in files
        ))
        (package / "RUN_COMPLETE.json").write_text(json.dumps({
            "status": "COMPLETE", "substantive_identity": "fixture_identity",
            "completed_at_utc": "2026-09-24T03:30:00Z",
        }))

    def test_loads_final_zero_goal_outcome_from_preserved_official_evidence(self):
        with tempfile.TemporaryDirectory(prefix="nhl_official_outcome_artifact_") as tmp:
            root = Path(tmp)
            self.create_artifact(root)
            result = load_official_outcomes(root)
            self.assertEqual(len(result), 1)
            self.assertEqual(int(result.iloc[0].final_home_goals), 0)
            self.assertEqual(int(result.iloc[0].final_away_goals), 2)
            self.assertEqual(result.iloc[0].score_status, "QUALIFIED")
            self.assertEqual(result.iloc[0].score_source, "OFFICIAL_NHL_FINAL_SCORE")
            self.assertEqual(result.iloc[0].official_score_endpoint, "/v1/schedule/2026-09-24")
            self.assertEqual(len(result.iloc[0].official_schedule_response_sha256), 64)
            self.assertTrue(Path(result.iloc[0].official_schedule_response_path).is_file())

    def test_nonfinal_official_response_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="nhl_nonfinal_outcome_artifact_") as tmp:
            root = Path(tmp)
            self.create_artifact(root, state="LIVE")
            with self.assertRaisesRegex(ValueError, "CROSS_MARKET_OFFICIAL_GAME_NOT_FINAL"):
                load_official_outcomes(root)

    def test_official_home_away_orientation_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="nhl_orientation_outcome_artifact_") as tmp:
            root = Path(tmp)
            self.create_artifact(root, home_id=8, away_id=6)
            with self.assertRaisesRegex(ValueError, "CROSS_MARKET_OFFICIAL_TEAM_ORIENTATION_MISMATCH"):
                load_official_outcomes(root)

    def test_conflicting_official_response_and_outcome_scores_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="nhl_conflicting_outcome_artifact_") as tmp:
            root = Path(tmp)
            self.create_artifact(root, home_score=1)
            with self.assertRaisesRegex(ValueError, "CROSS_MARKET_OFFICIAL_SCORE_MISMATCH_OR_INVALID"):
                load_official_outcomes(root)

    def test_duplicate_outcome_evidence_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="nhl_duplicate_outcome_artifact_") as tmp:
            root = Path(tmp)
            self.create_artifact(root, duplicate=True)
            with self.assertRaisesRegex(ValueError, "CROSS_MARKET_DUPLICATE_OFFICIAL_OUTCOME_IDENTITY"):
                load_official_outcomes(root)


if __name__ == "__main__":
    unittest.main()
