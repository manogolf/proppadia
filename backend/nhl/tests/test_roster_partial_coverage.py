"""Per-game roster coverage (NHL backlog P2.13, roster part)."""
from __future__ import annotations

import csv
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from backend.nhl.daily_capture import (
    ROSTER_CONTRACT,
    ROSTER_PARTIAL_CONTRACT,
    CanonicalGame,
    _manifest,
    canonical_game_set_hash,
    write_roster_observation,
)
from backend.nhl.daily_orchestration import (
    verify_partial_roster_observation,
    verify_roster_observation_reuse,
    verify_slate_roster_coverage,
)
from backend.nhl.reconciliation_certificate import (
    PARTIAL_ROSTER_NOT_SUPPORTED,
    PARTIAL_ROSTER_STATUS,
    RosterObservationNotReady,
    require_full_roster_coverage,
    require_slate_roster_observation,
    roster_covered_game_ids,
)

# The roster child resolves its database URL at import; tests never connect.
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://offline.invalid/test")

UTC = timezone.utc
SLATE = "2026-10-24"
NOW = datetime(2026, 10, 24, 14, tzinfo=UTC)
GAMES = [
    CanonicalGame(2026020168, "2026-10-24T17:00:00Z", "TOR", "OTT"),
    CanonicalGame(2026020169, "2026-10-24T23:00:00Z", "BOS", "MTL"),
    CanonicalGame(2026020170, "2026-10-25T02:00:00Z", "VAN", "SEA"),
]
GAME_IDS = [game.game_id for game in GAMES]
GAME_HASH = canonical_game_set_hash(GAME_IDS)


def payload(player_id: int, last: str = "Player") -> dict:
    return {"forwards": [{"id": player_id, "firstName": {"default": "A"},
                          "lastName": {"default": last}}]}


def response(team: str, body: dict) -> dict:
    return {"team": team, "source_url": f"https://api-web.nhle.com/v1/roster/{team}/current",
            "observed_at_utc": "2026-10-24T14:00:00Z", "payload": body}


def all_responses(skip: tuple[str, ...] = ()) -> list[dict]:
    teams = ["TOR", "OTT", "BOS", "MTL", "VAN", "SEA"]
    return [response(team, payload(100 + index)) for index, team in enumerate(teams)
            if team not in skip]


def write(root: Path, responses: list[dict], **kwargs) -> Path:
    return write_roster_observation(
        root=root, season=2026, slate_date=SLATE, phase="EARLY",
        parent_daily_run_id="daily", canonical_games=GAMES,
        source_responses=responses, now=NOW, **kwargs)


def verify(package: Path, **kwargs) -> dict:
    return verify_slate_roster_coverage(
        package, season=2026, slate_date=SLATE, phase="EARLY",
        canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH, **kwargs)


def coverage_rows(package: Path) -> dict[int, dict]:
    with (package / "roster_game_coverage.csv").open(newline="") as handle:
        return {int(row["game_id"]): row for row in csv.DictReader(handle)}


def rewrite(package: Path, name: str, mutate) -> None:
    """Tamper with a file and re-seal the manifest, as a forger would."""
    path = package / name
    if name.endswith(".json"):
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))
    else:
        path.write_text(mutate(path.read_text()))
    _manifest(package)


class RosterWriterCoverageTests(unittest.TestCase):
    def test_complete_slate_still_writes_the_unchanged_full_package(self):
        with tempfile.TemporaryDirectory() as temp:
            package = write(Path(temp), all_responses())
            summary = json.loads((package / "observation_summary.json").read_text())
            marker = json.loads((package / "RUN_COMPLETE.json").read_text())
            names = sorted(path.name for path in package.iterdir())
            verified = verify_roster_observation_reuse(
                package, season=2026, slate_date=SLATE, phase="EARLY",
                canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)
        self.assertEqual(summary["schema_version"], ROSTER_CONTRACT)
        self.assertEqual(marker["status"], "COMPLETE")
        self.assertEqual(names, ["RUN_COMPLETE.json", "SHA256SUMS", "observation_summary.json",
                                 "raw_roster_responses.jsonl", "roster_conflicts.json",
                                 "roster_snapshot.jsonl"])
        self.assertNotIn("covered_game_ids", summary)
        self.assertEqual(verified["snapshot_row_count"], 6)

    def test_each_team_problem_excludes_only_that_teams_game(self):
        cases = {
            "TEAM_RESPONSE_MISSING": dict(responses=all_responses(skip=("MTL",))),
            "TEAM_RESPONSE_EMPTY": dict(responses=all_responses(skip=("MTL",))
                                        + [response("MTL", {"forwards": []})]),
            "TEAM_RESPONSE_FAILED": dict(responses=all_responses(skip=("MTL",)),
                                         team_failures={"MTL": "HTTPError"}),
            "TEAM_DIVERGENT_SNAPSHOTS": dict(responses=all_responses()
                                             + [response("MTL", payload(999))]),
            "TEAM_NATURAL_KEY_CONFLICT": dict(responses=all_responses()
                                              + [response("MTL", payload(103, "Changed"))]),
        }
        for reason, kwargs in cases.items():
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as temp:
                package = write(Path(temp), kwargs["responses"],
                                team_failures=kwargs.get("team_failures"))
                summary = json.loads((package / "observation_summary.json").read_text())
                rows = coverage_rows(package)
                snapshot = [json.loads(line) for line in
                            (package / "roster_snapshot.jsonl").read_text().splitlines()]
                verified = verify(package)
                self.assertEqual(summary["schema_version"], ROSTER_PARTIAL_CONTRACT)
                self.assertEqual(summary["covered_game_ids"], [2026020168, 2026020170])
                self.assertEqual(summary["excluded_game_ids"], [2026020169])
                self.assertEqual(summary["excluded_teams"], {"MTL": reason})
                self.assertEqual(rows[2026020169]["coverage"], "EXCLUDED")
                self.assertEqual(rows[2026020169]["away_team_reason"], reason)
                self.assertEqual(rows[2026020169]["home_team_reason"], "")
                self.assertEqual({row["game_id"] for row in snapshot}, {2026020168, 2026020170})
                self.assertEqual(verified["roster_coverage"], "PARTIAL")
                self.assertEqual(verified["roster_game_exclusions"], [{
                    "game_id": 2026020169,
                    "reason": f"ROSTER_COVERAGE_EXCLUDED:MTL={reason}"}])
                # The unchanged full-slate validator never accepts it.
                with self.assertRaisesRegex(RuntimeError, "ROSTER_REUSE_VALIDATION_FAILED"):
                    verify_roster_observation_reuse(
                        package, season=2026, slate_date=SLATE, phase="EARLY",
                        canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH,
                        require_strictly_prestart=False)

    def test_no_covered_game_writes_no_package(self):
        bad = [response("TOR", payload(1)), response("BOS", payload(2)),
               response("VAN", payload(3))]
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "NO_COVERED_GAMES"):
                write(Path(temp), bad, team_failures={"OTT": "TransportExhausted"})
            day = Path(temp) / "season=2026" / f"slate_date={SLATE}"
            self.assertEqual(list(day.glob("observation=*")), [])

    def test_unexpected_team_still_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "INCOMPLETE_OR_CONFLICTING"):
                write(Path(temp), all_responses(skip=("MTL",)) + [response("NYR", payload(9))])

    def test_failure_for_an_unscheduled_team_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "ROSTER_TEAM_FAILURE_UNEXPECTED_TEAM"):
                write(Path(temp), all_responses(skip=("MTL",)), team_failures={"NYR": "x"})

    def test_malformed_response_still_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "ROSTER_SECTION_MALFORMED"):
                write(Path(temp), all_responses(skip=("MTL",))
                      + [response("MTL", {"forwards": "bad"})])


class PartialValidatorTamperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.package = write(Path(self.temp.name), all_responses(skip=("MTL",)))

    def tearDown(self):
        self.temp.cleanup()

    def assert_rejected(self, check: str):
        with self.assertRaisesRegex(RuntimeError, check):
            verify(self.package, require_strictly_prestart=False)

    def test_tampered_file_without_reseal_fails_the_manifest(self):
        (self.package / "roster_game_coverage.csv").write_text("tampered\n")
        with self.assertRaisesRegex(RuntimeError, "OBSERVATION_MANIFEST_MISMATCH"):
            verify(self.package)

    def test_relabeling_an_excluded_game_as_covered_is_rejected(self):
        rewrite(self.package, "roster_game_coverage.csv",
                lambda text: text.replace("EXCLUDED,,TEAM_RESPONSE_MISSING", "COVERED,,"))
        self.assert_rejected("covered_matches_file")

    def test_covered_hash_mismatch_is_rejected(self):
        rewrite(self.package, "observation_summary.json",
                lambda value: value.update(covered_game_set_hash="0" * 64))
        self.assert_rejected("covered_game_set_hash")

    def test_snapshot_rows_on_an_excluded_game_are_rejected(self):
        extra = json.dumps({"game_id": 2026020169, "team": "BOS", "player_id": 102})
        rewrite(self.package, "roster_snapshot.jsonl", lambda text: text + extra + "\n")
        self.assert_rejected("snapshot_only_covered_games")

    def test_wrong_date_and_game_set_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "slate_date"):
            verify_partial_roster_observation(
                self.package, season=2026, slate_date="2026-10-25", phase="EARLY",
                canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)
        with self.assertRaisesRegex(RuntimeError, "canonical_game_ids"):
            verify_partial_roster_observation(
                self.package, season=2026, slate_date=SLATE, phase="EARLY",
                canonical_game_ids=GAME_IDS[:2], canonical_game_set_hash=GAME_HASH)

    def test_marker_relabeled_complete_is_rejected_by_both_validators(self):
        rewrite(self.package, "RUN_COMPLETE.json", lambda value: value.update(status="COMPLETE"))
        self.assert_rejected("partial_marker")
        with self.assertRaisesRegex(RuntimeError, "complete_per_game_coverage"):
            verify_roster_observation_reuse(
                self.package, season=2026, slate_date=SLATE, phase="EARLY",
                canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)

    def test_partial_validator_never_accepts_a_full_package(self):
        with tempfile.TemporaryDirectory() as temp:
            full = write(Path(temp), all_responses())
            with self.assertRaisesRegex(RuntimeError, "ROSTER_PARTIAL_PACKAGE_INCOMPLETE"):
                verify_partial_roster_observation(
                    full, season=2026, slate_date=SLATE, phase="EARLY",
                    canonical_game_ids=GAME_IDS, canonical_game_set_hash=GAME_HASH)

    def test_strict_prestart_still_applies_to_whole_slate_callers(self):
        late = write(Path(self.temp.name) / "late", all_responses(skip=("MTL",)))
        rewrite(late, "observation_summary.json", lambda value: value.update(strictly_prestart=False))
        with self.assertRaisesRegex(RuntimeError, "strictly_prestart"):
            verify(late)
        self.assertEqual(verify(late, require_strictly_prestart=False)["roster_coverage"], "PARTIAL")


class ObserverGateTests(unittest.TestCase):
    def gate(self, root: Path) -> dict:
        with patch("backend.nhl.daily_capture.load_canonical_slate", return_value=GAMES):
            return require_slate_roster_observation(
                SLATE, slate_root=root / "slates", roster_root=root / "roster")

    def test_partial_package_reports_its_own_state_and_covered_games(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write(root / "roster", all_responses(skip=("MTL",)))
            roster = self.gate(root)
        self.assertEqual(roster["status"], PARTIAL_ROSTER_STATUS)
        self.assertEqual(roster["covered_game_ids"], [2026020168, 2026020170])
        self.assertEqual(roster_covered_game_ids(roster), {2026020168, 2026020170})
        with self.assertRaisesRegex(RuntimeError, PARTIAL_ROSTER_NOT_SUPPORTED):
            require_full_roster_coverage(roster)

    def test_full_package_result_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write(root / "roster", all_responses())
            roster = self.gate(root)
        self.assertEqual(roster["status"], "VERIFIED_CURRENT_SLATE_ROSTER")
        self.assertEqual(sorted(roster), [
            "canonical_game_set_hash", "game_ids", "observation_timestamp_utc",
            "package_manifest_sha256", "package_path", "slate_date", "status",
            "strictly_prestart"])
        self.assertIsNone(roster_covered_game_ids(roster))
        require_full_roster_coverage(roster)

    def test_missing_package_is_still_not_ready(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(RosterObservationNotReady):
                self.gate(Path(temp))

    def test_corrupt_partial_package_is_invalid_not_ready(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = write(root / "roster", all_responses(skip=("MTL",)))
            (package / "roster_snapshot.jsonl").write_text("")
            with self.assertRaisesRegex(RuntimeError, "CURRENT_SLATE_ROSTER_OBSERVATION_INVALID") as caught:
                self.gate(root)
            self.assertNotIsInstance(caught.exception, RosterObservationNotReady)


class RosterChildTeamFailureTests(unittest.TestCase):
    def test_only_single_team_provider_failures_are_recorded(self):
        import requests
        from backend.nhl.official_request_journal import OfficialTransportExhausted
        from backend.nhl.scripts.refresh_players_and_roster_today import per_team_roster_failure
        self.assertEqual(per_team_roster_failure(requests.HTTPError("HTTP 500")), "HTTP_ERROR")
        self.assertEqual(per_team_roster_failure(OfficialTransportExhausted("ConnectionError:x")),
                         "TRANSPORT_EXHAUSTED")
        self.assertEqual(per_team_roster_failure(RuntimeError("OFFICIAL_RESPONSE_ROSTER_EMPTY")),
                         "OFFICIAL_RESPONSE_ROSTER_EMPTY")
        for integrity in ("OFFICIAL_REDIRECT_LOOP", "PRESERVED_RESPONSE_HASH_MISMATCH",
                          "OFFICIAL_REQUEST_JOURNAL_CONFIG_INCOMPLETE:x"):
            self.assertIsNone(per_team_roster_failure(RuntimeError(integrity)))
        self.assertIsInstance(OfficialTransportExhausted("x"), RuntimeError)

    def test_journal_rejected_empty_roster_is_recorded_as_empty(self):
        with tempfile.TemporaryDirectory() as temp:
            package = write(Path(temp), all_responses(skip=("MTL",)),
                            team_failures={"MTL": "OFFICIAL_RESPONSE_ROSTER_EMPTY"})
            summary = json.loads((package / "observation_summary.json").read_text())
        self.assertEqual(summary["excluded_teams"], {"MTL": "TEAM_RESPONSE_EMPTY"})

    def test_database_rows_are_limited_to_covered_games_teams(self):
        from backend.nhl.scripts.refresh_players_and_roster_today import covered_team_ids
        games = [{"game_id": game.game_id, "home_team_id": index * 2 + 1,
                  "away_team_id": index * 2 + 2} for index, game in enumerate(GAMES)]
        with tempfile.TemporaryDirectory() as temp:
            partial = write(Path(temp) / "partial", all_responses(skip=("MTL",)))
            full = write(Path(temp) / "full", all_responses())
            self.assertEqual(covered_team_ids(partial, games), {1, 2, 5, 6})
            self.assertIsNone(covered_team_ids(full, games))


class DailyLaneRosterExclusionTests(unittest.TestCase):
    CANONICAL = [
        {"game_id": 2026020168, "start_time_utc": "2026-10-24T23:00:00Z",
         "home_team_id": 1, "away_team_id": 2},
        {"game_id": 2026020169, "start_time_utc": "2026-10-24T23:30:00Z",
         "home_team_id": 3, "away_team_id": 4},
    ]
    EXCLUDED = {2026020169: "ROSTER_COVERAGE_EXCLUDED:MTL=TEAM_RESPONSE_MISSING"}

    def prepare(self, root: Path, **kwargs) -> dict:
        import pandas as pd
        from backend.nhl.prediction_lineage import prepare_scoring_input
        source = root / "source.csv"
        pd.DataFrame([{"player_id": 11, "game_id": 2026020168},
                      {"player_id": 12, "game_id": 2026020169}]).to_csv(source, index=False)
        kwargs.setdefault("allow_partial_slate", True)
        return prepare_scoring_input(
            source_path=source, output_path=root / "input.csv", canonical_games=self.CANONICAL,
            slate=SLATE, parent_daily_run_id="run", feature_input_cutoff_utc="2026-10-24T18:00:00Z",
            expected_game_set_hash=canonical_game_set_hash([2026020168, 2026020169]), **kwargs)

    def test_scoring_input_drops_and_records_roster_excluded_games(self):
        import pandas as pd
        with tempfile.TemporaryDirectory() as temp:
            identity = self.prepare(Path(temp), roster_excluded_games=self.EXCLUDED)
            rows = pd.read_csv(Path(temp) / "input.csv")
        self.assertEqual(rows.game_id.tolist(), [2026020168])
        self.assertEqual(identity["scored_game_ids"], [2026020168])
        self.assertEqual(identity["input_excluded_games"], [])
        self.assertEqual(identity["roster_excluded_games"], [
            {"game_id": 2026020169, "reason": self.EXCLUDED[2026020169]}])

    def test_full_roster_input_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            identity = self.prepare(Path(temp))
        self.assertNotIn("roster_excluded_games", identity)
        self.assertEqual(identity["scored_game_ids"], [2026020168, 2026020169])

    def test_whole_slate_callers_cannot_take_roster_exclusions(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "REQUIRES_PARTIAL_SLATE"):
                self.prepare(Path(temp), allow_partial_slate=False,
                             roster_excluded_games=self.EXCLUDED)

    def test_prediction_on_an_excluded_game_fails_validation(self):
        import pandas as pd
        from backend.nhl.prediction_lineage import POINTS_LINES, validate_prediction_output
        with tempfile.TemporaryDirectory() as temp:
            self.prepare(Path(temp))
            frame = pd.read_csv(Path(temp) / "input.csv")
            rows = pd.concat([frame.assign(line=line, prob_over=0.4) for line in POINTS_LINES])
            rows.to_csv(Path(temp) / "pred.csv", index=False)
            kwargs = dict(path=Path(temp) / "pred.csv", lane="points",
                          canonical_games=self.CANONICAL, slate=SLATE, parent_daily_run_id="run",
                          feature_input_cutoff_utc="2026-10-24T18:00:00Z",
                          expected_game_set_hash=canonical_game_set_hash([2026020168, 2026020169]),
                          expected_lines=POINTS_LINES)
            validate_prediction_output(**kwargs)
            with self.assertRaisesRegex(RuntimeError, "PREDICTION_ROSTER_EXCLUDED_GAME_INCLUDED"):
                validate_prediction_output(**kwargs, roster_excluded_game_ids=self.EXCLUDED)

    def test_daily_recorder_exposes_the_receipt_exclusions(self):
        from types import SimpleNamespace
        from backend.nhl.cli import _roster_excluded_games
        recorder = SimpleNamespace(roster_observation={"roster_game_exclusions": [
            {"game_id": 2026020169, "reason": "R"}]})
        self.assertEqual(_roster_excluded_games(recorder), {2026020169: "R"})
        self.assertEqual(_roster_excluded_games(SimpleNamespace(roster_observation=None)), {})


PARTIAL_ROSTER = {
    "status": PARTIAL_ROSTER_STATUS, "package_path": "/roster/partial",
    "package_manifest_sha256": "roster-hash", "covered_game_ids": [2026020001],
    "excluded_game_ids": [2026020002],
    "roster_game_exclusions": [{"game_id": 2026020002,
                                "reason": "ROSTER_COVERAGE_EXCLUDED:MTL=TEAM_RESPONSE_MISSING"}],
}
CERTIFICATE = {"status": "CERTIFIED_FINAL", "slate_date": "2026-10-17",
               "package_manifest_sha256": "verified-hash", "game_ids": [2026020000]}
OBSERVED = datetime(2026, 10, 18, 16, 0, tzinfo=UTC)


class ObserverPartialRosterTests(unittest.TestCase):
    def test_points_observer_excludes_uncovered_games_with_the_roster_reason(self):
        import pandas as pd
        from backend.nhl.scripts import nhl_prediction_only_common as common
        games = pd.DataFrame({"game_id": [2026020001, 2026020002],
                              "scheduled_start_time_utc": ["2026-10-18T20:00:00Z"] * 2})
        features = pd.DataFrame({"game_id": [2026020001, 2026020002], "player_id": [7, 8]})
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / "run"
            run_dir.mkdir()
            (run_dir / "run_metadata.json").write_text(json.dumps({"prediction_rows": 3}))
            with patch.object(common, "require_slate_history_certificate", return_value=CERTIFICATE), \
                    patch.object(common, "require_slate_roster_observation", return_value=PARTIAL_ROSTER), \
                    patch.object(common, "export_lane_inputs", return_value=(games, features)), \
                    patch.object(common, "publish_points",
                                 return_value=(run_dir, "COMPLETE_NEW_APPEND_ONLY")) as publish:
                path = common.observe(
                    lane="POINTS", season=2026, slate_date="2026-10-18",
                    requested_phase="MIDDAY", observation_timestamp=OBSERVED,
                    canonical_run_identifier="test", dsn="research-dsn",
                    output_root=Path(temp) / "observer")
                result = json.loads(path.read_text())
        self.assertEqual(result["status"], "CAPTURED")
        self.assertIn({"game_id": 2026020002,
                       "reason": "ROSTER_COVERAGE_EXCLUDED:MTL=TEAM_RESPONSE_MISSING"},
                      result["game_exclusions"])
        published = publish.call_args.kwargs
        self.assertEqual(published["players"].game_id.tolist(), [2026020001])
        self.assertEqual(published["readiness_evidence"]["current_slate_roster_observation"],
                         PARTIAL_ROSTER)

    def test_points_observer_noops_when_every_game_is_roster_excluded(self):
        import pandas as pd
        from backend.nhl.scripts import nhl_prediction_only_common as common
        games = pd.DataFrame({"game_id": [2026020002],
                              "scheduled_start_time_utc": ["2026-10-18T20:00:00Z"]})
        features = pd.DataFrame({"game_id": [2026020002], "player_id": [8]})
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(common, "require_slate_history_certificate", return_value=CERTIFICATE), \
                patch.object(common, "require_slate_roster_observation", return_value=PARTIAL_ROSTER), \
                patch.object(common, "export_lane_inputs", return_value=(games, features)), \
                patch.object(common, "publish_points") as publish:
            path = common.observe(
                lane="POINTS", season=2026, slate_date="2026-10-18", requested_phase="MIDDAY",
                observation_timestamp=OBSERVED, canonical_run_identifier="test",
                dsn="research-dsn", output_root=Path(temp) / "observer")
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "NOOP_NO_QUALIFIED_FEATURE_INPUTS")
        publish.assert_not_called()

    def test_publisher_game_exclusions_use_the_roster_reason(self):
        import pandas as pd
        from backend.nhl.prediction_only.core import _game_exclusion_rows, _readiness_roster_exclusions
        roster = _readiness_roster_exclusions({"current_slate_roster_observation": PARTIAL_ROSTER})
        games = pd.DataFrame({"game_id": [2026020001, 2026020002]})
        rows = _game_exclusion_rows(games, [2026020001, 2026020002], [], {2026020001},
                                    empty_reason="EMPTY", roster_exclusions=roster)
        self.assertEqual(rows.to_dict("records"), [
            {"game_id": 2026020002, "reason": "ROSTER_COVERAGE_EXCLUDED:MTL=TEAM_RESPONSE_MISSING"}])
        with self.assertRaisesRegex(RuntimeError, "ROSTER_EXCLUDED_GAME_REPRESENTED"):
            _game_exclusion_rows(games, [2026020001, 2026020002], [], {2026020002},
                                 empty_reason="EMPTY", roster_exclusions=roster)
        self.assertEqual(_readiness_roster_exclusions({"current_slate_roster_observation": {
            "status": "VERIFIED_CURRENT_SLATE_ROSTER"}}), {})

    def test_sog_observer_predicts_only_covered_games(self):
        import pandas as pd
        from backend.nhl.scripts import run_nhl_sog_prediction_only_warn_only as sog
        columns = ["canonical_season", "slate_date", "game_id", "player_id", "player_name",
                   "team_id", "opponent_id", "position", "roster_status", "lineup_status",
                   "scheduled_start_time_utc", "feature_cutoff_utc"]
        features = pd.DataFrame([
            [2026, "2026-10-18", game_id, player, "P", 1, 2, "F", "ACTIVE_ROSTER",
             "ACTIVE_ROSTER_UNCONFIRMED", "2026-10-18T20:00:00Z", "2026-10-18T16:00:00Z"]
            for game_id, player in ((2026020001, 7), (2026020002, 8))], columns=columns)
        schedule = pd.DataFrame({"game_id": [2026020001, 2026020002],
                                 "scheduled_start_time_utc": ["2026-10-18T20:00:00Z"] * 2})
        empty = pd.DataFrame(columns=[*columns, "phase", "exclusion_reason", "contract_version"])
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(sog, "require_slate_history_certificate", return_value=CERTIFICATE), \
                patch.object(sog, "require_slate_roster_observation", return_value=PARTIAL_ROSTER), \
                patch.object(sog, "export_features", return_value=(schedule, features)), \
                patch.object(sog, "build_predictions",
                             return_value=(pd.DataFrame(), pd.DataFrame(), empty)) as build:
            path = sog.observe(slate="2026-10-18", requested="MIDDAY", dsn="research-dsn",
                               root=Path(temp), now=OBSERVED)
            result = json.loads(path.read_text())
            excluded = pd.read_csv(Path(result["run_dir"]) / "excluded_players.csv")
        self.assertEqual(result["status"], "CAPTURED")
        self.assertEqual(build.call_args.args[0].game_id.tolist(), [2026020001])
        self.assertEqual(excluded.to_dict("records")[0]["exclusion_reason"],
                         "ROSTER_COVERAGE_EXCLUDED:MTL=TEAM_RESPONSE_MISSING")
        self.assertEqual(excluded.game_id.tolist(), [2026020002])

    def test_mainline_blocks_on_partial_roster_before_export_or_odds(self):
        from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as mainline
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(mainline, "utc_now", return_value=OBSERVED), \
                patch.object(mainline, "ensure_prior_day_official_outcomes",
                             return_value={"status": "EXISTING_GOVERNED_PACKAGE_VALID"}), \
                patch.object(mainline, "require_slate_history_certificate", return_value=CERTIFICATE), \
                patch.object(mainline, "require_slate_roster_observation", return_value=PARTIAL_ROSTER), \
                patch.object(mainline, "export_inputs") as export, \
                patch.object(mainline, "fetch_markets") as markets:
            path = mainline.observe(Path(temp), "2026-10-18", "AUTO", False, "research-dsn")
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertIn(PARTIAL_ROSTER_NOT_SUPPORTED, result["failure"])
        self.assertNotIn("retry_eligible_on_later_poll", result)
        export.assert_not_called()
        markets.assert_not_called()


if __name__ == "__main__":
    unittest.main()
