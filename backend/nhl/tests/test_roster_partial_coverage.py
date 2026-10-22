"""Per-game roster coverage (NHL backlog P2.13, roster part)."""
from __future__ import annotations

import csv
import json
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


if __name__ == "__main__":
    unittest.main()
