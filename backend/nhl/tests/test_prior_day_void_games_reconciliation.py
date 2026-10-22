"""P2.24 D: a postponed, suspended or cancelled prior-day game is void.

Before the fix one such game made reconciliation admit an unplayable game,
fail ADMITTED_GAMES_NOT_OFFICIAL_FINAL (exit 2, NOT_FINAL) and stop the
morning run. Now the official schedule's gameScheduleState leaves it out of
the played set, which is threaded through the request journal, acquisition
receipt, binding, package and certificate. A game still in progress with
state OK still waits (NOT_FINAL).
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl import official_request_journal as journal
from backend.nhl.official_request_journal import (
    ENV_CACHE, ENV_GAME_HASH, ENV_GAME_IDS, ENV_JOURNAL, ENV_REQUIRED, ENV_RUN_ID,
    ENV_SLATE, ENV_VOIDED_GAME_IDS, canonical_game_set_hash, skip_voided_games)
from backend.nhl.postgame_reconcile.core import validate_final_slate
from backend.nhl.reconciliation_certificate import verify_reconciliation_certificate
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture
from backend.nhl.scripts import run_nhl_postgame_reconciliation as reconciler

DAY = "2026-12-10"
A, B, C = 2026020400, 2026020401, 2026020402
TEAMS = {A: ("AAA", "BBB", 1, 2), B: ("CCC", "DDD", 3, 4), C: ("EEE", "FFF", 5, 6)}
VOID_STATES = ("PPD", "SUSP", "CNCL")


def schedule_payload(states: dict[int, tuple[str, str]]) -> dict:
    games = []
    for game_id, (game_state, schedule_state) in states.items():
        home, away, home_id, away_id = TEAMS[game_id]
        games.append({
            "id": game_id, "gameType": 2, "gameState": game_state,
            "gameScheduleState": schedule_state, "startTimeUTC": "2026-12-11T00:00:00Z",
            "homeTeam": {"id": home_id, "abbrev": home, "score": 3},
            "awayTeam": {"id": away_id, "abbrev": away, "score": 2}})
    return {"gameWeek": [{"date": DAY, "games": games}]}


class FakeResponse:
    def __init__(self, status: int, payload: dict | None = None, location: str | None = None):
        self.status_code = status
        self.content = json.dumps(payload or {}).encode()
        self.headers = {"Location": location} if location else {}


class FakeNhl:
    """Official NHL endpoints served from memory; records each requested URL."""

    def __init__(self, schedule: dict, box_dates: dict[int, str] | None = None):
        self.schedule = schedule
        self.box_dates = box_dates or {}
        self.urls: list[str] = []

    def session(self, *args, **kwargs):
        fake = self

        class Session:
            headers: dict = {}

            def get(self, url, **_):
                fake.urls.append(url)
                if "/schedule/" in url:
                    return FakeResponse(200, fake.schedule)
                if "/boxscore" in url:
                    game_id = int(url.split("/")[-2])
                    return FakeResponse(200, {"id": game_id,
                                              "gameDate": fake.box_dates.get(game_id, DAY)})
                team = url.split("/")[-2]
                if url.endswith("/current"):
                    return FakeResponse(307, location=f"https://api-web.nhle.com/v1/roster/{team}/20262027")
                return FakeResponse(200, {"forwards": [{"id": 1, "team": team}]})

            def close(self):
                return None

        return Session()


def binding(game_ids, voided: dict[int, str] | None = None) -> dict:
    voided = voided or {}
    played = [game_id for game_id in game_ids if game_id not in voided]
    result = {"game_ids": played,
              "team_codes": sorted(code for game_id in played for code in TEAMS[game_id][:2])}
    if voided:
        result["scheduled_game_ids"] = sorted(game_ids)
        result["schedule_voided_games"] = [{
            "slate_date": DAY, "game_id": game_id, "game_schedule_state": state,
            "reason": f"VOID_{state}", "scheduled_start_time_utc": "2026-12-11T00:00:00+00:00",
            "game_type_code": 2, "home_team_id": TEAMS[game_id][2],
            "away_team_id": TEAMS[game_id][3], "home_team": TEAMS[game_id][0],
            "away_team": TEAMS[game_id][1]} for game_id, state in sorted(voided.items())]
    return result


class VoidGameAcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.output = self.root / "postgame_reconciliation"
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def acquire(self, states: dict[int, tuple[str, str]]):
        nhl = FakeNhl(schedule_payload(states))
        with patch.object(reconciler, "ROOT", self.root), \
                patch.object(journal.requests, "Session", side_effect=nhl.session):
            result = reconciler.authority_roster_acquisition(
                slate_date=DAY, source_binding=binding(list(states)),
                output_root=self.output,
                rebind=lambda voided: binding(list(states), voided))
        return result, nhl

    def test_void_game_is_left_out_of_the_acquired_played_set(self):
        for state in VOID_STATES:
            with self.subTest(state=state):
                result, nhl = self.acquire({A: ("OFF", "OK"), B: ("FUT", state), C: ("OFF", "OK")})
                self.assertEqual(result["game_ids"], [A, C])
                self.assertEqual(result["canonical_game_set_hash"], canonical_game_set_hash([A, C]))
                self.assertEqual([(row["game_id"], row["game_schedule_state"])
                                  for row in result["schedule_voided_games"]], [(B, state)])
                self.assertFalse(any(f"/{B}/boxscore" in url for url in nhl.urls))
                self.assertFalse(any("/CCC/" in url or "/DDD/" in url for url in nhl.urls))
                # The schedule-state probe run is retained and linked.
                probe_id = result["schedule_state_probe"]["run_id"]
                probe = json.loads(reconciler._acquisition_receipt_path(
                    self.output, DAY, probe_id).read_text())
                self.assertEqual(probe["contract_version"], "NHL_SCHEDULE_STATE_PROBE_V1")
                self.assertEqual(probe["voided_games"], {str(B): state})
                self.assertEqual(probe["request_accounting"]["total_logical_requests"], 1)
                # The execute run's binding re-verifies the void games against
                # the preserved schedule and binds the played set.
                with patch.object(reconciler, "ROOT", self.root):
                    self.assertEqual(reconciler._receipt_voided_games(
                        self.output, DAY, [f"AUTHORITY_RESPONSE_SOURCE={result['run_id']}"]),
                        {B: state})
                    bound = reconciler._binding_from_acquisition(
                        output_root=self.output, run_id=result["run_id"],
                        role="AUTHORITY_RESPONSE_SOURCE", slate_date=DAY,
                        game_ids=[A, C], teams=["AAA", "BBB", "EEE", "FFF"])
                self.assertEqual(bound["canonical_game_set_hash"], canonical_game_set_hash([A, C]))

    def test_slate_without_void_games_is_acquired_as_before(self):
        result, nhl = self.acquire({A: ("OFF", "OK"), C: ("OFF", "OK")})
        self.assertEqual(result["game_ids"], [A, C])
        self.assertNotIn("schedule_voided_games", result)
        self.assertEqual(sum("/schedule/" in url for url in nhl.urls), 1)

    def test_tampered_void_record_fails_the_binding(self):
        result, _ = self.acquire({A: ("OFF", "OK"), B: ("FUT", "PPD"), C: ("OFF", "OK")})
        path = reconciler._acquisition_receipt_path(self.output, DAY, result["run_id"])
        receipt = json.loads(path.read_text())
        receipt["schedule_voided_games"] = []
        path.write_text(json.dumps(receipt))
        with patch.object(reconciler, "ROOT", self.root), \
                self.assertRaisesRegex(RuntimeError, "ACQUISITION_RECEIPT_VOIDED_GAMES_MISMATCH"):
            reconciler._binding_from_acquisition(
                output_root=self.output, run_id=result["run_id"],
                role="AUTHORITY_RESPONSE_SOURCE", slate_date=DAY,
                game_ids=[A, C], teams=["AAA", "BBB", "EEE", "FFF"])

    def test_unknown_schedule_state_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "OFFICIAL_SCHEDULE_STATE_UNSUPPORTED"):
            self.acquire({A: ("OFF", "OK"), B: ("FUT", "TBD")})


class VoidGameExecuteTests(unittest.TestCase):
    """The execute run's official fetch and FINAL check use the played set."""

    def fetch(self, states, expected, voided):
        nhl = FakeNhl(schedule_payload(states))
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {
                ENV_REQUIRED: "1", ENV_RUN_ID: "execute_fixture",
                ENV_JOURNAL: str(Path(temp) / "journal.jsonl"),
                ENV_CACHE: str(Path(temp) / "cache"), ENV_SLATE: DAY,
                ENV_GAME_HASH: canonical_game_set_hash(expected),
                ENV_GAME_IDS: ",".join(map(str, sorted(expected)))}), \
                patch.object(journal.requests, "Session", side_effect=nhl.session):
            official, boxes, counts = reconciler.fetch_official(
                DAY, set(expected), voided_games=voided)
        return official, boxes, nhl

    @staticmethod
    def canonical(game_ids):
        return pd.DataFrame([{
            "canonical_season": 2026, "slate_date": DAY, "game_id": game_id,
            "scheduled_start_time_utc": "2026-12-11T00:00:00Z",
            "home_team_id": TEAMS[game_id][2], "away_team_id": TEAMS[game_id][3],
            "game_type_code": 2} for game_id in game_ids])

    def test_played_games_certify_with_a_void_game_on_the_schedule(self):
        for state in VOID_STATES:
            with self.subTest(state=state):
                states = {A: ("OFF", "OK"), B: ("FUT", state), C: ("FINAL", "OK")}
                # The old path admitted every scheduled game and stopped here.
                old = reconciler.official_games_for_slate(schedule_payload(states), DAY)
                with self.assertRaisesRegex(RuntimeError, "ADMITTED_GAMES_NOT_OFFICIAL_FINAL"):
                    validate_final_slate(self.canonical([A, B, C]), old, DAY)
                official, boxes, nhl = self.fetch(states, [A, C], {B: state})
                self.assertEqual(sorted(official.game_id), [A, C])
                self.assertEqual(sorted(boxes), [A, C])
                self.assertFalse(any(f"/{B}/boxscore" in url for url in nhl.urls))
                validated = validate_final_slate(self.canonical([A, C]), official, DAY)
                self.assertEqual(sorted(validated.game_id), [A, C])

    def test_a_game_still_in_progress_still_waits(self):
        states = {A: ("OFF", "OK"), B: ("FUT", "PPD"), C: ("LIVE", "OK")}
        official, _, _ = self.fetch(states, [A, C], {B: "PPD"})
        with self.assertRaisesRegex(RuntimeError, "ADMITTED_GAMES_NOT_OFFICIAL_FINAL"):
            validate_final_slate(self.canonical([A, C]), official, DAY)

    def test_void_state_differing_from_the_binding_fails_closed(self):
        states = {A: ("OFF", "OK"), B: ("FUT", "PPD"), C: ("OFF", "OK")}
        with self.assertRaisesRegex(RuntimeError, "OFFICIAL_GAME_SET_MISMATCH"):
            self.fetch(states, [A, C], {})
        with self.assertRaisesRegex(RuntimeError, "OFFICIAL_SCHEDULE_VOID_STATE_CHANGED"):
            self.fetch(states, [A, C], {B: "SUSP"})

    def test_collectors_skip_only_the_declared_void_games(self):
        with patch.dict(os.environ, {ENV_VOIDED_GAME_IDS: str(B)}):
            self.assertEqual(skip_voided_games([A, B, C]), [A, C])
        with patch.dict(os.environ, {ENV_VOIDED_GAME_IDS: ""}):
            self.assertEqual(skip_voided_games([A, B, C]), [A, B, C])


def write_package(root: Path, *, voided_rows) -> Path:
    package = root / DAY / "reconciliation=fixture"
    package.mkdir(parents=True)
    played = [A, C]

    def write(name, rows):
        with (package / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    write("canonical_admitted_slate.csv", [{"slate_date": DAY, "game_id": g} for g in played])
    write("canonical_game_outcomes.csv", [{
        "slate_date": DAY, "game_id": g, "official_final": "True",
        "official_final_home_goals": 3, "official_final_away_goals": 2,
        "outcome_source": "NHL_OFFICIAL_GAMECENTER",
        "outcome_source_timestamp_utc": "2026-12-11T04:00:00Z"} for g in played])
    write("graded_moneyline.csv", [{"game_id": g} for g in played])
    write("graded_puck_line.csv", [{"game_id": g} for g in played])
    write("schedule_voided_games.csv", voided_rows)
    (package / "summary.json").write_text(json.dumps(
        {"status": "COMPLETE", "slate_date": DAY, "games": 2}))
    (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
    (package / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in sorted(package.iterdir())))
    return package


class VoidGameCertificateTests(unittest.TestCase):
    def test_certificate_names_void_games_and_cross_market_check_accepts_them(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = write_package(root, voided_rows=[{
                "slate_date": DAY, "game_id": B, "game_schedule_state": "PPD", "reason": "VOID_PPD"}])
            certificate = verify_reconciliation_certificate(package, DAY)
            self.assertEqual(certificate["game_ids"], [A, C])
            self.assertEqual(certificate["voided_game_ids"], [B])
            # nhl.games still lists the postponed game on its original date.
            with patch.object(capture, "load_official_outcomes",
                              return_value=pd.DataFrame({"game_id": [A, C]})):
                verified = capture._verify_prior_day_package(root, DAY, [A, B, C])
                self.assertEqual(verified["game_ids"], [A, C])
                # ...or it has already moved to its new date.
                capture._verify_prior_day_package(root, DAY, [A, C])

    def test_void_game_that_was_also_admitted_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            package = write_package(Path(temp), voided_rows=[{
                "slate_date": DAY, "game_id": C, "game_schedule_state": "SUSP", "reason": "VOID_SUSP"}])
            with self.assertRaisesRegex(RuntimeError, "VOIDED_GAMES_INVALID"):
                verify_reconciliation_certificate(package, DAY)



class AllVoidAndMovedGameTests(unittest.TestCase):
    """P2.24 D follow-ups: an all-void day certifies like an off day, and a game
    dropped from its original date is void only when its own box score moved."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.output = self.root / "postgame_reconciliation"
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def acquire(self, listed: dict[int, tuple[str, str]], spine, box_dates=None):
        nhl = FakeNhl(schedule_payload(listed), box_dates)
        with patch.object(reconciler, "ROOT", self.root), \
                patch.object(journal.requests, "Session", side_effect=nhl.session):
            result = reconciler.authority_roster_acquisition(
                slate_date=DAY, source_binding=binding(list(spine)),
                output_root=self.output,
                rebind=lambda voided: binding(list(spine), voided))
        return result, nhl

    def test_all_void_day_is_certified_like_an_off_day(self):
        from backend.nhl.reconciliation_certificate import (
            certificate_identity, require_prior_day_certificate,
            verify_prior_day_certificate)
        result, nhl = self.acquire(
            {A: ("FUT", "PPD"), B: ("FUT", "CNCL"), C: ("FUT", "PPD")}, [A, B, C])
        self.assertEqual(result["status"], "ALL_SCHEDULED_GAMES_VOID")
        self.assertEqual(result["game_ids"], [])
        self.assertFalse(any("/boxscore" in url or "/roster/" in url for url in nhl.urls))
        certificate = require_prior_day_certificate(
            DAY, reconciliation_root=self.output, slate_root=self.root / "slates")
        self.assertEqual(certificate["status"], "VERIFIED_ALL_GAMES_VOID")
        self.assertEqual(certificate["voided_game_ids"], [A, B, C])
        again = verify_prior_day_certificate(certificate, DAY, reconciliation_root=self.output)
        self.assertEqual(certificate_identity(again), certificate_identity(certificate))
        self.assertEqual(certificate_identity(certificate), certificate["probe_receipt_sha256"])

    def test_all_void_certificate_rejects_a_tampered_schedule(self):
        from backend.nhl.reconciliation_certificate import verified_all_games_void
        result, _ = self.acquire({A: ("FUT", "PPD"), C: ("FUT", "SUSP")}, [A, C])
        objects = self.output / "request_runs" / DAY / result["run_id"] / "preserved_responses" / "objects"
        for path in objects.glob("*.json"):
            path.write_text(path.read_text().replace('"SUSP"', '"OK"'))
        with self.assertRaisesRegex(RuntimeError, "HASH_MISMATCH"):
            verified_all_games_void(DAY, reconciliation_root=self.output)

    def test_daily_and_cross_market_accept_an_all_void_day(self):
        from backend.nhl import cli, postgame_learning
        self.acquire({A: ("FUT", "PPD"), C: ("FUT", "PPD")}, [A, C])
        learning = postgame_learning.ensure_prior_learning(DAY, reconciliation_root=self.output)
        self.assertEqual(learning["reconciliation_status"], "ALL_GAMES_VOID")
        self.assertEqual(learning["provider_calls"], 0)
        from backend.nhl.reconciliation_certificate import verified_all_games_void
        certificate = verified_all_games_void(DAY, reconciliation_root=self.output)
        with patch.object(cli, "require_prior_day_certificate", return_value=certificate):
            self.assertEqual(cli._require_daily_prior_certificate(DAY, learning), certificate)
        learning["certificate"] = certificate
        self.assertEqual(cli.prior_day_voided_env(learning, DAY),
                         {ENV_VOIDED_GAME_IDS: f"{A},{C}"})
        checked = capture._all_games_void_certificate(self.output, DAY, [A, C])
        self.assertEqual(checked["status"], "PRIOR_DAY_ALL_GAMES_VOID")
        with self.assertRaisesRegex(RuntimeError, "ALL_VOID_GAME_SET_MISMATCH"):
            capture._all_games_void_certificate(self.output, DAY, [A, B, C])

    def test_game_dropped_from_its_date_is_void_when_its_box_score_moved(self):
        result, nhl = self.acquire({A: ("OFF", "OK"), C: ("OFF", "OK")}, [A, B, C],
                                   box_dates={B: "2027-01-15"})
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["game_ids"], [A, C])
        self.assertEqual([(row["game_id"], row["game_schedule_state"])
                          for row in result["schedule_voided_games"]], [(B, "MOVED")])
        self.assertEqual(sum(f"/{B}/boxscore" in url for url in nhl.urls), 1)  # probe only
        with patch.object(reconciler, "ROOT", self.root):
            bound = reconciler._binding_from_acquisition(
                output_root=self.output, run_id=result["run_id"],
                role="AUTHORITY_RESPONSE_SOURCE", slate_date=DAY,
                game_ids=[A, C], teams=["AAA", "BBB", "EEE", "FFF"])
        self.assertEqual(bound["role"], "AUTHORITY_RESPONSE_SOURCE")
        official, boxes, _ = VoidGameExecuteTests().fetch(
            {A: ("OFF", "OK"), C: ("OFF", "OK")}, [A, C], {B: "MOVED"})
        self.assertEqual(sorted(boxes), [A, C])

    def test_dropped_game_still_on_its_date_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "ACQUISITION_OFFICIAL_GAME_SET_MISMATCH"):
            self.acquire({A: ("OFF", "OK"), C: ("OFF", "OK")}, [A, B, C])

    def test_moved_game_without_probe_evidence_fails_the_binding(self):
        result, _ = self.acquire({A: ("OFF", "OK"), C: ("OFF", "OK")}, [A, B, C],
                                 box_dates={B: "2027-01-15"})
        probe = result["schedule_state_probe"]["run_id"]
        objects = self.output / "request_runs" / DAY / probe / "preserved_responses" / "objects"
        for path in objects.glob("*.json"):
            path.write_text(path.read_text().replace("2027-01-15", DAY))
        with patch.object(reconciler, "ROOT", self.root), \
                self.assertRaisesRegex(RuntimeError, "HASH_MISMATCH"):
            reconciler._binding_from_acquisition(
                output_root=self.output, run_id=result["run_id"],
                role="AUTHORITY_RESPONSE_SOURCE", slate_date=DAY,
                game_ids=[A, C], teams=["AAA", "BBB", "EEE", "FFF"])


if __name__ == "__main__":
    unittest.main()
