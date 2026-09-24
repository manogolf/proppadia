from __future__ import annotations

import json
import base64
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from backend.nhl import cli
from backend.nhl.daily_capture import (
    CanonicalGame,
    HttpExchange,
    ObservationAlreadyClaimed,
    OddsObservationResult,
    ProviderCapture,
    RequestsOddsProvider,
    capture_odds_observation,
    load_canonical_slate,
    plan_first_puck_phases,
    sha256_bytes,
    sha256_file,
    verify_package,
    write_roster_observation,
)


UTC = timezone.utc
SLATE = "2026-09-24"


def games() -> list[CanonicalGame]:
    return [CanonicalGame(
        2026010037, "2026-09-24T23:00:00Z", "NJD", "NYR",
        ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"),
    )]


def exchange(*, status: int = 200, body: bytes = b"[]") -> HttpExchange:
    from hashlib import sha256
    return HttpExchange(
        method="GET", sanitized_url="https://api.the-odds-api.com/v4/sports/icehockey_nhl/events",
        sanitized_parameters={"regions": "us"},
        started_at_utc="2026-09-24T18:00:00Z", ended_at_utc="2026-09-24T18:00:01Z",
        duration_ms=1000, status=status, response_bytes=len(body),
        response_sha256=sha256(body).hexdigest(), headers={"content-type": "application/json"},
    )


def market_event(*, home: str = "New Jersey Devils", away: str = "New York Rangers") -> dict:
    return {
        "id": "provider-event-1", "home_team": home, "away_team": away,
        "commence_time": "2026-09-24T23:00:00Z",
        "bookmakers": [{"key": "research-book", "title": "Research Book", "markets": [{
            "key": "player_points", "last_update": "2026-09-24T18:00:00Z",
            "outcomes": [{"name": "Over", "description": "Sample Player", "price": -110, "point": 0.5}],
        }]}],
    }


def provider_event(*, home: str = "New Jersey Devils", away: str = "New York Rangers") -> dict:
    return {
        "id": "provider-event-1", "home_team": home, "away_team": away,
        "commence_time": "2026-09-24T23:00:00Z",
    }


class FakeProvider:
    def __init__(self, result: ProviderCapture):
        self.result = result
        if len(result.exchanges) == 1 and not result.transport_response_bodies:
            result.transport_response_bodies = [result.raw_response_bytes]
            result.exchanges[0].response_bytes = len(result.raw_response_bytes)
            result.exchanges[0].response_sha256 = sha256_bytes(result.raw_response_bytes)
        self.calls = 0
        self.lock = threading.Lock()

    def capture(self, **_kwargs) -> ProviderCapture:
        with self.lock:
            self.calls += 1
        return self.result


class ComprehensiveDailyCaptureTests(unittest.TestCase):
    def capture(self, root: Path, provider: FakeProvider, *, phase: str = "EARLY",
                now: datetime | None = None, compatibility: Path | None = None) -> OddsObservationResult:
        return capture_odds_observation(
            root=root, season=2026, slate_date=SLATE, phase=phase,
            parent_daily_run_id="daily-run-1", canonical_games=games(), provider=provider,
            authorized=True, compatibility_dir=compatibility,
            invocation_id=f"invocation-{phase}",
            now=now or datetime(2026, 9, 24, 18, tzinfo=UTC),
        )

    def test_historical_command_compatibility_and_single_execution_graph(self):
        args = cli.build_arg_parser().parse_args(["daily", "--with-odds"])
        self.assertEqual(args.cmd, "daily")
        self.assertTrue(args.with_odds)
        self.assertEqual(args.odds_phase, "EARLY")
        graph = cli.DAILY_EXECUTION_GRAPH
        self.assertLess(graph.index("SLATE_ROSTER_CAPTURE_AND_NORMALIZATION"),
                        graph.index("FEATURE_AND_PREDICTION_DURABILITY"))
        self.assertLess(graph.index("FEATURE_AND_PREDICTION_DURABILITY"),
                        graph.index("OPTIONAL_GOVERNED_ODDS_OBSERVATION"))
        self.assertLess(graph.index("OPTIONAL_GOVERNED_ODDS_OBSERVATION"),
                        graph.index("OPTIONAL_MARKET_ATTACHMENT"))
        with patch("backend.nhl.cli.fetch_odds", return_value="capture") as fetch:
            self.assertEqual(cli.run_optional_odds_observation(
                with_odds=True, slate=SLATE, phase="EARLY"), "capture")
        fetch.assert_called_once()
        command_deck = (Path(__file__).resolve().parents[2] / "bin/nhl_ops.sh").read_text()
        self.assertIn(".venv/bin/python -m backend.nhl.cli daily --with-odds", command_deck)

    def test_no_odds_option_makes_no_acquisition(self):
        with patch("backend.nhl.cli.fetch_odds") as fetch:
            self.assertIsNone(cli.run_optional_odds_observation(with_odds=False))
        fetch.assert_not_called()

    def test_requests_provider_has_zero_retry_and_does_not_follow_redirects(self):
        class Response:
            status_code = 200
            content = b"[]"
            headers = {"content-type": "application/json", "x-requests-last": "1"}
            ok = True

            @staticmethod
            def json():
                return []

        import requests
        session = requests.Session()
        with patch.object(session, "get", return_value=Response()) as get:
            result = RequestsOddsProvider("super-secret-api-key", session=session).capture(
                days_from=1, markets="player_points", regions="us", odds_format="american")
        get.assert_called_once()
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        self.assertEqual(result.retry_count, 0)
        self.assertEqual(result.transport_response_bodies, [b"[]"])
        self.assertNotIn("apiKey", result.exchanges[0].sanitized_parameters)
        self.assertEqual(session.get_adapter("https://").max_retries.total, 0)

    def test_nonempty_capture_is_complete_append_only_and_matched(self):
        payload = [market_event()]
        provider = FakeProvider(ProviderCapture(
            None, [provider_event()], payload,
            [exchange(body=json.dumps(payload).encode())],
            (json.dumps(payload) + "\n").encode(), credits_consumed=2, credits_remaining=498,
        ))
        with tempfile.TemporaryDirectory() as temp:
            result = self.capture(Path(temp), provider)
            self.assertEqual(result.classification, "CAPTURED_NONEMPTY")
            self.assertTrue((result.observation_dir / "RUN_COMPLETE.json").is_file())
            self.assertEqual(verify_package(result.observation_dir), result.manifest_sha256)
            summary = json.loads((result.observation_dir / "observation_summary.json").read_text())
            self.assertEqual(summary["canonical_matched_event_count"], 1)
            self.assertEqual(summary["normalized_price_row_count"], 1)
            self.assertEqual(summary["credits_consumed"], 2)
            envelope = json.loads((result.observation_dir / "response_envelope.json").read_text())
            self.assertEqual(envelope["provider_timestamp_or_asof"], ["2026-09-24T18:00:00Z"])
            transport = json.loads(
                (result.observation_dir / "transport_response_bodies.jsonl").read_text())
            self.assertEqual(base64.b64decode(transport["body_base64"]), provider.result.raw_response_bytes)
            self.assertEqual(provider.calls, 1)

    def test_successful_empty_exact_body_is_valid_and_complete(self):
        provider = FakeProvider(ProviderCapture(
            None, [], [], [exchange(body=b"[]")], b"[]", empty_reason="NO_EVENTS"))
        with tempfile.TemporaryDirectory() as temp:
            result = self.capture(Path(temp), provider)
            self.assertEqual(result.classification, "CAPTURED_VALID_EMPTY")
            self.assertEqual((result.observation_dir / "raw_response.json").read_bytes(), b"[]")
            self.assertEqual(result.summary["valid_empty_reason"], "NO_EVENTS")
            self.assertTrue((result.observation_dir / "RUN_COMPLETE.json").exists())
            self.assertEqual(cli.daily_health_for_odds(requested=True, result=result), "READY")

    def test_provider_markets_without_canonical_match_are_unmatched(self):
        payload = [market_event(home="Toronto Maple Leafs", away="Ottawa Senators")]
        provider = FakeProvider(ProviderCapture(
            None, [provider_event(home="Toronto Maple Leafs", away="Ottawa Senators")], payload,
            [exchange(body=json.dumps(payload).encode())], (json.dumps(payload) + "\n").encode()))
        with tempfile.TemporaryDirectory() as temp:
            result = self.capture(Path(temp), provider)
            self.assertEqual(result.classification, "CAPTURED_UNMATCHED")
            self.assertEqual(result.summary["unmatched_event_count"], 1)
            self.assertEqual(cli.daily_health_for_odds(requested=True, result=result), "READY")

    def test_provider_and_malformed_failures_are_never_valid_empty(self):
        cases = [
            ProviderCapture("FAILED_PROVIDER", [], [], [exchange(status=401)], b'{"message":"unauthorized"}', error_type="HTTP_ERROR"),
            ProviderCapture("FAILED_PROVIDER", [], [], [exchange(status=429)], b'{"message":"quota"}', error_type="HTTP_ERROR"),
            ProviderCapture("FAILED_PROVIDER", [], [], [], b"null\n", error_type="Timeout"),
            ProviderCapture("FAILED_MALFORMED_RESPONSE", [], [], [exchange()], b"not-json", error_type="JSONDecodeError"),
        ]
        for index, capture in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as temp:
                result = self.capture(Path(temp), FakeProvider(capture))
                self.assertIn(result.classification, {"FAILED_PROVIDER", "FAILED_MALFORMED_RESPONSE"})
                self.assertTrue((result.observation_dir / "ATTEMPT_COMPLETE.json").exists())
                self.assertEqual(cli.daily_health_for_odds(requested=True, result=result), "READY_WITH_ODDS_WARNING")

    def test_explicit_request_without_credentials_is_skipped_and_recorded(self):
        with tempfile.TemporaryDirectory() as temp:
            result = capture_odds_observation(
                root=Path(temp), season=2026, slate_date=SLATE, phase="EARLY",
                parent_daily_run_id="daily", canonical_games=games(),
                provider=None, authorized=False,
                now=datetime(2026, 9, 24, 18, tzinfo=UTC),
            )
            self.assertEqual(result.classification, "SKIPPED_NO_AUTHORIZATION")
            self.assertTrue((result.observation_dir / "ATTEMPT_COMPLETE.json").exists())
            self.assertEqual(result.summary["credits_consumed"], None)

    def test_replay_and_concurrent_claim_protection_make_one_provider_call(self):
        payload = [market_event()]
        provider = FakeProvider(ProviderCapture(
            None, [provider_event()], payload,
            [exchange(body=json.dumps(payload).encode())], (json.dumps(payload) + "\n").encode()))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = self.capture(root, provider)
            second = self.capture(root, provider)
            self.assertTrue(second.replayed)
            self.assertEqual(first.observation_dir, second.observation_dir)
            self.assertEqual(provider.calls, 1)

            claim = root / ".claims" / "season=2026" / f"slate_date={SLATE}" / "phase=REFRESH.claim.json"
            claim.parent.mkdir(parents=True, exist_ok=True)
            claim.write_text("{}\n")
            with self.assertRaises(ObservationAlreadyClaimed):
                self.capture(root, provider, phase="REFRESH")
            self.assertEqual(provider.calls, 1)

    def test_two_concurrent_invocations_cannot_make_two_paid_calls(self):
        payload = [market_event()]
        provider = FakeProvider(ProviderCapture(
            None, [provider_event()], payload,
            [exchange(body=json.dumps(payload).encode())], json.dumps(payload).encode()))
        original_capture = provider.capture

        def slow_capture(**kwargs):
            time.sleep(0.05)
            return original_capture(**kwargs)

        provider.capture = slow_capture
        outcomes: list[object] = []
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            def invoke():
                try:
                    outcomes.append(self.capture(root, provider))
                except Exception as error:
                    outcomes.append(error)

            workers = [threading.Thread(target=invoke) for _ in range(2)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            self.assertEqual(provider.calls, 1)
            self.assertEqual(sum(isinstance(x, OddsObservationResult) for x in outcomes), 1)
            self.assertEqual(sum(isinstance(x, ObservationAlreadyClaimed) for x in outcomes), 1)

    def test_artifacts_contain_no_secret_and_tampering_fails(self):
        payload = [market_event()]
        provider = FakeProvider(ProviderCapture(
            None, [provider_event()], payload,
            [exchange(body=json.dumps(payload).encode())], (json.dumps(payload) + "\n").encode()))
        with tempfile.TemporaryDirectory() as temp:
            result = self.capture(Path(temp), provider)
            serialized = b"".join(path.read_bytes() for path in result.observation_dir.iterdir() if path.is_file())
            self.assertNotIn(b"super-secret-api-key", serialized)
            summary = result.observation_dir / "observation_summary.json"
            original_summary = summary.read_bytes()
            summary.write_bytes(original_summary + b" ")
            with self.assertRaisesRegex(RuntimeError, "MANIFEST_MISMATCH"):
                verify_package(result.observation_dir)

            extra = result.observation_dir / "unmanifested.txt"
            extra.write_text("unexpected")
            summary.write_bytes(original_summary)
            with self.assertRaisesRegex(RuntimeError, "MANIFEST_FILE_SET_MISMATCH"):
                verify_package(result.observation_dir)

    def test_valid_empty_does_not_destroy_latest_nonempty(self):
        nonempty_payload = [market_event()]
        first_provider = FakeProvider(ProviderCapture(
            None, [provider_event()], nonempty_payload,
            [exchange(body=json.dumps(nonempty_payload).encode())],
            (json.dumps(nonempty_payload) + "\n").encode()))
        empty_provider = FakeProvider(ProviderCapture(None, [], [], [exchange(body=b"[]")], b"[]", empty_reason="NO_EVENTS"))
        with tempfile.TemporaryDirectory() as temp:
            root, compatibility = Path(temp) / "observations", Path(temp) / "site"
            self.capture(root, first_provider, compatibility=compatibility)
            latest_hash = sha256_file(compatibility / "odds_latest.json")
            self.capture(root, empty_provider, phase="REFRESH", compatibility=compatibility,
                         now=datetime(2026, 9, 24, 20, tzinfo=UTC))
            self.assertEqual(sha256_file(compatibility / "odds_latest.json"), latest_hash)
            self.assertEqual(json.loads((compatibility / "odds_nhl_playerprops_today.json").read_text()), [])

    def test_failed_attempt_does_not_change_compatibility_outputs(self):
        payload = [market_event()]
        good = FakeProvider(ProviderCapture(
            None, [provider_event()], payload,
            [exchange(body=json.dumps(payload).encode())], json.dumps(payload).encode()))
        failed_body = b'{"message":"quota"}'
        failed = FakeProvider(ProviderCapture(
            "FAILED_PROVIDER", [], [], [exchange(status=429, body=failed_body)], failed_body,
            error_type="HTTP_ERROR"))
        with tempfile.TemporaryDirectory() as temp:
            root, compatibility = Path(temp) / "observations", Path(temp) / "site"
            self.capture(root, good, compatibility=compatibility)
            before = {path.name: sha256_file(path) for path in compatibility.iterdir()}
            result = self.capture(root, failed, phase="REFRESH", compatibility=compatibility)
            self.assertEqual(result.classification, "FAILED_PROVIDER")
            self.assertEqual(before, {path.name: sha256_file(path) for path in compatibility.iterdir()})

    def test_daily_archive_never_misattributes_stale_mutable_odds(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            site, archive = root / "site", root / "archive"
            site.mkdir()
            for name in (
                "odds_latest.json", "odds_nhl_playerprops_today.json",
                "events_today.json", "odds_observation_latest.json",
            ):
                (site / name).write_text("[]\n")
            with patch.object(cli, "SITE_DIR", site), patch.object(
                    cli, "EXPORTS_ODDS_HISTORY_DIR", archive):
                cli.archive_site_artifacts(SLATE, odds_result=None)
                no_odds_archive = archive / SLATE
                self.assertFalse((no_odds_archive / "odds_latest.json").exists())

                cli.archive_site_artifacts(
                    "2026-09-25",
                    odds_result=SimpleNamespace(classification="CAPTURED_VALID_EMPTY"),
                )
                empty_archive = archive / "2026-09-25"
                self.assertFalse((empty_archive / "odds_latest.json").exists())
                self.assertTrue((empty_archive / "odds_nhl_playerprops_today.json").exists())
                self.assertTrue((empty_archive / "odds_observation_latest.json").exists())

    def test_phase_planner_weekday_weekend_early_dst_and_states(self):
        ordinary = plan_first_puck_phases(
            slate_date="2026-09-24", first_puck_utc="2026-09-24T23:00:00Z",
            now_utc=datetime(2026, 9, 24, 12, tzinfo=UTC))
        self.assertEqual([row["target_pt"][11:16] for row in ordinary], ["06:30", "13:30", "14:45"])

        early = plan_first_puck_phases(
            slate_date="2026-11-15", first_puck_utc="2026-11-15T14:00:00Z",
            now_utc=datetime(2026, 11, 14, 20, tzinfo=UTC))
        self.assertEqual(early[0]["target_pt"][:16], "2026-11-15T01:30")
        self.assertEqual(early[1]["target_pt"][:16], "2026-11-15T03:30")
        self.assertFalse(early[0]["prior_calendar_date"])

        saturday = plan_first_puck_phases(
            slate_date="2026-10-10", first_puck_utc="2026-10-10T19:00:00Z",
            now_utc=datetime(2026, 10, 10, 11, tzinfo=UTC))
        self.assertEqual([row["target_pt"][11:16] for row in saturday], ["06:30", "09:30", "10:45"])

        prior_day = plan_first_puck_phases(
            slate_date="2026-11-15", first_puck_utc="2026-11-15T10:00:00Z",
            now_utc=datetime(2026, 11, 14, 18, tzinfo=UTC))
        self.assertTrue(prior_day[0]["prior_calendar_date"])

        dst = plan_first_puck_phases(
            slate_date="2026-11-01", first_puck_utc="2026-11-02T00:00:00Z",
            now_utc=datetime(2026, 11, 1, 12, tzinfo=UTC))
        self.assertTrue(dst[0]["target_pt"].endswith("-08:00"))
        spring_dst = plan_first_puck_phases(
            slate_date="2027-03-14", first_puck_utc="2027-03-14T23:00:00Z",
            now_utc=datetime(2027, 3, 14, 11, tzinfo=UTC))
        self.assertTrue(spring_dst[0]["target_pt"].endswith("-07:00"))
        self.assertEqual(plan_first_puck_phases(
            slate_date="2026-09-25", first_puck_utc=None,
            now_utc=datetime(2026, 9, 25, 12, tzinfo=UTC)), [])

        due = plan_first_puck_phases(
            slate_date="2026-09-24", first_puck_utc="2026-09-24T23:00:00Z",
            now_utc=datetime(2026, 9, 24, 21, tzinfo=UTC))
        self.assertEqual([row["state"] for row in due], ["DUE", "DUE", "PLANNED"])

        completed = {"EARLY": {"target_utc": ordinary[0]["target_utc"]}}
        states = plan_first_puck_phases(
            slate_date="2026-09-24", first_puck_utc="2026-09-24T23:00:00Z",
            now_utc=datetime(2026, 9, 24, 20, tzinfo=UTC), completed=completed)
        self.assertEqual(states[0]["state"], "COMPLETED")
        changed = plan_first_puck_phases(
            slate_date="2026-09-24", first_puck_utc="2026-09-24T17:00:00Z",
            now_utc=datetime(2026, 9, 24, 20, tzinfo=UTC), completed=completed)
        self.assertEqual(changed[0]["state"], "SUPERSEDED")
        missed = plan_first_puck_phases(
            slate_date="2026-09-24", first_puck_utc="2026-09-24T23:00:00Z",
            now_utc=datetime(2026, 9, 25, 0, tzinfo=UTC))
        self.assertTrue(all(row["state"] == "MISSED" for row in missed))

    def test_canonical_slate_uses_pacific_date_at_utc_midnight(self):
        raw = {"games": [{
            "id": 2026010037, "startTimeUTC": "2026-09-25T01:00:00Z",
            "homeTeam": {"abbrev": "NJD"}, "awayTeam": {"abbrev": "NYR"},
        }]}
        canonical_bytes = (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()
        health = {
            "slate_date": SLATE, "downstream_ready": True,
            "normalized_game_count": 1, "raw_source_hash": sha256_bytes(canonical_bytes),
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "raw.json").write_text(json.dumps(raw))
            (root / "health.json").write_text(json.dumps(health))
            loaded = load_canonical_slate(
                slate_date=SLATE, raw_schedule_path=root / "raw.json",
                slate_health_path=root / "health.json")
            self.assertEqual([game.game_id for game in loaded], [2026010037])

    def test_roster_snapshot_preserves_split_squad_game_multiplicity(self):
        canonical = [
            CanonicalGame(1, "2026-09-24T20:00:00Z", "TOR", "OTT"),
            CanonicalGame(2, "2026-09-24T22:00:00Z", "OTT", "TOR"),
        ]
        tor = {"forwards": [{"id": 10, "firstName": {"default": "A"}, "lastName": {"default": "One"}}]}
        ott = {"goalies": [{"id": 20, "firstName": {"default": "B"}, "lastName": {"default": "Two"}}]}
        responses = [
            {"team": "TOR", "source_url": "https://api-web.nhle.com/v1/roster/TOR/current", "observed_at_utc": "2026-09-24T12:00:00Z", "payload": tor},
            {"team": "OTT", "source_url": "https://api-web.nhle.com/v1/roster/OTT/current", "observed_at_utc": "2026-09-24T12:00:00Z", "payload": ott},
            {"team": "TOR", "source_url": "https://api-web.nhle.com/v1/roster/TOR/current", "observed_at_utc": "2026-09-24T12:00:01Z", "payload": tor},
            {"team": "OTT", "source_url": "https://api-web.nhle.com/v1/roster/OTT/current", "observed_at_utc": "2026-09-24T12:00:01Z", "payload": ott},
        ]
        with tempfile.TemporaryDirectory() as temp:
            package = write_roster_observation(
                root=Path(temp), season=2026, slate_date=SLATE, phase="EARLY",
                parent_daily_run_id="daily", canonical_games=canonical,
                source_responses=responses, now=datetime(2026, 9, 24, 12, tzinfo=UTC))
            summary = json.loads((package / "observation_summary.json").read_text())
            rows = (package / "roster_snapshot.jsonl").read_text().splitlines()
            self.assertEqual(summary["snapshot_row_count"], 4)
            self.assertEqual(len(rows), 4)
            self.assertEqual(summary["duplicate_source_row_count"], 4)
            self.assertTrue(summary["complete_per_game_coverage"])
            raw_rows = [json.loads(line) for line in (package / "raw_roster_responses.jsonl").read_text().splitlines()]
            self.assertTrue(all(len(row["response_sha256"]) == 64 for row in raw_rows))
            self.assertTrue(all(row["official_source_identity"] == "NHL_API_ROSTER" for row in raw_rows))
            self.assertEqual(verify_package(package), sha256_file(package / "SHA256SUMS"))

    def test_roster_snapshot_conflict_fails_closed(self):
        canonical = [CanonicalGame(1, "2026-09-24T20:00:00Z", "TOR", "OTT")]
        base = {"forwards": [{"id": 10, "firstName": {"default": "A"}, "lastName": {"default": "One"}}]}
        changed = {"forwards": [{"id": 10, "firstName": {"default": "A"}, "lastName": {"default": "Changed"}}]}
        responses = [
            {"team": "TOR", "source_url": "https://api-web.nhle.com/v1/roster/TOR/current", "payload": base},
            {"team": "TOR", "source_url": "https://api-web.nhle.com/v1/roster/TOR/current", "payload": changed},
            {"team": "OTT", "source_url": "https://api-web.nhle.com/v1/roster/OTT/current", "payload": {"goalies": [{"id": 20}]}},
        ]
        with tempfile.TemporaryDirectory() as temp, self.assertRaisesRegex(RuntimeError, "INCOMPLETE_OR_CONFLICTING"):
            write_roster_observation(
                root=Path(temp), season=2026, slate_date=SLATE, phase="EARLY",
                parent_daily_run_id="daily", canonical_games=canonical,
                source_responses=responses, now=datetime(2026, 9, 24, 12, tzinfo=UTC))

    def test_market_loader_never_falls_back_to_mutable_latest(self):
        from backend.nhl.scripts import build_points_with_market, build_saves_with_market, build_sog_with_market
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.json"
            self.assertIsNone(build_points_with_market.load_odds_json(missing))
            self.assertIsNone(build_saves_with_market.load_odds_json(missing))
            self.assertIsNone(build_sog_with_market.load_odds_json(missing))


if __name__ == "__main__":
    unittest.main()
