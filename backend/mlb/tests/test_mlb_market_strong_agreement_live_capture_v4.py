import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.scripts import capture_mlb_market_strong_agreement_live_v4 as capture


class Response:
    def __init__(self, payload, status=200, last="1"):
        self.content = json.dumps(payload).encode()
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {"x-requests-last": last, "x-requests-used": "101",
                        "x-requests-remaining": "899"}


def snapshot(barrier="2026-09-10T12:30:00Z"):
    row = ("2026-09-10", 1, "2026-09-10T23:00:00Z", "2026-09-10T12:29:55Z",
           "2026-09-10T12:00:00Z", "Home Club", "Away Club", .65, .35, "a" * 64,
           capture.v1.MODEL_HASH, capture.v1.ADMISSION, barrier)
    return capture.PredictionSnapshot("2026-09-10", barrier, 1, (row,))


def event(book_state="complete"):
    books = []
    if book_state != "absent":
        outcomes = [{"name": "Home Club", "price": -180}]
        if book_state == "complete":
            outcomes.append({"name": "Away Club", "price": 160})
        books.append({"key": "pinnacle", "last_update": "2026-09-10T12:30:10Z",
                      "markets": [{"key": "h2h", "last_update": "2026-09-10T12:30:10Z",
                                   "outcomes": outcomes}]})
    return {"id": "event-1", "commence_time": "2026-09-10T23:00:00Z",
            "home_team": "Home Club", "away_team": "Away Club", "bookmakers": books}


class LiveCaptureV4Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.ledger = self.base / "ledger.sqlite3"
        self.runtime = self.base / "runtime"
        self.freeze = self.base / "freeze.json"
        self.freeze.write_text("{}\n")

    def tearDown(self):
        self.tmp.cleanup()

    def execute(self, response, times=None, run="run-1"):
        calls = []
        values = iter(times or ["2026-09-10T12:30:20Z", "2026-09-10T12:30:21Z"])

        def getter(url, params, timeout):
            calls.append((url, params, timeout))
            return response

        with patch.dict(os.environ, {"ODDS_API_KEY": "unit-test-secret"}):
            result = capture.execute_capture(game_date="2026-09-10", run_identity=run,
                mode="LIVE", snapshot=snapshot(), ledger=self.ledger, runtime=self.runtime,
                freeze_path=self.freeze, getter=getter, clock=lambda: next(values))
        return result, calls

    def test_success_preserves_raw_safe_params_quota_and_pregame_prices(self):
        result, calls = self.execute(Response([event()]))
        self.assertEqual(result["status"], "SUCCESS_VALID_CAPTURE")
        self.assertEqual(len(calls), 1)
        _, params, _ = calls[0]
        self.assertEqual(params["markets"], "h2h")
        self.assertEqual(params["bookmakers"].split(","), list(capture.BOOKS))
        self.assertNotIn("regions", params)
        files = list(self.runtime.rglob("*.json"))
        self.assertEqual(len(files), 3)
        persisted = "\n".join(path.read_text() for path in files)
        self.assertNotIn("unit-test-secret", persisted)
        self.assertIn('"x-requests-last": "1"', persisted)
        with sqlite3.connect(self.ledger) as conn:
            risk = conn.execute("SELECT risk_state,requested_timestamp_utc,returned_snapshot_timestamp_utc FROM risk_set").fetchone()
            prices = conn.execute("SELECT COUNT(*) FROM bookmaker_prices").fetchone()[0]
            event_row = conn.execute("SELECT request_started_at_utc,response_received_at_utc,x_requests_last FROM live_capture_events_v4 WHERE status='SUCCESS_RESPONSE_PRESERVED'").fetchone()
        self.assertEqual(risk, ("MARKET_STRONG_MODEL_AGREES", "2026-09-10T12:30:20Z", "2026-09-10T12:30:21Z"))
        self.assertEqual(prices, 10)
        self.assertEqual(event_row, ("2026-09-10T12:30:20Z", "2026-09-10T12:30:21Z", "1"))

    def test_idempotency_blocks_a_second_charged_call_for_date(self):
        first, calls = self.execute(Response([event()]))
        second, second_calls = self.execute(Response([event()]), run="run-2")
        self.assertEqual(first["status"], "SUCCESS_VALID_CAPTURE")
        self.assertEqual(second["status"], "SKIPPED_PRIOR_ATTEMPT")
        self.assertEqual(len(calls), 1)
        self.assertEqual(second_calls, [])

    def test_live_timing_fails_closed_without_reading_credential_or_calling(self):
        with patch.dict(os.environ, {}, clear=True):
            result = capture.execute_capture(game_date="2026-09-10", run_identity="late", mode="LIVE",
                snapshot=snapshot(), ledger=self.ledger, runtime=self.runtime, freeze_path=self.freeze,
                getter=lambda *_: self.fail("network called"), clock=lambda: "2026-09-10T12:35:00.001Z")
        self.assertEqual(result["status"], "LIVE_TIMING_FAIL_CLOSED")
        self.assertFalse(result["charged_request"])

    def test_successful_incomplete_response_is_classified_and_not_recoverable(self):
        result, _ = self.execute(Response([event("absent")]))
        self.assertEqual(result["status"], "SUCCESS_VALID_CAPTURE")
        with sqlite3.connect(self.ledger) as conn:
            state = conn.execute("SELECT risk_state FROM risk_set").fetchone()[0]
        self.assertEqual(state, "REFERENCE_BOOKMAKER_ABSENT")
        recovery = capture.execute_capture(game_date="2026-09-10", run_identity="manual",
            mode="HISTORICAL_RECOVERY", snapshot=snapshot(), ledger=self.ledger,
            runtime=self.runtime, freeze_path=self.freeze, getter=lambda *_: self.fail("network called"))
        self.assertEqual(recovery["status"], "RECOVERY_NOT_ELIGIBLE")

    def test_http_failure_is_redacted_and_allows_one_historical_recovery(self):
        failed, _ = self.execute(Response({"error_code": "BAD_REQUEST", "message": "no"}, status=500, last="1"))
        self.assertEqual(failed["status"], "HTTP_ERROR")
        historical = {"timestamp": "2026-09-10T12:30:19Z", "data": [event()]}
        calls = []
        with patch.dict(os.environ, {"ODDS_API_KEY": "recovery-secret"}):
            recovered = capture.execute_capture(game_date="2026-09-10", run_identity="manual",
                mode="HISTORICAL_RECOVERY", snapshot=snapshot(), ledger=self.ledger, runtime=self.runtime,
                freeze_path=self.freeze,
                getter=lambda url, params, timeout: (calls.append((url, params)) or Response(historical, last="10")),
                clock=lambda: "2026-09-10T18:00:00Z")
        self.assertEqual(recovered["status"], "SUCCESS_VALID_CAPTURE")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["date"], "2026-09-10T12:30:20Z")
        with sqlite3.connect(self.ledger) as conn:
            mode = conn.execute("SELECT capture_mode FROM capture_provenance_v4").fetchone()[0]
        self.assertEqual(mode, "HISTORICAL_RECOVERY")

    def test_invalid_or_missing_quota_cost_stops_future_dates(self):
        result, _ = self.execute(Response([event()], last=""))
        self.assertEqual(result["status"], "SUCCESS_COST_MISMATCH")
        other = snapshot("2026-09-10T12:31:00Z")
        other = capture.PredictionSnapshot("2026-09-11", other.barrier_utc, 1,
            (("2026-09-11",) + other.rows[0][1:],))
        blocked = capture.execute_capture(game_date="2026-09-11", run_identity="run-2", mode="LIVE",
            snapshot=other, ledger=self.ledger, runtime=self.runtime, freeze_path=self.freeze,
            getter=lambda *_: self.fail("network called"), clock=lambda: "2026-09-10T12:31:10Z")
        self.assertEqual(blocked["status"], "QUOTA_COST_UNKNOWN_STOP")

    def test_transport_exception_is_sanitized(self):
        def failure(url, params, timeout):
            raise RuntimeError(f"failed {url}?apiKey={params['apiKey']}")
        with patch.dict(os.environ, {"ODDS_API_KEY": "trace-secret"}):
            result = capture.execute_capture(game_date="2026-09-10", run_identity="run", mode="LIVE",
                snapshot=snapshot(), ledger=self.ledger, runtime=self.runtime, freeze_path=self.freeze,
                getter=failure, clock=lambda: "2026-09-10T12:30:20Z")
        self.assertEqual(result["status"], "TRANSPORT_ERROR")
        with sqlite3.connect(self.ledger) as conn:
            error = conn.execute("SELECT error FROM live_capture_events_v4 WHERE status='TRANSPORT_ERROR'").fetchone()[0]
        self.assertNotIn("trace-secret", error)
        self.assertIn("apiKey=[REDACTED]", error)

    def test_fourth_historical_recovery_is_stopped_before_network(self):
        with sqlite3.connect(self.ledger) as conn:
            conn.row_factory = sqlite3.Row
            capture.schema(conn); capture.establish_authorization(conn, self.freeze)
            for offset, game_date in enumerate(("2026-09-10", "2026-09-11", "2026-09-12")):
                started = f"{game_date}T12:30:00Z"
                capture.claim(conn, game_date, "HISTORICAL_RECOVERY", f"recovery-{offset}", started, started)
                capture.append_event(conn, game_date=game_date, run_identity=f"recovery-{offset}",
                    capture_mode="HISTORICAL_RECOVERY", status="SUCCESS_RESPONSE_PRESERVED",
                    prediction_barrier_utc=started, request_started_at_utc=started,
                    x_requests_last="10")
            target = "2026-09-13"
            capture.claim(conn, target, "LIVE", "live-failed", f"{target}T12:29:00Z", f"{target}T12:30:00Z")
            capture.append_event(conn, game_date=target, run_identity="live-failed", capture_mode="LIVE",
                status="HTTP_ERROR", prediction_barrier_utc=f"{target}T12:29:00Z",
                request_started_at_utc=f"{target}T12:30:00Z", x_requests_last="1")
            conn.commit()
        result = capture.execute_capture(game_date="2026-09-13", run_identity="fourth", mode="HISTORICAL_RECOVERY",
            snapshot=capture.PredictionSnapshot("2026-09-13", "2026-09-13T12:29:00Z", 1, tuple()),
            ledger=self.ledger, runtime=self.runtime, freeze_path=self.freeze,
            getter=lambda *_: self.fail("network called"), clock=lambda: "2026-09-13T18:00:00Z")
        self.assertEqual(result["status"], "RECOVERY_COUNT_CEILING_STOP")
        self.assertFalse(result["charged_request"])


if __name__ == "__main__":
    unittest.main()
