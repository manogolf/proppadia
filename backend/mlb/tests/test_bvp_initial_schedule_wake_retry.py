"""Offline initial-schedule contract tests: no network, actual sleep or database."""
import errno
import json
import queue
import socket
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
import requests

from backend.mlb.scripts import refresh_mlb_bvp_pvb as bvp


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(bvp.requests, "get", Mock(side_effect=AssertionError("LIVE_NETWORK_FORBIDDEN")))
    monkeypatch.setattr(bvp, "pg_connect", Mock(side_effect=AssertionError("DATABASE_FORBIDDEN")))


@pytest.fixture
def clock(monkeypatch):
    value = Clock()
    monkeypatch.setattr(bvp.time, "monotonic", lambda: value.now)
    monkeypatch.setattr(bvp.time, "sleep", value.sleep)
    class FixtureDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            stamp = datetime(2026, 9, 18, 10, 39, 34, tzinfo=timezone.utc) + timedelta(seconds=value.now)
            return stamp.astimezone(tz) if tz is not None else stamp.replace(tzinfo=None)

    monkeypatch.setattr(bvp, "datetime", FixtureDatetime)
    return value


def response(payload=None, *, status=200, parser_error=None):
    value = Mock()
    value.raise_for_status.side_effect = requests.HTTPError("secret/private-address") if status >= 400 else None
    value.json.side_effect = parser_error
    value.json.return_value = {"dates": []} if payload is None else payload
    return value


def transient_dns():
    return requests.ConnectionError(socket.gaierror(socket.EAI_NONAME, "secret/private-address"))


def events(capsys):
    return [json.loads(line.split("[bvp-refresh] ", 1)[1])
            for line in capsys.readouterr().err.splitlines()]


def fetch():
    return bvp._fetch_schedule_games("2026-09-18", timeout_sec=20, retries=3)


def test_first_attempt_uses_governed_request_and_preserves_payload(clock, monkeypatch, capsys):
    payload = {"dates": [{"games": [{"gamePk": 123, "teams": {
        "home": {"team": {"id": 1}, "probablePitcher": {"id": 11}},
        "away": {"team": {"id": 2}, "probablePitcher": {"id": 22}},
    }}]}]}
    reply = response(payload)
    request = Mock(return_value=reply)
    monkeypatch.setattr(bvp.requests, "get", request)
    assert fetch() == [bvp.GameRow(123, "2026-09-18", 1, 2, 11, 22)]
    assert request.call_count == 1
    assert request.call_args.args == (
        "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=2026-09-18&hydrate=probablePitcher",)
    assert request.call_args.kwargs["stream"] is True
    assert request.call_args.kwargs["timeout"].total == 20
    assert not clock.waits
    reply.close.assert_called_once()
    assert events(capsys)[0]["status"] == "INITIAL_SCHEDULE_REQUEST_SUCCESS"


@pytest.mark.parametrize("failure,classification", [
    (transient_dns(), "DNS_NAME_RESOLUTION_FAILURE"),
    (requests.ConnectionError(OSError(errno.ENETUNREACH, "private-address")), "CONNECTION_ESTABLISHMENT_FAILURE"),
    (requests.ConnectionError(OSError(errno.EHOSTUNREACH, "private-address")), "CONNECTION_ESTABLISHMENT_FAILURE"),
    (requests.ConnectTimeout("secret"), "PRE_RESPONSE_TIMEOUT"),
    (requests.ReadTimeout("secret"), "PRE_RESPONSE_TIMEOUT"),
])
def test_transient_then_success(clock, monkeypatch, capsys, failure, classification):
    request = Mock(side_effect=[failure, response()])
    monkeypatch.setattr(bvp.requests, "get", request)
    assert fetch() == []
    assert clock.waits == [10.0]
    assert request.call_count == 2
    assert request.call_args.kwargs["timeout"].total == 5
    failed, succeeded = events(capsys)
    assert failed["classification"] == classification
    assert failed["http_response_received"] is False
    assert failed["attempt"] == 1 and failed["next_wait_sec"] == 10
    assert succeeded["status"] == "ACQUISITION_SUCCESS_AFTER_TRANSIENT_NETWORK_RETRY"
    assert succeeded["attempts_used"] == 2 and succeeded["total_retry_delay_sec"] == 10
    assert succeeded["first_failure_timestamp_utc"] == failed["timestamp_utc"]
    assert succeeded["successful_attempt_timestamp_utc"]


def test_september18_readiness_at_7_4_seconds_succeeds_exactly_once(clock, monkeypatch):
    successful = []

    def request(*args, **kwargs):
        if clock.now < 7.4:
            raise transient_dns()
        successful.append(clock.now)
        return response()

    monkeypatch.setattr(bvp.requests, "get", request)
    assert fetch() == []
    assert successful == [10.0]
    assert clock.waits == [10.0]  # Old path exhausted at about 4.7s.


def test_exhaustion_waits_30_seconds_and_stays_nonzero(clock, monkeypatch, capsys):
    request = Mock(side_effect=transient_dns())
    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError, match="ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED"):
        fetch()
    assert request.call_count == 3
    assert clock.waits == [10.0, 20.0] and clock.now == 30
    rows = events(capsys)
    assert [item["attempt"] for item in rows[:-1]] == [1, 2, 3]
    assert rows[-1]["status"] == "ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED"
    bvp.pg_connect.assert_not_called()


def test_nominal_slowest_attempts_fit_60_second_deadline(clock, monkeypatch):
    timeouts = []

    def request(*args, **kwargs):
        seconds = kwargs["timeout"].total
        timeouts.append(seconds)
        clock.now += seconds
        raise requests.ConnectTimeout()

    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    assert timeouts == [20, 5, 5]
    assert clock.now == 60 and sum(clock.waits) == 30


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500, 503])
def test_http_errors_never_retry(clock, monkeypatch, capsys, status):
    reply = response(status=status)
    request = Mock(return_value=reply)
    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    assert request.call_count == 1 and not clock.waits
    assert events(capsys)[0]["http_response_received"] is True
    reply.json.assert_not_called()
    reply.close.assert_called_once()


@pytest.mark.parametrize("payload", [[], {}, {"dates": "invalid"}])
def test_semantic_schema_failure_never_retries(clock, monkeypatch, payload):
    request = Mock(return_value=response(payload))
    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    assert request.call_count == 1 and not clock.waits


@pytest.mark.parametrize("error", [ValueError("secret"), requests.ReadTimeout("secret")])
def test_parser_or_body_timeout_after_response_never_retries(clock, monkeypatch, error):
    request = Mock(return_value=response(parser_error=error))
    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    assert request.call_count == 1 and not clock.waits


@pytest.mark.parametrize("error", [
    requests.exceptions.SSLError("secret"), requests.exceptions.ProxyError("secret"),
    requests.ConnectionError("unclassified secret"), TypeError("programming secret"),
])
def test_unclassified_security_or_programming_failure_fails_closed(clock, monkeypatch, error):
    request = Mock(side_effect=error)
    monkeypatch.setattr(bvp.requests, "get", request)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    assert request.call_count == 1 and not clock.waits


def test_stuck_header_request_never_overlaps_another_request(clock, monkeypatch, capsys):
    monkeypatch.setattr(threading.Thread, "start", lambda self: None)
    monkeypatch.setattr(queue.Queue, "get", Mock(side_effect=queue.Empty))
    monkeypatch.setattr(queue.Queue, "get_nowait", Mock(side_effect=queue.Empty))
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        fetch()
    row = events(capsys)[0]
    assert row["classification"] == "UNFINISHED_REQUEST_FAIL_CLOSED"
    assert row["http_response_received"] == "UNKNOWN"
    assert not clock.waits
    bvp.requests.get.assert_not_called()


def test_uninterruptible_worker_is_not_retried_and_closes_late_response(clock, monkeypatch):
    started, release, closed = threading.Event(), threading.Event(), threading.Event()
    reply = response()
    reply.close.side_effect = closed.set

    def blocked_request(*args, **kwargs):
        started.set()
        assert release.wait(timeout=2)
        return reply

    class DeadlineQueue:
        def __init__(self, **kwargs):
            pass

        def get(self, **kwargs):
            assert started.wait(timeout=2)
            raise queue.Empty

        def get_nowait(self):
            raise queue.Empty

        def put(self, item):
            pass

    request = Mock(side_effect=blocked_request)
    monkeypatch.setattr(bvp.requests, "get", request)
    monkeypatch.setattr(queue, "Queue", DeadlineQueue)
    try:
        with pytest.raises(bvp.InitialScheduleAcquisitionError):
            fetch()
        assert request.call_count == 1
        assert not clock.waits
    finally:
        release.set()
        assert closed.wait(timeout=2)


def test_write_failure_not_retried_and_no_duplicate_prepared_rows(clock, monkeypatch):
    request = Mock(side_effect=[transient_dns(), response()])
    monkeypatch.setattr(bvp.requests, "get", request)
    build = bvp._build_rows_for_date
    row = ("hits", 10, 20, "2026-09-18", {"bvp_hits": 1}, "v1", "bvp_pvb_refresh_v1")

    def prepare(*args, **kwargs):
        rows, counters = build(*args, **kwargs)
        assert rows == []
        return [row], defaultdict(int, games=1, rows=1)

    monkeypatch.setattr(bvp, "_map_games_to_local_game_ids", lambda games, date: (games, {}))
    monkeypatch.setattr(bvp, "_augment_games_with_db_starters", lambda games, date: (games, {}))
    monkeypatch.setattr(bvp, "_build_rows_for_date", prepare)
    write = Mock(side_effect=RuntimeError("DATABASE_WRITE_FAILURE"))
    monkeypatch.setattr(bvp, "_upsert_rows", write)
    with pytest.raises(RuntimeError, match="DATABASE_WRITE_FAILURE"):
        bvp.main(["--date", "2026-09-18"])
    assert request.call_count == 2
    write.assert_called_once_with([row], batch_size=1000)


def test_exhaustion_does_not_reach_mapping_or_writes(clock, monkeypatch):
    monkeypatch.setattr(bvp.requests, "get", Mock(side_effect=transient_dns()))
    mapping, write = Mock(), Mock()
    monkeypatch.setattr(bvp, "_map_games_to_local_game_ids", mapping)
    monkeypatch.setattr(bvp, "_upsert_rows", write)
    with pytest.raises(bvp.InitialScheduleAcquisitionError):
        bvp.main(["--date", "2026-09-18"])
    mapping.assert_not_called()
    write.assert_not_called()


def test_successful_retry_admits_one_row_once_with_original_date(clock, monkeypatch):
    monkeypatch.setattr(bvp.requests, "get", Mock(side_effect=[transient_dns(), response()]))
    build = bvp._build_rows_for_date
    row = ("hits", 10, 20, "2026-09-18", {"bvp_hits": 1}, "v1", "bvp_pvb_refresh_v1")

    def prepare(*args, **kwargs):
        assert args[0] == "2026-09-18"
        assert build(*args, **kwargs)[0] == []
        return [row], defaultdict(int, games=1, rows=1)

    monkeypatch.setattr(bvp, "_map_games_to_local_game_ids", lambda games, date: (games, {}))
    monkeypatch.setattr(bvp, "_augment_games_with_db_starters", lambda games, date: (games, {}))
    monkeypatch.setattr(bvp, "_build_rows_for_date", prepare)
    write = Mock(return_value=1)
    monkeypatch.setattr(bvp, "_upsert_rows", write)
    assert bvp.main(["--date", "2026-09-18"]) == 0
    write.assert_called_once_with([row], batch_size=1000)
    assert bvp.requests.get.call_count == 2


def test_telemetry_and_terminal_error_do_not_echo_secrets(clock, monkeypatch, capsys):
    secrets = "apikey=VERY_SECRET 192.168.91.23 aa:bb:cc:dd:ee:ff password=PRIVATE"
    monkeypatch.setattr(bvp.requests, "get", Mock(side_effect=requests.ConnectionError(socket.gaierror(8, secrets))))
    with pytest.raises(bvp.InitialScheduleAcquisitionError) as caught:
        fetch()
    output = capsys.readouterr().err + str(caught.value)
    for secret in ("VERY_SECRET", "192.168", "aa:bb", "PRIVATE", "apikey", "password"):
        assert secret not in output


def test_other_acquisition_reads_keep_original_retry_contract(clock, monkeypatch):
    reply = response({"unrelated": True})
    request = Mock(side_effect=[requests.HTTPError(), reply])
    monkeypatch.setattr(bvp.requests, "get", request)
    assert bvp._fetch_json("https://statsapi.mlb.com/unrelated", timeout_sec=20, retries=3) == {"unrelated": True}
    assert clock.waits == [1.5]
    assert request.call_args.kwargs == {"timeout": 20}
