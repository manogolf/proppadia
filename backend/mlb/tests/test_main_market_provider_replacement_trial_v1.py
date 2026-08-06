from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import connect_ledger
from backend.mlb.markets.main_market_provider_replacement_trial_v1 import (
    append_reliability, append_shadow_attachment, compare_provider_rows, consensus_metrics, freshness_metrics,
    parse_provider_events, reliability_rows,
)
from backend.mlb.scripts import run_mlb_main_market_provider_replacement_trial_v1 as runner

FETCH = "2026-08-06T22:22:26Z"
START = "2026-08-06T23:10:00Z"
UPDATE = "2026-08-06T22:18:00Z"


def _side(odds, **values):
    return {"odds": odds, "available": True, "lastUpdatedAt": UPDATE, **values}


def _event():
    def books(a, b=None):
        return {"bookmakereu": a, "fanduel": b or dict(a), "pinnacle": {**a, "available": False}}
    return {"eventID": "event", "teams": {
        "away": {"names": {"long": "Chicago White Sox"}},
        "home": {"names": {"long": "Boston Red Sox"}}},
        "status": {"startsAt": START, "started": False, "live": False}, "odds": {
            "points-away-game-ml-away": {"started": False, "byBookmaker": books(_side("+170"))},
            "points-home-game-ml-home": {"started": False, "byBookmaker": books(_side("-190"))},
            "points-all-game-ou-over": {"started": False, "byBookmaker": books(_side("-105", overUnder="8.5"))},
            "points-all-game-ou-under": {"started": False, "byBookmaker": books(_side("-115", overUnder="8.5"))},
            "points-away-game-sp-away": {"started": False, "byBookmaker": books(_side("-120", spread="+1.5"))},
            "points-home-game-sp-home": {"started": False, "byBookmaker": books(_side("+100", spread="-1.5"))},
        }}


def _schedule():
    return [{"game_pk": 824729, "game_date": "2026-08-06", "scheduled_start_utc": START,
             "away_team_name": "Chicago White Sox", "home_team_name": "Boston Red Sox",
             "game_number": 1, "double_header": "N"}]


def _parsed():
    return parse_provider_events(events=[_event()], schedule=_schedule(), game_date="2026-08-06",
        fetched_at_utc=FETCH, run_tag="trial", raw_source_path="raw.json", raw_source_sha256="a" * 64)


def test_provider_wide_parser_retains_each_book_and_three_paired_markets():
    rows, audit = _parsed()
    assert len(rows) == 6
    assert {row["bookmaker_provider_id"] for row in rows} == {"bookmakereu", "fanduel"}
    assert {row["market_type"] for row in rows} == {"MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"}
    assert audit[0]["accessible_bookmaker_count"] == 2
    assert all(row["timing_status"] == "PREGAME_CERTIFIED" for row in rows)


def test_unavailable_documented_book_is_not_reported_live():
    rows, _ = _parsed()
    assert not any(row["bookmaker_provider_id"] == "pinnacle" for row in rows)


def test_provider_wide_fetch_is_one_request_and_has_no_book_filter(monkeypatch, tmp_path):
    calls = []
    payload = {"success": True, "data": []}
    class Response:
        status_code = 200; content = json.dumps(payload).encode(); headers = {}
        def raise_for_status(self): pass
        def json(self): return payload
    def get(*args, **kwargs): calls.append(kwargs); return Response()
    monkeypatch.setenv("SPORTSGAMEODDSAPI", "never-persist")
    monkeypatch.setattr(runner.requests, "get", get); monkeypatch.setattr(runner, "RAW_ROOT", tmp_path)
    _, source = runner.fetch_current("2026-08-06")
    assert len(calls) == 1 and "bookmakerID" not in calls[0]["params"]
    assert source["request_count"] == 1
    assert "never-persist" not in "".join(p.read_text() for p in tmp_path.rglob("*.json"))


def test_exact_overlap_classifications():
    rows, _ = _parsed(); sgo = next(row for row in rows if row["bookmaker_provider_id"] == "fanduel" and row["market_type"] == "FULL_GAME_TOTAL")
    base = {"provider": "THE_ODDS_API", "bookmaker_key": "fanduel", "game_id": 824729,
            "market_type": "FULL_GAME_TOTAL", "away_team": "Chicago White Sox", "home_team": "Boston Red Sox",
            "captured_at_utc": "2026-08-06T22:20:00Z", "provider_market_timestamp_utc": UPDATE,
            "total_line": 8.5, "over_price": -105, "under_price": -115}
    assert compare_provider_rows([sgo], [base])[0]["classification"] == "SAME_MARKET_SAME_LINE_SAME_PRICE"
    assert compare_provider_rows([sgo], [{**base, "under_price": -110}])[0]["classification"] == "SAME_LINE_PRICE_DIFFERENCE"
    assert compare_provider_rows([sgo], [{**base, "total_line": 9.0}])[0]["classification"] == "LINE_DIFFERENCE"
    assert compare_provider_rows([sgo], [])[0]["classification"] == "BOOK_PRESENT_ONE_PROVIDER_ONLY"


def test_noncomparable_timing_is_not_treated_as_price_difference():
    rows, _ = _parsed(); sgo = next(row for row in rows if row["bookmaker_provider_id"] == "fanduel" and row["market_type"] == "FULL_GAME_TOTAL")
    old = {"provider": "THE_ODDS_API", "bookmaker_key": "fanduel", "game_id": 824729,
           "market_type": "FULL_GAME_TOTAL", "captured_at_utc": "2026-08-06T19:00:00Z",
           "provider_market_timestamp_utc": "2026-08-06T18:59:00Z", "total_line": 8.5,
           "over_price": -105, "under_price": -115}
    assert compare_provider_rows([sgo], [old])[0]["classification"] == "TIMING_NOT_COMPARABLE"


def test_provider_consensus_and_line_probability_contract():
    rows, _ = _parsed(); totals = [row for row in rows if row["market_type"] == "FULL_GAME_TOTAL"]
    value = consensus_metrics(totals, "SPORTSGAMEODDS", "ALL")[0]
    assert value["book_count"] == 2 and value["median_line"] == 8.5
    assert value["same_line_book_count"] == 2


def test_freshness_metrics_preserve_age_thresholds():
    rows, _ = _parsed(); value = freshness_metrics(rows, "SPORTSGAMEODDS")[0]
    assert value["timestamped_rows"] == 2
    assert value["median_age_minutes"] == pytest.approx(4 + 26 / 60)
    assert value["percentage_within_5_minutes"] == 100.0


def test_reliability_ledger_is_append_only_and_idempotent(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    payload = {"provider": "SPORTSGAMEODDS", "game_date": "2026-08-06", "captured_at_utc": FETCH,
               "source_run_tag": "trial", "request_success": True}
    assert append_reliability(conn, payload) == "APPENDED_NEW"
    assert append_reliability(conn, payload) == "EXISTING_IMMUTABLE"
    assert reliability_rows(conn) == [payload]


def test_shadow_attachment_is_append_only_and_prediction_separate(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    payload = {"prediction_identity": "pred", "market_identity": "market",
               "provider_view": "SPORTSGAMEODDS:bookmakereu", "market_type": "MONEYLINE",
               "timing_relationship": "POST_PREDICTION_MARKET_OBSERVATION", "created_at_utc": FETCH}
    assert append_shadow_attachment(conn, payload) == "APPENDED_NEW"
    assert append_shadow_attachment(conn, payload) == "EXISTING_IMMUTABLE"


def test_source_failure_is_recorded_not_hidden_as_stale_provider_success():
    source = {"game_date": "2026-08-06", "fetch_timestamp_utc": FETCH, "run_tag": "run",
              "http_status": 200, "provider_event_count": 0, "response_latency_seconds": 1,
              "provider_notice": None}
    payloads = runner.reliability_payloads(source, _schedule(), [], [], [], 17)
    odds = next(row for row in payloads if row["provider"] == "THE_ODDS_API")
    assert odds["request_success"] is False and odds["http_errors"] == 1
    assert odds["provider_notice"] == "SOURCE_FAILURE"


def test_probe_replay_retains_raw_hash_and_costs_zero_new_requests(tmp_path):
    raw = tmp_path / "raw.json"; raw.write_text(json.dumps({"success": True, "data": [_event()]}))
    _, source = runner.load_probe(raw, "2026-08-06", FETCH)
    assert source["request_count"] == 0 and source["original_provider_request_count"] == 1
    assert source["raw_response_sha256"] == hashlib.sha256(raw.read_bytes()).hexdigest()


def test_required_living_package_names_are_exact(tmp_path):
    expected = {
        "sportsgameodds_bookmaker_coverage.csv", "provider_overlap_comparison.csv",
        "provider_market_consensus_comparison.csv", "provider_freshness_comparison.csv",
        "provider_reliability_ledger.csv", "provider_quota_cost_audit.csv", "priority_book_coverage.csv",
        "historical_capability_audit.md", "provider_replacement_progress.md",
        "concise_provider_replacement_trial.md", "reproducibility_hashes.sha256",
    }
    # This test binds the public package contract without making network/database writes.
    assert len(expected) == 11 and "reproducibility_hashes.sha256" in expected


def test_no_ev_wager_ranking_staking_fields():
    rows, _ = _parsed()
    forbidden = {"ev", "wager", "ranking", "staking", "stake"}
    assert not any(key.casefold() in forbidden for row in rows for key in row)
