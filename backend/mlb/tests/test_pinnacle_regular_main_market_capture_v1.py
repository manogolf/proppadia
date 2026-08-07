from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import append_market, build_consensus, connect_ledger
from backend.mlb.markets.pinnacle_main_market_capture_v1 import (
    BOOKMAKER_KEY, MARKETS, PROVIDER, RUN_LINE_MODEL_STATUS, bind_event, parse_events,
)
from backend.mlb.scripts import capture_mlb_pinnacle_main_markets_v1 as runner

FETCH = "2026-08-07T18:00:00Z"
START = "2026-08-07T22:00:00Z"


def schedule(start=START):
    return [{"game_pk": 10, "away_team_name": "Boston Red Sox", "home_team_name": "New York Yankees",
             "scheduled_start_utc": start, "game_number": 1}]


def event(start=START):
    return {"id": "evt", "away_team": "Boston Red Sox", "home_team": "New York Yankees", "commence_time": start,
            "bookmakers": [{"key": "pinnacle", "title": "Pinnacle", "markets": [
                {"key": "h2h", "last_update": "2026-08-07T17:59:45Z", "outcomes": [{"name": "Boston Red Sox", "price": 120}, {"name": "New York Yankees", "price": -130}]},
                {"key": "totals", "last_update": "2026-08-07T17:59:44Z", "outcomes": [{"name": "Over", "point": 8.5, "price": -105}, {"name": "Under", "point": 8.5, "price": -115}]},
                {"key": "spreads", "last_update": "2026-08-07T17:59:43Z", "outcomes": [{"name": "Boston Red Sox", "point": 1.5, "price": -150}, {"name": "New York Yankees", "point": -1.5, "price": 130}]},
            ]}]}


def parsed():
    return parse_events(events=[event()], schedule=schedule(), game_date="2026-08-07", fetched_at_utc=FETCH,
                        run_tag="regular", raw_source_path="raw.json", raw_source_sha256="abc")


def test_01_explicit_pinnacle_contract():
    assert PROVIDER == "THE_ODDS_API" and BOOKMAKER_KEY == "pinnacle" and MARKETS == ("h2h", "totals", "spreads")


def test_02_exact_identity():
    game, status, ids = bind_event(event(), schedule(), FETCH)
    assert game["game_pk"] == 10 and status == "CERTIFIED_EXACT_OR_DETERMINISTIC" and ids == [10]


def test_03_ambiguous_identity_fails_closed():
    game, status, _ = bind_event(event(), schedule() * 2, FETCH)
    assert game is None and status == "AMBIGUOUS"


def test_04_post_start_rejected():
    rows, audit = parse_events(events=[event("2026-08-07T17:00:00Z")], schedule=schedule("2026-08-07T17:00:00Z"), game_date="2026-08-07", fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    assert rows == [] and audit[0]["certification_status"] == "POST_START"


def test_05_moneyline_normalized():
    row = next(x for x in parsed()[0] if x["market_type"] == "MONEYLINE")
    assert row["away_raw_american_price"] == "120" and row["home_decimal_price"] == pytest.approx(1.7692307)


def test_06_moneyline_no_vig_sums_to_one():
    row = next(x for x in parsed()[0] if x["market_type"] == "MONEYLINE")
    assert row["no_vig_away_probability"] + row["no_vig_home_probability"] == pytest.approx(1)


def test_07_total_pair_and_line():
    row = next(x for x in parsed()[0] if x["market_type"] == "FULL_GAME_TOTAL")
    assert row["total_line"] == 8.5 and row["over_american_price"] == -105 and row["under_american_price"] == -115


def test_08_run_line_exact_opposites():
    row = next(x for x in parsed()[0] if x["market_type"] == "RUN_LINE")
    assert row["away_spread"] == -row["home_spread"] and row["home_spread"] == -1.5


def test_09_provider_update_distinct_from_fetch():
    row = parsed()[0][0]
    assert row["provider_market_updated_at_utc"] != row["captured_at_utc"]


def test_10_only_pinnacle_admitted():
    value = event(); value["bookmakers"].append({"key": "fanduel", "markets": value["bookmakers"][0]["markets"]})
    rows, _ = parse_events(events=[value], schedule=schedule(), game_date="2026-08-07", fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    assert len(rows) == 3 and {row["bookmaker_key"] for row in rows} == {"pinnacle"}


def test_11_append_is_immutable_and_idempotent(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); row = parsed()[0][0]
    assert append_market(conn, row) == "APPENDED_NEW" and append_market(conn, row) == "EXISTING_IMMUTABLE"


def test_12_duplicate_identities_zero(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); [append_market(conn, row) for row in parsed()[0]]
    assert conn.execute("SELECT COUNT(*) FROM (SELECT canonical_market_identity FROM supplemental_main_market_snapshots GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0] == 0


def test_13_consensus_counts_pinnacle_once():
    rows = parsed()[0]; money = next(x for x in rows if x["market_type"] == "MONEYLINE")
    value = build_consensus(rows=[money, {**money, "provider": "X", "bookmaker_key": "other", "captured_at_utc": "2026-08-07T17:59:00Z"}], game_date="2026-08-07", game_id=10, market_type="MONEYLINE", captured_at_utc=FETCH)
    assert value["books_captured"] == 2 and value["bookmaker_keys"].count("pinnacle") == 1


def test_14_total_consensus_method_unchanged():
    total = next(x for x in parsed()[0] if x["market_type"] == "FULL_GAME_TOTAL")
    value = build_consensus(rows=[total, {**total, "provider": "X", "bookmaker_key": "other", "total_line": 9.5}], game_date="2026-08-07", game_id=10, market_type="FULL_GAME_TOTAL", captured_at_utc=FETCH)
    assert value["median_line"] == 9.0 and value["books_at_consensus_line"] == 0


def test_15_run_line_model_unavailable():
    assert RUN_LINE_MODEL_STATUS == "MODEL_COMPARISON_UNAVAILABLE_NO_QUALIFIED_RUN_LINE_MODEL"


def test_16_request_has_no_region_or_props(monkeypatch, tmp_path):
    seen = {}
    class Response:
        content = b"[]"; status_code = 200; headers = {"x-requests-last": "3"}
        def raise_for_status(self): pass
        def json(self): return []
    monkeypatch.setenv("ODDS_API_KEY", "secret"); monkeypatch.setattr(runner, "RAW_ROOT", tmp_path)
    monkeypatch.setattr(runner.requests, "get", lambda url, params, timeout: (seen.update(params) or Response()))
    runner.fetch("2026-08-07", "run")
    assert seen["bookmakers"] == "pinnacle" and seen["markets"] == "h2h,totals,spreads" and "regions" not in seen and "apiKey" in seen


def test_17_manifest_redacts_key_and_preserves_cost(monkeypatch, tmp_path):
    class Response:
        content = b"[]"; status_code = 200; headers = {"x-requests-last": "3", "x-requests-used": "9", "x-requests-remaining": "91"}
        def raise_for_status(self): pass
        def json(self): return []
    monkeypatch.setenv("ODDS_API_KEY", "do-not-store"); monkeypatch.setattr(runner, "RAW_ROOT", tmp_path)
    monkeypatch.setattr(runner.requests, "get", lambda *a, **k: Response())
    _, manifest = runner.fetch("2026-08-07", "run")
    assert manifest["request_cost_headers"]["x-requests-last"] == "3"
    assert "do-not-store" not in Path(tmp_path / "2026-08-07/odds_mlb_pinnacle_main_markets__run.manifest.json").read_text()


def test_18_existing_hook_keeps_broad_request_and_adds_independent_pinnacle():
    hook = (runner.ROOT / "bin/mlb_full_game_totals_daily_hook.sh").read_text()
    assert "capture_mlb_full_game_totals_v1" in hook and "capture_mlb_pinnacle_main_markets_v1" in hook
    assert hook.index("capture_mlb_full_game_totals_v1") < hook.index("capture_mlb_pinnacle_main_markets_v1")


def test_19_current_date_event_classified_current_slate():
    _, audit = parsed()
    assert audit[0]["event_classification"] == "CURRENT_SLATE"


def test_20_next_date_event_classified_future_slate():
    future = event("2026-08-08T22:00:00Z")
    future_schedule = schedule("2026-08-08T22:00:00Z")
    rows, audit = parse_events(events=[future], schedule=future_schedule, game_date="2026-08-07",
        fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    assert audit[0]["event_classification"] == "FUTURE_SLATE_PREGAME"
    assert {row["observation_timing_class"] for row in rows} == {"EARLY_FUTURE_SLATE_PREGAME_OBSERVATION"}
    assert {row["game_date"] for row in rows} == {"2026-08-08"}


def test_21_future_doubleheader_uses_game_number():
    future = event("2026-08-08T22:00:00Z"); future["game_number"] = 2
    games = schedule("2026-08-08T22:00:00Z")
    games += [{**games[0], "game_pk": 11, "game_number": 2}]
    game, status, _ = bind_event(future, games, FETCH)
    assert status == "CERTIFIED_EXACT_OR_DETERMINISTIC" and game["game_pk"] == 11


def test_22_started_event_classified_past_or_started():
    started = event("2026-08-07T17:00:00Z")
    _, audit = parse_events(events=[started], schedule=schedule("2026-08-07T17:00:00Z"), game_date="2026-08-07",
        fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    assert audit[0]["event_classification"] == "PAST_OR_STARTED"


def test_23_ambiguous_event_classified_ambiguous():
    _, audit = parse_events(events=[event()], schedule=schedule() * 2, game_date="2026-08-07",
        fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    assert audit[0]["event_classification"] == "IDENTITY_AMBIGUOUS"


def test_24_future_snapshot_does_not_enter_current_consensus(tmp_path):
    future = event("2026-08-08T22:00:00Z")
    rows, _ = parse_events(events=[future], schedule=schedule("2026-08-08T22:00:00Z"), game_date="2026-08-07",
        fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); [append_market(conn, row) for row in rows]
    assert conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots WHERE game_date='2026-08-07'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots WHERE game_date='2026-08-08'").fetchone()[0] == 3


def test_25_future_snapshot_remains_immutable(tmp_path):
    future = event("2026-08-08T22:00:00Z")
    rows, _ = parse_events(events=[future], schedule=schedule("2026-08-08T22:00:00Z"), game_date="2026-08-07",
        fetched_at_utc=FETCH, run_tag="x", raw_source_path="x", raw_source_sha256="x")
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    assert append_market(conn, rows[0]) == "APPENDED_NEW"
    assert append_market(conn, rows[0]) == "EXISTING_IMMUTABLE"
