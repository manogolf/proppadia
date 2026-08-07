from __future__ import annotations

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    append_event_discovery, append_market, connect_ledger, mark_first_observed_prices,
)
from backend.mlb.markets.full_game_total_capture_v1 import attach_all_markets, connect_ledger as connect_total_ledger
from backend.mlb.markets.pinnacle_main_market_capture_v1 import parse_events
from backend.mlb.scripts.report_mlb_future_slate_main_markets_v1 import report_rows
from backend.mlb.tests.test_pinnacle_regular_main_market_capture_v1 import FETCH, event, schedule


def future_rows(captured=FETCH):
    return parse_events(events=[event("2026-08-08T22:00:00Z")], schedule=schedule("2026-08-08T22:00:00Z"),
        game_date="2026-08-07", fetched_at_utc=captured, run_tag="x", raw_source_path="raw", raw_source_sha256="sha")


def discovery(priced=False):
    return {"provider": "THE_ODDS_API", "provider_event_id": "evt", "game_date": "2026-08-08",
        "game_id": 10, "captured_at_utc": FETCH, "scheduled_start_utc": "2026-08-08T22:00:00Z",
        "event_classification": "FUTURE_SLATE_PREGAME", "raw_source_path": "raw", "raw_source_sha256": "sha",
        "main_market_prices_present": priced, "bookmaker_scope": "pinnacle", "matchup": "Boston Red Sox @ New York Yankees"}


def test_01_unpriced_future_event_creates_identity_only(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    assert append_event_discovery(conn, discovery()) == "APPENDED_NEW"
    assert conn.execute("SELECT COUNT(*) FROM main_market_event_discoveries").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots").fetchone()[0] == 0


def test_02_later_prices_create_first_priced_observation(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); append_event_discovery(conn, discovery())
    rows = mark_first_observed_prices(conn, future_rows()[0])
    assert all(row["price_observation_class"] == "FIRST_PROPPADIA_OBSERVED_PREGAME_LINE" for row in rows)
    assert all(append_market(conn, row) == "APPENDED_NEW" for row in rows)


def test_03_first_priced_observation_is_immutable(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); row = mark_first_observed_prices(conn, future_rows()[0])[0]
    assert append_market(conn, row) == "APPENDED_NEW" and append_market(conn, row) == "EXISTING_IMMUTABLE"


def test_04_later_snapshot_appends(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); first = mark_first_observed_prices(conn, future_rows()[0])
    [append_market(conn, row) for row in first]
    later, _ = future_rows("2026-08-07T19:00:00Z"); later = mark_first_observed_prices(conn, later)
    assert all(row["price_observation_class"] == "LATER_PROPPADIA_OBSERVED_PREGAME_LINE" for row in later)
    assert all(append_market(conn, row) == "APPENDED_NEW" for row in later)


def test_05_future_rows_do_not_exist_under_current_date(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); [append_market(conn, row) for row in mark_first_observed_prices(conn, future_rows()[0])]
    assert conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots WHERE game_date='2026-08-07'").fetchone()[0] == 0


def test_06_game_becomes_current_without_losing_prior_observations(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); [append_market(conn, row) for row in mark_first_observed_prices(conn, future_rows()[0])]
    assert conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots WHERE game_date='2026-08-08'").fetchone()[0] == 3


def _prediction():
    return {"game_date": "2026-08-08", "game_pk": 10, "model_version": "v", "prediction_snapshot_class": "PRIMARY",
            "prediction_timestamp_utc": "2026-08-08T12:30:00Z"}


def _total(identity, captured):
    return {"canonical_market_identity": identity, "game_id": 10, "bookmaker_key": "pinnacle",
            "captured_at_utc": captured, "scheduled_start_utc": "2026-08-08T22:00:00Z", "total_line": 8.5}


def test_07_model_attachment_selects_latest_at_or_before_prediction(tmp_path):
    conn = connect_total_ledger(tmp_path / "ledger.sqlite3")
    attached = attach_all_markets(conn, _prediction(), [_total("early", "2026-08-07T19:00:00Z"), _total("latest", "2026-08-08T12:00:00Z")], FETCH)
    latest = next(row for row in attached if row["canonical_market_identity"] == "latest")
    assert "LATEST_CERTIFIED_AT_OR_BEFORE_PREDICTION" in latest["model_time_attachment_roles"]


def test_08_post_prediction_snapshot_remains_later(tmp_path):
    conn = connect_total_ledger(tmp_path / "ledger.sqlite3")
    attached = attach_all_markets(conn, _prediction(), [_total("later", "2026-08-08T13:00:00Z")], FETCH)
    assert attached[0]["timing_relationship"] == "POST_PREDICTION_MARKET_OBSERVATION"
    assert "LATER_POST_PREDICTION_PREGAME_OBSERVATION" in attached[0]["model_time_attachment_roles"]


def test_09_doubleheaders_remain_distinct():
    future = event("2026-08-08T22:00:00Z"); future["game_number"] = 2
    games = schedule("2026-08-08T22:00:00Z") + [{**schedule("2026-08-08T22:00:00Z")[0], "game_pk": 11, "game_number": 2}]
    rows, _ = parse_events(events=[future], schedule=games, game_date="2026-08-07", fetched_at_utc=FETCH,
        run_tag="x", raw_source_path="raw", raw_source_sha256="sha")
    assert {row["game_id"] for row in rows} == {11}


def test_10_post_start_excluded_from_pregame_evidence():
    rows, audit = parse_events(events=[event("2026-08-07T17:00:00Z")], schedule=schedule("2026-08-07T17:00:00Z"),
        game_date="2026-08-07", fetched_at_utc=FETCH, run_tag="x", raw_source_path="raw", raw_source_sha256="sha")
    assert rows == [] and audit[0]["event_classification"] == "PAST_OR_STARTED"


def test_11_report_tracks_first_price_and_snapshot_count(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); append_event_discovery(conn, discovery())
    [append_market(conn, row) for row in mark_first_observed_prices(conn, future_rows()[0])]
    report = report_rows(conn, "2026-08-08")
    assert report[0]["first_event_discovery_utc"] == FETCH and report[0]["retained_snapshots"] == 3


def test_12_no_prediction_created_early(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3"); append_event_discovery(conn, discovery())
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert not any("prediction" in name for name in tables)


def test_13_market_normalization_unchanged():
    rows, _ = future_rows(); types = {row["market_type"] for row in rows}
    assert types == {"MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"}
    assert next(row for row in rows if row["market_type"] == "FULL_GAME_TOTAL")["total_line"] == 8.5
