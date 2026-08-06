from __future__ import annotations

import json
import sqlite3

import pytest

from backend.app.services.mlb.market_odds_service import get_market_to_prop_map
from backend.mlb.markets.full_game_total_capture_v1 import (
    append_market, attach_market, bind_event, connect_ledger, ledger_counts, parse_totals,
)
from backend.mlb.scripts.capture_mlb_full_game_totals_v1 import load_or_fetch


def schedule(start="2026-08-06T23:00:00Z", game_pk=10, game_number=1):
    return {"game_pk": game_pk, "game_date": "2026-08-06", "scheduled_start_utc": start,
            "away_team_name": "New York Yankees", "home_team_name": "Boston Red Sox", "game_number": game_number}


def event(start="2026-08-06T23:00:00Z", outcomes=None):
    return {"id": "evt-1", "commence_time": start, "away_team": "New York Yankees", "home_team": "Boston Red Sox",
            "bookmakers": [{"key": "betonlineag", "title": "BetOnline.ag", "markets": [{"key": "totals", "last_update": "2026-08-06T20:00:00Z", "outcomes": outcomes or [
                {"name": "Over", "point": 8.5, "price": -115}, {"name": "Under", "point": 8.5, "price": -105}]}]}]}


def parse(events, games, captured="2026-08-06T21:00:00Z"):
    return parse_totals(events=events, schedule=games, game_date="2026-08-06", captured_at_utc=captured,
                        source_run_tag="run-1", raw_source_path="raw.json", raw_source_sha256="a" * 64)


def test_full_game_totals_remain_separate_from_player_prop_mapping():
    assert "totals" not in get_market_to_prop_map()


def test_full_game_total_parser_binds_both_american_prices_and_exact_line():
    rows, audit = parse([event()], [schedule()])
    assert len(rows) == 1
    assert rows[0]["game_id"] == 10
    assert rows[0]["total_line"] == 8.5
    assert (rows[0]["over_price"], rows[0]["under_price"]) == (-115, -105)
    assert rows[0]["market_status"] == "TOTAL_MARKET_CERTIFIED_PAIRED"
    assert audit[0]["identity_certification"] == "EXACT_UNIQUE_MATCH"


def test_line_only_is_retained_without_inferred_opposite_price():
    rows, _ = parse([event(outcomes=[{"name": "Over", "point": 9.0, "price": 105}])], [schedule()])
    assert len(rows) == 1
    assert rows[0]["market_status"] == "TOTAL_MARKET_LINE_ONLY"
    assert rows[0]["under_price"] is None


def test_post_start_snapshot_rejected():
    rows, audit = parse([event()], [schedule()], captured="2026-08-06T23:00:00Z")
    assert rows == []
    assert audit[0]["admission_status"] == "REJECTED_POST_START"


def test_doubleheader_resolves_only_by_unique_scheduled_time():
    games = [schedule("2026-08-06T18:00:00Z", 10, 1), schedule("2026-08-06T23:00:00Z", 11, 2)]
    game, status, ids = bind_event(event("2026-08-06T23:00:00Z"), games)
    assert status == "EXACT_UNIQUE_MATCH" and game["game_pk"] == 11 and ids == [11]
    ambiguous = [schedule("2026-08-06T22:55:00Z", 10, 1), schedule("2026-08-06T23:05:00Z", 11, 2)]
    game, status, ids = bind_event(event("2026-08-06T23:00:00Z"), ambiguous)
    assert game is None and status == "EVENT_IDENTITY_AMBIGUOUS" and ids == [10, 11]


def test_raw_snapshot_retention_and_sha(tmp_path):
    path = tmp_path / "raw.json"
    path.write_text(json.dumps({"captured_at_utc": "2026-08-06T21:00:00Z", "events": [event()]}))
    events, captured, run_tag, raw_path, digest = load_or_fetch("2026-08-06", path)
    assert len(events) == 1 and captured == "2026-08-06T21:00:00Z"
    assert raw_path == path.resolve() and len(digest) == 64 and run_tag == "raw"


def test_market_ledger_is_immutable_idempotent_and_duplicate_free(tmp_path):
    row = parse([event()], [schedule()])[0][0]
    conn = connect_ledger(tmp_path / "market.sqlite3")
    assert append_market(conn, row) == "APPENDED_NEW"
    assert append_market(conn, row) == "EXISTING_IMMUTABLE"
    changed = {**row, "over_price": -120}
    assert append_market(conn, changed) == "EXISTING_CONFLICT_PRESERVED"
    assert ledger_counts(conn)["market_rows"] == 1
    with pytest.raises(sqlite3.IntegrityError, match="APPEND_ONLY_TOTAL_MARKET_LEDGER"):
        conn.execute("UPDATE full_game_total_market_snapshots SET total_line=9")


def test_shadow_attachment_preserves_later_market_as_post_prediction(tmp_path):
    row = parse([event()], [schedule()])[0][0]
    conn = connect_ledger(tmp_path / "market.sqlite3")
    append_market(conn, row)
    prediction = {"game_date": "2026-08-06", "game_pk": 10, "model_version": "DIRECT_NEGATIVE_BINOMIAL",
                  "prediction_snapshot_class": "DAILY_DESIGNATED_PREGAME", "prediction_timestamp_utc": "2026-08-06T20:00:00Z"}
    attached, action = attach_market(conn, prediction, [row], "2026-08-06T21:01:00Z")
    assert action == "APPENDED_NEW"
    assert attached["timing_relationship"] == "POST_PREDICTION_MARKET_OBSERVATION"
    assert ledger_counts(conn)["bridge_rows"] == 1


def test_shadow_attachment_prefers_latest_prior_snapshot_and_betonline(tmp_path):
    base = parse([event()], [schedule()])[0][0]
    earlier = {**base, "captured_at_utc": "2026-08-06T19:00:00Z"}
    earlier["canonical_market_identity"] = "10|betonlineag|FULL_GAME_TOTAL|2026-08-06T19:00:00Z|8.5"
    other = {**earlier, "bookmaker": "DraftKings", "bookmaker_key": "draftkings", "total_line": 9.0}
    other["canonical_market_identity"] = "10|draftkings|FULL_GAME_TOTAL|2026-08-06T19:00:00Z|9"
    later = {**base, "captured_at_utc": "2026-08-06T20:30:00Z"}
    later["canonical_market_identity"] = "10|betonlineag|FULL_GAME_TOTAL|2026-08-06T20:30:00Z|8.5"
    conn = connect_ledger(tmp_path / "market.sqlite3")
    for row in (earlier, other, later): append_market(conn, row)
    prediction = {"game_date": "2026-08-06", "game_pk": 10, "model_version": "DIRECT_NEGATIVE_BINOMIAL",
                  "prediction_snapshot_class": "DAILY_DESIGNATED_PREGAME", "prediction_timestamp_utc": "2026-08-06T21:00:00Z"}
    attached, _ = attach_market(conn, prediction, [earlier, other, later], "2026-08-06T21:01:00Z")
    assert attached["bookmaker_key"] == "betonlineag"
    assert attached["captured_at_utc"] == "2026-08-06T20:30:00Z"
    assert attached["timing_relationship"] == "AT_OR_BEFORE_PREDICTION"
