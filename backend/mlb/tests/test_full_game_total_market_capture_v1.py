from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from backend.app.services.mlb.market_odds_service import get_market_to_prop_map
from backend.mlb.markets.full_game_total_capture_v1 import (
    append_consensus, append_market, attach_all_markets, attach_market, bind_event,
    build_consensus, connect_ledger, ledger_counts, parse_totals,
)
from backend.mlb.scripts.capture_mlb_full_game_totals_v1 import ROOT, load_or_fetch, write_evidence


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


def test_all_book_bridge_preserves_each_book_without_collapsing(tmp_path):
    betonline = parse([event()], [schedule()])[0][0]
    draftkings = {**betonline, "bookmaker": "DraftKings", "bookmaker_key": "draftkings", "total_line": 9.0,
                    "canonical_market_identity": "10|draftkings|FULL_GAME_TOTAL|2026-08-06T21:00:00Z|9"}
    conn = connect_ledger(tmp_path / "market.sqlite3")
    for row in (betonline, draftkings): append_market(conn, row)
    prediction = {"game_date": "2026-08-06", "game_pk": 10, "model_version": "DIRECT_NEGATIVE_BINOMIAL",
                  "prediction_snapshot_class": "DAILY_DESIGNATED_PREGAME", "prediction_timestamp_utc": "2026-08-06T20:00:00Z"}
    attached = attach_all_markets(conn, prediction, [betonline, draftkings], "2026-08-06T21:01:00Z")
    assert len(attached) == 2
    assert {row["bookmaker_key"] for row in attached} == {"betonlineag", "draftkings"}
    assert {row["timing_relationship"] for row in attached} == {"POST_PREDICTION_MARKET_OBSERVATION"}
    assert ledger_counts(conn)["all_book_bridge_rows"] == 2
    assert all(row["bridge_action"] == "EXISTING_IMMUTABLE" for row in attach_all_markets(conn, prediction, [betonline, draftkings], "2026-08-06T21:01:00Z"))


def test_consensus_uses_median_line_and_same_line_prices_only(tmp_path):
    prediction = {"game_date": "2026-08-06", "game_pk": 10, "model_version": "DIRECT_NEGATIVE_BINOMIAL",
                  "prediction_snapshot_class": "DAILY_DESIGNATED_PREGAME", "prediction_timestamp_utc": "2026-08-06T20:00:00Z"}
    rows = []
    for key, line, over, under in (("a", 8.0, 500, -900), ("b", 8.5, -110, -110), ("c", 8.5, -120, 100), ("d", 9.0, -500, 300)):
        rows.append({"game_id": 10, "captured_at_utc": "2026-08-06T21:00:00Z", "total_line": line,
                     "over_price": over, "under_price": under, "bookmaker_key": key})
    consensus = build_consensus(prediction, rows, "2026-08-06T21:00:00Z")
    assert consensus["books_captured"] == 4 and consensus["distinct_lines"] == 3
    assert consensus["median_total_line"] == 8.5 and consensus["modal_total_line"] == 8.5
    assert consensus["books_at_consensus_line"] == 2
    assert consensus["median_over_price_at_consensus_line"] == -115
    assert consensus["median_under_price_at_consensus_line"] == -5
    assert consensus["minimum_total_line"] == 8.0 and consensus["maximum_total_line"] == 9.0
    conn = connect_ledger(tmp_path / "market.sqlite3")
    assert append_consensus(conn, consensus, "2026-08-06T21:01:00Z") == "APPENDED_NEW"
    assert append_consensus(conn, consensus, "2026-08-06T21:01:00Z") == "EXISTING_IMMUTABLE"
    assert ledger_counts(conn)["consensus_rows"] == 1


def test_evidence_writer_accepts_market_capture_without_frozen_totals_predictions(tmp_path):
    summary = {
        "game_date": "2026-08-07", "captured_at_utc": "2026-08-07T12:49:03Z", "eligible_games": 15,
        "market_rows_parsed": 159, "paired_rows": 159, "line_only_rows": 0,
        "prediction_attachments": 0, "ledger_after": {"market_rows": 159},
    }
    write_evidence(tmp_path, summary, [], [], [], ROOT / "backend/mlb/exports/test-ledger.sqlite3")
    report = (tmp_path / "concise_mlb_full_game_total_market_capture_v1.md").read_text()
    assert "UNAVAILABLE_NO_FROZEN_TOTALS_PREDICTION" in report
    assert "2026-08-07 / 15" in report and "no attachment was inferred" in report
