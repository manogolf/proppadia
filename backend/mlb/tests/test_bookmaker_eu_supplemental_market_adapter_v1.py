from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    BOOKMAKER_ID, LEDGER_BOOKMAKER_KEY, RUN_LINE_MODEL_STATUS, append_attachment,
    append_consensus, append_market, american_decimal, american_implied, bind_event,
    build_consensus, connect_ledger, ledger_counts, no_vig, parse_american, parse_events,
)
from backend.mlb.scripts import capture_mlb_bookmaker_eu_supplemental_v1 as runner

ROOT = Path(__file__).resolve().parents[3]
FETCH = "2026-08-06T21:00:00Z"
START = "2026-08-06T23:00:00Z"
UPDATE = "2026-08-06T20:59:00Z"


def game(game_pk=10, start=START, number=1):
    return {
        "game_pk": game_pk, "game_date": "2026-08-06", "scheduled_start_utc": start,
        "away_team_name": "New York Yankees", "home_team_name": "Boston Red Sox",
        "game_number": number, "double_header": "N",
    }


def side(odds, **extra):
    return {"odds": odds, "lastUpdatedAt": UPDATE, "available": True, **extra}


def event(*, started=False, start=START, game_number=None):
    odds = {
        "points-away-game-ml-away": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("+170"), "other": side("+999")}},
        "points-home-game-ml-home": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("-194"), "other": side("-999")}},
        "points-all-game-ou-over": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("-108", overUnder="8.5")}},
        "points-all-game-ou-under": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("-108", overUnder="8.5")}},
        "points-away-game-sp-away": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("-120", spread="+1.5")}},
        "points-home-game-sp-home": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("+102", spread="-1.5")}},
        "points-all-1i-ou-over": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("-110", overUnder="0.5")}},
        "points-all-game-ou-over-alt-9.5": {"started": False, "byBookmaker": {BOOKMAKER_ID: side("+100", overUnder="9.5")}},
    }
    value = {
        "eventID": "event-1", "leagueID": "MLB",
        "teams": {"away": {"names": {"long": "New York Yankees"}},
                  "home": {"names": {"long": "Boston Red Sox"}}},
        "status": {"startsAt": start, "started": started, "live": started},
        "odds": odds,
    }
    if game_number is not None:
        value["info"] = {"gameNumber": game_number}
    return value


def parsed(tmp_path, *, fetched=FETCH, events=None, games=None):
    return parse_events(
        events=events or [event()], schedule=games or [game()], game_date="2026-08-06",
        fetched_at_utc=fetched, run_tag="run-1", raw_source_path="raw.json",
        raw_source_sha256="a" * 64,
    )


def test_01_authentication_without_secret_exposure(monkeypatch, tmp_path):
    secret = "test-secret-never-persist"
    payload = {"success": True, "data": []}
    calls = []

    class Response:
        status_code = 200
        content = json.dumps(payload).encode()
        def raise_for_status(self): pass
        def json(self): return payload

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setenv("SPORTSGAMEODDSAPI", secret)
    monkeypatch.setattr(runner.requests, "get", fake_get)
    monkeypatch.setattr(runner, "RAW_ROOT", tmp_path)
    _, manifest = runner.fetch_current("2026-08-06")
    assert calls[0][1]["headers"] == {"x-api-key": secret}
    assert "apiKey" not in calls[0][1]["params"]
    assert secret not in "".join(path.read_text() for path in tmp_path.rglob("*.json"))
    assert manifest["http_status"] == 200


def test_02_bookmaker_filtering_uses_only_bookmakereu(tmp_path):
    rows, _ = parsed(tmp_path)
    money = next(row for row in rows if row["market_type"] == "MONEYLINE")
    assert money["away_raw_american_price"] == "+170"
    assert money["bookmaker_provider_id"] == "bookmakereu"


def test_03_exact_mlb_game_identity(tmp_path):
    rows, audit = parsed(tmp_path)
    assert {row["game_id"] for row in rows} == {10}
    assert audit[0]["certification_status"] == "CERTIFIED_EXACT_OR_DETERMINISTIC"


def test_04_doubleheader_uses_game_number_and_fails_ambiguous():
    games = [game(10, "2026-08-06T22:58:00Z", 1), game(11, "2026-08-06T23:02:00Z", 2)]
    bound, status, method, ids = bind_event(event(game_number=2), games, FETCH)
    assert bound["game_pk"] == 11 and status == "CERTIFIED_EXACT_OR_DETERMINISTIC"
    assert method.endswith("AND_GAME_NUMBER") and ids == [11]
    bound, status, _, ids = bind_event(event(), games, FETCH)
    assert bound is None and status == "AMBIGUOUS" and ids == [10, 11]


def test_05_moneyline_parsing(tmp_path):
    rows, _ = parsed(tmp_path)
    row = next(row for row in rows if row["market_type"] == "MONEYLINE")
    assert (row["away_american_price"], row["home_american_price"]) == (170, -194)
    assert row["no_vig_home_probability"] == pytest.approx(no_vig(-194, 170))


def test_06_total_parsing(tmp_path):
    rows, _ = parsed(tmp_path)
    row = next(row for row in rows if row["market_type"] == "FULL_GAME_TOTAL")
    assert row["total_line"] == 8.5
    assert (row["over_raw_american_price"], row["under_raw_american_price"]) == ("-108", "-108")


def test_07_run_line_parsing(tmp_path):
    rows, _ = parsed(tmp_path)
    row = next(row for row in rows if row["market_type"] == "RUN_LINE")
    assert (row["away_spread"], row["home_spread"]) == (1.5, -1.5)
    assert (row["away_american_price"], row["home_american_price"]) == (-120, 102)


def test_08_american_odds_normalization():
    assert parse_american("+170") == 170 and parse_american("-200") == -200
    assert american_decimal(170) == pytest.approx(2.7)
    assert american_decimal(-200) == pytest.approx(1.5)
    assert american_implied(170) == pytest.approx(100 / 270)
    with pytest.raises(ValueError): parse_american("1.91")


def test_09_provider_update_timestamps_preserved(tmp_path):
    rows, _ = parsed(tmp_path)
    assert all(row["provider_market_updated_at_utc"] == UPDATE for row in rows)
    assert all(any(key.endswith("_provider_updated_at_utc") for key in row) for row in rows)


def test_10_fetch_timestamp_and_lead_time_preserved(tmp_path):
    rows, _ = parsed(tmp_path)
    assert all(row["captured_at_utc"] == FETCH for row in rows)
    assert all(row["lead_time_minutes"] == pytest.approx(120) for row in rows)


def test_11_post_start_rejection(tmp_path):
    rows, audit = parsed(tmp_path, fetched=START)
    assert rows == [] and audit[0]["certification_status"] == "POST_START"


def test_12_raw_response_hashing(monkeypatch, tmp_path):
    payload = {"success": True, "data": [], "notice": "bounded"}
    body = json.dumps(payload, separators=(",", ":")).encode()
    class Response:
        status_code = 200
        content = body
        def raise_for_status(self): pass
        def json(self): return payload
    monkeypatch.setenv("SPORTSGAMEODDSAPI", "not-persisted")
    monkeypatch.setattr(runner.requests, "get", lambda *a, **k: Response())
    monkeypatch.setattr(runner, "RAW_ROOT", tmp_path)
    _, manifest = runner.fetch_current("2026-08-06")
    assert manifest["raw_response_sha256"] == hashlib.sha256(body).hexdigest()
    events, replay = runner.load_immutable_capture(Path(manifest["run_manifest_path"]))
    assert events == [] and replay["request_count"] == 0 and replay["replay_of_request_count"] == 1


def test_13_immutable_ledger_append(tmp_path):
    row = parsed(tmp_path)[0][0]
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    assert append_market(conn, row) == "APPENDED_NEW"
    assert append_market(conn, row) == "EXISTING_IMMUTABLE"
    with pytest.raises(sqlite3.IntegrityError, match="APPEND_ONLY_SUPPLEMENTAL_MAIN_MARKET"):
        conn.execute("UPDATE supplemental_main_market_snapshots SET market_type='X'")


def test_14_duplicate_protection_and_conflict_preservation(tmp_path):
    row = parsed(tmp_path)[0][0]
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    append_market(conn, row)
    assert append_market(conn, {**row, "away_american_price": 175}) == "EXISTING_CONFLICT_PRESERVED"
    assert ledger_counts(conn)["duplicate_market_identities"] == 0


def test_15_the_odds_api_rows_unchanged(tmp_path):
    row = parsed(tmp_path)[0][0]
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    conn.execute("CREATE TABLE legacy_odds(identity TEXT PRIMARY KEY,payload_hash TEXT NOT NULL)")
    conn.execute("INSERT INTO legacy_odds VALUES ('odds-api-row','abc')")
    conn.commit()
    before = conn.execute("SELECT * FROM legacy_odds").fetchall()
    append_market(conn, row)
    assert conn.execute("SELECT * FROM legacy_odds").fetchall() == before


def test_16_bookmaker_identity_is_provider_qualified(tmp_path):
    row = parsed(tmp_path)[0][0]
    assert row["bookmaker_key"] == LEDGER_BOOKMAKER_KEY
    assert row["canonical_market_identity"].startswith("SPORTSGAMEODDS|bookmakereu|")


def _consensus_row(provider, book, market, **values):
    return {
        "provider": provider, "bookmaker_key": book, "game_date": "2026-08-06", "game_id": 10,
        "market_type": market, "captured_at_utc": "2026-08-06T20:59:00Z",
        "scheduled_start_utc": START, "timing_status": "PREGAME_CERTIFIED", **values,
    }


def test_17_totals_consensus_includes_bookmaker():
    rows = [
        _consensus_row("THE_ODDS_API", "draftkings", "FULL_GAME_TOTAL", total_line=8.5, no_vig_over_probability=.49),
        _consensus_row("SPORTSGAMEODDS", LEDGER_BOOKMAKER_KEY, "FULL_GAME_TOTAL", total_line=8.5, no_vig_over_probability=.51),
    ]
    value = build_consensus(rows=rows, game_date="2026-08-06", game_id=10, market_type="FULL_GAME_TOTAL", captured_at_utc=FETCH)
    assert value["books_captured"] == 2 and LEDGER_BOOKMAKER_KEY in value["bookmaker_keys"]
    assert value["median_no_vig_over_probability_at_consensus_line"] == pytest.approx(.5)


def test_18_moneyline_consensus_includes_bookmaker():
    rows = [
        _consensus_row("THE_ODDS_API", "draftkings", "MONEYLINE", no_vig_home_probability=.48, no_vig_away_probability=.52),
        _consensus_row("SPORTSGAMEODDS", LEDGER_BOOKMAKER_KEY, "MONEYLINE", no_vig_home_probability=.52, no_vig_away_probability=.48),
    ]
    value = build_consensus(rows=rows, game_date="2026-08-06", game_id=10, market_type="MONEYLINE", captured_at_utc=FETCH)
    assert value["consensus_status"] == "MULTI_BOOK_CONSENSUS"
    assert value["median_no_vig_home_probability"] == pytest.approx(.5)


def test_19_differing_total_lines_are_not_probability_averaged():
    rows = [
        _consensus_row("A", "a", "FULL_GAME_TOTAL", total_line=8.5, no_vig_over_probability=.60),
        _consensus_row("B", "b", "FULL_GAME_TOTAL", total_line=9.0, no_vig_over_probability=.40),
    ]
    value = build_consensus(rows=rows, game_date="2026-08-06", game_id=10, market_type="FULL_GAME_TOTAL", captured_at_utc=FETCH)
    assert value["median_line"] == 8.75 and value["books_at_consensus_line"] == 0
    assert value["median_no_vig_over_probability_at_consensus_line"] is None


def test_20_differing_run_lines_are_not_probability_averaged():
    rows = [
        _consensus_row("A", "a", "RUN_LINE", home_spread=-1.5, no_vig_home_probability=.55),
        _consensus_row("B", "b", "RUN_LINE", home_spread=-2.5, no_vig_home_probability=.45),
    ]
    value = build_consensus(rows=rows, game_date="2026-08-06", game_id=10, market_type="RUN_LINE", captured_at_utc=FETCH)
    assert value["median_line"] == -2.0 and value["books_at_consensus_line"] == 0
    assert value["median_no_vig_home_probability_at_consensus_spread"] is None


def test_21_totals_shadow_attachment_append_only(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    payload = {"timing_relationship": "POST_PREDICTION_MARKET_OBSERVATION", "model_expected_total": 8.7, "bookmaker_eu_total": 8.5}
    action = append_attachment(conn, table="bookmaker_eu_totals_shadow_attachments", prediction_identity="pred", market_identity="market", payload=payload, created_at_utc=FETCH)
    assert action == "APPENDED_NEW"
    assert append_attachment(conn, table="bookmaker_eu_totals_shadow_attachments", prediction_identity="pred", market_identity="market", payload=payload, created_at_utc=FETCH) == "EXISTING_IMMUTABLE"


def test_22_moneyline_shadow_attachment_append_only(tmp_path):
    conn = connect_ledger(tmp_path / "ledger.sqlite3")
    payload = {"timing_relationship": "AT_OR_BEFORE_PREDICTION", "model_home_probability": .52, "bookmaker_eu_no_vig_home_probability": .5}
    assert append_attachment(conn, table="bookmaker_eu_moneyline_shadow_attachments", prediction_identity="pred", market_identity="market", payload=payload, created_at_utc=FETCH) == "APPENDED_NEW"


def test_23_run_line_preserved_without_model(tmp_path):
    rows, _ = parsed(tmp_path)
    run_line = next(row for row in rows if row["market_type"] == "RUN_LINE")
    assert run_line["away_spread"] == 1.5
    assert RUN_LINE_MODEL_STATUS == "MODEL_COMPARISON_UNAVAILABLE_NO_QUALIFIED_RUN_LINE_MODEL"


def test_24_provider_failure_isolation(tmp_path):
    workspace = tmp_path
    (workspace / "bin").mkdir()
    (workspace / ".venv/bin").mkdir(parents=True)
    hook = (ROOT / "bin/mlb_full_game_totals_daily_hook.sh").read_text()
    (workspace / "hook.sh").write_text(hook)
    (workspace / "hook.sh").chmod(0o755)
    fake_book = workspace / "bin/mlb_bookmaker_eu_daily_hook.sh"
    fake_book.write_text("#!/bin/zsh\nexit ${BOOK_RC:-0}\n")
    fake_book.chmod(0o755)
    fake_python = workspace / ".venv/bin/python"
    fake_python.write_text("#!/bin/zsh\nexit ${ODDS_RC:-0}\n")
    fake_python.chmod(0o755)
    for odds_rc, book_rc, expected in ((0, 1, 0), (1, 0, 0), (1, 1, 1)):
        result = subprocess.run([str(workspace / "hook.sh"), "2026-08-06", "run"], cwd=workspace,
                                env={"PATH": "/bin:/usr/bin", "ODDS_RC": str(odds_rc), "BOOK_RC": str(book_rc)},
                                text=True, capture_output=True)
        assert result.returncode == expected


def test_25_no_ev_wager_ranking_or_staking_fields_and_evidence_smoke(tmp_path, monkeypatch):
    rows, _ = parsed(tmp_path)
    forbidden = {"ev", "edge", "wager", "ranking", "staking", "stake"}
    assert not any(key.casefold() in forbidden for row in rows for key in row)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    raw = tmp_path / "raw.json"
    raw.write_text('{"success":true,"data":[]}')
    manifest = tmp_path / "run_manifest.json"
    manifest.write_text('{"request_count":1}')
    output = tmp_path / "evidence"
    summary = {
        "raw_response_path": "raw.json", "raw_response_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
        "provider_events": 0, "current_games_captured": 0, "moneyline_rows": 0, "total_rows": 0,
        "run_line_rows": 0, "pregame_certified_rows": 0, "post_start_rejected_rows": 0,
        "moneyline_prediction_source_status": "AVAILABLE", "the_odds_api_rows_unchanged": True,
        "http_status": 200, "request_count": 1, "run_manifest_path": "run_manifest.json",
    }
    runner.write_evidence(output_dir=output, summary=summary, rows=[], audit=[], actions=[], consensus=[],
                          total_attach=[], money_attach=[], old_consensus={}, validated_test_count=25)
    assert len(list(output.iterdir())) == 13
