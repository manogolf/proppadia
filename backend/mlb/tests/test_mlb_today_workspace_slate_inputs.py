"""Offline checks for the MLB today-workspace slate input contract."""

from pathlib import Path
from datetime import datetime, timezone

import pytest

from backend.mlb.scripts import build_mlb_today_workspace as workspace


ROOT = Path(__file__).resolve().parents[3]


def test_observed_missing_qualified_slate_input_is_an_explicit_skip(tmp_path, monkeypatch, capsys):
    missing = ROOT / "backend/mlb/exports/odds_history/2026-10-17/mlb_slate_output.csv"
    wide = missing.with_name("mlb_predictions_wide_calibrated.csv")
    assert not missing.exists()
    assert not wide.exists()
    status_file = tmp_path / "gate_status.txt"
    status_file.write_text(workspace.QUALIFICATION_SKIP_STATUS + "\n")

    def unexpected_database_connection():
        raise AssertionError("no-qualified-model skip must not connect to the database")

    monkeypatch.setattr(workspace, "pg_connect", unexpected_database_connection)

    result = workspace.main(
        [
            "--slate-date",
            "2026-10-17",
            "--odds-root",
            str(missing.parent.parent),
            "--qualification-status-file",
            str(status_file),
        ]
    )

    assert result == workspace.SKIPPED_NO_QUALIFIED_PREDICTION_SLATE
    assert "SKIP status=EXPECTED_SKIP" in capsys.readouterr().out


def test_qualified_gate_with_missing_inputs_remains_an_unexpected_failure(tmp_path):
    (tmp_path / "2026-10-17").mkdir()
    status_file = tmp_path / "gate_status.txt"
    status_file.write_text(workspace.QUALIFICATION_SUCCESS_STATUS + "\n")

    with pytest.raises(FileNotFoundError, match="slate csv not found"):
        workspace.main(
            [
                "--slate-date",
                "2026-10-17",
                "--odds-root",
                str(tmp_path),
                "--qualification-status-file",
                str(status_file),
            ]
        )


def test_qualified_populated_inputs_continue_through_workspace_build(tmp_path, monkeypatch, capsys):
    (tmp_path / "2026-06-07").mkdir()
    status_file = tmp_path / "gate_status.txt"
    status_file.write_text(workspace.QUALIFICATION_SUCCESS_STATUS + "\n")
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text("{}")
    slate = ROOT / "backend/mlb/exports/odds_history/2026-06-07/mlb_slate_output.csv"
    wide = slate.with_name("mlb_predictions_wide_calibrated.csv")
    snapshot_ref = workspace.SnapshotRef(
        snapshot,
        datetime(2026, 6, 7, tzinfo=timezone.utc),
    )
    parsed_row = {
        "slate_date": "2026-06-07",
        "game_date": "2026-06-07",
        "snapshot_ts": datetime(2026, 6, 7, tzinfo=timezone.utc),
        "snapshot_file": snapshot.name,
        "event_id": "event-1",
        "game_id": 824670,
        "player_id": 123,
        "player_name": "Sample Player",
        "player_name_norm": "sample player",
        "home_team_code": "CHC",
        "away_team_code": "SF",
        "team": "CHC",
        "opponent": "SF",
        "is_home": True,
        "prop_type": "hits",
        "market_key": "batter_hits",
        "line": 0.5,
        "bookmaker_key": "testbook",
        "price_over_american": -110,
        "price_under_american": 100,
    }
    summary_counts = {
        "events": 1,
        "raw_outcomes": 2,
        "unsupported_market": 0,
        "missing_team_map": 0,
        "missing_player_or_line": 0,
        "unmatched_to_slate": 0,
        "matched_rows": 1,
    }
    monkeypatch.setattr(workspace, "_find_snapshot_files", lambda *_args: [snapshot_ref])
    monkeypatch.setattr(
        workspace,
        "_parse_snapshot_rows",
        lambda **_kwargs: ([parsed_row], summary_counts),
    )
    monkeypatch.setattr(workspace, "_ensure_stage_tables", lambda: None)
    monkeypatch.setattr(
        workspace,
        "_replace_date_rows",
        lambda **_kwargs: {
            "today_slate_rows": 134,
            "today_wide_rows": 128,
            "today_odds_book_rows": 1,
        },
    )

    def unexpected_database_connection():
        raise AssertionError("offline workspace test must not connect to the database")

    monkeypatch.setattr(workspace, "pg_connect", unexpected_database_connection)
    result = workspace.main(
        [
            "--slate-date",
            "2026-06-07",
            "--odds-root",
            str(tmp_path),
            "--slate-csv",
            str(slate),
            "--wide-csv",
            str(wide),
            "--snapshot-json",
            str(snapshot),
            "--qualification-status-file",
            str(status_file),
        ]
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "today_slate_rows=134" in output
    assert "today_wide_rows=128" in output
    assert "today_odds_book_rows=1" in output


def test_retained_populated_slate_input_loads_with_game_and_probability_rows():
    retained = ROOT / "backend/mlb/exports/odds_history/2026-06-07/mlb_slate_output.csv"
    retained_wide = retained.with_name("mlb_predictions_wide_calibrated.csv")

    rows = workspace._load_slate_rows(retained, "2026-06-07")
    wide_rows = workspace._load_wide_rows(retained_wide, "2026-06-07")

    assert len(rows) == 134
    assert set(rows["slate_date"].astype(str)) == {"2026-06-07"}
    assert rows["game_id"].notna().all()
    assert rows["player_id"].notna().all()
    assert rows["prob_over"].notna().all()
    assert rows["prob_under"].notna().all()
    assert len(wide_rows) == 128
    assert set(wide_rows["slate_date"].astype(str)) == {"2026-06-07"}
    assert wide_rows["game_id"].notna().all()
    assert wide_rows["player_id"].notna().all()
