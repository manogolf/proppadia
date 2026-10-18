"""Offline checks for the MLB today-workspace slate input contract."""

from pathlib import Path

import pytest

from backend.mlb.scripts.build_mlb_today_workspace import _load_slate_rows


ROOT = Path(__file__).resolve().parents[3]


def test_observed_missing_qualified_slate_input_fails_closed():
    missing = ROOT / "backend/mlb/exports/odds_history/2026-10-17/mlb_slate_output.csv"

    with pytest.raises(FileNotFoundError, match="slate csv not found"):
        _load_slate_rows(missing, "2026-10-17")


def test_retained_populated_slate_input_loads_with_game_and_probability_rows():
    retained = ROOT / "backend/mlb/exports/odds_history/2026-06-07/mlb_slate_output.csv"

    rows = _load_slate_rows(retained, "2026-06-07")

    assert len(rows) == 134
    assert set(rows["slate_date"].astype(str)) == {"2026-06-07"}
    assert rows["game_id"].notna().all()
    assert rows["player_id"].notna().all()
    assert rows["prob_over"].notna().all()
    assert rows["prob_under"].notna().all()
