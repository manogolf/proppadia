"""Offline checks for the MLB today-workspace slate input contract."""

from pathlib import Path

from backend.mlb.scripts import build_mlb_today_workspace as workspace


ROOT = Path(__file__).resolve().parents[3]


def test_observed_missing_qualified_slate_input_is_an_explicit_skip(monkeypatch, capsys):
    missing = ROOT / "backend/mlb/exports/odds_history/2026-10-17/mlb_slate_output.csv"
    wide = missing.with_name("mlb_predictions_wide_calibrated.csv")
    assert not missing.exists()
    assert not wide.exists()
    monkeypatch.setattr(workspace, "_model_authority_status", lambda: "NO_QUALIFIED_MLB_MODEL")

    def unexpected_database_connection():
        raise AssertionError("no-qualified-model skip must not connect to the database")

    monkeypatch.setattr(workspace, "pg_connect", unexpected_database_connection)

    result = workspace.main(
        ["--slate-date", "2026-10-17", "--odds-root", str(missing.parent.parent)]
    )

    assert result == workspace.SKIPPED_NO_QUALIFIED_PREDICTION_SLATE
    assert "SKIP status=NO_QUALIFIED_PREDICTION_SLATE" in capsys.readouterr().out


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
