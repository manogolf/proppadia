from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.app.api_server import app
from backend.app.services.mlb import public_game_prediction_service as service
from backend.mlb.public_game_predictions import baseline_v1 as baseline

ROOT = Path(__file__).resolve().parents[3]


def schedule_fixture(doubleheader: bool = False):
    game = {
        "gamePk": 900001,
        "officialDate": "2026-08-05",
        "gameDate": "2026-08-05T23:00:00Z",
        "gameNumber": 2 if doubleheader else 1,
        "doubleHeader": "Y" if doubleheader else "N",
        "teams": {"away": {"team": {"name": "Away Club"}}, "home": {"team": {"name": "Home Club"}}},
    }
    return {"dates": [{"date": "2026-08-05", "games": [game]}]}


def score(payload=None, timestamp="2026-08-05T16:00:00Z"):
    return baseline.score_schedule_payload(
        payload or schedule_fixture(), prediction_timestamp_utc=timestamp, source_schedule_hash="a" * 64
    )


def test_01_config_hash_validates():
    assert baseline.load_candidate()["model_hash"] == "36a886eb08d9458ddf0c2158f59552e4c62ccb671eaee230066b8ff4128ca651"


def test_02_immutable_version():
    assert baseline.load_candidate()["model_identity"]["model_version"] == baseline.MODEL_VERSION


def test_03_archived_baseline_population_contract_is_self_contained():
    assert baseline.load_candidate()["model_identity"]["holdout_population"] == 156


def test_04_archived_baseline_metrics_are_hash_bound():
    candidate=baseline.load_candidate()
    assert candidate["historical_evaluation"]["home_score_mae"] == pytest.approx(2.6256155544235016)
    assert candidate["historical_evaluation"]["away_score_mae"] == pytest.approx(2.566182713533707)
    assert candidate["historical_evaluation"]["total_runs_mae"] == pytest.approx(3.8263995018961907)


def test_05_august5_shadow_values_reproduce():
    row = score()[0]
    assert row["expected_home_runs"] == pytest.approx(4.475816993464052)
    assert row["expected_away_runs"] == pytest.approx(4.504575163398693)
    assert row["home_win_probability"] == pytest.approx(0.4956365615982994)


def test_06_repeat_scoring_is_field_stable():
    assert score() == score()


def test_07_scoring_contract_has_no_outcome_fields():
    forbidden = {"home_runs", "away_runs", "official_winner", "roi", "ev", "profit"}
    assert not forbidden.intersection(score()[0])


def test_08_pregame_cutoff_fails_closed():
    row = score(timestamp="2026-08-06T00:00:00Z")[0]
    assert row["failure_reason"] == "PREGAME_CUTOFF_FAILED"


def test_09_doubleheader_keeps_exact_game_id():
    row = score(schedule_fixture(doubleheader=True))[0]
    assert row["game_id"] == 900001


def test_10_prediction_ledger_appends_once(tmp_path):
    path = tmp_path / "predictions.jsonl"
    assert baseline.append_prediction_rows(score(), path) == 1
    assert len(path.read_text().splitlines()) == 1


def test_11_prediction_ledger_is_idempotent(tmp_path):
    path = tmp_path / "predictions.jsonl"; rows = score()
    baseline.append_prediction_rows(rows, path)
    assert baseline.append_prediction_rows(rows, path) == 0


def test_12_grading_requires_official_final(tmp_path):
    row = {**score()[0], "official_status": "In Progress"}
    with pytest.raises(baseline.PublicGamePredictionError, match="OFFICIAL_FINAL"):
        baseline.append_grading_rows([row], tmp_path / "outcomes.jsonl")


def test_13_grading_ledger_is_append_only(tmp_path):
    row = {**score()[0], "official_status": "Final", "official_home_runs": 5, "official_away_runs": 3}
    path = tmp_path / "outcomes.jsonl"
    assert baseline.append_grading_rows([row], path) == 1
    assert baseline.append_grading_rows([row], path) == 0


def test_14_feature_flag_defaults_off(monkeypatch):
    monkeypatch.delenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", raising=False)
    assert baseline.feature_enabled() is False


def test_15_authorities_remain_separated(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    status = baseline.authority_status()
    assert status["betting_authority"] == "NO_QUALIFIED_MLB_BETTING_MODEL"
    assert status["player_prop_authority"] == "NO_QUALIFIED_MLB_PROP_MODEL"


def test_16_api_flag_off_returns_no_rows(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "0")
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-05"})
    assert response.status_code == 200 and response.json()["rows"] == []


def test_17_api_test_on_renders_rows(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: score())
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-05"})
    assert response.status_code == 200 and response.json()["count"] == 1


def test_18_public_schema_has_disclosure_and_no_ev(monkeypatch):
    row = score()[0]
    assert row["disclosure"] == "MODEL PREDICTION — BETTING EDGE NOT DEMONSTRATED"
    assert "ev" not in row and "recommended_wager" not in row


def test_19_scorer_isolated_from_retired_and_betting_paths():
    source = Path(baseline.__file__).read_text()
    for forbidden in ("import joblib", "import backend.mlb.model_trainer", "import backend.app.services.model_registry", "assert_predictive_model_qualified"):
        assert forbidden not in source.lower()


def test_20_baseline_remains_archived_but_is_not_public_binding():
    source = (ROOT / "frontend/src/components/mlb/MLBPublicGamePredictionsPanel.jsx").read_text()
    assert "45.51%" not in source and "Pythagorean/Log5 v1" in source
    assert "BETTING EDGE NOT DEMONSTRATED" in source
    assert "Confidence reflects model separation from 50%" in source
