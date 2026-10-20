from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sklearn.metrics import brier_score_loss, log_loss

from backend.app.api_server import app
from backend.app.services.mlb import public_game_prediction_service as service
from backend.mlb.public_game_predictions import pythagorean_log5_v1 as model

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "backend/mlb/tests/fixtures/public_game_predictions_v1"


def schedule_fixture(home_id=110, away_id=111, game_id=824401, start="2026-08-06T17:10:00Z"):
    return {"dates": [{"date": "2026-08-06", "games": [{
        "gamePk": game_id, "officialDate": "2026-08-06", "gameDate": start,
        "gameNumber": 1, "doubleHeader": "N",
        "teams": {"away": {"team": {"id": away_id, "name": "Away Club"}},
                  "home": {"team": {"id": home_id, "name": "Home Club"}}},
    }]}]}


def score(payload=None, timestamp="2026-08-06T12:00:00Z"):
    return model.score_schedule_payload(payload or schedule_fixture(), prediction_timestamp_utc=timestamp,
                                        source_schedule_hash="a" * 64)


def selected_predictions():
    return pd.read_csv(FIXTURES / "pythagorean_log5_exact_reproduction.csv.gz")


def test_01_frozen_model_hash_and_version():
    candidate = model.load_candidate()
    assert candidate["model_hash"] == "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
    assert candidate["model_identity"]["model_version"] == model.MODEL_VERSION


@pytest.mark.parametrize("split,n,accuracy,brier,ll", [
    ("FROZEN_VALIDATION", 2120, .5556603773584906, .244239080020185, .6813624655515292),
    ("2026_SEQUENTIAL_EARLY", 563, .5364120781527531, .250398375373888, .694007666402166),
    ("2026_LATE_HOLDOUT", 202, .594059405940594, .237168056439727, .666933958889105),
])
def test_02_benchmark_metrics_reproduce(split, n, accuracy, brier, ll):
    x = selected_predictions().query("split == @split")
    y, p = x.winner_home.astype(int), x.home_win_probability
    assert len(x) == n
    assert np.mean((p >= .5) == y) == pytest.approx(accuracy)
    assert brier_score_loss(y, p) == pytest.approx(brier)
    assert log_loss(y, p, labels=[0, 1]) == pytest.approx(ll)


def test_03_team_state_source_hash_and_strict_prior_state():
    candidate = model.load_candidate()
    source = ROOT / candidate["frozen_team_state_source"]["path"]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == candidate["frozen_team_state_source"]["sha256"]
    states = model._frozen_team_states(candidate)
    assert states[110]["games"] == 513


def test_04_pythagorean_and_log5_contract():
    assert model.pythagorean_strength(4.3, 4.3, 1.83) == pytest.approx(.5)
    assert model.log5_probability(.5, .5) == pytest.approx(.5)
    assert model.matchup_probability(.5, .5, .15, .01, .99) == pytest.approx(.5374298453437496)


def test_05_season_start_and_sparse_contract_are_not_invented():
    identity = model.load_candidate()["model_identity"]
    assert identity["season_initialization"] == "CONTINUOUS_CUMULATIVE_STRICT_PRIOR_STATE_NO_SEASON_RESET"
    assert identity["sparse_history_regression"] == "NONE_IN_ACCEPTED_BENCHMARK"
    assert identity["sparse_history_fallback"] == .5


def test_06_probability_bounds():
    p = model.matchup_probability(.999999, .000001, .15, .01, .99)
    assert 1e-6 <= p <= 1 - 1e-6


def test_07_doubleheader_exact_game_identity():
    payload = schedule_fixture(game_id=900002)
    payload["dates"][0]["games"][0].update({"gameNumber": 2, "doubleHeader": "Y"})
    assert score(payload)[0]["game_id"] == 900002


def test_08_pregame_cutoff_and_unknown_team_fail_closed():
    assert score(timestamp="2026-08-06T18:00:00Z")[0]["failure_reason"] == "PREGAME_CUTOFF_FAILED"
    assert score(schedule_fixture(home_id=999999))[0]["failure_reason"] == "TEAM_STRICT_PRIOR_STATE_UNAVAILABLE"


def test_09_no_outcome_prop_market_or_value_fields():
    row = score()[0]
    forbidden = {"official_winner", "home_runs", "away_runs", "ev", "roi", "stake", "recommended_wager"}
    assert not forbidden.intersection(row)
    assert row["disclosure"] == model.DISCLOSURE


def test_10_deterministic_repeat():
    assert score() == score()


def test_10a_august6_exact_shadow_reproduction():
    payload=json.loads((FIXTURES / "august6_schedule.json").read_text())
    expected=pd.read_csv(FIXTURES / "august6_expected.csv").set_index('game_pk')
    rows=model.score_schedule_payload(payload,prediction_timestamp_utc='2026-08-06T00:00:00Z',source_schedule_hash='a'*64)
    assert len(rows)==11
    for row in rows:
        accepted=expected.loc[int(row['game_id'])]
        assert row['home_win_probability']==pytest.approx(accepted.home_win_probability,abs=1e-12)
        assert row['predicted_winner']==accepted.predicted_winner


def test_11_prediction_ledger_append_only(tmp_path):
    path = tmp_path / "predictions.jsonl"
    assert model.append_prediction_rows(score(), path) == 1
    before = path.read_bytes()
    assert model.append_prediction_rows(score(), path) == 0
    assert path.read_bytes() == before


def test_12_grading_requires_final_and_is_idempotent(tmp_path):
    row = {**score()[0], "official_status": "In Progress"}
    with pytest.raises(model.PublicGamePredictionError, match="OFFICIAL_FINAL"):
        model.append_grading_rows([row], tmp_path / "grades.jsonl")
    row["official_status"] = "Final"
    path = tmp_path / "grades.jsonl"
    assert model.append_grading_rows([row], path) == 1
    assert model.append_grading_rows([row], path) == 0


def test_12a_official_final_grade_preserves_prediction_and_scores_probability():
    prediction = score()[0]
    before = json.dumps(prediction, sort_keys=True)
    grade = model.build_official_final_grade(
        prediction, official_home_runs=5, official_away_runs=3,
        official_source_path="retained/official.json", official_source_sha256="b" * 64,
        grading_timestamp_utc="2026-08-07T12:00:00Z",
    )
    assert json.dumps(prediction, sort_keys=True) == before
    assert grade["official_winner"] == prediction["home_team"]
    assert grade["brier_contribution"] == pytest.approx((prediction["home_win_probability"] - 1) ** 2)
    assert grade["official_source_sha256"] == "b" * 64


def test_13_feature_flag_defaults_off_and_authorities_isolated(monkeypatch):
    monkeypatch.delenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", raising=False)
    assert model.feature_enabled() is False
    status = model.authority_status()
    assert status["betting_authority"] == "NO_QUALIFIED_MLB_BETTING_MODEL"
    assert status["player_prop_authority"] == "NO_QUALIFIED_MLB_PROP_MODEL"


def test_14_api_flag_off_returns_no_rows(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "0")
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 200 and response.json()["rows"] == []
    assert response.json()["model_version"] == model.MODEL_VERSION


def test_15_api_test_on_renders_team_specific_moneyline_only(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: score())
    payload = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"}).json()
    row = payload["rows"][0]
    assert row["home_win_probability"] != pytest.approx(.5)
    assert row["predicted_winner"] in {"Home Club", "Away Club"}
    assert row["model_hash"] == "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
    assert row["picked_side_probability"] == pytest.approx(max(row["home_win_probability"], row["away_win_probability"]))
    assert not {"expected_home_runs", "expected_total_runs", "home_minus_1_5_probability"}.intersection(row)
    assert not {"ev", "edge", "roi", "wager", "stake", "betting_edge_status"}.intersection(row)


def test_16_old_baseline_not_bound_and_retired_paths_isolated():
    service_source = Path(service.__file__).read_text()
    scorer_source = Path(model.__file__).read_text().lower()
    assert "baseline_v1" not in service_source
    for forbidden in ("joblib", "model_registry", "player_prop", "wager", "ranking"):
        assert f"import {forbidden}" not in scorer_source


def test_17_frontend_contract_is_honest():
    source = (ROOT / "frontend/src/components/mlb/MLBPublicGamePredictionsPanel.jsx").read_text()
    assert "MLB_GAME_PYTHAGOREAN_LOG5_V1" in source
    assert "picked_side_probability" in source
    assert "BETTING EDGE NOT DEMONSTRATED" in source
    assert "recommended wager" not in source.lower()


def test_18_certified_public_adapter_fails_closed(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    valid = score()[0]
    for field, value in (
        ("winner_model_version", "RETIRED_OR_RESEARCH_MODEL"),
        ("winner_model_hash", "0" * 64),
    ):
        bad = {**valid, field: value}
        monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date, row=bad: [row])
        response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
        assert response.status_code == 503


def test_19_duplicate_identity_fails_closed(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    row = score()[0]
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [row, dict(row)])
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 503


def test_20_date_scope_and_parity(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    row = score()[0]
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [row])
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 200
    public = response.json()["rows"][0]
    assert public["home_win_probability"] == row["home_win_probability"]
    assert public["away_win_probability"] == row["away_win_probability"]
    assert public["confidence_band"] == row["confidence_band"]
    assert public["immutable_prediction_identity"].endswith(model.SNAPSHOT_CLASS)
    wrong_date = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-07"})
    assert wrong_date.status_code == 503


def test_21_status_preserves_certification_disclosure_and_authority(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    payload = TestClient(app).get("/api/mlb/model-status").json()
    assert payload["certification_status"] == "MONEYLINE_STANDALONE_PREDICTION_CERTIFIED"
    assert payload["display_status"] == "MONEYLINE_PUBLIC_PREDICTION_READY"
    assert payload["required_disclosure"] == model.DISCLOSURE
    assert payload["betting_authority"] == "NO_QUALIFIED_MLB_BETTING_MODEL"


def test_22_missing_phase_evidence_returns_governed_unavailable(monkeypatch):
    from backend.mlb.season_transition.game_phase_authority_v1 import GamePhaseAuthorityError

    def missing():
        raise GamePhaseAuthorityError("GAME_PHASE_EVIDENCE_MISSING", detail="artifacts/raw/schedule.json")

    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: score())
    monkeypatch.setattr(service, "snapshot_verified_moneyline_phase_authority", missing)
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"] == [] and payload["count"] == 0
    assert payload["unavailable_status"] == "PHASE_AUTHORITY_EVIDENCE_UNAVAILABLE"
    assert payload["unavailable_code"] == "GAME_PHASE_EVIDENCE_MISSING"
    # Evidence paths stay out of the public payload.
    assert "artifacts/" not in response.text


def test_23_no_durable_rows_does_not_load_phase_authority(monkeypatch):
    def unexpected():
        raise AssertionError("authority loaded without rows")

    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [])
    monkeypatch.setattr(service, "snapshot_verified_moneyline_phase_authority", unexpected)
    payload = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"}).json()
    assert payload["count"] == 0 and "unavailable_status" not in payload


def test_24_certification_failures_still_fail_closed_with_503(monkeypatch):
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    bad = {**score()[0], "winner_model_hash": "0" * 64}
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [bad])
    # Certification runs before the phase join, so missing evidence cannot mask it.
    monkeypatch.setattr(service, "snapshot_verified_moneyline_phase_authority", lambda: (_ for _ in ()).throw(
        AssertionError("phase join reached before certification")))
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 503


def _scored_game(game_id, start):
    return score(schedule_fixture(game_id=game_id, start=start))[0]


def test_25_game_absent_from_snapshot_is_withheld_not_503(monkeypatch):
    # 849815 (2026 ALCS) was admitted locally by the runtime overlay but is not
    # in the committed V5 snapshot; 849817 is.
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    in_snapshot = {**_scored_game(849817, "2026-10-18T21:08:00Z"), "game_date": "2026-10-18"}
    absent = {**_scored_game(849815, "2026-10-18T23:08:00Z"), "game_date": "2026-10-18"}
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [in_snapshot, absent])
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-10-18"})
    assert response.status_code == 200
    payload = response.json()
    assert [row["game_id"] for row in payload["rows"]] == [849817]
    assert payload["rows"][0]["normalized_phase"] == "POSTSEASON"
    assert payload["count"] == 1
    assert payload["excluded_rows"] == [
        {"game_id": 849815, "excluded_code": "PHASE_AUTHORITY_GAME_NOT_IN_COMMITTED_SNAPSHOT"}
    ]


def test_26_other_phase_failures_still_fail_closed_with_503(monkeypatch):
    from backend.mlb.public_game_predictions.phase_gating_v1 import MoneylinePhaseGateError

    def conflict(row, *, authority):
        raise MoneylinePhaseGateError("MONEYLINE_SOURCE_TYPE_CONFLICT", game_pk=849817)

    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: score())
    monkeypatch.setattr(service, "classify_moneyline_row", conflict)
    response = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-08-06"})
    assert response.status_code == 503


def test_27_pregame_row_is_served_after_its_game_finished(monkeypatch):
    # Snapshot membership is by exact gamePk only; live or final game state
    # never affects the read-time phase join.
    monkeypatch.setenv("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "1")
    row = {**_scored_game(849816, "2026-10-18T00:08:00Z"), "game_date": "2026-10-17",
           "grading_status": "GRADED", "official_winner": "Home Club", "prediction_correct": True}
    monkeypatch.setattr(service, "fetch_prediction_rows", lambda game_date: [row])
    payload = TestClient(app).get("/api/mlb/game-predictions", params={"game_date": "2026-10-17"}).json()
    assert payload["count"] == 1 and "excluded_rows" not in payload
    assert payload["rows"][0]["postseason_round"] == "LEAGUE_CHAMPIONSHIP_SERIES"
