from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    REPO_ROOT,
    SCHEDULE_FIELDS,
    TotalsLiveContextError,
    attach_context,
    canonical_hash,
    load_candidate,
    normalize_schedule,
    score_context,
)
from tmp.analysis.run_mlb_totals_prediction_representative_rerun_v1 import strict_team

FIXTURE = REPO_ROOT / "backend/mlb/tests/fixtures/totals_live_context_v1/hydrated_schedule.json"
MONEYLINE_CONFIG = REPO_ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
SPINE = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06/totals_core_feature_spine.csv"
EXPECTED_MONEYLINE_HASH = "afc257a5ede1c5bc352dcb1e990b710272d472cd62d2dadfb7dafb7254b35722"


def fixture_payload():
    return json.loads(FIXTURE.read_text())


def fake_history():
    park = {"venue_id": 2681, "park_name": "Citizens Bank Park", "park_history_depth": 100,
            "strict_prior_total_run_factor": 1.02, "fallback_status": "DIRECT_REGRESSED_PARK_HISTORY",
            "roof_type": "Open", "elevation": 20, "latest_included_game_id": 899900}
    return {"league_total": 8.8, "league_home": 4.5, "league_away": 4.3, "teams": {}, "appearances": {},
            "team_starts": {}, "league_starts": [], "team_relievers": {}, "parks": {2681: park}, "core": pd.DataFrame()}


def starter_record(pitcher_id, team_id, game_pk, day, outs=15, runs=2):
    return {"game_pk": game_pk, "date": pd.Timestamp(day).date(), "pitcher_id": pitcher_id,
            "team_id": team_id, "is_starter": True, "outs": outs, "batters_faced": 21,
            "pitches": 85, "runs": runs, "earned_runs": runs, "hits": 5, "walks": 2,
            "strikeouts": 5, "home_runs": 1}


def history_with_starts(away_count, home_count):
    history = fake_history()
    away = [starter_record(571945, 120, 800000+i, f"2026-07-{i+1:02d}") for i in range(away_count)]
    home = [starter_record(650911, 143, 810000+i, f"2026-07-{i+1:02d}") for i in range(home_count)]
    team_away = [starter_record(700001, 120, 820000+i, f"2026-06-{i+1:02d}") for i in range(5)]
    team_home = [starter_record(700002, 143, 830000+i, f"2026-06-{i+1:02d}") for i in range(5)]
    history["appearances"] = {571945: away, 650911: home}
    history["team_starts"] = {120: team_away + away, 143: team_home + home}
    history["league_starts"] = team_away + team_home + away + home
    return history


def test_hydrated_parser_preserves_probables_venue_and_doubleheader_identity():
    rows = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")
    assert [(r["game_pk"], r["game_number"]) for r in rows] == [(900001, 1), (900002, 2)]
    assert rows[0]["away_probable_pitcher_id"] == 571945
    assert rows[0]["home_probable_pitcher_id"] == 650911
    assert rows[0]["venue_id"] == 2681
    assert all(r["doubleheader_state"] == "Y" for r in rows)


def test_starter_and_park_attach_to_exact_team_game():
    row = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0]
    context = attach_context(row, fake_history())
    assert context["away_starter_state"]["probable_pitcher_id"] == row["away_probable_pitcher_id"]
    assert context["home_starter_state"]["probable_pitcher_id"] == row["home_probable_pitcher_id"]
    assert context["park_state"]["park_history_depth"] == 100
    assert context["park_state"]["park_factor"] == 1.02


def test_resolved_probables_with_direct_history_are_complete():
    row = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0]
    history = history_with_starts(3, 3)
    context = attach_context(row, history)
    assert context["data_quality_status"] == "TOTALS_CONTEXT_COMPLETE"
    assert context["starter_history_quality_state"] == "DIRECT_STARTER_HISTORY_BOTH"
    assert score_context(context, history, load_candidate(), "2026-08-06T19:00:00Z")["expected_total"] > 0


@pytest.mark.parametrize("sparse_starts", [1, 2])
def test_resolved_probable_with_one_or_two_starts_uses_governed_fallback_and_is_complete(sparse_starts):
    row = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0]
    history = history_with_starts(sparse_starts, 3)
    context = attach_context(row, history)
    assert context["data_quality_status"] == "TOTALS_CONTEXT_COMPLETE"
    assert context["away_starter_state"]["fallback_tier"] == "PITCHER_ROLE_COHORT"
    assert context["away_starter_state"]["prior_starts"] == sparse_starts
    assert context["starter_history_quality_state"] == "GOVERNED_SPARSE_STARTER_HISTORY"
    assert score_context(context, history, load_candidate(), "2026-08-06T19:00:00Z")["expected_total"] > 0


def test_resolved_probable_with_zero_starts_uses_team_fallback_and_is_complete():
    row = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0]
    history = history_with_starts(0, 3)
    context = attach_context(row, history)
    assert context["data_quality_status"] == "TOTALS_CONTEXT_COMPLETE"
    assert context["away_starter_state"]["fallback_tier"] == "TEAM_STARTER_HISTORY"
    assert context["away_starter_state"]["prior_starts"] == 0
    assert context["away_starter_state"]["certification_status"] == "STRICT_PRIOR_STARTER_STATE"
    assert context["starter_history_fallback_tier"].startswith("away=TEAM_STARTER_HISTORY|")
    assert score_context(context, history, load_candidate(), "2026-08-06T19:00:00Z")["expected_total"] > 0


def test_missing_probable_uses_governed_starter_fallback():
    payload = fixture_payload(); del payload["dates"][0]["games"][0]["teams"]["away"]["probablePitcher"]
    row = normalize_schedule(payload, "2026-08-06T19:00:00Z", "abc")[0]
    context = attach_context(row, fake_history())
    assert row["away_probable_pitcher_status"] == "PROBABLE_PITCHER_UNAVAILABLE"
    assert context["away_starter_state"]["certification_status"] == "GOVERNED_STARTER_FALLBACK"
    assert context["data_quality_status"] == "TOTALS_CONTEXT_PARTIAL_FALLBACK"


def test_missing_probable_remains_retryable_by_identity_state():
    payload = fixture_payload(); del payload["dates"][0]["games"][0]["teams"]["away"]["probablePitcher"]
    row = normalize_schedule(payload, "2026-08-06T19:00:00Z", "abc")[0]
    context = attach_context(row, history_with_starts(0, 3))
    assert row["away_probable_pitcher_status"] == "PROBABLE_PITCHER_UNAVAILABLE"
    assert context["data_quality_status"] == "TOTALS_CONTEXT_PARTIAL_FALLBACK"


def test_missing_venue_uses_governed_park_fallback():
    payload = fixture_payload(); del payload["dates"][0]["games"][0]["venue"]
    context = attach_context(normalize_schedule(payload, "2026-08-06T19:00:00Z", "abc")[0], fake_history())
    assert context["park_state"]["fallback_status"] == "LEAGUE_PARK_FALLBACK"
    assert context["data_quality_status"] == "TOTALS_CONTEXT_PARTIAL_FALLBACK"


def test_unresolved_pitcher_identity_is_not_promoted():
    payload = fixture_payload(); del payload["dates"][0]["games"][0]["teams"]["away"]["probablePitcher"]["id"]
    context = attach_context(normalize_schedule(payload, "2026-08-06T19:00:00Z", "abc")[0], fake_history())
    assert context["data_quality_status"] == "TOTALS_CONTEXT_UNRESOLVED"


def test_post_start_game_fails_closed_and_no_outcome_fields_requested():
    row = normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0]
    context = attach_context(row, fake_history())
    with pytest.raises(TotalsLiveContextError, match="POST_START_GAME_NOT_ELIGIBLE"):
        score_context(context, fake_history(), load_candidate(), "2026-08-06T23:00:00Z")
    assert not {"score", "runs", "iswinner"} & set(SCHEDULE_FIELDS.lower().split(","))


def test_feature_cutoffs_are_strictly_pregame():
    context = attach_context(normalize_schedule(fixture_payload(), "2026-08-06T19:00:00Z", "abc")[0], fake_history())
    assert context["away_starter_state"]["feature_cutoff_utc"] == context["scheduled_start_utc"]
    assert context["home_starter_state"]["feature_cutoff_utc"] == context["scheduled_start_utc"]
    assert context["park_state"]["feature_cutoff_utc"] == context["scheduled_start_utc"]
    assert context["away_starter_state"]["latest_included_game_date"] is None
    assert context["home_starter_state"]["latest_included_game_date"] is None
    assert context["park_state"]["latest_included_game_id"] < context["game_pk"]


def test_frozen_candidate_reconstructs_exact_coefficients_alpha_and_order():
    candidate = load_candidate()
    data = pd.read_csv(SPINE); data["game_date"] = pd.to_datetime(data.game_date); data["scheduled_start_utc"] = pd.to_datetime(data.scheduled_start_utc, utc=True)
    data = data.merge(strict_team(data), on="game_pk", how="left")
    data["home_starter_ra9"] = data.home_starter_season_ra9.fillna(data.league_total / 2)
    data["away_starter_ra9"] = data.away_starter_season_ra9.fillna(data.league_total / 2)
    data["home_bullpen_ra9"] = data.home_bullpen_bullpen_ra9.fillna(data.league_total / 2)
    data["away_bullpen_ra9"] = data.away_bullpen_bullpen_ra9.fillna(data.league_total / 2)
    cols = candidate["feature_order"]; data[cols] = data[cols].replace([np.inf, -np.inf], np.nan).fillna(0)
    train = data[data.game_date.dt.year <= 2024]
    model = Pipeline([("s", StandardScaler()), ("p", PoissonRegressor(alpha=.1, max_iter=1000))]).fit(train[cols], train.final_total)
    mu = model.predict(train[cols]); alpha = max(0, float((((train.final_total - mu) ** 2 - train.final_total).sum()) / np.maximum((mu ** 2).sum(), 1)))
    assert model["p"].intercept_ == candidate["intercept"]
    assert np.array_equal(model["p"].coef_, np.array(candidate["coefficients"]))
    assert np.array_equal(model["s"].mean_, np.array(candidate["scaler_mean"]))
    assert np.array_equal(model["s"].scale_, np.array(candidate["scaler_scale"]))
    assert alpha == candidate["dispersion_alpha"] == 0.12944479977012996
    assert canonical_hash({k: candidate[k] for k in candidate if k != "canonical_model_hash"}) == candidate["canonical_model_hash"]


def test_moneyline_candidate_bytes_unchanged():
    assert hashlib.sha256(MONEYLINE_CONFIG.read_bytes()).hexdigest() == EXPECTED_MONEYLINE_HASH
