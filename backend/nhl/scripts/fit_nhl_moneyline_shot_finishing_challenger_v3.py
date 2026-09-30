#!/usr/bin/env python3
"""Fit and validate the isolated NHL prior-strength Moneyline V3 shadow model."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

from backend.nhl.cross_market_shadow.core import PARAMETER_PATH, canonical_json, sha256
from backend.nhl.cross_market_shadow.moneyline_challenger import (
    CHALLENGER_FEATURES, CHALLENGER_NAME, FINISHING_WEIGHT, POLICY_VERSION,
    shot_prior_weight,
)

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
OUT = ROOT / "artifacts/analysis/model_development/nhl_moneyline_shot_finishing_challenger_v3/2026-09-30"
MODEL_PATH = ROOT / "backend/nhl/cross_market_shadow/moneyline_shot_finishing_v3.json"
PINNED = {
    2023: "dfaf507014c9a0153dc5cb2b6cfe7643197bd1fe3d3dd472e4a03ba74522055d",
    2024: "90988e9c7110d1c7569848c98995841740593e40ae103a88a49331b4b5bf2c4e",
    2025: "0c80ae9fbd3319db0b558a060a5c4c437798cadd61dac0ba015a48635712cf2c",
}
V2_FEATURES = ["diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg",
               "diff_days_rest", "home_back_to_back", "away_back_to_back"]
V2_PATH = ROOT / "backend/nhl/cross_market_shadow/frozen_control_v2.json"
OUTCOMES = ["final_home_goals", "final_away_goals"]
TEAM_MAP = {"ARI": "UTA", "UTA": "UTA"}
HISTORICAL_TEAM_SPINE = ROOT / "artifacts/analysis/model_development/nhl_moneyline_team_goalie_feature_spine/2026-07-13/nhl_moneyline_team_feature_spine_2026-07-13.csv"
HISTORICAL_MATRIX = ROOT / "artifacts/analysis/model_development/nhl_moneyline_simple_baseline_process_validation/2026-07-13/nhl_moneyline_simple_baseline_feature_matrix_audit_2026-07-13.csv"
HISTORICAL_TEAM_SPINE_SHA256 = "02cbebe71176943942aa2a68b2a572210dfce06029cd32f4225e9962fa50fb65"
HISTORICAL_MATRIX_SHA256 = "63adc75f949bc6d60b531bc083f331a6cc11355a7bd2662de62a7919829d7f90"


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def read_season(year: int) -> pd.DataFrame:
    path = SOURCE / f"corrected_spine_{year}.csv"
    if sha256(path) != PINNED[year]:
        raise RuntimeError(f"CERTIFIED_SOURCE_HASH_MISMATCH:{year}")
    frame = pd.read_csv(path)
    frame = frame[pd.to_numeric(frame.game_type, errors="coerce").eq(2)].copy()
    frame["scheduled_start_time_utc"] = pd.to_datetime(frame.scheduled_start_time_utc, utc=True)
    frame["game_date"] = pd.to_datetime(frame.game_date).dt.date
    for col in ["final_home_goals", "final_away_goals", "home_shots", "away_shots"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=OUTCOMES + ["home_shots", "away_shots"])
    frame["home_team"] = frame.home_team.astype(str).str.upper().map(lambda x: TEAM_MAP.get(x, x))
    frame["away_team"] = frame.away_team.astype(str).str.upper().map(lambda x: TEAM_MAP.get(x, x))
    if len(frame) != 1312 or frame.game_id.duplicated().any():
        raise RuntimeError(f"CERTIFIED_SEASON_GAME_GRAIN_INVALID:{year}:{len(frame)}")
    return frame.sort_values(["scheduled_start_time_utc", "game_id"]).reset_index(drop=True)


def team_games(games: pd.DataFrame) -> pd.DataFrame:
    h = pd.DataFrame({"game_id": games.game_id, "start": games.scheduled_start_time_utc,
        "date": games.game_date, "team": games.home_team, "gf": games.final_home_goals,
        "ga": games.final_away_goals, "sf": games.home_shots, "sa": games.away_shots})
    a = pd.DataFrame({"game_id": games.game_id, "start": games.scheduled_start_time_utc,
        "date": games.game_date, "team": games.away_team, "gf": games.final_away_goals,
        "ga": games.final_home_goals, "sf": games.away_shots, "sa": games.home_shots})
    return pd.concat([h, a], ignore_index=True).sort_values(["team", "start", "game_id"])


def prior_states(games: pd.DataFrame) -> tuple[dict, float]:
    tg = team_games(games)
    league_rate = float(tg.gf.sum() / tg.sf.sum())
    states = {}
    for team, group in tg.groupby("team"):
        states[team] = {
            "shot": float((group.sf - group.sa).mean()),
            "finish": float((group.gf - league_rate * group.sf).sum() / len(group)),
        }
    return states, league_rate


def row_features(game, current: pd.DataFrame, prior: dict, league_rate: float) -> dict:
    all_games = current[current.scheduled_start_time_utc.lt(game.scheduled_start_time_utc)]
    sides = {}
    for side in ["home", "away"]:
        team = getattr(game, f"{side}_team")
        g = all_games[(all_games.home_team.eq(team)) | (all_games.away_team.eq(team))].copy()
        g["gf"] = np.where(g.home_team.eq(team), g.final_home_goals, g.final_away_goals)
        g["ga"] = np.where(g.home_team.eq(team), g.final_away_goals, g.final_home_goals)
        g["sf"] = np.where(g.home_team.eq(team), g.home_shots, g.away_shots)
        g["sa"] = np.where(g.home_team.eq(team), g.away_shots, g.home_shots)
        g = g.sort_values(["scheduled_start_time_utc", "game_id"])
        n = len(g)
        last10 = g.tail(10)
        current_shot = float((g.sf - g.sa).mean()) if n else np.nan
        current_goal = float((g.gf - g.ga).mean()) if n else np.nan
        r10_goal = float((last10.gf - last10.ga).mean()) if n else np.nan
        if n:
            delta = (game.game_date - g.iloc[-1].game_date).days
            rest = float(max(delta - 1, 0))
            b2b = float(delta == 1)
        else:
            rest = b2b = np.nan
        sides[side] = {"team": team, "n": n, "current_shot": current_shot,
                       "goal": current_goal, "r10": r10_goal, "rest": rest, "b2b": b2b,
                       "prior": prior.get(team)}
    home, away = sides["home"], sides["away"]
    v2shot = home["current_shot"] - away["current_shot"]
    blended = {}
    for side in [home, away]:
        state = side["prior"]
        weight = shot_prior_weight(side["n"])
        if state is None:
            value = side["current_shot"]
        elif side["n"] == 0:
            value = state["shot"]
        else:
            value = weight * state["shot"] + (1 - weight) * side["current_shot"]
        blended[id(side)] = (value, weight)
    raw_finish_gap = (home["prior"]["finish"] - away["prior"]["finish"]
                      if home["prior"] is not None and away["prior"] is not None else np.nan)
    feat = {
        "diff_std_goal_diff_pg": home["goal"] - away["goal"],
        "diff_r10_goal_diff_pg": home["r10"] - away["r10"],
        "diff_prior_blended_shot_diff_pg": blended[id(home)][0] - blended[id(away)][0]
            if pd.notna(blended[id(home)][0]) and pd.notna(blended[id(away)][0]) else v2shot,
        "diff_days_rest": home["rest"] - away["rest"],
        "home_back_to_back": home["b2b"], "away_back_to_back": away["b2b"],
        "shrunk_prior_finishing_residual_gap": FINISHING_WEIGHT * raw_finish_gap
            if pd.notna(raw_finish_gap) else np.nan,
    }
    v2 = {"diff_std_goal_diff_pg": feat["diff_std_goal_diff_pg"],
          "diff_r10_goal_diff_pg": feat["diff_r10_goal_diff_pg"],
          "diff_std_shot_diff_pg": v2shot,
          "diff_days_rest": feat["diff_days_rest"],
          "home_back_to_back": feat["home_back_to_back"], "away_back_to_back": feat["away_back_to_back"]}
    prov = {
        "prior_season": int(game.canonical_season) - 1,
        "prior_home_full_shot_diff_pg": home["prior"]["shot"] if home["prior"] else np.nan,
        "prior_away_full_shot_diff_pg": away["prior"]["shot"] if away["prior"] else np.nan,
        "current_home_shot_diff_pg": home["current_shot"], "current_away_shot_diff_pg": away["current_shot"],
        "current_home_games": home["n"], "current_away_games": away["n"],
        "home_shot_prior_weight": blended[id(home)][1], "away_shot_prior_weight": blended[id(away)][1],
        "blended_home_shot_diff_pg": blended[id(home)][0], "blended_away_shot_diff_pg": blended[id(away)][0],
        "v2_shot_diff_gap": v2shot,
        "prior_home_finishing_residual": home["prior"]["finish"] if home["prior"] else np.nan,
        "prior_away_finishing_residual": away["prior"]["finish"] if away["prior"] else np.nan,
        "raw_finishing_residual_gap": raw_finish_gap,
        "shrunk_finishing_residual_gap": feat["shrunk_prior_finishing_residual_gap"],
        "league_goals_per_shot": league_rate,
        "home_latest_prior_start_utc": all_games[(all_games.home_team.eq(home["team"])) | (all_games.away_team.eq(home["team"]))].scheduled_start_time_utc.max(),
        "away_latest_prior_start_utc": all_games[(all_games.home_team.eq(away["team"])) | (all_games.away_team.eq(away["team"]))].scheduled_start_time_utc.max(),
    }
    return feat, v2, prov


def build_population(year: int, games: dict[int, pd.DataFrame], states: dict, rates: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    current, prior = games[year], states[year - 1]
    rows, provenance = [], []
    if year == 2024:
        # Preserve the certified, audited V2 feature construction exactly for
        # the training season. Add only strict-prior season-level policy fields.
        if (sha256(HISTORICAL_TEAM_SPINE) != HISTORICAL_TEAM_SPINE_SHA256
                or sha256(HISTORICAL_MATRIX) != HISTORICAL_MATRIX_SHA256):
            raise RuntimeError("CERTIFIED_V2_FEATURE_SOURCE_HASH_MISMATCH")
        team_spine = pd.read_csv(HISTORICAL_TEAM_SPINE)
        team_spine = team_spine[team_spine.canonical_season.eq(year)].copy()
        matrix = pd.read_csv(HISTORICAL_MATRIX)
        matrix = matrix[matrix.canonical_season.eq(year)].copy()
        feature = team_spine.merge(matrix, on=["canonical_season", "game_id"],
                                   how="inner", validate="one_to_one", suffixes=("", "_audit"))
        official = current[["game_id", "scheduled_start_time_utc", "home_team", "away_team",
                            "final_home_goals", "final_away_goals"]]
        feature = feature.merge(official, on="game_id", how="inner", validate="one_to_one",
                                suffixes=("_spine", "_source"))
        if len(feature) != 1312:
            raise RuntimeError(f"CERTIFIED_2024_FEATURE_SPINE_COVERAGE_INVALID:{len(feature)}")
        for g in feature.itertuples(index=False):
            if (str(g.home_team_spine) != str(g.home_team_source)
                    or str(g.away_team_spine) != str(g.away_team_source)
                    or int(g.final_home_goals_spine) != int(g.final_home_goals_source)
                    or int(g.final_away_goals_spine) != int(g.final_away_goals_source)):
                raise RuntimeError(f"CERTIFIED_FEATURE_OUTCOME_IDENTITY_CONFLICT:{g.game_id}")
            v2 = {f: getattr(g, f"raw__{f}") for f in V2_FEATURES}
            home_n, away_n = int(g.home_prior_games), int(g.away_prior_games)
            home_current, away_current = g.home_std_shot_diff_pg, g.away_std_shot_diff_pg
            home_prior, away_prior = prior.get(str(g.home_team_spine)), prior.get(str(g.away_team_spine))
            home_w, away_w = shot_prior_weight(home_n), shot_prior_weight(away_n)
            def blend(state, current_value, n, weight):
                if state is None:
                    return current_value
                if n == 0:
                    return state["shot"]
                if pd.isna(current_value):
                    return state["shot"]
                return weight * state["shot"] + (1-weight) * current_value
            home_blended = blend(home_prior, home_current, home_n, home_w)
            away_blended = blend(away_prior, away_current, away_n, away_w)
            finish_gap = (home_prior["finish"]-away_prior["finish"]
                          if home_prior is not None and away_prior is not None else np.nan)
            features = {
                "diff_std_goal_diff_pg": v2["diff_std_goal_diff_pg"],
                "diff_r10_goal_diff_pg": v2["diff_r10_goal_diff_pg"],
                "diff_prior_blended_shot_diff_pg": home_blended-away_blended
                    if pd.notna(home_blended) and pd.notna(away_blended) else v2["diff_std_shot_diff_pg"],
                "diff_days_rest": v2["diff_days_rest"],
                "home_back_to_back": v2["home_back_to_back"],
                "away_back_to_back": v2["away_back_to_back"],
                "shrunk_prior_finishing_residual_gap": FINISHING_WEIGHT*finish_gap if pd.notna(finish_gap) else np.nan,
            }
            rows.append({"canonical_season": year, "game_id": int(g.game_id),
                         "scheduled_start_time_utc": g.scheduled_start_time_utc,
                         "home_team": g.home_team_spine, "away_team": g.away_team_spine,
                         "home_win": int(g.final_home_goals_source > g.final_away_goals_source),
                         "home_goal_margin": float(g.final_home_goals_source-g.final_away_goals_source),
                         "max_prior_games": max(home_n, away_n), **features,
                         **{f"v2__{k}": value for k, value in v2.items()}})
            provenance.append({"canonical_season": year, "game_id": int(g.game_id),
                "prior_season": year-1, "prior_home_full_shot_diff_pg": home_prior["shot"] if home_prior else np.nan,
                "prior_away_full_shot_diff_pg": away_prior["shot"] if away_prior else np.nan,
                "current_home_shot_diff_pg": home_current, "current_away_shot_diff_pg": away_current,
                "current_home_games": home_n, "current_away_games": away_n,
                "home_shot_prior_weight": home_w, "away_shot_prior_weight": away_w,
                "blended_home_shot_diff_pg": home_blended, "blended_away_shot_diff_pg": away_blended,
                "v2_shot_diff_gap": v2["diff_std_shot_diff_pg"],
                "prior_home_finishing_residual": home_prior["finish"] if home_prior else np.nan,
                "prior_away_finishing_residual": away_prior["finish"] if away_prior else np.nan,
                "raw_finishing_residual_gap": finish_gap,
                "shrunk_finishing_residual_gap": features["shrunk_prior_finishing_residual_gap"],
                "league_goals_per_shot": rates[year-1],
                "home_latest_prior_start_utc": pd.NA, "away_latest_prior_start_utc": pd.NA})
        return pd.DataFrame(rows), pd.DataFrame(provenance)
    if year == 2025:
        # The corrected source spine carries certified V2 strict-prior features
        # and the per-team counts/states used by that feature contract.
        for g in current.itertuples(index=False):
            v2 = {feature: getattr(g, feature) for feature in V2_FEATURES}
            home_n, away_n = int(g.home_prior_games), int(g.away_prior_games)
            home_current, away_current = g.home_std_shot_diff_pg, g.away_std_shot_diff_pg
            home_prior, away_prior = prior.get(str(g.home_team)), prior.get(str(g.away_team))
            home_w, away_w = shot_prior_weight(home_n), shot_prior_weight(away_n)
            def blend(state, current_value, n, weight):
                if state is None:
                    return current_value
                if n == 0 or pd.isna(current_value):
                    return state["shot"]
                return weight * state["shot"] + (1-weight) * current_value
            home_blended = blend(home_prior, home_current, home_n, home_w)
            away_blended = blend(away_prior, away_current, away_n, away_w)
            finish_gap = (home_prior["finish"]-away_prior["finish"]
                          if home_prior is not None and away_prior is not None else np.nan)
            features = {
                "diff_std_goal_diff_pg": v2["diff_std_goal_diff_pg"],
                "diff_r10_goal_diff_pg": v2["diff_r10_goal_diff_pg"],
                "diff_prior_blended_shot_diff_pg": home_blended-away_blended
                    if pd.notna(home_blended) and pd.notna(away_blended) else v2["diff_std_shot_diff_pg"],
                "diff_days_rest": v2["diff_days_rest"],
                "home_back_to_back": v2["home_back_to_back"],
                "away_back_to_back": v2["away_back_to_back"],
                "shrunk_prior_finishing_residual_gap": FINISHING_WEIGHT*finish_gap if pd.notna(finish_gap) else np.nan,
            }
            rows.append({"canonical_season": year, "game_id": int(g.game_id),
                         "scheduled_start_time_utc": g.scheduled_start_time_utc,
                         "home_team": g.home_team, "away_team": g.away_team,
                         "home_win": int(g.final_home_goals > g.final_away_goals),
                         "home_goal_margin": float(g.final_home_goals-g.final_away_goals),
                         "max_prior_games": max(home_n, away_n), **features,
                         **{f"v2__{k}": value for k, value in v2.items()}})
            provenance.append({"canonical_season": year, "game_id": int(g.game_id),
                "prior_season": year-1, "prior_home_full_shot_diff_pg": home_prior["shot"] if home_prior else np.nan,
                "prior_away_full_shot_diff_pg": away_prior["shot"] if away_prior else np.nan,
                "current_home_shot_diff_pg": home_current, "current_away_shot_diff_pg": away_current,
                "current_home_games": home_n, "current_away_games": away_n,
                "home_shot_prior_weight": home_w, "away_shot_prior_weight": away_w,
                "blended_home_shot_diff_pg": home_blended, "blended_away_shot_diff_pg": away_blended,
                "v2_shot_diff_gap": v2["diff_std_shot_diff_pg"],
                "prior_home_finishing_residual": home_prior["finish"] if home_prior else np.nan,
                "prior_away_finishing_residual": away_prior["finish"] if away_prior else np.nan,
                "raw_finishing_residual_gap": finish_gap,
                "shrunk_finishing_residual_gap": features["shrunk_prior_finishing_residual_gap"],
                "league_goals_per_shot": rates[year-1],
                "home_latest_prior_start_utc": g.home_latest_prior_start_utc,
                "away_latest_prior_start_utc": g.away_latest_prior_start_utc})
        return pd.DataFrame(rows), pd.DataFrame(provenance)
    for game in current.itertuples(index=False):
        features, v2, prov = row_features(game, current, prior, rates[year - 1])
        rows.append({"canonical_season": year, "game_id": int(game.game_id),
                     "scheduled_start_time_utc": game.scheduled_start_time_utc,
                     "home_team": game.home_team, "away_team": game.away_team,
                     "home_win": int(game.final_home_goals > game.final_away_goals),
                     "home_goal_margin": float(game.final_home_goals - game.final_away_goals),
                     "max_prior_games": max(prov["current_home_games"], prov["current_away_games"]),
                     **features, **{f"v2__{k}": v for k, v in v2.items()}})
        provenance.append({"canonical_season": year, "game_id": int(game.game_id), **prov})
    return pd.DataFrame(rows), pd.DataFrame(provenance)


def metrics(y, p):
    p = np.clip(np.asarray(p, float), 1e-12, 1 - 1e-12)
    y = np.asarray(y, int)
    conf = np.maximum(p, 1-p)
    corr = ((p >= .5).astype(int) == y).astype(float)
    bins = pd.cut(conf, np.linspace(0, 1, 11), include_lowest=True)
    ece = sum(len(g)/len(y)*abs(g.correct.mean()-g.confidence.mean()) for _, g in
              pd.DataFrame({"correct": corr, "confidence": conf, "bin": bins}).groupby("bin", observed=False) if len(g))
    return {"n": len(y), "brier": float(brier_score_loss(y, p)),
            "log_loss": float(log_loss(y, p, labels=[0, 1])),
            "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else np.nan,
            "accuracy": float(accuracy_score(y, p >= .5)), "ece": float(ece)}


def predict_v2(frame: pd.DataFrame, parameters: dict) -> np.ndarray:
    vals = frame[V2_FEATURES].copy()
    for feature in V2_FEATURES:
        vals[feature] = pd.to_numeric(vals[feature], errors="coerce").fillna(parameters["imputation_medians"][feature])
        vals[feature] = (vals[feature] - parameters["standardization_means"][feature]) / parameters["standardization_scales"][feature]
    logits = parameters["intercept"] + sum(vals[f] * parameters["coefficients"][f] for f in V2_FEATURES)
    return 1 / (1 + np.exp(-np.clip(logits, -709, 709)))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    games = {y: read_season(y) for y in (2023, 2024, 2025)}
    states_rates = {y: prior_states(games[y]) for y in (2023, 2024, 2025)}
    states = {y: states_rates[y][0] for y in states_rates}
    rates = {y: states_rates[y][1] for y in states_rates}
    populations, provenance = {}, {}
    for y in (2024, 2025):
        populations[y], provenance[y] = build_population(y, games, states, rates)
        populations[y].to_csv(OUT / f"season_{y}_feature_matrix.csv", index=False)
        provenance[y].to_csv(OUT / f"season_{y}_feature_provenance.csv", index=False)

    train, test = populations[2024], populations[2025]
    X_train = train[CHALLENGER_FEATURES].astype(float).replace([np.inf, -np.inf], np.nan)
    X_test = test[CHALLENGER_FEATURES].astype(float).replace([np.inf, -np.inf], np.nan)
    imputer = SimpleImputer(strategy="median").fit(X_train)
    train_imp, test_imp = imputer.transform(X_train), imputer.transform(X_test)
    scaler = StandardScaler().fit(train_imp)
    train_scaled, test_scaled = scaler.transform(train_imp), scaler.transform(test_imp)
    clf = LogisticRegression(C=1.0, max_iter=1000, solver="liblinear", penalty="l2",
                            fit_intercept=True, random_state=20260713, tol=.0001)
    clf.fit(train_scaled, train.home_win.astype(int))
    p_train = clf.predict_proba(train_scaled)[:, 1]
    p_test = clf.predict_proba(test_scaled)[:, 1]

    # Freeze the full fitted preprocessing and coefficient state.
    policy = {"shot_prior_weights_by_current_games": {"0-3": 1.0, "4-5": .75, "6-10": .5, ">10": 0.0},
              "prior_finishing_weight": .5, "franchise_mapping": TEAM_MAP,
              "history_contract": "same-season type-2 final games with scheduled start strictly earlier than target; preceding season type-2 final only"}
    policy_sha = digest_bytes(canonical_json(policy).encode())
    model = {
        "model_version": CHALLENGER_NAME, "feature_policy_version": POLICY_VERSION,
        "feature_order": CHALLENGER_FEATURES, "v2_feature_order": V2_FEATURES,
        "finishing_weight": FINISHING_WEIGHT, "prior_season": 2024,
        "training_season": 2024, "training_rows": len(train),
        "training_game_ids_sha256": digest_bytes(train.game_id.astype(str).sort_values().str.cat(sep="\n").encode()),
        "training_matrix_sha256": digest_bytes(train[CHALLENGER_FEATURES + ["home_win"]].to_csv(index=False, float_format="%.15g", lineterminator="\n").encode()),
        "training_outcome_contract": "corrected certified regular-season type-2 final home-winner target",
        "training_source_spine_sha256": {str(y): PINNED[y] for y in (2023, 2024)},
        "training_v2_feature_spine_sha256": sha256(HISTORICAL_TEAM_SPINE),
        "training_v2_feature_matrix_sha256": sha256(HISTORICAL_MATRIX),
        "heldout_source_spine_sha256": PINNED[2025],
        "frozen_v2_parameter_sha256": sha256(V2_PATH),
        "fit_population_note": "All 1,312 certified 2024 regular-season outcomes; differs from frozen V2's 701-game 2023 fit because complete strict-prior preceding-season shot/finishing inputs are available beginning with 2024.",
        "preprocessing": "fit-only median imputation then StandardScaler",
        "model_configuration": {"family": "logistic_regression", "C": 1.0, "penalty": "l2", "fit_intercept": True,
                                "max_iter": 1000, "solver": "liblinear", "random_state": 20260713, "tol": .0001},
        "imputation_medians": dict(zip(CHALLENGER_FEATURES, map(float, imputer.statistics_))),
        "standardization_means": dict(zip(CHALLENGER_FEATURES, map(float, scaler.mean_))),
        "standardization_scales": dict(zip(CHALLENGER_FEATURES, map(float, scaler.scale_))),
        "intercept": float(clf.intercept_[0]),
        "coefficients": dict(zip(CHALLENGER_FEATURES, map(float, clf.coef_[0]))),
        "policy": policy, "policy_sha256": policy_sha,
        "strict_prior": True, "production_promotion": False,
    }
    model["parameter_content_sha256"] = digest_bytes(canonical_json(model).encode())
    MODEL_PATH.write_text(json.dumps(model, indent=2, sort_keys=True) + "\n")
    model["parameter_file_sha256"] = sha256(MODEL_PATH)

    v2_parameters = json.loads(V2_PATH.read_text())
    rows = []
    for year, probabilities in [(2024, p_train), (2025, p_test)]:
        pop = populations[year]
        # Challenger is in-sample in 2024 and out-of-sample in 2025.
        v2p = predict_v2(pd.DataFrame({k: pop[f"v2__{k}"] for k in V2_FEATURES}), v2_parameters)
        for model_name, p in [("V2_REFERENCE", v2p), (CHALLENGER_NAME, probabilities)]:
            rows.extend({"canonical_season": year, "game_id": int(gid), "model": model_name,
                         "home_win": int(y), "probability": float(prob),
                         "max_prior_games": int(depth), "home_goal_margin": float(margin),
                         "evaluation_status": "TRAIN_IN_SAMPLE" if year == 2024 and model_name == CHALLENGER_NAME else
                             "FROZEN_REFERENCE" if model_name == "V2_REFERENCE" else "OUT_OF_SAMPLE_HOLDOUT"}
                        for gid, y, prob, depth, margin in zip(pop.game_id, pop.home_win, p, pop.max_prior_games, pop.home_goal_margin))
    prediction = pd.DataFrame(rows)
    prediction.to_csv(OUT / "historical_v2_challenger_predictions.csv", index=False)
    metric_rows = []
    for year, group in prediction.groupby("canonical_season"):
        for model_name, sub in group.groupby("model"):
            metric_rows.append({"season": year, "model": model_name,
                                "evaluation_status": sub.evaluation_status.iloc[0],
                                **metrics(sub.home_win, sub.probability)})
            for bucket, mask in [("0-1", sub.max_prior_games.le(1)), ("<=3", sub.max_prior_games.le(3)),
                                 ("<=5", sub.max_prior_games.le(5)), ("<=10", sub.max_prior_games.le(10)),
                                 (">10", sub.max_prior_games.gt(10))]:
                if mask.sum():
                    metric_rows.append({"season": year, "model": model_name,
                        "evaluation_status": sub.evaluation_status.iloc[0], "history_bucket": bucket,
                        **metrics(sub.loc[mask, "home_win"], sub.loc[mask, "probability"])})
    pd.DataFrame(metric_rows).to_csv(OUT / "historical_metrics.csv", index=False)
    paired = prediction.pivot(index=["canonical_season", "game_id"], columns="model", values="probability").reset_index()
    paired["probability_delta_challenger_minus_v2"] = paired[CHALLENGER_NAME] - paired["V2_REFERENCE"]
    paired["absolute_probability_delta"] = paired.probability_delta_challenger_minus_v2.abs()
    paired["side_changed"] = paired[CHALLENGER_NAME].ge(.5) != paired["V2_REFERENCE"].ge(.5)
    paired.to_csv(OUT / "historical_probability_deltas.csv", index=False)
    pooled_rows = []
    for model_name, sub in prediction.groupby("model"):
        pooled_rows.append({"period": "2024_2025_POOLED", "model": model_name,
                            "evaluation_status": "MIXED_TRAIN_AND_HOLDOUT" if model_name == CHALLENGER_NAME else "FROZEN_REFERENCE",
                            **metrics(sub.home_win, sub.probability)})
    pd.DataFrame(pooled_rows).to_csv(OUT / "pooled_metrics.csv", index=False)
    movement_rows = []
    for season, sub in paired.groupby("canonical_season"):
        candidate = prediction[(prediction.canonical_season.eq(season)) & prediction.model.eq(CHALLENGER_NAME)]
        movement_rows.append({"season": int(season), "games": len(sub),
                              "mean_probability_delta": sub.probability_delta_challenger_minus_v2.mean(),
                              "mean_absolute_probability_delta": sub.absolute_probability_delta.mean(),
                              "p95_absolute_probability_delta": sub.absolute_probability_delta.quantile(.95),
                              "maximum_absolute_probability_delta": sub.absolute_probability_delta.max(),
                              "side_changes": int(sub.side_changed.sum()),
                              "challenger_probabilities_below_0_05": int(candidate.probability.lt(.05).sum()),
                              "challenger_probabilities_above_0_95": int(candidate.probability.gt(.95).sum())})
    pd.DataFrame(movement_rows).to_csv(OUT / "probability_movement_summary.csv", index=False)
    summary = {"model_version": CHALLENGER_NAME, "policy_version": POLICY_VERSION,
               "training_rows": len(train), "holdout_rows": len(test),
               "v2_reference_parameters_unchanged": sha256(V2_PATH),
               "candidate_parameter_file_sha256": model["parameter_file_sha256"],
               "training_season_metrics_are_in_sample": True,
               "holdout_season": 2025, "production_changes": 0, "puck_line_changes": 0,
               "promotion": False, "provider_calls": 0, "wagers": 0}
    (OUT / "study_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
