#!/usr/bin/env python3
"""Bounded, research-only Pinnacle-anchored MLB moneyline residual experiment."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from backend.app.deps import pg_connect

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_pinnacle_anchored_moneyline_residual_v1/2026-08-11"
JOIN = ROOT / "artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/moneyline_pinnacle_join.csv"
STATE = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv"
SEED = 20260811
DEV_END, VAL_END = "2026-06-16", "2026-07-02"

STATE_FEATURES = [
    "home_games", "away_games", "home_wp", "away_wp", "home_rs", "away_rs",
    "home_ra", "away_ra", "home_last10_wp", "away_last10_wp",
    "home_last10_diff", "away_last10_diff", "home_rest", "away_rest",
    "league_total", "month_sin", "month_cos", "starter_state_available",
    "bullpen_state_available", "park_state_available", "weather_state_available",
    "lineup_state_available", "doubleheader_state_available",
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def clip(p):
    return np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)


def logit(p):
    p = clip(p)
    return np.log(p / (1 - p))


def sigmoid(x):
    return 1 / (1 + np.exp(-np.clip(np.asarray(x, dtype=float), -30, 30)))


def ece(y, p, bins=10):
    y, p = np.asarray(y, int), clip(p)
    edges = np.linspace(0, 1, bins + 1)
    ids = np.minimum(np.digitize(p, edges[1:-1], right=False), bins - 1)
    return float(sum(np.mean(ids == i) * abs(np.mean(p[ids == i]) - np.mean(y[ids == i]))
                     for i in range(bins) if np.any(ids == i)))


def metrics(y, p):
    y, p = np.asarray(y, int), clip(p)
    return {
        "games": len(y), "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])), "ece_10": ece(y, p),
        "accuracy": float(np.mean((p > .5) == y)), "probability_mean": float(np.mean(p)),
        "probability_sd": float(np.std(p)), "probability_iqr": float(np.quantile(p, .75) - np.quantile(p, .25)),
        "observed_home_win_rate": float(np.mean(y)),
    }


class OffsetLogistic:
    """L2-regularized logistic correction with fixed market-logit offset."""

    def __init__(self, c=.25):
        self.c = c

    def fit(self, x, y, offset):
        x = np.asarray(x, float); y = np.asarray(y, int); offset = np.asarray(offset, float)
        self.imputer_ = SimpleImputer(strategy="median").fit(x)
        xi = self.imputer_.transform(x)
        self.scaler_ = StandardScaler().fit(xi)
        z = self.scaler_.transform(xi)

        def objective(beta):
            eta = offset + beta[0] + z @ beta[1:]
            loss = np.logaddexp(0, eta).sum() - np.dot(y, eta)
            penalty = .5 / self.c * np.dot(beta[1:], beta[1:])
            return loss + penalty

        result = minimize(objective, np.zeros(z.shape[1] + 1), method="L-BFGS-B")
        if not result.success:
            raise RuntimeError(f"OFFSET_LOGISTIC_FIT_FAILED:{result.message}")
        self.intercept_, self.coef_ = float(result.x[0]), result.x[1:]
        return self

    def correction(self, x):
        z = self.scaler_.transform(self.imputer_.transform(np.asarray(x, float)))
        return self.intercept_ + z @ self.coef_

    def predict_proba(self, x, offset):
        return sigmoid(np.asarray(offset, float) + self.correction(x))


def population():
    market = pd.read_csv(JOIN)
    state = pd.read_csv(STATE)
    keep = ["game_pk", "home_team_id", "away_team_id"] + STATE_FEATURES
    d = market.merge(state[keep], on="game_pk", how="left", validate="one_to_one")
    d["scheduled_start_utc"] = pd.to_datetime(d.scheduled_start_utc, utc=True)
    d["provider_snapshot_utc"] = pd.to_datetime(d.provider_snapshot_utc, utc=True)
    d["market_last_update"] = pd.to_datetime(d.market_last_update, utc=True)
    d["snapshot_lead_minutes"] = (d.scheduled_start_utc - d.provider_snapshot_utc).dt.total_seconds() / 60
    valid = (
        d.mapping_status.astype(str).str.startswith("EXACT")
        & d.snapshot_lead_minutes.gt(0)
        & d.market_last_update.lt(d.scheduled_start_utc)
        & d.pinnacle_home_price.notna() & d.pinnacle_away_price.notna()
        & d.pinnacle_home_no_vig_probability.between(0, 1, inclusive="neither")
        & d.winner_home.isin([0, 1])
    )
    rejected = d.loc[~valid, ["game_pk", "mapping_status", "snapshot_lead_minutes"]]
    if len(rejected):
        raise RuntimeError(f"ACCEPTED_POPULATION_FAIL_CLOSED_REJECTIONS:{len(rejected)}")
    d = d.loc[valid].sort_values(["scheduled_start_utc", "game_pk"]).reset_index(drop=True)
    if len(d) != 764 or d.game_pk.duplicated().any():
        raise RuntimeError(f"ACCEPTED_POPULATION_MISMATCH:rows={len(d)} duplicates={d.game_pk.duplicated().sum()}")
    d["market_logit"] = logit(d.pinnacle_home_no_vig_probability)
    d["temporal_split"] = np.select(
        [d.game_date <= DEV_END, d.game_date <= VAL_END],
        ["DEVELOPMENT", "VALIDATION"], default="FINAL_HOLDOUT")
    manifest_cols = [
        "game_pk", "game_date", "scheduled_start_utc", "home_team_abbr", "away_team_abbr",
        "pinnacle_home_price", "pinnacle_away_price", "pinnacle_home_raw_probability",
        "pinnacle_away_raw_probability", "pinnacle_home_no_vig_probability",
        "pinnacle_away_no_vig_probability", "winner_home", "official_winner",
        "requested_snapshot_utc", "provider_snapshot_utc", "market_last_update",
        "snapshot_lead_minutes", "mapping_status", "source_sha256", "temporal_split",
    ]
    d[manifest_cols].to_csv(OUT / "moneyline_residual_population_manifest.csv", index=False)
    counts = d.temporal_split.value_counts()
    contract = {
        "experiment": "MLB_PINNACLE_ANCHORED_MONEYLINE_RESIDUAL_V1",
        "ordering": "scheduled_start_utc, game_pk; whole game dates",
        "development": {"through": DEV_END, "games": int(counts["DEVELOPMENT"])},
        "validation": {"from": "2026-06-17", "through": VAL_END, "games": int(counts["VALIDATION"])},
        "final_holdout": {"from": "2026-07-03", "through": "2026-08-04", "games": int(counts["FINAL_HOLDOUT"])},
        "selection": "fixed candidates; validation Brier then log loss only; final holdout untouched",
        "snapshot_contract": "accepted benchmark Pinnacle snapshot; exact identity, paired h2h, pregame, certified outcome",
    }
    (OUT / "moneyline_residual_temporal_split_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    return d


def feature_manifest(d):
    concepts = {
        "home_games": "strict-prior history depth", "away_games": "strict-prior history depth",
        "home_wp": "season-to-date team performance", "away_wp": "season-to-date team performance",
        "home_rs": "season-to-date offense", "away_rs": "season-to-date offense",
        "home_ra": "season-to-date run prevention", "away_ra": "season-to-date run prevention",
        "home_last10_wp": "recent team performance", "away_last10_wp": "recent team performance",
        "home_last10_diff": "recent run differential", "away_last10_diff": "recent run differential",
        "home_rest": "certified rest", "away_rest": "certified rest",
        "league_total": "strict-prior run environment", "month_sin": "season environment",
        "month_cos": "season environment", "starter_state_available": "starter availability/fallback indicator",
        "bullpen_state_available": "bullpen availability/fallback indicator",
        "park_state_available": "park-state availability indicator", "weather_state_available": "environment availability indicator",
        "lineup_state_available": "lineup availability indicator", "doubleheader_state_available": "doubleheader context availability indicator",
    }
    rows = [{"feature": "pinnacle_home_no_vig_probability", "role": "fixed market anchor / model B-C input", "primary": True,
             "strict_prior_status": "CERTIFIED_PREGAME_MARKET", "missing_rate": 0.0, "concept": "Pinnacle no-vig probability"}]
    rows += [{"feature": f, "role": "baseball_state_correction", "primary": True,
              "strict_prior_status": "CERTIFIED_STRICT_PRIOR", "missing_rate": float(d[f].isna().mean()), "concept": concepts[f]}
             for f in STATE_FEATURES]
    unavailable = ["starter_identity", "starter_quality", "starter_workload", "prior_start_depth", "starter_handedness",
                   "bullpen_workload", "travel", "park_identity", "elevation"]
    rows += [{"feature": f, "role": "not used", "primary": False, "strict_prior_status": "UNAVAILABLE_IN_ACCEPTED_SPINE",
              "missing_rate": np.nan, "concept": "not backfilled; no external acquisition"} for f in unavailable]
    excluded = ["home_win_probability", "totals_v1_output", "run_line_output", "consensus_moneyline", "bookmaker",
                "later_pinnacle_observation", "closing_price", "final_score", "postgame_statistics"]
    rows += [{"feature": f, "role": "excluded", "primary": False, "strict_prior_status": "EXCLUDED_PRIMARY",
              "missing_rate": np.nan, "concept": "contract exclusion"} for f in excluded]
    pd.DataFrame(rows).to_csv(OUT / "moneyline_residual_feature_manifest.csv", index=False)


def fit_predict(name, train, test, features, y):
    if name == "MODEL_A_LOG_ODDS_OFFSET_REGULARIZED_CORRECTION":
        model = OffsetLogistic(c=.25).fit(train[features], train[y], train.market_logit)
        return model, model.predict_proba(test[features], test.market_logit)
    if name == "MODEL_B_MARKET_PLUS_BASEBALL_REGULARIZED_LOGISTIC":
        cols = ["market_logit"] + features
        model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                              LogisticRegression(C=.25, max_iter=3000, random_state=SEED))
        model.fit(train[cols], train[y])
        return model, model.predict_proba(test[cols])[:, 1]
    if name == "MODEL_C_SHALLOW_HGB_MARKET_ANCHORED_CLASSIFIER":
        cols = ["pinnacle_home_no_vig_probability"] + features
        model = make_pipeline(SimpleImputer(strategy="median"), HistGradientBoostingClassifier(
            max_iter=100, max_leaf_nodes=15, min_samples_leaf=25, learning_rate=.05,
            l2_regularization=1.0, early_stopping=False, random_state=SEED))
        model.fit(train[cols], train[y])
        return model, model.predict_proba(test[cols])[:, 1]
    if name == "CONTROL_PINNACLE_CALIBRATION_ONLY":
        model = make_pipeline(StandardScaler(), LogisticRegression(C=1.0, max_iter=3000, random_state=SEED))
        model.fit(train[["market_logit"]], train[y])
        return model, model.predict_proba(test[["market_logit"]])[:, 1]
    raise ValueError(name)


MODELS = [
    "MODEL_A_LOG_ODDS_OFFSET_REGULARIZED_CORRECTION",
    "MODEL_B_MARKET_PLUS_BASEBALL_REGULARIZED_LOGISTIC",
    "MODEL_C_SHALLOW_HGB_MARKET_ANCHORED_CLASSIFIER",
    "CONTROL_PINNACLE_CALIBRATION_ONLY",
]


def primary_models(d):
    dev = d.temporal_split.eq("DEVELOPMENT")
    val = d.temporal_split.eq("VALIDATION")
    hold = d.temporal_split.eq("FINAL_HOLDOUT")
    rows, val_preds = [], {}
    raw = d.pinnacle_home_no_vig_probability
    for name in MODELS:
        _, p = fit_predict(name, d[dev], d[val], STATE_FEATURES, "winner_home")
        val_preds[name] = p
        m = metrics(d.loc[val, "winner_home"], p); b = metrics(d.loc[val, "winner_home"], raw[val])
        rows.append({"phase": "VALIDATION", "model": name, **m,
                     "brier_delta_vs_raw_pinnacle": m["brier"] - b["brier"],
                     "log_loss_delta_vs_raw_pinnacle": m["log_loss"] - b["log_loss"],
                     "ece_delta_vs_raw_pinnacle": m["ece_10"] - b["ece_10"]})
    residual_names = MODELS[:3]
    selected = sorted(residual_names, key=lambda n: (
        metrics(d.loc[val, "winner_home"], val_preds[n])["brier"],
        metrics(d.loc[val, "winner_home"], val_preds[n])["log_loss"], n))[0]
    hold_preds, fitted = {}, {}
    train = dev | val
    for name in MODELS:
        model, p = fit_predict(name, d[train], d[hold], STATE_FEATURES, "winner_home")
        fitted[name], hold_preds[name] = model, p
        m = metrics(d.loc[hold, "winner_home"], p); b = metrics(d.loc[hold, "winner_home"], raw[hold])
        rows.append({"phase": "FINAL_HOLDOUT", "model": name, **m,
                     "brier_delta_vs_raw_pinnacle": m["brier"] - b["brier"],
                     "log_loss_delta_vs_raw_pinnacle": m["log_loss"] - b["log_loss"],
                     "ece_delta_vs_raw_pinnacle": m["ece_10"] - b["ece_10"]})
    for phase, mask in [("VALIDATION", val), ("FINAL_HOLDOUT", hold)]:
        m = metrics(d.loc[mask, "winner_home"], raw[mask])
        rows.append({"phase": phase, "model": "PINNACLE_RAW_NO_VIG", **m,
                     "brier_delta_vs_raw_pinnacle": 0.0, "log_loss_delta_vs_raw_pinnacle": 0.0,
                     "ece_delta_vs_raw_pinnacle": 0.0})
    comparison = pd.DataFrame(rows)
    comparison["selected_on_validation"] = comparison.model.eq(selected)
    comparison.to_csv(OUT / "moneyline_residual_model_comparison.csv", index=False)
    hold_table = comparison[comparison.phase.eq("FINAL_HOLDOUT")].copy()
    control = hold_table[hold_table.model.eq("CONTROL_PINNACLE_CALIBRATION_ONLY")].iloc[0]
    hold_table["brier_delta_vs_calibration_control"] = hold_table.brier - control.brier
    hold_table["log_loss_delta_vs_calibration_control"] = hold_table.log_loss - control.log_loss
    hold_table["ece_delta_vs_calibration_control"] = hold_table.ece_10 - control.ece_10
    hold_table.to_csv(OUT / "moneyline_residual_holdout_metrics.csv", index=False)
    pred = d[["game_pk", "game_date", "temporal_split", "winner_home", "pinnacle_home_no_vig_probability", "home_win_probability"] + STATE_FEATURES].copy()
    pred["corrected_home_probability"] = np.nan
    pred.loc[val, "corrected_home_probability"] = val_preds[selected]
    pred.loc[hold, "corrected_home_probability"] = hold_preds[selected]
    dev_model, dev_pred = fit_predict(selected, d[dev], d[dev], STATE_FEATURES, "winner_home")
    pred.loc[dev, "corrected_home_probability"] = dev_pred
    pred["probability_correction"] = pred.corrected_home_probability - pred.pinnacle_home_no_vig_probability
    return selected, pred, fitted[selected], dev_model, dev, val, hold


def group_probability_metrics(g):
    base = metrics(g.winner_home, g.pinnacle_home_no_vig_probability)
    corrected = metrics(g.winner_home, g.corrected_home_probability)
    return base, corrected


def correction_analyses(pred):
    abs_corr = pred.probability_correction.abs()
    pred["correction_band"] = pd.cut(abs_corr, [-1, .01, .025, .05, .075, .10, np.inf],
        labels=["<1.0pp", "1.0-2.49pp", "2.5-4.99pp", "5.0-7.49pp", "7.5-9.99pp", ">=10pp"], right=False)
    bands = []
    for phase in ["VALIDATION", "FINAL_HOLDOUT"]:
        for band, g in pred[pred.temporal_split.eq(phase)].groupby("correction_band", observed=True):
            b, c = group_probability_metrics(g)
            bands.append({"phase": phase, "band": band, "games": len(g),
                          "mean_predicted_correction": g.probability_correction.mean(),
                          "observed_home_win_rate": g.winner_home.mean(), "pinnacle_brier": b["brier"],
                          "corrected_brier": c["brier"], "pinnacle_log_loss": b["log_loss"],
                          "corrected_log_loss": c["log_loss"],
                          "correction_direction_accuracy": np.mean(np.sign(g.probability_correction) == np.sign(g.winner_home - g.pinnacle_home_no_vig_probability))})
    pd.DataFrame(bands).to_csv(OUT / "moneyline_residual_probability_correction_bands.csv", index=False)
    directions = []
    pred["correction_direction"] = np.where(pred.probability_correction >= 0, "PINNACLE_UNDERRATES_HOME", "PINNACLE_OVERRATES_HOME")
    for phase in ["VALIDATION", "FINAL_HOLDOUT"]:
        for direction, g in pred[pred.temporal_split.eq(phase)].groupby("correction_direction"):
            b, c = group_probability_metrics(g)
            directions.append({"phase": phase, "direction": direction, "games": len(g),
                "mean_correction": g.probability_correction.mean(),
                "actual_residual_home_win_minus_pinnacle_probability": (g.winner_home - g.pinnacle_home_no_vig_probability).mean(),
                "pinnacle_brier": b["brier"], "corrected_brier": c["brier"],
                "pinnacle_log_loss": b["log_loss"], "corrected_log_loss": c["log_loss"]})
    pd.DataFrame(directions).to_csv(OUT / "moneyline_residual_direction_analysis.csv", index=False)
    p = pred.pinnacle_home_no_vig_probability
    pred["market_strength_band"] = pd.cut(p, [-np.inf, .40, .45, .50, .55, .60, .65, np.inf],
        labels=["<40%", "40-44.99%", "45-49.99%", "50-54.99%", "55-59.99%", "60-64.99%", ">=65%"], right=False)
    regimes = []
    for phase in ["VALIDATION", "FINAL_HOLDOUT"]:
        phase_rows = pred[pred.temporal_split.eq(phase)]
        groups = [(str(k), g) for k, g in phase_rows.groupby("market_strength_band", observed=True)]
        groups += [("PINNACLE_HOME_FAVORITE", phase_rows[p[phase_rows.index] > .5]),
                   ("PINNACLE_AWAY_FAVORITE", phase_rows[p[phase_rows.index] < .5]),
                   ("NEAR_EVEN_45_TO_55", phase_rows[p[phase_rows.index].between(.45, .55, inclusive="left")])]
        for regime, g in groups:
            if not len(g): continue
            b, c = group_probability_metrics(g)
            regimes.append({"phase": phase, "regime": regime, "games": len(g),
                            "mean_pinnacle_home_probability": g.pinnacle_home_no_vig_probability.mean(),
                            "mean_absolute_correction": g.probability_correction.abs().mean(),
                            "pinnacle_brier": b["brier"], "corrected_brier": c["brier"],
                            "brier_delta": c["brier"] - b["brier"], "pinnacle_log_loss": b["log_loss"],
                            "corrected_log_loss": c["log_loss"], "log_loss_delta": c["log_loss"] - b["log_loss"]})
    pd.DataFrame(regimes).to_csv(OUT / "moneyline_residual_market_strength_analysis.csv", index=False)


def temporal_stability(pred):
    pred = pred.sort_values(["game_date", "game_pk"]).copy()
    pred["month"] = pred.game_date.str[:7]
    pred["rolling_50"] = np.arange(len(pred)) // 50
    rows = []
    for kind, grouped in [("split", pred.groupby("temporal_split")), ("month", pred.groupby("month")), ("rolling_50", pred.groupby("rolling_50"))]:
        for value, g in grouped:
            b, c = group_probability_metrics(g)
            rows.append({"slice_type": kind, "slice_value": value, "games": len(g),
                "pinnacle_brier": b["brier"], "corrected_brier": c["brier"], "brier_delta": c["brier"] - b["brier"],
                "pinnacle_log_loss": b["log_loss"], "corrected_log_loss": c["log_loss"], "log_loss_delta": c["log_loss"] - b["log_loss"],
                "pinnacle_ece": b["ece_10"], "corrected_ece": c["ece_10"], "ece_delta": c["ece_10"] - b["ece_10"],
                "average_absolute_correction": g.probability_correction.abs().mean()})
    pd.DataFrame(rows).to_csv(OUT / "moneyline_residual_temporal_stability.csv", index=False)


def feature_novelty(d, pred, selected, fitted, dev_model, val, hold):
    # Importance is validation-only. Directional stability uses feature/correction correlation by frozen split.
    if selected.startswith("MODEL_A"):
        importance = dict(zip(STATE_FEATURES, np.abs(dev_model.coef_)))
        importance_sd = {f: np.nan for f in STATE_FEATURES}
    else:
        cols = (["market_logit"] if selected.startswith("MODEL_B") else ["pinnacle_home_no_vig_probability"]) + STATE_FEATURES
        pi = permutation_importance(dev_model, d.loc[val, cols], d.loc[val, "winner_home"], scoring="neg_brier_score", n_repeats=10, random_state=SEED)
        importance = dict(zip(cols, pi.importances_mean)); importance_sd = dict(zip(cols, pi.importances_std))
    rows = []
    pred["actual_market_residual"] = pred.winner_home - pred.pinnacle_home_no_vig_probability
    for feature in STATE_FEATURES:
        corrs = {}
        for split in ["DEVELOPMENT", "VALIDATION", "FINAL_HOLDOUT"]:
            g = pred[pred.temporal_split.eq(split)][[feature, "actual_market_residual"]].dropna()
            corrs[split] = float(g[feature].corr(g.actual_market_residual)) if len(g) > 2 and g[feature].nunique() > 1 else np.nan
        signs = [np.sign(v) for v in corrs.values() if pd.notna(v) and abs(v) > .02]
        stable = len(signs) == 3 and len(set(signs)) == 1
        imp = float(importance.get(feature, 0.0)); sd = float(importance_sd.get(feature, np.nan))
        classification = ("plausible incremental baseball information" if imp > 0.01 and stable else
                          "largely market-redundant" if abs(imp) <= 0.01 or stable else "unstable/noisy")
        rows.append({"feature": feature, "importance": imp, "importance_sd": sd, "classification": classification,
                     "development_actual_residual_correlation": corrs["DEVELOPMENT"], "validation_actual_residual_correlation": corrs["VALIDATION"],
                     "holdout_actual_residual_correlation": corrs["FINAL_HOLDOUT"], "same_direction_across_splits": stable,
                     "causal_or_pinnacle_internal_usage_claim": False})
    pd.DataFrame(rows).sort_values("importance", ascending=False).to_csv(OUT / "moneyline_residual_feature_novelty.csv", index=False)


def log5_diagnostic(d, selected, dev, val, hold):
    rows = []
    base_features = STATE_FEATURES
    plus_features = STATE_FEATURES + ["home_win_probability"]
    for phase, train, test in [("VALIDATION", dev, val), ("FINAL_HOLDOUT", dev | val, hold)]:
        _, pa = fit_predict(selected, d[train], d[test], base_features, "winner_home")
        _, pb = fit_predict(selected, d[train], d[test], plus_features, "winner_home")
        for name, p in [("A_PRIMARY_BASEBALL_RESIDUAL", pa), ("B_PRIMARY_PLUS_FROZEN_LOG5", pb)]:
            m = metrics(d.loc[test, "winner_home"], p)
            rows.append({"phase": phase, "diagnostic": name, **m})
    out = pd.DataFrame(rows)
    a = out[out.diagnostic.str.startswith("A_")].set_index("phase")
    b = out[out.diagnostic.str.startswith("B_")].set_index("phase")
    out["log5_brier_delta_B_minus_A"] = out.phase.map((b.brier - a.brier).to_dict())
    out["log5_log_loss_delta_B_minus_A"] = out.phase.map((b.log_loss - a.log_loss).to_dict())
    out["log5_ece_delta_B_minus_A"] = out.phase.map((b.ece_10 - a.ece_10).to_dict())
    out.to_csv(OUT / "moneyline_log5_incremental_diagnostic.csv", index=False)


def prospective_evidence():
    sql = """
      SELECT p.game_date::text,p.game_id,p.home_win_probability,p.predicted_winner,p.confidence_band,
             p.prediction_timestamp_utc,o.official_winner,o.prediction_correct,o.brier_contribution,o.log_loss_contribution
      FROM mlb.public_game_moneyline_predictions p
      JOIN mlb.public_game_moneyline_outcomes o USING (game_date,game_id,model_version,prediction_snapshot_class)
      WHERE p.model_version='MLB_GAME_PYTHAGOREAN_LOG5_V1' AND p.admission_status='ADMITTED_SHADOW'
        AND p.game_date <= DATE '2026-08-10'
      ORDER BY p.game_date,p.game_id
    """
    columns = ["game_date", "game_id", "home_win_probability", "predicted_winner", "confidence_band",
               "prediction_timestamp_utc", "official_winner", "prediction_correct", "brier_contribution",
               "log_loss_contribution"]
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        g = pd.DataFrame(cur.fetchall(), columns=columns)
    if g.duplicated(["game_date", "game_id"]).any():
        raise RuntimeError("PROSPECTIVE_DUPLICATE_IDENTITIES")
    g["correct"] = g.prediction_correct.astype(bool)
    bands = g.groupby("confidence_band").correct.agg(["count", "sum"])
    band_text = ", ".join(f"{idx} {int(r['sum'])}-{int(r['count']-r['sum'])}" for idx, r in bands.iterrows())
    # The retained prospective attachment inventory is totals-only at this grain;
    # it does not expose synchronized no-vig moneyline probabilities.
    comparable = pd.DataFrame()
    wins = int(g.correct.sum()); losses = len(g) - wins
    result = {"games": len(g), "wins": wins, "losses": losses, "accuracy": wins / len(g),
              "brier": float(g.brier_contribution.mean()), "log_loss": float(g.log_loss_contribution.mean()),
              "bands": band_text, "pinnacle_comparable_games": len(comparable),
              "pinnacle_brier": np.nan, "pinnacle_log_loss": np.nan}
    return result


def materiality_and_report(d, pred, selected, prospective):
    comparison = pd.read_csv(OUT / "moneyline_residual_model_comparison.csv")
    val = comparison[(comparison.phase == "VALIDATION") & (comparison.model == selected)].iloc[0]
    hold = comparison[(comparison.phase == "FINAL_HOLDOUT") & (comparison.model == selected)].iloc[0]
    control = comparison[(comparison.phase == "FINAL_HOLDOUT") & (comparison.model == "CONTROL_PINNACLE_CALIBRATION_ONLY")].iloc[0]
    raw = comparison[(comparison.phase == "FINAL_HOLDOUT") & (comparison.model == "PINNACLE_RAW_NO_VIG")].iloc[0]
    stable = pd.read_csv(OUT / "moneyline_residual_temporal_stability.csv")
    months = stable[stable.slice_type.eq("month")]
    regimes = pd.read_csv(OUT / "moneyline_residual_market_strength_analysis.csv")
    directions = pd.read_csv(OUT / "moneyline_residual_direction_analysis.csv")
    novelty = pd.read_csv(OUT / "moneyline_residual_feature_novelty.csv").head(5)
    diag = pd.read_csv(OUT / "moneyline_log5_incremental_diagnostic.csv")
    log5 = diag[(diag.phase == "FINAL_HOLDOUT") & diag.diagnostic.str.startswith("B_")].iloc[0]
    abs_corr = pred.loc[pred.temporal_split.eq("FINAL_HOLDOUT"), "probability_correction"].abs()
    val_improves = val.brier_delta_vs_raw_pinnacle < 0 and val.log_loss_delta_vs_raw_pinnacle < 0
    hold_improves = hold.brier_delta_vs_raw_pinnacle < 0 and hold.log_loss_delta_vs_raw_pinnacle < 0
    beyond_control = hold.brier < control.brier and hold.log_loss < control.log_loss
    calibration_ok = hold.ece_10 <= raw.ece_10 + .01
    distributed = float((months.brier_delta < 0).mean()) >= .6
    meaningful = abs_corr.mean() >= .01 and abs(hold.brier_delta_vs_raw_pinnacle) >= .001
    no_tiny_regime_dependence = not (regimes.query("phase=='FINAL_HOLDOUT' and brier_delta<0").games.max() if len(regimes.query("phase=='FINAL_HOLDOUT' and brier_delta<0")) else 0) < 10
    if val_improves and hold_improves and beyond_control and calibration_ok and distributed and meaningful and no_tiny_regime_dependence:
        decision = "MONEYLINE_MARKET_RESIDUAL_INCREMENTAL_INFORMATION_PRESENT"
    elif val_improves and hold_improves and beyond_control and calibration_ok:
        decision = "MONEYLINE_MARKET_RESIDUAL_STATISTICALLY_POSITIVE_BUT_IMMATERIAL"
    elif val_improves != hold_improves:
        decision = "MONEYLINE_MARKET_RESIDUAL_RESULT_MIXED"
    else:
        decision = "MONEYLINE_MARKET_RESIDUAL_NO_INCREMENTAL_SIGNAL"
    shadow_justified = decision == "MONEYLINE_MARKET_RESIDUAL_INCREMENTAL_INFORMATION_PRESENT"
    consistency = (
        "directionally compatible only" if prospective["games"] and val_improves and hold_improves else
        "not directionally supported by stable historical residual evidence")
    (OUT / "moneyline_historical_vs_prospective_consistency.md").write_text(
        f"# Historical versus prospective consistency\n\nThrough 2026-08-10 the immutable prospective ledger contains "
        f"{prospective['games']} graded games: {prospective['wins']}-{prospective['losses']} "
        f"({prospective['accuracy']:.2%}), Brier {prospective['brier']:.6f}, log loss {prospective['log_loss']:.6f}. "
        f"Confidence bands: {prospective['bands']}. Retained synchronized Pinnacle-comparable rows available in the "
        f"accepted moneyline attachment grain: {prospective['pinnacle_comparable_games']}. Prospective rows were read only "
        f"and excluded from training. The STRONG/LEAN separation is {consistency}; it is not a selector or threshold change.\n")
    declaration = pd.DataFrame([{"declaration": decision, "historical_practical_bar_cleared": shadow_justified,
        "selected_model": selected, "population": len(d), "validation_games": int((d.temporal_split == 'VALIDATION').sum()),
        "holdout_games": int((d.temporal_split == 'FINAL_HOLDOUT').sum())}])
    declaration.to_csv(OUT / "moneyline_residual_decision.csv", index=False)
    correction = pred.loc[pred.temporal_split.eq("FINAL_HOLDOUT"), "probability_correction"]
    report = f"""# MLB Pinnacle-Anchored Moneyline Residual v1

Experiment: `MLB_PINNACLE_ANCHORED_MONEYLINE_RESIDUAL_V1`

## Declaration

`{decision}`

## Frozen result

- Exact historical population: {len(d)} games, {d.game_date.min()} through {d.game_date.max()}; split {d.temporal_split.value_counts().to_dict()}.
- Selected on validation only: `{selected}`. Model A uses Pinnacle log-odds as a mathematically fixed offset plus an L2-regularized baseball-state correction.
- Raw Pinnacle holdout Brier/log loss/ECE: {raw.brier:.6f}/{raw.log_loss:.6f}/{raw.ece_10:.6f}.
- Corrected holdout Brier/log loss/ECE: {hold.brier:.6f}/{hold.log_loss:.6f}/{hold.ece_10:.6f}; deltas {hold.brier_delta_vs_raw_pinnacle:+.6f}/{hold.log_loss_delta_vs_raw_pinnacle:+.6f}/{hold.ece_delta_vs_raw_pinnacle:+.6f}.
- Pinnacle-only calibration control: {control.brier:.6f}/{control.log_loss:.6f}/{control.ece_10:.6f}.
- Holdout correction mean signed {correction.mean()*100:+.3f} pp; mean/median absolute {correction.abs().mean()*100:.3f}/{correction.abs().median()*100:.3f} pp; SD {correction.std(ddof=0)*100:.3f} pp; positive/negative {(correction>0).mean():.1%}/{(correction<0).mean():.1%}.
- Directional, fixed market-strength, correction-band, month, and rolling-50 results are in the companion CSVs. Improved-month share: {(months.brier_delta < 0).mean():.1%}.
- Leading attribution fields: {', '.join(novelty.feature.astype(str))}; classifications are predictive diagnostics, not claims about Pinnacle internals.
- Frozen Log5 addition holdout delta versus the primary residual: Brier {log5.log5_brier_delta_B_minus_A:+.6f}, log loss {log5.log5_log_loss_delta_B_minus_A:+.6f}; it did not alter primary selection.
- Prospective through August 10: {prospective['wins']}-{prospective['losses']} in {prospective['games']} games, Brier/log loss {prospective['brier']:.6f}/{prospective['log_loss']:.6f}; {prospective['bands']}.
- Current-slate no-write shadow justified: {shadow_justified}. {'No ledger was written; the practical bar did not clear.' if not shadow_justified else 'The bar cleared, but this script intentionally emits no slate rows without a separately certified synchronized feature adapter.'}

## Evidence boundary

The accepted spine does not expose granular certified starter quality/workload/handedness, bullpen workload, travel, park identity, or elevation at this historical snapshot grain. No replacement data was acquired. No model was deployed, no wager/EV/staking output was created, and no prospective ledger was mutated. Predictive profitability and future generalization remain unproven.
"""
    (OUT / "concise_mlb_pinnacle_anchored_moneyline_residual_v1.md").write_text(report)
    return decision


def hashes():
    files = sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "reproducibility_hashes.sha256")
    (OUT / "reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    d = population(); feature_manifest(d)
    selected, pred, fitted, dev_model, dev, val, hold = primary_models(d)
    correction_analyses(pred); temporal_stability(pred)
    feature_novelty(d, pred, selected, fitted, dev_model, val, hold)
    log5_diagnostic(d, selected, dev, val, hold)
    prospective = prospective_evidence()
    decision = materiality_and_report(d, pred, selected, prospective)
    hashes()
    print(json.dumps({"experiment": "MLB_PINNACLE_ANCHORED_MONEYLINE_RESIDUAL_V1", "population": len(d),
                      "split": d.temporal_split.value_counts().to_dict(), "selected_model": selected,
                      "declaration": decision, "output": str(OUT.relative_to(ROOT))}, indent=2))


if __name__ == "__main__":
    main()
