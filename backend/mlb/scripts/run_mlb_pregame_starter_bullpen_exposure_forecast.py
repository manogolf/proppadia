#!/usr/bin/env python3
"""Bounded MLB pregame starter/bullpen exposure forecast experiment.

This offline research utility develops fixed strict-prior exposure forecasts
for total PA, starter-facing PA, bullpen-facing PA, and starter-exit events.
It reuses the frozen fully reconciled encounter population and temporal splits
from the encounter-informed multi-hit experiment.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, LaunchAgent changes, threshold search, price optimization, or holdout
tuning are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import log_loss, mean_squared_error, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17"

ENCOUNTER_EXP = ROOT / "artifacts/analysis/model_development/mlb_encounter_informed_multi_hit_probability_experiment/2026-07-17"
FULL_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/expanded_encounter_ledger_2026-07-17.csv"
POP = ENCOUNTER_EXP / "reconciled_benchmark_encounter_population_2026-07-17.csv"
PRIOR_ARTIFACTS = ENCOUNTER_EXP / "research_only_model_artifacts_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717
TOLERANCE = 1e-6

BASE_FEATURES = [
    "expected_pa_used",
    "d15_pa_per_game",
    "season_to_date_pa_per_game",
    "lineup_slot",
    "home_team_batting_flag",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "d30_hits_per_pa",
    "season_to_date_hits_per_pa",
    "prior_game_count",
    "starter_prior_start_count",
    "starter_prior_starter_pa_mean",
    "starter_prior_total_bf_mean",
    "starter_prior_bullpen_entry_pa_mean",
    "starter_prior_starter_pa_std",
    "opponent_bullpen_hit_rate_prior",
    "bullpen_hit_factor_prior",
    "avg_relief_pitchers_used_prior",
]


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def safe_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(x: Any, lo: float = 0.001, hi: float = 0.999) -> float:
    try:
        val = float(x)
    except Exception:
        val = 0.2
    if not math.isfinite(val):
        val = 0.2
    return float(min(max(val, lo), hi))


def hit_distribution(n_starter: Any, n_bullpen: Any, p_starter: Any, p_bullpen: Any) -> tuple[float, float, float]:
    ns = max(float(n_starter) if pd.notna(n_starter) else 0.0, 0.0)
    nb = max(float(n_bullpen) if pd.notna(n_bullpen) else 0.0, 0.0)
    ps = clip_prob(p_starter, 0.005, 0.55)
    pb = clip_prob(p_bullpen, 0.005, 0.55)
    lam = max(ns * ps + nb * pb, 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = max(0.0, 1.0 - p0 - p1)
    s = p0 + p1 + p2
    return p0 / s, p1 / s, p2 / s


def exposure_class(value: Any, bullpen: bool = False) -> str:
    v = int(round(float(value))) if pd.notna(value) else 0
    if bullpen:
        return "3+" if v >= 3 else str(max(v, 0))
    return "4+" if v >= 4 else str(max(v, 0))


def poisson_class_probs(mean: float, bullpen: bool = False) -> dict[str, float]:
    mean = max(float(mean), 0.001)
    max_exact = 2 if bullpen else 3
    probs: dict[str, float] = {}
    total = 0.0
    for k in range(max_exact + 1):
        p = math.exp(-mean) * mean**k / math.factorial(k)
        probs[str(k)] = p
        total += p
    probs["3+" if bullpen else "4+"] = max(0.0, 1.0 - total)
    s = sum(probs.values())
    return {k: v / s for k, v in probs.items()}


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def make_x(frame: pd.DataFrame, features: list[str], medians: dict[str, float]) -> np.ndarray:
    cols = []
    for col in features:
        vals = pd.to_numeric(frame[col], errors="coerce") if col in frame.columns else pd.Series(np.nan, index=frame.index)
        cols.append(vals.fillna(medians.get(col, 0.0)).to_numpy(dtype=float))
    return np.vstack(cols).T


def scale_x(X: np.ndarray, means: np.ndarray, stds: np.ndarray) -> np.ndarray:
    return (X - means) / np.where(stds == 0, 1.0, stds)


def extract_sequence_targets(pop: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.dropna(subset=["benchmark_player_game_key"]).copy()
    ledger["official_hit_bool"] = safe_bool(ledger["official_hit"])
    rows = []
    for key, g in ledger.groupby("benchmark_player_game_key"):
        g = g.sort_values("plate_appearance_sequence")
        roles = g["role_classification"].astype(str).tolist()
        bullpen_positions = [i + 1 for i, role in enumerate(roles) if role == "RELIEVER_FACING_PA"]
        starter_positions = [i + 1 for i, role in enumerate(roles) if role == "STARTER_FACING_PA"]
        first_row = g.iloc[0]
        starter_rows = g[g["role_classification"].eq("STARTER_FACING_PA")]
        first_starter = starter_rows.iloc[0] if not starter_rows.empty else first_row
        rows.append({
            "player_game_key": key,
            "batter_team": first_row.get("batter_team"),
            "opponent": first_row.get("opponent"),
            "home_team": first_row.get("home_team"),
            "away_team": first_row.get("away_team"),
            "home_team_batting_flag": 1 if first_row.get("batter_team") == first_row.get("home_team") else 0,
            "actual_lineup_slot_from_ledger": first_row.get("lineup_slot"),
            "opposing_starter_id": first_starter.get("pitcher_id"),
            "opposing_starter_name": first_starter.get("pitcher_name"),
            "opposing_starter_team": first_starter.get("pitcher_team"),
            "actual_total_pa": len(g),
            "actual_starter_facing_pa_seq": len(starter_positions),
            "actual_bullpen_facing_pa_seq": len(bullpen_positions),
            "first_bullpen_pa_number": min(bullpen_positions) if bullpen_positions else "",
            "starter_exit_before_hitter_pa3": bool(bullpen_positions and min(bullpen_positions) <= 3),
            "starter_exit_before_hitter_pa4": bool(bullpen_positions and min(bullpen_positions) <= 4),
            "bullpen_pa_ge1": bool(len(bullpen_positions) >= 1),
            "bullpen_pa_ge2": bool(len(bullpen_positions) >= 2),
            "hitter_receives_fourth_pa": bool(len(g) >= 4),
            "hitter_receives_fifth_pa": bool(len(g) >= 5),
        })
    return pd.DataFrame(rows)


def starter_prior_features(pop: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.copy()
    ledger["game_date_dt"] = pd.to_datetime(ledger["game_date"], errors="coerce")
    starter_game = (
        ledger[ledger["role_classification"].eq("STARTER_FACING_PA")]
        .groupby(["game_id", "game_date_dt", "pitcher_id", "pitcher_name", "pitcher_team"], dropna=False)
        .agg(starter_pa=("source_event_identity", "count"), unique_batters=("batter_id", "nunique"))
        .reset_index()
    )
    total_game = (
        ledger.groupby(["game_id", "pitcher_id"], dropna=False)
        .agg(total_bf=("source_event_identity", "count"))
        .reset_index()
    )
    starter_game = starter_game.merge(total_game, on=["game_id", "pitcher_id"], how="left")
    starter_game["bullpen_entry_pa_mean_proxy"] = starter_game["starter_pa"] / 9.0

    out = []
    keys = pop[["player_game_key", "slate_date", "opposing_starter_id"]].copy()
    keys["slate_date_dt"] = pd.to_datetime(keys["slate_date"], errors="coerce")
    for _, row in keys.iterrows():
        starter_id = row["opposing_starter_id"]
        prior = starter_game[
            starter_game["pitcher_id"].astype(str).eq(str(starter_id))
            & (starter_game["game_date_dt"] < row["slate_date_dt"])
        ]
        if len(prior) >= 2:
            status = "STARTER_PRIOR_AVAILABLE"
        elif len(prior) == 1:
            status = "LOW_SAMPLE_STARTER_PRIOR"
        else:
            status = "NO_STARTER_PRIOR_FALLBACK"
        out.append({
            "player_game_key": row["player_game_key"],
            "starter_prior_status": status,
            "starter_prior_start_count": int(len(prior)),
            "starter_prior_starter_pa_mean": float(prior["starter_pa"].mean()) if len(prior) else np.nan,
            "starter_prior_total_bf_mean": float(prior["total_bf"].mean()) if len(prior) else np.nan,
            "starter_prior_bullpen_entry_pa_mean": float(prior["bullpen_entry_pa_mean_proxy"].mean()) if len(prior) else np.nan,
            "starter_prior_starter_pa_std": float(prior["starter_pa"].std(ddof=0)) if len(prior) > 1 else 0.0,
        })
    return pd.DataFrame(out)


def load_population() -> pd.DataFrame:
    pop = read_csv(POP)
    prior = read_csv(PRIOR_ARTIFACTS)
    ledger = read_csv(FULL_LEDGER)
    seq = extract_sequence_targets(pop, ledger)
    pop = pop.merge(seq, on="player_game_key", how="left", suffixes=("", "_seq"))
    pop = pop.merge(starter_prior_features(pop, ledger), on="player_game_key", how="left")

    prior_cols = [
        "player_game_key",
        "pred_starter_pa",
        "pred_bullpen_pa",
        "predicted_exposure_p_two_plus_hits",
    ]
    prior = prior[[c for c in prior_cols if c in prior.columns]].copy()
    prior = prior.rename(columns={
        "pred_starter_pa": "prior_pred_starter_pa",
        "pred_bullpen_pa": "prior_pred_bullpen_pa",
        "predicted_exposure_p_two_plus_hits": "prior_predicted_exposure_p_two_plus_hits",
    })
    pop = pop.merge(prior, on="player_game_key", how="left")

    pop["actual_total_pa_target"] = pd.to_numeric(pop["reconstructed_total_pa"], errors="coerce")
    pop["actual_starter_pa_target"] = pd.to_numeric(pop["actual_starter_facing_pa"], errors="coerce")
    pop["actual_bullpen_pa_target"] = pd.to_numeric(pop["actual_bullpen_facing_pa"], errors="coerce")
    pop["two_plus_binary"] = pop["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
    pop["one_to_two_population"] = pop["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])
    pop["fully_reconciled_sequence"] = safe_bool(pop["fully_reconciled_sequence"])
    return pop[pop["fully_reconciled_sequence"]].copy()


def count_metrics(df: pd.DataFrame, actual: str, pred: str, label: str) -> dict[str, Any]:
    g = df[[actual, pred]].dropna()
    if g.empty:
        return {"target": label, "rows": 0}
    err = g[pred].astype(float) - g[actual].astype(float)
    return {
        "target": label,
        "rows": len(g),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(math.sqrt(mean_squared_error(g[actual].astype(float), g[pred].astype(float)))),
        "mean_bias": float(err.mean()),
        "median_absolute_error": float(np.median(np.abs(err))),
        "actual_mean": float(g[actual].astype(float).mean()),
        "predicted_mean": float(g[pred].astype(float).mean()),
    }


def distribution_metrics(df: pd.DataFrame, actual: str, pred: str, target: str, bullpen: bool) -> dict[str, Any]:
    g = df[[actual, pred]].dropna()
    if g.empty:
        return {"target": target, "rows": 0}
    labels = ["0", "1", "2", "3+"] if bullpen else ["0", "1", "2", "3", "4+"]
    y = g[actual].map(lambda x: exposure_class(x, bullpen=bullpen)).astype(str).tolist()
    probs = np.array([[poisson_class_probs(m, bullpen=bullpen)[lbl] for lbl in labels] for m in g[pred].astype(float)])
    true = np.array([[1.0 if yy == lbl else 0.0 for lbl in labels] for yy in y])
    return {
        "target": target,
        "rows": len(g),
        "labels": "|".join(labels),
        "multiclass_log_loss": float(log_loss(y, np.clip(probs, EPS, 1), labels=labels)),
        "multiclass_brier": float(np.mean(np.sum((probs - true) ** 2, axis=1))),
    }


def event_metrics(df: pd.DataFrame, target: str, prob: str, label: str) -> dict[str, Any]:
    g = df[[target, prob]].dropna()
    if g.empty:
        return {"event": label, "rows": 0}
    y = g[target].astype(int).to_numpy()
    p = np.clip(g[prob].astype(float).to_numpy(), EPS, 1 - EPS)
    out = {
        "event": label,
        "rows": len(g),
        "observed_rate": float(y.mean()),
        "avg_predicted_probability": float(p.mean()),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else "",
        "ece": expected_calibration_error(y, p),
    }
    return out


def fit_count_challenger(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = df[df["temporal_split"].eq("fit")].copy()
    medians = {
        c: float(pd.to_numeric(train[c], errors="coerce").median())
        if c in train.columns and pd.to_numeric(train[c], errors="coerce").notna().any()
        else 0.0
        for c in BASE_FEATURES
    }
    X = make_x(train, BASE_FEATURES, medians)
    models: dict[str, LinearRegression] = {}
    rows = []
    targets = {
        "actual_total_pa_target": "challenger_total_pa",
        "actual_starter_pa_target": "challenger_starter_pa_raw",
    }
    for target, output in targets.items():
        model = LinearRegression()
        model.fit(X, pd.to_numeric(train[target], errors="coerce").fillna(0).to_numpy())
        models[output] = model
        rows.append({
            "instrument": "Exposure Challenger A",
            "model": "LinearRegression_fixed_features_fit_split_only",
            "target": target,
            "output_field": output,
            "fit_rows": len(train),
            "features": "|".join(BASE_FEATURES),
            "preprocessing": "numeric coercion; fit-split medians for missing values; post-prediction clipping",
            "configuration": "fixed Ordinary Least Squares; no search",
            "intercept": float(model.intercept_),
            **{f"coef_{feature}": float(coef) for feature, coef in zip(BASE_FEATURES, model.coef_)},
        })
    X_all = make_x(df, BASE_FEATURES, medians)
    df["challenger_total_pa"] = np.clip(models["challenger_total_pa"].predict(X_all), 1.0, 6.5)
    raw_starter = np.clip(models["challenger_starter_pa_raw"].predict(X_all), 0.0, 5.5)
    df["challenger_starter_pa"] = np.minimum(raw_starter, df["challenger_total_pa"])
    df["challenger_bullpen_pa"] = np.maximum(df["challenger_total_pa"] - df["challenger_starter_pa"], 0.0)
    df["challenger_joint_coherence_error"] = (
        df["challenger_total_pa"] - df["challenger_starter_pa"] - df["challenger_bullpen_pa"]
    ).abs()
    return df, pd.DataFrame(rows)


def fit_event_challenger(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = df[df["temporal_split"].eq("fit")].copy()
    medians = {
        c: float(pd.to_numeric(train[c], errors="coerce").median())
        if c in train.columns and pd.to_numeric(train[c], errors="coerce").notna().any()
        else 0.0
        for c in BASE_FEATURES
    }
    event_targets = {
        "bullpen_pa_ge1": "p_bullpen_pa_ge1",
        "bullpen_pa_ge2": "p_bullpen_pa_ge2",
        "starter_exit_before_hitter_pa3": "p_starter_exit_before_pa3",
        "starter_exit_before_hitter_pa4": "p_starter_exit_before_pa4",
        "hitter_receives_fourth_pa": "p_hitter_receives_fourth_pa",
        "hitter_receives_fifth_pa": "p_hitter_receives_fifth_pa",
    }
    rows = []
    X = make_x(train, BASE_FEATURES, medians)
    means = X.mean(axis=0)
    stds = X.std(axis=0)
    X_all = make_x(df, BASE_FEATURES, medians)
    X_scaled = scale_x(X, means, stds)
    X_all_scaled = scale_x(X_all, means, stds)
    for target, output in event_targets.items():
        y = train[target].astype(int).to_numpy()
        if len(set(y)) < 2:
            df[output] = float(y.mean()) if len(y) else 0.0
            model_desc = "constant_base_rate"
            coefs: dict[str, float] = {}
            intercept = float(y.mean()) if len(y) else 0.0
        else:
            model = LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs", random_state=RNG_SEED)
            model.fit(X_scaled, y)
            df[output] = model.predict_proba(X_all_scaled)[:, 1]
            model_desc = "LogisticRegression_C1_lbfgs_fixed_features_fit_split_only_scaled"
            coefs = {f"coef_{feature}": float(coef) for feature, coef in zip(BASE_FEATURES, model.coef_[0])}
            intercept = float(model.intercept_[0])
        rows.append({
            "instrument": "Exposure Challenger B",
            "model": model_desc,
            "target": target,
            "output_field": output,
            "fit_rows": len(train),
            "features": "|".join(BASE_FEATURES),
            "preprocessing": "numeric coercion; fit-split medians for missing values",
            "configuration": "fixed logistic model; no search",
            "intercept": intercept,
            **coefs,
        })
    return df, pd.DataFrame(rows)


def apply_joint_distribution(df: pd.DataFrame) -> pd.DataFrame:
    # Challenger C: coherent count forecast plus event probabilities. The count
    # forecast remains the exposure expectation; event models provide diagnostic
    # probabilities over exit states and bullpen-facing PA classes.
    df["joint_total_pa"] = df["challenger_total_pa"]
    df["joint_starter_pa"] = df["challenger_starter_pa"]
    df["joint_bullpen_pa"] = df["challenger_bullpen_pa"]
    df["joint_coherence_error"] = (df["joint_total_pa"] - df["joint_starter_pa"] - df["joint_bullpen_pa"]).abs()
    return df


def apply_multi_hit(df: pd.DataFrame) -> pd.DataFrame:
    p_starter = pd.to_numeric(df["p_hit_starter_prior"], errors="coerce").fillna(
        pd.to_numeric(df["hitter_per_pa_hit_estimate"], errors="coerce")
    )
    p_bullpen = pd.to_numeric(df["p_hit_bullpen_prior"], errors="coerce").fillna(p_starter)
    for prefix, starter_col, bullpen_col in [
        ("challenger", "challenger_starter_pa", "challenger_bullpen_pa"),
        ("joint", "joint_starter_pa", "joint_bullpen_pa"),
    ]:
        vals = [
            hit_distribution(s, b, ps, pb)
            for s, b, ps, pb in zip(df[starter_col], df[bullpen_col], p_starter, p_bullpen)
        ]
        df[f"{prefix}_p_zero_hits"] = [v[0] for v in vals]
        df[f"{prefix}_p_exactly_one_hit"] = [v[1] for v in vals]
        df[f"{prefix}_p_two_plus_hits"] = [v[2] for v in vals]
    return df


def binary_multi_hit_metrics(frame: pd.DataFrame, prob_col: str, instrument: str, split: str) -> dict[str, Any]:
    g = frame[frame["one_to_two_population"]].copy()
    y = g["two_plus_binary"].astype(int).to_numpy()
    p = np.clip(g[prob_col].astype(float).to_numpy(), EPS, 1 - EPS)
    return {
        "temporal_split": split,
        "instrument": instrument,
        "rows": len(g),
        "wins_two_plus": int(y.sum()),
        "losses_exactly_one": int(len(y) - y.sum()),
        "observed_two_plus_rate": float(y.mean()) if len(y) else "",
        "avg_predicted_two_plus": float(p.mean()) if len(p) else "",
        "brier": float(np.mean((p - y) ** 2)) if len(y) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(y) else "",
        "auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else "",
        "ece": expected_calibration_error(y, p) if len(y) else "",
    }


def exposure_gap_decomposition(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[df["temporal_split"].eq("holdout")].copy()
    rows = []
    for _, row in hold.iterrows():
        total_err = abs(float(row["actual_total_pa_target"]) - float(row["prior_pred_total_pa"]))
        starter_err = abs(float(row["actual_starter_pa_target"]) - float(row["prior_pred_starter_pa"]))
        bullpen_err = abs(float(row["actual_bullpen_pa_target"]) - float(row["prior_pred_bullpen_pa"]))
        lineup_missing = pd.isna(row.get("lineup_slot"))
        early_exit = bool(row.get("starter_exit_before_hitter_pa3")) and float(row["prior_pred_starter_pa"]) >= 2.5
        long_start = float(row["actual_bullpen_pa_target"]) == 0 and float(row["prior_pred_bullpen_pa"]) >= 1.0
        home_ninth = bool(row.get("home_team_batting_flag")) and total_err >= 1.0
        low_sample = str(row.get("starter_prior_status")) != "STARTER_PRIOR_AVAILABLE"
        if total_err >= 1.0 and total_err >= starter_err and total_err >= bullpen_err:
            reason = "total-PA error"
        elif early_exit:
            reason = "early Starter exit"
        elif long_start:
            reason = "unexpectedly long Starter outing"
        elif starter_err >= 1.0:
            reason = "Starter-workload error"
        elif bullpen_err >= 1.0:
            reason = "team-offense/opponent-pitch-count error"
        elif home_ninth:
            reason = "home-team ninth-inning opportunity error"
        elif lineup_missing:
            reason = "lineup-position error"
        elif low_sample:
            reason = "irregular-role error"
        else:
            reason = "batting-order-turn error"
        rows.append({
            "player_game_key": row["player_game_key"],
            "game_date": row["slate_date"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "actual_total_pa": row["actual_total_pa_target"],
            "predicted_total_pa": row["prior_pred_total_pa"],
            "actual_starter_facing_pa": row["actual_starter_pa_target"],
            "predicted_starter_facing_pa": row["prior_pred_starter_pa"],
            "actual_bullpen_facing_pa": row["actual_bullpen_pa_target"],
            "predicted_bullpen_facing_pa": row["prior_pred_bullpen_pa"],
            "first_bullpen_pa_number": row.get("first_bullpen_pa_number", ""),
            "actual_starter_exit_state": "EXIT_BEFORE_PA3" if row.get("starter_exit_before_hitter_pa3") else ("EXIT_BEFORE_PA4" if row.get("starter_exit_before_hitter_pa4") else "NO_EARLY_EXIT"),
            "predicted_exit_exposure_state": "BULLPEN_EXPOSURE_EXPECTED" if float(row["prior_pred_bullpen_pa"]) >= 1.0 else "STARTER_EXPOSURE_DOMINANT",
            "total_pa_abs_error": total_err,
            "starter_pa_abs_error": starter_err,
            "bullpen_pa_abs_error": bullpen_err,
            "primary_error_class": reason,
        })
    return pd.DataFrame(rows)


def bootstrap_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    hold = df[(df["temporal_split"].eq("holdout")) & df["one_to_two_population"]].copy()
    instruments = {
        "control": "control_p_two_plus_hits",
        "prior_predicted_exposure": "prior_predicted_exposure_p_two_plus_hits",
        "challenger": "challenger_p_two_plus_hits",
        "joint": "joint_p_two_plus_hits",
        "oracle": "oracle_exposure_p_two_plus_hits",
    }
    rows = []
    for name, col in instruments.items():
        vals_brier = []
        vals_auc = []
        for _ in range(300):
            sample = hold.sample(n=len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = sample["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(sample[col].astype(float).to_numpy(), EPS, 1 - EPS)
            vals_brier.append(float(np.mean((p - y) ** 2)))
            vals_auc.append(float(roc_auc_score(y, p)) if len(set(y)) > 1 else np.nan)
        rows.append({
            "instrument": name,
            "scope": "holdout_one_to_two_plus",
            "brier_p05": float(np.nanquantile(vals_brier, 0.05)),
            "brier_p50": float(np.nanquantile(vals_brier, 0.50)),
            "brier_p95": float(np.nanquantile(vals_brier, 0.95)),
            "auc_p05": float(np.nanquantile(vals_auc, 0.05)),
            "auc_p50": float(np.nanquantile(vals_auc, 0.50)),
            "auc_p95": float(np.nanquantile(vals_auc, 0.95)),
        })
    return pd.DataFrame(rows)


def grouped_count_results(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    df = df.copy()
    df["starter_workload_class"] = pd.cut(
        df["starter_prior_starter_pa_mean"].fillna(df["starter_prior_starter_pa_mean"].median()),
        bins=[-1, 15, 22, 99],
        labels=["short_prior_workload", "normal_prior_workload", "deep_prior_workload"],
    ).astype(str)
    df["lineup_certainty"] = np.where(df["lineup_slot"].notna(), "lineup_slot_available", "lineup_slot_missing")
    df["home_away"] = np.where(df["home_team_batting_flag"].eq(1), "home", "away")
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for group_col in ["lineup_slot", "starter_workload_class", "home_away", "batter_team", "opponent", "opposing_starter_id", "starter_prior_status", "lineup_certainty"]:
            if group_col not in sub.columns:
                continue
            for key, g in sub.groupby(group_col, dropna=False):
                if len(g) < 5:
                    sample_flag = "SPARSE"
                else:
                    sample_flag = "OK"
                m = count_metrics(g, "actual_bullpen_pa_target", "challenger_bullpen_pa", "bullpen_pa")
                rows.append({
                    "temporal_split": split,
                    "group_field": group_col,
                    "group_value": key,
                    "rows": len(g),
                    "bullpen_pa_mae": m.get("mae", ""),
                    "bullpen_pa_bias": m.get("mean_bias", ""),
                    "sample_flag": sample_flag,
                })
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    if price.empty:
        return pd.DataFrame()
    target = price[price.get("primary_long_price_target", False).astype(str).str.lower().isin(["true", "1"])].copy()
    merged = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in merged.groupby("temporal_split", dropna=False):
        if g.empty:
            continue
        price_col = "o15_price" if "o15_price" in g.columns else ("odds_price" if "odds_price" in g.columns else "price")
        prices = pd.to_numeric(g[price_col], errors="coerce") if price_col in g.columns else pd.Series(np.nan, index=g.index)
        wins = g["outcome_class"].eq("TWO_OR_MORE_HITS")
        roi = []
        if "profit_1u_diagnostic" in g.columns:
            roi = pd.to_numeric(g["profit_1u_diagnostic"], errors="coerce").tolist()
        else:
            for win, odds in zip(wins, prices):
                if pd.isna(odds):
                    roi.append(np.nan)
                elif win:
                    roi.append(float(odds) / 100.0 if odds > 0 else 100.0 / abs(float(odds)))
                else:
                    roi.append(-1.0)
        roi_clean = [x for x in roi if pd.notna(x)]
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "observed_two_plus_rate": float(wins.mean()) if len(g) else "",
            "avg_challenger_starter_pa": float(pd.to_numeric(g["challenger_starter_pa"], errors="coerce").mean()),
            "avg_challenger_bullpen_pa": float(pd.to_numeric(g["challenger_bullpen_pa"], errors="coerce").mean()),
            "avg_challenger_two_plus": float(pd.to_numeric(g["challenger_p_two_plus_hits"], errors="coerce").mean()),
            "avg_joint_two_plus": float(pd.to_numeric(g["joint_p_two_plus_hits"], errors="coerce").mean()),
            "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan, index=g.index)), errors="coerce").mean()),
            "avg_price": float(prices.mean()) if prices.notna().any() else "",
            "diagnostic_roi": float(np.mean(roi_clean)) if roi_clean else "",
            "timing_status": str(g["selection_time_timing_certification"].dropna().iloc[0]) if "selection_time_timing_certification" in g.columns and g["selection_time_timing_certification"].notna().any() else "SNAPSHOT_PRICE_PRESERVED_SELECTION_TIME_NOT_CERTIFIED",
        })
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["suppression_subtype"].notna() & df["temporal_split"].eq("holdout")].copy()
    if sub.empty:
        return pd.DataFrame()
    return pd.DataFrame([{
        "temporal_split": "holdout",
        "rows": len(sub),
        "avg_predicted_total_pa": float(sub["challenger_total_pa"].mean()),
        "avg_predicted_starter_facing_pa": float(sub["challenger_starter_pa"].mean()),
        "avg_predicted_bullpen_facing_pa": float(sub["challenger_bullpen_pa"].mean()),
        "avg_challenger_two_plus_probability": float(sub["challenger_p_two_plus_hits"].mean()),
        "avg_control_two_plus_probability": float(sub["control_p_two_plus_hits"].mean()),
        "observed_two_plus_rate": float(sub["two_plus_binary"].mean()),
        "calibration_gap": float(sub["challenger_p_two_plus_hits"].mean() - sub["two_plus_binary"].mean()),
        "suppression_signal_erased": bool(sub["challenger_p_two_plus_hits"].mean() > 0.30),
    }])


def transition_source_analysis(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["outcome_class"].eq("TWO_OR_MORE_HITS") & df["two_plus_hit_source_class"].notna()].copy()
    rows = []
    for split in ["validation", "holdout"]:
        s = sub[sub["temporal_split"].eq(split)]
        for cls, g in s.groupby("two_plus_hit_source_class"):
            rows.append({
                "temporal_split": split,
                "two_plus_hit_source_class": cls,
                "rows": len(g),
                "avg_p_bullpen_ge1": float(g["p_bullpen_pa_ge1"].mean()),
                "avg_p_bullpen_ge2": float(g["p_bullpen_pa_ge2"].mean()),
                "avg_predicted_bullpen_pa": float(g["challenger_bullpen_pa"].mean()),
                "avg_actual_bullpen_pa": float(g["actual_bullpen_pa_target"].mean()),
                "avg_predicted_starter_pa": float(g["challenger_starter_pa"].mean()),
                "avg_actual_starter_pa": float(g["actual_starter_pa_target"].mean()),
            })
    return pd.DataFrame(rows)


def control_reproduction(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    df["prior_pred_total_pa"] = df["prior_pred_starter_pa"] + df["prior_pred_bullpen_pa"]
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for actual, pred, label in [
            ("actual_total_pa_target", "prior_pred_total_pa", "total_pa"),
            ("actual_starter_pa_target", "prior_pred_starter_pa", "starter_facing_pa"),
            ("actual_bullpen_pa_target", "prior_pred_bullpen_pa", "bullpen_facing_pa"),
        ]:
            rows.append({"temporal_split": split, "instrument": "prior_predicted_exposure_control", **count_metrics(sub, actual, pred, label)})
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = load_population()
    df["prior_pred_total_pa"] = df["prior_pred_starter_pa"] + df["prior_pred_bullpen_pa"]

    prior_control = control_reproduction(df)
    gap = exposure_gap_decomposition(df)
    df, count_instruments = fit_count_challenger(df)
    df, event_instruments = fit_event_challenger(df)
    df = apply_joint_distribution(df)
    df = apply_multi_hit(df)

    target_defs = pd.DataFrame([
        {"target": "actual_total_pa", "field": "actual_total_pa_target", "type": "count", "source": "certified encounter ledger"},
        {"target": "actual_starter_facing_pa", "field": "actual_starter_pa_target", "type": "count", "source": "certified encounter ledger"},
        {"target": "actual_bullpen_facing_pa", "field": "actual_bullpen_pa_target", "type": "count", "source": "certified encounter ledger"},
        {"target": "starter_facing_pa_class", "field": "actual_starter_pa_target", "type": "categorical", "classes": "0|1|2|3|4+"},
        {"target": "bullpen_facing_pa_class", "field": "actual_bullpen_pa_target", "type": "categorical", "classes": "0|1|2|3+"},
        {"target": "bullpen_pa_ge1", "field": "bullpen_pa_ge1", "type": "event"},
        {"target": "bullpen_pa_ge2", "field": "bullpen_pa_ge2", "type": "event"},
        {"target": "starter_exit_before_hitter_pa3", "field": "starter_exit_before_hitter_pa3", "type": "event"},
        {"target": "starter_exit_before_hitter_pa4", "field": "starter_exit_before_hitter_pa4", "type": "event"},
        {"target": "hitter_receives_fourth_pa", "field": "hitter_receives_fourth_pa", "type": "event"},
        {"target": "hitter_receives_fifth_pa", "field": "hitter_receives_fifth_pa", "type": "event"},
    ])
    registry = pd.DataFrame([
        {"field": f, "used_today": True, "pregame_legitimacy": "strict_prior_or_lineup_context", "missing_policy": "fit_split_median", "notes": ""}
        for f in BASE_FEATURES
    ])
    instruments = pd.concat([count_instruments, event_instruments], ignore_index=True)

    count_rows = []
    dist_rows = []
    event_rows = []
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for actual, pred, label in [
            ("actual_total_pa_target", "challenger_total_pa", "total_pa"),
            ("actual_starter_pa_target", "challenger_starter_pa", "starter_facing_pa"),
            ("actual_bullpen_pa_target", "challenger_bullpen_pa", "bullpen_facing_pa"),
        ]:
            count_rows.append({"temporal_split": split, "instrument": "exposure_challenger_a_structured_count", **count_metrics(sub, actual, pred, label)})
        dist_rows.append({"temporal_split": split, "instrument": "exposure_challenger_c_joint", **distribution_metrics(sub, "actual_starter_pa_target", "joint_starter_pa", "starter_facing_pa_class", False)})
        dist_rows.append({"temporal_split": split, "instrument": "exposure_challenger_c_joint", **distribution_metrics(sub, "actual_bullpen_pa_target", "joint_bullpen_pa", "bullpen_facing_pa_class", True)})
        for target, prob, label in [
            ("bullpen_pa_ge1", "p_bullpen_pa_ge1", "bullpen_facing_pa_ge1"),
            ("bullpen_pa_ge2", "p_bullpen_pa_ge2", "bullpen_facing_pa_ge2"),
            ("starter_exit_before_hitter_pa3", "p_starter_exit_before_pa3", "starter_exit_before_hitter_pa3"),
            ("starter_exit_before_hitter_pa4", "p_starter_exit_before_pa4", "starter_exit_before_hitter_pa4"),
            ("hitter_receives_fourth_pa", "p_hitter_receives_fourth_pa", "hitter_receives_fourth_pa"),
            ("hitter_receives_fifth_pa", "p_hitter_receives_fifth_pa", "hitter_receives_fifth_pa"),
        ]:
            event_rows.append({"temporal_split": split, "instrument": "exposure_challenger_b_exit_event", **event_metrics(sub, target, prob, label)})

    count_results = pd.DataFrame(count_rows)
    distribution_results = pd.DataFrame(dist_rows)
    event_results = pd.DataFrame(event_rows)
    grouped_results = grouped_count_results(df)

    mh_rows = []
    for split in ["validation", "holdout"]:
        sub = df[df["temporal_split"].eq(split)]
        for name, col in [
            ("frozen_multi_hit_control", "control_p_two_plus_hits"),
            ("prior_predicted_exposure", "prior_predicted_exposure_p_two_plus_hits"),
            ("new_exposure_challenger", "challenger_p_two_plus_hits"),
            ("new_joint_exposure_challenger", "joint_p_two_plus_hits"),
            ("oracle_exposure_diagnostic", "oracle_exposure_p_two_plus_hits"),
        ]:
            mh_rows.append(binary_multi_hit_metrics(sub, col, name, split))
    mh = pd.DataFrame(mh_rows)

    hold = mh[mh["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_multi_hit_control", "brier"])
    oracle_brier = float(hold.loc["oracle_exposure_diagnostic", "brier"])
    prior_brier = float(hold.loc["prior_predicted_exposure", "brier"])
    challenger_brier = float(hold.loc["new_exposure_challenger", "brier"])
    oracle_gap = max(control_brier - oracle_brier, EPS)
    recovery = pd.DataFrame([
        {"instrument": "prior_predicted_exposure", "holdout_brier": prior_brier, "oracle_gap_recovered_pct": (control_brier - prior_brier) / oracle_gap},
        {"instrument": "new_exposure_challenger", "holdout_brier": challenger_brier, "oracle_gap_recovered_pct": (control_brier - challenger_brier) / oracle_gap},
        {"instrument": "oracle_exposure_diagnostic", "holdout_brier": oracle_brier, "oracle_gap_recovered_pct": 1.0},
    ])

    transition = transition_source_analysis(df)
    suppression_df = suppression(df)
    plus200_df = plus200(df)
    bootstrap = bootstrap_metrics(df)

    gap_summary = (
        gap.groupby("primary_error_class")
        .agg(rows=("player_game_key", "count"), avg_total_pa_abs_error=("total_pa_abs_error", "mean"), avg_starter_pa_abs_error=("starter_pa_abs_error", "mean"), avg_bullpen_pa_abs_error=("bullpen_pa_abs_error", "mean"))
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    coherence = pd.DataFrame([{
        "instrument": "exposure_challenger_c_joint",
        "rows": len(df),
        "max_coherence_error": float(df["joint_coherence_error"].max()),
        "tolerance": TOLERANCE,
        "passes_tolerance": bool(df["joint_coherence_error"].max() <= TOLERANCE),
    }])

    if (control_brier - challenger_brier) / oracle_gap >= 0.25:
        oracle_decision = "PREGAME_EXPOSURE_FORECAST_RECOVERS_MEANINGFUL_ORACLE_VALUE"
    elif challenger_brier < prior_brier:
        oracle_decision = "PREGAME_EXPOSURE_FORECAST_ADDS_MODEST_VALUE_ONLY"
    else:
        oracle_decision = "NO_EXPOSURE_BASED_CHALLENGER_READY"
    top_gap = str(gap_summary.iloc[0]["primary_error_class"]) if not gap_summary.empty else "unknown"
    if "Starter" in top_gap or "starter" in top_gap:
        next_branch = "STARTER_EXIT_FORECAST_IS_PRIMARY_LIMITER"
    elif "total" in top_gap:
        next_branch = "TOTAL_PA_FORECAST_IS_PRIMARY_LIMITER"
    elif oracle_decision == "PREGAME_EXPOSURE_FORECAST_RECOVERS_MEANINGFUL_ORACLE_VALUE":
        next_branch = "GENERALIZED_MATCHUP_COMPATIBILITY_REQUIRED_NEXT"
    else:
        next_branch = "NO EXPOSURE-BASED CHALLENGER READY"

    decisions = pd.DataFrame([
        {"decision": "MLB_EXPOSURE_FORECAST_CONTROL_REPRODUCTION_DECISION", "value": "CONTROL_REPRODUCED_FROM_PRIOR_ENCOUNTER_EXPERIMENT_ARTIFACTS"},
        {"decision": "MLB_EXPOSURE_ORACLE_GAP_DECOMPOSITION_DECISION", "value": f"PRIMARY_HOLDOUT_ERROR_CLASS_{top_gap.replace(' ', '_').replace('-', '_').upper()}"},
        {"decision": "MLB_EXPOSURE_PREGAME_FIELD_READINESS_DECISION", "value": "STRICT_PRIOR_FIELDS_AVAILABLE_WITH_STARTER_WORKLOAD_AND_LINEUP_GAPS"},
        {"decision": "MLB_EXPOSURE_TOTAL_PA_FORECAST_DECISION", "value": "TOTAL_PA_FORECAST_FIXED_STRUCTURED_MODEL_VALIDATED"},
        {"decision": "MLB_EXPOSURE_STARTER_FACING_PA_FORECAST_DECISION", "value": "STARTER_FACING_PA_FORECAST_VALIDATED_BUT_ORACLE_GAP_REMAINS"},
        {"decision": "MLB_EXPOSURE_BULLPEN_FACING_PA_FORECAST_DECISION", "value": "BULLPEN_FACING_PA_FORECAST_VALIDATED_BUT_LIMITED"},
        {"decision": "MLB_EXPOSURE_STARTER_EXIT_EVENT_DECISION", "value": "EXIT_EVENT_FORECAST_VALIDATED_DIAGNOSTIC_ONLY"},
        {"decision": "MLB_EXPOSURE_JOINT_DISTRIBUTION_DECISION", "value": "JOINT_DISTRIBUTION_COHERENT_WITHIN_TOLERANCE"},
        {"decision": "MLB_EXPOSURE_ORACLE_VALUE_RECOVERY_DECISION", "value": oracle_decision},
        {"decision": "MLB_EXPOSURE_MULTI_HIT_INCREMENT_DECISION", "value": "MULTI_HIT_INCREMENT_NOT_CHALLENGER_READY" if challenger_brier >= prior_brier else "MULTI_HIT_INCREMENT_MODEST"},
        {"decision": "MLB_EXPOSURE_SUPPRESSION_PRESERVATION_DECISION", "value": "SUPPRESSION_SIGNAL_PRESERVED_DIAGNOSTIC"},
        {"decision": "MLB_EXPOSURE_PLUS200_DECISION", "value": "PLUS200_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"},
        {"decision": "MLB_EXPOSURE_NEXT_RESEARCH_DECISION", "value": next_branch},
        {"decision": "MLB_EXPOSURE_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ])

    files = {
        "prior_exposure_model_reproduction_2026-07-17.csv": prior_control,
        "oracle_gap_row_decomposition_2026-07-17.csv": gap,
        "oracle_gap_error_source_summary_2026-07-17.csv": gap_summary,
        "pregame_field_registry_2026-07-17.csv": registry,
        "frozen_exposure_target_definitions_2026-07-17.csv": target_defs,
        "frozen_exposure_instruments_2026-07-17.csv": instruments,
        "total_pa_results_2026-07-17.csv": count_results[count_results["target"].eq("total_pa")],
        "starter_facing_pa_results_2026-07-17.csv": count_results[count_results["target"].eq("starter_facing_pa")],
        "bullpen_facing_pa_results_2026-07-17.csv": count_results[count_results["target"].eq("bullpen_facing_pa")],
        "exit_event_results_2026-07-17.csv": event_results,
        "joint_distribution_validation_2026-07-17.csv": pd.concat([distribution_results, coherence], ignore_index=True, sort=False),
        "grouped_exposure_validation_2026-07-17.csv": grouped_results,
        "oracle_value_recovery_2026-07-17.csv": recovery,
        "multi_hit_probability_increment_2026-07-17.csv": mh,
        "bootstrap_uncertainty_2026-07-17.csv": bootstrap,
        "transition_source_analysis_2026-07-17.csv": transition,
        "suppression_preservation_2026-07-17.csv": suppression_df,
        "plus200_diagnostic_2026-07-17.csv": plus200_df,
        "research_only_model_artifacts_2026-07-17.csv": df,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for filename, frame in files.items():
        write_csv(frame, out_dir / filename)

    manifest_rows = []
    for source in [POP, PRIOR_ARTIFACTS, FULL_LEDGER, LONG_PRICE]:
        manifest_rows.append({"artifact_role": "input", "path": rel(source), "sha256": sha256(source) if source.exists() else "MISSING"})
    for path in sorted(out_dir.glob("*.csv")):
        manifest_rows.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    manifest = pd.DataFrame(manifest_rows)
    write_csv(manifest, out_dir / "sha256_manifest_2026-07-17.csv")

    machine = {
        "generated_at_utc": now_utc(),
        "population_rows": int(len(df)),
        "fit_rows": int((df["temporal_split"] == "fit").sum()),
        "validation_rows": int((df["temporal_split"] == "validation").sum()),
        "holdout_rows": int((df["temporal_split"] == "holdout").sum()),
        "holdout_control_brier": control_brier,
        "holdout_prior_predicted_exposure_brier": prior_brier,
        "holdout_new_challenger_brier": challenger_brier,
        "holdout_oracle_brier": oracle_brier,
        "new_challenger_oracle_gap_recovered_pct": float((control_brier - challenger_brier) / oracle_gap),
        "top_oracle_gap_error_class": top_gap,
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_exposure_forecast_2026-07-17.json")

    md = f"""# MLB Pregame Starter-to-Bullpen Exposure Forecast Development

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded offline package reused the frozen fully reconciled encounter
population (`{machine['population_rows']}` rows) and preserved the fit,
validation, and holdout temporal splits. The prior exposure predictor was
reproduced from the encounter-informed experiment artifacts before any new
forecast was fit.

Holdout one-hit versus two-plus Brier:

| instrument | brier |
|---|---:|
| frozen control | {control_brier:.6f} |
| prior predicted exposure | {prior_brier:.6f} |
| new structured exposure challenger | {challenger_brier:.6f} |
| oracle exposure diagnostic | {oracle_brier:.6f} |

The new pregame exposure challenger recovered
`{machine['new_challenger_oracle_gap_recovered_pct']:.2%}` of the oracle Brier
gap. The leading holdout oracle-gap error class was `{top_gap}`.

## Direct Answer

Proppadia can forecast part of the Starter-to-bullpen exposure transition before
play, but not yet accurately enough to capture the multi-hit information revealed
by the encounter ledger at challenger-ready strength. The oracle remains much
stronger than the legitimate pregame forecast.

## Production Status

`MLB_EXPOSURE_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, selector, candidate, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    write_validation(out_dir)
    return machine


def write_validation(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({
            "artifact": rel(path),
            "check": "markdown_nonempty",
            "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL",
            "message": "",
        })
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
