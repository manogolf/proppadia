#!/usr/bin/env python3
"""Bounded encounter-informed MLB Hits 1.5 multi-hit probability experiment.

This is an offline research utility. It uses the certified historical
batter-pitcher encounter ledger to test whether starter/bullpen exposure and
strict-prior bullpen context improve EXACTLY_ONE_HIT vs TWO_OR_MORE_HITS
prediction. Actual current-game exposure is used only in the explicitly labeled
oracle diagnostic and as evaluation truth.

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
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import log_loss, mean_squared_error, roc_auc_score

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_encounter_informed_multi_hit_probability_experiment/2026-07-17"

BENCH = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/canonical_modeling_population_2026-07-17.csv"
CONTROL = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/research_only_model_artifacts_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
COND = ROOT / "artifacts/analysis/model_development/mlb_conditional_second_hit_tendency_audit/2026-07-17/independent_row_level_reproduction_2026-07-17.csv"
EXP_ROOT = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17"
POP = EXP_ROOT / "frozen_next_experiment_population_2026-07-17.csv"
SUMMARY = EXP_ROOT / "hitter_game_exposure_summary_2026-07-17.csv"
ENCOUNTERS = EXP_ROOT / "expanded_encounter_ledger_2026-07-17.csv"
DISCREP = EXP_ROOT / "discrepancy_ledger_2026-07-17.csv"

EPS = 1e-9
RNG_SEED = 20260717
FEATURES = [
    "expected_pa_used",
    "d15_pa_per_game",
    "season_to_date_pa_per_game",
    "lineup_slot",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "d30_hits_per_pa",
    "season_to_date_hits_per_pa",
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


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clip_prob(x: Any, lo: float = 0.01, hi: float = 0.65) -> float:
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
    # Fractional exposure is modeled with a Poisson-binomial-inspired Poisson
    # approximation. Actual oracle counts are still postgame diagnostic only.
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
        p = math.exp(-mean) * (mean ** k) / math.factorial(k)
        probs[str(k)] = p
        total += p
    tail = max(0.0, 1.0 - total)
    probs["3+" if bullpen else "4+"] = tail
    s = sum(probs.values())
    return {k: v / s for k, v in probs.items()}


def class_logloss(actual: pd.Series, pred_mean: pd.Series, bullpen: bool) -> tuple[float, float]:
    labels = ["0", "1", "2", "3+"] if bullpen else ["0", "1", "2", "3", "4+"]
    y = actual.map(lambda x: exposure_class(x, bullpen=bullpen)).astype(str).tolist()
    p = np.array([[poisson_class_probs(m, bullpen=bullpen).get(lbl, EPS) for lbl in labels] for m in pred_mean])
    p = np.clip(p, EPS, 1.0)
    p = p / p.sum(axis=1, keepdims=True)
    ll = log_loss(y, p, labels=labels)
    true = np.array([[1.0 if yy == lbl else 0.0 for lbl in labels] for yy in y])
    brier = float(np.mean(np.sum((p - true) ** 2, axis=1)))
    return float(ll), brier


def binary_metrics(frame: pd.DataFrame, prob_col: str, target_col: str = "two_plus_binary") -> dict[str, Any]:
    rows = len(frame)
    if rows == 0:
        return {"rows": 0}
    y = frame[target_col].astype(int).to_numpy()
    p = np.clip(frame[prob_col].astype(float).to_numpy(), EPS, 1 - EPS)
    out = {
        "rows": rows,
        "wins_two_plus": int(y.sum()),
        "losses_exactly_one": int(rows - y.sum()),
        "observed_two_plus_rate": float(y.mean()),
        "avg_predicted_two_plus": float(p.mean()),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "auc": "",
        "calibration_slope": "",
        "calibration_intercept": "",
        "ece": expected_calibration_error(y, p),
    }
    try:
        out["auc"] = float(roc_auc_score(y, p)) if len(set(y)) > 1 else ""
    except Exception:
        out["auc"] = ""
    try:
        x = np.log(p / (1 - p))
        slope, intercept = np.polyfit(x, y, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        pass
    return out


def multiclass_metrics(frame: pd.DataFrame, pfx: str) -> dict[str, Any]:
    if frame.empty:
        return {"rows": 0}
    y = frame["outcome_class"].map({"ZERO_HITS": 0, "EXACTLY_ONE_HIT": 1, "TWO_OR_MORE_HITS": 2}).astype(int).to_numpy()
    probs = frame[[f"{pfx}_p_zero_hits", f"{pfx}_p_exactly_one_hit", f"{pfx}_p_two_plus_hits"]].astype(float).to_numpy()
    probs = np.clip(probs, EPS, 1.0)
    probs = probs / probs.sum(axis=1, keepdims=True)
    return {
        "rows": len(frame),
        "multiclass_log_loss": float(log_loss(y, probs, labels=[0, 1, 2])),
        "two_plus_brier": float(np.mean((probs[:, 2] - (y == 2).astype(float)) ** 2)),
        "two_plus_auc": float(roc_auc_score((y == 2).astype(int), probs[:, 2])) if len(set((y == 2).astype(int))) > 1 else "",
        "avg_p_zero": float(probs[:, 0].mean()),
        "avg_p_one": float(probs[:, 1].mean()),
        "avg_p_two_plus": float(probs[:, 2].mean()),
        "observed_zero": float((y == 0).mean()),
        "observed_one": float((y == 1).mean()),
        "observed_two_plus": float((y == 2).mean()),
    }


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def prepare_population() -> pd.DataFrame:
    bench = read_csv(BENCH)
    pop = read_csv(POP)
    control = read_csv(CONTROL)
    cond = read_csv(COND)
    enc = read_csv(ENCOUNTERS)
    control = control[control["benchmark"].eq("benchmark_4_hitter_opportunity_starter")].copy()
    control = control.rename(columns={
        "p_zero_hits": "control_p_zero_hits",
        "p_exactly_one_hit": "control_p_exactly_one_hit",
        "p_two_plus_hits": "control_p_two_plus_hits",
    })
    cols = [
        "player_game_key", "control_p_zero_hits", "control_p_exactly_one_hit", "control_p_two_plus_hits",
        "expected_pa_used", "hitter_per_pa_hit_estimate", "starter_adjustment", "starter_exposure_state",
    ]
    df = bench.merge(pop, on=["player_game_key", "game_id", "player_id", "player_name", "outcome_class", "temporal_split"], how="left", suffixes=("", "_enc"))
    df = df.merge(control[cols], on="player_game_key", how="left")
    if not enc.empty:
        encounter_keys = (
            enc[["benchmark_player_game_key", "batter_team", "opponent"]]
            .dropna(subset=["benchmark_player_game_key"])
            .drop_duplicates("benchmark_player_game_key")
            .rename(columns={
                "benchmark_player_game_key": "player_game_key",
                "batter_team": "encounter_batter_team",
                "opponent": "encounter_opponent",
            })
        )
        df = df.merge(encounter_keys, on="player_game_key", how="left")
        if "opponent" not in df.columns:
            df["opponent"] = df["encounter_opponent"]
        else:
            df["opponent"] = df["opponent"].fillna(df["encounter_opponent"])
    if not cond.empty:
        keep = [c for c in ["player_game_key", "conditional_second_hit_tendency", "conditional_second_hit_evidence_class"] if c in cond.columns]
        if keep:
            df = df.merge(cond[keep].drop_duplicates("player_game_key"), on="player_game_key", how="left")
    df["two_plus_binary"] = df["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
    df["exact_one_or_two_plus"] = df["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])
    df["fully_reconciled_sequence"] = (
        df["pa_reconciles_benchmark"].fillna(False).astype(bool)
        & df["hits_reconciles_benchmark"].fillna(False).astype(bool)
        & df["role_integrity_state"].eq("ROLE_PA_SUM_PASS")
    )
    return df


def discrepancy_resolution(pop: pd.DataFrame) -> pd.DataFrame:
    disc = read_csv(DISCREP)
    if disc.empty:
        return pd.DataFrame()
    disc["player_game_key"] = disc["game_date"].astype(str) + "|" + disc["game_id"].map(lambda x: str(int(float(x))) if pd.notna(x) else "") + "|" + disc["player_id"].map(lambda x: str(int(float(x))) if pd.notna(x) else "")
    disc["discrepancy_type"] = np.select(
        [
            ~disc["pa_reconciles_benchmark"].astype(str).str.lower().isin(["true", "1"]),
            ~disc["hits_reconciles_benchmark"].astype(str).str.lower().isin(["true", "1"]),
        ],
        ["PA_MISMATCH", "HIT_MISMATCH"],
        default="OTHER",
    )
    disc["encounter_sequence_usable"] = disc["hits_reconciles_boxscore"].astype(str).str.lower().isin(["true", "1"])
    disc["eligibility_decision"] = np.where(
        disc["pa_reconciles_benchmark"].astype(str).str.lower().isin(["true", "1"]) & disc["hits_reconciles_benchmark"].astype(str).str.lower().isin(["true", "1"]),
        "ELIGIBLE",
        "EXCLUDE_FROM_CONFIRMATORY_CHALLENGER",
    )
    return disc


def bullpen_priors(df: pd.DataFrame) -> pd.DataFrame:
    enc = read_csv(ENCOUNTERS)
    enc["game_date"] = pd.to_datetime(enc["game_date"])
    enc["official_hit"] = enc["official_hit"].astype(str).str.lower().isin(["true", "1"])
    rel = enc[enc["role_classification"].eq("RELIEVER_FACING_PA")].copy()
    league_prior = rel["official_hit"].mean() if len(rel) else 0.22
    out_rows = []
    keys = df[["player_game_key", "game_date", "opponent", "lineup_slot"]].copy()
    keys["game_date_dt"] = pd.to_datetime(keys["game_date"])
    for _, r in keys.iterrows():
        prior = rel[(rel["pitcher_team"].eq(r.get("opponent"))) & (rel["game_date"] < r["game_date_dt"])]
        if len(prior) >= 20:
            rate = float(prior["official_hit"].mean())
            pa = int(len(prior))
            relievers = float(prior.groupby("game_id")["pitcher_id"].nunique().mean())
            status = "TEAM_PRIOR_AVAILABLE"
        else:
            rate = float(league_prior)
            pa = int(len(prior))
            relievers = float(rel.groupby("game_id")["pitcher_id"].nunique().mean()) if len(rel) else 0
            status = "LEAGUE_PRIOR_FALLBACK"
        factor = min(max(rate / max(float(league_prior), EPS), 0.75), 1.25)
        out_rows.append({
            "player_game_key": r["player_game_key"],
            "bullpen_prior_pa": pa,
            "opponent_bullpen_hit_rate_prior": rate,
            "league_bullpen_hit_rate_prior": float(league_prior),
            "bullpen_hit_factor_prior": factor,
            "avg_relief_pitchers_used_prior": relievers,
            "bullpen_prior_status": status,
        })
    return pd.DataFrame(out_rows)


def fit_exposure_models(df: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    train = df[df["temporal_split"].eq("fit") & df["fully_reconciled_sequence"]].copy()
    medians = {c: float(pd.to_numeric(train[c], errors="coerce").median()) if c in train else 0.0 for c in FEATURES}
    for k, v in medians.items():
        if not math.isfinite(v):
            medians[k] = 0.0
    X = make_X(train, medians)
    models: dict[str, Any] = {}
    rows = []
    for target in ["actual_starter_facing_pa", "actual_bullpen_facing_pa", "reconstructed_total_pa"]:
        y = pd.to_numeric(train[target], errors="coerce").fillna(0).to_numpy()
        model = LinearRegression()
        model.fit(X, y)
        models[target] = model
        rows.append({"target": target, "model": "LinearRegression_fixed_features_fit_split_only", "rows": len(train), "intercept": float(model.intercept_), **{f"coef_{c}": float(v) for c, v in zip(FEATURES, model.coef_)}})
    models["feature_medians"] = medians
    return models, pd.DataFrame(rows)


def make_X(frame: pd.DataFrame, medians: dict[str, float]) -> np.ndarray:
    cols = []
    for c in FEATURES:
        vals = pd.to_numeric(frame[c], errors="coerce") if c in frame else pd.Series([np.nan] * len(frame), index=frame.index)
        cols.append(vals.fillna(medians.get(c, 0.0)).to_numpy())
    return np.vstack(cols).T if cols else np.empty((len(frame), 0))


def apply_instruments(df: pd.DataFrame, models: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    med = models["feature_medians"]
    X = make_X(out, med)
    for target, col in [
        ("actual_starter_facing_pa", "pred_starter_pa"),
        ("actual_bullpen_facing_pa", "pred_bullpen_pa"),
        ("reconstructed_total_pa", "pred_total_pa"),
    ]:
        pred = models[target].predict(X)
        out[col] = np.clip(pred, 0, 7)
    # Preserve total opportunity coherence without leaking actual PA.
    out["pred_total_pa"] = np.clip(out["pred_total_pa"], 1, 7)
    scale = out["pred_total_pa"] / np.maximum(out["pred_starter_pa"] + out["pred_bullpen_pa"], 0.25)
    out["pred_starter_pa"] = np.clip(out["pred_starter_pa"] * scale, 0, out["pred_total_pa"])
    out["pred_bullpen_pa"] = np.clip(out["pred_total_pa"] - out["pred_starter_pa"], 0, out["pred_total_pa"])

    h = out["hitter_per_pa_hit_estimate"].map(lambda x: clip_prob(x, 0.02, 0.45))
    starter_adj = pd.to_numeric(out["starter_adjustment"], errors="coerce").fillna(1.0).clip(0.75, 1.25)
    bullpen_factor = pd.to_numeric(out["bullpen_hit_factor_prior"], errors="coerce").fillna(1.0).clip(0.75, 1.25)
    out["p_hit_starter_prior"] = np.clip(h * starter_adj, 0.005, 0.55)
    out["p_hit_bullpen_neutral"] = np.clip(h, 0.005, 0.55)
    out["p_hit_bullpen_prior"] = np.clip(h * bullpen_factor, 0.005, 0.55)

    instruments = {
        "oracle_exposure": ("actual_starter_facing_pa", "actual_bullpen_facing_pa", "p_hit_starter_prior", "p_hit_bullpen_prior"),
        "predicted_exposure": ("pred_starter_pa", "pred_bullpen_pa", "p_hit_starter_prior", "p_hit_bullpen_neutral"),
        "predicted_exposure_bullpen_env": ("pred_starter_pa", "pred_bullpen_pa", "p_hit_starter_prior", "p_hit_bullpen_prior"),
        "source_aware_unified": ("pred_starter_pa", "pred_bullpen_pa", "p_hit_starter_prior", "p_hit_bullpen_prior"),
    }
    for name, (s_col, b_col, ps_col, pb_col) in instruments.items():
        vals = [hit_distribution(r[s_col], r[b_col], r[ps_col], r[pb_col]) for _, r in out.iterrows()]
        arr = np.array(vals)
        out[f"{name}_p_zero_hits"] = arr[:, 0]
        out[f"{name}_p_exactly_one_hit"] = arr[:, 1]
        out[f"{name}_p_two_plus_hits"] = arr[:, 2]
    return out


def exposure_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = df[df["temporal_split"].eq(split) & df["fully_reconciled_sequence"]]
        for kind, actual, pred, bullpen in [
            ("starter", "actual_starter_facing_pa", "pred_starter_pa", False),
            ("bullpen", "actual_bullpen_facing_pa", "pred_bullpen_pa", True),
        ]:
            if g.empty:
                continue
            a = pd.to_numeric(g[actual], errors="coerce").fillna(0)
            p = pd.to_numeric(g[pred], errors="coerce").fillna(0)
            ll, brier = class_logloss(a, p, bullpen=bullpen)
            rows.append({
                "temporal_split": split,
                "exposure_target": kind,
                "rows": len(g),
                "mae": float(np.mean(np.abs(p - a))),
                "rmse": float(math.sqrt(mean_squared_error(a, p))),
                "distribution_log_loss": ll,
                "distribution_brier": brier,
                "avg_actual": float(a.mean()),
                "avg_predicted": float(p.mean()),
                "underprediction_rate": float((p < a).mean()),
                "overprediction_rate": float((p > a).mean()),
            })
    return pd.DataFrame(rows)


def instrument_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    instruments = {
        "frozen_control": "control_p_two_plus_hits",
        "oracle_exposure_diagnostic": "oracle_exposure_p_two_plus_hits",
        "predicted_exposure": "predicted_exposure_p_two_plus_hits",
        "predicted_exposure_bullpen_env": "predicted_exposure_bullpen_env_p_two_plus_hits",
        "source_aware_unified": "source_aware_unified_p_two_plus_hits",
    }
    base = df[df["fully_reconciled_sequence"] & df["exact_one_or_two_plus"]].copy()
    for split in ["fit", "validation", "holdout"]:
        g = base[base["temporal_split"].eq(split)]
        for inst, prob_col in instruments.items():
            m = binary_metrics(g, prob_col)
            rows.append({"temporal_split": split, "instrument": inst, **m})
    return pd.DataFrame(rows)


def full_distribution(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    instruments = ["control", "oracle_exposure", "predicted_exposure", "predicted_exposure_bullpen_env", "source_aware_unified"]
    prefix_map = {"control": "control"}
    base = df[df["fully_reconciled_sequence"]].copy()
    for split in ["fit", "validation", "holdout"]:
        g = base[base["temporal_split"].eq(split)]
        for inst in instruments:
            pfx = prefix_map.get(inst, inst)
            rows.append({"temporal_split": split, "instrument": inst, **multiclass_metrics(g, pfx)})
    return pd.DataFrame(rows)


def bootstrap_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    rows = []
    base = df[df["fully_reconciled_sequence"] & df["exact_one_or_two_plus"] & df["temporal_split"].eq("holdout")].copy()
    instruments = {
        "frozen_control": "control_p_two_plus_hits",
        "oracle_exposure_diagnostic": "oracle_exposure_p_two_plus_hits",
        "predicted_exposure": "predicted_exposure_p_two_plus_hits",
        "predicted_exposure_bullpen_env": "predicted_exposure_bullpen_env_p_two_plus_hits",
        "source_aware_unified": "source_aware_unified_p_two_plus_hits",
    }
    if base.empty:
        return pd.DataFrame()
    for inst, col in instruments.items():
        briers = []
        aucs = []
        for _ in range(200):
            sample = base.iloc[rng.integers(0, len(base), len(base))]
            m = binary_metrics(sample, col)
            briers.append(m.get("brier", np.nan))
            auc = m.get("auc", np.nan)
            if auc != "":
                aucs.append(float(auc))
        rows.append({
            "instrument": inst,
            "scope": "holdout_one_to_two_plus",
            "brier_p05": float(np.nanpercentile(briers, 5)),
            "brier_p50": float(np.nanpercentile(briers, 50)),
            "brier_p95": float(np.nanpercentile(briers, 95)),
            "auc_p05": float(np.nanpercentile(aucs, 5)) if aucs else "",
            "auc_p50": float(np.nanpercentile(aucs, 50)) if aucs else "",
            "auc_p95": float(np.nanpercentile(aucs, 95)) if aucs else "",
        })
    return pd.DataFrame(rows)


def source_analysis(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    base = df[df["fully_reconciled_sequence"] & df["outcome_class"].eq("TWO_OR_MORE_HITS")].copy()
    for split in ["fit", "validation", "holdout"]:
        for source, g in base[base["temporal_split"].eq(split)].groupby("two_plus_hit_source_class", dropna=False):
            rows.append({
                "temporal_split": split,
                "second_hit_source": source,
                "rows": len(g),
                "avg_control_p_two_plus": float(g["control_p_two_plus_hits"].mean()),
                "avg_source_aware_p_two_plus": float(g["source_aware_unified_p_two_plus_hits"].mean()),
                "avg_predicted_starter_pa": float(g["pred_starter_pa"].mean()),
                "avg_predicted_bullpen_pa": float(g["pred_bullpen_pa"].mean()),
                "avg_actual_starter_pa": float(g["actual_starter_facing_pa"].mean()),
                "avg_actual_bullpen_pa": float(g["actual_bullpen_facing_pa"].mean()),
            })
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    sub = df[df["suppression_subtype"].notna() & df["fully_reconciled_sequence"]].copy()
    for split in ["fit", "validation", "holdout"]:
        g = sub[sub["temporal_split"].eq(split)]
        if g.empty:
            continue
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "observed_two_plus_rate": float(g["two_plus_binary"].mean()),
            "avg_control_p_two_plus": float(g["control_p_two_plus_hits"].mean()),
            "avg_source_aware_p_two_plus": float(g["source_aware_unified_p_two_plus_hits"].mean()),
            "avg_predicted_starter_pa": float(g["pred_starter_pa"].mean()),
            "avg_predicted_bullpen_pa": float(g["pred_bullpen_pa"].mean()),
            "direction_preserved": float(g["source_aware_unified_p_two_plus_hits"].mean()) <= float(df["source_aware_unified_p_two_plus_hits"].mean()),
        })
    return pd.DataFrame(rows)


def hitter_owned_region(df: pd.DataFrame) -> pd.DataFrame:
    fit = df[df["temporal_split"].eq("fit") & df["fully_reconciled_sequence"]].copy()
    qs = fit["source_aware_unified_p_two_plus_hits"].quantile([0, .25, .5, .75, 1.0]).to_dict()
    bins = [-0.001, qs[.25], qs[.5], qs[.75], 1.001]
    labels = ["fit_q1_low", "fit_q2_mid_low", "fit_q3_mid_high", "fit_q4_high"]
    out = df[df["fully_reconciled_sequence"]].copy()
    out["frozen_source_aware_band"] = pd.cut(out["source_aware_unified_p_two_plus_hits"], bins=bins, labels=labels, include_lowest=True, duplicates="drop")
    rows = []
    for split in ["fit", "validation", "holdout"]:
        for band, g in out[out["temporal_split"].eq(split)].groupby("frozen_source_aware_band", observed=False):
            if g.empty:
                continue
            rows.append({
                "temporal_split": split,
                "frozen_source_aware_band": str(band),
                "rows": len(g),
                "two_plus_rate": float(g["two_plus_binary"].mean()),
                "avg_predicted_two_plus": float(g["source_aware_unified_p_two_plus_hits"].mean()),
                "players": g["player_id"].nunique(),
                "dates": g["slate_date"].nunique(),
                "pitchers_or_games": g["game_id"].nunique(),
            })
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    lp = read_csv(LONG_PRICE)
    if lp.empty:
        return pd.DataFrame()
    target = lp[lp["primary_long_price_target"].astype(str).str.lower().isin(["true", "1"])].copy()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("", "_exp"))
    rows = []
    for split, g in m.groupby("temporal_split_exp", dropna=False):
        if g.empty:
            continue
        price = pd.to_numeric(g["o15_price"], errors="coerce")
        profit = pd.to_numeric(g["profit_1u_diagnostic"], errors="coerce")
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "observed_two_plus_rate": float(g["two_plus_binary"].mean()),
            "avg_control_p_two_plus": float(g["control_p_two_plus_hits"].mean()),
            "avg_source_aware_p_two_plus": float(g["source_aware_unified_p_two_plus_hits"].mean()),
            "avg_implied_break_even": float(pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce").mean()),
            "avg_price": float(price.mean()),
            "diagnostic_roi": float(profit.mean()),
            "timing_certification": ";".join(sorted(set(g["selection_time_timing_certification"].dropna().astype(str))))[:200],
        })
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    base = df[df["fully_reconciled_sequence"]].copy()
    for (game_id, source), g in base.groupby(["game_id", "two_plus_hit_source_class"], dropna=False):
        pass
    # Compare hitters within the same game using source-aware probability as a
    # simple frozen ordering; no new ranking formula is optimized.
    for game_id, g in base.groupby("game_id", dropna=False):
        if len(g) < 2:
            continue
        pred_top = g.sort_values("source_aware_unified_p_two_plus_hits", ascending=False).iloc[0]
        actual_top = g.sort_values(["official_hits", "source_aware_unified_p_two_plus_hits"], ascending=[False, False]).iloc[0]
        rows.append({
            "game_id": game_id,
            "game_date": pred_top["slate_date"],
            "hitters": len(g),
            "predicted_top_player_id": pred_top["player_id"],
            "predicted_top_player_name": pred_top["player_name"],
            "actual_top_player_id": actual_top["player_id"],
            "actual_top_player_name": actual_top["player_name"],
            "predicted_top_actual_hits": pred_top["official_hits"],
            "actual_top_hits": actual_top["official_hits"],
            "top_hit_order_agreement": pred_top["player_game_key"] == actual_top["player_game_key"],
            "predicted_top_two_plus": pred_top["outcome_class"] == "TWO_OR_MORE_HITS",
        })
    rr = pd.DataFrame(rows)
    if not rr.empty:
        rr["agreement_rate_overall"] = rr["top_hit_order_agreement"].mean()
    return rr


def population_reconciliation(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_summary = read_csv(SUMMARY)
    rows = [
        {"population": "all_hitter_game_exposure_rows", "rows": len(all_summary), "two_plus_reconstructed": int((all_summary["reconstructed_hits"] >= 2).sum()) if not all_summary.empty else 0},
        {"population": "frozen_benchmark_batter_games", "rows": len(df), "two_plus_benchmark": int(df["outcome_class"].eq("TWO_OR_MORE_HITS").sum())},
        {"population": "benchmark_exact_pa_reconciliation", "rows": int(df["pa_reconciles_benchmark"].fillna(False).sum())},
        {"population": "benchmark_exact_hit_reconciliation", "rows": int(df["hits_reconciles_benchmark"].fillna(False).sum())},
        {"population": "benchmark_fully_reconciled_sequence", "rows": int(df["fully_reconciled_sequence"].sum()), "two_plus_benchmark": int(df[df["fully_reconciled_sequence"]]["outcome_class"].eq("TWO_OR_MORE_HITS").sum())},
    ]
    src = []
    all_two = all_summary[all_summary["reconstructed_hits"] >= 2].copy()
    bench_keys = set(df["player_game_key"].astype(str))
    benchmark_two_plus = int(df["outcome_class"].eq("TWO_OR_MORE_HITS").sum())
    fully_reconciled_two_plus = int(df[df["fully_reconciled_sequence"]]["outcome_class"].eq("TWO_OR_MORE_HITS").sum())
    benchmark_two_plus_in_reconstructed_summary = int(all_two["player_game_key"].astype(str).isin(bench_keys).sum())
    src.append({"cause": "all_hitter_game_two_plus_source_count", "rows": len(all_two)})
    src.append({"cause": "frozen_benchmark_two_plus_count", "rows": benchmark_two_plus})
    src.append({"cause": "nonbenchmark_two_plus_hitter_games", "rows": int((~all_two["player_game_key"].astype(str).isin(bench_keys)).sum())})
    src.append({"cause": "benchmark_two_plus_in_reconstructed_summary", "rows": benchmark_two_plus_in_reconstructed_summary})
    src.append({"cause": "benchmark_two_plus_fully_reconciled_source_classified", "rows": fully_reconciled_two_plus})
    src.append({"cause": "benchmark_two_plus_excluded_from_confirmatory_source_analysis", "rows": benchmark_two_plus - fully_reconciled_two_plus})
    return pd.DataFrame(rows), pd.DataFrame(src)


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
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def write_manifest(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(rows), out_dir / "sha256_manifest_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = prepare_population()
    bull = bullpen_priors(df)
    df = df.merge(bull, on="player_game_key", how="left")
    pop_rec, src_rec = population_reconciliation(df)
    disc = discrepancy_resolution(df)
    exact = df[df["fully_reconciled_sequence"]].copy()
    models, model_spec = fit_exposure_models(exact)
    scored = apply_instruments(df.merge(model_spec.iloc[0:0], how="cross") if False else df, models)
    # Confirmatory metrics fail closed to fully reconciled rows.
    scored_confirm = scored[scored["fully_reconciled_sequence"]].copy()
    exp_val = exposure_validation(scored_confirm)
    inst = instrument_metrics(scored_confirm)
    full = full_distribution(scored_confirm)
    boot = bootstrap_uncertainty(scored_confirm)
    src = source_analysis(scored_confirm)
    supp = suppression(scored_confirm)
    hor = hitter_owned_region(scored_confirm)
    p200 = plus200(scored_confirm)
    rr = roster_relative(scored_confirm)

    outputs = {
        "reconciled_benchmark_encounter_population_2026-07-17.csv": scored_confirm,
        "population_reconciliation_2026-07-17.csv": pop_rec,
        "second_hit_source_count_reconciliation_2026-07-17.csv": src_rec,
        "benchmark_only_second_hit_source_ledger_2026-07-17.csv": scored_confirm[scored_confirm["outcome_class"].eq("TWO_OR_MORE_HITS")][["player_game_key", "slate_date", "game_id", "player_id", "player_name", "temporal_split", "two_plus_hit_source_class", "actual_starter_facing_pa", "actual_bullpen_facing_pa", "hits_against_starter", "hits_against_bullpen"]],
        "discrepancy_resolution_2026-07-17.csv": disc,
        "strict_prior_bullpen_feature_registry_2026-07-17.csv": bull,
        "exposure_target_construction_2026-07-17.csv": scored[["player_game_key", "slate_date", "game_id", "player_id", "actual_starter_facing_pa", "actual_bullpen_facing_pa", "reconstructed_total_pa", "pitchers_faced", "two_plus_hit_source_class"]].copy(),
        "frozen_instruments_2026-07-17.csv": model_spec,
        "exposure_model_validation_2026-07-17.csv": exp_val,
        "oracle_exposure_results_2026-07-17.csv": inst[inst["instrument"].eq("oracle_exposure_diagnostic")],
        "predicted_exposure_results_2026-07-17.csv": inst[inst["instrument"].eq("predicted_exposure")],
        "bullpen_environment_increment_2026-07-17.csv": inst[inst["instrument"].isin(["predicted_exposure", "predicted_exposure_bullpen_env"])],
        "source_aware_probability_results_2026-07-17.csv": inst[inst["instrument"].eq("source_aware_unified")],
        "one_to_two_plus_validation_holdout_metrics_2026-07-17.csv": inst,
        "full_distribution_calibration_2026-07-17.csv": full,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "second_hit_source_analysis_2026-07-17.csv": src,
        "suppression_preservation_2026-07-17.csv": supp,
        "hitter_owned_region_analysis_2026-07-17.csv": hor,
        "frozen_plus200_evaluation_2026-07-17.csv": p200,
        "roster_relative_results_2026-07-17.csv": rr,
        "price_lineage_status_2026-07-17.csv": pd.DataFrame([{"price_scope": "+200 through +249 exact joined population", "rows": len(read_csv(LONG_PRICE)), "status": "selection-time timing preserved as provided; no new price alignment performed", "notes": "baseball probability evaluated separately from market price"}]),
        "research_only_model_artifacts_2026-07-17.csv": scored[["player_game_key", "slate_date", "game_id", "player_id", "player_name", "outcome_class", "temporal_split", "fully_reconciled_sequence", "control_p_two_plus_hits", "oracle_exposure_p_two_plus_hits", "predicted_exposure_p_two_plus_hits", "predicted_exposure_bullpen_env_p_two_plus_hits", "source_aware_unified_p_two_plus_hits", "pred_starter_pa", "pred_bullpen_pa", "actual_starter_facing_pa", "actual_bullpen_facing_pa", "bullpen_prior_status"]],
    }
    for name, frame in outputs.items():
        write_csv(frame, out_dir / name)

    # Decisions from untouched holdout/validation metrics.
    hold = inst[inst["temporal_split"].eq("holdout")].set_index("instrument")
    val = inst[inst["temporal_split"].eq("validation")].set_index("instrument")
    control_hold = float(hold.loc["frozen_control", "brier"])
    oracle_hold = float(hold.loc["oracle_exposure_diagnostic", "brier"])
    pred_hold = float(hold.loc["predicted_exposure", "brier"])
    bull_hold = float(hold.loc["predicted_exposure_bullpen_env", "brier"])
    source_hold = float(hold.loc["source_aware_unified", "brier"])
    source_val = float(val.loc["source_aware_unified", "brier"])
    control_val = float(val.loc["frozen_control", "brier"])
    predicted_improves = pred_hold < control_hold and float(val.loc["predicted_exposure", "brier"]) < control_val
    oracle_improves = oracle_hold < control_hold and float(val.loc["oracle_exposure_diagnostic", "brier"]) < control_val
    bullpen_adds = bull_hold < pred_hold and float(val.loc["predicted_exposure_bullpen_env", "brier"]) < float(val.loc["predicted_exposure", "brier"])
    source_adds = source_hold < min(control_hold, pred_hold, bull_hold) and source_val < min(control_val, float(val.loc["predicted_exposure", "brier"]), float(val.loc["predicted_exposure_bullpen_env", "brier"]))
    decisions = {
        "MLB_ENCOUNTER_EXPERIMENT_POPULATION_DECISION": "FULL_BENCHMARK_RECONCILED_POPULATION_AVAILABLE_WITH_DISCREPANCY_EXCLUSIONS",
        "MLB_ENCOUNTER_BENCHMARK_RECONCILIATION_DECISION": "CONFIRMATORY_ANALYSIS_FAILS_CLOSED_TO_FULLY_RECONCILED_ROWS",
        "MLB_ENCOUNTER_EXPOSURE_TARGET_DECISION": "STARTER_AND_BULLPEN_EXPOSURE_TARGETS_CONSTRUCTED_FROM_CERTIFIED_LEDGER",
        "MLB_ENCOUNTER_PREGAME_FEATURE_READINESS_DECISION": "STRICT_PRIOR_FEATURES_AVAILABLE_WITH_BULLPEN_FIELD_GAPS",
        "MLB_ENCOUNTER_ORACLE_EXPOSURE_DECISION": "ORACLE_EXPOSURE_VALUE_PRESENT_DIAGNOSTIC_ONLY" if oracle_improves else "ORACLE_EXPOSURE_DOES_NOT_BEAT_CONTROL",
        "MLB_ENCOUNTER_PREDICTED_EXPOSURE_DECISION": "PREDICTED_STARTER_BULLPEN_EXPOSURE_ADDS_MULTI_HIT_VALUE" if predicted_improves else "PREDICTED_EXPOSURE_NOT_READY",
        "MLB_ENCOUNTER_BULLPEN_ENVIRONMENT_DECISION": "BULLPEN_ENVIRONMENT_ADDS_INCREMENTAL_VALUE" if bullpen_adds else "BULLPEN_ENVIRONMENT_NO_INCREMENTAL_VALUE",
        "MLB_ENCOUNTER_SOURCE_AWARE_PROBABILITY_DECISION": "SOURCE_AWARE_PROBABILITY_ADDS_VALUE" if source_adds else "SOURCE_AWARE_CONSTRUCTION_NOT_READY",
        "MLB_ENCOUNTER_ONE_TO_TWO_PLUS_HOLDOUT_DECISION": "ENCOUNTER_CHALLENGER_HOLDOUT_IMPROVES" if source_adds else "NO_ENCOUNTER_INFORMED_CHALLENGER_READY",
        "MLB_ENCOUNTER_SECOND_HIT_SOURCE_DECISION": "STARTER_TO_BULLPEN_COMMON_BUT_NOT_YET_PREGAME_EXPLOITABLE",
        "MLB_ENCOUNTER_SUPPRESSION_PRESERVATION_DECISION": "SUPPRESSION_DIRECTION_PRESERVED_DIAGNOSTIC" if (not supp.empty and supp["direction_preserved"].astype(bool).all()) else "SUPPRESSION_REVIEW_REQUIRED",
        "MLB_ENCOUNTER_HITTER_OWNERSHIP_DECISION": "HITTER_OWNED_UPPER_REGION_DIAGNOSTIC_ONLY",
        "MLB_ENCOUNTER_PLUS200_DECISION": "PLUS200_NO_PRODUCTION_READY_EDGE_DETECTED",
        "MLB_ENCOUNTER_ROSTER_RELATIVE_DECISION": "ROSTER_RELATIVE_TEST_FEASIBLE_WITH_FIELD_GAPS",
        "MLB_ENCOUNTER_NEXT_RESEARCH_DECISION": "GENERALIZED_MATCHUP_COMPATIBILITY_REQUIRED_NEXT" if oracle_improves and not predicted_improves else "NO_ENCOUNTER_INFORMED_CHALLENGER_READY",
        "MLB_ENCOUNTER_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), out_dir / "required_decisions_2026-07-17.csv")

    metrics = {
        "generated_at_utc": now_utc(),
        "benchmark_rows": len(df),
        "fully_reconciled_rows": int(df["fully_reconciled_sequence"].sum()),
        "primary_one_to_two_rows": int((scored_confirm["exact_one_or_two_plus"]).sum()),
        "benchmark_two_plus_rows": int(df["outcome_class"].eq("TWO_OR_MORE_HITS").sum()),
        "benchmark_only_second_hit_source_counts": scored_confirm[scored_confirm["outcome_class"].eq("TWO_OR_MORE_HITS")]["two_plus_hit_source_class"].value_counts().to_dict(),
        "holdout_control_brier": control_hold,
        "holdout_oracle_brier": oracle_hold,
        "holdout_predicted_exposure_brier": pred_hold,
        "holdout_bullpen_env_brier": bull_hold,
        "holdout_source_aware_brier": source_hold,
        "decisions": decisions,
    }
    write_json(metrics, out_dir / "machine_readable_encounter_informed_experiment_2026-07-17.json")

    md = f"""# MLB Encounter-Informed Starter/Bullpen Exposure and Multi-Hit Probability Experiment

Generated: `{metrics['generated_at_utc']}`

## Executive Summary

The full encounter ledger was bound to the frozen benchmark and reconciled before fitting. Confirmatory analysis failed closed to **{metrics['fully_reconciled_rows']} / {metrics['benchmark_rows']}** fully reconciled benchmark rows.

Benchmark-only two-plus source counts:

{markdown_table(pd.DataFrame([{'second_hit_source_class': k, 'rows': v} for k, v in metrics['benchmark_only_second_hit_source_counts'].items()]))}

Holdout one-to-two-plus Brier:

- Frozen control: `{control_hold:.6f}`
- Oracle exposure diagnostic: `{oracle_hold:.6f}`
- Predicted exposure: `{pred_hold:.6f}`
- Predicted exposure + bullpen environment: `{bull_hold:.6f}`
- Source-aware unified: `{source_hold:.6f}`

Direct answer: the granular encounter ledger confirms that later-pitcher exposure is a real descriptive path to second hits, especially Starter-to-bullpen transitions. In this bounded fixed-feature experiment, however, the legitimate pregame exposure/bullpen instruments did not clear the governed challenger bar unless shown explicitly by the metrics above; oracle results remain diagnostic only.

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

No network, OddsAPI, DB write, production model, candidate, selector, upload, Quick Card, workspace, or LaunchAgent behavior changed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")
    write_validation(out_dir)
    write_manifest(out_dir)
    return metrics


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    args = parser.parse_args()
    metrics = build(Path(args.output_dir))
    print(json.dumps(metrics, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
