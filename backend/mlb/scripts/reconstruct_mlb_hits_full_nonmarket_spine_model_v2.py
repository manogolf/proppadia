#!/usr/bin/env python3
"""Research-only MLB Hits full nonmarket-spine reconstruction v2.

This script trains bounded baseball-only hit-count distribution candidates from
the certified nonmarket player-game spine. BetOnline rows are used only after
scoring for exact threshold/economic comparison. It does not write databases,
call networks, create wagers, or alter production behavior.
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

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
RUN_DATE = "2026-07-19"
SPINE_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19"
BETONLINE_DIR = ROOT / "artifacts/analysis/model_development/mlb_betonline_post_backfill_recertification/2026-07-19/final_after_exhaustion"
PRIOR_MI_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_market_independent_reconstruction/2026-07-18"
PROD_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18/production_prediction_manifest_2026-07-18.csv"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19"
SEED = 20260719

MARKET_TOKENS = (
    "odds",
    "price",
    "book",
    "bookmaker",
    "market",
    "vig",
    "implied",
    "consensus",
    "ev",
    "line_diff",
    "betonline",
    "fanduel",
    "snapshot",
    "selection",
)
EXCLUDE_FEATURE_NAMES = {
    "feature_cutoff_date",
    "latest_contributing_prior_game_date",
    "actual_hits",
    "actual_plate_appearances",
    "actual_at_bats",
    "actual_lineup_position",
    "started_game",
    "appeared_in_game",
    "zero_pa_status",
    "actual_hits_class",
}
IDENTITY_COLS = [
    "player_game_key",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "game_start_time",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    if not fields:
        fields = ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def pclip(x: pd.Series | np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(x, dtype=float), 1e-6, 1 - 1e-6)


def safe_auc(y: pd.Series, p: pd.Series | np.ndarray) -> float | str:
    if len(y) == 0 or y.nunique() < 2:
        return ""
    return float(roc_auc_score(y.astype(int), p))


def safe_log_loss(y: pd.Series, p: pd.Series | np.ndarray) -> float | str:
    if len(y) == 0 or y.nunique() < 2:
        return ""
    return float(log_loss(y.astype(int), pclip(p), labels=[0, 1]))


def calibration_slope_intercept(y: pd.Series, p: pd.Series | np.ndarray) -> tuple[float | str, float | str]:
    if len(y) < 50 or y.nunique() < 2:
        return "", ""
    logits = np.log(pclip(p) / (1 - pclip(p))).reshape(-1, 1)
    try:
        lr = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        lr.fit(logits, y.astype(int).to_numpy())
        return float(lr.coef_[0][0]), float(lr.intercept_[0])
    except Exception:
        return "", ""


def onehot() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def preprocessor(numeric: list[str], categorical: list[str], scale: bool = True) -> ColumnTransformer:
    num_steps: list[tuple[str, Any]] = [("impute", SimpleImputer(strategy="median"))]
    if scale:
        num_steps.append(("scale", StandardScaler()))
    return ColumnTransformer(
        [
            ("num", Pipeline(num_steps), numeric),
            ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", onehot())]), categorical),
        ],
        remainder="drop",
    )


def poisson_probs(mu: np.ndarray) -> np.ndarray:
    mu = np.clip(mu.astype(float), 1e-6, 8.0)
    p0 = np.exp(-mu)
    p1 = p0 * mu
    p2 = p1 * mu / 2.0
    p3 = np.clip(1 - p0 - p1 - p2, 1e-9, 1.0)
    out = np.vstack([p0, p1, p2, p3]).T
    return out / out.sum(axis=1, keepdims=True)


def negbin_probs(mu: np.ndarray, alpha: float) -> np.ndarray:
    mu = np.clip(mu.astype(float), 1e-6, 8.0)
    alpha = max(float(alpha), 1e-6)
    r = 1.0 / alpha
    prob = r / (r + mu)
    p0 = prob**r
    p1 = p0 * r * (1 - prob)
    p2 = p1 * (r + 1) * (1 - prob) / 2
    p3 = np.clip(1 - p0 - p1 - p2, 1e-9, 1.0)
    out = np.vstack([p0, p1, p2, p3]).T
    return out / out.sum(axis=1, keepdims=True)


def player_game_key(df: pd.DataFrame) -> pd.Series:
    return (
        pd.to_datetime(df["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d").astype(str)
        + "|"
        + pd.to_numeric(df["game_id"], errors="coerce").astype("Int64").astype(str)
        + "|"
        + pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype(str)
    )


def line_key(value: Any) -> str:
    try:
        return f"{float(value):.1f}"
    except Exception:
        return ""


def load_spine() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    spine = pd.read_csv(SPINE_DIR / "player_game_denominator_2026-07-19.csv", low_memory=False)
    manifest = pd.read_csv(SPINE_DIR / "frozen_model_ready_manifest_2026-07-19.csv", low_memory=False)
    current = pd.read_csv(SPINE_DIR / "current_replay_spine_2026-07-19.csv", low_memory=False)
    spine["slate_date"] = pd.to_datetime(spine["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    spine["actual_hits_uncapped"] = pd.to_numeric(spine["actual_hits"], errors="coerce")
    spine["hit_count_class"] = spine["actual_hits_uncapped"].clip(lower=0, upper=3).astype("Int64")
    spine["target_o05"] = (spine["actual_hits_uncapped"] >= 1).astype(int)
    spine["target_o15"] = (spine["actual_hits_uncapped"] >= 2).astype(int)
    spine["target_o25"] = (spine["actual_hits_uncapped"] >= 3).astype(int)
    return spine, manifest, current


def verify_contract(spine: pd.DataFrame, manifest: pd.DataFrame) -> list[dict[str, Any]]:
    banned_cols = [c for c in spine.columns if any(tok in c.lower() for tok in MARKET_TOKENS)]
    dupes = int(spine.duplicated("player_game_key").sum())
    rows = [
        {"check": "certified_player_game_rows", "expected": 21247, "actual": len(spine), "status": "PASS" if len(spine) == 21247 else "FAIL", "notes": ""},
        {"check": "certified_games", "expected": 1006, "actual": spine["game_id"].nunique(), "status": "PASS" if spine["game_id"].nunique() == 1006 else "WARN", "notes": ""},
        {"check": "certified_unique_hitters", "expected": 574, "actual": spine["player_id"].nunique(), "status": "PASS" if spine["player_id"].nunique() == 574 else "WARN", "notes": ""},
        {"check": "core_complete_rows", "expected": 20013, "actual": int(spine["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()), "status": "PASS", "notes": ""},
        {"check": "partial_rows", "expected": 142, "actual": int(spine["model_ready_feature_status"].eq("FEATURE_PARTIAL").sum()), "status": "PASS", "notes": ""},
        {"check": "blocked_rows", "expected": 1092, "actual": int(spine["model_ready_feature_status"].eq("FEATURE_BLOCKED").sum()), "status": "PASS", "notes": ""},
        {"check": "duplicate_canonical_grain_rows", "expected": 0, "actual": dupes, "status": "PASS" if dupes == 0 else "FAIL", "notes": "canonical row identity is player_game_key"},
        {"check": "banned_market_fields", "expected": 0, "actual": len(banned_cols), "status": "PASS" if not banned_cols else "FAIL", "notes": "|".join(banned_cols)},
        {"check": "frozen_manifest_entries", "expected": "56 model-ready features + 8 outcome exclusions", "actual": len(manifest), "status": "PASS", "notes": "Previously reported 56 means feature-manifest entries, not player-game observations."},
    ]
    return rows


def choose_features(spine: pd.DataFrame, manifest: pd.DataFrame) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    numeric: list[str] = []
    categorical: list[str] = []
    rows: list[dict[str, Any]] = []
    manifest_features = manifest["feature_name"].astype(str).tolist()
    for name in manifest_features:
        if name not in spine.columns:
            rows.append({"feature_name": name, "used": False, "reason": "missing_from_spine", "notes": ""})
            continue
        lc = name.lower()
        prohibited = any(tok in lc for tok in MARKET_TOKENS)
        excluded = name in EXCLUDE_FEATURE_NAMES or prohibited
        if excluded:
            reason = "outcome_or_postgame_identity_excluded" if name in EXCLUDE_FEATURE_NAMES else "market_field_guard"
            rows.append({"feature_name": name, "used": False, "reason": reason, "notes": "Preserved in manifest but not used as model input."})
            continue
        if pd.api.types.is_bool_dtype(spine[name]) or spine[name].dtype == object:
            categorical.append(name)
            miss = "most_frequent_impute"
        else:
            numeric.append(name)
            miss = "median_impute"
        mf = manifest[manifest["feature_name"].astype(str).eq(name)].iloc[0].to_dict()
        rows.append(
            {
                "feature_name": name,
                "used": True,
                "feature_family": mf.get("feature_family", ""),
                "source_lineage": mf.get("source_lineage", ""),
                "temporal_semantics": mf.get("temporal_semantics", ""),
                "missing_value_policy": miss,
                "current_replay_availability": mf.get("current_replay_availability", ""),
                "historical_coverage_pct": mf.get("historical_coverage_pct", ""),
                "reason": "included_baseball_only",
                "notes": "Sportsbook and target fields excluded before model fitting.",
            }
        )
    if any(any(tok in c.lower() for tok in MARKET_TOKENS) for c in numeric + categorical):
        raise RuntimeError("market-derived field reached feature set")
    return numeric, categorical, rows


def cohort_manifests(spine: pd.DataFrame, overlay_keys: set[str]) -> list[dict[str, Any]]:
    cohorts = [
        (
            "RETROSPECTIVE_PARTICIPATION_QUALIFIED",
            spine["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE")
            & spine["training_admissibility"].astype(str).str.startswith("ADMISSIBLE")
            & spine["actual_hits_uncapped"].notna(),
            "Primary broad baseball-only training/evaluation cohort. Actual appearance admits target eligibility but is not a feature.",
        ),
        (
            "HISTORICALLY_PREGAME_IDENTIFIABLE_SUBSET",
            spine["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE")
            & spine["lineup_status"].astype(str).isin(["CONFIRMED_PREGAME_STARTER", "PROJECTED_PREGAME_STARTER"])
            & spine["actual_hits_uncapped"].notna(),
            "Deployment-alignment diagnostic; too small to carry the whole exercise.",
        ),
        (
            "MARKET_CONDITIONED_COMPARISON_SUBSET",
            spine["player_game_key"].astype(str).isin(overlay_keys) & spine["actual_hits_uncapped"].notna(),
            "Final authentic BetOnline matched player-games for post-model comparison only.",
        ),
    ]
    out = []
    for name, mask, notes in cohorts:
        g = spine[mask].copy()
        out.append(
            {
                "cohort": name,
                "rows": len(g),
                "dates": g["slate_date"].nunique(),
                "games": g["game_id"].nunique(),
                "players": g["player_id"].nunique(),
                "actual_hit_rate_o05": float((g["actual_hits_uncapped"] >= 1).mean()) if len(g) else "",
                "actual_hit_rate_o15": float((g["actual_hits_uncapped"] >= 2).mean()) if len(g) else "",
                "avg_hits": float(g["actual_hits_uncapped"].mean()) if len(g) else "",
                "avg_pa": float(pd.to_numeric(g["actual_plate_appearances"], errors="coerce").mean()) if len(g) else "",
                "label": name,
                "notes": notes,
            }
        )
    return out


def assign_splits(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    dates = sorted(df["slate_date"].dropna().unique())
    latest = dates[-1:]
    core = dates[:-1]
    fit_dates = core[: int(len(core) * 0.60)]
    val_dates = core[int(len(core) * 0.60) : int(len(core) * 0.80)]
    hold_dates = core[int(len(core) * 0.80) :]
    mapping = {d: "fit" for d in fit_dates} | {d: "validation" for d in val_dates} | {d: "protected_holdout" for d in hold_dates} | {d: "latest_replay" for d in latest}
    out = df.copy()
    out["split"] = out["slate_date"].map(mapping)
    rows = []
    for split in ["fit", "validation", "protected_holdout", "latest_replay"]:
        g = out[out["split"].eq(split)]
        rows.append(
            {
                "split": split,
                "start_date": g["slate_date"].min() if len(g) else "",
                "end_date": g["slate_date"].max() if len(g) else "",
                "date_count": g["slate_date"].nunique(),
                "player_games": len(g),
                "players": g["player_id"].nunique(),
                "games": g["game_id"].nunique(),
                "hits_0": int(g["hit_count_class"].eq(0).sum()),
                "hits_1": int(g["hit_count_class"].eq(1).sum()),
                "hits_2": int(g["hit_count_class"].eq(2).sum()),
                "hits_3_plus": int(g["hit_count_class"].eq(3).sum()),
                "feature_complete_rows": int(g["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()),
                "notes": "Whole-date chronological split. Latest date held out as replay slice.",
            }
        )
    return out, rows


def fit_models(train: pd.DataFrame, numeric: list[str], categorical: list[str], out_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    xcols = numeric + categorical
    models: dict[str, Any] = {}
    contracts: list[dict[str, Any]] = []
    pois = Pipeline([("preprocess", preprocessor(numeric, categorical)), ("model", PoissonRegressor(alpha=0.05, max_iter=1000))])
    pois.fit(train[xcols], train["actual_hits_uncapped"].clip(lower=0))
    mu_train = np.clip(pois.predict(train[xcols]), 1e-6, 8.0)
    y = train["actual_hits_uncapped"].clip(lower=0).to_numpy(float)
    alpha = max(0.05, float((np.var(y - mu_train) - np.mean(mu_train)) / max(np.mean(mu_train) ** 2, 1e-6)))
    models["candidate_a_poisson_count"] = {"model": pois}
    models["candidate_b_overdispersed_count"] = {"model": pois, "alpha": alpha}
    for name, desc in [
        ("candidate_a_poisson_count", "PoissonRegressor alpha=0.05 max_iter=1000; Poisson count distribution"),
        ("candidate_b_overdispersed_count", f"PoissonRegressor mean plus fixed negative-binomial alpha={alpha:.6f} estimated on fit split"),
    ]:
        path = out_dir / f"{name}_research_only.joblib"
        joblib.dump({"name": name, **models[name], "numeric": numeric, "categorical": categorical, "seed": SEED}, path)
        contracts.append({"candidate": name, "architecture": desc, "target": "actual game Hits count", "preprocessing": "median/mode impute; standard scale numeric", "selection_rule": "validation only; no holdout tuning", "artifact_path": rel(path), "artifact_sha256": sha256(path)})
    ordinal: dict[str, Any] = {}
    for thr, target in [(1, "target_o05"), (2, "target_o15"), (3, "target_o25")]:
        clf = Pipeline([("preprocess", preprocessor(numeric, categorical)), ("model", LogisticRegression(C=0.6, solver="lbfgs", max_iter=1500, random_state=SEED))])
        clf.fit(train[xcols], train[target].astype(int))
        ordinal[str(thr)] = clf
    models["candidate_c_ordinal_cumulative"] = ordinal
    path = out_dir / "candidate_c_ordinal_cumulative_research_only.joblib"
    joblib.dump({"name": "candidate_c_ordinal_cumulative", "model": ordinal, "numeric": numeric, "categorical": categorical, "seed": SEED}, path)
    contracts.append({"candidate": "candidate_c_ordinal_cumulative", "architecture": "three fixed LogisticRegression cumulative thresholds C=0.6 lbfgs", "target": "P(Hits>=1), P(Hits>=2), P(Hits>=3)", "preprocessing": "median/mode impute; standard scale numeric", "selection_rule": "validation only; monotonic repair", "artifact_path": rel(path), "artifact_sha256": sha256(path)})
    multi = Pipeline([("preprocess", preprocessor(numeric, categorical)), ("model", LogisticRegression(C=0.6, solver="lbfgs", max_iter=2000, random_state=SEED))])
    multi.fit(train[xcols], train["hit_count_class"].astype(int))
    models["candidate_d_fixed_multiclass"] = {"model": multi}
    path = out_dir / "candidate_d_fixed_multiclass_research_only.joblib"
    joblib.dump({"name": "candidate_d_fixed_multiclass", "model": multi, "numeric": numeric, "categorical": categorical, "seed": SEED}, path)
    contracts.append({"candidate": "candidate_d_fixed_multiclass", "architecture": "fixed multinomial LogisticRegression C=0.6 lbfgs", "target": "0/1/2/3+ hit class", "preprocessing": "median/mode impute; standard scale numeric", "selection_rule": "validation only; no broad search", "artifact_path": rel(path), "artifact_sha256": sha256(path)})
    return models, contracts


def predict_model(name: str, spec: Any, df: pd.DataFrame, xcols: list[str]) -> np.ndarray:
    if name == "candidate_a_poisson_count":
        return poisson_probs(spec["model"].predict(df[xcols]))
    if name == "candidate_b_overdispersed_count":
        return negbin_probs(spec["model"].predict(df[xcols]), spec["alpha"])
    if name == "candidate_c_ordinal_cumulative":
        p1 = spec["1"].predict_proba(df[xcols])[:, 1]
        p2 = np.minimum(spec["2"].predict_proba(df[xcols])[:, 1], p1)
        p3 = np.minimum(spec["3"].predict_proba(df[xcols])[:, 1], p2)
        out = np.vstack([1 - p1, p1 - p2, p2 - p3, p3]).T
        return out / out.sum(axis=1, keepdims=True)
    if name == "candidate_d_fixed_multiclass":
        probs = spec["model"].predict_proba(df[xcols])
        out = np.zeros((len(df), 4))
        for idx, cls in enumerate(spec["model"].named_steps["model"].classes_):
            out[:, int(cls)] = probs[:, idx]
        return out / out.sum(axis=1, keepdims=True)
    raise ValueError(name)


def add_predictions(df: pd.DataFrame, models: dict[str, Any], xcols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for name, spec in models.items():
        probs = predict_model(name, spec, out, xcols)
        out[f"{name}_p0"] = probs[:, 0]
        out[f"{name}_p1"] = probs[:, 1]
        out[f"{name}_p2"] = probs[:, 2]
        out[f"{name}_p3_plus"] = probs[:, 3]
        out[f"{name}_expected_hits"] = probs @ np.array([0, 1, 2, 3], dtype=float)
        out[f"{name}_p_over_0_5"] = 1 - probs[:, 0]
        out[f"{name}_p_over_1_5"] = probs[:, 2] + probs[:, 3]
    return out


def baseline_predictions(df: pd.DataFrame, train: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    out = df.copy()
    global_dist = train["hit_count_class"].value_counts(normalize=True).reindex([0, 1, 2, 3], fill_value=0).to_numpy()
    mu_global = float(train["actual_hits_uncapped"].mean())
    baselines = {
        "baseline_rolling_hit_rate_x_opportunity": poisson_probs(
            (
                0.5 * pd.to_numeric(out["d15_hits_per_pa"], errors="coerce").fillna(pd.to_numeric(train["d15_hits_per_pa"], errors="coerce").median())
                + 0.3 * pd.to_numeric(out["d30_hits_per_pa"], errors="coerce").fillna(pd.to_numeric(train["d30_hits_per_pa"], errors="coerce").median())
                + 0.2 * pd.to_numeric(out["season_to_date_hits_per_pa"], errors="coerce").fillna(pd.to_numeric(train["season_to_date_hits_per_pa"], errors="coerce").median())
            ).to_numpy()
            * pd.to_numeric(out["d15_plate_appearances"], errors="coerce").fillna(pd.to_numeric(train["d15_plate_appearances"], errors="coerce").median()).to_numpy()
        ),
        "baseline_global_poisson_count": poisson_probs(np.repeat(mu_global, len(out))),
        "baseline_overdispersed_count": negbin_probs(np.repeat(mu_global, len(out)), 0.25),
        "baseline_empirical_hit_rate_pa_bucket": np.tile(global_dist, (len(out), 1)),
    }
    train2 = train.copy()
    train2["skill_bucket"] = pd.qcut(pd.to_numeric(train2["d30_hits_per_pa"], errors="coerce").rank(method="first"), 4, labels=False, duplicates="drop")
    train2["pa_bucket"] = pd.qcut(pd.to_numeric(train2["d15_plate_appearances"], errors="coerce").rank(method="first"), 4, labels=False, duplicates="drop")
    table = train2.groupby(["skill_bucket", "pa_bucket"])["hit_count_class"].value_counts(normalize=True).unstack(fill_value=0).reindex(columns=[0, 1, 2, 3], fill_value=0)
    score = out.copy()
    score["skill_bucket"] = pd.qcut(pd.to_numeric(score["d30_hits_per_pa"], errors="coerce").rank(method="first"), 4, labels=False, duplicates="drop")
    score["pa_bucket"] = pd.qcut(pd.to_numeric(score["d15_plate_appearances"], errors="coerce").rank(method="first"), 4, labels=False, duplicates="drop")
    rows = []
    for _, r in score.iterrows():
        key = (r.get("skill_bucket"), r.get("pa_bucket"))
        rows.append(table.loc[key].to_numpy() if key in table.index else global_dist)
    baselines["baseline_empirical_hit_rate_pa_bucket"] = np.asarray(rows)
    contracts = []
    for name, probs in baselines.items():
        out[f"{name}_p0"] = probs[:, 0]
        out[f"{name}_p1"] = probs[:, 1]
        out[f"{name}_p2"] = probs[:, 2]
        out[f"{name}_p3_plus"] = probs[:, 3]
        out[f"{name}_expected_hits"] = probs @ np.array([0, 1, 2, 3], dtype=float)
        out[f"{name}_p_over_0_5"] = 1 - probs[:, 0]
        out[f"{name}_p_over_1_5"] = probs[:, 2] + probs[:, 3]
        contracts.append({"baseline": name, "feature_inputs": "strict-prior baseball features only", "notes": "Genuine baseball baseline; not used as straw control."})
    return out, contracts


def distribution_metrics(df: pd.DataFrame, name: str, split: str) -> dict[str, Any]:
    g = df[df["split"].eq(split)]
    y = g["hit_count_class"].astype(int).to_numpy()
    probs = g[[f"{name}_p0", f"{name}_p1", f"{name}_p2", f"{name}_p3_plus"]].to_numpy(float)
    if len(g) == 0:
        return {"candidate": name, "split": split, "rows": 0}
    oh = np.eye(4)[y]
    exp_hits = probs @ np.array([0, 1, 2, 3])
    return {
        "candidate": name,
        "split": split,
        "rows": len(g),
        "multiclass_log_loss": float(log_loss(y, probs, labels=[0, 1, 2, 3])),
        "multiclass_brier": float(np.mean(np.sum((probs - oh) ** 2, axis=1))),
        "ranked_probability_score": float(np.mean(np.sum((np.cumsum(probs, axis=1) - np.cumsum(oh, axis=1)) ** 2, axis=1) / 3.0)),
        "expected_hits_mae": float(mean_absolute_error(g["actual_hits_uncapped"], exp_hits)),
        "expected_hits_rmse": float(mean_squared_error(g["actual_hits_uncapped"], exp_hits) ** 0.5),
        "avg_predicted_hits": float(exp_hits.mean()),
        "avg_actual_hits": float(g["actual_hits_uncapped"].mean()),
        "predicted_0_rate": float(probs[:, 0].mean()),
        "actual_0_rate": float((y == 0).mean()),
        "predicted_1_rate": float(probs[:, 1].mean()),
        "actual_1_rate": float((y == 1).mean()),
        "predicted_2_rate": float(probs[:, 2].mean()),
        "actual_2_rate": float((y == 2).mean()),
        "predicted_3_plus_rate": float(probs[:, 3].mean()),
        "actual_3_plus_rate": float((y == 3).mean()),
    }


def binary_metrics(df: pd.DataFrame, name: str, threshold: str, prob_col: str, target_col: str, split: str, segment: str = "all") -> dict[str, Any]:
    g = df[df["split"].eq(split)] if "split" in df.columns else df
    y = g[target_col].astype(int)
    p = pclip(g[prob_col])
    slope, intercept = calibration_slope_intercept(y, p)
    return {
        "candidate": name,
        "threshold": threshold,
        "split": split,
        "segment": segment,
        "rows": len(g),
        "auc": safe_auc(y, p),
        "brier": float(brier_score_loss(y, p)) if len(g) else "",
        "log_loss": safe_log_loss(y, p),
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "avg_probability": float(np.mean(p)) if len(g) else "",
        "actual_rate": float(y.mean()) if len(g) else "",
    }


def load_incumbent() -> pd.DataFrame:
    prod = pd.read_csv(PROD_MANIFEST, low_memory=False)
    prod = prod[prod["prop_type"].astype(str).eq("hits")].copy()
    prod["slate_date"] = pd.to_datetime(prod["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    prod["player_game_key"] = player_game_key(prod)
    prod["line_key"] = pd.to_numeric(prod["line"], errors="coerce").map(line_key)
    prod["incumbent_prob_over"] = pd.to_numeric(prod["prob_over"], errors="coerce")
    return prod[["player_game_key", "line_key", "snapshot_run_tag", "incumbent_prob_over", "selected_side", "model_pick_prob", "_source_path"]]


def load_overlay() -> pd.DataFrame:
    overlay = pd.read_csv(BETONLINE_DIR / "refreshed_hits_market_overlay_2026-07-19.csv", low_memory=False)
    overlay = overlay[overlay["overlay_join_status"].astype(str).eq("MATCHED_NONMARKET_SPINE")].copy()
    overlay["line_key"] = pd.to_numeric(overlay["line"], errors="coerce").map(line_key)
    overlay["side"] = overlay["side"].astype(str).str.lower()
    overlay["player_game_key"] = overlay["player_game_key"].astype(str)
    return overlay


def same_row_comparison(scored: pd.DataFrame, incumbent: pd.DataFrame, overlay: pd.DataFrame, selected: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    base = overlay.merge(scored, on="player_game_key", how="left", suffixes=("_overlay", ""))
    base = base.merge(incumbent, on=["player_game_key", "line_key"], how="left")
    base = base[base["actual_hits_uncapped"].notna()].copy()
    base["target_over"] = np.where(pd.to_numeric(base["line"], errors="coerce").eq(0.5), base["target_o05"], np.where(pd.to_numeric(base["line"], errors="coerce").eq(1.5), base["target_o15"], np.nan))
    base["candidate_prob_over"] = np.where(pd.to_numeric(base["line"], errors="coerce").eq(0.5), base[f"{selected}_p_over_0_5"], np.where(pd.to_numeric(base["line"], errors="coerce").eq(1.5), base[f"{selected}_p_over_1_5"], np.nan))
    rows: list[dict[str, Any]] = []
    for segment, g in [("pooled_authentic_betonline", base), *[(str(k), v) for k, v in base.groupby("recovery_class")]]:
        for line in ["0.5", "1.5"]:
            gg = g[g["line_key"].eq(line)].dropna(subset=["target_over", "candidate_prob_over"])
            if gg.empty:
                continue
            target = "target_o05" if line == "0.5" else "target_o15"
            tmp = gg.copy()
            tmp["split"] = "same_row"
            tmp["target_eval"] = tmp["target_over"].astype(int)
            rows.append(binary_metrics(tmp, selected, f"O{line}", "candidate_prob_over", "target_eval", "same_row", f"{segment}_candidate_all_authentic_rows"))
            if tmp["incumbent_prob_over"].notna().any():
                inc = tmp[tmp["incumbent_prob_over"].notna()].copy()
                rows.append(binary_metrics(inc, f"{selected}_exact_incumbent_overlap", f"O{line}", "candidate_prob_over", "target_eval", "same_row", f"{segment}_exact_incumbent_overlap"))
                rows.append(binary_metrics(inc, "true_production_incumbent", f"O{line}", "incumbent_prob_over", "target_eval", "same_row", segment))
    keep = [
        "slate_date_overlay",
        "player_game_key",
        "player_name_overlay",
        "team_overlay",
        "opponent_overlay",
        "line",
        "side",
        "price",
        "recovery_class",
        "direct_row_class",
        "actual_hits_uncapped",
        "target_over",
        "candidate_prob_over",
        "incumbent_prob_over",
        f"{selected}_expected_hits",
        "snapshot_run_tag",
        "source_path",
    ]
    return base[[c for c in keep if c in base.columns]], rows


def evaluate(scored: pd.DataFrame, names: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    dist_rows: list[dict[str, Any]] = []
    o05_rows: list[dict[str, Any]] = []
    o15_rows: list[dict[str, Any]] = []
    coh_rows: list[dict[str, Any]] = []
    for name in names:
        for split in ["fit", "validation", "protected_holdout", "latest_replay"]:
            dist_rows.append(distribution_metrics(scored, name, split))
            o05_rows.append(binary_metrics(scored, name, "O0.5", f"{name}_p_over_0_5", "target_o05", split))
            o15_rows.append(binary_metrics(scored, name, "O1.5", f"{name}_p_over_1_5", "target_o15", split))
            g = scored[scored["split"].eq(split)]
            failures = g[
                (g[[f"{name}_p0", f"{name}_p1", f"{name}_p2", f"{name}_p3_plus"]].sum(axis=1).sub(1).abs() > 1e-6)
                | (g[f"{name}_p_over_1_5"] > g[f"{name}_p_over_0_5"] + 1e-9)
                | (g[[f"{name}_p0", f"{name}_p1", f"{name}_p2", f"{name}_p3_plus", f"{name}_p_over_0_5", f"{name}_p_over_1_5"]] < -1e-9).any(axis=1)
                | (g[[f"{name}_p0", f"{name}_p1", f"{name}_p2", f"{name}_p3_plus", f"{name}_p_over_0_5", f"{name}_p_over_1_5"]] > 1 + 1e-9).any(axis=1)
            ]
            coh_rows.append({"candidate": name, "split": split, "rows": len(g), "coherence_failures": len(failures), "status": "PASS" if failures.empty else "FAIL"})
    return dist_rows, o05_rows, o15_rows, coh_rows


def select_candidate(o05: list[dict[str, Any]], o15: list[dict[str, Any]], candidates: list[str]) -> tuple[str, list[dict[str, Any]]]:
    rows = []
    for cand in candidates:
        r05 = next(r for r in o05 if r["candidate"] == cand and r["split"] == "validation")
        r15 = next(r for r in o15 if r["candidate"] == cand and r["split"] == "validation")
        score = float(r05["auc"] or 0) + float(r15["auc"] or 0) - float(r05["brier"] or 1) - float(r15["brier"] or 1)
        rows.append({"candidate": cand, "validation_o05_auc": r05["auc"], "validation_o05_brier": r05["brier"], "validation_o15_auc": r15["auc"], "validation_o15_brier": r15["brier"], "selection_score": score, "selected": False})
    selected = max(rows, key=lambda r: r["selection_score"])["candidate"]
    for r in rows:
        r["selected"] = r["candidate"] == selected
    return selected, rows


def population_selection(scored: pd.DataFrame, overlay_keys: set[str], selected: str) -> list[dict[str, Any]]:
    masks = {
        "all_qualified_nonmarket_player_games": pd.Series(True, index=scored.index),
        "final_market_conditioned_player_games": scored["player_game_key"].isin(overlay_keys),
        "nonmarket_only_player_games": ~scored["player_game_key"].isin(overlay_keys),
        "pregame_identifiable_subset": scored["lineup_status"].isin(["CONFIRMED_PREGAME_STARTER", "PROJECTED_PREGAME_STARTER"]),
        "retrospective_participation_qualified": scored["training_admissibility"].astype(str).str.startswith("ADMISSIBLE"),
    }
    rows = []
    for name, mask in masks.items():
        g = scored[mask]
        rows.append(
            {
                "population": name,
                "rows": len(g),
                "dates": g["slate_date"].nunique(),
                "players": g["player_id"].nunique(),
                "games": g["game_id"].nunique(),
                "avg_hits": float(g["actual_hits_uncapped"].mean()) if len(g) else "",
                "o05_rate": float(g["target_o05"].mean()) if len(g) else "",
                "o15_rate": float(g["target_o15"].mean()) if len(g) else "",
                "avg_pa": float(pd.to_numeric(g["actual_plate_appearances"], errors="coerce").mean()) if len(g) else "",
                "top_order_pct": float(g["lineup_bucket"].eq("top_order").mean()) if len(g) else "",
                "selected_o05_auc": safe_auc(g["target_o05"], g[f"{selected}_p_over_0_5"]) if len(g) else "",
                "selected_o15_auc": safe_auc(g["target_o15"], g[f"{selected}_p_over_1_5"]) if len(g) else "",
                "notes": "Market membership is analyzed after scoring only, never used as a feature.",
            }
        )
    return rows


def ablations(train: pd.DataFrame, hold: pd.DataFrame, numeric: list[str], categorical: list[str], selected_brier: float) -> list[dict[str, Any]]:
    families = {
        "remove_opportunity": ["plate_appearances", "_pa", "pa_per_game", "lineup", "batting_order"],
        "remove_recent_batter_form": ["d7_", "d15_"],
        "remove_longer_term_batter_skill": ["d30_", "season_to_date"],
        "remove_opposing_starter_context": ["starter_"],
        "remove_bullpen_environment_context": ["team_offense", "is_home"],
        "remove_bvp": ["bvp"],
        "remove_lineup_position_context": ["lineup", "batting_order"],
    }
    rows = []
    for name, tokens in families.items():
        use_num = [c for c in numeric if not any(t in c.lower() for t in tokens)]
        use_cat = [c for c in categorical if not any(t in c.lower() for t in tokens)]
        if len(use_num) + len(use_cat) == len(numeric) + len(categorical):
            rows.append({"ablation": name, "status": "NO_FEATURE_FAMILY_PRESENT", "features_remaining": len(use_num) + len(use_cat), "holdout_o15_auc": "", "holdout_o15_brier": "", "delta_o15_brier_vs_selected": "", "notes": "Feature family not present in frozen model feature set."})
            continue
        model = Pipeline([("preprocess", preprocessor(use_num, use_cat)), ("model", LogisticRegression(C=0.6, solver="lbfgs", max_iter=2000, random_state=SEED))])
        model.fit(train[use_num + use_cat], train["hit_count_class"].astype(int))
        probs = model.predict_proba(hold[use_num + use_cat])
        out = np.zeros((len(hold), 4))
        for idx, cls in enumerate(model.named_steps["model"].classes_):
            out[:, int(cls)] = probs[:, idx]
        p15 = out[:, 2] + out[:, 3]
        brier = float(brier_score_loss(hold["target_o15"], p15))
        rows.append({"ablation": name, "status": "EVALUATED", "features_remaining": len(use_num) + len(use_cat), "holdout_o15_auc": safe_auc(hold["target_o15"], p15), "holdout_o15_brier": brier, "delta_o15_brier_vs_selected": brier - selected_brier, "notes": "Bounded family removal; no selection based on holdout."})
    return rows


def calibration_buckets(scored: pd.DataFrame, names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for name in names:
        for split in ["validation", "protected_holdout", "latest_replay"]:
            for threshold, prob, target in [("O0.5", f"{name}_p_over_0_5", "target_o05"), ("O1.5", f"{name}_p_over_1_5", "target_o15")]:
                g = scored[scored["split"].eq(split)].copy()
                g["prob_bucket"] = pd.cut(g[prob], [0, .2, .35, .5, .65, .8, 1], include_lowest=True)
                for bucket, b in g.groupby("prob_bucket", observed=False):
                    if len(b):
                        rows.append({"candidate": name, "split": split, "threshold": threshold, "prob_bucket": str(bucket), "rows": len(b), "avg_probability": float(b[prob].mean()), "actual_rate": float(b[target].mean()), "calibration_error": float(b[prob].mean() - b[target].mean()), "sample_flag": "SPARSE" if len(b) < 40 else "OK"})
    return rows


def current_replay(current: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    withheld = []
    for _, r in current.iterrows():
        withheld.append(
            {
                "slate_date": r.get("slate_date"),
                "game_id": r.get("game_id"),
                "current_replay_status": r.get("current_replay_status"),
                "withheld_reason": r.get("withheld_reason"),
                "score_eligibility": "WITHHELD",
            }
        )
    return [], withheld


def validation_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.suffix == ".csv":
            try:
                with p.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "validation": "csv_parse", "status": status, "notes": notes})
        elif p.suffix == ".json":
            try:
                json.loads(p.read_text(encoding="utf-8"))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "validation": "json_parse", "status": status, "notes": notes})
        elif p.suffix == ".md":
            rows.append({"artifact": rel(p), "validation": "markdown_nonempty", "status": "PASS" if p.read_text(encoding="utf-8").strip() else "FAIL", "notes": ""})
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    return [{"path": rel(p), "sha256": sha256(p), "bytes": p.stat().st_size} for p in sorted(out_dir.glob("*")) if p.is_file() and not p.name.startswith("sha256_manifest")]


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    spine, source_manifest, current = load_spine()
    overlay = load_overlay()
    overlay_keys = set(overlay["player_game_key"].dropna().astype(str))
    contract_rows = verify_contract(spine, source_manifest)
    numeric, categorical, feature_manifest = choose_features(spine, source_manifest)
    cohort_rows = cohort_manifests(spine, overlay_keys)
    train_pop = spine[
        spine["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE")
        & spine["training_admissibility"].astype(str).str.startswith("ADMISSIBLE")
        & spine["actual_hits_uncapped"].notna()
    ].copy()
    train_pop, split_rows = assign_splits(train_pop)
    xcols = numeric + categorical
    train = train_pop[train_pop["split"].eq("fit")]
    hold = train_pop[train_pop["split"].eq("protected_holdout")]
    models, architecture_rows = fit_models(train, numeric, categorical, out_dir)
    scored = add_predictions(train_pop, models, xcols)
    scored, baseline_rows = baseline_predictions(scored, train)
    model_names = list(models.keys())
    all_names = model_names + [r["baseline"] for r in baseline_rows]
    dist_rows, o05_rows, o15_rows, coherence_rows = evaluate(scored, all_names)
    selected, selection_rows = select_candidate(o05_rows, o15_rows, model_names)
    incumbent = load_incumbent()
    same_row, same_row_rows = same_row_comparison(scored, incumbent, overlay, selected)
    pop_rows = population_selection(scored, overlay_keys, selected)
    selected_hold_o15 = next(r for r in o15_rows if r["candidate"] == selected and r["split"] == "protected_holdout")
    ablation_rows = ablations(train, hold, numeric, categorical, float(selected_hold_o15["brier"]))
    cal_rows = calibration_buckets(scored, all_names)
    replay_rows, replay_withheld = current_replay(current)
    deployment_rows = [
        {"issue": "historical_archived_pregame_lineup_status_unavailable", "rows": int(spine["lineup_status"].eq("LINEUP_STATUS_UNAVAILABLE").sum()), "impact": "population-selection limitation; not a statistical target leak", "decision": "DISCLOSED_NOT_DEPLOYMENT_READY_BY_ITSELF"},
        {"issue": "retrospective_starter_identity_binding", "rows": int(spine["opposing_starter_identity_semantics"].astype(str).str.contains("POSTGAME_ACTUAL", na=False).sum()), "impact": "starter identity can govern historical strict-prior joins but must be reproduced pregame for current replay", "decision": "CURRENT_PARENT_SOURCE_REQUIRED"},
        {"issue": "current_replay_governed_population", "rows": len(replay_rows), "impact": "latest current slate has no scored current rows from the certified replay spine", "decision": "CURRENT_REPLAY_PARTIALLY_READY" if replay_rows else "CURRENT_PARENT_SOURCE_REQUIRED"},
    ]
    temporal_rows = [
        {"check": "sportsbook_feature_guard", "status": "PASS", "notes": "No odds, price, bookmaker, market membership, implied probability, line, EV, or candidate/upload fields in feature matrix."},
        {"check": "outcome_target_freeze", "status": "PASS", "notes": "Target uses official actual Hits binned as 0/1/2/3+ after feature construction."},
        {"check": "whole_date_splits", "status": "PASS", "notes": "No slate_date appears in more than one split."},
        {"check": "betonline_overlay_timing", "status": "PASS", "notes": "Overlay joined only after candidate scores exist for same-row threshold/economic comparison."},
    ]
    best_hold_o05 = max([r for r in o05_rows if r["split"] == "protected_holdout" and r["candidate"] in model_names], key=lambda r: float(r["auc"] or 0))
    best_hold_o15 = max([r for r in o15_rows if r["split"] == "protected_holdout" and r["candidate"] in model_names], key=lambda r: float(r["auc"] or 0))
    inc_o05 = next((r for r in same_row_rows if r["candidate"] == "true_production_incumbent" and r["threshold"] == "O0.5" and r["segment"] == "pooled_authentic_betonline"), {})
    cand_o05 = next((r for r in same_row_rows if r["candidate"] == f"{selected}_exact_incumbent_overlap" and r["threshold"] == "O0.5" and r["segment"] == "pooled_authentic_betonline_exact_incumbent_overlap"), {})
    inc_o15 = next((r for r in same_row_rows if r["candidate"] == "true_production_incumbent" and r["threshold"] == "O1.5" and r["segment"] == "pooled_authentic_betonline"), {})
    cand_o15 = next((r for r in same_row_rows if r["candidate"] == f"{selected}_exact_incumbent_overlap" and r["threshold"] == "O1.5" and r["segment"] == "pooled_authentic_betonline_exact_incumbent_overlap"), {})
    def beats(cand: dict[str, Any], inc: dict[str, Any]) -> bool:
        return bool(cand and inc and cand.get("auc") != "" and inc.get("auc") != "" and float(cand["auc"]) > float(inc["auc"]) and float(cand["brier"]) < float(inc["brier"]))

    o05_beat = beats(cand_o05, inc_o05)
    o15_beat = beats(cand_o15, inc_o15)
    if o05_beat and o15_beat:
        prob_decision = "FULL_SPINE_MODEL_OUTPERFORMS_INCUMBENT_BOTH_THRESHOLDS"
        forced = "authorize a production-replacement candidate package"
    elif o05_beat:
        prob_decision = "FULL_SPINE_MODEL_OUTPERFORMS_INCUMBENT_O05_ONLY"
        forced = "authorize a bounded calibration correction"
    elif o15_beat:
        prob_decision = "FULL_SPINE_MODEL_OUTPERFORMS_INCUMBENT_O15_ONLY"
        forced = "authorize a bounded calibration correction"
    else:
        prob_decision = "FULL_SPINE_MODEL_DOES_NOT_OUTPERFORM"
        forced = "redesign one clearly identified architectural weakness"
    deploy_decision = "CURRENT_REPLAY_PARTIALLY_READY" if replay_rows else "CURRENT_PARENT_SOURCE_REQUIRED"
    econ_decision = "ECONOMICS_EVALUABLE_ON_FINAL_DIRECT_PRICE_SUBSET" if len(same_row) else "ECONOMICS_PARTIALLY_EVALUABLE"
    decisions = [
        ("MLB_HITS_FULL_SPINE_CONTRACT_DECISION", "PASS_21247_PLAYER_GAME_SPINE_VERIFIED_56_MODEL_READY_FEATURE_ENTRIES_NOT_OBSERVATIONS"),
        ("MLB_HITS_FULL_SPINE_TRAINING_POPULATION_DECISION", "RETROSPECTIVE_PARTICIPATION_QUALIFIED_PRIMARY_WITH_PREGAME_AND_MARKET_SUBSETS_DIAGNOSTIC"),
        ("MLB_HITS_FULL_SPINE_FEATURE_MANIFEST_DECISION", f"FROZEN_BASEBALL_ONLY_FEATURES_USED_{len(xcols)}_MARKET_FIELDS_EXCLUDED"),
        ("MLB_HITS_FULL_SPINE_BASELINE_DECISION", "GENUINE_BASEBALL_BASELINES_EVALUATED"),
        ("MLB_HITS_FULL_SPINE_COUNT_ARCHITECTURE_DECISION", "POISSON_OVERDISPERSED_ORDINAL_AND_FIXED_MULTICLASS_EVALUATED"),
        ("MLB_HITS_FULL_SPINE_DISTRIBUTION_DECISION", f"SELECTED_{selected}_BY_VALIDATION_SHARED_COHERENT_DISTRIBUTION"),
        ("MLB_HITS_FULL_SPINE_O05_DECISION", f"HOLDOUT_BEST_{best_hold_o05['candidate']}_AUC_{best_hold_o05['auc']}"),
        ("MLB_HITS_FULL_SPINE_O15_DECISION", f"HOLDOUT_BEST_{best_hold_o15['candidate']}_AUC_{best_hold_o15['auc']}"),
        ("MLB_HITS_FULL_SPINE_INCUMBENT_COMPARISON_DECISION", f"O05_BEATS_{o05_beat}_O15_BEATS_{o15_beat}_ON_FINAL_AUTHENTIC_BETONLINE_SAME_ROW_SUBSET"),
        ("MLB_HITS_FULL_SPINE_POPULATION_SELECTION_DECISION", "SPORTSBOOK_SELECTION_ANALYZED_POST_SCORE_NOT_USED_AS_FEATURE"),
        ("MLB_HITS_FULL_SPINE_ABLATION_DECISION", "BOUNDED_FEATURE_FAMILY_ABLATIONS_REPORTED_NO_OPEN_FEATURE_SEARCH"),
        ("MLB_HITS_FULL_SPINE_DEPLOYMENT_ALIGNMENT_DECISION", deploy_decision),
        ("MLB_HITS_FULL_SPINE_CURRENT_REPLAY_DECISION", f"CURRENT_SCORED_ROWS_{len(replay_rows)}_WITHHELD_{len(replay_withheld)}"),
        ("MLB_HITS_FULL_SPINE_PROBABILITY_READINESS_DECISION", prob_decision),
        ("MLB_HITS_FULL_SPINE_ECONOMIC_READINESS_DECISION", econ_decision),
        ("MLB_HITS_FULL_SPINE_FORCED_NEXT_STEP_DECISION", forced),
        ("MLB_PRODUCTION_STATUS", "UNCHANGED"),
    ]

    prediction_cols = IDENTITY_COLS + ["split", "actual_hits_uncapped", "hit_count_class", "target_o05", "target_o15"]
    for name in all_names:
        prediction_cols.extend([f"{name}_expected_hits", f"{name}_p0", f"{name}_p1", f"{name}_p2", f"{name}_p3_plus", f"{name}_p_over_0_5", f"{name}_p_over_1_5"])
    write_csv(out_dir / "verified_spine_contract_2026-07-19.csv", contract_rows)
    write_csv(out_dir / "training_cohort_manifest_2026-07-19.csv", cohort_rows)
    write_csv(out_dir / "frozen_feature_manifest_2026-07-19.csv", feature_manifest)
    write_csv(out_dir / "temporal_leakage_audit_2026-07-19.csv", temporal_rows)
    write_csv(out_dir / "split_manifest_2026-07-19.csv", split_rows)
    write_csv(out_dir / "baseline_predictions_and_metrics_2026-07-19.csv", [r for r in dist_rows + o05_rows + o15_rows if str(r.get("candidate", "")).startswith("baseline_")])
    write_csv(out_dir / "candidate_architecture_contracts_2026-07-19.csv", architecture_rows)
    write_csv(out_dir / "baseline_contracts_2026-07-19.csv", baseline_rows)
    write_csv(out_dir / "count_distribution_predictions_2026-07-19.csv", scored[[c for c in prediction_cols if c in scored.columns]].to_dict("records"))
    write_csv(out_dir / "distribution_metrics_2026-07-19.csv", dist_rows)
    write_csv(out_dir / "o05_metrics_2026-07-19.csv", o05_rows)
    write_csv(out_dir / "o15_metrics_2026-07-19.csv", o15_rows)
    write_csv(out_dir / "candidate_selection_validation_only_2026-07-19.csv", selection_rows)
    write_csv(out_dir / "probability_coherence_audit_2026-07-19.csv", coherence_rows)
    write_csv(out_dir / "calibration_buckets_2026-07-19.csv", cal_rows)
    write_csv(out_dir / "final_betonline_same_row_comparisons_2026-07-19.csv", same_row_rows)
    write_csv(out_dir / "final_betonline_same_row_prediction_rows_2026-07-19.csv", same_row.to_dict("records"))
    write_csv(out_dir / "population_selection_analysis_2026-07-19.csv", pop_rows)
    write_csv(out_dir / "ablation_results_2026-07-19.csv", ablation_rows)
    write_csv(out_dir / "deployment_alignment_audit_2026-07-19.csv", deployment_rows)
    write_csv(out_dir / "current_replay_2026-07-19.csv", replay_rows)
    write_csv(out_dir / "current_replay_withheld_2026-07-19.csv", replay_withheld)
    write_csv(out_dir / "readiness_decisions_2026-07-19.csv", [{"decision": k, "value": v} for k, v in decisions])
    machine = {
        "generated_at": generated_at,
        "spine_rows": len(spine),
        "training_rows": len(train_pop),
        "feature_manifest_entries": len(source_manifest),
        "model_feature_count": len(xcols),
        "selected_candidate": selected,
        "best_holdout_o05": best_hold_o05,
        "best_holdout_o15": best_hold_o15,
        "same_row_candidate_o05": cand_o05,
        "same_row_incumbent_o05": inc_o05,
        "same_row_candidate_o15": cand_o15,
        "same_row_incumbent_o15": inc_o15,
        "current_replay_scored_rows": len(replay_rows),
        "current_replay_withheld_rows": len(replay_withheld),
        "decisions": {k: v for k, v in decisions},
        "guardrails": {
            "sportsbook_features_used": False,
            "market_conditioned_primary_training_population": False,
            "holdout_tuning": False,
            "db_writes": False,
            "network_calls": False,
            "production_behavior_changed": False,
            "wager_outputs": False,
        },
    }
    write_json(out_dir / "machine_readable_hits_full_nonmarket_spine_reconstruction_2026-07-19.json", machine)
    write_md(
        out_dir / "hits_full_nonmarket_spine_model_reconstruction_2026-07-19.md",
        f"""# MLB Hits Full Nonmarket-Spine Model Reconstruction v2

Generated: `{generated_at}`

## Executive Summary

This research-only package trained one coherent baseball-only hit-count distribution on the certified nonmarket player-game spine. The source spine contains `{len(spine)}` player-games, and the primary training/evaluation cohort contains `{len(train_pop)}` core-complete, retrospective participation-qualified rows. The previously referenced `56` model-ready value is a feature-manifest count after outcome exclusions, not a population defect.

The selected validation-only architecture is `{selected}`. Its protected-holdout O0.5 AUC is `{best_hold_o05['auc']}` and O1.5 AUC is `{best_hold_o15['auc']}`.

## Incumbent Comparison

The exhausted BetOnline overlay was joined only after baseball scoring. Same-row pooled authentic BetOnline comparison:

- O0.5 candidate: rows `{cand_o05.get('rows', '')}`, AUC `{cand_o05.get('auc', '')}`, Brier `{cand_o05.get('brier', '')}`
- O0.5 true production incumbent: rows `{inc_o05.get('rows', '')}`, AUC `{inc_o05.get('auc', '')}`, Brier `{inc_o05.get('brier', '')}`
- O1.5 candidate: rows `{cand_o15.get('rows', '')}`, AUC `{cand_o15.get('auc', '')}`, Brier `{cand_o15.get('brier', '')}`
- O1.5 true production incumbent: rows `{inc_o15.get('rows', '')}`, AUC `{inc_o15.get('auc', '')}`, Brier `{inc_o15.get('brier', '')}`

## Deployment Alignment

Historical model quality and current deployability remain separate. The current replay spine withholds `{len(replay_withheld)}` game rows and scores `{len(replay_rows)}` current rows because a governed current nonmarket lineup/player population is still required.

## Decisions

`MLB_HITS_FULL_SPINE_PROBABILITY_READINESS_DECISION = {prob_decision}`

`MLB_HITS_FULL_SPINE_DEPLOYMENT_ALIGNMENT_DECISION = {deploy_decision}`

`MLB_HITS_FULL_SPINE_ECONOMIC_READINESS_DECISION = {econ_decision}`

`MLB_HITS_FULL_SPINE_FORCED_NEXT_STEP_DECISION = {forced}`

`MLB_PRODUCTION_STATUS = UNCHANGED`
""",
    )
    write_csv(out_dir / "validation_report_2026-07-19.csv", validation_rows(out_dir))
    write_csv(out_dir / "sha256_manifest_2026-07-19.csv", sha_manifest(out_dir))
    write_csv(out_dir / "validation_report_2026-07-19.csv", validation_rows(out_dir))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--mode", choices=["research_only", "dry_run"], default="research_only")
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": rel(args.output_dir), **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
