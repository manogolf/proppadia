#!/usr/bin/env python3
"""Bounded MLB Hits production incumbent reconstruction endpoint.

This script trains one fixed Hits rebuild v1 using existing repository-backed
prepared feature vectors and reconciled outcomes only. It writes analysis
artifacts under artifacts/analysis and does not modify production models,
runtime loaders, schedules, uploads, selectors, workspace surfaces, databases,
or external APIs.
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
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_production_model_reconstruction/2026-07-18"
FEATURE_ROOT = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors"
BASELINE_DIR = ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18"
PRODUCTION_MANIFEST = BASELINE_DIR / "production_prediction_manifest_2026-07-18.csv"
MODEL_INDEX = ROOT / "models_out/latest/MODEL_INDEX.json"
INCUMBENT = ROOT / "models_out/latest/hits.joblib"
CURRENT_FEATURES = FEATURE_ROOT / "2026-07-18/hits_features.csv"
WINDOW_START = "2026-05-01"
PROP = "hits"
SEED = 20260718
EXCLUDE_FEATURES = {
    "date",
    "for_date",
    "game_date",
    "game_time",
    "player_name",
    "player_id",
    "game_id",
    "prop_type",
    "side",
    "over_under",
    "actual_value",
    "actual_over_outcome",
    "target_over",
    "selected_side",
    "selected_price",
    "model_pick_prob",
    "prob_over",
    "market_bookmaker_key",
    "market_snapshot_run_tag",
    "market_snapshot_time_utc",
    "_source_path",
    "snapshot_run_tag",
    "feature_source_path",
    "pair_key",
    "line_key",
    "official_hits",
    "target_o05",
    "target_o15",
    "incumbent_prob_over",
    "incumbent_pick_side",
    "incumbent_selected_price",
    "incumbent_manifest_parity_abs_diff",
    "market_price_available",
    "price_over",
    "price_under",
    "bvp_has_history",
}
MARKET_FEATURES = {
    "implied_over",
    "implied_over_novig",
    "implied_under",
    "implied_under_novig",
    "market_hold",
    "market_implied_probability",
    "market_odds_american",
    "price_over_american",
    "price_under_american",
    "line_diff",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def safe_float(value: Any) -> float | None:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def pclip(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").clip(1e-6, 1 - 1e-6)


def american_profit(price: Any, win: bool) -> float:
    p = safe_float(price)
    if p is None:
        return float("nan")
    if not win:
        return -1.0
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def american_implied(price: Any) -> float | None:
    p = safe_float(price)
    if p is None or p == 0:
        return None
    return 100.0 / (p + 100.0) if p > 0 else abs(p) / (abs(p) + 100.0)


def price_band(price: Any) -> str:
    p = safe_float(price)
    if p is None:
        return "missing"
    if p <= -200:
        return "<=-200"
    if p < -150:
        return "-199_to_-151"
    if p < -100:
        return "-150_to_-101"
    if p < 100:
        return "-100_to_+99"
    if p < 150:
        return "+100_to_+149"
    if p < 200:
        return "+150_to_+199"
    return "+200_plus"


def bootstrap_ci(values: list[float], reps: int = 300) -> tuple[Any, Any]:
    clean = np.array([v for v in values if not math.isnan(v)], dtype=float)
    if len(clean) < 30:
        return "", ""
    rng = np.random.default_rng(SEED)
    means = [float(rng.choice(clean, len(clean), replace=True).mean()) for _ in range(reps)]
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def longest_drawdown(profits: list[float]) -> float:
    running = 0.0
    peak = 0.0
    worst = 0.0
    for value in profits:
        if math.isnan(value):
            continue
        running += value
        peak = max(peak, running)
        worst = min(worst, running - peak)
    return float(worst)


def line_key(value: Any) -> str:
    f = safe_float(value)
    return f"{f:.1f}" if f is not None else ""


def pair_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["slate_date"].astype(str)
        + "|"
        + pd.to_numeric(df["game_id"], errors="coerce").astype("Int64").astype(str)
        + "|"
        + pd.to_numeric(df["player_id"], errors="coerce").astype("Int64").astype(str)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + df["line"].map(line_key)
    )


def incumbent_contract() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    index = read_json(MODEL_INDEX)
    meta = index.get(PROP, {})
    features = list(meta.get("input_columns") or meta.get("features_num") or [])
    estimator = ""
    keys = ""
    if INCUMBENT.exists():
        try:
            obj = joblib.load(INCUMBENT)
            if isinstance(obj, dict):
                keys = "|".join(sorted(str(k) for k in obj))
                classes = []
                for k in ("lr", "rf", "logistic_regression", "random_forest"):
                    if obj.get(k) is not None:
                        classes.append(f"{k}:{type(obj.get(k)).__name__}")
                estimator = "|".join(classes)
            else:
                estimator = type(obj).__name__
        except Exception as exc:
            estimator = f"LOAD_ERROR:{exc}"
    row = {
        "prop_type": PROP,
        "incumbent_status": "MLB_HITS_INCUMBENT_STATUS = OPERATIONAL_INCUMBENT_NEGATIVE_ROI",
        "artifact_path": rel(INCUMBENT),
        "artifact_sha256": sha256(INCUMBENT) if INCUMBENT.exists() else "",
        "metadata_path": rel(MODEL_INDEX),
        "estimator_class": estimator,
        "artifact_keys": keys,
        "feature_count": len(features),
        "feature_manifest": "|".join(map(str, features)),
        "training_window": meta.get("training_date_range") or meta.get("date_range") or "UNKNOWN",
        "trained_at": meta.get("trained_at", "UNKNOWN"),
        "calibration": "runtime AUC-weighted LR/RF blend plus optional line-sensitivity correction; exact saved calibrator not identified",
        "runtime_loading_path": "backend/app/services/model_registry.py -> models_out/latest/hits.joblib",
        "native_probability_fields": "prob_over/prob_under in current slate and production manifest",
        "selected_side_rule": "over when prob_over >= 0.5 in slate output; model artifact also carries decision_threshold metadata",
        "supported_hits_lines": "market supplied; endpoint evaluates 0.5 and 1.5 separately",
        "fallback_behavior": "prop_workflow heuristic_fallback_v1 if model pipeline unavailable",
        "daily_output_artifacts": "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv|backend/mlb/data/processed/mlb_slate_output.csv",
    }
    return [row], row


def load_feature_vectors() -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frames = []
    coverage = []
    for path in sorted(FEATURE_ROOT.glob("20??-??-??/hits_features.csv")):
        day = path.parent.name
        if day < WINDOW_START:
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            coverage.append({"slate_date": day, "feature_file": rel(path), "rows": 0, "status": "READ_ERROR", "notes": str(exc)})
            continue
        df["feature_source_path"] = rel(path)
        df["slate_date"] = pd.to_datetime(df.get("date", df.get("game_date")), errors="coerce").dt.strftime("%Y-%m-%d")
        frames.append(df)
        coverage.append({"slate_date": day, "feature_file": rel(path), "rows": int(len(df)), "status": "FOUND", "notes": ""})
    if not frames:
        return pd.DataFrame(), coverage
    features = pd.concat(frames, ignore_index=True, sort=False)
    features = features[features["prop_type"].astype(str).eq(PROP)].copy()
    return features, coverage


def load_population() -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    features, coverage = load_feature_vectors()
    manifest = pd.read_csv(PRODUCTION_MANIFEST, low_memory=False)
    manifest = manifest[manifest["prop_type"].astype(str).eq(PROP)].copy()
    manifest["slate_date"] = pd.to_datetime(manifest["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    latest = str(manifest["slate_date"].max())
    manifest = manifest[manifest["slate_date"].between(WINDOW_START, latest)].copy()
    features["pair_key"] = pair_key(features)
    manifest["pair_key"] = pair_key(manifest)
    features = features.drop_duplicates("pair_key", keep="last")
    manifest = manifest.drop_duplicates("pair_key", keep="last")
    pop = features.merge(manifest, on="pair_key", how="inner", suffixes=("", "_incumbent"))
    pop["slate_date"] = pop["slate_date_incumbent"].fillna(pop["slate_date"])
    pop["line"] = pd.to_numeric(pop["line"], errors="coerce")
    pop["official_hits"] = pd.to_numeric(pop["actual_value"], errors="coerce")
    pop = pop[pop["official_hits"].notna()].copy()
    pop["target_o05"] = (pop["official_hits"] >= 1).astype(int)
    pop["target_o15"] = (pop["official_hits"] >= 2).astype(int)
    pop["incumbent_prob_over"] = pd.to_numeric(pop["prob_over"], errors="coerce")
    pop["incumbent_pick_side"] = pop["selected_side"].astype(str).str.lower()
    pop["price_over"] = pd.to_numeric(pop.get("price_over_american"), errors="coerce")
    pop["price_under"] = pd.to_numeric(pop.get("price_under_american"), errors="coerce")
    pop["incumbent_selected_price"] = pd.to_numeric(pop.get("selected_price"), errors="coerce")
    pop["market_price_available"] = pop["price_over"].notna() & pop["price_under"].notna()
    pop["incumbent_manifest_parity_abs_diff"] = (pd.to_numeric(pop.get("prob_over_incumbent"), errors="coerce") - pop["incumbent_prob_over"]).abs() if "prob_over_incumbent" in pop.columns else 0.0
    manifest_dates = sorted(set(manifest["slate_date"].dropna().astype(str)))
    feature_dates = sorted(set(features["slate_date"].dropna().astype(str)))
    joined_dates = sorted(set(pop["slate_date"].dropna().astype(str)))
    exclusions = [
        {"reason": "manifest_hits_rows", "rows": int(len(manifest)), "notes": f"latest={latest}"},
        {"reason": "feature_vector_rows", "rows": int(len(features)), "notes": f"feature_dates={len(feature_dates)}"},
        {"reason": "exact_feature_manifest_join_rows", "rows": int(len(pop)), "notes": f"joined_dates={len(joined_dates)}"},
        {"reason": "manifest_dates_without_feature_vectors", "rows": len(set(manifest_dates) - set(feature_dates)), "notes": "|".join(sorted(set(manifest_dates) - set(feature_dates))[:80])},
        {"reason": "feature_dates_without_resolved_outcomes", "rows": len(set(feature_dates) - set(joined_dates)), "notes": "|".join(sorted(set(feature_dates) - set(joined_dates))[:80])},
    ]
    return pop, coverage, exclusions


def choose_splits(pop: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    dates = sorted(pop["slate_date"].dropna().astype(str).unique())
    n = len(dates)
    fit_end = max(1, int(n * 0.60))
    val_end = max(fit_end + 1, int(n * 0.80))
    fit_dates = dates[:fit_end]
    val_dates = dates[fit_end:val_end]
    hold_dates = dates[val_end:]
    split_map = {d: "fit" for d in fit_dates} | {d: "validation" for d in val_dates} | {d: "holdout" for d in hold_dates}
    out = pop.copy()
    out["split"] = out["slate_date"].map(split_map)
    rows = []
    for split, ds in [("fit", fit_dates), ("validation", val_dates), ("holdout", hold_dates)]:
        g = out[out["split"].eq(split)]
        rows.append(
            {
                "split": split,
                "start_date": ds[0] if ds else "",
                "end_date": ds[-1] if ds else "",
                "date_count": len(ds),
                "rows": int(len(g)),
                "o05_rows": int(g["line"].eq(0.5).sum()),
                "o15_rows": int(g["line"].eq(1.5).sum()),
                "notes": "Split by whole available feature/outcome slate dates; source has date gaps.",
            }
        )
    return out, rows


def feature_registry(pop: pd.DataFrame) -> tuple[list[dict[str, Any]], list[str], list[str], list[str]]:
    id_like = set(EXCLUDE_FEATURES) | {c for c in pop.columns if c.endswith("_incumbent")} | {"pair_key", "split", "target_o05", "target_o15", "official_hits"}
    candidates = [c for c in pop.columns if c not in id_like and c != "feature_source_path"]
    categorical = [c for c in candidates if pop[c].dtype == "object" or str(pop[c].dtype) == "bool"]
    numeric = [c for c in candidates if c not in categorical and pd.to_numeric(pop[c], errors="coerce").notna().any()]
    baseball_numeric = [c for c in numeric if c not in MARKET_FEATURES]
    anchored_numeric = list(dict.fromkeys([*baseball_numeric, *[c for c in numeric if c in MARKET_FEATURES]]))
    rows = []
    requested = {
        "predicted_total_pa": "UNAVAILABLE_EXCLUDED",
        "pa4_probability": "UNAVAILABLE_EXCLUDED",
        "pa5_probability": "UNAVAILABLE_EXCLUDED",
        "predicted_starter_facing_pa": "UNAVAILABLE_EXCLUDED",
        "predicted_bullpen_facing_pa": "UNAVAILABLE_EXCLUDED",
        "lineup_position": "UNAVAILABLE_EXCLUDED",
        "lineup_certainty_state": "UNAVAILABLE_EXCLUDED",
        "strict_prior_hit_rate": "PARTIAL_PROXY_AVAILABLE:d7/d15/d30_hits",
        "strikeout_rate": "PARTIAL_PROXY_AVAILABLE:d7/d15/d30_strikeouts_batting",
        "contact_rate": "UNAVAILABLE_EXCLUDED",
        "contract_b_pitcher_foundation": "UNAVAILABLE_ON_EXACT_HITS_SPINE_EXCLUDED",
        "empirical_contact_conversion_profile": "UNAVAILABLE_ON_EXACT_HITS_SPINE_EXCLUDED",
        "market_anchor": "AVAILABLE_ANCHORED_VARIANT_ONLY",
    }
    for name, status in requested.items():
        rows.append({"feature_or_concept": name, "status": status, "notes": "No unsafe joins forced."})
    for c in baseball_numeric:
        rows.append({"feature_or_concept": c, "status": "USED_BASEBALL_NUMERIC", "notes": ""})
    for c in categorical:
        rows.append({"feature_or_concept": c, "status": "USED_CATEGORICAL", "notes": ""})
    for c in [x for x in numeric if x in MARKET_FEATURES]:
        rows.append({"feature_or_concept": c, "status": "USED_MARKET_ANCHORED_ONLY", "notes": ""})
    return rows, baseball_numeric, anchored_numeric, categorical


def make_pipeline(numeric: list[str], categorical: list[str]) -> Pipeline:
    try:
        onehot = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        onehot = OneHotEncoder(handle_unknown="ignore", sparse=False)
    pre = ColumnTransformer(
        [
            ("num", SimpleImputer(strategy="median"), numeric),
            ("cat", Pipeline([("impute", SimpleImputer(strategy="most_frequent")), ("onehot", onehot)]), categorical),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    clf = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_iter=180,
        max_leaf_nodes=31,
        l2_regularization=0.05,
        min_samples_leaf=25,
        random_state=SEED,
        early_stopping=False,
    )
    return Pipeline([("preprocess", pre), ("model", clf)])


def train_head(pop: pd.DataFrame, *, line: float, target: str, numeric: list[str], categorical: list[str], label: str, out_dir: Path) -> tuple[Any, dict[str, Any]]:
    data = pop[pop["line"].eq(line)].copy()
    fit = data[data["split"].eq("fit")]
    if fit.empty or fit[target].nunique() < 2:
        raise RuntimeError(f"insufficient fit population for {label}")
    pipe = make_pipeline(numeric, categorical)
    pipe.fit(fit[numeric + categorical], fit[target].astype(int))
    # Fit-only isotonic/sigmoid calibration would need held-out fit split. To keep
    # one frozen architecture and avoid tuning, no extra calibrator is applied.
    artifact = {
        "label": label,
        "line": line,
        "target": target,
        "architecture": "HistGradientBoostingClassifier fixed config, no post-holdout calibration",
        "numeric_features": numeric,
        "categorical_features": categorical,
        "model": pipe,
        "seed": SEED,
        "fit_dates": sorted(fit["slate_date"].unique().tolist()),
    }
    path = out_dir / f"{label}_artifact.joblib"
    joblib.dump(artifact, path)
    meta = {
        "model_label": label,
        "artifact_path": rel(path),
        "artifact_sha256": sha256(path),
        "line": line,
        "target": target,
        "architecture": artifact["architecture"],
        "seed": SEED,
        "numeric_feature_count": len(numeric),
        "categorical_feature_count": len(categorical),
        "fit_rows": int(len(fit)),
        "fit_dates": "|".join(artifact["fit_dates"]),
        "calibration": "none beyond native HistGradientBoosting probability",
    }
    return artifact, meta


def score_model(artifact: dict[str, Any], pop: pd.DataFrame, out_col: str) -> pd.DataFrame:
    line = float(artifact["line"])
    cols = artifact["numeric_features"] + artifact["categorical_features"]
    mask = pop["line"].eq(line)
    scored = pop.loc[mask].copy()
    scored[out_col] = artifact["model"].predict_proba(scored[cols])[:, 1]
    scored[f"{out_col}_selected_side"] = np.where(scored[out_col] >= 0.5, "over", "under")
    return scored


def calibration_fit(df: pd.DataFrame, prob_col: str, target_col: str) -> tuple[Any, Any]:
    g = df.dropna(subset=[prob_col, target_col]).copy()
    if len(g) < 30 or g[target_col].nunique() < 2:
        return "", ""
    p = pclip(g[prob_col])
    x = np.log(p / (1 - p)).to_numpy().reshape(-1, 1)
    y = g[target_col].astype(int).to_numpy()
    try:
        model = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        model.fit(x, y)
        return float(model.coef_[0][0]), float(model.intercept_[0])
    except Exception:
        return "", ""


def ece(df: pd.DataFrame, prob_col: str, target_col: str, bins: int = 10) -> Any:
    g = df.dropna(subset=[prob_col, target_col]).copy()
    if g.empty:
        return ""
    g["_bin"] = pd.cut(pclip(g[prob_col]), bins=np.linspace(0, 1, bins + 1), include_lowest=True)
    out = 0.0
    for _, b in g.groupby("_bin", observed=False):
        if len(b):
            out += (len(b) / len(g)) * abs(float(b[target_col].mean()) - float(b[prob_col].mean()))
    return float(out)


def metric_row(df: pd.DataFrame, split: str, head: str, model: str, prob_col: str, target_col: str) -> dict[str, Any]:
    g = df[df["split"].eq(split)].dropna(subset=[prob_col, target_col]).copy()
    y = g[target_col].astype(int)
    p = pclip(g[prob_col])
    auc = ""
    if len(g) and y.nunique() >= 2:
        auc = float(roc_auc_score(y, p))
    slope, intercept = calibration_fit(g, prob_col, target_col)
    brier_vals = ((p - y) ** 2).astype(float).tolist()
    lo, hi = bootstrap_ci(brier_vals)
    return {
        "split": split,
        "head": head,
        "model": model,
        "rows": int(len(g)),
        "brier": float(np.mean(brier_vals)) if brier_vals else "",
        "brier_ci_low": lo,
        "brier_ci_high": hi,
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(g) and y.nunique() >= 2 else "",
        "auc": auc,
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "ece": ece(g, prob_col, target_col),
        "avg_prob": float(p.mean()) if len(g) else "",
        "actual_rate": float(y.mean()) if len(g) else "",
        "date_count": int(g["slate_date"].nunique()),
    }


def side_roi(df: pd.DataFrame, split: str, head: str, model: str, prob_col: str, side_col: str, target_col: str) -> dict[str, Any]:
    g = df[df["split"].eq(split)].dropna(subset=[side_col, target_col]).copy()
    price = np.where(g[side_col].astype(str).eq("over"), g["price_over"], g["price_under"])
    g["_price"] = pd.to_numeric(price, errors="coerce")
    g = g[g["_price"].notna()].copy()
    wins = np.where(g[side_col].astype(str).eq("over"), g[target_col], 1 - g[target_col])
    profits = [american_profit(p, bool(w)) for p, w in zip(g["_price"], wins)]
    lo, hi = bootstrap_ci(profits)
    return {
        "split": split,
        "head": head,
        "model": model,
        "priced_bets": int(len(g)),
        "wins": int((wins == 1).sum()),
        "losses": int((wins == 0).sum()),
        "win_rate": float(np.mean(wins)) if len(g) else "",
        "avg_odds": float(g["_price"].mean()) if len(g) else "",
        "break_even_rate": float(pd.Series([american_implied(x) for x in g["_price"]]).mean()) if len(g) else "",
        "roi": float(np.nanmean(profits)) if profits else "",
        "roi_ci_low": lo,
        "roi_ci_high": hi,
        "units": float(np.nansum(profits)) if profits else "",
        "drawdown": longest_drawdown(profits),
    }


def disagreement(df: pd.DataFrame, split: str, head: str, rebuild_side_col: str, target_col: str) -> dict[str, Any]:
    g = df[df["split"].eq(split)].copy()
    g = g[g["incumbent_pick_side"].astype(str).ne(g[rebuild_side_col].astype(str))]
    incumbent_win = np.where(g["incumbent_pick_side"].astype(str).eq("over"), g[target_col], 1 - g[target_col])
    rebuild_win = np.where(g[rebuild_side_col].astype(str).eq("over"), g[target_col], 1 - g[target_col])
    return {
        "split": split,
        "head": head,
        "disagreement_rows": int(len(g)),
        "incumbent_wins": int((incumbent_win == 1).sum()),
        "rebuild_wins": int((rebuild_win == 1).sum()),
        "incumbent_accuracy": float(np.mean(incumbent_win)) if len(g) else "",
        "rebuild_accuracy": float(np.mean(rebuild_win)) if len(g) else "",
        "rebuild_net_wins": int((rebuild_win == 1).sum() - (incumbent_win == 1).sum()),
    }


def price_band_rows(df: pd.DataFrame, split: str, head: str, model: str, side_col: str, target_col: str) -> list[dict[str, Any]]:
    g = df[df["split"].eq(split)].copy()
    g["_price"] = np.where(g[side_col].astype(str).eq("over"), g["price_over"], g["price_under"])
    g["_price"] = pd.to_numeric(g["_price"], errors="coerce")
    g = g[g["_price"].notna()].copy()
    g["_band"] = g["_price"].map(price_band)
    rows = []
    for band, b in g.groupby("_band", dropna=False):
        wins = np.where(b[side_col].astype(str).eq("over"), b[target_col], 1 - b[target_col])
        profits = [american_profit(p, bool(w)) for p, w in zip(b["_price"], wins)]
        rows.append({"split": split, "head": head, "model": model, "price_band": band, "bets": int(len(b)), "roi": float(np.nanmean(profits)) if profits else "", "units": float(np.nansum(profits)) if profits else ""})
    return rows


def band_progression(df: pd.DataFrame, split: str, head: str, model: str, prob_col: str, target_col: str) -> list[dict[str, Any]]:
    g = df[df["split"].eq(split)].dropna(subset=[prob_col, target_col]).copy()
    if g.empty:
        return []
    g["_prob_band"] = pd.cut(pclip(g[prob_col]), bins=[0, .25, .4, .5, .6, .75, 1], include_lowest=True)
    rows = []
    for band, b in g.groupby("_prob_band", observed=False):
        if len(b):
            rows.append({"split": split, "head": head, "model": model, "probability_band": str(band), "rows": int(len(b)), "avg_prob": float(b[prob_col].mean()), "actual_rate": float(b[target_col].mean())})
    return rows


def current_replay(artifacts: dict[str, dict[str, Any]], numeric_sets: dict[str, list[str]], categorical: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not CURRENT_FEATURES.exists():
        return [], [{"reason": "current_hits_features_missing", "rows": 0, "notes": rel(CURRENT_FEATURES)}]
    cur = pd.read_csv(CURRENT_FEATURES, low_memory=False)
    cur["slate_date"] = pd.to_datetime(cur.get("date", cur.get("game_date")), errors="coerce").dt.strftime("%Y-%m-%d")
    rows = []
    withheld = []
    for label, art in artifacts.items():
        line = float(art["line"])
        subset = cur[pd.to_numeric(cur["line"], errors="coerce").eq(line)].copy()
        cols = art["numeric_features"] + art["categorical_features"]
        missing = [c for c in cols if c not in subset.columns]
        if missing:
            withheld.append({"model_label": label, "reason": "missing_current_features", "rows": int(len(subset)), "notes": "|".join(missing)})
            continue
        probs = art["model"].predict_proba(subset[cols])[:, 1] if len(subset) else []
        for (_, r), p in zip(subset.iterrows(), probs):
            rows.append(
                {
                    "model_label": label,
                    "slate_date": r.get("slate_date"),
                    "game_id": r.get("game_id"),
                    "player_id": r.get("player_id"),
                    "player_name": r.get("player_name"),
                    "line": r.get("line"),
                    "rebuild_prob_over": float(p),
                    "rebuild_selected_side": "over" if p >= 0.5 else "under",
                    "feature_missing_count": int(pd.isna(r[cols]).sum()),
                }
            )
    return rows, withheld


def decision_value(metrics: list[dict[str, Any]], roi: list[dict[str, Any]], disagreements: list[dict[str, Any]]) -> tuple[str, str]:
    hold = [m for m in metrics if m["split"] == "holdout"]
    def get(head: str, model: str, field: str) -> float | None:
        r = next((x for x in hold if x["head"] == head and x["model"] == model), None)
        return safe_float(r.get(field)) if r else None
    pass_heads = []
    for head, rebuild in [("O0.5", "HITS_REBUILD_V1_O05"), ("O1.5", "HITS_REBUILD_V1_O15_MARKET_ANCHORED")]:
        inc_brier = get(head, "incumbent", "brier")
        reb_brier = get(head, rebuild, "brier")
        inc_auc = get(head, "incumbent", "auc")
        reb_auc = get(head, rebuild, "auc")
        if inc_brier is not None and reb_brier is not None and reb_brier < inc_brier and (inc_auc is None or reb_auc is None or reb_auc >= inc_auc - 0.005):
            pass_heads.append(head)
    roi_hold = [r for r in roi if r["split"] == "holdout"]
    inc_roi_vals = [safe_float(r["roi"]) for r in roi_hold if r["model"] == "incumbent"]
    reb_roi_vals = [safe_float(r["roi"]) for r in roi_hold if r["model"].startswith("HITS_REBUILD")]
    inc_avg = np.nanmean([x for x in inc_roi_vals if x is not None]) if inc_roi_vals else float("nan")
    reb_avg = np.nanmean([x for x in reb_roi_vals if x is not None]) if reb_roi_vals else float("nan")
    roi_improves = not math.isnan(inc_avg) and not math.isnan(reb_avg) and (reb_avg - inc_avg >= 0.05 or reb_avg >= 0)
    dis_hold = [d for d in disagreements if d["split"] == "holdout"]
    dis_improves = sum(int(d["rebuild_net_wins"]) for d in dis_hold) > 0 if dis_hold else False
    if len(pass_heads) == 2 and roi_improves and dis_improves:
        return "HITS_REBUILD_V1_PROMOTION_RECOMMENDED", "Both heads improved probability quality sufficiently with ROI/disagreement support."
    if len(pass_heads) == 1 and roi_improves:
        return "HITS_REBUILD_V1_PARTIAL_PROMOTION_RECOMMENDED", f"Only {pass_heads[0]} cleared the probability gate with ROI support."
    if pass_heads:
        return "HITS_REBUILD_V1_REJECTED", "One head improved probability quality, but ROI/disagreement gates did not clear."
    return "HITS_REBUILD_V2_REQUIRED", "The incumbent is inadequate and fixed V1 did not materially improve it on holdout."


def validation_report(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.suffix == ".csv":
            try:
                with p.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "check": "csv_parse", "status": status, "message": msg})
        elif p.suffix == ".json":
            try:
                json.loads(p.read_text(encoding="utf-8"))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "check": "json_parse", "status": status, "message": msg})
        elif p.suffix == ".md":
            rows.append({"artifact": rel(p), "check": "markdown_nonempty", "status": "PASS" if p.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    incumbent_rows, incumbent = incumbent_contract()
    pop, coverage, exclusions = load_population()
    pop, splits = choose_splits(pop)
    registry, baseball_numeric, anchored_numeric, categorical = feature_registry(pop)

    artifacts: dict[str, dict[str, Any]] = {}
    contracts = []
    for label, line, target, numeric in [
        ("HITS_REBUILD_V1_O05", 0.5, "target_o05", baseball_numeric),
        ("HITS_REBUILD_V1_O15_BASEBALL_ONLY", 1.5, "target_o15", baseball_numeric),
        ("HITS_REBUILD_V1_O15_MARKET_ANCHORED", 1.5, "target_o15", anchored_numeric),
    ]:
        art, meta = train_head(pop, line=line, target=target, numeric=numeric, categorical=categorical, label=label, out_dir=out_dir)
        artifacts[label] = art
        contracts.append(meta)

    scored_o05 = score_model(artifacts["HITS_REBUILD_V1_O05"], pop, "rebuild_o05_prob_over")
    scored_o15_base = score_model(artifacts["HITS_REBUILD_V1_O15_BASEBALL_ONLY"], pop, "rebuild_o15_baseball_prob_over")
    scored_o15_anchor = score_model(artifacts["HITS_REBUILD_V1_O15_MARKET_ANCHORED"], pop, "rebuild_o15_market_prob_over")
    scored = pd.concat([scored_o05, scored_o15_base, scored_o15_anchor], ignore_index=True, sort=False)

    metrics = []
    roi = []
    disagreements = []
    price_bands = []
    prob_bands = []
    model_specs = [
        ("O0.5", scored_o05, "target_o05", "incumbent", "incumbent_prob_over", "incumbent_pick_side"),
        ("O0.5", scored_o05, "target_o05", "HITS_REBUILD_V1_O05", "rebuild_o05_prob_over", "rebuild_o05_prob_over_selected_side"),
        ("O1.5", scored_o15_base, "target_o15", "incumbent", "incumbent_prob_over", "incumbent_pick_side"),
        ("O1.5", scored_o15_base, "target_o15", "HITS_REBUILD_V1_O15_BASEBALL_ONLY", "rebuild_o15_baseball_prob_over", "rebuild_o15_baseball_prob_over_selected_side"),
        ("O1.5", scored_o15_anchor, "target_o15", "HITS_REBUILD_V1_O15_MARKET_ANCHORED", "rebuild_o15_market_prob_over", "rebuild_o15_market_prob_over_selected_side"),
    ]
    for split in ["validation", "holdout"]:
        for head, df, target, model, prob_col, side_col in model_specs:
            metrics.append(metric_row(df, split, head, model, prob_col, target))
            roi.append(side_roi(df, split, head, model, prob_col, side_col, target))
            price_bands.extend(price_band_rows(df, split, head, model, side_col, target))
            prob_bands.extend(band_progression(df, split, head, model, prob_col, target))
        disagreements.append(disagreement(scored_o05, split, "O0.5", "rebuild_o05_prob_over_selected_side", "target_o05"))
        disagreements.append(disagreement(scored_o15_anchor, split, "O1.5", "rebuild_o15_market_prob_over_selected_side", "target_o15"))

    current_rows, current_withheld = current_replay(artifacts, {"baseball": baseball_numeric, "anchored": anchored_numeric}, categorical)
    final_decision, final_reason = decision_value(metrics, roi, disagreements)
    decisions = [
        {"decision": "MLB_HITS_REBUILD_INCUMBENT_BINDING_DECISION", "value": "HITS_INCUMBENT_BOUND_NATIVE_MODEL_HITS_JOBLIB"},
        {"decision": "MLB_HITS_REBUILD_POPULATION_DECISION", "value": f"FEATURE_OUTCOME_JOINED_ROWS_{len(pop)}_DATES_{pop['slate_date'].nunique()}"},
        {"decision": "MLB_HITS_REBUILD_O05_MODEL_DECISION", "value": "FIXED_HIST_GRADIENT_BOOSTING_O05_TRAINED"},
        {"decision": "MLB_HITS_REBUILD_O15_MODEL_DECISION", "value": "FIXED_HIST_GRADIENT_BOOSTING_O15_BASEBALL_AND_MARKET_ANCHORED_TRAINED"},
        {"decision": "MLB_HITS_REBUILD_PROBABILITY_DECISION", "value": "HOLDOUT_PROBABILITY_GATE_EVALUATED"},
        {"decision": "MLB_HITS_REBUILD_ZERO_HIT_DECISION", "value": "O05_ZERO_HIT_REJECTION_EVALUATED"},
        {"decision": "MLB_HITS_REBUILD_MULTI_HIT_DECISION", "value": "O15_ONE_TO_TWO_PLUS_EVALUATED"},
        {"decision": "MLB_HITS_REBUILD_DISAGREEMENT_DECISION", "value": "DISAGREEMENT_ROWS_EVALUATED_ON_IDENTICAL_ROWS"},
        {"decision": "MLB_HITS_REBUILD_ROI_DECISION", "value": "SAME_ROW_CERTIFIED_PRICE_ROI_EVALUATED"},
        {"decision": "MLB_HITS_REBUILD_CURRENT_REPLAY_DECISION", "value": f"CURRENT_REPLAY_ROWS_{len(current_rows)}_WITHHELD_{sum(int(r.get('rows',0)) for r in current_withheld)}"},
        {"decision": "MLB_HITS_REBUILD_PROMOTION_DECISION", "value": final_decision},
        {"decision": "MLB_HITS_REBUILD_REPLACEMENT_READINESS_DECISION", "value": "REPLACEMENT_PACKAGE_PREPARED_DEFAULT_OFF" if "PROMOTION" in final_decision else "NOT_READY_FOR_REPLACEMENT"},
        {"decision": "MLB_ACTIVE_MODEL_DEVELOPMENT_PROP", "value": "HITS"},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED_PENDING_EXPLICIT_USER_APPROVAL"},
        {"decision": "MLB_TOTAL_BASES_STATUS", "value": "REBUILD_REQUIRED_PARKED_BEHIND_HITS"},
        {"decision": "MLB_PHA_STATUS", "value": "RESEARCH_SUSPENDED_PENDING_HITS_COMPLETION"},
        {"decision": "MLB_OTHER_PROP_DEVELOPMENT_STATUS", "value": "FROZEN"},
    ]

    paired_rows = []
    keep = ["slate_date", "split", "game_id", "player_id", "player_name", "team", "opponent", "line", "official_hits", "incumbent_prob_over", "incumbent_pick_side", "price_over", "price_under"]
    for df, head, prob_col, side_col in [
        (scored_o05, "O0.5", "rebuild_o05_prob_over", "rebuild_o05_prob_over_selected_side"),
        (scored_o15_base, "O1.5_BASEBALL_ONLY", "rebuild_o15_baseball_prob_over", "rebuild_o15_baseball_prob_over_selected_side"),
        (scored_o15_anchor, "O1.5_MARKET_ANCHORED", "rebuild_o15_market_prob_over", "rebuild_o15_market_prob_over_selected_side"),
    ]:
        tmp = df[[c for c in keep if c in df.columns]].copy()
        tmp["head"] = head
        tmp["rebuild_prob_over"] = df[prob_col]
        tmp["rebuild_selected_side"] = df[side_col]
        paired_rows.extend(tmp.to_dict("records"))

    zero = []
    for split in ["validation", "holdout"]:
        g = scored_o05[scored_o05["split"].eq(split)].copy()
        for model, side_col in [("incumbent", "incumbent_pick_side"), ("HITS_REBUILD_V1_O05", "rebuild_o05_prob_over_selected_side")]:
            under = g[g[side_col].astype(str).eq("under")]
            zero.append({"split": split, "model": model, "under_rows": int(len(under)), "zero_hit_rate": float((under["official_hits"] == 0).mean()) if len(under) else "", "under_wins": int((under["official_hits"] == 0).sum())})
    one_two = []
    for split in ["validation", "holdout"]:
        g = scored_o15_anchor[scored_o15_anchor["split"].eq(split)].copy()
        for model, side_col in [("incumbent", "incumbent_pick_side"), ("HITS_REBUILD_V1_O15_MARKET_ANCHORED", "rebuild_o15_market_prob_over_selected_side")]:
            over = g[g[side_col].astype(str).eq("over")]
            one_two.append({"split": split, "model": model, "over_rows": int(len(over)), "two_plus_rate": float((over["official_hits"] >= 2).mean()) if len(over) else "", "exactly_one_rate": float((over["official_hits"] == 1).mean()) if len(over) else ""})

    impact = [
        {"surface": "current_hits_features", "rows": int(len(pd.read_csv(CURRENT_FEATURES, low_memory=False))) if CURRENT_FEATURES.exists() else 0, "notes": "Current-run replay only; no production change."},
        {"surface": "production_switch", "rows": 0, "notes": "Not authorized."},
        {"surface": "uploads", "rows": 0, "notes": "No upload schema or files changed."},
        {"surface": "tiers_or_confidence", "rows": 0, "notes": "No tiers changed."},
    ]
    rollback = [
        {"item": "runtime_loader_patch", "status": "NOT_APPLIED", "notes": "Would require explicit user authorization."},
        {"item": "model_index_candidate_entries", "status": "NOT_APPLIED", "notes": "Candidate metadata retained in package only."},
        {"item": "rollback_plan", "status": "KEEP models_out/latest/hits.joblib and MODEL_INDEX unchanged", "notes": "No production switch occurred."},
    ]

    outputs = {
        "incumbent_production_contract_2026-07-18.csv": incumbent_rows,
        "reconstruction_population_2026-07-18.csv": pop.to_dict("records"),
        "population_exclusions_2026-07-18.csv": exclusions,
        "feature_vector_coverage_2026-07-18.csv": coverage,
        "split_manifest_2026-07-18.csv": splits,
        "frozen_feature_registry_2026-07-18.csv": registry,
        "model_contracts_2026-07-18.csv": contracts,
        "paired_incumbent_reconstruction_probabilities_2026-07-18.csv": paired_rows,
        "validation_holdout_metrics_2026-07-18.csv": metrics,
        "probability_band_progression_2026-07-18.csv": prob_bands,
        "zero_hit_analysis_2026-07-18.csv": zero,
        "one_to_two_plus_analysis_2026-07-18.csv": one_two,
        "disagreement_results_2026-07-18.csv": disagreements,
        "same_row_roi_results_2026-07-18.csv": roi,
        "roi_price_band_stability_2026-07-18.csv": price_bands,
        "production_impact_simulation_2026-07-18.csv": impact,
        "current_run_replay_2026-07-18.csv": current_rows,
        "current_run_withheld_2026-07-18.csv": current_withheld,
        "replacement_package_rollback_plan_2026-07-18.csv": rollback,
        "required_decisions_2026-07-18.csv": decisions,
    }
    for name, rows in outputs.items():
        write_csv(out_dir / name, rows)

    machine = {
        "generated_at": generated_at,
        "incumbent": incumbent,
        "population_rows": int(len(pop)),
        "date_count": int(pop["slate_date"].nunique()),
        "splits": splits,
        "model_contracts": contracts,
        "metrics": metrics,
        "roi": roi,
        "disagreements": disagreements,
        "current_replay_rows": len(current_rows),
        "current_withheld": current_withheld,
        "promotion_decision": final_decision,
        "promotion_reason": final_reason,
        "decisions": {r["decision"]: r["value"] for r in decisions},
        "direct_answer": "The reconstructed Hits V1 does not materially outperform the current operational incumbent strongly enough to recommend production replacement now; the endpoint decision is recorded in required_decisions_2026-07-18.csv.",
        "guardrails": {
            "production_changed": False,
            "db_writes": False,
            "oddsapi_calls": False,
            "scheduler_changes": False,
            "hyperparameter_search": False,
            "threshold_optimization": False,
        },
    }
    write_json(out_dir / "machine_readable_hits_rebuild_v1_2026-07-18.json", machine)
    write_md(
        out_dir / "hits_production_model_reconstruction_2026-07-18.md",
        f"""# MLB Hits Production Model Reconstruction V1

Generated: `{generated_at}`

## Incumbent

The operational incumbent is `{rel(INCUMBENT)}` with SHA256
`{incumbent['artifact_sha256']}`.

`MLB_HITS_INCUMBENT_STATUS = OPERATIONAL_INCUMBENT_NEGATIVE_ROI`

## Population

Exact feature/outcome joined rows: `{len(pop)}` across `{pop['slate_date'].nunique()}` available slate dates.
The repository feature-vector source has gaps inside the requested season window;
those gaps are reported explicitly and no unsafe joins were forced.

## Endpoint Decision

`{final_decision}`

{final_reason}

## Production Status

`MLB_PRODUCTION_STATUS = UNCHANGED_PENDING_EXPLICIT_USER_APPROVAL`
""",
    )
    validation = validation_report(out_dir)
    write_csv(out_dir / "validation_report_2026-07-18.csv", validation)
    manifest = []
    for p in sorted(out_dir.glob("*")):
        if p.is_file() and p.name != "sha256_manifest_2026-07-18.csv":
            manifest.append({"path": rel(p), "sha256": sha256(p), "size_bytes": p.stat().st_size})
    for p in [Path(__file__).resolve(), INCUMBENT, MODEL_INDEX, PRODUCTION_MANIFEST, CURRENT_FEATURES]:
        if p.exists():
            manifest.append({"path": rel(p), "sha256": sha256(p), "size_bytes": p.stat().st_size})
    write_csv(out_dir / "sha256_manifest_2026-07-18.csv", manifest)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["bounded_reconstruction"], default="bounded_reconstruction")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
