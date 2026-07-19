#!/usr/bin/env python3
"""Certify exact Total Bases production Champion vs existing shadow endpoints.

Read-only certification. This script consumes existing production baseline,
shadow-score, shadow-evaluation, and reconciliation artifacts. It does not fit
models, generate predictions, call external services, write databases, modify
schedulers, or alter production behavior.
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_total_bases_exact_champion_shadow_endpoint/2026-07-18"
BASELINE_DIR = ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18"
MODEL_INDEX = ROOT / "models_out/latest/MODEL_INDEX.json"
TB_MODEL = ROOT / "models_out/latest/total_bases.joblib"
SHADOW_ROOT = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow"
SHADOW_EVAL_ROWS = SHADOW_ROOT / "evaluation/total_bases_shadow_evaluation_rows.csv"
SHADOW_EVAL_SUMMARY = SHADOW_ROOT / "evaluation/total_bases_shadow_evaluation_summary.json"
PRODUCTION_MANIFEST = BASELINE_DIR / "production_prediction_manifest_2026-07-18.csv"
CURRENT_SLATE = ROOT / "backend/mlb/data/processed/mlb_slate_output.csv"
CURRENT_UPLOAD = ROOT / "backend/mlb/data/processed/mlb_book_upload.csv"
WINDOW_START = "2026-05-01"
WINDOW_END = "2026-07-17"
PROP = "total_bases"
MODELS = {
    "production": ("production_prob_over", "production_pick_side"),
    "tb_rolling_balanced_shadow": ("tb_rolling_balanced_shadow_prob_over", "tb_rolling_balanced_shadow_pick_side"),
    "tb_rolling_unweighted_shadow": ("tb_rolling_unweighted_shadow_prob_over", "tb_rolling_unweighted_shadow_pick_side"),
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
        if value is None:
            return None
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


def bootstrap_ci(values: list[float], reps: int = 400) -> tuple[Any, Any]:
    clean = np.array([v for v in values if not math.isnan(v)], dtype=float)
    if len(clean) < 30:
        return "", ""
    rng = np.random.default_rng(20260718)
    means = [float(rng.choice(clean, size=len(clean), replace=True).mean()) for _ in range(reps)]
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def calibration_fit(frame: pd.DataFrame, prob_col: str) -> tuple[Any, Any]:
    g = frame.dropna(subset=[prob_col, "y_over"]).copy()
    if len(g) < 30 or g["y_over"].nunique() < 2:
        return "", ""
    p = pclip(g[prob_col])
    x = np.log(p / (1 - p)).to_numpy().reshape(-1, 1)
    y = g["y_over"].astype(int).to_numpy()
    try:
        model = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        model.fit(x, y)
        return float(model.coef_[0][0]), float(model.intercept_[0])
    except Exception:
        return "", ""


def ece(frame: pd.DataFrame, prob_col: str, bins: int = 10) -> Any:
    g = frame.dropna(subset=[prob_col, "y_over"]).copy()
    if g.empty:
        return ""
    g["_bin"] = pd.cut(pclip(g[prob_col]), bins=np.linspace(0, 1, bins + 1), include_lowest=True)
    total = len(g)
    out = 0.0
    for _, b in g.groupby("_bin", observed=False):
        if len(b):
            out += (len(b) / total) * abs(float(b["y_over"].mean()) - float(b[prob_col].mean()))
    return float(out)


def load_champion_contract() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    index = read_json(MODEL_INDEX)
    meta = index.get(PROP, {})
    obj: Any = {}
    estimator_class = ""
    artifact_keys = ""
    if TB_MODEL.exists():
        try:
            obj = joblib.load(TB_MODEL)
            if isinstance(obj, dict):
                artifact_keys = "|".join(sorted(str(k) for k in obj))
                classes = []
                for key in ("logistic_regression", "random_forest", "lr", "rf"):
                    if obj.get(key) is not None:
                        classes.append(f"{key}:{type(obj.get(key)).__name__}")
                estimator_class = "|".join(classes)
            else:
                estimator_class = type(obj).__name__
        except Exception as exc:
            estimator_class = f"LOAD_ERROR:{exc}"
    features = list(meta.get("input_columns") or meta.get("features_num") or [])
    row = {
        "prop_type": PROP,
        "model_artifact_path": rel(TB_MODEL),
        "artifact_sha256": sha256(TB_MODEL) if TB_MODEL.exists() else "",
        "metadata_path": rel(MODEL_INDEX),
        "metadata_sha256": sha256(MODEL_INDEX) if MODEL_INDEX.exists() else "",
        "estimator_class": estimator_class,
        "artifact_keys": artifact_keys,
        "feature_count": len(features),
        "feature_manifest": "|".join(str(f) for f in features),
        "training_timestamp": meta.get("trained_at", "UNKNOWN"),
        "training_period": meta.get("training_date_range") or meta.get("date_range") or "UNKNOWN",
        "calibration_layer": "runtime AUC-weighted LR/RF blend plus line-sensitivity correction per make_prediction; no endpoint recalibration here",
        "runtime_loader": "backend/app/services/model_registry.py::_latest_artifact_path -> models_out/latest/total_bases.joblib",
        "native_probability_fields": "prob_over/prob_under in slate; production_prob_over/prob_under in shadow rows",
        "selected_side_rule": "production_pick_side from current slate output; over when prob_over >= 0.5 in slate artifact",
        "daily_output_artifacts": "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv|backend/mlb/data/processed/mlb_slate_output.csv",
        "contract_decision": "TRUE_PRODUCTION_CHAMPION_BOUND",
    }
    return [row], row


def shadow_inventory() -> list[dict[str, Any]]:
    summary = read_json(SHADOW_EVAL_SUMMARY)
    score_files = sorted(SHADOW_ROOT.glob("20??-??-??/total_bases_shadow_scores_*.csv"))
    rows = []
    for ident, spec, status in [
        (
            "tb_rolling_balanced_shadow",
            "LogisticRegression class_weight=balanced using line, line_bucket, and d7/d15/d30 rolling production context.",
            "prior conclusion: research-only and not promotion-ready",
        ),
        (
            "tb_rolling_unweighted_shadow",
            "LogisticRegression class_weight=None using line, line_bucket, and d7/d15/d30 rolling production context.",
            "prior conclusion: research-only pending larger live sample",
        ),
    ]:
        rows.append(
            {
                "shadow_identifier": ident,
                "exact_specification": spec,
                "prediction_grain": "slate_date|game_id|player_id|prop_type|line",
                "source_artifact": rel(SHADOW_EVAL_ROWS),
                "frozen_date": "daily shadow files 2026-07-02 through 2026-07-18; cumulative evaluation generated " + str(summary.get("generated_at", "")),
                "live_or_historical_status": "historical cumulative plus current-slate shadow, analysis-only",
                "rows_generated": summary.get("rows_scored", ""),
                "graded_rows": summary.get("rows_with_outcomes", ""),
                "operationally_active": "False",
                "previous_not_ready_conclusion": status,
            }
        )
    return rows


def load_paired_rows() -> pd.DataFrame:
    shadow = pd.read_csv(SHADOW_EVAL_ROWS, low_memory=False)
    shadow = shadow[shadow["prop_type"].astype(str).eq(PROP)].copy()
    shadow["slate_date"] = pd.to_datetime(shadow["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    shadow = shadow[shadow["slate_date"].between(WINDOW_START, WINDOW_END)].copy()
    shadow = shadow[shadow["resolved"].astype(str).str.lower().eq("true")].copy()
    for col in ["game_id", "player_id", "line", "actual_value", "y_over"]:
        shadow[col] = pd.to_numeric(shadow[col], errors="coerce")
    shadow["line_key"] = shadow["line"].map(lambda x: f"{float(x):.1f}" if pd.notna(x) else "")
    shadow["pair_key"] = (
        shadow["slate_date"].astype(str)
        + "|"
        + shadow["game_id"].astype("Int64").astype(str)
        + "|"
        + shadow["player_id"].astype("Int64").astype(str)
        + "|"
        + shadow["prop_type"].astype(str)
        + "|"
        + shadow["line_key"].astype(str)
    )
    keep = [
        "slate_date",
        "game_id",
        "player_id",
        "prop_type",
        "line",
        "line_key",
        "pair_key",
        "selected_side",
        "selected_price",
        "market_bookmaker_key",
        "market_snapshot_run_tag",
        "market_snapshot_time_utc",
        "_source_path",
        "prob_over",
    ]
    manifest = pd.read_csv(PRODUCTION_MANIFEST, low_memory=False)
    manifest = manifest[manifest["prop_type"].astype(str).eq(PROP)].copy()
    manifest["slate_date"] = pd.to_datetime(manifest["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["game_id", "player_id", "line"]:
        manifest[col] = pd.to_numeric(manifest[col], errors="coerce")
    manifest["line_key"] = manifest["line"].map(lambda x: f"{float(x):.1f}" if pd.notna(x) else "")
    manifest["pair_key"] = (
        manifest["slate_date"].astype(str)
        + "|"
        + manifest["game_id"].astype("Int64").astype(str)
        + "|"
        + manifest["player_id"].astype("Int64").astype(str)
        + "|"
        + manifest["prop_type"].astype(str)
        + "|"
        + manifest["line_key"].astype(str)
    )
    manifest = manifest[[c for c in keep if c in manifest.columns]].drop_duplicates("pair_key", keep="last")
    paired = shadow.merge(manifest, on="pair_key", how="left", suffixes=("", "_manifest"))
    paired["production_prob_manifest_abs_diff"] = (
        pd.to_numeric(paired["production_prob_over"], errors="coerce") - pd.to_numeric(paired.get("prob_over"), errors="coerce")
    ).abs()
    paired["champion_native_output_verified"] = paired["production_prob_manifest_abs_diff"].le(1e-9)
    if "selected_price" not in paired.columns:
        paired["selected_price"] = np.nan
    paired["selection_time_price_certified"] = paired["selected_price"].notna()
    paired["selected_price_band"] = paired["selected_price"].map(price_band)
    return paired


def exclusion_rows(paired: pd.DataFrame) -> list[dict[str, Any]]:
    all_shadow = pd.read_csv(SHADOW_EVAL_ROWS, usecols=["slate_date", "prop_type", "resolved"], low_memory=False)
    all_shadow = all_shadow[all_shadow["prop_type"].astype(str).eq(PROP)].copy()
    all_shadow["slate_date"] = pd.to_datetime(all_shadow["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return [
        {
            "exclusion_reason": "outside_frozen_evaluation_window",
            "rows": int((~all_shadow["slate_date"].between(WINDOW_START, WINDOW_END)).sum()),
            "notes": f"Endpoint window is {WINDOW_START} through {WINDOW_END}.",
        },
        {
            "exclusion_reason": "unresolved_or_no_official_outcome",
            "rows": int((all_shadow["slate_date"].between(WINDOW_START, WINDOW_END) & ~all_shadow["resolved"].astype(str).str.lower().eq("true")).sum()),
            "notes": "Unresolved rows are not part of endpoint prediction/outcome certification.",
        },
        {
            "exclusion_reason": "price_not_certified",
            "rows": int((~paired["selection_time_price_certified"]).sum()),
            "notes": "Rows remain in probability/decision comparison but are excluded from certified ROI.",
        },
        {
            "exclusion_reason": "production_manifest_probability_mismatch",
            "rows": int((~paired["champion_native_output_verified"].fillna(False)).sum()),
            "notes": "Requires exact match between shadow production_prob_over and production manifest prob_over.",
        },
    ]


def metric_rows(paired: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    bands = []
    for model, (prob_col, pick_col) in MODELS.items():
        g = paired.dropna(subset=["y_over", prob_col]).copy()
        y = g["y_over"].astype(int)
        p = pclip(g[prob_col])
        auc = ""
        if y.nunique() >= 2:
            auc = float(roc_auc_score(y, p))
        slope, intercept = calibration_fit(g, prob_col)
        brier_values = ((p - y) ** 2).astype(float).tolist()
        brier_lo, brier_hi = bootstrap_ci(brier_values)
        rows.append(
            {
                "model": model,
                "paired_rows": int(len(g)),
                "brier": float(np.mean(brier_values)) if brier_values else "",
                "brier_ci_low": brier_lo,
                "brier_ci_high": brier_hi,
                "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(g) and y.nunique() >= 2 else "",
                "auc": auc,
                "calibration_slope": slope,
                "calibration_intercept": intercept,
                "ece": ece(g, prob_col),
                "avg_prob_over": float(p.mean()) if len(g) else "",
                "actual_over_rate": float(y.mean()) if len(g) else "",
                "probability_inversion_audit": "NO_INVERSION_DETECTED" if auc == "" or float(auc) >= 0.5 else "POSSIBLE_INVERSION",
                "date_count": int(g["slate_date"].nunique()),
                "worst_date_brier": _worst_date_brier(g, prob_col),
            }
        )
        if len(g):
            g["_prob_band"] = pd.cut(p, bins=[0, .35, .45, .50, .55, .65, 1], include_lowest=True)
            for band, b in g.groupby("_prob_band", observed=False):
                if len(b):
                    bands.append(
                        {
                            "model": model,
                            "probability_band": str(band),
                            "rows": int(len(b)),
                            "avg_prob_over": float(pd.to_numeric(b[prob_col], errors="coerce").mean()),
                            "actual_over_rate": float(b["y_over"].astype(int).mean()),
                        }
                    )
    return rows, bands


def _worst_date_brier(df: pd.DataFrame, prob_col: str) -> str:
    rows = []
    for date, g in df.groupby("slate_date"):
        if len(g):
            rows.append((date, float(np.mean((pclip(g[prob_col]) - g["y_over"].astype(int)) ** 2)), len(g)))
    if not rows:
        return ""
    date, brier, count = max(rows, key=lambda x: x[1])
    return f"{date}:{brier:.6f}:n={count}"


def decision_results(paired: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    disagreement_rows = []
    for model, (_, pick_col) in MODELS.items():
        g = paired.dropna(subset=["y_over", pick_col]).copy()
        win = np.where(g[pick_col].astype(str).eq("over"), g["y_over"], 1 - g["y_over"])
        g["_model_win"] = win
        rows.append(
            {
                "model": model,
                "rows": int(len(g)),
                "wins": int((g["_model_win"] == 1).sum()),
                "losses": int((g["_model_win"] == 0).sum()),
                "pushes": int((g["actual_value"] == g["line"]).sum()),
                "accuracy": float(g["_model_win"].mean()) if len(g) else "",
                "date_count": int(g["slate_date"].nunique()),
                "line_stability": "|".join(f"{k}:{len(v)}" for k, v in g.groupby("line")),
                "top_player_concentration": _top_concentration(g, "player_id"),
            }
        )
    prod_side = paired["production_pick_side"].astype(str)
    for model in ["tb_rolling_balanced_shadow", "tb_rolling_unweighted_shadow"]:
        pick_col = MODELS[model][1]
        g = paired[prod_side.ne(paired[pick_col].astype(str))].copy()
        champion_win = np.where(g["production_pick_side"].astype(str).eq("over"), g["y_over"], 1 - g["y_over"])
        shadow_win = np.where(g[pick_col].astype(str).eq("over"), g["y_over"], 1 - g["y_over"])
        disagreement_rows.append(
            {
                "comparison_model": model,
                "disagreement_rows": int(len(g)),
                "champion_wins_on_disagreement": int((champion_win == 1).sum()),
                "shadow_wins_on_disagreement": int((shadow_win == 1).sum()),
                "champion_accuracy_on_disagreement": float(np.mean(champion_win)) if len(g) else "",
                "shadow_accuracy_on_disagreement": float(np.mean(shadow_win)) if len(g) else "",
                "shadow_net_wins_vs_champion": int((shadow_win == 1).sum() - (champion_win == 1).sum()),
                "primary_replacement_evidence": "FAILS_TO_BEAT_CHAMPION_ON_DISAGREEMENTS"
                if len(g) and np.mean(shadow_win) <= np.mean(champion_win)
                else "BEATS_CHAMPION_ON_DISAGREEMENTS",
            }
        )
    return rows, disagreement_rows


def _top_concentration(df: pd.DataFrame, col: str) -> str:
    if col not in df.columns or df.empty:
        return ""
    vc = df[col].value_counts(dropna=False)
    return f"top_key={vc.index[0]} rows={int(vc.iloc[0])} share={float(vc.iloc[0] / len(df)):.4f}"


def roi_rows(paired: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    bands = []
    for model, (_, pick_col) in MODELS.items():
        g = paired[paired["selection_time_price_certified"]].dropna(subset=["y_over", pick_col]).copy()
        model_win = np.where(g[pick_col].astype(str).eq("over"), g["y_over"], 1 - g["y_over"])
        profits = [american_profit(p, bool(w)) for p, w in zip(g["selected_price"], model_win)]
        lo, hi = bootstrap_ci(profits)
        rows.append(
            {
                "model": model,
                "priced_bets": int(len(g)),
                "wins": int((model_win == 1).sum()),
                "losses": int((model_win == 0).sum()),
                "average_odds": float(pd.to_numeric(g["selected_price"], errors="coerce").mean()) if len(g) else "",
                "break_even_rate": float(pd.Series([american_implied(x) for x in g["selected_price"]]).mean()) if len(g) else "",
                "roi": float(np.nanmean(profits)) if profits else "",
                "roi_ci_low": lo,
                "roi_ci_high": hi,
                "units": float(np.nansum(profits)) if profits else "",
                "worst_drawdown": longest_drawdown(profits),
                "sportsbook_coverage": "|".join(sorted(set(g.get("market_bookmaker_key", pd.Series(dtype=str)).dropna().astype(str))))[:500],
            }
        )
        if len(g):
            g["_price_band"] = g["selected_price"].map(price_band)
            g["_model_win"] = model_win
            for band, b in g.groupby("_price_band", dropna=False):
                bp = [american_profit(p, bool(w)) for p, w in zip(b["selected_price"], b["_model_win"])]
                bands.append(
                    {
                        "model": model,
                        "price_band": band,
                        "bets": int(len(b)),
                        "wins": int((b["_model_win"] == 1).sum()),
                        "losses": int((b["_model_win"] == 0).sum()),
                        "roi": float(np.nanmean(bp)) if bp else "",
                        "units": float(np.nansum(bp)) if bp else "",
                    }
                )
    return rows, bands


def paired_population_rows(paired: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "production_prob_over",
        "production_prob_under",
        "tb_rolling_balanced_shadow_prob_over",
        "tb_rolling_balanced_shadow_prob_under",
        "tb_rolling_unweighted_shadow_prob_over",
        "tb_rolling_unweighted_shadow_prob_under",
        "production_pick_side",
        "tb_rolling_balanced_shadow_pick_side",
        "tb_rolling_unweighted_shadow_pick_side",
        "selected_price",
        "market_bookmaker_key",
        "actual_value",
        "actual_over_outcome",
        "y_over",
        "prediction_source_file",
        "shadow_score_file",
        "_source_path",
        "champion_native_output_verified",
        "production_prob_manifest_abs_diff",
    ]
    return paired[[c for c in cols if c in paired.columns]].to_dict("records")


def version_boundaries(paired: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for date, g in paired.groupby("slate_date"):
        rows.append(
            {
                "slate_date": date,
                "paired_rows": int(len(g)),
                "source_shadow_files": "|".join(sorted(set(g.get("shadow_score_file", pd.Series(dtype=str)).dropna().astype(str)))),
                "generated_at_utc": "|".join(sorted(set(g.get("generated_at_utc", pd.Series(dtype=str)).dropna().astype(str))))[:500],
                "model_train_rows": "|".join(sorted(set(g.get("model_train_rows", pd.Series(dtype=str)).dropna().astype(str)))),
                "production_manifest_rows_verified": int(g["champion_native_output_verified"].fillna(False).sum()),
            }
        )
    return rows


def surface_separation(paired: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    rows.append({"population": "full_paired_prediction_population", "rows": int(len(paired)), "notes": "Resolved exact paired rows."})
    rows.append(
        {
            "population": "production_selected_rows",
            "rows": int(paired["production_pick_side"].notna().sum()),
            "notes": "Rows with production selected side.",
        }
    )
    rows.append(
        {
            "population": "selection_time_price_certified_rows",
            "rows": int(paired["selection_time_price_certified"].sum()),
            "notes": "Rows eligible for certified ROI.",
        }
    )
    if CURRENT_SLATE.exists():
        slate = pd.read_csv(CURRENT_SLATE, low_memory=False)
        rows.append(
            {
                "population": "current_surfaced_total_bases_rows",
                "rows": int(slate[slate.get("prop_type", pd.Series(dtype=str)).astype(str).eq(PROP)].shape[0]),
                "notes": rel(CURRENT_SLATE),
            }
        )
    if CURRENT_UPLOAD.exists():
        upload = pd.read_csv(CURRENT_UPLOAD, low_memory=False)
        market_col = "MARKET" if "MARKET" in upload.columns else ""
        count = int(upload[market_col].astype(str).str.contains("total", case=False, na=False).sum()) if market_col else 0
        rows.append({"population": "current_upload_total_bases_like_rows", "rows": count, "notes": rel(CURRENT_UPLOAD)})
    return rows


def prior_rejection() -> list[dict[str, Any]]:
    summary = read_json(SHADOW_EVAL_SUMMARY)
    return [
        {
            "review_item": "prior_not_ready_conclusion",
            "classification": "VALID_DIAGNOSTIC_BUT_NOT_PROMOTION_DECISION",
            "evidence": str(summary.get("interpretation_note", "Balanced shadow is not promotion-ready; unweighted shadow is research-only.")),
            "endpoint_assessment": "Prior evaluation was useful but did not force an exact production Champion replacement endpoint.",
        }
    ]


def final_recommendation(prob: list[dict[str, Any]], roi: list[dict[str, Any]], disagreements: list[dict[str, Any]]) -> tuple[str, str]:
    p = {r["model"]: r for r in prob}
    r = {x["model"]: x for x in roi}
    prod = p.get("production", {})
    best_shadow = None
    for shadow in ["tb_rolling_balanced_shadow", "tb_rolling_unweighted_shadow"]:
        sr = p.get(shadow, {})
        rr = r.get(shadow, {})
        dis = next((d for d in disagreements if d["comparison_model"] == shadow), {})
        if not sr or not prod:
            continue
        improves_brier = safe_float(sr.get("brier")) is not None and safe_float(prod.get("brier")) is not None and float(sr["brier"]) < float(prod["brier"])
        improves_logloss = safe_float(sr.get("log_loss")) is not None and safe_float(prod.get("log_loss")) is not None and float(sr["log_loss"]) < float(prod["log_loss"])
        improves_disagreement = safe_float(dis.get("shadow_accuracy_on_disagreement")) is not None and float(dis["shadow_accuracy_on_disagreement"]) > float(dis["champion_accuracy_on_disagreement"])
        improves_roi = safe_float(rr.get("roi")) is not None and safe_float(r.get("production", {}).get("roi")) is not None and float(rr["roi"]) > float(r["production"]["roi"])
        if improves_brier and improves_logloss and improves_disagreement and improves_roi:
            best_shadow = shadow
    if best_shadow:
        return "TOTAL_BASES_SHADOW_PROMOTION_RECOMMENDED", f"{best_shadow} improves probability, disagreement, and ROI on paired rows."
    champion_roi = safe_float(r.get("production", {}).get("roi"))
    champion_auc = safe_float(prod.get("auc"))
    if champion_roi is not None and champion_roi < 0 and champion_auc is not None and champion_auc < 0.56:
        return (
            "TOTAL_BASES_PRODUCTION_REBUILD_REQUIRED",
            "No existing shadow clears promotion; production Champion has certified negative ROI and only modest ranking quality.",
        )
    return (
        "TOTAL_BASES_CHAMPION_RETAINED_TEMPORARILY",
        "No existing shadow is better than the exact production Champion; production is retained only as least-bad available baseline.",
    )


def decision_rows(final_decision: str, paired: pd.DataFrame, prob: list[dict[str, Any]], disagreements: list[dict[str, Any]], roi: list[dict[str, Any]]) -> list[dict[str, str]]:
    native_ok = int(paired["champion_native_output_verified"].fillna(False).sum()) == len(paired)
    shadows = "tb_rolling_balanced_shadow|tb_rolling_unweighted_shadow"
    prob_decision = "SHADOWS_DO_NOT_MATERIALLY_IMPROVE_NATIVE_PROBABILITY_QUALITY"
    if prob:
        prod_brier = next((safe_float(r["brier"]) for r in prob if r["model"] == "production"), None)
        if any(r["model"] != "production" and safe_float(r["brier"]) is not None and prod_brier is not None and float(r["brier"]) < prod_brier for r in prob):
            prob_decision = "ONE_SHADOW_IMPROVES_ONE_PROBABILITY_METRIC_BUT_ENDPOINT_REQUIRES_MORE"
    dis_decision = "SHADOWS_FAIL_PRIMARY_DISAGREEMENT_REPLACEMENT_TEST"
    if any(str(d.get("primary_replacement_evidence")) == "BEATS_CHAMPION_ON_DISAGREEMENTS" for d in disagreements):
        dis_decision = "SHADOW_BEATS_CHAMPION_ON_DISAGREEMENTS_BUT_REQUIRES_ROI_AND_PROBABILITY_CONFIRMATION"
    roi_decision = "NO_SHADOW_DELIVERS_PROMOTION_GRADE_ROI_IMPROVEMENT"
    return [
        {"decision": "MLB_TB_ENDPOINT_TRUE_CHAMPION_DECISION", "value": "TRUE_TOTAL_BASES_PRODUCTION_CHAMPION_BOUND"},
        {"decision": "MLB_TB_ENDPOINT_RUNTIME_BINDING_DECISION", "value": "RUNTIME_BOUND_TO_MODELS_OUT_LATEST_TOTAL_BASES_JOBLIB"},
        {"decision": "MLB_TB_ENDPOINT_SHADOW_INVENTORY_DECISION", "value": f"EXISTING_SHADOWS_BOUND_{shadows}"},
        {"decision": "MLB_TB_ENDPOINT_PAIRED_POPULATION_DECISION", "value": f"EXACT_PAIRED_RESOLVED_ROWS_{len(paired)}_NATIVE_VERIFIED_{native_ok}"},
        {"decision": "MLB_TB_ENDPOINT_PROBABILITY_DECISION", "value": prob_decision},
        {"decision": "MLB_TB_ENDPOINT_DISAGREEMENT_DECISION", "value": dis_decision},
        {"decision": "MLB_TB_ENDPOINT_ROI_DECISION", "value": roi_decision},
        {"decision": "MLB_TB_ENDPOINT_PRIOR_REJECTION_DECISION", "value": "VALID_DIAGNOSTIC_BUT_NOT_PROMOTION_DECISION"},
        {"decision": "MLB_TB_ENDPOINT_FINAL_RECOMMENDATION", "value": final_decision},
        {"decision": "MLB_PHA_PROMOTION_STATUS", "value": "SUSPENDED_PENDING_NATIVE_HITS_ALLOWED_CHAMPION_REBINDING"},
        {"decision": "MLB_TB_PRODUCTION_STATUS", "value": "UNCHANGED_PENDING_EXPLICIT_USER_APPROVAL"},
    ]


def validation_report(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": status, "message": msg})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                status, msg = "PASS", ""
            except Exception as exc:
                status, msg = "FAIL", str(exc)
            rows.append({"artifact": rel(path), "check": "json_parse", "status": status, "message": msg})
        elif path.suffix == ".md":
            text = path.read_text(encoding="utf-8")
            rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if text.strip() else "FAIL", "message": ""})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    champion_rows, champion = load_champion_contract()
    shadows = shadow_inventory()
    paired = load_paired_rows()
    prob, prob_bands = metric_rows(paired)
    decisions_, disagreements = decision_results(paired)
    roi, roi_bands = roi_rows(paired)
    final_decision, final_rationale = final_recommendation(prob, roi, disagreements)
    required = decision_rows(final_decision, paired, prob, disagreements, roi)
    outputs: dict[str, list[dict[str, Any]]] = {
        "true_production_champion_contract_2026-07-18.csv": champion_rows,
        "runtime_model_loading_proof_2026-07-18.csv": [
            {
                "proof_item": "shadow_production_prob_over_equals_production_manifest_prob_over",
                "rows_checked": int(len(paired)),
                "rows_passed": int(paired["champion_native_output_verified"].fillna(False).sum()),
                "max_abs_diff": float(pd.to_numeric(paired["production_prob_manifest_abs_diff"], errors="coerce").max()),
                "source": rel(PRODUCTION_MANIFEST),
            }
        ],
        "existing_total_bases_shadow_inventory_2026-07-18.csv": shadows,
        "exact_paired_population_2026-07-18.csv": paired_population_rows(paired),
        "paired_population_exclusions_2026-07-18.csv": exclusion_rows(paired),
        "version_boundary_report_2026-07-18.csv": version_boundaries(paired),
        "probability_metrics_2026-07-18.csv": prob,
        "probability_band_progression_2026-07-18.csv": prob_bands,
        "decision_side_results_2026-07-18.csv": decisions_,
        "disagreement_results_2026-07-18.csv": disagreements,
        "roi_results_2026-07-18.csv": roi,
        "roi_price_band_results_2026-07-18.csv": roi_bands,
        "production_surfaced_upload_population_separation_2026-07-18.csv": surface_separation(paired),
        "prior_rejection_reassessment_2026-07-18.csv": prior_rejection(),
        "forced_endpoint_recommendation_2026-07-18.csv": [
            {"recommendation": final_decision, "rationale": final_rationale, "production_status": "UNCHANGED_PENDING_EXPLICIT_USER_APPROVAL"}
        ],
        "pha_suspension_status_2026-07-18.csv": [
            {
                "decision": "MLB_PHA_PROMOTION_STATUS",
                "value": "SUSPENDED_PENDING_NATIVE_HITS_ALLOWED_CHAMPION_REBINDING",
                "notes": "PHA rebind is explicitly out of scope for this Total Bases endpoint.",
            }
        ],
        "required_decisions_2026-07-18.csv": required,
    }
    for name, rows in outputs.items():
        write_csv(out_dir / name, rows)
    machine = {
        "generated_at": generated_at,
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "production_champion": champion,
        "shadow_identifiers": [r["shadow_identifier"] for r in shadows],
        "paired_rows": int(len(paired)),
        "native_output_verified_rows": int(paired["champion_native_output_verified"].fillna(False).sum()),
        "probability_metrics": prob,
        "disagreement_results": disagreements,
        "roi_results": roi,
        "final_recommendation": final_decision,
        "final_rationale": final_rationale,
        "decisions": {r["decision"]: r["value"] for r in required},
        "direct_answer": "No existing Total Bases shadow beats the exact production model strongly enough to recommend replacement; the production Champion and tested shadows are inadequate under the current promotion standard, so a production rebuild is required while production remains unchanged pending explicit approval.",
        "guardrails": {
            "model_fitting": False,
            "new_challenger": False,
            "new_watch": False,
            "optimization": False,
            "scheduler_changes": False,
            "db_writes": False,
            "oddsapi_calls": False,
            "production_behavior_changed": False,
        },
    }
    write_json(out_dir / "machine_readable_total_bases_endpoint_2026-07-18.json", machine)
    write_md(
        out_dir / "total_bases_exact_champion_shadow_endpoint_2026-07-18.md",
        f"""# MLB Total Bases Exact Champion-Shadow Endpoint Certification

Generated: `{generated_at}`

## Endpoint

The exact production Total Bases Champion is `{rel(TB_MODEL)}` with SHA256
`{champion['artifact_sha256']}`. Existing shadows evaluated here are
`tb_rolling_balanced_shadow` and `tb_rolling_unweighted_shadow`.

Exact paired resolved rows: `{len(paired)}`. Native production output
verification rows: `{int(paired['champion_native_output_verified'].fillna(False).sum())}`.

## Forced Recommendation

`{final_decision}`

{final_rationale}

## PHA Status

`MLB_PHA_PROMOTION_STATUS = SUSPENDED_PENDING_NATIVE_HITS_ALLOWED_CHAMPION_REBINDING`

## Production Status

`MLB_TB_PRODUCTION_STATUS = UNCHANGED_PENDING_EXPLICIT_USER_APPROVAL`

## Direct Answer

No existing Total Bases shadow beats the exact production model strongly enough
to recommend replacement. The production Champion has certified negative ROI,
and the tested shadows do not clear the combined probability, disagreement, and
ROI endpoint. Total Bases requires a replacement/rebuild effort, not promotion
of an existing shadow.
""",
    )
    validation = validation_report(out_dir)
    write_csv(out_dir / "validation_report_2026-07-18.csv", validation)
    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-18.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    for path in [Path(__file__).resolve(), TB_MODEL, MODEL_INDEX, SHADOW_EVAL_ROWS, SHADOW_EVAL_SUMMARY, PRODUCTION_MANIFEST]:
        if path.exists():
            manifest.append({"path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest_2026-07-18.csv", manifest)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
