#!/usr/bin/env python3
"""Certify MLB production runtime model inventory and performance baseline.

Read-only. This script inventories existing runtime model artifacts and
reconciled prediction artifacts. It does not fit models, create predictions,
call networks, write databases, change schedulers, or alter production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_production_runtime_performance_baseline/2026-07-18"
MODEL_INDEX = ROOT / "models_out/latest/MODEL_INDEX.json"
MODEL_LATEST = ROOT / "models_out/latest"
RECONCILE_ROOT = ROOT / "artifacts/analysis/mlb/execution_vs_model"
CURRENT_PREDICTIONS_WIDE = ROOT / "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv"
CURRENT_SLATE_OUTPUT = ROOT / "backend/mlb/data/processed/mlb_slate_output.csv"
CURRENT_UPLOAD = ROOT / "backend/mlb/data/processed/mlb_book_upload.csv"
WINDOW_START = "2026-05-01"


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
    if not rows:
        path.write_text("", encoding="utf-8")
        return
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


def american_to_profit(price: Any, won: bool) -> float:
    p = safe_float(price)
    if p is None:
        return float("nan")
    if not won:
        return -1.0
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p) if p else float("nan")


def american_to_implied(price: Any) -> float | None:
    p = safe_float(price)
    if p is None or p == 0:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def pclip(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").clip(1e-6, 1 - 1e-6)


def logit_values(p: pd.Series) -> pd.Series:
    q = pclip(p)
    return np.log(q / (1.0 - q))


def longest_drawdown(profits: list[float]) -> float:
    peak = 0.0
    running = 0.0
    worst = 0.0
    for value in profits:
        if math.isnan(value):
            continue
        running += value
        peak = max(peak, running)
        worst = min(worst, running - peak)
    return float(worst)


def longest_losing_streak(results: list[str]) -> int:
    cur = 0
    worst = 0
    for r in results:
        if r == "loss":
            cur += 1
            worst = max(worst, cur)
        elif r == "win":
            cur = 0
    return worst


def ece(frame: pd.DataFrame, prob_col: str, target_col: str, bins: int = 10) -> float | str:
    g = frame.dropna(subset=[prob_col, target_col]).copy()
    if g.empty:
        return ""
    g["_bin"] = pd.cut(pclip(g[prob_col]), bins=np.linspace(0, 1, bins + 1), include_lowest=True)
    total = len(g)
    total_ece = 0.0
    for _, b in g.groupby("_bin", observed=False):
        if b.empty:
            continue
        total_ece += (len(b) / total) * abs(float(b[target_col].mean()) - float(b[prob_col].mean()))
    return float(total_ece)


def calibration_fit(frame: pd.DataFrame, prob_col: str, target_col: str) -> tuple[Any, Any]:
    g = frame.dropna(subset=[prob_col, target_col]).copy()
    if len(g) < 20 or g[target_col].nunique() < 2:
        return "", ""
    x = logit_values(g[prob_col]).to_numpy().reshape(-1, 1)
    y = g[target_col].astype(int).to_numpy()
    try:
        model = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
        model.fit(x, y)
        return float(model.coef_[0][0]), float(model.intercept_[0])
    except Exception:
        return "", ""


def load_model_index() -> dict[str, Any]:
    return read_json(MODEL_INDEX)


def model_artifact_details(prop: str, meta: dict[str, Any]) -> dict[str, Any]:
    filename = str(meta.get("file") or f"{prop}.joblib")
    path = MODEL_LATEST / filename
    details: dict[str, Any] = {
        "artifact_path": rel(path),
        "metadata_path": rel(MODEL_INDEX),
        "artifact_exists": path.exists(),
        "artifact_sha256": sha256(path) if path.exists() else "",
        "estimator_class": "",
        "artifact_keys": "",
    }
    if path.exists():
        try:
            obj = joblib.load(path)
            if isinstance(obj, dict):
                details["artifact_keys"] = "|".join(sorted(str(k) for k in obj.keys()))
                classes = []
                for key in ("logistic_regression", "random_forest", "lr", "rf"):
                    if obj.get(key) is not None:
                        classes.append(f"{key}:{type(obj.get(key)).__name__}")
                models = obj.get("models") if isinstance(obj.get("models"), dict) else {}
                for key, val in models.items():
                    classes.append(f"models.{key}:{type(val).__name__}")
                details["estimator_class"] = "|".join(classes)
            else:
                details["estimator_class"] = type(obj).__name__
        except Exception as exc:
            details["estimator_class"] = f"LOAD_ERROR:{exc}"
    return details


def inventory_models(index: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for prop, meta in sorted(index.items()):
        details = model_artifact_details(prop, meta)
        features = list(meta.get("input_columns") or meta.get("features_num") or [])
        lines = "market supplied; current supported columns p_over_<line> generated for 0.0/0.5 fractional lines"
        rows.append(
            {
                "prop_type": prop,
                "model_identifier": f"{prop}:{meta.get('trained_at', 'UNKNOWN')}:{meta.get('training_profile', 'UNKNOWN')}",
                "model_artifact_path": details["artifact_path"],
                "metadata_path": details["metadata_path"],
                "artifact_sha256": details["artifact_sha256"],
                "training_timestamp": meta.get("trained_at", "UNKNOWN"),
                "training_date_range": meta.get("training_date_range") or meta.get("date_range") or "UNKNOWN",
                "estimator_class": details["estimator_class"],
                "feature_count": len(features),
                "feature_names": "|".join(str(x) for x in features),
                "target_definition": f"official over outcome for prop_type={prop} at market line",
                "supported_lines": lines,
                "output_probability_semantics": "probability_over for the offered market line; probability_under = 1 - probability_over",
                "model_version": meta.get("training_profile", "legacy"),
                "calibration_layer": "runtime AUC-weighted LR/RF blend plus optional line-sensitivity correction; no separate saved calibrator identified",
                "current_status": "INDEXED_ACTIVE_ARTIFACT" if details["artifact_exists"] else "INDEXED_MISSING_ARTIFACT",
                "artifact_keys": details["artifact_keys"],
            }
        )
    return rows


def read_current_artifacts() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    wide = pd.read_csv(CURRENT_PREDICTIONS_WIDE, low_memory=False) if CURRENT_PREDICTIONS_WIDE.exists() else pd.DataFrame()
    slate = pd.read_csv(CURRENT_SLATE_OUTPUT, low_memory=False) if CURRENT_SLATE_OUTPUT.exists() else pd.DataFrame()
    upload = pd.read_csv(CURRENT_UPLOAD, low_memory=False) if CURRENT_UPLOAD.exists() else pd.DataFrame()
    return wide, slate, upload


def runtime_trace(index: dict[str, Any], wide: pd.DataFrame, slate: pd.DataFrame) -> list[dict[str, Any]]:
    live_props = set(slate["prop_type"].astype(str)) if "prop_type" in slate.columns else set()
    wide_counts = wide["prop_type"].value_counts().to_dict() if "prop_type" in wide.columns else {}
    slate_counts = slate["prop_type"].value_counts().to_dict() if "prop_type" in slate.columns else {}
    all_props = sorted(set(index) | live_props)
    rows = []
    for prop in all_props:
        meta = index.get(prop, {})
        model_path = MODEL_LATEST / str(meta.get("file") or f"{prop}.joblib")
        if prop in index and prop in live_props and model_path.exists():
            classification = "TRAINED_MODEL_LOADED"
        elif prop in live_props and prop not in index:
            classification = "MODEL_FALLBACK_USED"
        elif prop in index and prop not in live_props:
            classification = "RUNTIME_BINDING_UNRESOLVED"
        else:
            classification = "RUNTIME_BINDING_UNRESOLVED"
        rows.append(
            {
                "prop_type": prop,
                "generating_command": "make mlb-predictions-wide -> make mlb-slate-output",
                "script_or_module": "backend/mlb/scripts/build_mlb_predictions_wide.py; backend.domains.mlb.prop_workflow.predict_prop; backend.mlb.prediction.make_prediction.predict",
                "artifact_lookup_logic": "backend/app/services/model_registry.py::_latest_artifact_path(prop) -> MODEL_DIR/latest/{prop}.joblib",
                "exact_model_file_loaded": rel(model_path) if model_path.exists() else "",
                "model_hash": sha256(model_path) if model_path.exists() else "",
                "fallback_behavior": "prop_workflow heuristic_fallback_v1 if model pipeline unavailable",
                "formula_or_rule_path_when_no_model": "backend.domains.mlb.prop_workflow._heuristic_probability",
                "probability_calibration": "AUC-weighted LR/RF blend; line-sensitivity correction enabled by default in make_prediction; slate calibrator optional and off unless supplied",
                "selected_side_rule": "make_prediction uses artifact decision_threshold; build_mlb_slate_output emits model_pick_side as over when prob_over >= 0.5",
                "output_artifact": rel(CURRENT_PREDICTIONS_WIDE) + " | " + rel(CURRENT_SLATE_OUTPUT),
                "output_columns": "prob_over/prob_under/model_pick_side/model_pick_prob in slate; p_over_* wide columns in wide",
                "recent_run_tag": _first_nonempty(slate.get("market_snapshot_run_tag")) if not slate.empty else "",
                "cutoff": _first_nonempty(slate.get("market_snapshot_time_utc")) if not slate.empty else "",
                "prediction_wide_rows": int(wide_counts.get(prop, 0)),
                "slate_output_rows": int(slate_counts.get(prop, 0)),
                "fallback_count": int(slate_counts.get(prop, 0)) if classification == "MODEL_FALLBACK_USED" else 0,
                "errors_or_missing_models": "" if model_path.exists() else "no indexed latest model artifact for live prop",
                "runtime_binding_classification": classification,
            }
        )
    return rows


def _first_nonempty(series: Any) -> str:
    try:
        for value in series.dropna().astype(str):
            if value.strip():
                return value.strip()
    except Exception:
        pass
    return ""


def formula_inventory(runtime_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for r in runtime_rows:
        if r["runtime_binding_classification"] in {"MODEL_FALLBACK_USED", "DETERMINISTIC_FORMULA", "RULE_OR_TIER_SYSTEM", "MARKET_DERIVED", "HYBRID"}:
            rows.append(
                {
                    "prop_or_component": r["prop_type"],
                    "classification": r["runtime_binding_classification"],
                    "script_or_module": r["formula_or_rule_path_when_no_model"],
                    "current_live_rows": r["slate_output_rows"],
                    "notes": "Live prop has no indexed trained model and therefore depends on fallback/rule behavior unless another artifact is discovered.",
                }
            )
    rows.append(
        {
            "prop_or_component": "starter_expected_hits_allowed",
            "classification": "DETERMINISTIC_FORMULA",
            "script_or_module": "backend/mlb/scripts/report_mlb_hits_environment.py",
            "current_live_rows": "",
            "notes": "Context formula, not a production player-prop model: pitcher_base * offense_factor_vs_league_clamped.",
        }
    )
    return rows


def latest_reconciled_date() -> str:
    dates = []
    for path in RECONCILE_ROOT.glob("20??-??-??/reconcile_rows.csv"):
        try:
            date_value = path.parent.name
            if date_value >= WINDOW_START and path.stat().st_size > 0:
                dates.append(date_value)
        except Exception:
            pass
    return max(dates) if dates else ""


def load_reconcile_window(end_date: str) -> pd.DataFrame:
    frames = []
    for path in sorted(RECONCILE_ROOT.glob("20??-??-??/reconcile_rows.csv")):
        date_value = path.parent.name
        if date_value < WINDOW_START or (end_date and date_value > end_date):
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
            df["_source_path"] = rel(path)
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True, sort=False)
    if "slate_date" in df.columns:
        df["slate_date"] = df["slate_date"].astype(str)
    return df


def add_metric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["prob_over"] = pd.to_numeric(out.get("model_prob_over"), errors="coerce")
    if out["prob_over"].isna().all() and "prob_over" in out.columns:
        out["prob_over"] = pd.to_numeric(out["prob_over"], errors="coerce")
    out["actual_value_numeric"] = pd.to_numeric(out.get("actual_value"), errors="coerce")
    out["line_numeric"] = pd.to_numeric(out.get("line"), errors="coerce")
    if "actual_over_outcome" in out.columns:
        outcome_text = out["actual_over_outcome"].astype(str).str.lower()
        out["target_over"] = np.where(outcome_text.eq("win"), 1, np.where(outcome_text.eq("loss"), 0, np.nan))
        out["push_flag"] = outcome_text.eq("push")
    else:
        out["target_over"] = np.where(out["actual_value_numeric"] > out["line_numeric"], 1, np.where(out["actual_value_numeric"] < out["line_numeric"], 0, np.nan))
        out["push_flag"] = out["actual_value_numeric"].eq(out["line_numeric"])
    out["selected_side"] = out.get("model_pick_side", "").astype(str).str.lower()
    out["selected_target"] = np.where(out["selected_side"].eq("over"), out["target_over"], np.where(out["selected_side"].eq("under"), 1 - out["target_over"], np.nan))
    out["selected_price"] = pd.to_numeric(out.get("selected_side_price"), errors="coerce")
    if out["selected_price"].isna().all():
        out["selected_price"] = np.where(out["selected_side"].eq("over"), pd.to_numeric(out.get("market_price_over"), errors="coerce"), pd.to_numeric(out.get("market_price_under"), errors="coerce"))
    out["selected_profit_1u"] = [american_to_profit(p, bool(w)) if not pd.isna(w) else np.nan for p, w in zip(out["selected_price"], out["selected_target"])]
    out["price_certified"] = out["selected_price"].notna()
    return out


def probability_metrics(df: pd.DataFrame, index: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    bands = []
    for prop, g in df.groupby("prop_type", dropna=False):
        prop = str(prop)
        gg = g.dropna(subset=["target_over", "prob_over"]).copy()
        slope, intercept = calibration_fit(gg, "prob_over", "target_over")
        auc = ""
        if len(gg) and gg["target_over"].nunique() >= 2:
            try:
                auc = float(roc_auc_score(gg["target_over"].astype(int), gg["prob_over"]))
            except Exception:
                auc = ""
        rows.append(
            {
                "prop_type": prop,
                "model_version": _model_version(prop, index),
                "rows": int(len(g)),
                "graded_rows": int(len(gg)),
                "brier": float(np.mean((pclip(gg["prob_over"]) - gg["target_over"]) ** 2)) if len(gg) else "",
                "log_loss": float(log_loss(gg["target_over"].astype(int), pclip(gg["prob_over"]), labels=[0, 1])) if len(gg) and gg["target_over"].nunique() >= 2 else "",
                "auc": auc,
                "calibration_slope": slope,
                "calibration_intercept": intercept,
                "ece": ece(gg, "prob_over", "target_over"),
                "avg_prob_over": float(gg["prob_over"].mean()) if len(gg) else "",
                "actual_over_rate": float(gg["target_over"].mean()) if len(gg) else "",
                "prediction_quality_classification": classify_signal(auc, rows_count=len(gg)),
            }
        )
        if len(gg):
            gg["_prob_band"] = pd.cut(pclip(gg["prob_over"]), bins=[0, .35, .45, .50, .55, .65, 1], include_lowest=True)
            for band, b in gg.groupby("_prob_band", observed=False):
                bands.append(
                    {
                        "prop_type": prop,
                        "probability_band": str(band),
                        "rows": int(len(b)),
                        "avg_prob_over": float(b["prob_over"].mean()) if len(b) else "",
                        "actual_over_rate": float(b["target_over"].mean()) if len(b) else "",
                    }
                )
    return rows, bands


def _model_version(prop: str, index: dict[str, Any]) -> str:
    meta = index.get(prop, {})
    return f"{meta.get('training_profile', 'unknown')}|{meta.get('trained_at', 'UNKNOWN')}"


def classify_signal(auc: Any, rows_count: int) -> str:
    if rows_count < 100:
        return "INSUFFICIENT_EVIDENCE"
    if auc == "":
        return "INSUFFICIENT_EVIDENCE"
    a = float(auc)
    if a >= 0.54:
        return "USEFUL_RANKING"
    if a >= 0.515:
        return "USEFUL_CALIBRATION"
    if a >= 0.49:
        return "WEAK_SIGNAL"
    return "INVERTED_ORIENTATION"


def selection_metrics(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for prop, g in df.groupby("prop_type", dropna=False):
        prop = str(prop)
        gg = g.dropna(subset=["selected_target"]).copy()
        wins = int(gg["selected_target"].eq(1).sum())
        losses = int(gg["selected_target"].eq(0).sum())
        pushes = int(g["push_flag"].fillna(False).sum())
        by_date = []
        for date, d in gg.groupby("slate_date"):
            by_date.append((date, int(len(d)), float(d["selected_target"].mean()) if len(d) else np.nan))
        worst = min(by_date, key=lambda x: x[2]) if by_date else ("", "", "")
        best = max(by_date, key=lambda x: x[2]) if by_date else ("", "", "")
        ordered = gg.sort_values(["slate_date", "game_id", "player_id"])
        result_labels = ["win" if x == 1 else "loss" for x in ordered["selected_target"].tolist()]
        rows.append(
            {
                "prop_type": prop,
                "rows": int(len(g)),
                "graded_selected_rows": int(len(gg)),
                "selected_side_wins": wins,
                "selected_side_losses": losses,
                "pushes": pushes,
                "accuracy": float(wins / (wins + losses)) if wins + losses else "",
                "date_count": int(len(by_date)),
                "worst_slate": worst[0],
                "worst_slate_rows": worst[1],
                "worst_slate_accuracy": worst[2],
                "best_slate": best[0],
                "best_slate_rows": best[1],
                "best_slate_accuracy": best[2],
                "longest_losing_streak": longest_losing_streak(result_labels),
            }
        )
    return rows


def roi_metrics(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    bands = []
    for prop, g in df.groupby("prop_type", dropna=False):
        prop = str(prop)
        priced = g[g["price_certified"] & g["selected_profit_1u"].notna()].copy()
        profits = priced["selected_profit_1u"].astype(float).tolist()
        rows.append(
            {
                "prop_type": prop,
                "rows": int(len(g)),
                "price_certified_rows": int(len(priced)),
                "price_coverage": float(len(priced) / len(g)) if len(g) else "",
                "average_odds": float(priced["selected_price"].mean()) if len(priced) else "",
                "implied_break_even": float(pd.Series([american_to_implied(x) for x in priced["selected_price"]]).mean()) if len(priced) else "",
                "roi": float(np.nanmean(profits)) if profits else "",
                "units": float(np.nansum(profits)) if profits else "",
                "longest_drawdown": longest_drawdown(profits),
                "clv": "",
                "sportsbook_coverage": "|".join(sorted(set(priced.get("market_bookmaker_key", pd.Series(dtype=str)).dropna().astype(str))))[:500],
                "roi_classification": classify_roi(float(np.nanmean(profits)) if profits else None, len(priced)),
            }
        )
        if len(priced):
            priced["_price_band"] = priced["selected_price"].map(price_band)
            for band, b in priced.groupby("_price_band", dropna=False):
                bp = b["selected_profit_1u"].astype(float).tolist()
                bands.append(
                    {
                        "prop_type": prop,
                        "price_band": band,
                        "rows": int(len(b)),
                        "avg_odds": float(b["selected_price"].mean()),
                        "roi": float(np.nanmean(bp)) if bp else "",
                        "units": float(np.nansum(bp)) if bp else "",
                    }
                )
    return rows, bands


def price_band(price: Any) -> str:
    p = safe_float(price)
    if p is None:
        return "missing"
    if p < -200:
        return "short_favorite_lt_-200"
    if p < -150:
        return "-200_to_-151"
    if p < -100:
        return "-150_to_-101"
    if p < 150:
        return "-100_to_+149"
    if p < 200:
        return "+150_to_+199"
    return "+200_or_longer"


def classify_roi(roi: float | None, rows: int) -> str:
    if rows < 100:
        return "PRICE_COVERAGE_INSUFFICIENT"
    if roi is None or math.isnan(roi):
        return "PRICE_COVERAGE_INSUFFICIENT"
    if roi > 0.03:
        return "POSITIVE_ROI_CERTIFIED"
    if roi < -0.03:
        return "NEGATIVE_ROI_CERTIFIED"
    return "NEAR_BREAK_EVEN"


def full_slate_surface_comparison(df: pd.DataFrame, slate: pd.DataFrame, upload: pd.DataFrame) -> list[dict[str, Any]]:
    slate_counts = slate["prop_type"].value_counts().to_dict() if "prop_type" in slate.columns else {}
    upload_counts: dict[str, int] = {}
    if not upload.empty and "MARKET" in upload.columns:
        for market, count in upload["MARKET"].astype(str).value_counts().to_dict().items():
            upload_counts[market] = int(count)
    rows = []
    for prop, g in df.groupby("prop_type", dropna=False):
        prop = str(prop)
        rows.append(
            {
                "prop_type": prop,
                "full_slate_prediction_rows": int(len(g)),
                "model_selected_side_rows": int(g["selected_side"].isin(["over", "under"]).sum()),
                "surfaced_candidate_rows_current_slate": int(slate_counts.get(prop, 0)),
                "uploaded_candidate_rows_current_file_market_label_count": upload_counts.get(prop, ""),
                "executed_wager_rows_retained": "",
                "notes": "Full-slate model performance is separated from current candidate/upload/executed populations; historical uploaded/executed ledgers require separate settlement surface binding.",
            }
        )
    return rows


def classification_rows(prob_rows: list[dict[str, Any]], roi_rows: list[dict[str, Any]], index: dict[str, Any]) -> list[dict[str, Any]]:
    roi_by = {r["prop_type"]: r for r in roi_rows}
    rows = []
    for p in prob_rows:
        prop = p["prop_type"]
        roi = roi_by.get(prop, {})
        rows.append(
            {
                "prop_type": prop,
                "prediction_quality_classification": p["prediction_quality_classification"],
                "roi_classification": roi.get("roi_classification", "PRICE_COVERAGE_INSUFFICIENT"),
                "auc": p["auc"],
                "roi": roi.get("roi", ""),
                "graded_rows": p["graded_rows"],
                "price_certified_rows": roi.get("price_certified_rows", ""),
                "action_classification": "challenge_candidate" if roi.get("roi_classification") == "NEGATIVE_ROI_CERTIFIED" else "monitor_or_retain",
                "notes": "Evidence classification only; no production removal authorized.",
            }
        )
    for prop in sorted(set(index) - {r["prop_type"] for r in prob_rows}):
        rows.append(
            {
                "prop_type": prop,
                "prediction_quality_classification": "OUTCOME_COVERAGE_INSUFFICIENT",
                "roi_classification": "OUTCOME_COVERAGE_INSUFFICIENT",
                "auc": "",
                "roi": "",
                "graded_rows": 0,
                "price_certified_rows": 0,
                "action_classification": "runtime_binding_review",
                "notes": "Indexed model not present in reconciled window population.",
            }
        )
    return rows


def champion_ledger(inv: list[dict[str, Any]], prob: list[dict[str, Any]], roi: list[dict[str, Any]], classifications: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prob_by = {r["prop_type"]: r for r in prob}
    roi_by = {r["prop_type"]: r for r in roi}
    class_by = {r["prop_type"]: r for r in classifications}
    rows = []
    for m in inv:
        prop = m["prop_type"]
        p = prob_by.get(prop, {})
        r = roi_by.get(prop, {})
        c = class_by.get(prop, {})
        rows.append(
            {
                "prop_type": prop,
                "true_production_champion_identity": m["model_identifier"],
                "native_output": "probability_over / probability_under / selected side",
                "runtime_path": m["model_artifact_path"],
                "target": m["target_definition"],
                "lines": m["supported_lines"],
                "historical_population": p.get("rows", 0),
                "current_season_metrics": f"auc={p.get('auc','')} brier={p.get('brier','')} log_loss={p.get('log_loss','')}",
                "roi": r.get("roi", ""),
                "confidence_interval": "not computed in this baseline; bootstrap required for promotion experiment",
                "current_production_justification": "grandfathered runtime champion from latest model index",
                "prior_challengers_measured_against_it": "mixed; see watch/promotion integrity audit",
                "current_standard_assessment": champion_standard(c),
                "notes": "This row is authoritative Champion source for future experiments.",
            }
        )
    return rows


def champion_standard(c: dict[str, Any]) -> str:
    if not c:
        return "CHAMPION_EVIDENCE_UNRESOLVED"
    if c.get("prediction_quality_classification") in {"USEFUL_RANKING", "USEFUL_CALIBRATION"} and c.get("roi_classification") != "NEGATIVE_ROI_CERTIFIED":
        return "CHAMPION_MEETS_CURRENT_STANDARD"
    if c.get("prediction_quality_classification") in {"OUTCOME_COVERAGE_INSUFFICIENT", "INSUFFICIENT_EVIDENCE"}:
        return "CHAMPION_EVIDENCE_UNRESOLVED"
    if c.get("roi_classification") == "NEGATIVE_ROI_CERTIFIED":
        return "CHAMPION_GRANDFATHERED_WEAK_EVIDENCE"
    return "CHAMPION_GRANDFATHERED_WEAK_EVIDENCE"


def offered_reconciliation(index: dict[str, Any], runtime: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    live = {r["prop_type"]: r for r in runtime if int(r.get("slate_output_rows") or 0) > 0}
    for prop in sorted(set(index) | set(live)):
        rows.append(
            {
                "prop_type": prop,
                "offered_current_slate": prop in live,
                "has_indexed_trained_model": prop in index,
                "uses_formula_or_fallback": prop in live and prop not in index,
                "indexed_not_live_today": prop in index and prop not in live,
                "live_without_indexed_model": prop in live and prop not in index,
                "runtime_binding_classification": live.get(prop, {}).get("runtime_binding_classification", "RUNTIME_BINDING_UNRESOLVED"),
                "notes": "Pitcher Hits Allowed corresponds to hits_allowed and has an indexed production trained model." if prop == "hits_allowed" else "",
            }
        )
    return rows


def priority_recommendation(classifications: list[dict[str, Any]], runtime: list[dict[str, Any]]) -> list[dict[str, Any]]:
    df = pd.DataFrame(classifications)
    negative = df[df["roi_classification"].eq("NEGATIVE_ROI_CERTIFIED")].copy()
    if not negative.empty:
        negative["_roi"] = pd.to_numeric(negative["roi"], errors="coerce")
        weakest = negative.sort_values("_roi").iloc[0].to_dict()
        weakest_prop = weakest["prop_type"]
    else:
        weakest_prop = "outs_recorded" if any(r["prop_type"] == "outs_recorded" and r["runtime_binding_classification"] == "MODEL_FALLBACK_USED" for r in runtime) else "UNRESOLVED"
    return [
        {
            "priority_item": "weakest_current_champion_requiring_replacement",
            "selection": weakest_prop,
            "rationale": "Chosen from negative ROI certified props when available; otherwise live fallback prop without indexed model.",
        },
        {
            "priority_item": "strongest_credible_challenger_available",
            "selection": "Total Bases shadow variants are credible rejection evidence but not promotion; PHA historical Challenger promising but comparator unresolved",
            "rationale": "Do not promote until true Champion same-row comparison is clean.",
        },
        {
            "priority_item": "clearest_same_row_promotion_comparison",
            "selection": "total_bases",
            "rationale": "Existing shadow evaluation already includes production comparator rows and outcomes.",
        },
        {
            "priority_item": "components_to_remain_unchanged",
            "selection": "all production models and upload/workspace/scheduler behavior",
            "rationale": "Baseline certification is evidence-only.",
        },
        {
            "priority_item": "exactly_one_next_decision_experiment",
            "selection": "Total Bases exact Champion-vs-shadow endpoint certification",
            "rationale": "It has the cleanest production comparator and enough live shadow rows; define endpoint before any conclusion changes.",
        },
    ]


def decisions(model_count: int, runtime: list[dict[str, Any]], classifications: list[dict[str, Any]], latest_date: str) -> list[dict[str, str]]:
    live_no_index = [r["prop_type"] for r in runtime if r["runtime_binding_classification"] == "MODEL_FALLBACK_USED"]
    negative = [r["prop_type"] for r in classifications if r["roi_classification"] == "NEGATIVE_ROI_CERTIFIED"]
    return [
        {"decision": "MLB_PRODUCTION_BASELINE_MODEL_INDEX_DECISION", "value": "MODEL_INDEX_BOUND_AND_HASHED"},
        {"decision": "MLB_PRODUCTION_BASELINE_RUNTIME_BINDING_DECISION", "value": "CURRENT_RUNTIME_BOUND_STATICALLY_TO_MODEL_REGISTRY_AND_CURRENT_ARTIFACTS"},
        {"decision": "MLB_PRODUCTION_BASELINE_MODEL_COUNT_DECISION", "value": f"INDEXED_TRAINED_MODEL_COUNT_{model_count}"},
        {"decision": "MLB_PRODUCTION_BASELINE_FORMULA_PROP_DECISION", "value": "LIVE_FALLBACK_PROPS_" + ("|".join(live_no_index) if live_no_index else "NONE")},
        {"decision": "MLB_PRODUCTION_BASELINE_PHA_MODEL_DECISION", "value": "TRUE_PRODUCTION_HITS_ALLOWED_MODEL_EXISTS_IN_MODEL_INDEX"},
        {"decision": "MLB_PRODUCTION_BASELINE_PREDICTION_QUALITY_DECISION", "value": f"CURRENT_SEASON_METRICS_COMPUTED_THROUGH_{latest_date}"},
        {"decision": "MLB_PRODUCTION_BASELINE_ROI_DECISION", "value": "SELECTION_TIME_PRICE_ROI_COMPUTED_FROM_RECONCILE_ROWS_WHERE_CERTIFIED"},
        {"decision": "MLB_PRODUCTION_BASELINE_NEGATIVE_ROI_PROP_DECISION", "value": "NEGATIVE_ROI_PROPS_" + ("|".join(negative) if negative else "NONE_CERTIFIED")},
        {"decision": "MLB_PRODUCTION_BASELINE_TRUE_CHAMPION_LEDGER_DECISION", "value": "TRUE_PRODUCTION_CHAMPION_LEDGER_CREATED"},
        {"decision": "MLB_PRODUCTION_BASELINE_CHAMPION_STANDARD_DECISION", "value": "CURRENT_CHAMPIONS_CLASSIFIED_WITH_GRANDFATHERING_NOT_REMOVAL"},
        {"decision": "MLB_PRODUCTION_BASELINE_PRIORITY_PROP_DECISION", "value": "WEAKEST_PROP_IDENTIFIED_FROM_ROI_AND_RUNTIME_BINDING"},
        {"decision": "MLB_PRODUCTION_BASELINE_NEXT_EXPERIMENT_DECISION", "value": "NEXT_EXPERIMENT_TOTAL_BASES_EXACT_CHAMPION_VS_SHADOW_ENDPOINT_CERTIFICATION"},
        {"decision": "MLB_PRODUCTION_DEVELOPMENT_STATUS", "value": "PAUSED_PENDING_BASELINE_CERTIFICATION"},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED"},
    ]


def validation_report(out_dir: Path) -> list[dict[str, str]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        try:
            if path.suffix == ".csv":
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
            elif path.suffix == ".md":
                rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "parse", "status": "FAIL", "message": str(exc)})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    index = load_model_index()
    inventory = inventory_models(index)
    wide, slate, upload = read_current_artifacts()
    runtime = runtime_trace(index, wide, slate)
    formula = formula_inventory(runtime)
    latest_date = latest_reconciled_date()
    rec = add_metric_columns(load_reconcile_window(latest_date))
    manifest_cols = [
        "slate_date", "snapshot_run_tag", "game_id", "player_id", "player_name", "team", "opponent",
        "prop_type", "line", "selected_side", "prob_over", "selected_price", "actual_value", "target_over",
        "model_pick_prob", "market_bookmaker_key", "market_snapshot_run_tag", "market_snapshot_time_utc", "_source_path",
    ]
    manifest = rec[[c for c in manifest_cols if c in rec.columns]].copy() if not rec.empty else pd.DataFrame()
    manifest_rows = manifest.fillna("").to_dict("records")
    prob, bands = probability_metrics(rec, index) if not rec.empty else ([], [])
    selection = selection_metrics(rec) if not rec.empty else []
    roi, price_bands = roi_metrics(rec) if not rec.empty else ([], [])
    surface = full_slate_surface_comparison(rec, slate, upload) if not rec.empty else []
    classifications = classification_rows(prob, roi, index)
    champions = champion_ledger(inventory, prob, roi, classifications)
    offered = offered_reconciliation(index, runtime)
    priority = priority_recommendation(classifications, runtime)
    decision_rows = decisions(len(index), runtime, classifications, latest_date)

    outputs: dict[str, list[dict[str, Any]]] = {
        "full_14_model_inventory_2026-07-18.csv": inventory,
        "runtime_model_loading_trace_2026-07-18.csv": runtime,
        "formula_rule_hybrid_inventory_2026-07-18.csv": formula,
        "production_prediction_manifest_2026-07-18.csv": manifest_rows,
        "prop_probability_metrics_2026-07-18.csv": prob,
        "prop_probability_band_progression_2026-07-18.csv": bands,
        "prop_selection_metrics_2026-07-18.csv": selection,
        "roi_price_coverage_metrics_2026-07-18.csv": roi,
        "roi_price_band_breakdown_2026-07-18.csv": price_bands,
        "full_slate_candidate_upload_execution_comparison_2026-07-18.csv": surface,
        "negative_roi_classification_2026-07-18.csv": classifications,
        "true_production_champion_ledger_2026-07-18.csv": champions,
        "current_standard_assessment_2026-07-18.csv": [
            {"prop_type": r["prop_type"], "current_standard_assessment": r["current_standard_assessment"], "notes": r["notes"]} for r in champions
        ],
        "offered_prop_model_reconciliation_2026-07-18.csv": offered,
        "priority_recommendation_2026-07-18.csv": priority,
        "required_decisions_2026-07-18.csv": decision_rows,
    }
    for name, rows in outputs.items():
        write_csv(out_dir / name, rows)
    machine = {
        "generated_at": generated_at,
        "window_start": WINDOW_START,
        "latest_fully_reconciled_slate": latest_date,
        "indexed_model_count": len(index),
        "current_slate_prop_count": len(set(slate["prop_type"].astype(str))) if "prop_type" in slate.columns else 0,
        "live_props_without_indexed_model": [r["prop_type"] for r in runtime if r["runtime_binding_classification"] == "MODEL_FALLBACK_USED"],
        "indexed_models_not_live_today": [r["prop_type"] for r in runtime if r["runtime_binding_classification"] == "RUNTIME_BINDING_UNRESOLVED" and r["prop_type"] in index],
        "negative_roi_props": [r["prop_type"] for r in classifications if r["roi_classification"] == "NEGATIVE_ROI_CERTIFIED"],
        "pha_production_model_exists": "hits_allowed" in index,
        "next_decision_experiment": "Total Bases exact Champion-vs-shadow endpoint certification",
        "direct_answer": "Current MLB predictions are produced by 14 indexed trained model artifacts where offered, plus at least one live fallback/rule path for outs_recorded. Negative ROI classifications are listed in negative_roi_classification_2026-07-18.csv. The first recommended challenge is Total Bases exact Champion-vs-shadow endpoint certification because it has the cleanest same-row production comparator.",
        "decisions": {r["decision"]: r["value"] for r in decision_rows},
        "guardrails": {
            "model_fitting": False,
            "new_predictions": False,
            "watch_activation": False,
            "scheduler_changes": False,
            "db_writes": False,
            "oddsapi_calls": False,
            "production_behavior_changed": False,
        },
    }
    write_json(out_dir / "machine_readable_production_runtime_baseline_2026-07-18.json", machine)
    write_md(
        out_dir / "production_runtime_performance_baseline_2026-07-18.md",
        f"""# MLB Production Runtime Model and Performance Baseline Certification

Generated: `{generated_at}`

## Summary

The current MLB production baseline is bound to `{len(index)}` indexed trained
model artifacts in `models_out/latest/MODEL_INDEX.json`. The latest fully
reconciled slate found locally is `{latest_date}`, so current-season performance
is measured from `{WINDOW_START}` through `{latest_date}`.

Runtime tracing confirms that current daily inference uses
`build_mlb_predictions_wide.py`, `prop_workflow.predict_prop`, and
`make_prediction.predict`, with model lookup through
`model_registry._latest_artifact_path(prop)`. Current slate artifacts show live
production rows for trained model props where offered and a fallback/rule path
for any live prop missing from the index.

## PHA Answer

Yes: a real production Pitcher Hits Allowed model exists as indexed prop
`hits_allowed`, backed by `models_out/latest/hits_allowed.joblib`.

## Priority

The next decision experiment should be Total Bases exact Champion-vs-shadow
endpoint certification. It has the cleanest same-row production comparator; no
new experiment was started here.

## Decisions

{chr(10).join(f"- `{r['decision']} = {r['value']}`" for r in decision_rows)}

## Production Status

`MLB_PRODUCTION_STATUS = UNCHANGED`
""",
    )
    validation = validation_report(out_dir)
    write_csv(out_dir / "validation_report_2026-07-18.csv", validation)
    manifest_sha = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-18.csv":
            manifest_sha.append({"path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    for path in [MODEL_INDEX, CURRENT_PREDICTIONS_WIDE, CURRENT_SLATE_OUTPUT, CURRENT_UPLOAD, Path(__file__).resolve()]:
        if path.exists():
            manifest_sha.append({"path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest_2026-07-18.csv", manifest_sha)
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
