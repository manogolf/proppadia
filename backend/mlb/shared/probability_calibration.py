"""Small MLB probability calibration helpers.

The calibration layer is intentionally independent from the model artifact:
it maps raw side probabilities to observed win rates using resolved reconcile
rows, without retraining the underlying model.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd


GLOBAL_PROP = "__global__"


def _clip_prob(value: Any, *, eps: float = 1e-6) -> Optional[float]:
    try:
        x = float(value)
    except Exception:
        return None
    if not np.isfinite(x):
        return None
    return float(min(1.0 - eps, max(eps, x)))


def brier_score(probs: Iterable[Any], outcomes: Iterable[Any]) -> Optional[float]:
    vals: list[float] = []
    for p_raw, y_raw in zip(probs, outcomes):
        p = _clip_prob(p_raw)
        try:
            y = int(y_raw)
        except Exception:
            continue
        if p is None or y not in (0, 1):
            continue
        vals.append((p - float(y)) ** 2)
    if not vals:
        return None
    return float(np.mean(vals))


def fit_isotonic_model(probs: Iterable[Any], outcomes: Iterable[Any]) -> Dict[str, Any]:
    pairs: list[tuple[float, int]] = []
    for p_raw, y_raw in zip(probs, outcomes):
        p = _clip_prob(p_raw)
        try:
            y = int(y_raw)
        except Exception:
            continue
        if p is not None and y in (0, 1):
            pairs.append((float(p), int(y)))

    if not pairs:
        return {
            "n": 0,
            "base_rate": None,
            "x_thresholds": [],
            "y_thresholds": [],
            "brier_raw": None,
            "brier_calibrated": None,
        }

    pairs.sort(key=lambda t: t[0])
    blocks: list[dict[str, float]] = []
    for x, y in pairs:
        blocks.append({"x_min": x, "x_max": x, "weight": 1.0, "sum_y": float(y)})
        while len(blocks) >= 2:
            left = blocks[-2]
            right = blocks[-1]
            left_mean = left["sum_y"] / left["weight"]
            right_mean = right["sum_y"] / right["weight"]
            if left_mean <= right_mean:
                break
            merged = {
                "x_min": left["x_min"],
                "x_max": right["x_max"],
                "weight": left["weight"] + right["weight"],
                "sum_y": left["sum_y"] + right["sum_y"],
            }
            blocks[-2:] = [merged]

    x_thresholds = [float(b["x_min"]) for b in blocks]
    y_thresholds = [float(b["sum_y"] / b["weight"]) for b in blocks]
    if blocks:
        x_thresholds.append(float(blocks[-1]["x_max"]))
        y_thresholds.append(float(y_thresholds[-1]))

    raw_probs = [p for p, _ in pairs]
    ys = [y for _, y in pairs]
    cal_probs = [predict_isotonic_probability({"x_thresholds": x_thresholds, "y_thresholds": y_thresholds}, p) for p in raw_probs]
    return {
        "n": int(len(pairs)),
        "base_rate": float(np.mean(ys)),
        "x_thresholds": x_thresholds,
        "y_thresholds": y_thresholds,
        "brier_raw": brier_score(raw_probs, ys),
        "brier_calibrated": brier_score(cal_probs, ys),
    }


def predict_isotonic_probability(model: Dict[str, Any], raw_prob: Any) -> Optional[float]:
    p = _clip_prob(raw_prob)
    if p is None:
        return None
    xs = [float(x) for x in model.get("x_thresholds") or []]
    ys = [float(y) for y in model.get("y_thresholds") or []]
    if not xs or not ys or len(xs) != len(ys):
        base = model.get("base_rate")
        return _clip_prob(base) if base is not None else p
    return float(min(1.0, max(0.0, np.interp(float(p), xs, ys))))


def load_calibrator(path: str | Path | None) -> Optional[Dict[str, Any]]:
    if path is None or not str(path).strip():
        return None
    p = Path(str(path)).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"missing probability calibration json: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def calibrate_probability(
    calibrator: Optional[Dict[str, Any]],
    *,
    prop_type: Any,
    raw_prob: Any,
    min_prop_samples: int = 200,
) -> Optional[float]:
    p = _clip_prob(raw_prob)
    if p is None:
        return None
    if not calibrator:
        return p
    models = calibrator.get("models") or {}
    prop = str(prop_type or "").strip().lower()
    model = models.get(prop)
    if not isinstance(model, dict) or int(model.get("n") or 0) < int(min_prop_samples):
        model = models.get(GLOBAL_PROP)
    if not isinstance(model, dict):
        return p
    calibrated = predict_isotonic_probability(model, p)
    return p if calibrated is None else calibrated


def build_calibrator(
    rows: pd.DataFrame,
    *,
    prop_types: Iterable[str] | None = None,
    min_prop_samples: int = 200,
    training_scope: str = "model_picks",
) -> Dict[str, Any]:
    df = rows.copy()
    for col in (
        "prop_type",
        "model_prob_over",
        "model_prob_under",
        "model_pick_prob",
        "model_pick_side",
        "actual_model_pick_outcome",
        "actual_over_outcome",
        "actual_under_outcome",
    ):
        if col not in df.columns:
            df[col] = pd.NA
    df["prop_type"] = df["prop_type"].astype(str).str.strip().str.lower()
    selected_props = {str(p).strip().lower() for p in (prop_types or []) if str(p).strip()}
    if selected_props:
        df = df[df["prop_type"].isin(selected_props)].copy()

    scope = str(training_scope or "model_picks").strip().lower()
    side_rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        prop = str(row.get("prop_type") or "").strip().lower()
        if not prop:
            continue
        if scope == "all_sides":
            over_p = _clip_prob(row.get("model_prob_over"))
            under_p = _clip_prob(row.get("model_prob_under"))
            if under_p is None and over_p is not None:
                under_p = 1.0 - over_p
            over_out = str(row.get("actual_over_outcome") or "").strip().lower()
            under_out = str(row.get("actual_under_outcome") or "").strip().lower()
            if over_p is not None and over_out in {"win", "loss"}:
                side_rows.append({"prop_type": prop, "side": "over", "raw_prob": over_p, "actual_win": 1 if over_out == "win" else 0})
            if under_p is not None and under_out in {"win", "loss"}:
                side_rows.append({"prop_type": prop, "side": "under", "raw_prob": under_p, "actual_win": 1 if under_out == "win" else 0})
            continue

        p = _clip_prob(row.get("model_pick_prob"))
        outcome = str(row.get("actual_model_pick_outcome") or "").strip().lower()
        if p is not None and outcome in {"win", "loss"}:
            side_rows.append(
                {
                    "prop_type": prop,
                    "side": str(row.get("model_pick_side") or "").strip().lower(),
                    "raw_prob": p,
                    "actual_win": 1 if outcome == "win" else 0,
                }
            )

    side_df = pd.DataFrame(side_rows)
    models: dict[str, dict[str, Any]] = {}
    if side_df.empty:
        models[GLOBAL_PROP] = fit_isotonic_model([], [])
    else:
        models[GLOBAL_PROP] = fit_isotonic_model(side_df["raw_prob"], side_df["actual_win"])
        for prop, group in side_df.groupby("prop_type"):
            model = fit_isotonic_model(group["raw_prob"], group["actual_win"])
            if int(model.get("n") or 0) >= int(min_prop_samples):
                models[str(prop)] = model

    return {
        "version": 1,
        "method": "isotonic",
        "fitted_at_utc": datetime.now(timezone.utc).isoformat(),
        "min_prop_samples": int(min_prop_samples),
        "training_rows": int(len(df)),
        "training_side_rows": int(len(side_df)),
        "training_scope": scope,
        "prop_types": sorted(selected_props) if selected_props else sorted(set(df["prop_type"].dropna().astype(str))),
        "models": models,
    }


def calibration_curve(df: pd.DataFrame, *, prob_col: str, actual_col: str, group_cols: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work[prob_col] = pd.to_numeric(work[prob_col], errors="coerce")
    work[actual_col] = pd.to_numeric(work[actual_col], errors="coerce")
    work = work[work[prob_col].notna() & work[actual_col].isin([0, 1])].copy()
    if work.empty:
        return pd.DataFrame()
    bins = np.linspace(0.0, 1.0, 11)
    labels = [f"{bins[i]:.1f}-{bins[i + 1]:.1f}" for i in range(len(bins) - 1)]
    work["prob_bucket"] = pd.cut(work[prob_col].clip(0, 1), bins=bins, labels=labels, include_lowest=True)
    groups = [*(group_cols or []), "prob_bucket"]
    out = (
        work.groupby(groups, dropna=False, observed=False)
        .agg(rows=(actual_col, "size"), avg_prob=(prob_col, "mean"), actual_win_rate=(actual_col, "mean"))
        .reset_index()
    )
    out["abs_error"] = (out["avg_prob"] - out["actual_win_rate"]).abs()
    return out
