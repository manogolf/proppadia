#!/usr/bin/env python3
"""
score_nhl_props.py — v1.1
Load a saved NHL model and produce P(Over line) for a CSV — now with **guarded calibration**.

What’s new:
- Reads MODEL_INDEX.json + MODEL_ARTIFACT.json
- If an isotonic calibrator exists for a line, it is applied **only if** the
  model’s *calibrated* holdout metric (prefers log_loss, else brier) was
  <= the *raw* holdout metric for that line. Otherwise we skip calibration.
- Enforces monotonicity across lines (p_over_2.5 ≥ p_over_3.5 ≥ ...). Disable with --no-monotonic.

Usage:
  python scripts/score_nhl_props.py \
    --model-dir /Users/jerrystrain/Projects/Proppadia/nhl-props/models/latest/shots_on_goal \
    --csv /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/nhl_sog_training.csv \
    --feature-json /Users/jerrystrain/Projects/Proppadia/nhl-props/features/feature_metadata_nhl.json \
    --feature-key shots_on_goal \
    --line 2.5,3.5 \
    --out /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/sog_predictions.csv
"""

import argparse, json, os, sys, hashlib, math
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
from scipy.stats import poisson as poi_dist, nbinom
import pandas.api.types as ptypes

# ------------- utils ------------- #

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def winsorize_series(s: pd.Series, p_low=0.01, p_high=0.99) -> pd.Series:
    lo, hi = s.quantile(p_low), s.quantile(p_high)
    return s.clip(lo, hi)

def prepare_X(df: pd.DataFrame, raw_feature_list: List[str], model_feature_order: List[str]) -> pd.DataFrame:
    """
    Build features to match the model’s saved dummy-encoded order.
    - Numeric columns: cast to float, winsorize (except boolean/binary), fill median.
    - Boolean/Binary columns: cast to {0.0,1.0}, no winsorize.
    - Non-numeric: fill "other" and one-hot encode.
    - Reindex to model_feature_order and fill missing columns with 0.0.
    """
    def is_bool_or_binary(s: pd.Series) -> bool:
        if ptypes.is_bool_dtype(s):
            return True
        # Treat columns with only {0,1} as binary even if stored as int/float/object
        vals = pd.Series(s.dropna().unique())
        if len(vals) <= 2:
            try:
                as_int = pd.to_numeric(vals, errors="coerce").dropna().astype(int)
                return set(as_int.unique()).issubset({0, 1})
            except Exception:
                return False
        return False

    X = df[raw_feature_list].copy()
    for c in X.columns:
        col = X[c]
        if is_bool_or_binary(col):
            # binary/boolean -> 0.0/1.0, skip winsorize
            X[c] = pd.to_numeric(col, errors="coerce").fillna(0).astype(float)
        elif ptypes.is_numeric_dtype(col):
            X[c] = pd.to_numeric(col, errors="coerce").astype(float)
            # winsorize only true continuous numerics
            lo, hi = X[c].quantile(0.01), X[c].quantile(0.99)
            X[c] = X[c].clip(lo, hi).fillna(X[c].median())
        else:
            # categorical-like
            X[c] = col.astype(str).fillna("other")

    # One-hot encode any remaining non-numeric columns
    cat_cols = [c for c in X.columns if not ptypes.is_numeric_dtype(X[c])]
    if cat_cols:
        X = pd.get_dummies(X, columns=cat_cols, dummy_na=False)

    # Align to model order and ensure numeric
    X = X.reindex(columns=model_feature_order, fill_value=0.0)
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)

    return X

# --- add near the top (after imports) or alongside other helpers ---
def resolve_feature_list(meta: dict, key: str) -> list[str]:
    """
    Accepts either:
      - a list of feature names, or
      - a dict with one of: 'features', 'columns', 'feature_cols'
    Returns a concrete list[str] suitable for df[feature_cols].
    """
    if key not in meta:
        raise KeyError(f"feature_key '{key}' not found. Available: {list(meta.keys())}")

    spec = meta[key]
    if isinstance(spec, list):
        feats = spec
    elif isinstance(spec, dict):
        feats = (spec.get("features")
                 or spec.get("columns")
                 or spec.get("feature_cols"))
    else:
        raise TypeError(f"feature spec for '{key}' must be list or dict, got {type(spec).__name__}")

    if not isinstance(feats, list) or not feats:
        raise ValueError(f"feature spec for '{key}' did not resolve to a non-empty list")

    # normalize to strings
    return [str(c) for c in feats]


def prob_over_poisson(mu: np.ndarray, line: float) -> np.ndarray:
    k = int(math.floor(line))
    return 1.0 - poi_dist.cdf(k, mu)

def prob_over_nb(mu: np.ndarray, alpha: float, line: float) -> np.ndarray:
    a = max(alpha, 1e-8)
    r = 1.0 / a
    p = r / (r + mu)
    k = int(math.floor(line))
    return 1.0 - nbinom.cdf(k, r, p)

def interp_apply(p_raw: np.ndarray, grid_x: np.ndarray, grid_y: np.ndarray) -> np.ndarray:
    p_raw = np.clip(p_raw, 0.0, 1.0)
    return np.interp(p_raw, grid_x, grid_y, left=grid_y[0], right=grid_y[-1])

def is_num(x) -> bool:
    if x is None: return False
    if isinstance(x, (int,)): return True
    if isinstance(x, float):
        return not (math.isnan(x) or math.isinf(x))
    try:
        float(x); return True
    except Exception:
        return False

# ------------- main scoring ------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--feature-json", required=True)
    ap.add_argument("--feature-key", required=True)
    ap.add_argument("--line", required=True, help="comma-separated lines, e.g., 2.5,3.5")
    ap.add_argument("--date-col", default="game_date")
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-monotonic", action="store_true", help="disable monotonic enforcement across lines")
    args = ap.parse_args()

    idx_path = os.path.join(args.model_dir, "MODEL_INDEX.json")
    art_path = os.path.join(args.model_dir, "MODEL_ARTIFACT.json")
    if not (os.path.exists(idx_path) and os.path.exists(art_path)):
        sys.exit(f"Missing model files in {args.model_dir}")

    with open(idx_path, "r") as f:
        model_index = json.load(f)
    with open(art_path, "r") as f:
        artifact = json.load(f)

    family = model_index["family"]
    params = model_index.get("params", {})
    lines = [float(x) for x in args.line.split(",") if x.strip()]
    lines = sorted(lines)

    # ---- Load CSV now (needed for sanity checks) ----
    df = pd.read_csv(args.csv)

    # ---- Load & normalize feature metadata ----
    with open(args.feature_json, "r") as f:
        feat_meta = json.load(f)

    if args.feature_key not in feat_meta:
        sys.exit(f"feature_key '{args.feature_key}' not found in {args.feature_json}")

    meta_item = feat_meta[args.feature_key]

    def _to_feature_list(x):
        """Accept list or dict; if dict, prefer common list fields; else use keys."""
        if isinstance(x, list):
            return x
        if isinstance(x, dict):
            for k in ("features", "columns", "feature_cols", "cols"):
                v = x.get(k)
                if isinstance(v, list):
                    return v
            return list(x.keys())
        if isinstance(x, str):
            return [x]
        return []

    feature_cols = _to_feature_list(meta_item)
    if not feature_cols:
        sys.exit(f"No usable feature list for key '{args.feature_key}' in {args.feature_json}")

    # Allow metadata to override the date column if provided
    meta_date_col = meta_item.get("date_col") if isinstance(meta_item, dict) else None
    date_col = meta_date_col or args.date_col

    # ---- Model's expected order (from artifact) ----
    if family == "poisson":
        model_feature_order = artifact["sklearn_poisson"]["feature_order"]
    elif family == "neg_binomial":
        order_with_const = artifact["statsmodels_nb"]["feature_order_with_const"]
        model_feature_order = [c for c in order_with_const if c != "const"]
    else:
        sys.exit(f"Unsupported family in model index: {family}")

    # ---- CSV sanity ----
    if date_col not in df.columns:
        sys.exit(f"Missing date column: {date_col}")

    # Informational: identifiers present
    id_cols = [c for c in ("player_id", "game_id") if c in df.columns]

    # What the model expects vs what the CSV actually has
    missing_in_csv = [c for c in model_feature_order if c not in df.columns]
    extra_in_csv   = [c for c in df.columns if c not in model_feature_order]
    unused_from_meta = [c for c in feature_cols if c not in model_feature_order]

    print("Feature check:")
    print("  family:", family)
    print("  date_col:", date_col)
    print("  expected (model):", len(model_feature_order))
    print("  provided (meta):", len(feature_cols))
    print("  missing_in_csv:", missing_in_csv)
    print("  extra_in_csv (ignored):", extra_in_csv[:25], "…(%d total)" % max(0, len(extra_in_csv) - 25) if len(extra_in_csv) > 25 else "")
    if unused_from_meta:
        print("  note: metadata includes features not used by this model:", unused_from_meta)

    if missing_in_csv:
        sys.exit(f"CSV missing required model features: {missing_in_csv}")

    # Recreate feature matrix in model's order
    if family == "poisson":
        art = artifact["sklearn_poisson"]
        model_feature_order = art["feature_order"]
        X = prepare_X(df, feature_cols, model_feature_order)
        coef = np.asarray(art["coef"], dtype=float)
        intercept = float(art["intercept"])
        eta = X.values @ coef + intercept
        mu = np.exp(eta)
        def raw_probs_for_line(L: float) -> np.ndarray:
            return np.clip(prob_over_poisson(mu, L), 1e-6, 1-1e-6)
    elif family == "neg_binomial":
        art = artifact["statsmodels_nb"]
        order_with_const = art["feature_order_with_const"]
        model_feature_order = [c for c in order_with_const if c != "const"]
        X = prepare_X(df, feature_cols, model_feature_order)
        Xc = X.copy()
        Xc.insert(0, "const", 1.0)
        Xc = Xc.reindex(columns=order_with_const, fill_value=0.0)
        params_vec = np.asarray(art["params"], dtype=float)
        eta = Xc.values @ params_vec
        mu = np.exp(eta)
        alpha = float(params.get("disp_alpha", 1.0))
        def raw_probs_for_line(L: float) -> np.ndarray:
            return np.clip(prob_over_nb(mu, alpha, L), 1e-6, 1-1e-6)
    else:
        sys.exit(f"Unsupported family in model index: {family}")

    # Decide per-line whether to use calibration, based on holdout metrics
    cal_cfg: Dict[str, Dict] = artifact.get("calibration", {}) or {}
    mh_raw: Dict[str, Dict] = model_index.get("metrics_holdout", {}) or {}
    mh_cal: Dict[str, Dict] = model_index.get("metrics_holdout_calibrated", {}) or {}

    def should_apply_calibration(line_str: str) -> bool:
        if line_str not in cal_cfg:
            return False
        raw_m = mh_raw.get(line_str)
        cal_m = mh_cal.get(line_str)
        if not raw_m or not cal_m:
            # No calibrated holdout metrics to compare → play safe: don't apply
            return False
        # Prefer log_loss if both present; else use brier
        raw_ll = raw_m.get("log_loss"); cal_ll = cal_m.get("log_loss")
        if is_num(raw_ll) and is_num(cal_ll):
            return float(cal_ll) <= float(raw_ll)
        # fallback: brier
        raw_b = raw_m.get("brier"); cal_b = cal_m.get("brier")
        if is_num(raw_b) and is_num(cal_b):
            return float(cal_b) <= float(raw_b)
        return False

    # Compute probs per line (raw, then optionally calibrated if beneficial)
    results = df[["player_id","game_id"]].copy() if all(c in df.columns for c in ["player_id","game_id"]) else df.copy()
    per_line_probs: Dict[float, np.ndarray] = {}

    for L in lines:
        raw = raw_probs_for_line(L)
        key = f"{L:g}"  # "2.5" formatting
        if should_apply_calibration(key):
            ccfg = cal_cfg[key]
            gx = np.asarray(ccfg.get("grid_x", []), dtype=float)
            gy = np.asarray(ccfg.get("grid_y", []), dtype=float)
            if gx.size == 0 or gy.size == 0 or gx.size != gy.size:
                # malformed calibrator; fall back to raw
                per_line_probs[L] = raw
            else:
                per_line_probs[L] = np.clip(interp_apply(raw, gx, gy), 1e-6, 1-1e-6)
        else:
            per_line_probs[L] = raw

    # Optional monotonic enforcement across lines (default ON)
    if not args.no_monotonic and len(lines) >= 2:
        # ensure p_over is non-increasing as the line increases
        # build a (n_rows x n_lines) matrix and enforce row-wise monotone
        mat = np.column_stack([per_line_probs[L] for L in lines])
        # For each row, enforce cumulative min along columns
        # i.e., mat[:, i] = min(mat[:, i], mat[:, i-1])
        for i in range(1, mat.shape[1]):
            mat[:, i] = np.minimum(mat[:, i], mat[:, i-1])
        # write back
        for j, L in enumerate(lines):
            per_line_probs[L] = mat[:, j]

    # Assemble output
    for L in lines:
        results[f"p_over_{L}"] = per_line_probs[L]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    results.to_csv(args.out, index=False)
    print(f"✅ Wrote predictions to: {args.out}")
    # Print which lines used calibration
    used = [f"{L:g}" for L in lines if should_apply_calibration(f"{L:g}")]
    if used:
        print(f"ℹ️ Applied calibration for lines: {', '.join(used)}")
    else:
        print("ℹ️ No calibration applied (either unavailable or not better on holdout).")

if __name__ == "__main__":
    main()
