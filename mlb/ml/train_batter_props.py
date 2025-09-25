# ml/train_batter_props.py
"""
Batter prop trainer (numeric-only, market-agnostic) with time-aware
bagged calibration. Produces artifacts the runtime scorer consumes.

Props (examples): hits, singles, total_bases, runs, hrr, home_runs, rbis, walks, ...
Input CSV: ml/train_batter_<prop>.csv (must contain y_<prop> target)

Key rules:
- Features are **numeric only** (no raw strings). Any object cols are coerced to numeric;
  non-numeric leftovers are dropped from features automatically.
- Saved feature spec: {"features": [...]}  (used at inference to align/zero-fill)
- Model: HistGradientBoostingRegressor(loss="poisson")
- Calibration: per-fold isotonic (default) or Platt, bagged + clipped.

Artifacts (per prop):
  <prefix>_poisson_v1.joblib
  <prefix>_features_v1.json
  <prefix>_calibrators_v1.json   (optional)
  ml/pred_<prop>_test.csv        (diagnostics)

Usage:
  python ml/train_batter_props.py 
    --prop total_bases --csv ml/train_batter_total_bases.csv 
    --lines 0.5 1.5 2.5 --folds 5 --calibration isotonic
"""

from __future__ import annotations
import argparse, json, math, os
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from joblib import dump
from pandas.api.types import is_numeric_dtype
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import HistGradientBoostingRegressor as HGBR
from scipy.stats import poisson

# ----------------------
# Helpers
# ----------------------
def prop_prefix(prop: str) -> str:
    prop = prop.lower().strip()
    return {
        "total_bases": "tb",
        "home_runs": "hr",
        "hits": "hi",
        "rbis": "rb",
        "runs": "ru",
        "walks": "bb",
        "singles": "si",
        "doubles": "2b",
        "triples": "3b",
        "stolen_bases": "sb",
        "strikeouts_batting": "ks",
        "earned_runs": "er",
        "hits_allowed": "ha",
        "walks_allowed": "wa",
        "strikeouts_pitching": "kp",
        "outs_recorded": "or",
        "runs_rbis": "rr",
        "hrr": "hrr",
    }.get(prop, prop[:2])

def make_time_splits(dates: pd.Series, k: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    d = pd.to_datetime(dates).sort_values().unique()
    if len(d) < k:
        k = max(1, len(d))
    edges = np.linspace(0, len(d), k + 1, dtype=int)
    out = []
    for i in range(k):
        lo_idx, hi_idx = edges[i], edges[i + 1]
        if hi_idx - lo_idx <= 0:
            continue
        start = pd.Timestamp(d[lo_idx])
        end = pd.Timestamp(d[hi_idx]) if hi_idx < len(d) else pd.Timestamp(d[-1]) + pd.Timedelta(days=1)
        out.append((start, end))
    return out

def eval_line(name: str, y_true: np.ndarray, p_over: np.ndarray, line: float) -> None:
    y_bin = (y_true > line).astype(int)
    brier = float(np.mean((p_over - y_bin) ** 2))
    auc = float(roc_auc_score(y_bin, p_over)) if len(np.unique(y_bin)) > 1 else float("nan")
    ap  = float(average_precision_score(y_bin, p_over)) if len(np.unique(y_bin)) > 1 else float("nan")
    print(f"{name} Line {line}: Brier={brier:.4f}  ROC-AUC={auc:.3f}  PR-AUC={ap:.3f}")

def apply_calib_bag(p_raw: np.ndarray, bag: List[Dict], clip_min=0.02, clip_max=0.98) -> np.ndarray:
    """Average predictions from calibrator dicts; robust to bad folds."""
    if not bag:
        return np.clip(p_raw, clip_min, clip_max)
    preds = []
    for cal in bag:
        t = cal.get("type", "identity")
        if t == "isotonic":
            x = np.asarray(cal["x"], dtype=float)
            y = np.asarray(cal["y"], dtype=float)
            if x.size < 2 or (x[-1] - x[0]) < 0.15:  # ignore degenerate maps
                preds.append(p_raw)
            else:
                preds.append(np.interp(p_raw, x, y, left=y[0], right=y[-1]))
        elif t == "platt":
            z = cal["coef"] * p_raw + cal["intercept"]
            preds.append(1.0 / (1.0 + np.exp(-z)))
        else:
            preds.append(p_raw)
    p = np.median(np.vstack(preds), axis=0)  # robust aggregate
    return np.clip(p, clip_min, clip_max)

def fit_platt_calibrator(p_over_raw: np.ndarray, y_bin: np.ndarray) -> Dict:
    lr = LogisticRegression(C=3.0, solver="liblinear", max_iter=1000)
    lr.fit(p_over_raw.reshape(-1, 1), y_bin.astype(int))
    return {"type": "platt", "coef": float(lr.coef_[0, 0]), "intercept": float(lr.intercept_[0])}

# ----------------------
# Model builder (numeric-only)
# ----------------------
def build_numeric_pipe() -> Pipeline:
    return Pipeline([
        ("impute", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("hgb", HGBR(loss="poisson",
                     learning_rate=0.06,
                     max_depth=6,
                     max_iter=300,
                     early_stopping=False,
                     random_state=42))
    ])

# ----------------------
# Main
# ----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prop", required=True, help="e.g., hits|singles|total_bases|runs|hrr")
    ap.add_argument("--csv", required=True, help="path to ml/train_batter_<prop>.csv")
    ap.add_argument("--lines", nargs="+", type=float, default=[0.5, 1.5, 2.5])
    ap.add_argument("--line-weights", nargs="*", type=float, default=None,
                    help="optional weights per line (same length as --lines)")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--val-days", type=int, default=28)
    ap.add_argument("--test-days", type=int, default=28)
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS", "540")))
    ap.add_argument("--clip-min", type=float, default=0.02)
    ap.add_argument("--clip-max", type=float, default=0.98)
    ap.add_argument("--calibration", choices=["isotonic", "platt"], default="isotonic")
    ap.add_argument("--zero-select", choices=["auto", "zip", "poisson"], default="auto")
    args = ap.parse_args()

    TARGET = f"y_{args.prop}"
    ID_COLS = ["game_id", "player_id", "game_date"]
    DROP_ALWAYS = {"prop_source"}  # never a feature
    DROP_IF_PRESENT = {"y_over"} | {c for c in
        ["line_hits","line_singles","line_total_bases","line_runs","line_hrr","line_home_runs","line_rbis","line_walks"]
        if c != f"line_{args.prop}"}

    if args.line_weights is not None and len(args.line_weights) != len(args.lines):
        raise ValueError("--line-weights must match --lines length")

    # ---- load
    df = pd.read_csv(args.csv)
    if "game_date" not in df.columns:
        raise ValueError("CSV must include game_date")
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values("game_date").reset_index(drop=True)

    # ---- lookback window
    max_date = df["game_date"].max()
    min_keep = max_date - pd.Timedelta(days=args.lookback_days - 1)
    df = df[df["game_date"] >= min_keep].copy()

    # ---- coerce everything non-ID/target to numeric where possible
    for c in df.columns:
        if c not in ID_COLS + [TARGET]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # ---- BvP presence flag (so missing ≠ zero)
    BVP_COLS = [
        "bvp_pa_prior","bvp_ab_prior","bvp_hits_prior","bvp_tb_prior",
        "bvp_hr_prior","bvp_bb_prior","bvp_so_prior",
        "bvp_avg_prior_sm","bvp_tb_per_ab_prior_sm","bvp_bb_rate_prior_sm","bvp_so_rate_prior_sm",
    ]
    present = [c for c in BVP_COLS if c in df.columns]
    if present:
        if "bvp_pa_prior" in df.columns:
            df["bvp_has_history"] = df["bvp_pa_prior"].notna().astype(int)
        else:
            df["bvp_has_history"] = (~df[present].isna().all(axis=1)).astype(int)
    else:
        df["bvp_has_history"] = 0

    # ---- target & features
    if TARGET not in df.columns:
        raise ValueError(f"CSV must include {TARGET}")
    y = df[TARGET].astype(float).values
    ids = df[ID_COLS].copy()
    if "user_added" in df.columns:
        ids["user_added"] = df["user_added"]

    # drop IDs, target, any line_* columns, and hard-drop list; keep strictly numeric
    LINE_COLS = [c for c in df.columns if c.startswith("line_")]
    cols_exclude = set(ID_COLS + [TARGET]) | set(LINE_COLS) | DROP_ALWAYS
    candidate = [c for c in df.columns if c not in cols_exclude]

    # keep only numeric columns (object → NaN already coerced above)
    feature_cols = [c for c in candidate if is_numeric_dtype(df[c])]
    if not feature_cols:
        raise ValueError("No numeric features found after coercion")

    X = df[feature_cols].copy()

        # drop fully-empty numeric columns to avoid SimpleImputer warnings
    X = X.loc[:, X.notna().any(axis=0)]
    feature_cols = list(X.columns)  # ensure we save the exact set used


    # ---- time-based split
    test_start = max_date - pd.Timedelta(days=args.test_days - 1)
    val_start  = test_start - pd.Timedelta(days=args.val_days)
    saved_min = df.loc[df["game_date"] < test_start, "game_date"].min()
    saved_max = test_start - pd.Timedelta(days=1)
    print(f"Saved model trained on: {saved_min.date()} → {saved_max.date()}")
    print(f"  train < {val_start.date()}  |  val [{val_start.date()}, {test_start.date()})  |  test ≥ {test_start.date()}")

    train_m = df["game_date"] < val_start
    val_m   = (df["game_date"] >= val_start) & (df["game_date"] < test_start)
    test_m  = df["game_date"] >= test_start

    X_train, y_train = X[train_m], y[train_m]
    X_val,   y_val   = X[val_m],   y[val_m]   # not used for selection; kept for ref
    X_test,  y_test  = X[test_m],  y[test_m]

    print("Shapes:", X_train.shape, X_val.shape, X_test.shape)

    # ----------------------
    # Build bagged calibrators (TRAIN only)
    # ----------------------
    lines = list(args.lines)
    folds = make_time_splits(df.loc[train_m, "game_date"], k=args.folds)

    cal_bag: Dict[str, List[Dict]] = {str(L).replace(".","_"): [] for L in lines}

    for (fold_start, fold_end) in folds:
        fold_mask = train_m & (df["game_date"] >= fold_start) & (df["game_date"] < fold_end)
        pre_mask  = train_m & (df["game_date"] < fold_start)
        if pre_mask.sum() < 100 or fold_mask.sum() == 0:
            continue

        pipe = build_numeric_pipe()
        pipe.fit(X[pre_mask], y[pre_mask].astype(float))

        lam_f = np.clip(pipe.predict(X[fold_mask]), 1e-6, 1e6)

        for L in lines:
            p_over_raw = 1.0 - poisson.cdf(math.floor(L), lam_f)
            y_bin = (y[fold_mask] > L).astype(int)

            if args.calibration == "platt":
                cal = fit_platt_calibrator(p_over_raw, y_bin)
            else:
                iso = IsotonicRegression(out_of_bounds="clip")
                iso.fit(p_over_raw, y_bin)
                cal = {"type": "isotonic",
                       "x": iso.X_thresholds_.tolist(),
                       "y": iso.y_thresholds_.tolist()}
            cal.update({
                "fold_start": str(fold_start.date()),
                "fold_end": str((fold_end - pd.Timedelta(days=1)).date()),
            })
            cal_bag[str(L).replace(".","_")].append(cal)

    # ----------------------
    # Final fit on TRAIN; evaluate on TEST
    # ----------------------
    pipe_final = build_numeric_pipe()
    pipe_final.fit(X_train, y_train.astype(float))

    lam_test = np.clip(pipe_final.predict(X_test), 1e-6, 1e6)
    out = ids.loc[test_m].copy()
    out["y_true"] = y_test
    out["lambda_raw"] = lam_test

    for L in lines:
        raw = 1.0 - poisson.cdf(math.floor(L), lam_test)
        bag = cal_bag.get(str(L).replace(".","_"), [])
        cal = apply_calib_bag(raw, bag, clip_min=args.clip_min, clip_max=args.clip_max)
        out[f"p_over_{str(L).replace('.','_')}"] = cal
        eval_line("test", y_test, cal, L)

    # ----------------------
    # Persist artifacts
    # ----------------------
    models_dir = Path("ml/models") / "batter" / args.prop
    models_dir.mkdir(parents=True, exist_ok=True)

    prefix = prop_prefix(args.prop)
    dump(pipe_final, models_dir / f"{prefix}_poisson_v1.joblib")
    with open(models_dir / f"{prefix}_features_v1.json", "w") as f:
        json.dump({"features": feature_cols}, f, indent=2)
    if any(cal_bag.values()):
        with open(models_dir / f"{prefix}_calibrators_v1.json", "w") as f:
            json.dump({
                "prop": args.prop,
                "lines": cal_bag,
                "clip": {"min": args.clip_min, "max": args.clip_max},
                "folds": args.folds,
                "calibration": args.calibration,
            }, f, indent=2)

    out_path = Path("ml") / f"pred_{args.prop}_test.csv"
    out.to_csv(out_path, index=False)
    print(f"\n[saved] {models_dir}/{prefix}_poisson_v1.joblib")
    print(f"[saved] {models_dir}/{prefix}_features_v1.json (n={len(feature_cols)})")
    if any(cal_bag.values()):
        print(f"[saved] {models_dir}/{prefix}_calibrators_v1.json")
    print(f"[saved] {out_path}")

if __name__ == "__main__":
    main()
