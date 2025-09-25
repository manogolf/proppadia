#  ml/train_pitcher_props.py

"""
Reusable trainer for pitcher props with batter-style metrics.

Usage examples (run from your repo root):

  python3 tools/train_pitcher_prop.py \
    --prop earned_runs \
    --csv ~/Downloads/train_pitcher_earned_runs.csv

  python3 tools/train_pitcher_prop.py \
    --prop hits_allowed \
    --csv ~/Downloads/train_pitcher_hits_allowed.csv \
    --lines 2.5,4.5,5.5,6.5

Artifacts are saved to: ml/models/pitcher/<prop>/
 - <prop>.joblib
 - <prop>_features.json (expanded post-OHE feature names)
 - <prop>_calibrators.json (identity)

Printed metrics mimic batter props: Brier / ROC-AUC / PR-AUC for "over" at sportsbook half-lines.
"""

import argparse
import json
import math
import os
from typing import List

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score, average_precision_score, mean_absolute_error, r2_score

TARGETS = {
    "earned_runs": "y_earned_runs",
    "hits_allowed": "y_hits_allowed",
    "outs_recorded": "y_outs_recorded",
    "walks_allowed": "y_walks_allowed",
    "strikeouts_pitching": "y_strikeouts_pitching",
}

DEFAULT_LINES = {
    "earned_runs": [0.5, 1.5, 2.5, 3.5],
    "hits_allowed": [2.5, 4.5, 5.5, 6.5],
    "outs_recorded": [12.5, 14.5, 15.5, 17.5, 18.5],
    "walks_allowed": [0.5, 1.5, 2.5],
    "strikeouts_pitching": [2.5, 3.5, 4.5, 5.5, 6.5],
}

EXCLUDE_COLS = {"game_id", "player_id", "game_date"}


def make_ohe():
    """Handle sklearn versions gracefully."""
    try:
        return OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    except TypeError:
        return OneHotEncoder(sparse=False, handle_unknown="ignore")


def p_ge_k(mu_vec: np.ndarray, k: int) -> np.ndarray:
    """P(Poisson(mu) >= k)."""
    if k <= 0:
        return np.ones_like(mu_vec, dtype=float)
    p0 = np.exp(-mu_vec)
    cdf = p0.copy()
    pk = p0.copy()
    for r in range(1, k):
        pk = pk * (mu_vec / r)
        cdf += pk
    return 1.0 - cdf


def safe_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    try:
        if len(np.unique(y_true)) < 2:
            return float("nan")
        return float(roc_auc_score(y_true, y_score))
    except Exception:
        return float("nan")


def safe_ap(y_true: np.ndarray, y_score: np.ndarray) -> float:
    try:
        if len(np.unique(y_true)) < 2:
            return float("nan")
        return float(average_precision_score(y_true, y_score))
    except Exception:
        return float("nan")


def train_one(prop: str, csv_path: str, out_root: str, lines: List[float], split: float, seed: int,
              lr: float, l2: float, max_leaf_nodes: int, min_samples_leaf: int):
    target_col = TARGETS[prop]
    outdir = os.path.join(out_root, prop)
    os.makedirs(outdir, exist_ok=True)

    df = pd.read_csv(os.path.expanduser(csv_path))

    # Coerce common ID-like columns to numeric; keep date for split only
    for c in ["team", "opponent", "game_id", "player_id", "is_starter", "days_rest"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if target_col not in df.columns:
        raise SystemExit(f"Target column '{target_col}' not found in {csv_path}")

    y = pd.to_numeric(df[target_col], errors="coerce")

    base_cols = [c for c in df.columns if c not in (EXCLUDE_COLS | {target_col})]
    X = df[base_cols].copy()

    # Decide cat vs num
    cat_cols = [c for c in ("team", "opponent") if c in X.columns]
    num_cols = [c for c in base_cols if c not in cat_cols]

    # Build valid mask (drop rows with NaNs in used cols)
    gd = pd.to_datetime(df.get("game_date"), errors="coerce")
    mask = ~(y.isna() | gd.isna())
    for c in num_cols:
        mask &= ~pd.to_numeric(X[c], errors="coerce").isna()
    for c in cat_cols:
        mask &= ~X[c].isna()

    X, y, gd = X[mask], y[mask], gd[mask]

    # Temporal split
    order = gd.sort_values().index
    cut = max(1, int(split * len(order)))
    tr, va = order[:cut], order[cut:]
    if len(va) == 0:  # fallback if tiny dataset
        tr, va = order[::2], order[1::2]

    # Pipeline
    ct = ColumnTransformer(
        transformers=[
            ("num", "passthrough", num_cols),
            ("cat", make_ohe(), cat_cols),
        ],
        remainder="drop",
    )

    model = HistGradientBoostingRegressor(
        loss="poisson",
        learning_rate=lr,
        max_leaf_nodes=max_leaf_nodes,
        min_samples_leaf=min_samples_leaf,
        l2_regularization=l2,
        early_stopping=True,
        validation_fraction=0.1,
        random_state=seed,
    )

    pipe = Pipeline([("prep", ct), ("hgb", model)])
    pipe.fit(X.loc[tr], y.loc[tr])

    # Metrics
    mu = np.clip(pipe.predict(X.loc[va]), 1e-9, None)
    yva = y.loc[va].values

    print(f"\n=== {prop} — validation metrics ({len(va)} rows) ===")
    # Regression sanity
    mae = mean_absolute_error(y.loc[va], mu)
    r2  = r2_score(y.loc[va], mu)
    print(f" MAE={mae:.3f}  R2={r2:.3f}")

    for line in lines:
        k = math.floor(line) + 1  # over threshold
        p_over = p_ge_k(mu, k)
        y_over = (yva > line).astype(int)
        brier  = float(np.mean((p_over - y_over) ** 2))
        auc    = safe_auc(y_over, p_over)
        ap     = safe_ap(y_over, p_over)
        base   = float(y_over.mean())
        auc_s  = "None" if np.isnan(auc) else f"{auc:.3f}"
        ap_s   = "None" if np.isnan(ap) else f"{ap:.3f}"
        print(f" line {line:>5}: Brier={brier:.5f}  ROC-AUC={auc_s}  PR-AUC={ap_s}  base_rate={base:.3f}")

    # Persist artifacts
    prep = pipe.named_steps["prep"]
    num_names = num_cols
    cat_names = list(prep.named_transformers_["cat"].get_feature_names_out(cat_cols)) if cat_cols else []
    feature_names = num_names + cat_names

    dump(pipe, os.path.join(outdir, f"{prop}.joblib"))
    with open(os.path.join(outdir, f"{prop}_features.json"), "w") as f:
        json.dump(feature_names, f)
    with open(os.path.join(outdir, f"{prop}_calibrators.json"), "w") as f:
        json.dump({"method": "identity"}, f)

    print(f"saved: {outdir}")
    print(f"features: {len(feature_names)}")


def parse_lines(arg: str) -> List[float]:
    return [float(x.strip()) for x in arg.split(",") if x.strip()]


def main():
    p = argparse.ArgumentParser(description="Train a pitcher prop model with batter-style metrics.")
    p.add_argument("--prop", required=True, choices=sorted(TARGETS.keys()))
    p.add_argument("--csv", required=True, help="Path to training CSV")
    p.add_argument("--out-root", default=os.path.join("ml","models","pitcher"))
    p.add_argument("--lines", type=parse_lines, default=None, help="Comma-separated sportsbook half-lines (e.g. '1.5,2.5,3.5')")
    p.add_argument("--split", type=float, default=0.9, help="Temporal train fraction (default 0.9)")
    p.add_argument("--seed", type=int, default=42)
    # Model knobs
    p.add_argument("--lr", type=float, default=0.05)
    p.add_argument("--l2", type=float, default=0.5)
    p.add_argument("--max-leaf-nodes", type=int, default=31)
    p.add_argument("--min-samples-leaf", type=int, default=50)
    args = p.parse_args()

    lines = args.lines if args.lines is not None else DEFAULT_LINES[args.prop]
    train_one(
        prop=args.prop,
        csv_path=args.csv,
        out_root=args.out_root,
        lines=lines,
        split=args.split,
        seed=args.seed,
        lr=args.lr,
        l2=args.l2,
        max_leaf_nodes=args.max_leaf_nodes,
        min_samples_leaf=args.min_samples_leaf,
    )


if __name__ == "__main__":
    main()
