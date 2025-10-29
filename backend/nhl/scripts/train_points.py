# backend/nhl/scripts/train_points.py
#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
import joblib

"""
Train NHL Points (goals+assists) models for a specific line (e.g., 0.5, 1.5, 2.5).

Expected input CSV (--train-csv):
  - One row per (player_id, game_id) historical game
  - MUST contain: 'points'  (actual goals+assists for that game)
  - SHOULD contain features like:
      d5_points_avg, d10_points_avg, is_home, (optionally more: toi_minutes, sog, ...)
  - MAY contain identifiers you can ignore (player_id, game_id, team_id, opponent_id, game_date)

Usage:
  python3 backend/nhl/scripts/train_points.py \
    --train-csv exports/train_nhl_points_v2.csv \
    --line 0.5 \
    --outdir models_out/nhl/points

Outputs (under models_out/nhl/points/<line>/):
  - lr.joblib                  (Logistic Regression model pipeline)
  - rf.joblib                  (Random Forest model)
  - feature_metadata.json      (feature list, line, version, metrics)
  - METRICS.json               (AUCs and counts)
"""

DEFAULT_FEATURES = [
    "d5_points_avg",
    "d10_points_avg",
    "is_home",
    # add more when available: 'toi_minutes', 'shot_attempts', 'sog', 'pp_toi_minutes', etc.
]

def positive_label(points: float, line: float) -> int:
    # NHL markets are >= line for Over; floating safety margin
    return int(points >= (line - 1e-9))

def pick_features(df: pd.DataFrame, preferred: list[str]) -> list[str]:
    feats = [c for c in preferred if c in df.columns]
    # If we somehow have none, try any numeric cols except targets/ids
    if not feats:
        ignore = {"points","player_id","game_id","team_id","opponent_id","game_date","line"}
        feats = [c for c in df.columns if c not in ignore and pd.api.types.is_numeric_dtype(df[c])]
    return feats

def main():
    ap = argparse.ArgumentParser(description="Train NHL Points models for a given line.")
    ap.add_argument("--train-csv", required=True, help="Historical CSV with 'points' and feature columns.")
    ap.add_argument("--line", type=float, required=True, help="Points line to train for (e.g., 0.5, 1.5, 2.5).")
    ap.add_argument("--outdir", default="models_out/nhl/points", help="Output directory for models.")
    ap.add_argument("--test-size", type=float, default=0.2, help="Holdout fraction (default 0.2).")
    args = ap.parse_args()

    train_path = Path(args.train_csv)
    if not train_path.exists():
        print(f"FATAL: training CSV not found: {train_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(train_path)
    if "points" not in df.columns:
        print("FATAL: training CSV must include 'points' (goals+assists).", file=sys.stderr)
        sys.exit(2)

    # Build label for this line
    line = float(args.line)
    df = df.copy()
    df["y_over"] = df["points"].apply(lambda p: positive_label(p, line)).astype(int)

    # Drop rows with missing labels or all-NaN features later
    feats = pick_features(df, DEFAULT_FEATURES)
    if not feats:
        print("FATAL: no usable feature columns found.", file=sys.stderr)
        sys.exit(2)

    # Keep only rows with non-null for all selected features
    df = df.dropna(subset=feats + ["y_over"])
    if len(df) < 200:
        print(f"WARNING: small training set after filtering (n={len(df)}).", file=sys.stderr)

    X = df[feats].astype(float).values
    y = df["y_over"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=y if len(np.unique(y)) == 2 else None
    )

    # Logistic Regression with scaling
    lr = Pipeline([
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(max_iter=200, solver="liblinear", class_weight="balanced")),
    ])
    lr.fit(X_train, y_train)
    lr_auc = roc_auc_score(y_test, lr.predict_proba(X_test)[:, 1]) if len(np.unique(y_test)) == 2 else float("nan")

    # Random Forest (robust baseline)
    rf = RandomForestClassifier(
        n_estimators=400,
        max_depth=None,
        min_samples_leaf=2,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample",
    )
    rf.fit(X_train, y_train)
    rf_auc = roc_auc_score(y_test, rf.predict_proba(X_test)[:, 1]) if len(np.unique(y_test)) == 2 else float("nan")

    # Prepare outdir
    line_dir = Path(args.outdir) / f"{str(line).replace('.','_')}"
    line_dir.mkdir(parents=True, exist_ok=True)

    # Save models
    joblib.dump(lr, line_dir / "lr.joblib")
    joblib.dump(rf, line_dir / "rf.joblib")

    # Save feature metadata
    metadata = {
        "prop_type": "points",
        "line": line,
        "features": feats,
        "version": "v1",
        "trained_from": str(train_path),
        "metrics": {"lr_auc": float(lr_auc), "rf_auc": float(rf_auc)},
    }
    (line_dir / "feature_metadata.json").write_text(json.dumps(metadata, indent=2))
    (line_dir / "METRICS.json").write_text(json.dumps({
        "n_rows": int(len(df)),
        "pos_rate": float(df["y_over"].mean()) if len(df) else float("nan"),
        "lr_auc": float(lr_auc),
        "rf_auc": float(rf_auc),
    }, indent=2))

    print(f"✅ Trained Points models @ line {line:.1f}")
    print(f"   Features: {feats}")
    print(f"   AUC — LR: {lr_auc:.3f} | RF: {rf_auc:.3f}")
    print(f"   Saved to: {line_dir}")

if __name__ == "__main__":
    main()
