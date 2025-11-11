#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys
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
Train NHL SOG models at the player-game level (phoenix spine).

Expected input (--train-csv):

  - One row per (season, game_id, shooterPlayerId)
  - Columns (from nhl.training_features_sog_player_game):
      season, game_id, teamCode, isHomeTeam, shooterPlayerId,
      y_sog,
      shots_attempts_game, sog_game,
      num_event_shot_last5, num_event_shot_last10,
      num_shotWasOnGoal_last5, num_shotWasOnGoal_last10,
      num_event_shot_season_to_date, num_shotWasOnGoal_season_to_date,
      team_num_event_shot_for_last10, team_num_shotWasOnGoal_for_last10, ...

We train a binary model for: Over(line) = (y_sog >= line).
"""

ID_COLS = {
    "season",
    "game_id",
    "shooterplayerid",
    "teamCode",
    "isHomeTeam",
}

TARGET_COL = "y_sog"

LEAK_COLS = {
    TARGET_COL,
    "y_over",
    "sog_game",
    "shots_attempts_game",
    "y_goals",
    "goals",
    "assists",
}


def main():
    ap = argparse.ArgumentParser(description="Train NHL SOG player-game models (phoenix).")
    ap.add_argument("--train-csv", required=True,
                    help="CSV exported from nhl.training_features_sog_player_game.")
    ap.add_argument("--line", type=float, required=True,
                    help="SOG line to train for, e.g. 1.5, 2.5, 3.5.")
    ap.add_argument("--outdir", default="models_out/nhl/sog_player",
                    help="Output directory base for models.")
    ap.add_argument("--test-size", type=float, default=0.2,
                    help="Holdout fraction (default 0.2).")
    args = ap.parse_args()

    path = Path(args.train_csv)
    if not path.exists():
        print(f"FATAL: training CSV not found: {path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(path)

    if TARGET_COL not in df.columns:
        print(f"FATAL: '{TARGET_COL}' column is required in training CSV.", file=sys.stderr)
        sys.exit(2)

    line = float(args.line)
    # Label: Over(line)
    df["y_over"] = (df[TARGET_COL] >= (line - 1e-9)).astype(int)

    # Select numeric features excluding IDs/targets
    ignore = ID_COLS.union(LEAK_COLS)
    feats = [
        c for c in df.columns
        if c not in ignore and pd.api.types.is_numeric_dtype(df[c])
    ]

    if not feats:
        print("FATAL: no usable numeric feature columns found.", file=sys.stderr)
        sys.exit(2)

    # Coerce numeric and clean
    for c in feats + ["y_over"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    before = len(df)
    df = df.dropna(subset=feats + ["y_over"])
    after = len(df)
    if after < before:
        print(f"Dropped {before - after} rows with NaNs.", file=sys.stderr)
    if after < 500:
        print(f"WARNING: small training set after filtering (n={after}).", file=sys.stderr)

    X = df[feats].to_numpy(dtype=float)
    y = df["y_over"].to_numpy(dtype=int)

    if np.unique(y).size < 2:
        print("FATAL: only one class present in y_over.", file=sys.stderr)
        sys.exit(2)

    pos_rate = float(y.mean())
    print(f"Class balance (Over {line}): {pos_rate:.3f}", file=sys.stderr)

    strat = y if np.unique(y).size == 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=args.test_size,
        random_state=42,
        stratify=strat,
    )

    # Logistic Regression model
    lr = Pipeline([
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(
            max_iter=400,
            solver="liblinear",
            class_weight="balanced",
        )),
    ])
    lr.fit(X_train, y_train)
    lr_auc = (
        roc_auc_score(y_test, lr.predict_proba(X_test)[:, 1])
        if np.unique(y_test).size == 2 else float("nan")
    )

    # Random Forest model
    rf = RandomForestClassifier(
        n_estimators=400,
        max_depth=None,
        min_samples_leaf=2,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample",
    )
    rf.fit(X_train, y_train)
    rf_auc = (
        roc_auc_score(y_test, rf.predict_proba(X_test)[:, 1])
        if np.unique(y_test).size == 2 else float("nan")
    )

    # Save models + metadata under line-specific dir
    outdir = Path(args.outdir) / f"{str(line).replace('.', '_')}"
    outdir.mkdir(parents=True, exist_ok=True)

    joblib.dump(lr, outdir / "lr.joblib")
    joblib.dump(rf, outdir / "rf.joblib")

    metadata = {
        "prop_type": "sog",
        "line": line,
        "features": feats,
        "trained_from": str(path),
        "metrics": {
            "lr_auc": float(lr_auc),
            "rf_auc": float(rf_auc),
        },
        "phoenix_spine": True,
    }
    (outdir / "feature_metadata.json").write_text(json.dumps(metadata, indent=2))
    (outdir / "METRICS.json").write_text(json.dumps({
        "n_rows": int(after),
        "pos_rate": float(pos_rate),
        "lr_auc": float(lr_auc),
        "rf_auc": float(rf_auc),
    }, indent=2))

    print(f"✅ Trained SOG models for line {line}")
    print(f"   n={after}, pos_rate={pos_rate:.3f}")
    print(f"   Features ({len(feats)}): {feats}")
    print(f"   AUC — LR: {lr_auc if lr_auc==lr_auc else 'NA'} | RF: {rf_auc if rf_auc==rf_auc else 'NA'}")
    print(f"   Saved to: {outdir}")

if __name__ == "__main__":
    main()
