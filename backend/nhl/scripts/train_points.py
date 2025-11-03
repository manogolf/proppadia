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
  - One row per (player_id, game_id)
  - Target columns: prefer 'y_points'; if missing, fall back to 'points'
  - Feature columns should include the set you export (see DEFAULT_FEATURES)

Usage:
  python3 backend/nhl/scripts/train_points.py \
    --train-csv exports/train_nhl_points_v2.csv \
    --line 0.5 \
    --outdir models_out/nhl/points
"""

DEFAULT_FEATURES = [
  "d5_points_avg",
  "d10_points_avg",
  "d10_sog_avg",
  "d10_attempts_avg",
  "d10_toi_min_avg",

  # --- Power-Play Time on Ice (new) ---
  "pp_toi_min_g",
  "d5_pp_toi_min_avg",
  "d10_pp_toi_min_avg",
  "ema3_pp_toi_min",
  "ema8_pp_toi_min",
  "pp_role_rank_g",
  "pp1_flag_g",
  "d10_pp1_rate",
  "pp_toi_share_team_g",
  "d10_pp_toi_share_avg",
  "pp_toi_trend_3v10",

  # Interactions
  "d10_sog_avg__x__d10_pp_toi_min_avg",
  "d10_attempts_avg__x__d10_pp_toi_min_avg",

  # Existing team/opponent pace/shot context
  "team_d10_sf_per60",
  "opp_d10_sf_per60",
  "pace_matchup_index",
]

# Legacy -> canonical aliases tolerated in older exports
FEATURE_ALIASES = {
  "d10_pp_min_avg": "d10_pp_toi_min_avg",
  # add more aliases here if you uncover older column names
}

PP_CLIP_RULES = {
  "pp_toi_min_g": (0.0, 10.0),
  "d5_pp_toi_min_avg": (0.0, 10.0),
  "d10_pp_toi_min_avg": (0.0, 10.0),
  "ema3_pp_toi_min": (0.0, 10.0),
  "ema8_pp_toi_min": (0.0, 10.0),
  "pp_toi_share_team_g": (0.0, 1.25),
  "d10_pp_toi_share_avg": (0.0, 1.25),
}


def positive_label(points: float, line: float) -> int:
    # NHL markets are >= line for Over; floating safety margin
    return int(points >= (line - 1e-9))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Target alias: points -> y_points if needed
    if "y_points" not in df.columns and "points" in df.columns:
        df = df.rename(columns={"points": "y_points"})
    # Feature aliases
    for old, new in FEATURE_ALIASES.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    return df


def pick_features(df: pd.DataFrame, preferred: list[str]) -> list[str]:
    feats = [c for c in preferred if c in df.columns]
    if not feats:
        ignore = {
            "y_points",
            "points",
            "y_over",
            "player_id",
            "game_id",
            "team_id",
            "opponent_id",
            "game_date",
            "line",
        }
        feats = [
            c for c in df.columns
            if c not in ignore and pd.api.types.is_numeric_dtype(df[c])
        ]
    return feats


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # replace inf with NaN to allow dropna
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df


def clip_pp_features(df: pd.DataFrame, enable: bool) -> pd.DataFrame:
    if not enable:
        return df
    for c, (lo, hi) in PP_CLIP_RULES.items():
        if c in df.columns:
            df[c] = df[c].clip(lower=lo, upper=hi)
    return df


def main():
    ap = argparse.ArgumentParser(description="Train NHL Points models for a given line.")
    ap.add_argument("--train-csv", required=True, help="Historical CSV with 'y_points' (or 'points') and feature columns.")
    ap.add_argument("--line", type=float, required=True, help="Points line to train for (e.g., 0.5, 1.5, 2.5).")
    ap.add_argument("--outdir", default="models_out/nhl/points", help="Output directory for models.")
    ap.add_argument("--test-size", type=float, default=0.2, help="Holdout fraction (default 0.2).")
    ap.add_argument("--clip-pp", action="store_true", help="Clip PP-TOI features to sane ranges (recommended).")
    args = ap.parse_args()

    train_path = Path(args.train_csv)
    if not train_path.exists():
        print(f"FATAL: training CSV not found: {train_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(train_path)
    df = normalize_columns(df)

    if "y_points" not in df.columns:
        print("FATAL: training CSV must include 'y_points' (or 'points').", file=sys.stderr)
        sys.exit(2)

    line = float(args.line)
    df["y_over"] = df["y_points"].apply(lambda p: positive_label(p, line)).astype(int)

    feats = pick_features(df, DEFAULT_FEATURES)
    missing_pref = [c for c in DEFAULT_FEATURES if c not in df.columns]
    print(f"Using {len(feats)} features. Missing from preferred list: {missing_pref}", file=sys.stderr)

    if not feats:
        print("FATAL: no usable feature columns found.", file=sys.stderr)
        sys.exit(2)

    # Ensure numeric types + clip if requested
    df = coerce_numeric(df, feats + ["y_over"])
    df = clip_pp_features(df, enable=args.clip_pp)

    # Drop rows missing any chosen feature or target
    before = len(df)
    df = df.dropna(subset=feats + ["y_over"])
    after = len(df)
    if after < before:
        print(f"Dropped {before - after} rows with NaNs in selected features/target.", file=sys.stderr)
    if after < 200:
        print(f"WARNING: small training set after filtering (n={after}).", file=sys.stderr)

    # Class balance info
    pos_rate = float(df["y_over"].mean()) if len(df) else float("nan")
    print(f"Class balance: positive rate = {pos_rate:.3f}", file=sys.stderr)

    X = df[feats].astype(float).values
    y = df["y_over"].values

    # Guarded stratification (only if both classes present)
    strat = y if (np.unique(y).size == 2) else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=strat
    )

    lr = Pipeline([
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(max_iter=300, solver="liblinear", class_weight="balanced")),
    ])
    lr.fit(X_train, y_train)
    lr_auc = float("nan")
    if np.unique(y_test).size == 2:
        lr_auc = roc_auc_score(y_test, lr.predict_proba(X_test)[:, 1])

    rf = RandomForestClassifier(
        n_estimators=400,
        max_depth=None,
        min_samples_leaf=2,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample",
    )
    rf.fit(X_train, y_train)
    rf_auc = float("nan")
    if np.unique(y_test).size == 2:
        rf_auc = roc_auc_score(y_test, rf.predict_proba(X_test)[:, 1])

    line_dir = Path(args.outdir) / f"{str(line).replace('.','_')}"
    line_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(lr, line_dir / "lr.joblib")
    joblib.dump(rf, line_dir / "rf.joblib")

    metadata = {
        "prop_type": "points",
        "line": line,
        "features": feats,
        "version": "v1",
        "trained_from": str(train_path),
        "metrics": {"lr_auc": float(lr_auc), "rf_auc": float(rf_auc)},
        "options": {"clip_pp": bool(args.clip_pp)},
    }
    (line_dir / "feature_metadata.json").write_text(json.dumps(metadata, indent=2))
    (line_dir / "METRICS.json").write_text(json.dumps({
        "n_rows": int(len(df)),
        "pos_rate": pos_rate,
        "lr_auc": float(lr_auc),
        "rf_auc": float(rf_auc),
    }, indent=2))

    print(f"✅ Trained Points models @ line {line:.1f}")
    print(f"   Features ({len(feats)}): {feats}")
    print(f"   AUC — LR: {lr_auc if lr_auc==lr_auc else 'NA'} | RF: {rf_auc if rf_auc==rf_auc else 'NA'}")
    print(f"   Saved to: {line_dir}")

if __name__ == "__main__":
    main()
