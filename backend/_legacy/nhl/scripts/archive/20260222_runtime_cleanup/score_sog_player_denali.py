# backend/nhl/scripts/score_sog_player_denali.py
"""
Score NHL SOG (Denali) for a single line using trained LR+RF models.

Usage example:
  python backend/nhl/scripts/score_sog_player_denali.py \
    --features-csv backend/nhl/exports/sog_features_today_denali.csv \
    --line 1.5 \
    --models-root models_out/nhl/sog_player_denali \
    --out-csv backend/nhl/exports/sog_predictions_1_5.csv

Notes:
- This is a *prediction-only* script. Training stays in train_sog_player.py.
- It expects the same Denali feature set used during training.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


SOG_FEATURE_KEY = "shots_on_goal_denali_pairings_v1"
TARGET_COL = "shots_on_goal"  # present in training data, ignored for scoring

# Must match the boolean features we normalized in train_sog_player.py
BOOL_FEATURES = {"is_home", "b2b_flag", "hot_last5_flag"}


def load_feature_list_for_line(models_root: Path, line: float) -> list[str]:
    """
    Load the canonical Denali feature list for this line from feature_metadata.json.
    """
    line_dir = str(line).replace(".", "_")
    md_path = models_root / line_dir / "feature_metadata.json"
    if not md_path.exists():
        print(f"FATAL: metadata not found for line {line} at {md_path}", file=sys.stderr)
        sys.exit(2)

    metadata = json.loads(md_path.read_text())
    key = metadata.get("feature_key")
    if key != SOG_FEATURE_KEY:
        print(
            f"WARNING: feature_key mismatch for line {line}: {key} (expected {SOG_FEATURE_KEY})",
            file=sys.stderr,
        )
    feats = metadata.get("features") or []
    if not feats:
        print(f"FATAL: no features listed in metadata for line {line}", file=sys.stderr)
        sys.exit(2)

    return feats


def normalize_booleans_inplace(df: pd.DataFrame, feats: list[str]) -> None:
    """
    Normalize boolean-like features to 0/1, in-place, just like training.

    IMPORTANT:
      - Hard-fail if any required feature in `feats` is missing from the CSV.
        (Prevents silent zero-fill / constant features from missing columns.)
      - Extra columns in the CSV are allowed (future buildouts).
    """
    missing = [c for c in feats if c not in df.columns]
    if missing:
        raise RuntimeError(
            "[DENALI_SOG] Features CSV missing required columns (hard error; no fill).\n"
            f"Missing ({len(missing)}): {missing}"
        )

    # Non-fatal visibility into forward-compatible columns
    extra = sorted(set(df.columns) - set(feats))
    if extra:
        print(f"[DENALI_SOG] extra_in_csv (ignored): {extra}")

    for c in feats:
        if c not in BOOL_FEATURES:
            continue

        df[c] = (
            df[c]
            .replace(
                {
                    True: 1,
                    False: 0,
                    "t": 1,
                    "f": 0,
                    "true": 1,
                    "false": 0,
                    "True": 1,
                    "False": 0,
                }
            )
            .infer_objects(copy=False)
        )


def prepare_features(df: pd.DataFrame, feats: list[str]) -> np.ndarray:
    """
    Coerce the Denali feature columns to numeric, replace inf/NaN, and return X.
    """

    # Only operate on the feature subset
    feat_df = df[feats].copy()

    # Coerce to numeric
    for c in feats:
        feat_df[c] = pd.to_numeric(feat_df[c], errors="coerce")

    # Replace infinities with NaN, then fill with 0.0
    feat_df = (
        feat_df.replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    X = feat_df.to_numpy(dtype=float)
    return X


def main() -> None:
    ap = argparse.ArgumentParser(description="Score NHL SOG (Denali) for a single line.")
    ap.add_argument(
        "--features-csv",
        required=True,
        help="Input features CSV for the slate (Denali spine).",
    )
    ap.add_argument(
        "--line",
        type=float,
        required=True,
        help="SOG line to score for, e.g. 1.5, 2.5, 3.5.",
    )
    ap.add_argument(
        "--models-root",
        default="models_out/nhl/sog_player_denali",
        help="Root dir where line-specific models live (each line has lr.joblib / rf.joblib / feature_metadata.json).",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Path to write predictions CSV (input columns + p_over_*).",
    )
    args = ap.parse_args()

    features_path = Path(args.features_csv)
    if not features_path.exists():
        print(f"FATAL: features CSV not found: {features_path}", file=sys.stderr)
        sys.exit(2)

    models_root = Path(args.models_root)
    line = float(args.line)
    line_dir = str(line).replace(".", "_")
    model_dir = models_root / line_dir

    if not model_dir.exists():
        print(f"FATAL: model directory not found for line {line}: {model_dir}", file=sys.stderr)
        sys.exit(2)

    lr_path = model_dir / "lr.joblib"
    rf_path = model_dir / "rf.joblib"
    if not lr_path.exists() or not rf_path.exists():
        print(
            f"FATAL: missing lr or rf model in {model_dir} (expected lr.joblib and rf.joblib).",
            file=sys.stderr,
        )
        sys.exit(2)

    # Load input features
    df = pd.read_csv(features_path)

    # Load canonical features for this line
    feats = load_feature_list_for_line(models_root, line)

    # Normalize boolean-like columns
    normalize_booleans_inplace(df, feats)

    # Prepare feature matrix
    X = prepare_features(df, feats)

    # Load models
    lr = joblib.load(lr_path)
    rf = joblib.load(rf_path)

    # Predict probabilities of Over(line)
    prob_lr = lr.predict_proba(X)[:, 1]
    prob_rf = rf.predict_proba(X)[:, 1]

    # Simple 50/50 blend (can be tuned later)
    prob_blend = 0.5 * prob_lr + 0.5 * prob_rf

    # Attach to dataframe with line-specific column names
    suffix = str(line).replace(".", "_")
    df[f"line"] = line
    df[f"p_over_lr_{suffix}"] = prob_lr
    df[f"p_over_rf_{suffix}"] = prob_rf
    df[f"p_over_{suffix}"] = prob_blend

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(
        f"✅ Scored SOG Denali for line {line} on {len(df)} rows.\n"
        f"   Models: {model_dir}\n"
        f"   Output: {out_path}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
