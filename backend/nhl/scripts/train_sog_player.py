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
Train NHL SOG models at the player-game level (Denali spine).

Expected input (--train-csv):

  - One row per (season, game_id, player_id)
  - Columns from nhl.training_features_sog_denali_export (or similar),
    e.g.:

      season, game_date, game_id, player_id, team_id, opponent_id,
      shots_on_goal,
      d5_sog_per60, d10_sog_per60, d20_sog_per60,
      attempts_d10_per60,
      team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
      pace_index,
      rest_days, b2b_flag,
      role_pp_share,
      opp_d10_sf_per60, team_d10_sa_per60, opp_d10_sa_per60,
      is_home,
      last10_team_sog_share, hot_last5_flag,
      num_shotwasongoal_last5, num_shotwasongoal_last10,
      num_shotwasongoal_season_to_date,
      num_event_shot_last5, num_event_shot_last10,
      num_event_shot_season_to_date,
      team_num_event_shot_for_last10,
      team_num_shotwasongoal_for_last10, ...

We train a binary model for: Over(line) = (shots_on_goal >= line).

Feature set is taken from backend/nhl/features/feature_metadata_nhl.json
under key "shots_on_goal_denali".
"""

# ---- Denali / feature-registry wiring ----

BACKEND_NHL_DIR = Path(__file__).resolve().parents[1]
FEATURE_REGISTRY_PATH = BACKEND_NHL_DIR / "features" / "feature_metadata_nhl.json"
SOG_FEATURE_KEY = "shots_on_goal_denali"

# Denali IDs (we do NOT treat is_home as an ID; it's a feature)
ID_COLS = {
    "season",
    "game_date",
    "game_id",
    "player_id",
    "team_id",
    "opponent_id",
}

TARGET_COL = "shots_on_goal"

# Columns we never want as features
LEAK_COLS = {
    TARGET_COL,
    "y_over",  # label we construct
}

def load_feature_list() -> list[str]:
    """
    Load the SOG feature list for Denali from the global feature registry JSON.
    """
    if not FEATURE_REGISTRY_PATH.exists():
        print(
            f"FATAL: feature registry not found at {FEATURE_REGISTRY_PATH}",
            file=sys.stderr,
        )
        sys.exit(2)

    meta = json.loads(FEATURE_REGISTRY_PATH.read_text())

    # meta can be { "shots_on_goal_denali": [...] } or nested under "feature_registry"
    if isinstance(meta, dict) and "feature_registry" in meta:
        fr = meta["feature_registry"]
    else:
        fr = meta

    if not isinstance(fr, dict):
        print(
            f"FATAL: unexpected structure in {FEATURE_REGISTRY_PATH}; "
            f"expected an object mapping keys -> feature lists.",
            file=sys.stderr,
        )
        sys.exit(2)

    feats = fr.get(SOG_FEATURE_KEY)
    if not isinstance(feats, list) or not feats:
        print(
            f"FATAL: key '{SOG_FEATURE_KEY}' missing or empty in {FEATURE_REGISTRY_PATH}",
            file=sys.stderr,
        )
        sys.exit(2)

    print(f"Using feature set '{SOG_FEATURE_KEY}' with {len(feats)} columns.", file=sys.stderr)
    return feats

# Default training CSV for SOG Denali
TRAIN_SOG_DENALI_CSV = (
    Path(__file__).resolve().parents[1] / "exports" / "train_sog_denali.csv"
)

def load_denali_training_df(csv_path: str | None = None) -> pd.DataFrame:
    """
    Load SOG Denali *training* data from CSV, enforcing:
      - non-null shots_on_goal
      - numeric dtypes where needed

    This is the *only* source for training the SOG Denali models.
    """
    path = Path(csv_path) if csv_path is not None else TRAIN_SOG_DENALI_CSV
    df = pd.read_csv(path)

    if "shots_on_goal" not in df.columns:
        print(
            f"FATAL: shots_on_goal column missing in {path}", file=sys.stderr
        )
        sys.exit(1)

    # drop rows with null label, just in case
    df = df[df["shots_on_goal"].notna()].copy()
    if df.empty:
        print(
            f"FATAL: no non-null shots_on_goal rows in {path}", file=sys.stderr
        )
        sys.exit(1)

    # Make sure label is numeric
    df["shots_on_goal"] = df["shots_on_goal"].astype(float)

    return df

def build_X_y_for_line(
    df: pd.DataFrame,
    line: float,
    feature_cols: list[str],
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Given the full Denali training df and a target line (e.g., 1.5),
    construct (X, y_over) for classification.

    y_over = 1{ shots_on_goal > line }
    """
    if "shots_on_goal" not in df.columns:
        print("FATAL: shots_on_goal column missing in training df.", file=sys.stderr)
        sys.exit(1)

    # no additional filtering here; df already has only training rows
    df_line = df.copy()

    # Label: over vs under
    y_over = (df_line["shots_on_goal"] > line).astype(int)

    # Make sure both classes are present
    n_classes = y_over.nunique(dropna=True)
    if n_classes < 2:
        print(
            f"FATAL: only one class present in y_over for line {line}.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Feature matrix
    missing_feats = [c for c in feature_cols if c not in df_line.columns]
    if missing_feats:
        print(
            "FATAL: missing feature columns for SOG Denali:\n  "
            + ", ".join(missing_feats),
            file=sys.stderr,
        )
        sys.exit(1)

    X = df_line[feature_cols].astype(float)

    return X, y_over

def main():
    ap = argparse.ArgumentParser(description="Train NHL SOG player-game models (Denali).")
    ap.add_argument(
        "--train-csv",
        required=True,
        help="CSV exported from nhl.training_features_sog_denali_export (or compatible).",
    )
    ap.add_argument(
        "--line",
        type=float,
        required=True,
        help="SOG line to train for, e.g. 1.5, 2.5, 3.5.",
    )
    ap.add_argument(
        "--outdir",
        default="models_out/nhl/sog_player_denali",
        help="Output directory base for models.",
    )
    ap.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Holdout fraction (default 0.2).",
    )
    args = ap.parse_args()

    path = Path(args.train_csv)
    if not path.exists():
        print(f"FATAL: training CSV not found: {path}", file=sys.stderr)
        sys.exit(2)

    # ✅ Use the helper so we’re guaranteed:
    #    - correct schema
    #    - non-null shots_on_goal
    df = load_denali_training_df(str(path))

    line = float(args.line)

    # --- Label: Over(line) ---
    # Recommended: strict "over" (>) rather than >= with an epsilon.
    df["y_over"] = (df[TARGET_COL] > line).astype(int)

    # Load canonical feature list and ensure all are present
    feats = load_feature_list()

    missing = [c for c in feats if c not in df.columns]
    if missing:
        print(
            f"FATAL: training CSV is missing required feature columns: {missing}",
            file=sys.stderr,
        )
        sys.exit(2)

    # Normalize boolean-like features to 0/1 first
    BOOL_FEATURES = {"is_home", "b2b_flag", "hot_last5_flag"}

    for c in feats:
        if c in df.columns and c in BOOL_FEATURES:
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
            )

    # --- Coerce numeric + handle NaNs (Denali-friendly) ---

    # Force everything to numeric first
    for c in feats + ["y_over"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Replace infinities with NaN, then:
    #   - features: fill NaN with 0.0
    #   - label:    fill NaN with 0 and cast to int
    df[feats] = (
        df[feats]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )
    df["y_over"] = (
        df["y_over"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .astype(int)
    )

    after = len(df)
    if after < 500:
        print(f"WARNING: small training set after cleaning (n={after}).", file=sys.stderr)

    X = df[feats].to_numpy(dtype=float)
    y = df["y_over"].to_numpy(dtype=int)

    # Still keep the guard for pathological cases
    if np.unique(y).size < 2:
        print("FATAL: only one class present in y_over.", file=sys.stderr)
        sys.exit(2)

    pos_rate = float(y.mean())
    print(f"Class balance (Over {line}): {pos_rate:.3f}", file=sys.stderr)

    strat = y if np.unique(y).size == 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=42,
        stratify=strat,
    )

    # Logistic Regression model
    lr = Pipeline(
        [
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            (
                "lr",
                LogisticRegression(
                    max_iter=400,
                    solver="liblinear",
                    class_weight="balanced",
                ),
            ),
        ]
    )
    lr.fit(X_train, y_train)
    lr_auc = (
        roc_auc_score(y_test, lr.predict_proba(X_test)[:, 1])
        if np.unique(y_test).size == 2
        else float("nan")
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
        if np.unique(y_test).size == 2
        else float("nan")
    )

    # Save models + metadata under line-specific dir
    outdir = Path(args.outdir) / f"{str(line).replace('.', '_')}"
    outdir.mkdir(parents=True, exist_ok=True)

    joblib.dump(lr, outdir / "lr.joblib")
    joblib.dump(rf, outdir / "rf.joblib")

    metadata = {
        "prop_type": "shots_on_goal",
        "line": line,
        "feature_key": SOG_FEATURE_KEY,
        "features": feats,
        "trained_from": str(path),
        "metrics": {
            "lr_auc": float(lr_auc),
            "rf_auc": float(rf_auc),
        },
        "denali_spine": True,
    }
    (outdir / "feature_metadata.json").write_text(json.dumps(metadata, indent=2))
    (outdir / "METRICS.json").write_text(
        json.dumps(
            {
                "n_rows": int(after),
                "pos_rate": float(pos_rate),
                "lr_auc": float(lr_auc),
                "rf_auc": float(rf_auc),
            },
            indent=2,
        )
    )

    print(f"✅ Trained SOG models for line {line}")
    print(f"   n={after}, pos_rate={pos_rate:.3f}")
    print(f"   Features ({len(feats)}): {feats}")
    print(f"   Saved to: {outdir}", file=sys.stderr)


if __name__ == "__main__":
    main()
