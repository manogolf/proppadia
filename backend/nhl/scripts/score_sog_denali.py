#!/usr/bin/env python3
"""
score_sog_phoenix.py

Use phoenix-trained SOG models to score a slate.

Inputs:
  --features-csv : CSV exported for the slate (now from the Denali union view)
  --model-root   : root dir containing per-line subdirs
                   e.g. backend/nhl/models/latest/shots_on_goal/sog_player_v2
  --out          : output CSV path

Models layout (per line):
  backend/nhl/models/latest/shots_on_goal/sog_player_v2/
    1_5/
      lr.joblib
    2_5/
      lr.joblib
    3_5/
      lr.joblib
    4_5/
      lr.joblib

Features:
  Loaded from backend/nhl/features/feature_metadata_nhl.json

We DO NOT read per-line feature_metadata.json anymore.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg2


# Feature registry (single source of truth)
FEATURE_REGISTRY_PATH = Path("backend/nhl/features/feature_metadata_nhl.json")

SOG_FEATURE_KEYS_PREFERENCE = [
    "shots_on_goal_denali",
]

def load_sog_denali_features(csv_path: str):
    """
    Load a Denali SOG features CSV and return:
      - df:   full DataFrame with all feature columns
      - meta: DataFrame with identifiers + outcome
    """

    df = pd.read_csv(csv_path)

    # ---- Alias / fill missing features that have clear semantics ----
    # pace_matchup_index := pace_index (by design, until we have a real matchup metric)
    if "pace_matchup_index" not in df.columns and "pace_index" in df.columns:
        print("[info] slate CSV missing pace_matchup_index; filling from pace_index")
        df["pace_matchup_index"] = df["pace_index"]

    meta_cols = [
        "season",
        "game_date",
        "game_id",
        "player_id",
        "team_id",
        "opponent_id",
        "shots_on_goal",
    ]
    meta = df[meta_cols].copy()

    return df, meta

def export_sog_denali_features(db_url: str, slate_date: str, out_path: Path) -> None:
    """
    Export all columns from nhl.training_features_sog_denali_export
    for the given slate_date into out_path.

    This keeps the CSV in lockstep with the view definition.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sql = f"""
    COPY (
      SELECT *
      FROM nhl.training_features_sog_denali_export
      WHERE game_date = DATE '{slate_date}'
      ORDER BY game_id, player_id
    ) TO STDOUT WITH CSV HEADER
    """

    with psycopg2.connect(db_url) as conn, conn.cursor() as cur, open(out_path, "w", newline="") as f:
        cur.copy_expert(sql, f)

def load_feature_list() -> list[str]:
    """
    Load the SOG feature list from the global feature registry JSON.

    We prefer the "shots_on_goal" key (v2 spec). If not present, we try the other
    keys in SOG_FEATURE_KEYS_PREFERENCE.
    """
    if not FEATURE_REGISTRY_PATH.exists():
        raise SystemExit(
            f"FATAL: feature registry not found at {FEATURE_REGISTRY_PATH}. "
            f"Expected a JSON file with SOG feature list."
        )

    meta = json.loads(FEATURE_REGISTRY_PATH.read_text())

    # meta can be either:
    # - { "shots_on_goal": [..], "shots_on_goal_phoenix": [..], ... }
    # - or nested under "features"/"feature_registry"
    if isinstance(meta, dict) and "feature_registry" in meta:
        fr = meta["feature_registry"]
    else:
        fr = meta

    if not isinstance(fr, dict):
        raise SystemExit(
            f"FATAL: unexpected structure in {FEATURE_REGISTRY_PATH}; "
            f"expected an object mapping keys -> feature lists."
        )

    for key in SOG_FEATURE_KEYS_PREFERENCE:
        if key in fr:
            feats = fr[key]
            if not isinstance(feats, list) or not feats:
                raise SystemExit(
                    f"FATAL: feature key '{key}' in {FEATURE_REGISTRY_PATH} is empty or not a list."
                )
            print(f"Using SOG feature set from key '{key}' in feature registry.")
            return feats

    raise SystemExit(
        f"FATAL: none of {SOG_FEATURE_KEYS_PREFERENCE} found in {FEATURE_REGISTRY_PATH}. "
        f"Please add a SOG feature list there."
    )


def load_line_model(line_dir: Path):
    """
    Load LR model for a specific line directory.

    We expect:
      line_dir / lr.joblib
    Feature list is global (from FEATURE_REGISTRY_PATH), not stored here.
    """
    lr_path = line_dir / "lr.joblib"
    if not lr_path.exists():
        return None
    lr = joblib.load(lr_path)
    return lr


def prepare_features(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """
    Given the full Denali df and a feature list, build the X matrix
    in the correct column order, applying minimal aliases (e.g. home_flag).
    """
    X = df.copy()

    # Minimal / explicit aliasing only.
    # home_flag <- is_home (if model expects home_flag and CSV has is_home).
    if "home_flag" in feats and "home_flag" not in X.columns and "is_home" in X.columns:
        X["home_flag"] = X["is_home"].astype(int)

    # Coerce numeric where appropriate.
    for c in feats:
        if c in X.columns:
            X[c] = pd.to_numeric(X[c], errors="coerce")

    # Build matrix strictly in feature order
    try:
        X_mat = X[feats].astype(float)
    except KeyError as e:
        missing = [m for m in feats if m not in X.columns]
        raise SystemExit(
            f"FATAL: missing required feature columns in slate CSV: {missing}"
        ) from e

    # NOTE: We assume training already handled NaNs/scale appropriately.
    # If any NaNs sneak in here (e.g., totally empty features for today),
    # caller should decide whether to drop or treat as zero. For now,
    # mirror training behavior and drop rows with NaNs upstream if needed.
    return X_mat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-csv", required=True)
    ap.add_argument("--model-root", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    features_path = Path(args.features_csv)
    if not features_path.exists():
        raise SystemExit(f"FATAL: features CSV not found: {features_path}")

    # Load Denali features: full df + meta (ids/outcome)
    df, meta = load_sog_denali_features(features_path)

    # Guard: empty slate (header only)
    if meta.shape[0] == 0:
        print(f"ℹ️ No rows in features CSV ({features_path}); nothing to score for this slate.")
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=["player_id", "game_id", "line", "prob_over", "model"]
        ).to_csv(out_path, index=False)
        return

    # Required columns guard (check against meta)
    for col in ("player_id", "game_id"):
        if col not in meta.columns:
            raise SystemExit(f"FATAL: features CSV missing required column '{col}'")

    # Load global SOG feature list (v2 spec preferred)
    feats = load_feature_list()

    model_root = Path(args.model_root)
    if not model_root.exists():
        raise SystemExit(f"FATAL: model root not found: {model_root}")

    # Discover per-line model dirs (e.g., 1_5, 2_5, 3_5, 4_5)
    line_dirs = []
    for d in sorted(model_root.iterdir()):
        if not d.is_dir():
            continue
        try:
            line = float(d.name.replace("_", "."))
        except ValueError:
            continue
        line_dirs.append((line, d))

    if not line_dirs:
        raise SystemExit(f"FATAL: no line subdirs found under {model_root}")

    out_rows = []

    for line, line_dir in line_dirs:
        lr = load_line_model(line_dir)
        if lr is None:
            print(f"[warn] skipping line {line}: no lr.joblib in {line_dir}")
            continue

        # Validate features presence up front against df (Denali CSV)
        missing = [
            c
            for c in feats
            if c not in df.columns
            and not (c == "home_flag" and "is_home" in df.columns)
        ]
        if missing:
            raise SystemExit(
                f"FATAL: line {line}: slate CSV missing required features: {missing}"
            )

        X = prepare_features(df, feats)
        if X.shape[0] == 0:
            print(f"[warn] line {line}: no usable rows after feature selection; skipping.")
            continue

        # --- NaN guardrail for Denali features ---
        # LogisticRegression (and the scaler) can't handle NaNs; fill numeric features.
        nan_counts = X.isna().sum()
        if nan_counts.any():
            print(
                f"⚠️ NaNs in feature matrix for line {line}; filling with 0. "
                "Counts by column:",
                {col: int(cnt) for col, cnt in nan_counts.items() if cnt > 0},
            )
            X = X.fillna(0.0)

        try:
            proba = lr.predict_proba(X)
        except Exception as e:
            raise SystemExit(f"FATAL: predict_proba failed for line {line}: {e}")

        p_over = proba[:, 1]

        for (player_id, game_id), prob in zip(
            zip(meta["player_id"].values, meta["game_id"].values),
            p_over,
        ):
            out_rows.append(
                {
                    "player_id": int(player_id),
                    "game_id": int(game_id),
                    "line": float(line),
                    "prob_over": float(prob),
                    "model": "sog_phoenix_lr",
                }
            )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not out_rows:
        print("ℹ️ No predictions generated; writing empty file.")
        pd.DataFrame(
            columns=["player_id", "game_id", "line", "prob_over", "model"]
        ).to_csv(out_path, index=False)
        return

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(out_path, index=False)
    print(f"✅ wrote {len(out_df)} SOG predictions to {out_path}")


if __name__ == "__main__":
    main()
