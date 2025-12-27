#!/usr/bin/env python3
"""
Score NHL SOG (Denali) models for a single slate.

Inputs:
  --features-csv : CSV exported from export_sog_denali_pregame.sql
                   (one row per player-game with Denali features)
  --model-root   : Directory containing per-line subdirs:
                     <model-root>/0_5/{lr.joblib, rf.joblib, feature_metadata.json}
                     <model-root>/1_5/...
                     <model-root>/2_5/...
                     <model-root>/3_5/...
  --out          : Output CSV (one row per player-game-line) with:
                     player_id, game_id, team_id, opponent_id, is_home, game_date, season,
                     prop_type, line, prob_over, prob_over_lr, prob_over_rf

This is prediction-only and kept separate from the training export.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Lines we train/score for
SOG_LINES = [0.5, 1.5, 2.5, 3.5]

# Bool-like features to coerce to 0/1
BOOL_FEATURES = {"is_home", "b2b_flag", "hot_last5_flag"}

# ID/context columns we expect in the features CSV
ID_COLS = [
    "player_id",
    "game_id",
    "team_id",
    "opponent_id",
    "is_home",
    "game_date",
    "season",
]


def line_dir_name(line: float) -> str:
    """Convert 0.5 -> '0_5', 1.5 -> '1_5', etc."""
    return str(line).replace(".", "_")


def load_line_models(model_root: Path, line: float):
    """
    Load LR + RF models and feature list for a given line from:
      <model_root>/<dir_name>/
    """
    dname = line_dir_name(line)
    line_dir = model_root / dname
    if not line_dir.exists():
        print(f"FATAL: model directory missing for line {line}: {line_dir}", file=sys.stderr)
        sys.exit(2)

    lr_path = line_dir / "lr.joblib"
    rf_path = line_dir / "rf.joblib"
    meta_path = line_dir / "feature_metadata.json"

    if not lr_path.exists() or not rf_path.exists():
        print(
            f"FATAL: missing lr/rf models for line {line} in {line_dir}",
            file=sys.stderr,
        )
        sys.exit(2)

    if not meta_path.exists():
        print(
            f"FATAL: feature_metadata.json missing for line {line} in {line_dir}",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        lr = joblib.load(lr_path)
        rf = joblib.load(rf_path)
    except Exception as e:
        print(f"FATAL: failed to load models for line {line}: {e}", file=sys.stderr)
        sys.exit(2)

    # --- Canonical feature selection (Denali SOG) ---
    # Priority:
    #   1) model.feature_names_in_ (if present)
    #   2) backend/nhl/features/feature_metadata_nhl.json -> shots_on_goal_denali
    # And we *loudly fail* if the per-line feature_metadata.json disagrees (prevents drift).

    # Load per-line model metadata features (kept for drift detection)
    try:
        line_meta = json.loads(meta_path.read_text())
        line_feats = line_meta.get("features") or []
    except Exception as e:
        print(f"FATAL: failed to read features from {meta_path}: {e}", file=sys.stderr)
        sys.exit(2)

    if not line_feats:
        print(
            f"FATAL: empty feature list in metadata for line {line} ({meta_path})",
            file=sys.stderr,
        )
        sys.exit(2)

    # Load repo-level canonical Denali feature list
    repo_meta_path = Path(__file__).resolve().parents[1] / "features" / "feature_metadata_nhl.json"
    try:
        repo_meta = json.loads(repo_meta_path.read_text())
        canonical_feats = repo_meta.get("shots_on_goal_denali") or []
    except Exception as e:
        print(f"FATAL: failed to read {repo_meta_path}: {e}", file=sys.stderr)
        sys.exit(2)

    if not isinstance(canonical_feats, list) or not canonical_feats:
        print(
            f"FATAL: shots_on_goal_denali missing/empty in {repo_meta_path}",
            file=sys.stderr,
        )
        sys.exit(2)

    # Prefer model-native feature list if available (strongest contract)
    model_feats = None
    try:
        model_feats = list(getattr(lr, "feature_names_in_", [])) or None
    except Exception:
        model_feats = None

    feats = model_feats if model_feats is not None else list(canonical_feats)

    # Drift checks (fail loudly so scorer cannot silently score with the wrong set)
    if set(feats) != set(canonical_feats):
        only_in_feats = sorted(set(feats) - set(canonical_feats))
        only_in_canon = sorted(set(canonical_feats) - set(feats))
        print(
            "FATAL: feature set mismatch vs canonical shots_on_goal_denali.\n"
            f"  canonical: {repo_meta_path}\n"
            f"  line: {line} ({line_dir})\n"
            f"  only_in_feats({len(only_in_feats)}): {only_in_feats}\n"
            f"  only_in_canonical({len(only_in_canon)}): {only_in_canon}",
            file=sys.stderr,
        )
        sys.exit(2)

    if set(line_feats) != set(canonical_feats):
        only_in_line = sorted(set(line_feats) - set(canonical_feats))
        only_in_canon2 = sorted(set(canonical_feats) - set(line_feats))
        print(
            "FATAL: per-line feature_metadata.json disagrees with canonical shots_on_goal_denali.\n"
            f"  meta_path: {meta_path}\n"
            f"  canonical: {repo_meta_path}\n"
            f"  line: {line}\n"
            f"  only_in_line_meta({len(only_in_line)}): {only_in_line}\n"
            f"  only_in_canonical({len(only_in_canon2)}): {only_in_canon2}",
            file=sys.stderr,
        )
        sys.exit(2)
    # --- end canonical feature selection ---

    return lr, rf, feats


def prepare_features(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """
    Prepare feature frame:
      - Normalize bool-like columns to 0/1
      - Coerce to numeric
      - Replace inf with NaN, then fill with 0.0
    Mirrors the trainer's behavior.
    """
    df_feat = df.copy()

    # Bool normalization
    for c in feats:
        if c in df_feat.columns and c in BOOL_FEATURES:
            df_feat[c] = (
                df_feat[c]
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

    # Coerce to numeric
    for c in feats:
        if c not in df_feat.columns:
            continue
        df_feat[c] = pd.to_numeric(df_feat[c], errors="coerce")

    # Replace infinities with NaN, then fill NaN with 0.0
    df_feat[feats] = (
        df_feat[feats]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    return df_feat[feats]

def report_feature_coverage(X_df, feature_list, tag=""):
    """
    Lightweight sanity report:
      - top null features
      - constant-ish features (nunique<=1) + sample value
    """
    n = len(X_df)
    if n == 0:
        print(f"[feature_check]{tag} rows=0 (nothing to score)")
        return

    # Nulls per feature
    nulls = X_df.isna().sum().sort_values(ascending=False)
    top_nulls = nulls[nulls > 0].head(15)

    # Constant-ish features (nunique<=1)
    nunique = X_df.nunique(dropna=True)
    const = nunique[nunique <= 1].sort_values()

    print(f"[feature_check]{tag} rows={n} features={len(feature_list)}")

    if len(top_nulls) > 0:
        pct = (top_nulls / n * 100).round(1)
        print(f"[feature_check]{tag} top_null_features (count / %):")
        for c in top_nulls.index:
            print(f"  - {c}: {int(top_nulls[c])} / {pct[c]}%")
    else:
        print(f"[feature_check]{tag} nulls: none ✅")

    if len(const) > 0:
        print(f"[feature_check]{tag} constant_features (nunique<=1): {len(const)}")

        # Print every constant feature + a sample value so we can see if it's all 0/1/etc.
        # (Use the first non-null if available; else None.)
        for c in const.index:
            series = X_df[c]
            sample = None
            try:
                non_null = series.dropna()
                if len(non_null) > 0:
                    sample = non_null.iloc[0]
            except Exception:
                sample = None

            # Keep formatting readable
            if isinstance(sample, float):
                sample_str = f"{sample:.6g}"
            else:
                sample_str = str(sample)

            print(f"  - {c}: nunique={int(const[c])} sample={sample_str}")
    else:
        print(f"[feature_check]{tag} constant_features: none ✅")


def main():
    ap = argparse.ArgumentParser(description="Score NHL SOG Denali models for a slate.")
    ap.add_argument(
        "--features-csv",
        required=True,
        help="Pregame features CSV from export_sog_denali_pregame.sql",
    )
    ap.add_argument(
        "--model-root",
        required=True,
        help="Root directory containing per-line Denali SOG models.",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output CSV path for SOG predictions.",
    )
    args = ap.parse_args()

    features_path = Path(args.features_csv)
    if not features_path.exists():
        print(f"FATAL: features CSV not found: {features_path}", file=sys.stderr)
        sys.exit(2)

    model_root = Path(args.model_root)
    if not model_root.exists():
        print(f"FATAL: model_root does not exist: {model_root}", file=sys.stderr)
        sys.exit(2)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Load features CSV
    df = pd.read_csv(features_path)
    if df.empty:
        print(f"FATAL: features CSV {features_path} is empty.", file=sys.stderr)
        sys.exit(2)

    missing_ids = [c for c in ID_COLS if c not in df.columns]
    if missing_ids:
        print(
            f"FATAL: features CSV missing required ID/context columns: {missing_ids}",
            file=sys.stderr,
        )
        sys.exit(2)

    # Base identity/context frame to carry through
    df_ids = df[ID_COLS].copy()

    # Collect all per-line results
    out_rows = []

    print(
        f"[score_sog_denali] Scoring {len(df)} player-game rows "
        f"with models from {model_root}",
        file=sys.stderr,
    )

    # We'll reuse the same StandardScaler behavior from training:
    # note: the LR pipeline inside joblib already contains scaling,
    # so we only need to feed raw numeric features; no external scaler.

    for line in SOG_LINES:
        print(f"[score_sog_denali] Loading models for line {line}", file=sys.stderr)
        lr, rf, feats = load_line_models(model_root, line)

        # Ensure the CSV has all needed features
        missing_feats = [c for c in feats if c not in df.columns]
        if missing_feats:
            print(
                f"FATAL: features CSV is missing required columns for line {line}: "
                f"{missing_feats}",
                file=sys.stderr,
            )
            sys.exit(2)

        # Prepare features
        X_df = prepare_features(df, feats)
        report_feature_coverage(X_df, feats, tag=f" line={line}")
        X = X_df.to_numpy(dtype=float)

        # Predict probabilities for Over(line)
        # Assumes both models are binary classifiers with predict_proba
        try:
            p_lr = lr.predict_proba(X)[:, 1]
        except Exception as e:
            print(f"FATAL: LR predict_proba failed for line {line}: {e}", file=sys.stderr)
            sys.exit(2)

        try:
            p_rf = rf.predict_proba(X)[:, 1]
        except Exception as e:
            print(f"FATAL: RF predict_proba failed for line {line}: {e}", file=sys.stderr)
            sys.exit(2)

        prob_over = 0.5 * (p_lr + p_rf)

        # Build per-line output frame
        df_line = df_ids.copy()
        df_line["prop_type"] = "shots_on_goal"
        df_line["line"] = float(line)
        df_line["prob_over_lr"] = p_lr
        df_line["prob_over_rf"] = p_rf
        df_line["prob_over"] = prob_over

        out_rows.append(df_line)

    if not out_rows:
        print("FATAL: no predictions produced.", file=sys.stderr)
        sys.exit(2)

    # Concatenate all lines into one long frame
    df_out = pd.concat(out_rows, axis=0, ignore_index=True)

    # Optional: sort for stable ordering
    sort_cols = [c for c in ("game_date", "game_id", "team_id", "player_id", "line") if c in df_out.columns]
    if sort_cols:
        df_out = df_out.sort_values(sort_cols).reset_index(drop=True)

    df_out.to_csv(out_path, index=False)
    print(
        f"[score_sog_denali] Wrote {len(df_out)} rows to {out_path}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
