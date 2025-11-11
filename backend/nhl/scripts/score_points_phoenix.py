#!/usr/bin/env python3
"""
score_points_phoenix.py

Use phoenix-trained per-line logistic models to score today's slate.

Inputs:
  --features-csv : CSV from backend/nhl/sql/export_points.sql
  --model-root   : root dir containing per-line subdirs (e.g. models_out/nhl/points_phoenix)
  --out          : output CSV path

Output schema (long-form):
  player_id, game_id, line, prob_over, model
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


def load_line_model(line_dir: Path):
    """
    Expect:
      line_dir / "lr.joblib"
      line_dir / "feature_metadata.json" with {"features": [...]}
    """
    meta_path = line_dir / "feature_metadata.json"
    lr_path = line_dir / "lr.joblib"

    if not meta_path.exists() or not lr_path.exists():
        return None, None

    meta = json.loads(meta_path.read_text())
    feats = (
        meta.get("features")
        or meta.get("feature_cols")
        or meta.get("feature_names")
        or []
    )
    if not feats:
        return None, None

    lr = joblib.load(lr_path)
    return feats, lr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-csv", required=True)
    ap.add_argument("--model-root", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    features_path = Path(args.features_csv)
    if not features_path.exists():
        raise SystemExit(f"FATAL: features CSV not found: {features_path}")

    df = pd.read_csv(features_path)

    # Guard: header-only / empty slate
    if df.shape[0] == 0:
        print(f"ℹ️ No rows in features CSV ({features_path}); nothing to score for this slate.")
        # Write an empty predictions file with header for downstream sanity if desired
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=["player_id", "game_id", "line", "prob_over", "model"]
        ).to_csv(out_path, index=False)
        return

    for col in ("player_id", "game_id"):
        if col not in df.columns:
            raise SystemExit(f"FATAL: features CSV missing required column '{col}'")

    model_root = Path(args.model_root)
    if not model_root.exists():
        raise SystemExit(f"FATAL: model root not found: {model_root}")

    # Discover per-line subdirs: e.g. 0_5, 1_5, 2_5
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
        feats, lr = load_line_model(line_dir)
        if not feats or lr is None:
            print(f"[warn] skipping line {line}: missing model or feature list in {line_dir}")
            continue

        # Ensure all required features exist
        missing = [c for c in feats if c not in df.columns]
        if missing:
            print(f"[warn] line {line}: missing features in slate CSV: {missing} — skipping this line")
            continue

        # Build feature matrix for this line
        X = df[feats].astype(float).values

        if X.shape[0] == 0:
            print(f"[warn] line {line}: no usable rows after feature selection; skipping.")
            continue

        try:
            proba = lr.predict_proba(X)
        except Exception as e:
            raise SystemExit(f"FATAL: predict_proba failed for line {line}: {e}")

        p_over = proba[:, 1]

        for (player_id, game_id), prob in zip(
            zip(df["player_id"].values, df["game_id"].values),
            p_over,
        ):
            out_rows.append(
                {
                    "player_id": int(player_id),
                    "game_id": int(game_id),
                    "line": float(line),
                    "prob_over": float(prob),
                    "model": "points_phoenix_lr",
                }
            )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not out_rows:
        print("ℹ️ No predictions generated (no matching models or features); writing empty file.")
        pd.DataFrame(
            columns=["player_id", "game_id", "line", "prob_over", "model"]
        ).to_csv(out_path, index=False)
        return

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(out_path, index=False)

    print(f"✅ wrote {len(out_df)} point-prop predictions to {out_path}")


if __name__ == "__main__":
    main()
