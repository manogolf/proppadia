# backend/nhl/scripts/calibrate_sog_denali.py
"""
Calibrate Denali SOG probabilities line-by-line using historical outcomes.

Workflow:
  1) Build calibration training set (already done):
       psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
         -f backend/nhl/sql/export_sog_denali_calibration_training.sql

  2) Run this script to fit per-line calibration curves and
     apply them to sog_predictions.csv:

    python backend/nhl/scripts/calibrate_sog_denali.py \
    --train-csv backend/nhl/data/processed/sog_calibration_training_denali.csv \
    --wide-in  backend/nhl/data/processed/sog_predictions.csv \
    --wide-out backend/nhl/data/processed/sog_predictions_wide_calibrated.csv \
    --blend-alpha 0.175


  3) (Optional) Check calibration with debug_sog_raw_vs_calibrated.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Calibrate Denali SOG probs per line.")
    ap.add_argument(
        "--train-csv",
        default="backend/nhl/data/processed/sog_calibration_training_denali.csv",
        help="Calibration training CSV (season, game_date, line, prob_over, y_over).",
    )
    ap.add_argument(
        "--wide-in",
        default="backend/nhl/data/processed/sog_predictions.csv",
        help="Input wide predictions CSV (Denali sog_predictions.csv).",
    )
    ap.add_argument(
        "--wide-out",
        default="backend/nhl/data/processed/sog_predictions_wide_calibrated.csv",
        help="Output wide CSV with calibrated p_over_* columns.",
    )
    ap.add_argument(
        "--curves-json",
        default="backend/nhl/data/processed/sog_denali_calibration_curves.json",
        help="Where to write fitted calibration curve metadata.",
    )
    ap.add_argument(
        "--min-rows",
        type=int,
        default=500,
        help="Minimum rows per line to attempt calibration; otherwise identity.",
    )
    ap.add_argument(
        "--blend-alpha",
        type=float,
        default=0.5,
        help=(
            "Blend weight between raw and isotonic-calibrated probs: "
            "p_calib = (1-alpha)*p_raw + alpha*p_iso. "
            "Use 0.0 for identity, 1.0 for full isotonic."
        ),
    )
    return ap.parse_args()


def _fit_line_isotonic(
    df: pd.DataFrame, line: float, blend_alpha: float
) -> Tuple[Dict[str, Any], callable]:
    """
    Fit an isotonic regression calibrator for a single line.

    Returns:
      meta: summary dict (for JSON)
      calibrator: function f(p_raw) -> p_calib
    """
    # Safety: filter out NaNs and clip to [0,1]
    df = df.dropna(subset=["prob_over", "y_over"]).copy()
    n = len(df)

    if n == 0:
        # Fallback: identity mapping
        def identity(p: np.ndarray) -> np.ndarray:
            return p

        meta = {"status": "no_data", "rows": 0}
        return meta, identity

    x = df["prob_over"].to_numpy(dtype=float)
    y = df["y_over"].to_numpy(dtype=float)

    x = np.clip(x, 0.0, 1.0)

    # Isotonic regression: monotone non-decreasing p_raw -> hit_rate
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(x, y)

    # Store curve for debugging (piecewise-constant knots)
    x_t = iso.X_thresholds_.tolist()
    y_t = iso.y_thresholds_.tolist()

    # Blend with raw to avoid over-flattening
    alpha = float(blend_alpha)

    def calibrator(prob: np.ndarray) -> np.ndarray:
        p = np.asarray(prob, dtype=float)
        out = np.full_like(p, np.nan, dtype=float)

        mask = ~np.isnan(p)
        if not mask.any():
            return out

        p_valid = np.clip(p[mask], 0.0, 1.0)
        p_iso = iso.predict(p_valid)
        # Soft blend: keep relative spread but move toward calibrated curve
        p_blend = (1.0 - alpha) * p_valid + alpha * p_iso
        out[mask] = p_blend
        return out

    meta = {
        "status": "ok",
        "rows": int(n),
        "line": float(line),
        "type": "isotonic",
        "blend_alpha": alpha,
        "x_thresholds": x_t,
        "y_thresholds": y_t,
    }
    return meta, calibrator


def main() -> None:
    args = parse_args()

    train_path = Path(args.train_csv)
    wide_in_path = Path(args.wide_in)
    wide_out_path = Path(args.wide_out)
    curves_json_path = Path(args.curves_json)

    if not train_path.exists():
        raise SystemExit(f"Training CSV not found: {train_path}")
    if not wide_in_path.exists():
        raise SystemExit(f"Wide predictions CSV not found: {wide_in_path}")

    print(f"▶ Loading calibration training data from {train_path} ...")
    train_df = pd.read_csv(train_path)

    required_cols = {"season", "game_date", "line", "prob_over", "y_over"}
    missing = required_cols - set(train_df.columns)
    if missing:
        raise SystemExit(f"Training CSV missing required columns: {sorted(missing)}")

    # Ensure numeric types where needed
    train_df["line"] = pd.to_numeric(train_df["line"], errors="coerce")
    train_df["prob_over"] = pd.to_numeric(train_df["prob_over"], errors="coerce")
    train_df["y_over"] = pd.to_numeric(train_df["y_over"], errors="coerce")

    # Restrict to valid rows
    train_df = train_df.dropna(subset=["line", "prob_over", "y_over"])

    print(f"  → Loaded {len(train_df)} training rows.")

    summary = (
        train_df.groupby("line")
        .agg(
            n=("y_over", "count"),
            avg_p=("prob_over", "mean"),
            hit_rate=("y_over", "mean"),
        )
        .reset_index()
    )

    print("Per-line summary from training:")
    print(summary.to_string(index=False))

    # Fit calibration per line
    curves_fitted: Dict[str, Any] = {}
    calibrators: Dict[str, Any] = {}
    min_rows = args.min_rows

    for line in sorted(train_df["line"].unique()):
        df_line = train_df[train_df["line"] == line].copy()
        n = len(df_line)
        line_tag = str(line).replace(".", "_")

        if n < min_rows:
            print(
                f"⚠️  Line {line}: only {n} rows (< {min_rows}); using identity mapping."
            )

            def identity(p: np.ndarray) -> np.ndarray:
                return p

            curves_fitted[line_tag] = {
                "status": "skipped_insufficient_rows",
                "rows": int(n),
                "line": float(line),
                "type": "identity",
                "blend_alpha": 0.0,
            }
            calibrators[line_tag] = identity
            continue

        meta, cal_fn = _fit_line_isotonic(df_line, line, args.blend_alpha)
        curves_fitted[line_tag] = meta
        calibrators[line_tag] = cal_fn
        print(
            f"✅ Fitted isotonic calibration for line {line}: "
            f"rows={meta['rows']}, blend_alpha={meta['blend_alpha']}"
        )

    # Load wide predictions and apply calibration
    print(f"\n▶ Loading wide predictions from {wide_in_path} ...")
    wide_df = pd.read_csv(wide_in_path)
    orig_cols = list(wide_df.columns)

    # Identify blend columns only (p_over_0_5, p_over_1_5, ...) and ignore lr/rf columns
    prob_cols: List[str] = [
        c
        for c in wide_df.columns
        if c.startswith("p_over_")
        and not c.startswith("p_over_lr_")
        and not c.startswith("p_over_rf_")
    ]

    print(f"  → Found {len(prob_cols)} p_over_* blend columns to calibrate: {prob_cols}")

    lines_seen_in_wide: List[Dict[str, Any]] = []
    lines_calibrated: List[Dict[str, Any]] = []
    lines_identity: List[Dict[str, Any]] = []

    for col in prob_cols:
        # Expect pattern p_over_0_5 -> line 0.5
        suffix = col.replace("p_over_", "")
        try:
            line_val = float(suffix.replace("_", "."))
        except ValueError:
            print(f"  ⚠️  Could not parse line from column {col}; leaving as-is.")
            continue

        line_tag = suffix  # e.g. "0_5"
        lines_seen_in_wide.append(
            {"line": line_val, "line_tag": line_tag, "column": col}
        )

        if line_tag not in calibrators:
            print(
                f"  ⚠️  No fitted curve for line {line_val} (tag {line_tag}); "
                f"leaving {col} as identity."
            )
            lines_identity.append({"line": line_val, "line_tag": line_tag})
            continue

        cal_fn = calibrators[line_tag]
        before = wide_df[col].to_numpy(dtype=float)
        after = cal_fn(before)

        wide_df[col] = after
        if curves_fitted.get(line_tag, {}).get("status") == "ok":
            lines_calibrated.append({"line": line_val, "line_tag": line_tag})
            print(
                f"  ✅ Calibrated column {col} for line {line_val} "
                f"(non-null rows: {np.isfinite(before).sum()})"
            )
        else:
            lines_identity.append({"line": line_val, "line_tag": line_tag})
            print(
                f"  ⚠️  Using identity for column {col} (insufficient training rows)."
            )

    # Save calibrated wide CSV
    wide_out_path.parent.mkdir(parents=True, exist_ok=True)
    wide_df.to_csv(wide_out_path, index=False)
    print(f"\n✅ Wrote calibrated wide predictions → {wide_out_path}")
    print(
        f"   Columns preserved: {len(orig_cols)}; final columns: {len(wide_df.columns)}"
    )

    # Save curves metadata JSON
    summary_json = {
        "train_csv": str(train_path),
        "wide_in": str(wide_in_path),
        "wide_out": str(wide_out_path),
        "min_rows": min_rows,
        "blend_alpha": args.blend_alpha,
        "curves_fitted": curves_fitted,
        "lines_seen_in_wide": lines_seen_in_wide,
        "lines_calibrated": lines_calibrated,
        "lines_identity": lines_identity,
    }

    curves_json_path.parent.mkdir(parents=True, exist_ok=True)
    curves_json_path.write_text(json.dumps(summary_json, indent=2))
    print(f"✅ Wrote calibration curves metadata → {curves_json_path}")


if __name__ == "__main__":
    main()
