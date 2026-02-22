#!/usr/bin/env python
"""
backend/nhl/scripts/calibrate_poisson_sog_wide.py

Calibrate wide SOG probabilities (p_over_0_5, p_over_1_5, ...)
using historical reliability curves from a training CSV:

  train_csv columns (denali):
    player_id, game_id, game_date, line, p_over_raw, y_over

  wide_in columns (current):
    player_id, game_id, team_id, opponent_id, is_home, game_date,
    p_over_lr_0_5, p_over_rf_0_5, p_over_0_5,
    p_over_lr_1_5, p_over_rf_1_5, p_over_1_5,
    p_over_lr_2_5, p_over_rf_2_5, p_over_2_5,
    p_over_lr_3_5, p_over_rf_3_5, p_over_3_5

Only the p_over_X_Y columns get calibrated; LR/RF stay untouched.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def line_to_tag(line_val: float) -> str:
    """
    Convert numeric line (e.g. 0.5) to tag used in wide file (e.g. '0_5').
    """
    try:
        v = float(line_val)
    except Exception:
        s = str(line_val)
        return s.replace(".", "_")
    s = f"{v:.1f}"  # 0.5 -> '0.5', 1.5 -> '1.5'
    return s.replace(".", "_")


def fit_curves_from_training(
    train_csv: Path,
    min_rows: int = 500,
    num_bins: int = 10,
):
    """
    Read training CSV and fit per-line calibration curves.

    Returns:
      curves: dict[tag] = {"bin_edges": [...], "bin_values": [...]}
      meta:   dict with debug info (curves_fitted, etc.)
    """
    meta = {
        "train_csv": str(train_csv),
        "min_rows": min_rows,
        "num_bins": num_bins,
        "curves_fitted": {},
    }

    if not train_csv.exists():
        return {}, meta

    df = pd.read_csv(train_csv)

    required_cols = {"line", "p_over_raw", "y_over"}
    if not required_cols.issubset(df.columns):
        missing = sorted(required_cols - set(df.columns))
        meta["error"] = f"Training CSV missing required columns: {missing}"
        return {}, meta

    # Clean & tag lines
    df = df.copy()
    df = df.dropna(subset=["line", "p_over_raw", "y_over"])

    # Ensure y_over is 0/1 numeric
    df["y_over"] = df["y_over"].astype(float)

    # Clip probabilities to [0,1]
    df["p_over_raw"] = df["p_over_raw"].astype(float).clip(0.0, 1.0)

    df["line_tag"] = df["line"].map(line_to_tag)

    curves = {}
    for line_tag, sub in df.groupby("line_tag"):
        n_rows = len(sub)
        if n_rows < min_rows:
            meta["curves_fitted"][line_tag] = {
                "status": "skipped_insufficient_rows",
                "rows": int(n_rows),
            }
            continue

        p = sub["p_over_raw"].values
        y = sub["y_over"].values

        # Bin edges from 0..1 (inclusive), num_bins equal-width bins
        bin_edges = np.linspace(0.0, 1.0, num_bins + 1)
        # Digitize: returns bin index in [1, num_bins] for p in (edge_{k-1}, edge_k]
        bin_idx = np.digitize(p, bin_edges, right=True)

        bin_values = np.full(num_bins, np.nan, dtype=float)
        for b in range(1, num_bins + 1):
            mask = bin_idx == b
            if not mask.any():
                continue
            bin_values[b - 1] = y[mask].mean()

        # If all NaN, we can't fit a curve
        if np.isnan(bin_values).all():
            meta["curves_fitted"][line_tag] = {
                "status": "failed_all_nan",
                "rows": int(n_rows),
            }
            continue

        # Fill gaps with nearest non-NaN, then fallback to global mean
        global_mean = float(y.mean())
        s = pd.Series(bin_values)
        s = s.fillna(method="ffill").fillna(method="bfill").fillna(global_mean)
        bin_values = s.values

        curves[line_tag] = {
            "bin_edges": bin_edges.tolist(),
            "bin_values": bin_values.tolist(),
        }
        meta["curves_fitted"][line_tag] = {
            "status": "ok",
            "rows": int(n_rows),
            "bin_edges_len": len(bin_edges),
            "bin_values_len": len(bin_values),
        }

    return curves, meta


def apply_curve_to_probs(
    probs: pd.Series,
    bin_edges: np.ndarray,
    bin_values: np.ndarray,
) -> pd.Series:
    """
    Apply a stepwise calibration curve defined by (bin_edges, bin_values)
    to a pandas Series of probabilities.
    """
    vals = probs.astype(float).values
    calibrated = vals.copy()

    # Clip to [0,1] and handle NaN separately
    mask_valid = ~np.isnan(vals)
    v = np.clip(vals[mask_valid], 0.0, 1.0)

    # Digitize using same convention as fit_curves_from_training
    idx = np.digitize(v, bin_edges, right=True)
    # Clip indices into [1, num_bins]
    idx = np.clip(idx, 1, len(bin_values))

    # Map bin index -> bin_values
    calibrated_vals = bin_values[idx - 1]

    calibrated[mask_valid] = calibrated_vals
    return pd.Series(calibrated, index=probs.index)


def calibrate_wide_file(
    curves: dict,
    wide_in: Path,
    wide_out: Path,
):
    """
    Apply per-line calibration curves to the wide predictions CSV.
    """
    meta = {
        "wide_in": str(wide_in),
        "wide_out": str(wide_out),
        "lines_seen_in_wide": [],
        "lines_calibrated": [],
        "lines_identity": [],
    }

    if not wide_in.exists():
        meta["error"] = f"Wide predictions file not found: {wide_in}"
        return meta

    df = pd.read_csv(wide_in)

    # Lines we expect in wide file; these can be extended later if needed
    lines = [0.5, 1.5, 2.5, 3.5]

    for line_val in lines:
        line_tag = line_to_tag(line_val)
        col_name = f"p_over_{line_tag}"

        if col_name not in df.columns:
            continue

        meta["lines_seen_in_wide"].append(
            {"line": line_val, "line_tag": line_tag, "column": col_name}
        )

        curve = curves.get(line_tag)
        if not curve:
            # Identity for this line
            meta["lines_identity"].append(
                {"line": line_val, "line_tag": line_tag}
            )
            continue

        bin_edges = np.array(curve["bin_edges"], dtype=float)
        bin_values = np.array(curve["bin_values"], dtype=float)

        df[col_name] = apply_curve_to_probs(df[col_name], bin_edges, bin_values)
        meta["lines_calibrated"].append(
            {"line": line_val, "line_tag": line_tag}
        )

    # Write calibrated wide file
    wide_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(wide_out, index=False)

    meta["wide_rows"] = int(len(df))
    return meta


def main():
    ap = argparse.ArgumentParser(
        description="Calibrate wide SOG probabilities using denali training CSV."
    )
    ap.add_argument(
        "--train-csv",
        default="backend/nhl/data/processed/sog_calibration_training_denali.csv",
        help="Training CSV with columns [player_id, game_id, game_date, line, p_over_raw, y_over]",
    )
    ap.add_argument(
        "--wide-in",
        default="backend/nhl/data/processed/sog_predictions.csv",
        help="Input wide predictions CSV (uncalibrated).",
    )
    ap.add_argument(
        "--wide-out",
        default="backend/nhl/data/processed/sog_predictions_wide_calibrated.csv",
        help="Output wide predictions CSV (calibrated).",
    )
    ap.add_argument(
        "--min-rows",
        type=int,
        default=500,
        help="Minimum training rows per line to fit a curve (otherwise identity).",
    )
    ap.add_argument(
        "--num-bins",
        type=int,
        default=10,
        help="Number of probability bins [0,1] for empirical calibration.",
    )

    args = ap.parse_args()

    train_csv = Path(args.train_csv)
    wide_in = Path(args.wide_in)
    wide_out = Path(args.wide_out)

    curves, fit_meta = fit_curves_from_training(
        train_csv=train_csv,
        min_rows=args.min_rows,
        num_bins=args.num_bins,
    )

    calib_meta = calibrate_wide_file(curves, wide_in=wide_in, wide_out=wide_out)

    summary = {
        "train_csv": str(train_csv),
        "wide_in": str(wide_in),
        "wide_out": str(wide_out),
        "min_rows": args.min_rows,
        "num_bins": args.num_bins,
        "curves_fitted": fit_meta.get("curves_fitted", {}),
        "lines_seen_in_wide": calib_meta.get("lines_seen_in_wide", []),
        "lines_calibrated": calib_meta.get("lines_calibrated", []),
        "lines_identity": calib_meta.get("lines_identity", []),
    }

    # Bubble up any high-level errors for quick debugging
    if "error" in fit_meta:
        summary["fit_error"] = fit_meta["error"]
    if "error" in calib_meta:
        summary["calib_error"] = calib_meta["error"]

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
