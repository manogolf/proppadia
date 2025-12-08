#!/usr/bin/env python3
"""
Test harness for SOG Poisson calibration.

Compares:
  - Uncalibrated wide predictions (original sog_predictions.csv)
  - Calibrated wide predictions (sog_predictions_calibrated.csv)

It runs:
  1) Smoke test (rows/columns/ranges)
  2) Basic column stats for p_over_* columns
  3) Reliability tables (before vs after) for selected lines

Usage examples:

  # Default paths + auto-detected lines
  python backend/nhl/scripts/test_sog_calibration.py

  # Explicit files, only test a subset of lines
  python backend/nhl/scripts/test_sog_calibration.py \
      --orig backend/nhl/data/processed/sog_predictions.csv \
      --cal  backend/nhl/data/processed/sog_predictions_calibrated.csv \
      --lines p_over_1_5 p_over_2_5 p_over_3_5

Note: This script is read-only; it does not modify any files.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Test SOG Poisson calibration.")
    p.add_argument(
        "--orig",
        dest="orig",
        type=str,
        default="backend/nhl/data/processed/sog_predictions.csv",
        help="Original (uncalibrated) wide predictions CSV.",
    )
    p.add_argument(
        "--cal",
        dest="cal",
        type=str,
        default="backend/nhl/data/processed/sog_predictions_calibrated.csv",
        help="Calibrated wide predictions CSV.",
    )
    p.add_argument(
        "--label",
        dest="label_col",
        type=str,
        default="sog",
        help="Outcome column for shots on goal (default: sog).",
    )
    p.add_argument(
        "--lines",
        nargs="*",
        default=None,
        help=(
            "Optional explicit list of p_over_* columns to test. "
            "If omitted, all p_over_* columns common to both files are used."
        ),
    )
    p.add_argument(
        "--bins",
        dest="num_bins",
        type=int,
        default=10,
        help="Number of probability buckets for reliability tables (default: 10).",
    )
    return p.parse_args()


def find_p_over_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith("p_over_")]


def smoke_test(orig: pd.DataFrame, cal: pd.DataFrame) -> None:
    print("=== 1) Smoke test ===")

    print(f"orig rows = {len(orig):,}, cal rows = {len(cal):,}")
    if len(orig) != len(cal):
        print("❌ Row count mismatch!")
    else:
        print("✅ Row counts match.")

    if list(orig.columns) != list(cal.columns):
        print("❌ Column sets or order differ between orig and cal.")
        # Print a quick diff
        orig_cols = set(orig.columns)
        cal_cols = set(cal.columns)
        only_orig = sorted(orig_cols - cal_cols)
        only_cal = sorted(cal_cols - orig_cols)
        if only_orig:
            print("  Columns only in orig:", only_orig)
        if only_cal:
            print("  Columns only in cal :", only_cal)
    else:
        print("✅ Column sets and order match.")

    # Check probability ranges for p_over_* in both
    for label, df in (("orig", orig), ("cal", cal)):
        pcols = find_p_over_cols(df)
        if not pcols:
            print(f"⚠️  No p_over_* columns found in {label}.")
            continue
        bad_cols = []
        for c in pcols:
            series = df[c].dropna()
            if series.empty:
                continue
            minv = series.min()
            maxv = series.max()
            if minv < -1e-6 or maxv > 1 + 1e-6:
                bad_cols.append((c, float(minv), float(maxv)))
        if bad_cols:
            print(f"❌ Some p_over_* columns in {label} have values outside [0,1]:")
            for c, mn, mx in bad_cols:
                print(f"  {c}: min={mn:.4f}, max={mx:.4f}")
        else:
            print(f"✅ All p_over_* columns in {label} are within [0,1] (or empty).")

    print()


def basic_stats(orig: pd.DataFrame, cal: pd.DataFrame, pcols: list[str]) -> None:
    print("=== 2) Basic stats for p_over_* columns ===")
    if not pcols:
        print("⚠️  No common p_over_* columns to summarize.")
        print()
        return

    rows = []
    for c in pcols:
        o = orig[c].dropna()
        k = cal[c].dropna()
        if o.empty and k.empty:
            continue
        rows.append(
            {
                "column": c,
                "orig_mean": float(o.mean() if not o.empty else np.nan),
                "orig_min": float(o.min() if not o.empty else np.nan),
                "orig_max": float(o.max() if not o.empty else np.nan),
                "cal_mean": float(k.mean() if not k.empty else np.nan),
                "cal_min": float(k.min() if not k.empty else np.nan),
                "cal_max": float(k.max() if not k.empty else np.nan),
                "n_orig": int(len(o)),
                "n_cal": int(len(k)),
            }
        )

    if not rows:
        print("⚠️  No data available to summarize.")
        print()
        return

    stat_df = pd.DataFrame(rows).sort_values("column")
    # Show a compact view
    with pd.option_context("display.max_rows", 200, "display.width", 120):
        print(stat_df.to_string(index=False))
    print()


def reliability_table(
    orig: pd.DataFrame,
    cal: pd.DataFrame,
    label_col: str,
    col: str,
    num_bins: int,
) -> None:
    print(f"=== 3) Reliability for {col} ===")

    if label_col not in orig.columns:
        print(f"⚠️  Label column '{label_col}' not found in orig; skipping.")
        return

    # use only rows with label present
    mask = orig[label_col].notna()
    if not mask.any():
        print(f"⚠️  No rows with non-null {label_col}; skipping.")
        return

    df_orig = orig.loc[mask, [col, label_col]].copy()
    df_cal = cal.loc[mask, [col]].copy()
    df_orig.rename(columns={col: "p_orig"}, inplace=True)
    df_cal.rename(columns={col: "p_cal"}, inplace=True)

    df = pd.concat([df_orig, df_cal], axis=1)

    # Label: is over for this line?
    # We infer line from column name: p_over_X_Y -> X.Y
    try:
        parts = col.split("_")
        # p_over, X, Y -> line = float("X.Y")
        line_val = float(f"{parts[2]}.{parts[3]}")
    except Exception:
        line_val = None

    if line_val is None:
        # Fallback: just treat label as "sog > line"
        df["y"] = df[label_col].astype(float) > 0.0
    else:
        df["y"] = df[label_col].astype(float) > line_val

    # Clean probs
    df = df[df["p_orig"].notna() & df["p_cal"].notna()]
    if df.empty:
        print("⚠️  No usable rows (both orig and cal probs non-null); skipping.")
        return

    # Clip probabilities
    df["p_orig"] = df["p_orig"].clip(0.0, 1.0)
    df["p_cal"] = df["p_cal"].clip(0.0, 1.0)

    # Bin by original probabilities for "raw" reliability
    df["bucket_raw"] = pd.qcut(
        df["p_orig"],
        q=min(num_bins, df["p_orig"].nunique(), len(df)),
        duplicates="drop",
    )

    # Bin by calibrated probabilities
    df["bucket_cal"] = pd.qcut(
        df["p_cal"],
        q=min(num_bins, df["p_cal"].nunique(), len(df)),
        duplicates="drop",
    )

    def summarize(df_bucket: pd.DataFrame, bucket_col: str, label: str) -> pd.DataFrame:
        g = (
            df_bucket.groupby(bucket_col)
            .agg(
                avg_pred=(label, "mean"),
                emp_rate=("y", "mean"),
                count=("y", "size"),
                min_pred=(label, "min"),
                max_pred=(label, "max"),
            )
            .reset_index(drop=True)
        )
        g.insert(0, "bucket", range(1, len(g) + 1))
        g["avg_pred"] = g["avg_pred"].astype(float)
        g["emp_rate"] = g["emp_rate"].astype(float)
        return g

    raw_stats = summarize(df, "bucket_raw", "p_orig")
    cal_stats = summarize(df, "bucket_cal", "p_cal")

    print("\nRaw (uncalibrated) reliability:")
    with pd.option_context("display.max_rows", 200, "display.width", 120):
        print(
            raw_stats[["bucket", "count", "min_pred", "max_pred", "avg_pred", "emp_rate"]]
            .round(4)
            .to_string(index=False)
        )

    print("\nCalibrated reliability:")
    with pd.option_context("display.max_rows", 200, "display.width", 120):
        print(
            cal_stats[["bucket", "count", "min_pred", "max_pred", "avg_pred", "emp_rate"]]
            .round(4)
            .to_string(index=False)
        )

    print()
    # Quick aggregate error metric: mean |pred - empirical| per table
    raw_err = float((raw_stats["avg_pred"] - raw_stats["emp_rate"]).abs().mean())
    cal_err = float((cal_stats["avg_pred"] - cal_stats["emp_rate"]).abs().mean())
    print(f"Mean |pred - emp| (raw): {raw_err:.4f}")
    print(f"Mean |pred - emp| (cal): {cal_err:.4f}")
    if cal_err < raw_err:
        print("✅ Calibration improved bucket-level reliability (lower error).")
    else:
        print("⚠️  Calibration did not reduce bucket-level error here (could be data sparsity).")
    print()


def main() -> None:
    args = parse_args()

    orig_path = Path(args.orig)
    cal_path = Path(args.cal)

    if not orig_path.is_file():
        print(f"❌ Original predictions file not found: {orig_path}")
        sys.exit(1)
    if not cal_path.is_file():
        print(f"❌ Calibrated predictions file not found: {cal_path}")
        sys.exit(1)

    orig = pd.read_csv(orig_path)
    cal = pd.read_csv(cal_path)

    smoke_test(orig, cal)

    # Determine which p_over_* columns to analyze
    orig_p = set(find_p_over_cols(orig))
    cal_p = set(find_p_over_cols(cal))
    common_p = sorted(orig_p & cal_p)

    if args.lines:
        # use only requested lines that are common
        requested = [c for c in args.lines if c in common_p]
        missing = [c for c in args.lines if c not in common_p]
        pcols = requested
        if missing:
            print("⚠️  Some requested lines not present in both files:", missing)
    else:
        pcols = common_p

    basic_stats(orig, cal, pcols)

    # Reliability tables per requested/common line (limit to keep output reasonable)
    max_lines = 5
    lines_to_show = pcols[:max_lines]
    if not lines_to_show:
        print("⚠️  No lines available for reliability checks.")
        return

    for col in lines_to_show:
        reliability_table(orig, cal, args.label_col, col, args.num_bins)


if __name__ == "__main__":
    main()
