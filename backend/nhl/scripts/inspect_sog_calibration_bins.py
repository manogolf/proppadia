#!/usr/bin/env python3
"""
backend/nhl/scripts/inspect_sog_calibration_bins.py

Inspect calibration bins for SOG Poisson / hybrid predictions.

Reads:
  backend/nhl/data/processed/sog_calibration_training.csv

Expected columns:
  player_id, game_id, game_date, line, p_over_raw, y_over

For each line (0.5, 1.5, 2.5, 3.5), it:
  - buckets p_over_raw into num_bins bins in [0,1]
  - prints, per bin:
      line, bin range, n, avg(p_over_raw), actual_over_rate
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    ap = argparse.ArgumentParser(description="Inspect SOG calibration bins.")
    ap.add_argument(
        "--train-csv",
        default="backend/nhl/data/processed/sog_calibration_training.csv",
        help="Calibration training CSV (p_over_raw + y_over).",
    )
    ap.add_argument(
        "--num-bins",
        type=int,
        default=10,
        help="Number of probability bins (default: 10 → 10% buckets).",
    )
    ap.add_argument(
        "--min-n",
        type=int,
        default=20,
        help="Minimum rows per bin to show (default: 20).",
    )
    args = ap.parse_args()

    path = Path(args.train_csv)
    if not path.exists():
        raise SystemExit(f"Training CSV not found: {path}")

    df = pd.read_csv(path)

    required = {"line", "p_over_raw", "y_over"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns in CSV: {sorted(missing)}")

    # Clean / filter
    df = df.copy()
    df = df[
        df["p_over_raw"].notna()
        & df["y_over"].notna()
        & (df["p_over_raw"] >= 0.0)
        & (df["p_over_raw"] <= 1.0)
    ]

    num_bins = args.num_bins
    bin_edges = np.linspace(0.0, 1.0, num_bins + 1)

    lines = sorted(df["line"].dropna().unique())

    for line_val in lines:
        sub = df[df["line"] == line_val].copy()
        if sub.empty:
            continue

        # Bin index 1..num_bins
        idx = np.digitize(sub["p_over_raw"].values, bin_edges, right=False)
        idx = np.clip(idx, 1, num_bins)
        sub["bin_idx"] = idx

        print(f"\n=== Line {line_val} (rows={len(sub)}) ===")
        print(f"{'bin':>3}  {'range':>11}  {'n':>6}  {'avg_p':>8}  {'hit_rate':>9}")
        print("-" * 45)

        for b in range(1, num_bins + 1):
            mask = sub["bin_idx"] == b
            n = int(mask.sum())
            if n < args.min_n:
                continue  # skip tiny bins; change if you want everything

            lo = bin_edges[b - 1]
            hi = bin_edges[b]
            avg_p = float(sub.loc[mask, "p_over_raw"].mean())
            hit_rate = float(sub.loc[mask, "y_over"].mean())

            print(
                f"{b:>3}  "
                f"{lo:4.2f}-{hi:4.2f}  "
                f"{n:6d}  "
                f"{avg_p:8.3f}  "
                f"{hit_rate:9.3f}"
            )


if __name__ == "__main__":
    main()
