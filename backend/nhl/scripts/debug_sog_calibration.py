#!/usr/bin/env python
"""
backend/nhl/scripts/debug_sog_calibration.py

Goal
----
Help diagnose where SOG probabilities are getting flattened.

1) Inspect the distribution of calibrated p_over_* columns in:
     backend/nhl/data/processed/sog_predictions_wide_calibrated.csv

2) If available, compare to what the Dashboard uses:
     backend/nhl/site/data/sog_with_market.csv

For each SOG line, we print:
  - count
  - min / max
  - mean / std
  - 10th / 50th / 90th percentiles
  - number of unique values

If both files exist and share p_over_* columns, we also:
  - join on (player_id, game_id)
  - compare Denali vs Dashboard distributions
  - print summary of the difference (Denali - Dashboard) per line.

Run
---
  python backend/nhl/scripts/debug_sog_calibration.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]

CAL_CSV = BASE_DIR / "nhl" / "data" / "processed" / "sog_predictions_wide_calibrated.csv"
DASH_CSV = BASE_DIR / "nhl" / "site" / "data" / "sog_with_market.csv"


def find_prob_cols(df: pd.DataFrame) -> List[str]:
    """Return columns that look like calibrated SOG probs: p_over_*."""
    return [c for c in df.columns if c.startswith("p_over_")]


def summarize_probs(df: pd.DataFrame, label: str) -> None:
    """
    For each p_over_* column in df, print a compact distribution summary.
    """
    prob_cols = find_prob_cols(df)
    if not prob_cols:
        print(f"[{label}] No p_over_* cols found.")
        return

    print(f"\n[{label}] Found probability columns: {prob_cols}\n")

    for col in sorted(prob_cols):
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            print(f"  {col}: no numeric values")
            continue

        vals = s.to_numpy()
        unique_vals = np.unique(vals)
        q10, q50, q90 = np.percentile(vals, [10, 50, 90])

        print(f"  --- {col} ---")
        print(f"    count       : {len(vals):,}")
        print(f"    min / max   : {vals.min():.4f}  /  {vals.max():.4f}")
        print(f"    mean / std  : {vals.mean():.4f}  /  {vals.std(ddof=1):.4f}")
        print(f"    p10 / p50 / p90 : {q10:.4f}  /  {q50:.4f}  /  {q90:.4f}")
        print(f"    unique vals : {len(unique_vals):,}")
        # If it's suspiciously small, show a few of them
        if len(unique_vals) <= 10:
            print(f"    unique sample: {unique_vals}")
        print()


def summarize_comparison(df_cal: pd.DataFrame, df_dash: pd.DataFrame) -> None:
    """
    If both calibrated and dashboard frames have p_over_* columns and
    share player_id + game_id, compare them.
    """
    cal_cols = set(find_prob_cols(df_cal))
    dash_cols = set(find_prob_cols(df_dash))
    common_cols = sorted(cal_cols & dash_cols)

    if not common_cols:
        print(
            "\n[COMPARE] No overlapping p_over_* columns between calibrated and dashboard CSVs."
        )
        return

    if not {"player_id", "game_id"}.issubset(df_cal.columns) or not {
        "player_id",
        "game_id",
    }.issubset(df_dash.columns):
        print(
            "\n[COMPARE] Missing player_id / game_id in one of the files; "
            "cannot do row-level comparison."
        )
        return

    print(
        "\n[COMPARE] Joining calibrated vs dashboard on (player_id, game_id) "
        f"for columns: {common_cols}\n"
    )

    # Keep only the essentials to avoid blowing up memory
    cal_small = df_cal[["player_id", "game_id"] + common_cols].copy()
    dash_small = df_dash[["player_id", "game_id"] + common_cols].copy()

    merged = cal_small.merge(
        dash_small,
        on=["player_id", "game_id"],
        suffixes=("_cal", "_dash"),
        how="inner",
    )

    print(f"  Joined rows: {len(merged):,}")

    if merged.empty:
        return

    for base in common_cols:
        cal_col = f"{base}_cal"
        dash_col = f"{base}_dash"

        cal_vals = pd.to_numeric(merged[cal_col], errors="coerce")
        dash_vals = pd.to_numeric(merged[dash_col], errors="coerce")

        mask = cal_vals.notna() & dash_vals.notna()
        if mask.sum() == 0:
            print(f"  {base}: no overlapping numeric values to compare.")
            continue

        cal_arr = cal_vals[mask].to_numpy()
        dash_arr = dash_vals[mask].to_numpy()
        diff = cal_arr - dash_arr

        print(f"  --- {base} (calibrated vs dashboard) ---")
        print(f"    overlap count       : {mask.sum():,}")
        print(f"    calibrated mean/std : {cal_arr.mean():.4f} / {cal_arr.std(ddof=1):.4f}")
        print(f"    dashboard  mean/std : {dash_arr.mean():.4f} / {dash_arr.std(ddof=1):.4f}")
        print(
            f"    diff (cal - dash)   : "
            f"min={diff.min():.4f}, max={diff.max():.4f}, "
            f"mean={diff.mean():.4f}, std={diff.std(ddof=1):.4f}"
        )
        print()


def main() -> None:
    # 1) Calibrated Denali predictions
    if not CAL_CSV.exists():
        print(f"ERROR: Calibrated SOG CSV not found: {CAL_CSV}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading calibrated SOG predictions from:\n  {CAL_CSV}")
    df_cal = pd.read_csv(CAL_CSV)

    summarize_probs(df_cal, label="CALIBRATED (Denali)")

    # 2) Dashboard CSV (if present)
    if DASH_CSV.exists():
        print(f"\nReading Dashboard SOG CSV from:\n  {DASH_CSV}")
        df_dash = pd.read_csv(DASH_CSV)
        summarize_probs(df_dash, label="DASHBOARD INPUT (sog_with_market.csv)")
        summarize_comparison(df_cal, df_dash)
    else:
        print(
            f"\nNOTE: Dashboard CSV not found at {DASH_CSV}.\n"
            "      Skipping dashboard comparison; only Denali calibrated stats shown."
        )


if __name__ == "__main__":
    main()
