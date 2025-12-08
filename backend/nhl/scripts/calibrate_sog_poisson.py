#!/usr/bin/env python3
"""
calibrate_sog_poisson.py

Temporary Denali-compatible SOG "calibration" step.

Goal:
  - Future-protect the scorer by NOT requiring a long format
    (`line`, `prob_over`) and instead working off the wide
    p_over_* columns that score_sog_denali.py writes.
  - For now, perform an identity-like calibration (clip to [0, 1])
    so the daily pipeline runs end-to-end.

Later:
  - Replace the identity step with true Poisson-based or
    empirical calibration per line, but keep the same IO contract.

Usage:
  # Default (used by cli.py)
  python backend/nhl/scripts/calibrate_sog_poisson.py

  # Optional overrides
  python backend/nhl/scripts/calibrate_sog_poisson.py \
      --in backend/nhl/data/processed/sog_predictions.csv \
      --out backend/nhl/data/processed/sog_predictions.csv
"""

from __future__ import annotations

import argparse
import sys
import re
from pathlib import Path
import numpy as np
import pandas as pd


def _nhl_base() -> Path:
    # backend/nhl/scripts -> backend/nhl
    return Path(__file__).resolve().parents[1]

NHL_BASE = _nhl_base()
PROC_DIR = NHL_BASE / "data" / "processed"


def find_p_over_columns(df: pd.DataFrame) -> list[str]:
    """
    Return all blended p_over_* columns we want to calibrate.

    We treat columns named like:
      - p_over_0_5
      - p_over_1_5
      - p_over_2_5
      - p_over_3_5
    as the "final" blended probabilities per line.

    LR/RF-specific columns (p_over_lr_*, p_over_rf_*) are left untouched.
    """
    cols: list[str] = []
    pattern = re.compile(r"^p_over_(\d+_5)$")  # captures 0_5, 1_5, 2_5, 3_5, etc.
    for c in df.columns:
        if pattern.match(c):
            cols.append(c)
    return cols


def identity_calibration(df: pd.DataFrame, target_cols: list[str]) -> pd.DataFrame:
    """
    Placeholder "calibration":
      - Clip all p_over_* columns to [0, 1].
      - Leaves all other columns unchanged.

    This is intentionally simple so the pipeline can run
    while we design a proper Poisson / empirical calibration.
    """
    if not target_cols:
        print(
            "[calibrate_sog_poisson] WARNING: No p_over_*_5 columns found; "
            "leaving file unchanged.",
            file=sys.stderr,
        )
        return df

    df = df.copy()
    for col in target_cols:
        # Force numeric, replace NaNs and infinities, clip to [0, 1]
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        df[col] = df[col].clip(0.0, 1.0)

    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Calibrate NHL SOG probabilities by line (Denali, wide format)."
    )
    ap.add_argument(
        "--in",
        dest="in_path",
        default=str(PROC_DIR / "sog_predictions.csv"),
        help="Input CSV from score_sog_denali.py (wide p_over_* columns).",
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default=None,
        help="Output CSV path (default: overwrite input).",
    )
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path) if args.out_path else in_path

    if not in_path.exists():
        print(
            f"[calibrate_sog_poisson] FATAL: input CSV not found: {in_path}",
            file=sys.stderr,
        )
        sys.exit(2)

    print(f"[calibrate_sog_poisson] Reading predictions from {in_path}", file=sys.stderr)
    df = pd.read_csv(in_path)

    # Identify which columns we consider "final p_over per line"
    p_cols = find_p_over_columns(df)
    print(
        f"[calibrate_sog_poisson] Found {len(p_cols)} p_over_* columns: {p_cols}",
        file=sys.stderr,
    )

    # Identity-style calibration (for now)
    df_cal = identity_calibration(df, p_cols)

    # Write result back to disk (default: overwrite the input file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_cal.to_csv(out_path, index=False)
    print(
        f"[calibrate_sog_poisson] Wrote calibrated predictions → {out_path}",
        file=sys.stderr,
    )

    # Simple summary for sanity
    if p_cols:
        summary = {
            col: {
                "min": float(df_cal[col].min()),
                "max": float(df_cal[col].max()),
                "mean": float(df_cal[col].mean()),
            }
            for col in p_cols
        }
        print(
            f"[calibrate_sog_poisson] Summary (post-calibration): {summary}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
