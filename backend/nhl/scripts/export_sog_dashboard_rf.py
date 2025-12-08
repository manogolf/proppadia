# backend/nhl/scripts/export_sog_dashboard_rf.py
"""
Export SOG RF probabilities into sog_with_market.csv for the dashboard.

Purpose
-------
The dashboard HTML (nhl/site/index.html) expects a CSV like:

  nhl/site/data/sog_with_market.csv

with columns including p_over_0_5, p_over_1_5, p_over_2_5, p_over_3_5.

Historically, these were Denali-blended, calibrated probabilities. For now we
want the dashboard to show the raw Random Forest probabilities instead, to get
a more realistic spread across players.

This script:

  - Reads backend/nhl/data/processed/sog_predictions.csv
      (wide RF/LR predictions: p_over_lr_*, p_over_rf_*, p_over_*)

  - Builds a CSV with:
      player_id, game_id, team_id, opponent_id, is_home, game_date,
      p_over_0_5, p_over_1_5, p_over_2_5, p_over_3_5

    where each p_over_* comes from the corresponding p_over_rf_* column.

  - Writes the result to:
      backend/nhl/site/data/sog_with_market.csv

Usage
-----
  python backend/nhl/scripts/export_sog_dashboard_rf.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


# Base paths
THIS_FILE = Path(__file__).resolve()
BACKEND_DIR = THIS_FILE.parents[2]  # .../backend
DATA_DIR = BACKEND_DIR / "nhl" / "data" / "processed"
SITE_DATA_DIR = BACKEND_DIR / "nhl" / "site" / "data"

PRED_CSV = DATA_DIR / "sog_predictions.csv"
OUT_CSV = SITE_DATA_DIR / "sog_with_market.csv"


def main() -> None:
    if not PRED_CSV.exists():
        print(f"ERROR: Predictions CSV not found: {PRED_CSV}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading SOG predictions from: {PRED_CSV}")
    df = pd.read_csv(PRED_CSV)

    # Required base columns for context
    base_required = {"player_id", "game_id"}
    missing = base_required - set(df.columns)
    if missing:
        print(f"ERROR: Missing required columns in {PRED_CSV}: {missing}", file=sys.stderr)
        sys.exit(1)

    # Optional context columns (use if present)
    context_cols = [
        "player_id",
        "game_id",
        "team_id",
        "opponent_id",
        "is_home",
        "game_date",
    ]
    context_cols = [c for c in context_cols if c in df.columns]

    # We want RF probabilities only
    rf_cols_map = {
        "p_over_rf_0_5": "p_over_0_5",
        "p_over_rf_1_5": "p_over_1_5",
        "p_over_rf_2_5": "p_over_2_5",
        "p_over_rf_3_5": "p_over_3_5",
    }

    # Check which RF columns we actually have
    available_rf = {src: dst for src, dst in rf_cols_map.items() if src in df.columns}
    if not available_rf:
        print(
            "ERROR: No p_over_rf_* columns found in sog_predictions.csv. "
            "Expected columns like p_over_rf_1_5.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Using RF columns for dashboard:")
    for src, dst in available_rf.items():
        print(f"  {src} → {dst}")

    # Build output DataFrame
    out = df[context_cols].copy()

    # Add p_over_* columns mapped from RF
    for src, dst in available_rf.items():
        out[dst] = df[src].astype(float)

    # Note: we are NOT including market columns here (price_over, p_over_mkt, etc.).
    # The dashboard can still compute EV using a typed price or odds_latest.json.
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Wrote {len(out)} rows to {OUT_CSV}")
    print("You can now open the dashboard HTML and it will read RF-based p_over_* values.")


if __name__ == "__main__":
    main()
