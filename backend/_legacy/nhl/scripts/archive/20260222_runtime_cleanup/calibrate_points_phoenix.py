#!/usr/bin/env python3
from __future__ import annotations

import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras


PROC_DIR = Path("backend/nhl/data/processed")
PRED_PATH = PROC_DIR / "points_predictions.csv"
PRED_CAL_PATH = PROC_DIR / "points_predictions_calibrated.csv"

# How much to lean on the *empirical* base rate by line.
#   p_cal = w * base_rate(line) + (1 - w) * p_model_raw
BLEND_W = 0.35  # you can tune this later if needed


def die(msg: str, code: int = 2) -> None:
    print(f"[calibrate_points_phoenix] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)


def fetch_base_rates(conn) -> dict[float, float]:
    """
    Pull empirical base rates for y_points at thresholds:
      0.5 → P(y >= 1)
      1.5 → P(y >= 2)
      2.5 → P(y >= 3)

    Returns mapping {0.5: base0, 1.5: base1, 2.5: base2}.
    """
    sql = """
        SELECT
          COUNT(*)::float AS games,
          SUM((y_points >= 1)::int)::float AS over_0_5,
          SUM((y_points >= 2)::int)::float AS over_1_5,
          SUM((y_points >= 3)::int)::float AS over_2_5
        FROM nhl.points_training_frame_phoenix;
    """
    with conn.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row or row["games"] <= 0:
            die("points_training_frame_phoenix has zero rows; cannot calibrate.")

        games = row["games"]
        base_0_5 = row["over_0_5"] / games
        base_1_5 = row["over_1_5"] / games
        base_2_5 = row["over_2_5"] / games

        print(
            f"[calibrate_points_phoenix] line=0.5 threshold=1 games={int(games)} "
            f"hits={int(row['over_0_5'])} base_rate={base_0_5:.4f}"
        )
        print(
            f"[calibrate_points_phoenix] line=1.5 threshold=2 games={int(games)} "
            f"hits={int(row['over_1_5'])} base_rate={base_1_5:.4f}"
        )
        print(
            f"[calibrate_points_phoenix] line=2.5 threshold=3 games={int(games)} "
            f"hits={int(row['over_2_5'])} base_rate={base_2_5:.4f}"
        )

        return {
            0.5: base_0_5,
            1.5: base_1_5,
            2.5: base_2_5,
        }


def load_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"predictions file not found: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        die(f"failed to read predictions CSV {path}: {e}")

    need = {"player_id", "game_id", "line", "prob_over"}
    missing = need - set(df.columns)
    if missing:
        die(f"predictions CSV missing required columns: {sorted(missing)}")

    if df.empty:
        die("predictions CSV has no rows; nothing to calibrate.")

    # Numeric clean-up
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["prob_over"] = pd.to_numeric(df["prob_over"], errors="coerce")

    df = df[df["line"].notna() & df["prob_over"].notna()].copy()
    if df.empty:
        die("after cleaning, predictions CSV has no valid rows.")

    return df


def main() -> None:
    # --- 1) DB connection for base rates ---
    db_url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not db_url:
        die("SUPABASE_DB_URL not set; cannot fetch base rates for calibration.")

    with psycopg2.connect(db_url) as conn:
        base_rates = fetch_base_rates(conn)

    print(
        "[calibrate_points_phoenix] base_rates="
        + ", ".join(f"{k}: {v:.4f}" for k, v in sorted(base_rates.items()))
    )

    # --- 2) Load raw Phoenix predictions ---
    df = load_predictions(PRED_PATH)

    # Keep a copy of the raw model probability for debugging/analysis.
    df["prob_over_raw"] = df["prob_over"].astype(float)

    # --- 3) Blend with base rates by line ---
    def quantize_line(x: float) -> float | None:
        if not isinstance(x, (int, float)) or not math.isfinite(x):
            return None
        # snap to nearest half-point like 0.5, 1.5, 2.5
        return round(x * 2.0) / 2.0

    def blend_prob(p_raw: float, line_val: float) -> float:
        if not (isinstance(p_raw, (int, float)) and math.isfinite(p_raw)):
            return float("nan")
        lnq = quantize_line(line_val)
        base = base_rates.get(lnq)
        if base is None or not math.isfinite(base):
            # if we somehow see a weird line (e.g., 3.5), fall back to raw
            return float(p_raw)
        return float(BLEND_W * base + (1.0 - BLEND_W) * p_raw)

    df["prob_over_calibrated"] = [
        blend_prob(p, ln) for p, ln in zip(df["prob_over_raw"], df["line"])
    ]

    # Overwrite prob_over with calibrated value for downstream consumers.
    df["prob_over"] = df["prob_over_calibrated"]

    # --- 4) Write BOTH files: calibrated-only + archival calibrated CSV ---
    PRED_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(PRED_PATH, index=False)        # <- this is what the site will now use
    df.to_csv(PRED_CAL_PATH, index=False)    # <- optional: archival / debugging

    distinct_lines = sorted(df["line"].dropna().unique().tolist())
    print(
        f"[calibrate_points_phoenix] wrote calibrated predictions to {PRED_PATH} "
        f"(rows={len(df)}, lines={distinct_lines}, blend_w={BLEND_W})"
    )


if __name__ == "__main__":
    main()