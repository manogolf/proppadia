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
PRED_PATH = PROC_DIR / "sog_predictions.csv"
PRED_CAL_PATH = PROC_DIR / "sog_predictions_calibrated.csv"

# Blend weight between Poisson baseline and model:
#   p_cal = w * p_poisson(line) + (1 - w) * p_model_raw
BLEND_W = 0.35  # tweak later if you want


def die(msg: str, code: int = 2) -> None:
    print(f"[calibrate_sog_poisson] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)


def poisson_tail(lmbda: float, k: int) -> float:
    """
    P(X >= k) for X ~ Poisson(lambda).
    """
    if lmbda <= 0 or not math.isfinite(lmbda):
        return float("nan")
    if k <= 0:
        return 1.0
    # 1 - CDF(k-1)
    cdf = 0.0
    term = math.exp(-lmbda)
    cdf += term  # P(X=0)
    for n in range(1, k):
        term *= lmbda / n
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))


def fetch_poisson_baseline(conn) -> dict[float, float]:
    """
    From nhl.training_features_sog_denali, estimate:
      - global lambda = E[shots_on_goal]
      - empirical hit rates at thresholds 1,2,3,4
    Then compute Poisson P(X >= threshold | lambda) per line
    for lines 0.5,1.5,2.5,3.5.
    """
    sql = """
        SELECT
          COUNT(*)::float AS games,
          AVG(shots_on_goal)::float AS lambda_hat,
          SUM((shots_on_goal >= 1)::int)::float AS over_0_5,
          SUM((shots_on_goal >= 2)::int)::float AS over_1_5,
          SUM((shots_on_goal >= 3)::int)::float AS over_2_5,
          SUM((shots_on_goal >= 4)::int)::float AS over_3_5
        FROM nhl.training_features_sog_denali
        WHERE shots_on_goal IS NOT NULL;
    """
    with conn.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()

    if not row or row["games"] <= 0 or row["lambda_hat"] is None:
        die("training_features_sog_denali has no usable rows for calibration.")

    games = row["games"]
    lam = row["lambda_hat"]

    base_emp = {
        0.5: row["over_0_5"] / games,
        1.5: row["over_1_5"] / games,
        2.5: row["over_2_5"] / games,
        3.5: row["over_3_5"] / games,
    }

    print(
        f"[calibrate_sog_poisson] lambda_hat={lam:.4f}  games={int(games)} "
        + " ".join(
            f"emp_over_{L:.1f}={base_emp[L]:.4f}" for L in sorted(base_emp.keys())
        )
    )

    # Map line → integer threshold k
    def line_to_k(L: float) -> int:
        # 0.5→1, 1.5→2, 2.5→3, 3.5→4
        return int(round(L * 2.0))

    base_poiss: dict[float, float] = {}
    for L in sorted(base_emp.keys()):
        k = line_to_k(L)
        p_tail = poisson_tail(lam, k)
        base_poiss[L] = p_tail

    print(
        "[calibrate_sog_poisson] poisson P(X>=k) by line: "
        + ", ".join(f"{L:.1f}→{base_poiss[L]:.4f}" for L in sorted(base_poiss.keys()))
    )

    return base_poiss


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

    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["prob_over"] = pd.to_numeric(df["prob_over"], errors="coerce")
    df = df[df["line"].notna() & df["prob_over"].notna()].copy()

    if df.empty:
        die("after cleaning, predictions CSV has no valid rows.")

    return df


def main() -> None:
    # --- 1) DB connection for Poisson baseline ---
    db_url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not db_url:
        die("SUPABASE_DB_URL not set; cannot fetch SOG Poisson baseline.")

    with psycopg2.connect(db_url) as conn:
        base_poiss = fetch_poisson_baseline(conn)

    # --- 2) Load raw SOG predictions ---
    df = load_predictions(PRED_PATH)

    # Preserve raw model probability for sanity checks
    df["prob_over_raw"] = df["prob_over"].astype(float)

    # Quantize line to nearest half-point for lookup
    def quantize_line(x: float) -> float | None:
        if not (isinstance(x, (int, float)) and math.isfinite(x)):
            return None
        return round(x * 2.0) / 2.0  # e.g. 0.5, 1.5, 2.5, ...

    def blend_prob(p_raw: float, line_val: float) -> float:
        if not (isinstance(p_raw, (int, float)) and math.isfinite(p_raw)):
            return float("nan")
        Lq = quantize_line(line_val)
        base = base_poiss.get(Lq)
        if base is None or not math.isfinite(base):
            # Weird line (e.g. 4.5) → just use raw probability
            return float(p_raw)
        return float(BLEND_W * base + (1.0 - BLEND_W) * p_raw)

    df["prob_over_calibrated"] = [
        blend_prob(p, ln) for p, ln in zip(df["prob_over_raw"], df["line"])
    ]

    # Overwrite prob_over with calibrated value for downstream
    df["prob_over"] = df["prob_over_calibrated"]

    PRED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PRED_PATH, index=False)       # what everything else will now read
    df.to_csv(PRED_CAL_PATH, index=False)   # archival / debugging

    distinct_lines = sorted(df["line"].dropna().unique().tolist())
    print(
        f"[calibrate_sog_poisson] wrote calibrated SOG predictions to {PRED_PATH} "
        f"(rows={len(df)}, lines={distinct_lines}, blend_w={BLEND_W})"
    )


if __name__ == "__main__":
    main()
