# backend/nhl/scripts/debug_sog_raw_vs_calibrated.py
"""
Compare raw Denali SOG probabilities vs calibrated probabilities
for the same slate, to see where compression is happening.

Usage:
  python backend/nhl/scripts/debug_sog_raw_vs_calibrated.py
"""

from pathlib import Path
import numpy as np
import pandas as pd


BASE = Path(__file__).resolve().parents[2]
RAW_PATH = BASE / "nhl" / "data" / "processed" / "sog_predictions.csv"
CAL_PATH = BASE / "nhl" / "data" / "processed" / "sog_predictions_wide_calibrated.csv"


def _summarize(label: str, s: pd.Series) -> None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        print(f"  --- {label} ---")
        print("    (no data)")
        return

    arr = s.values.astype(float)
    n = len(arr)
    mn, mx = float(arr.min()), float(arr.max())
    mean, std = float(arr.mean()), float(arr.std())
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    uniq = np.unique(np.round(arr, 6))

    print(f"  --- {label} ---")
    print(f"    count       : {n}")
    print(f"    min / max   : {mn:.4f}  /  {mx:.4f}")
    print(f"    mean / std  : {mean:.4f}  /  {std:.4f}")
    print(f"    p10 / p50 / p90 : {p10:.4f}  /  {p50:.4f}  /  {p90:.4f}")
    print(f"    unique vals : {len(uniq)}")
    print(f"    unique sample: {uniq[:8]}")
    print()


def main() -> None:
    print("RAW path:", RAW_PATH)
    print("CAL path:", CAL_PATH)
    if not RAW_PATH.exists():
        raise SystemExit(f"Missing raw predictions: {RAW_PATH}")
    if not CAL_PATH.exists():
        raise SystemExit(f"Missing calibrated predictions: {CAL_PATH}")

    raw = pd.read_csv(RAW_PATH)
    cal = pd.read_csv(CAL_PATH)

    # Make sure we're looking at the same slate
    raw_keys = raw[["player_id", "game_id"]].drop_duplicates()
    cal_keys = cal[["player_id", "game_id"]].drop_duplicates()
    merged_keys = raw_keys.merge(cal_keys, on=["player_id", "game_id"])
    common_ids = set(zip(merged_keys["player_id"], merged_keys["game_id"]))

    def _filter_common(df: pd.DataFrame) -> pd.DataFrame:
        return df[
            list(
                map(
                    lambda row: (row["player_id"], row["game_id"]) in common_ids,
                    df[["player_id", "game_id"]].to_dict("records"),
                )
            )
        ]

    raw = _filter_common(raw)
    cal = _filter_common(cal)

    print(f"\nCommon rows (player_id, game_id): {len(common_ids)}\n")

    # For each line, compare raw vs calibrated blend columns
    for line_tag in ["0_5", "1_5", "2_5", "3_5"]:
        col = f"p_over_{line_tag}"
        if col not in raw.columns or col not in cal.columns:
            continue

        print("=" * 70)
        print(f"Line {line_tag.replace('_', '.')} → column {col}")
        print("=" * 70)

        _summarize(f"RAW   {col}", raw[col])
        _summarize(f"CALIB {col}", cal[col])

    # Also show LR/RF spreads for context (should match in both files)
    print("\nLR / RF components (from calibrated file):\n")
    for col in ["p_over_lr_0_5", "p_over_lr_1_5", "p_over_lr_2_5", "p_over_lr_3_5",
                "p_over_rf_0_5", "p_over_rf_1_5", "p_over_rf_2_5", "p_over_rf_3_5"]:
        if col in cal.columns:
            _summarize(col, cal[col])


if __name__ == "__main__":
    main()
