#!/usr/bin/env python3
"""Score NHL SOG wide probabilities from a simple Poisson baseline."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_OUT = "backend/nhl/data/processed/sog_predictions_wide_calibrated.csv"


def _to_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _coalesce(*series: pd.Series) -> pd.Series:
    if not series:
        raise ValueError("Need at least one series to coalesce")
    out = series[0].copy()
    for s in series[1:]:
        out = out.where(out.notna(), s)
    return out


def _expected_bucket(v: float | None) -> str:
    if v is None or not math.isfinite(v):
        return "missing"
    if v < 1.5:
        return "<1.5"
    if v < 2.5:
        return "1.5-2.5"
    if v < 3.5:
        return "2.5-3.5"
    return "3.5+"


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _bucket_series(expected_sog: pd.Series) -> pd.Series:
    return expected_sog.apply(lambda v: _expected_bucket(float(v)) if pd.notna(v) else "missing")


def main() -> None:
    ap = argparse.ArgumentParser(description="Score NHL SOG probabilities using a Poisson baseline.")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", default=DEFAULT_OUT)
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    df = pd.read_csv(in_path)
    if df.empty:
        raise SystemExit(f"[poisson scorer] empty input CSV: {in_path}")

    rate = _coalesce(
        _to_numeric(df, "d10_sog_per60"),
        _to_numeric(df, "d20_sog_per60"),
        _to_numeric(df, "d5_sog_per60"),
    )
    toi = _coalesce(
        _to_numeric(df, "d10_toi_min_avg"),
        _to_numeric(df, "d20_toi_min_avg"),
        _to_numeric(df, "d5_toi_min_avg"),
        _to_numeric(df, "szn_toi_per_game_5on5") + _to_numeric(df, "szn_toi_per_game_pp"),
        (_to_numeric(df, "season_5on5_icetime_per_game") / 60.0)
        + (_to_numeric(df, "season_5on4_icetime_per_game") / 60.0),
    )

    expected_sog = (rate * toi) / 60.0
    expected_sog = expected_sog.where(expected_sog.notna(), 0.0).clip(lower=0.0)

    out = pd.DataFrame()
    for c in ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season"]:
        if c in df.columns:
            out[c] = df[c]

    out["expected_sog"] = expected_sog.astype(float)
    out["expected_sog_bucket"] = _bucket_series(out["expected_sog"])
    out["poisson_source"] = np.where(_to_numeric(df, "d10_sog_per60").notna() & _to_numeric(df, "d10_toi_min_avg").notna(), "d10", "fallback")

    p15 = out["expected_sog"].apply(lambda v: _poisson_tail(float(v), 2))
    p25 = out["expected_sog"].apply(lambda v: _poisson_tail(float(v), 3))
    p35 = out["expected_sog"].apply(lambda v: _poisson_tail(float(v), 4))

    out["p_over_1_5"] = p15.astype(float)
    out["p_over_2_5"] = p25.astype(float)
    out["p_over_3_5"] = p35.astype(float)

    out["p_0_1"] = (1.0 - out["p_over_1_5"]).clip(0, 1)
    out["p_2"] = (out["p_over_1_5"] - out["p_over_2_5"]).clip(0, 1)
    out["p_3"] = (out["p_over_2_5"] - out["p_over_3_5"]).clip(0, 1)
    out["p_4p"] = out["p_over_3_5"].clip(0, 1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[poisson scorer] rows={len(out)} wrote={out_path}")
    print(
        "[poisson scorer] source_counts="
        + str(out["poisson_source"].value_counts(dropna=False).to_dict())
    )


if __name__ == "__main__":
    main()
