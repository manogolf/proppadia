#!/usr/bin/env python3
"""Evaluate exact on-ice defender matchup surprise on top of the NHL SOG Poisson base."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from typing import Deque, Dict, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.shared.db.pg import pg_fetchall

from backend.nhl.scripts.experiment_sog_exact_onice_defender_deployment_base import (
    DEFENDER_ONICE_SHOTS_SQL,
    DEFENDER_TOI_SQL,
    TEST_SHIFTS_SQL,
    THRESHOLDS,
    _build_defender_prior_rate_map,
    _build_test_overlap_map,
    _combined_metric,
    _metric_rows,
    _norm_pos,
    _poisson_tail,
    _round,
    _split_df,
    _fetch_df,
)


def _build_matchup_signature(full_df: pd.DataFrame, overlap_map, defender_prior_map) -> pd.Series:
    rates: List[float] = []
    for row in full_df.itertuples(index=False):
        key = (int(row.game_id), int(row.player_id))
        defender_secs = overlap_map.get(key, {})
        pos_bucket = _norm_pos(getattr(row, "position_raw", "F"))
        weighted_num = 0.0
        weighted_den = 0.0
        for defender_id, sec in defender_secs.items():
            rate = defender_prior_map.get((int(row.game_id), int(defender_id), pos_bucket), math.nan)
            if rate is None or not math.isfinite(rate):
                continue
            weighted_num += float(rate) * float(sec)
            weighted_den += float(sec)
        rates.append((weighted_num / weighted_den) if weighted_den > 0 else math.nan)
    return pd.Series(rates, index=full_df.index, dtype="float64")


def _build_recent_faced_baseline(full_df: pd.DataFrame, sig_col: str) -> pd.Series:
    work = full_df.sort_values(["player_id", "game_date", "game_id"]).copy()
    hist: Dict[int, Deque[float]] = defaultdict(lambda: deque(maxlen=10))
    out: List[float] = []
    for row in work.itertuples(index=False):
        dq = hist[int(row.player_id)]
        out.append((sum(dq) / len(dq)) if dq else math.nan)
        sig = float(getattr(row, sig_col)) if pd.notna(getattr(row, sig_col)) else math.nan
        if math.isfinite(sig):
            dq.append(sig)
    work["faced_matchup_rate_last10"] = out
    return work.sort_index()["faced_matchup_rate_last10"]


def _apply_surprise(df: pd.DataFrame, alpha: float, clip_low: float, clip_high: float) -> pd.Series:
    base = pd.to_numeric(df["lambda_base"], errors="coerce").clip(lower=0.0)
    tonight = pd.to_numeric(df["matchup_signature_rate_per60"], errors="coerce")
    recent = pd.to_numeric(df["faced_matchup_rate_last10"], errors="coerce")
    ratio = (tonight / recent).replace([math.inf, -math.inf], math.nan)
    ratio = ratio.clip(lower=clip_low, upper=clip_high)
    mult = ratio.pow(alpha)
    mult = mult.where(mult.notna(), 1.0)
    return (base * mult).clip(lower=0.0)


def _score(df: pd.DataFrame, lam_col: str, prefix: str) -> Dict[str, object]:
    scored = df.copy()
    for line, threshold in THRESHOLDS.items():
        col = f"p_{prefix}_over_{str(line).replace('.', '_')}"
        scored[col] = pd.to_numeric(scored[lam_col], errors="coerce").apply(
            lambda lam: _poisson_tail(float(lam), threshold) if pd.notna(lam) else math.nan
        )
    out = {"overall": _combined_metric(scored, prefix), "by_line": {}}
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        out["by_line"][key] = _metric_rows(scored, f"p_{prefix}_over_{str(line).replace('.', '_')}", threshold)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate exact matchup-surprise adjustment on top of NHL SOG Poisson base.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--alpha", type=float, default=0.6)
    ap.add_argument("--clip-low", type=float, default=0.75)
    ap.add_argument("--clip-high", type=float, default=1.25)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
        if "season" in df.columns:
            df = df[pd.to_numeric(df["season"], errors="coerce") == int(args.season)].copy()
        if args.from_date:
            df = df[df["game_date"].astype(str) >= str(args.from_date)].copy()
        if args.to_date:
            df = df[df["game_date"].astype(str) <= str(args.to_date)].copy()
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)
    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)

    toi_df = _fetch_df(DEFENDER_TOI_SQL, args.season, args.from_date, args.to_date)
    shots_df = _fetch_df(DEFENDER_ONICE_SHOTS_SQL, args.season, args.from_date, args.to_date)
    if toi_df.empty or shots_df.empty:
        raise SystemExit("Missing defender TOI or exact on-ice shot-event data for requested window.")
    defender_prior_map = _build_defender_prior_rate_map(toi_df, shots_df)

    full_start = str(pd.Series(df["game_date"]).dropna().astype(str).min())
    full_end = str(pd.Series(df["game_date"]).dropna().astype(str).max())
    shift_rows = pg_fetchall(TEST_SHIFTS_SQL, (args.season, full_start, full_end))
    shifts_rows = pd.DataFrame(shift_rows or [])
    overlap_map = _build_test_overlap_map(shifts_rows, df)

    full = df.copy()
    full["matchup_signature_rate_per60"] = _build_matchup_signature(full, overlap_map, defender_prior_map)
    full["faced_matchup_rate_last10"] = _build_recent_faced_baseline(full, "matchup_signature_rate_per60")

    scored = full[full["game_date"].astype(str).isin(test_dates)].copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["lambda_matchup_surprise"] = _apply_surprise(scored, float(args.alpha), float(args.clip_low), float(args.clip_high))

    res_off = _score(scored.rename(columns={"lambda_offense": "lambda_eval"}), "lambda_eval", "offense")
    res_sur = _score(scored.rename(columns={"lambda_matchup_surprise": "lambda_eval"}), "lambda_eval", "matchup_surprise")

    summary = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "alpha": args.alpha,
        "clip_low": args.clip_low,
        "clip_high": args.clip_high,
        "train_rows": int(len(train)),
        "test_rows": int(len(scored)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "rows_with_matchup_signature": int(pd.to_numeric(scored['matchup_signature_rate_per60'], errors='coerce').notna().sum()),
            "rows_with_faced_matchup_baseline": int(pd.to_numeric(scored['faced_matchup_rate_last10'], errors='coerce').notna().sum()),
            "avg_matchup_signature": _round(pd.to_numeric(scored['matchup_signature_rate_per60'], errors='coerce').mean()),
            "avg_faced_matchup_baseline": _round(pd.to_numeric(scored['faced_matchup_rate_last10'], errors='coerce').mean()),
        },
        "offense": res_off,
        "matchup_surprise": res_sur,
    }
    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
