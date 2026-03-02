#!/usr/bin/env python3
"""Evaluate a defense-surprise adjustment: tonight's defense relative to recent faced defenses embedded in d10."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from typing import Deque, Dict, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_exact_onice_defender_deployment_base import (
    THRESHOLDS,
    _combined_metric,
    _metric_rows,
    _poisson_tail,
    _round,
    _split_df,
)


def _norm_pos(raw: str | None) -> str:
    return "D" if str(raw or "").strip() == "D" else "F"


def _gaussian_weight(dist: float, bandwidth: float) -> float:
    z = dist / max(1e-9, bandwidth)
    return math.exp(-0.5 * z * z)


def _build_goalie_rows(train: pd.DataFrame) -> Dict[Tuple[int, int, str], List[Tuple[float, float]]]:
    out: Dict[Tuple[int, int, str], List[Tuple[float, float]]] = {}
    if "projected_goalie_id" not in train.columns:
        return out
    work = train.copy()
    work = work[pd.to_numeric(work["projected_goalie_id"], errors="coerce").notna()].copy()
    work["pos_bucket"] = work["position_raw"].apply(_norm_pos)
    work["obs_rate_per60"] = (
        pd.to_numeric(work["shots_on_goal"], errors="coerce").clip(lower=0.0)
        * 60.0
        / pd.to_numeric(work["d10_toi_min_avg"], errors="coerce").clip(lower=1e-6)
    )
    for (opponent_id, goalie_id, pos_bucket), grp in work.groupby(["opponent_id", "projected_goalie_id", "pos_bucket"], sort=False):
        rows = [
            (float(r.lambda_base), float(r.obs_rate_per60))
            for r in grp.itertuples(index=False)
            if pd.notna(r.lambda_base) and pd.notna(r.obs_rate_per60)
        ]
        out[(int(opponent_id), int(goalie_id), str(pos_bucket))] = rows
    return out


def _build_opponent_rows(train: pd.DataFrame) -> Dict[Tuple[int, str], List[Tuple[float, float]]]:
    out: Dict[Tuple[int, str], List[Tuple[float, float]]] = {}
    work = train.copy()
    work["pos_bucket"] = work["position_raw"].apply(_norm_pos)
    work["obs_rate_per60"] = (
        pd.to_numeric(work["shots_on_goal"], errors="coerce").clip(lower=0.0)
        * 60.0
        / pd.to_numeric(work["d10_toi_min_avg"], errors="coerce").clip(lower=1e-6)
    )
    for (opponent_id, pos_bucket), grp in work.groupby(["opponent_id", "pos_bucket"], sort=False):
        rows = [
            (float(r.lambda_base), float(r.obs_rate_per60))
            for r in grp.itertuples(index=False)
            if pd.notna(r.lambda_base) and pd.notna(r.obs_rate_per60)
        ]
        out[(int(opponent_id), str(pos_bucket))] = rows
    return out


def _estimate_rate(rows: List[Tuple[float, float]], lam: float, bandwidth: float) -> float:
    if not rows:
        return math.nan
    num = 0.0
    den = 0.0
    for train_lam, obs_rate in rows:
        w = _gaussian_weight(float(train_lam) - float(lam), bandwidth)
        num += w * float(obs_rate)
        den += w
    return (num / den) if den > 0 else math.nan


def _attach_defense_signature(df: pd.DataFrame, opp_rows, goalie_rows, bandwidth: float, goalie_weight: float) -> pd.DataFrame:
    out = df.copy()
    out["pos_bucket"] = out["position_raw"].apply(_norm_pos)
    vals = []
    for row in out.itertuples(index=False):
        lam = float(row.lambda_base)
        pos = str(row.pos_bucket)
        opp_rate = _estimate_rate(opp_rows.get((int(row.opponent_id), pos), []), lam, bandwidth)
        goalie_rate = math.nan
        projected_goalie_id = getattr(row, "projected_goalie_id", math.nan)
        if pd.notna(projected_goalie_id):
            goalie_rate = _estimate_rate(
                goalie_rows.get((int(row.opponent_id), int(projected_goalie_id), pos), []),
                lam,
                bandwidth,
            )
        if math.isfinite(goalie_rate) and math.isfinite(opp_rate):
            val = goalie_weight * goalie_rate + (1.0 - goalie_weight) * opp_rate
        elif math.isfinite(goalie_rate):
            val = goalie_rate
        else:
            val = opp_rate
        vals.append(val)
    out["defense_signature_rate_per60"] = vals
    return out


def _build_recent_faced_baseline(full_df: pd.DataFrame) -> pd.Series:
    work = full_df.sort_values(["player_id", "game_date", "game_id"]).copy()
    hist: Dict[int, Deque[float]] = defaultdict(lambda: deque(maxlen=10))
    faced = []
    for row in work.itertuples(index=False):
        dq = hist[int(row.player_id)]
        if dq:
            faced.append(sum(dq) / len(dq))
        else:
            faced.append(math.nan)
        sig = float(row.defense_signature_rate_per60) if pd.notna(row.defense_signature_rate_per60) else math.nan
        if math.isfinite(sig):
            dq.append(sig)
    work["faced_defense_rate_last10"] = faced
    return work.sort_index()["faced_defense_rate_last10"]


def _apply_surprise_adjustment(df: pd.DataFrame, alpha: float, clip_low: float = 0.75, clip_high: float = 1.25) -> pd.Series:
    base = pd.to_numeric(df["lambda_base"], errors="coerce").clip(lower=0.0)
    tonight = pd.to_numeric(df["defense_signature_rate_per60"], errors="coerce")
    recent = pd.to_numeric(df["faced_defense_rate_last10"], errors="coerce")
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
    out = {
        "overall": _combined_metric(scored, prefix),
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        out["by_line"][key] = _metric_rows(scored, f"p_{prefix}_over_{str(line).replace('.', '_')}", threshold)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate defense-surprise adjustment on top of NHL SOG Poisson base.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--bandwidth", type=float, default=0.6)
    ap.add_argument("--goalie-weight", type=float, default=0.7)
    ap.add_argument("--alpha", type=float, default=0.25)
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
    opp_rows = _build_opponent_rows(train)
    goalie_rows = _build_goalie_rows(train)

    full = df.copy()
    full = _attach_defense_signature(full, opp_rows, goalie_rows, float(args.bandwidth), float(args.goalie_weight))
    full["faced_defense_rate_last10"] = _build_recent_faced_baseline(full)

    mask = full["game_date"].astype(str).isin(test_dates)
    scored = full[mask].copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["lambda_surprise"] = _apply_surprise_adjustment(scored, float(args.alpha))

    # preserve baseline for comparison
    res_off = _score(scored.rename(columns={"lambda_offense":"lambda_eval"}), "lambda_eval", "offense")
    res_sur = _score(scored.rename(columns={"lambda_surprise":"lambda_eval"}), "lambda_eval", "surprise")

    summary = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "bandwidth": args.bandwidth,
        "goalie_weight": args.goalie_weight,
        "alpha": args.alpha,
        "train_rows": int(len(train)),
        "test_rows": int(len(scored)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "rows_with_signature": int(pd.to_numeric(scored['defense_signature_rate_per60'], errors='coerce').notna().sum()),
            "rows_with_faced_baseline": int(pd.to_numeric(scored['faced_defense_rate_last10'], errors='coerce').notna().sum()),
            "avg_signature": _round(pd.to_numeric(scored['defense_signature_rate_per60'], errors='coerce').mean()),
            "avg_faced_baseline": _round(pd.to_numeric(scored['faced_defense_rate_last10'], errors='coerce').mean()),
        },
        "offense": res_off,
        "surprise": res_sur,
    }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary['write_scored_csv'] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
