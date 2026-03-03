#!/usr/bin/env python3
"""Evaluate a live-capable projected defense-surprise branch on top of the NHL SOG Poisson base."""

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


def _safe_float(v) -> float:
    try:
        x = float(v)
    except Exception:
        return math.nan
    return x if math.isfinite(x) else math.nan


def _gaussian_weight(dist: float, bandwidth: float) -> float:
    z = dist / max(1e-9, bandwidth)
    return math.exp(-0.5 * z * z)


def _feature_distance(target: dict, cand: dict) -> float:
    parts = []
    for key, scale in [
        ("lambda_base", 1.0),
        ("pace_matchup_index", 0.10),
        ("opp_d10_sf_per60", 1.50),
        ("opp_d10_sa_per60", 1.50),
    ]:
        t = _safe_float(target.get(key))
        c = _safe_float(cand.get(key))
        if math.isfinite(t) and math.isfinite(c):
            parts.append(((t - c) / scale) ** 2)
    if not parts:
        return math.nan
    return math.sqrt(sum(parts) / len(parts))


def _build_projected_rows(train: pd.DataFrame) -> tuple[
    Dict[Tuple[int, int, str], List[dict]],
    Dict[Tuple[int, str], List[dict]],
]:
    work = train.copy()
    work["pos_bucket"] = work["position_raw"].apply(_norm_pos)
    work["obs_rate_per60"] = (
        pd.to_numeric(work["shots_on_goal"], errors="coerce").clip(lower=0.0)
        * 60.0
        / pd.to_numeric(work["d10_toi_min_avg"], errors="coerce").clip(lower=1e-6)
    )
    goalie_map: Dict[Tuple[int, int, str], List[dict]] = defaultdict(list)
    opp_map: Dict[Tuple[int, str], List[dict]] = defaultdict(list)
    for row in work.itertuples(index=False):
        rec = {
            "lambda_base": _safe_float(row.lambda_base),
            "pace_matchup_index": _safe_float(getattr(row, "pace_matchup_index", math.nan)),
            "opp_d10_sf_per60": _safe_float(getattr(row, "opp_d10_sf_per60", math.nan)),
            "opp_d10_sa_per60": _safe_float(getattr(row, "opp_d10_sa_per60", math.nan)),
            "obs_rate_per60": _safe_float(row.obs_rate_per60),
        }
        if not math.isfinite(rec["obs_rate_per60"]) or not math.isfinite(rec["lambda_base"]):
            continue
        pos = str(row.pos_bucket)
        opp_key = (int(row.opponent_id), pos)
        opp_map[opp_key].append(rec)
        gid = getattr(row, "projected_goalie_id", math.nan)
        if pd.notna(gid):
            goalie_key = (int(row.opponent_id), int(gid), pos)
            goalie_map[goalie_key].append(rec)
    return goalie_map, opp_map


def _estimate_rate(rows: List[dict], target: dict, bandwidth: float) -> float:
    if not rows:
        return math.nan
    num = 0.0
    den = 0.0
    for cand in rows:
        dist = _feature_distance(target, cand)
        if not math.isfinite(dist):
            continue
        w = _gaussian_weight(dist, bandwidth)
        num += w * float(cand["obs_rate_per60"])
        den += w
    return (num / den) if den > 0 else math.nan


def _attach_projected_signature(
    df: pd.DataFrame,
    goalie_rows,
    opp_rows,
    bandwidth: float,
    goalie_weight: float,
) -> pd.DataFrame:
    out = df.copy()
    out["pos_bucket"] = out["position_raw"].apply(_norm_pos)
    vals: List[float] = []
    sig_source: List[str] = []
    for row in out.itertuples(index=False):
        target = {
            "lambda_base": _safe_float(getattr(row, "lambda_base", math.nan)),
            "pace_matchup_index": _safe_float(getattr(row, "pace_matchup_index", math.nan)),
            "opp_d10_sf_per60": _safe_float(getattr(row, "opp_d10_sf_per60", math.nan)),
            "opp_d10_sa_per60": _safe_float(getattr(row, "opp_d10_sa_per60", math.nan)),
        }
        pos = str(row.pos_bucket)
        opp_rate = _estimate_rate(opp_rows.get((int(row.opponent_id), pos), []), target, bandwidth)
        goalie_rate = math.nan
        gid = getattr(row, "projected_goalie_id", math.nan)
        if pd.notna(gid):
            goalie_rate = _estimate_rate(goalie_rows.get((int(row.opponent_id), int(gid), pos), []), target, bandwidth)
        if math.isfinite(goalie_rate) and math.isfinite(opp_rate):
            val = goalie_weight * goalie_rate + (1.0 - goalie_weight) * opp_rate
            src = "goalie+opponent"
        elif math.isfinite(goalie_rate):
            val = goalie_rate
            src = "goalie_only"
        else:
            val = opp_rate
            src = "opponent_only" if math.isfinite(opp_rate) else "missing"
        vals.append(val)
        sig_source.append(src)
    out["projected_signature_rate_per60"] = vals
    out["projected_signature_source"] = sig_source
    return out


def _build_recent_faced_baseline(full_df: pd.DataFrame) -> pd.Series:
    work = full_df.sort_values(["player_id", "game_date", "game_id"]).copy()
    hist: Dict[int, Deque[float]] = defaultdict(lambda: deque(maxlen=10))
    out: List[float] = []
    for row in work.itertuples(index=False):
        dq = hist[int(row.player_id)]
        out.append((sum(dq) / len(dq)) if dq else math.nan)
        sig = _safe_float(getattr(row, "projected_signature_rate_per60", math.nan))
        if math.isfinite(sig):
            dq.append(sig)
    work["faced_projected_rate_last10"] = out
    return work.sort_index()["faced_projected_rate_last10"]


def _apply_surprise(df: pd.DataFrame, alpha: float, clip_low: float, clip_high: float) -> pd.Series:
    base = pd.to_numeric(df["lambda_base"], errors="coerce").clip(lower=0.0)
    tonight = pd.to_numeric(df["projected_signature_rate_per60"], errors="coerce")
    recent = pd.to_numeric(df["faced_projected_rate_last10"], errors="coerce")
    ratio = (tonight / recent).replace([math.inf, -math.inf], math.nan)
    ratio = ratio.clip(lower=clip_low, upper=clip_high)
    mult = ratio.pow(alpha).where(ratio.notna(), 1.0)
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
        out["by_line"][str(line)] = _metric_rows(scored, f"p_{prefix}_over_{str(line).replace('.', '_')}", threshold)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate a pregame-capable projected defense-surprise branch for NHL SOG.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--bandwidth", type=float, default=0.6)
    ap.add_argument("--goalie-weight", type=float, default=0.7)
    ap.add_argument("--alpha", type=float, default=0.2)
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

    train, _, _, test_dates = _split_df(df, args.test_game_days)
    goalie_rows, opp_rows = _build_projected_rows(train)

    full = _attach_projected_signature(df.copy(), goalie_rows, opp_rows, float(args.bandwidth), float(args.goalie_weight))
    full["faced_projected_rate_last10"] = _build_recent_faced_baseline(full)

    scored = full[full["game_date"].astype(str).isin(test_dates)].copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["lambda_projected_surprise"] = _apply_surprise(
        scored,
        float(args.alpha),
        float(args.clip_low),
        float(args.clip_high),
    )

    res_off = _score(scored.rename(columns={"lambda_offense": "lambda_eval"}), "lambda_eval", "offense")
    res_sur = _score(scored.rename(columns={"lambda_projected_surprise": "lambda_eval"}), "lambda_eval", "projected_surprise")

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
            "rows_with_projected_signature": int(pd.to_numeric(scored["projected_signature_rate_per60"], errors="coerce").notna().sum()),
            "rows_with_faced_projected_baseline": int(pd.to_numeric(scored["faced_projected_rate_last10"], errors="coerce").notna().sum()),
            "projected_signature_source_counts": scored["projected_signature_source"].value_counts(dropna=False).to_dict(),
        },
        "offense": res_off,
        "projected_surprise": res_sur,
    }
    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
