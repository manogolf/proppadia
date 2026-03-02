#!/usr/bin/env python3
"""Evaluate opponent-conditioned continuous archetype defense using kernel-weighted similar players."""

from __future__ import annotations

import argparse
import json
import math
from typing import Dict, List, Tuple

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


def _build_group_rows(train: pd.DataFrame) -> Dict[Tuple[int, str], List[Tuple[float, float]]]:
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
    weighted_num = 0.0
    weighted_den = 0.0
    for train_lam, obs_rate in rows:
        w = _gaussian_weight(float(train_lam) - float(lam), bandwidth)
        weighted_num += w * float(obs_rate)
        weighted_den += w
    return (weighted_num / weighted_den) if weighted_den > 0 else math.nan


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate opponent-conditioned continuous archetype defense using kernel-weighted similar players."
    )
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--bandwidth", type=float, default=0.6)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)
    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    group_rows = _build_group_rows(train)

    scored = test.copy()
    scored["pos_bucket"] = scored["position_raw"].apply(_norm_pos)
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    defense_rates = []
    for row in scored.itertuples(index=False):
        rows = group_rows.get((int(row.opponent_id), str(row.pos_bucket)), [])
        defense_rates.append(_estimate_rate(rows, float(row.lambda_offense), float(args.bandwidth)))

    scored["opponent_kernel_allowed_rate_per60"] = defense_rates
    scored["lambda_defense_kernel"] = (
        pd.to_numeric(scored["opponent_kernel_allowed_rate_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_kernel"] > 0)
    scored["lambda_combined_kernel"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_kernel"] = (
        (scored.loc[both, "lambda_offense"] * scored.loc[both, "lambda_defense_kernel"]) ** 0.5
    )
    scored.loc[(~both) & (scored["lambda_defense_kernel"] > 0), "lambda_combined_kernel"] = scored.loc[
        (~both) & (scored["lambda_defense_kernel"] > 0), "lambda_defense_kernel"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_kernel", "lambda_defense_kernel"),
        ("combined_kernel", "lambda_combined_kernel"),
    ]:
        for line, threshold in THRESHOLDS.items():
            col = f"p_{kind}_over_{str(line).replace('.', '_')}"
            scored[col] = pd.to_numeric(scored[lam_col], errors="coerce").apply(
                lambda lam: _poisson_tail(float(lam), threshold) if pd.notna(lam) else math.nan
            )

    summary = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "bandwidth": args.bandwidth,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "groups": int(len(group_rows)),
            "rows_with_kernel_rate": int(pd.to_numeric(scored["opponent_kernel_allowed_rate_per60"], errors="coerce").notna().sum()),
            "avg_kernel_rate": _round(pd.to_numeric(scored["opponent_kernel_allowed_rate_per60"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_kernel": _combined_metric(scored, "defense_kernel"),
            "combined_kernel": _combined_metric(scored, "combined_kernel"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_kernel": _metric_rows(scored, f"p_defense_kernel_over_{str(line).replace('.', '_')}", threshold),
            "combined_kernel": _metric_rows(scored, f"p_combined_kernel_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
