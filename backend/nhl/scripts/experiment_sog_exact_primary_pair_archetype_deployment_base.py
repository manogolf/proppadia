#!/usr/bin/env python3
"""Evaluate primary opposing defense-pair deployment using shooter expected-SOG archetype buckets."""

from __future__ import annotations

import argparse
import json
import math

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
from backend.nhl.scripts.experiment_sog_exact_onice_pair_archetype_deployment_base import (
    SHIFT_ROWS_SQL,
    SHOT_EVENT_SQL,
    _fetch_df,
    _build_pair_prior_rate_map,
    _build_test_pair_overlap_map,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate primary opposing defense-pair deployment using shooter expected-SOG archetype buckets."
    )
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)
    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    bucket_map = {
        (int(row.game_id), int(row.player_id)): str(row.expected_sog_bucket)
        for row in train.itertuples(index=False)
    }

    shifts_df = _fetch_df(SHIFT_ROWS_SQL, args.season, args.from_date, args.to_date)
    shot_df = _fetch_df(SHOT_EVENT_SQL, args.season, args.from_date, args.to_date)
    if shifts_df.empty or shot_df.empty:
        raise SystemExit("Missing shiftcharts or shot event data for the requested window.")

    pair_prior_map = _build_pair_prior_rate_map(shifts_df, shot_df, bucket_map)
    test_shifts = shifts_df[shifts_df["game_date"].astype(str).isin(test_dates)].copy()
    pair_overlap_map = _build_test_pair_overlap_map(test_shifts, test)

    scored = test.copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    primary_rates = []
    primary_overlap_minutes = []
    for row in scored.itertuples(index=False):
        pair_secs = pair_overlap_map.get((int(row.game_id), int(row.player_id)), {})
        shooter_bucket = str(row.expected_sog_bucket)
        if not pair_secs:
            primary_rates.append(math.nan)
            primary_overlap_minutes.append(math.nan)
            continue
        primary_pair, primary_sec = max(pair_secs.items(), key=lambda kv: kv[1])
        rate = pair_prior_map.get((int(row.game_id), int(primary_pair[0]), int(primary_pair[1]), shooter_bucket), math.nan)
        primary_rates.append(rate if rate is not None else math.nan)
        primary_overlap_minutes.append(float(primary_sec) / 60.0)

    scored["primary_pair_archetype_allowed_d10_per60"] = primary_rates
    scored["primary_pair_overlap_minutes"] = primary_overlap_minutes
    scored["lambda_defense_primary_pair"] = (
        pd.to_numeric(scored["primary_pair_archetype_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_primary_pair"] > 0)
    scored["lambda_combined_primary_pair"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_primary_pair"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "primary_pair_archetype_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_primary_pair"] > 0), "lambda_combined_primary_pair"] = scored.loc[
        (~both) & (scored["lambda_defense_primary_pair"] > 0), "lambda_defense_primary_pair"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_primary_pair", "lambda_defense_primary_pair"),
        ("combined_primary_pair", "lambda_combined_primary_pair"),
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
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "shift_rows": int(len(shifts_df)),
            "shot_rows": int(len(shot_df)),
            "rows_with_primary_pair_rate": int(pd.to_numeric(scored["primary_pair_archetype_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_primary_pair_overlap_minutes": _round(pd.to_numeric(scored["primary_pair_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_primary_pair": _combined_metric(scored, "defense_primary_pair"),
            "combined_primary_pair": _combined_metric(scored, "combined_primary_pair"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_primary_pair": _metric_rows(scored, f"p_defense_primary_pair_over_{str(line).replace('.', '_')}", threshold),
            "combined_primary_pair": _metric_rows(scored, f"p_combined_primary_pair_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
