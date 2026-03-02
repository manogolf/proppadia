#!/usr/bin/env python3
"""Evaluate opponent+actual-goalie conditioned continuous defense using kernel-weighted similar players."""

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
from backend.shared.db.pg import pg_fetchall

GOALIE_SQL = """
SELECT DISTINCT ON (l.game_id, l.team_id)
  g.game_date::date AS game_date,
  l.game_id::bigint AS game_id,
  l.team_id::bigint AS defending_team_id,
  l.player_id::bigint AS goalie_id,
  COALESCE(l.start_flag, FALSE) AS start_flag,
  COALESCE(l.toi_minutes, 0)::float8 AS toi_minutes
FROM nhl.goalie_game_logs_raw l
JOIN nhl.games g USING (game_id)
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
ORDER BY l.game_id, l.team_id, COALESCE(l.start_flag, FALSE) DESC, COALESCE(l.toi_minutes, 0) DESC, l.player_id
"""


def _norm_pos(raw: str | None) -> str:
    return "D" if str(raw or "").strip() == "D" else "F"


def _gaussian_weight(dist: float, bandwidth: float) -> float:
    z = dist / max(1e-9, bandwidth)
    return math.exp(-0.5 * z * z)


def _fetch_goalie_df(season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(GOALIE_SQL, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _attach_goalies(df: pd.DataFrame, goalie_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    goalie_map = {
        (int(r.game_id), int(r.defending_team_id)): int(r.goalie_id)
        for r in goalie_df.itertuples(index=False)
        if pd.notna(r.goalie_id)
    }
    out["actual_goalie_id"] = out.apply(
        lambda row: goalie_map.get((int(row["game_id"]), int(row["opponent_id"])), math.nan),
        axis=1,
    )
    return out


def _build_group_rows(train: pd.DataFrame) -> Dict[Tuple[int, int, str], List[Tuple[float, float]]]:
    out: Dict[Tuple[int, int, str], List[Tuple[float, float]]] = {}
    work = train.copy()
    work = work[pd.to_numeric(work["actual_goalie_id"], errors="coerce").notna()].copy()
    work["pos_bucket"] = work["position_raw"].apply(_norm_pos)
    work["obs_rate_per60"] = (
        pd.to_numeric(work["shots_on_goal"], errors="coerce").clip(lower=0.0)
        * 60.0
        / pd.to_numeric(work["d10_toi_min_avg"], errors="coerce").clip(lower=1e-6)
    )
    for (opponent_id, goalie_id, pos_bucket), grp in work.groupby(["opponent_id", "actual_goalie_id", "pos_bucket"], sort=False):
        rows = [
            (float(r.lambda_base), float(r.obs_rate_per60))
            for r in grp.itertuples(index=False)
            if pd.notna(r.lambda_base) and pd.notna(r.obs_rate_per60)
        ]
        out[(int(opponent_id), int(goalie_id), str(pos_bucket))] = rows
    return out


def _build_opponent_group_rows(train: pd.DataFrame) -> Dict[Tuple[int, str], List[Tuple[float, float]]]:
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
        description="Evaluate opponent+actual-goalie conditioned continuous defense using kernel-weighted similar players."
    )
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--bandwidth", type=float, default=0.6)
    ap.add_argument("--goalie-weight", type=float, default=0.7)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)
    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    goalie_df = _fetch_goalie_df(args.season, args.from_date, args.to_date)
    if goalie_df.empty:
        raise SystemExit("No goalie rows available for the requested season/date range.")

    df = _attach_goalies(df, goalie_df)
    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    goalie_rows = _build_group_rows(train)
    opp_rows = _build_opponent_group_rows(train)

    scored = test.copy()
    scored["pos_bucket"] = scored["position_raw"].apply(_norm_pos)
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    opp_rates = []
    goalie_rates = []
    hybrid_rates = []
    for row in scored.itertuples(index=False):
        lam = float(row.lambda_offense)
        pos_bucket = str(row.pos_bucket)
        opp_rate = _estimate_rate(opp_rows.get((int(row.opponent_id), pos_bucket), []), lam, float(args.bandwidth))
        goalie_rate = math.nan
        if pd.notna(row.actual_goalie_id):
            goalie_rate = _estimate_rate(
                goalie_rows.get((int(row.opponent_id), int(row.actual_goalie_id), pos_bucket), []),
                lam,
                float(args.bandwidth),
            )
        if math.isfinite(goalie_rate) and math.isfinite(opp_rate):
            hybrid = float(args.goalie_weight) * goalie_rate + (1.0 - float(args.goalie_weight)) * opp_rate
        elif math.isfinite(goalie_rate):
            hybrid = goalie_rate
        else:
            hybrid = opp_rate
        opp_rates.append(opp_rate)
        goalie_rates.append(goalie_rate)
        hybrid_rates.append(hybrid)

    scored["opponent_kernel_allowed_rate_per60"] = opp_rates
    scored["goalie_kernel_allowed_rate_per60"] = goalie_rates
    scored["hybrid_kernel_allowed_rate_per60"] = hybrid_rates

    for rate_col, lam_col in [
        ("opponent_kernel_allowed_rate_per60", "lambda_defense_opp_kernel"),
        ("goalie_kernel_allowed_rate_per60", "lambda_defense_goalie_kernel"),
        ("hybrid_kernel_allowed_rate_per60", "lambda_defense_hybrid_kernel"),
    ]:
        scored[lam_col] = (
            pd.to_numeric(scored[rate_col], errors="coerce").clip(lower=0.0)
            * scored["d10_toi_min_avg"]
            / 60.0
        ).clip(lower=0.0)

    for label, def_col in [
        ("opp_kernel", "lambda_defense_opp_kernel"),
        ("goalie_kernel", "lambda_defense_goalie_kernel"),
        ("hybrid_kernel", "lambda_defense_hybrid_kernel"),
    ]:
        both = (scored["lambda_offense"] > 0) & (scored[def_col] > 0)
        out_col = f"lambda_combined_{label}"
        scored[out_col] = scored["lambda_offense"]
        scored.loc[both, out_col] = (scored.loc[both, "lambda_offense"] * scored.loc[both, def_col]) ** 0.5
        scored.loc[(~both) & (scored[def_col] > 0), out_col] = scored.loc[(~both) & (scored[def_col] > 0), def_col]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("combined_opp_kernel", "lambda_combined_opp_kernel"),
        ("combined_goalie_kernel", "lambda_combined_goalie_kernel"),
        ("combined_hybrid_kernel", "lambda_combined_hybrid_kernel"),
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
        "goalie_weight": args.goalie_weight,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "opp_groups": int(len(opp_rows)),
            "goalie_groups": int(len(goalie_rows)),
            "rows_with_actual_goalie": int(pd.to_numeric(scored["actual_goalie_id"], errors="coerce").notna().sum()),
            "rows_with_opp_rate": int(pd.to_numeric(scored["opponent_kernel_allowed_rate_per60"], errors="coerce").notna().sum()),
            "rows_with_goalie_rate": int(pd.to_numeric(scored["goalie_kernel_allowed_rate_per60"], errors="coerce").notna().sum()),
            "rows_with_hybrid_rate": int(pd.to_numeric(scored["hybrid_kernel_allowed_rate_per60"], errors="coerce").notna().sum()),
            "avg_opp_rate": _round(pd.to_numeric(scored["opponent_kernel_allowed_rate_per60"], errors="coerce").mean()),
            "avg_goalie_rate": _round(pd.to_numeric(scored["goalie_kernel_allowed_rate_per60"], errors="coerce").mean()),
            "avg_hybrid_rate": _round(pd.to_numeric(scored["hybrid_kernel_allowed_rate_per60"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "combined_opp_kernel": _combined_metric(scored, "combined_opp_kernel"),
            "combined_goalie_kernel": _combined_metric(scored, "combined_goalie_kernel"),
            "combined_hybrid_kernel": _combined_metric(scored, "combined_hybrid_kernel"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "combined_opp_kernel": _metric_rows(scored, f"p_combined_opp_kernel_over_{str(line).replace('.', '_')}", threshold),
            "combined_goalie_kernel": _metric_rows(scored, f"p_combined_goalie_kernel_over_{str(line).replace('.', '_')}", threshold),
            "combined_hybrid_kernel": _metric_rows(scored, f"p_combined_hybrid_kernel_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
