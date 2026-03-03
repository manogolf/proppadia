#!/usr/bin/env python3
"""Replay the live NHL SOG Poisson base over a full season/date range."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df


THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


def _score_probs(df: pd.DataFrame) -> pd.DataFrame:
    scored = df.copy()
    lam = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        scored[f"p_over_{key}"] = lam.apply(lambda v: _poisson_tail(float(v), threshold))
    return scored


def _metric_rows(df: pd.DataFrame, prob_col: str, threshold: int) -> Dict[str, Any]:
    if df.empty:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    probs = pd.to_numeric(df[prob_col], errors="coerce")
    ys = (pd.to_numeric(df["shots_on_goal"], errors="coerce") >= threshold).astype(int)
    mask = probs.notna() & ys.notna()
    probs = probs[mask].astype(float)
    ys = ys[mask].astype(int)
    if probs.empty:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    avg_p = float(probs.mean())
    hit_rate = float(ys.mean())
    brier = float(((probs - ys) ** 2).mean())
    return {
        "n": int(len(probs)),
        "avg_p": _round(avg_p),
        "hit_rate": _round(hit_rate),
        "gap": _round(avg_p - hit_rate),
        "brier": _round(brier),
    }


def _combined_metric(scored: pd.DataFrame) -> Dict[str, Any]:
    probs = pd.concat(
        [scored[f"p_over_{str(line).replace('.', '_')}"] for line in THRESHOLDS],
        ignore_index=True,
    )
    ys = pd.concat(
        [
            (pd.to_numeric(scored["shots_on_goal"], errors="coerce") >= threshold).astype(int)
            for threshold in THRESHOLDS.values()
        ],
        ignore_index=True,
    )
    mask = probs.notna() & ys.notna()
    probs = probs[mask].astype(float)
    ys = ys[mask].astype(int)
    return {
        "n": int(len(probs)),
        "avg_p": _round(float(probs.mean())),
        "hit_rate": _round(float(ys.mean())),
        "gap": _round(float(probs.mean() - ys.mean())),
        "brier": _round(float(((probs - ys) ** 2).mean())),
    }


def _topn_by_date(scored: pd.DataFrame, line: float, top_n: int) -> Dict[str, Any]:
    threshold = THRESHOLDS[line]
    col = f"p_over_{str(line).replace('.', '_')}"
    picks: List[pd.DataFrame] = []
    for _, group in scored.groupby("game_date"):
        ranked = group.sort_values(col, ascending=False).head(top_n)
        picks.append(ranked)
    if not picks:
        return {"n": 0, "hit_rate": None}
    picked = pd.concat(picks, ignore_index=True)
    hit_rate = float((pd.to_numeric(picked["shots_on_goal"], errors="coerce") >= threshold).mean())
    return {"n": int(len(picked)), "hit_rate": _round(hit_rate)}


def _build_report_df(scored: pd.DataFrame) -> pd.DataFrame:
    report = scored.copy()
    report["expected_sog"] = pd.to_numeric(report["lambda_base"], errors="coerce")
    report["actual_sog"] = pd.to_numeric(report["shots_on_goal"], errors="coerce")
    report["shots_minus_expected"] = report["actual_sog"] - report["expected_sog"]
    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        report[f"hit_over_{key}"] = (pd.to_numeric(report["shots_on_goal"], errors="coerce") >= threshold).astype(int)
    cols = [
        "game_date",
        "season",
        "game_id",
        "player_id",
        "player_name",
        "position_raw",
        "team_id",
        "opponent_id",
        "actual_sog",
        "expected_sog",
        "expected_sog_bucket",
        "shots_minus_expected",
        "d5_sog_per60",
        "d10_sog_per60",
        "d20_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "toi_trend_3v10",
        "d10_toi_cv",
        "p_over_1_5",
        "p_over_2_5",
        "p_over_3_5",
        "hit_over_1_5",
        "hit_over_2_5",
        "hit_over_3_5",
    ]
    existing = [c for c in cols if c in report.columns]
    return report[existing].sort_values(["game_date", "player_name", "game_id"]).reset_index(drop=True)


def _largest_misses(report: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    if report.empty:
        return []
    ranked = report.copy()
    ranked["abs_error"] = pd.to_numeric(ranked["shots_minus_expected"], errors="coerce").abs()
    ranked = ranked.sort_values("abs_error", ascending=False).head(limit)
    out: List[Dict[str, Any]] = []
    for row in ranked.to_dict(orient="records"):
        out.append(
            {
                "game_date": str(row.get("game_date")),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "actual_sog": _round(row.get("actual_sog")),
                "expected_sog": _round(row.get("expected_sog")),
                "shots_minus_expected": _round(row.get("shots_minus_expected")),
                "p_over_1_5": _round(row.get("p_over_1_5")),
                "p_over_2_5": _round(row.get("p_over_2_5")),
                "p_over_3_5": _round(row.get("p_over_3_5")),
            }
        )
    return out


def analyze(df: pd.DataFrame, report_csv: str | None = None) -> Dict[str, Any]:
    if df.empty:
        raise ValueError("No rows available for the requested season/date range.")
    scored = _score_probs(df)
    report = _build_report_df(scored)
    report_path: str | None = None
    if report_csv:
        out_path = Path(report_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(out_path, index=False)
        report_path = str(out_path)
    out: Dict[str, Any] = {
        "ok": True,
        "rows": int(len(scored)),
        "dates": {
            "min": str(scored["game_date"].min()),
            "max": str(scored["game_date"].max()),
            "distinct": int(scored["game_date"].nunique()),
        },
        "players": int(scored["player_id"].nunique()),
        "overall": _combined_metric(scored),
        "by_line": {},
        "top_n": {},
        "report": {
            "csv": report_path,
            "largest_misses": _largest_misses(report),
        },
    }
    for line, threshold in THRESHOLDS.items():
        lk = str(line)
        pk = str(line).replace(".", "_")
        out["by_line"][lk] = _metric_rows(scored, f"p_over_{pk}", threshold)
        out["top_n"][lk] = {}
        for n in (5, 10, 20):
            out["top_n"][lk][str(n)] = _topn_by_date(scored, line, n)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay the live NHL SOG Poisson base over a full season/date range.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--report-csv", default=None)
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

    print(json.dumps(analyze(df, args.report_csv), indent=2))


if __name__ == "__main__":
    main()
