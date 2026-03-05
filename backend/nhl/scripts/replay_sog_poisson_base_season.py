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


def _load_market_coverage(
    history_root: Path,
    from_date: str | None = None,
    to_date: str | None = None,
) -> Dict[float, pd.DataFrame]:
    out: Dict[float, pd.DataFrame] = {1.5: pd.DataFrame(), 2.5: pd.DataFrame(), 3.5: pd.DataFrame()}
    files = sorted(history_root.glob("*/sog_with_market.csv"))
    if not files:
        return out

    rows: list[pd.DataFrame] = []
    for fp in files:
        try:
            df = pd.read_csv(
                fp,
                usecols=["game_date", "game_id", "player_id", "line", "price_over"],
            )
        except Exception:
            continue
        if df.empty:
            continue
        df["game_date"] = df["game_date"].astype(str)
        if from_date:
            df = df[df["game_date"] >= str(from_date)]
        if to_date:
            df = df[df["game_date"] <= str(to_date)]
        if df.empty:
            continue
        df["line"] = pd.to_numeric(df["line"], errors="coerce")
        df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce")
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
        df["price_over"] = pd.to_numeric(df["price_over"], errors="coerce")
        df = df.dropna(subset=["line", "game_id", "player_id", "price_over"])
        if df.empty:
            continue
        rows.append(df[["game_date", "game_id", "player_id", "line"]].copy())

    if not rows:
        return out

    all_cov = pd.concat(rows, ignore_index=True).drop_duplicates()
    all_cov["game_id"] = all_cov["game_id"].astype(int)
    all_cov["player_id"] = all_cov["player_id"].astype(int)
    for line in THRESHOLDS:
        sub = all_cov[all_cov["line"] == float(line)][["game_date", "game_id", "player_id"]].drop_duplicates()
        out[line] = sub.reset_index(drop=True)
    return out


def _filter_by_market(scored: pd.DataFrame, coverage_df: pd.DataFrame) -> pd.DataFrame:
    if coverage_df is None or coverage_df.empty:
        return scored.iloc[0:0].copy()
    keys = ["game_date", "game_id", "player_id"]
    base = scored.copy()
    base["game_date"] = base["game_date"].astype(str)
    return base.merge(coverage_df, on=keys, how="inner")


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


def _any_market_coverage(market_coverage: Dict[float, pd.DataFrame] | None) -> pd.DataFrame:
    if not market_coverage:
        return pd.DataFrame()
    parts: list[pd.DataFrame] = []
    for line in THRESHOLDS:
        cov = market_coverage.get(line)
        if cov is None or cov.empty:
            continue
        parts.append(cov[["game_date", "game_id", "player_id"]].drop_duplicates())
    if not parts:
        return pd.DataFrame()
    merged = pd.concat(parts, ignore_index=True).drop_duplicates()
    return merged.reset_index(drop=True)


def _base_sections(scored: pd.DataFrame) -> Dict[str, Any]:
    by_line: Dict[str, Any] = {}
    top_n: Dict[str, Any] = {}
    for line, threshold in THRESHOLDS.items():
        lk = str(line)
        pk = str(line).replace(".", "_")
        by_line[lk] = _metric_rows(scored, f"p_over_{pk}", threshold)
        top_n[lk] = {}
        for n in (5, 10, 20):
            top_n[lk][str(n)] = _topn_by_date(scored, line, n)
    return {
        "overall": _combined_metric(scored),
        "by_line": by_line,
        "top_n": top_n,
    }


def _bettable_sections(scored: pd.DataFrame, market_coverage: Dict[float, pd.DataFrame]) -> Dict[str, Any]:
    by_line: Dict[str, Any] = {}
    top_n: Dict[str, Any] = {}
    coverage: Dict[str, Any] = {}
    overall_probs: list[pd.Series] = []
    overall_ys: list[pd.Series] = []

    for line, threshold in THRESHOLDS.items():
        lk = str(line)
        pk = str(line).replace(".", "_")
        cov = market_coverage.get(line, pd.DataFrame())
        bet_scored = _filter_by_market(scored, cov)

        coverage[lk] = {
            "rows_scored": int(len(scored)),
            "rows_with_market": int(len(bet_scored)),
        }
        by_line[lk] = _metric_rows(bet_scored, f"p_over_{pk}", threshold)
        top_n[lk] = {}
        for n in (5, 10, 20):
            top_n[lk][str(n)] = _topn_by_date(bet_scored, line, n)

        if not bet_scored.empty:
            probs = pd.to_numeric(bet_scored[f"p_over_{pk}"], errors="coerce")
            ys = (pd.to_numeric(bet_scored["shots_on_goal"], errors="coerce") >= threshold).astype(int)
            mask = probs.notna() & ys.notna()
            probs = probs[mask].astype(float)
            ys = ys[mask].astype(int)
            if not probs.empty:
                overall_probs.append(probs)
                overall_ys.append(ys)

    if overall_probs:
        probs = pd.concat(overall_probs, ignore_index=True)
        ys = pd.concat(overall_ys, ignore_index=True).astype(int)
        overall = {
            "n": int(len(probs)),
            "avg_p": _round(float(probs.mean())),
            "hit_rate": _round(float(ys.mean())),
            "gap": _round(float(probs.mean() - ys.mean())),
            "brier": _round(float(((probs - ys) ** 2).mean())),
        }
    else:
        overall = {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}

    return {
        "overall": overall,
        "by_line": by_line,
        "top_n": top_n,
        "coverage": coverage,
    }


def analyze(
    df: pd.DataFrame,
    report_csv: str | None = None,
    market_coverage: Dict[float, pd.DataFrame] | None = None,
    metrics_scope: str = "bettable_only",
) -> Dict[str, Any]:
    if df.empty:
        raise ValueError("No rows available for the requested season/date range.")
    scored = _score_probs(df)
    if metrics_scope not in {"bettable_only", "all_rows"}:
        raise ValueError(f"Unsupported metrics_scope={metrics_scope!r}")

    report_scored = scored
    if metrics_scope == "bettable_only":
        any_cov = _any_market_coverage(market_coverage)
        report_scored = _filter_by_market(scored, any_cov)

    report = _build_report_df(report_scored)
    report_path: str | None = None
    if report_csv:
        out_path = Path(report_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(out_path, index=False)
        report_path = str(out_path)
    base_sections = _base_sections(scored)
    out: Dict[str, Any] = {
        "ok": True,
        "metrics_scope": metrics_scope,
        "rows": int(len(scored)),
        "dates": {
            "min": str(scored["game_date"].min()),
            "max": str(scored["game_date"].max()),
            "distinct": int(scored["game_date"].nunique()),
        },
        "players": int(scored["player_id"].nunique()),
        "overall": base_sections["overall"],
        "by_line": base_sections["by_line"],
        "top_n": base_sections["top_n"],
        "coverage": {},
        "report": {
            "csv": report_path,
            "largest_misses": _largest_misses(report),
            "rows": int(len(report)),
        },
    }

    if market_coverage is not None:
        bettable = _bettable_sections(scored, market_coverage)
        out["bettable_only"] = bettable
        if metrics_scope == "bettable_only":
            out["overall"] = bettable["overall"]
            out["by_line"] = bettable["by_line"]
            out["top_n"] = bettable["top_n"]
            out["coverage"] = bettable["coverage"]
            out["all_rows"] = base_sections

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay the live NHL SOG Poisson base over a full season/date range.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--report-csv", default=None)
    ap.add_argument(
        "--market-history-root",
        default="backend/nhl/exports/odds_history",
        help="History root with */sog_with_market.csv for bettable-only scoring.",
    )
    ap.add_argument(
        "--metrics-scope",
        choices=["bettable_only", "all_rows"],
        default="bettable_only",
        help="Primary summary scope. bettable_only excludes rows without matched market lines.",
    )
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

    market_cov = None
    if args.market_history_root:
        market_cov = _load_market_coverage(Path(args.market_history_root), args.from_date, args.to_date)
    if args.metrics_scope == "bettable_only" and market_cov is None:
        raise SystemExit("--metrics-scope bettable_only requires --market-history-root.")
    if args.metrics_scope == "bettable_only":
        cov_rows = 0
        if market_cov is not None:
            cov_rows = sum(int(len(market_cov.get(line, pd.DataFrame()))) for line in THRESHOLDS)
        if cov_rows == 0:
            raise SystemExit(
                "No market coverage rows found for the requested range. "
                "Set --metrics-scope all_rows to bypass or provide populated --market-history-root."
            )

    print(json.dumps(analyze(df, args.report_csv, market_cov, args.metrics_scope), indent=2))


if __name__ == "__main__":
    main()
