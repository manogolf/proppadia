#!/usr/bin/env python3
"""Compare alternative NHL SOG foundations on equal footing."""

from __future__ import annotations

import argparse
import itertools
import json
import math
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_position_defense_base import _combined_metric, _metric_rows, _split_df

THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}
PRIOR_ATTEMPT_GRID = (2.5, 5.0, 10.0, 20.0, 40.0)
WEIGHT_GRID = tuple(i / 10.0 for i in range(11))


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


def _score_probs(df: pd.DataFrame, lambda_col: str, prefix: str) -> pd.DataFrame:
    scored = df.copy()
    scored[lambda_col] = pd.to_numeric(scored[lambda_col], errors="coerce").clip(lower=0.0)
    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        scored[f"p_{prefix}_over_{key}"] = scored[lambda_col].apply(lambda v: _poisson_tail(float(v), threshold))
    return scored


def _eval_prefix(scored: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    out = {
        "overall": _combined_metric(scored, prefix),
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        out["by_line"][str(line)] = _metric_rows(scored, f"p_{prefix}_over_{key}", threshold)
    return out


def _topn_by_date(scored: pd.DataFrame, prefix: str, line: float, top_n: int) -> Dict[str, Any]:
    threshold = THRESHOLDS[line]
    col = f"p_{prefix}_over_{str(line).replace('.', '_')}"
    picks: List[pd.DataFrame] = []
    for _, group in scored.groupby("game_date"):
        ranked = group.sort_values(col, ascending=False).head(top_n)
        picks.append(ranked)
    if not picks:
        return {"n": 0, "hit_rate": None}
    picked = pd.concat(picks, ignore_index=True)
    hit_rate = float((pd.to_numeric(picked["shots_on_goal"], errors="coerce") >= threshold).mean())
    return {"n": int(len(picked)), "hit_rate": _round(hit_rate)}


def _weight_candidates() -> Iterable[Tuple[float, float, float]]:
    for w5, w10, w20 in itertools.product(WEIGHT_GRID, WEIGHT_GRID, WEIGHT_GRID):
        if abs((w5 + w10 + w20) - 1.0) < 1e-9:
            yield (w5, w10, w20)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "d5_sog_per60",
        "d10_sog_per60",
        "d20_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "shots_on_goal",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["d10_toi_min_avg", "shots_on_goal"]).copy()
    out["d10_toi_min_avg"] = out["d10_toi_min_avg"].clip(lower=0.0)
    out["position_bucket"] = out["position_raw"].fillna("F").astype(str).apply(lambda v: "D" if v.strip().upper() == "D" else "F")
    return out


def _lambda_from_rate(rate: pd.Series, toi: pd.Series) -> pd.Series:
    return (pd.to_numeric(rate, errors="coerce").clip(lower=0.0) * pd.to_numeric(toi, errors="coerce").clip(lower=0.0) / 60.0).clip(lower=0.0)


def _best_window_blend(train: pd.DataFrame, val: pd.DataFrame) -> Tuple[Tuple[float, float, float], Dict[str, Any]]:
    best_weights = (0.0, 1.0, 0.0)
    best_brier = float("inf")
    rows: List[Dict[str, Any]] = []
    for w5, w10, w20 in _weight_candidates():
        rate = (
            pd.to_numeric(val["d5_sog_per60"], errors="coerce").fillna(0.0) * w5
            + pd.to_numeric(val["d10_sog_per60"], errors="coerce").fillna(0.0) * w10
            + pd.to_numeric(val["d20_sog_per60"], errors="coerce").fillna(0.0) * w20
        )
        tmp = val.copy()
        tmp["lambda_candidate"] = _lambda_from_rate(rate, tmp["d10_toi_min_avg"])
        tmp = _score_probs(tmp, "lambda_candidate", "candidate")
        brier = float(_eval_prefix(tmp, "candidate")["overall"]["brier"])
        rows.append({"w5": w5, "w10": w10, "w20": w20, "brier": _round(brier, 6)})
        if brier < best_brier:
            best_brier = brier
            best_weights = (w5, w10, w20)
    return best_weights, {"best_brier": _round(best_brier, 6), "grid_size": len(rows)}


def _best_attempt_prior(train: pd.DataFrame, val: pd.DataFrame) -> Tuple[float, Dict[str, Any]]:
    pos_ratio = (
        train.assign(
            _sog=pd.to_numeric(train["d10_sog_per60"], errors="coerce").clip(lower=0.0),
            _att=pd.to_numeric(train["attempts_d10_per60"], errors="coerce").clip(lower=0.0),
        )
        .groupby("position_bucket", dropna=False)
        .agg(sog=("_sog", "sum"), att=("_att", "sum"))
    )
    pos_ratio["ratio"] = pos_ratio.apply(lambda r: (float(r["sog"]) / float(r["att"])) if float(r["att"]) > 0 else 0.55, axis=1)
    pos_ratio_map = pos_ratio["ratio"].to_dict()
    best_prior = PRIOR_ATTEMPT_GRID[0]
    best_brier = float("inf")
    rows: List[Dict[str, Any]] = []
    val_att = pd.to_numeric(val["attempts_d10_per60"], errors="coerce").clip(lower=0.0)
    val_sog = pd.to_numeric(val["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    pos_mean = val["position_bucket"].map(pos_ratio_map).fillna(float(pos_ratio["ratio"].mean()) if not pos_ratio.empty else 0.55)
    for prior in PRIOR_ATTEMPT_GRID:
        conv = (val_sog + (pos_mean * prior)) / (val_att + prior)
        conv = pd.to_numeric(conv, errors="coerce").clip(lower=0.05, upper=0.95)
        rate = val_att * conv
        tmp = val.copy()
        tmp["lambda_candidate"] = _lambda_from_rate(rate, tmp["d10_toi_min_avg"])
        tmp = _score_probs(tmp, "lambda_candidate", "candidate")
        brier = float(_eval_prefix(tmp, "candidate")["overall"]["brier"])
        rows.append({"prior_attempt_per60": prior, "brier": _round(brier, 6)})
        if brier < best_brier:
            best_brier = brier
            best_prior = prior
    return best_prior, {"best_brier": _round(best_brier, 6), "grid": rows, "position_ratio": {k: _round(v, 6) for k, v in pos_ratio_map.items()}}


def analyze(df: pd.DataFrame, test_game_days: int, validation_days: int) -> Dict[str, Any]:
    work = _prepare(df)
    train_full, test, train_dates, test_dates = _split_df(work, test_game_days)
    inner_train, val, inner_train_dates, val_dates = _split_df(train_full, validation_days)

    best_weights, blend_meta = _best_window_blend(inner_train, val)
    best_prior, attempt_meta = _best_attempt_prior(inner_train, val)

    train_pos_ratio = (
        train_full.assign(
            _sog=pd.to_numeric(train_full["d10_sog_per60"], errors="coerce").clip(lower=0.0),
            _att=pd.to_numeric(train_full["attempts_d10_per60"], errors="coerce").clip(lower=0.0),
        )
        .groupby("position_bucket", dropna=False)
        .agg(sog=("_sog", "sum"), att=("_att", "sum"))
    )
    train_pos_ratio["ratio"] = train_pos_ratio.apply(lambda r: (float(r["sog"]) / float(r["att"])) if float(r["att"]) > 0 else 0.55, axis=1)
    pos_ratio_map = train_pos_ratio["ratio"].to_dict()
    pos_mean = test["position_bucket"].map(pos_ratio_map).fillna(float(train_pos_ratio["ratio"].mean()) if not train_pos_ratio.empty else 0.55)

    scored = test.copy()
    scored["lambda_base_d10"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["lambda_base_d5"] = _lambda_from_rate(scored["d5_sog_per60"], scored["d10_toi_min_avg"])
    scored["lambda_base_d20"] = _lambda_from_rate(scored["d20_sog_per60"], scored["d10_toi_min_avg"])

    w5, w10, w20 = best_weights
    blended_rate = (
        pd.to_numeric(scored["d5_sog_per60"], errors="coerce").fillna(0.0) * w5
        + pd.to_numeric(scored["d10_sog_per60"], errors="coerce").fillna(0.0) * w10
        + pd.to_numeric(scored["d20_sog_per60"], errors="coerce").fillna(0.0) * w20
    )
    scored["lambda_base_window_blend"] = _lambda_from_rate(blended_rate, scored["d10_toi_min_avg"])

    att = pd.to_numeric(scored["attempts_d10_per60"], errors="coerce").clip(lower=0.0)
    sog = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    conv = (sog + (pos_mean * best_prior)) / (att + best_prior)
    conv = pd.to_numeric(conv, errors="coerce").clip(lower=0.05, upper=0.95)
    scored["attempts_on_net_conversion"] = conv
    scored["lambda_base_attempts_first"] = _lambda_from_rate(att * conv, scored["d10_toi_min_avg"])

    for prefix, col in {
        "d10": "lambda_base_d10",
        "d5": "lambda_base_d5",
        "d20": "lambda_base_d20",
        "window_blend": "lambda_base_window_blend",
        "attempts_first": "lambda_base_attempts_first",
    }.items():
        scored = _score_probs(scored, col, prefix)

    result = {
        "ok": True,
        "rows": {"train": int(len(train_full)), "test": int(len(test))},
        "dates": {
            "train_min": min(train_dates),
            "train_max": max(train_dates),
            "test_min": min(test_dates),
            "test_max": max(test_dates),
            "validation_min": min(val_dates),
            "validation_max": max(val_dates),
            "test_game_days": int(test_game_days),
            "validation_days": int(validation_days),
        },
        "candidates": {
            "d10": _eval_prefix(scored, "d10"),
            "d5": _eval_prefix(scored, "d5"),
            "d20": _eval_prefix(scored, "d20"),
            "window_blend": _eval_prefix(scored, "window_blend"),
            "attempts_first": _eval_prefix(scored, "attempts_first"),
        },
        "tuning": {
            "window_blend": {
                "weights": {"d5": w5, "d10": w10, "d20": w20},
                **blend_meta,
            },
            "attempts_first": {
                "prior_attempt_per60": best_prior,
                **attempt_meta,
            },
        },
        "top_n": {},
    }
    for line in THRESHOLDS:
        lk = str(line)
        result["top_n"][lk] = {}
        for n in (5, 10, 20):
            result["top_n"][lk][str(n)] = {
                "d10": _topn_by_date(scored, "d10", line, n),
                "d5": _topn_by_date(scored, "d5", line, n),
                "d20": _topn_by_date(scored, "d20", line, n),
                "window_blend": _topn_by_date(scored, "window_blend", line, n),
                "attempts_first": _topn_by_date(scored, "attempts_first", line, n),
            }
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare alternative NHL SOG foundations on equal footing.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--validation-days", type=int, default=14)
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

    result = analyze(df, args.test_game_days, args.validation_days)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
