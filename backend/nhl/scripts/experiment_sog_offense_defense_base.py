#!/usr/bin/env python3
"""Compare offense-only, defense-only, and combined NHL SOG base estimators."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df


THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _metric_rows(df: pd.DataFrame, prob_col: str, threshold: int) -> Dict[str, Any]:
    if df.empty:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    probs = pd.to_numeric(df[prob_col], errors="coerce")
    ys = (pd.to_numeric(df["shots_on_goal"], errors="coerce") >= threshold).astype(int)
    mask = probs.notna() & ys.notna()
    probs = probs[mask].astype(float)
    ys = ys[mask].astype(int)
    n = int(len(probs))
    if n == 0:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    avg_p = float(probs.mean())
    hit_rate = float(ys.mean())
    brier = float(((probs - ys) ** 2).mean())
    return {
        "n": n,
        "avg_p": _round(avg_p),
        "hit_rate": _round(hit_rate),
        "gap": _round(avg_p - hit_rate),
        "brier": _round(brier),
    }


def _bucket_stats(df: pd.DataFrame, prob_col: str, threshold: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket, group in sorted(df.groupby("expected_sog_bucket"), key=lambda item: item[0]):
        out.append(
            {
                "segment_value": bucket,
                "n": int(len(group)),
                prob_col: _metric_rows(group, prob_col, threshold),
            }
        )
    return out


def _combined_metric(scored: pd.DataFrame, kind: str) -> Dict[str, Any]:
    probs = pd.concat(
        [scored[f"p_{kind}_over_{str(line).replace('.', '_')}"] for line in THRESHOLDS],
        ignore_index=True,
    )
    ys = pd.concat(
        [
            (pd.to_numeric(scored["shots_on_goal"], errors="coerce") >= threshold).astype(int)
            for threshold in THRESHOLDS.values()
        ],
        ignore_index=True,
    )
    return {
        "n": int(len(probs)),
        "avg_p": _round(float(probs.mean())),
        "hit_rate": _round(float(ys.mean())),
        "gap": _round(float(probs.mean() - ys.mean())),
        "brier": _round(float(((probs - ys) ** 2).mean())),
    }


def _split_df(df: pd.DataFrame, test_game_days: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return train, test, train_dates, test_dates


def _normalize_share(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    over_one = vals > 1.0
    if over_one.any():
        vals.loc[over_one] = vals.loc[over_one] / 100.0
    return vals.clip(lower=0.0)


def _safe_ratio(num: pd.Series, den: float | None, default: float = 1.0) -> pd.Series:
    vals = pd.to_numeric(num, errors="coerce")
    if den is None or not math.isfinite(den) or den <= 0:
        return pd.Series(default, index=vals.index, dtype=float)
    ratio = vals / float(den)
    ratio = ratio.where(ratio.notna(), other=default)
    return ratio.clip(lower=0.25, upper=4.0)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare offense-only vs defense-side vs combined NHL SOG base estimators.")
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

    scored = test.copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["last10_team_sog_share"] = _normalize_share(scored["last10_team_sog_share"])
    scored["opp_d10_sf_allowed_per_game"] = pd.to_numeric(scored["opp_d10_sf_allowed_per_game"], errors="coerce")
    scored["projected_goalie_d10_shots_faced_per60"] = pd.to_numeric(
        scored["projected_goalie_d10_shots_faced_per60"], errors="coerce"
    )

    league_team_allowed = float(
        pd.to_numeric(train["opp_d10_sf_allowed_per_game"], errors="coerce").dropna().mean()
    )
    league_goalie_sf60 = float(
        pd.to_numeric(train["projected_goalie_d10_shots_faced_per60"], errors="coerce").dropna().mean()
    )

    scored["goalie_env_factor"] = _safe_ratio(scored["projected_goalie_d10_shots_faced_per60"], league_goalie_sf60, default=1.0)
    scored["lambda_defense"] = (
        scored["last10_team_sog_share"]
        * scored["opp_d10_sf_allowed_per_game"].fillna(league_team_allowed)
        * scored["goalie_env_factor"]
    ).clip(lower=0.0)

    both_mask = (scored["lambda_offense"] > 0) & (scored["lambda_defense"] > 0)
    scored["lambda_combined"] = scored["lambda_offense"]
    scored.loc[both_mask, "lambda_combined"] = (
        scored.loc[both_mask, "lambda_offense"] * scored.loc[both_mask, "lambda_defense"]
    ) ** 0.5
    scored.loc[(~both_mask) & (scored["lambda_defense"] > 0), "lambda_combined"] = scored.loc[
        (~both_mask) & (scored["lambda_defense"] > 0), "lambda_defense"
    ]

    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        scored[f"p_offense_over_{key}"] = scored["lambda_offense"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_defense_over_{key}"] = scored["lambda_defense"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_combined_over_{key}"] = scored["lambda_combined"].apply(lambda v: _poisson_tail(float(v), threshold))

    result: Dict[str, Any] = {
        "ok": True,
        "rows": {
            "train": int(len(train)),
            "test": int(len(test)),
        },
        "dates": {
            "train_min": train_dates[0],
            "train_max": train_dates[-1],
            "test_min": test_dates[0],
            "test_max": test_dates[-1],
            "test_game_days": int(args.test_game_days),
        },
        "coverage": {
            "league_team_allowed": _round(league_team_allowed, 6),
            "league_goalie_sf60": _round(league_goalie_sf60, 6),
            "rows_with_team_share": int(scored["last10_team_sog_share"].notna().sum()),
            "rows_with_opp_allowed": int(scored["opp_d10_sf_allowed_per_game"].notna().sum()),
            "rows_with_goalie_env": int(scored["projected_goalie_d10_shots_faced_per60"].notna().sum()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense": _combined_metric(scored, "defense"),
            "combined": _combined_metric(scored, "combined"),
        },
        "by_line": {},
    }

    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        result["by_line"][str(line)] = {
            "offense": _metric_rows(scored, f"p_offense_over_{key}", threshold),
            "defense": _metric_rows(scored, f"p_defense_over_{key}", threshold),
            "combined": _metric_rows(scored, f"p_combined_over_{key}", threshold),
            "offense_by_expected_bucket": _bucket_stats(scored, f"p_offense_over_{key}", threshold),
            "defense_by_expected_bucket": _bucket_stats(scored, f"p_defense_over_{key}", threshold),
            "combined_by_expected_bucket": _bucket_stats(scored, f"p_combined_over_{key}", threshold),
        }

    if args.write_scored_csv:
        out_path = Path(args.write_scored_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scored.to_csv(out_path, index=False)
        result["scored_csv"] = str(out_path)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
