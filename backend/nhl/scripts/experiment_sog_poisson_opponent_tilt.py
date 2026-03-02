#!/usr/bin/env python3
"""Evaluate an opponent-defense tilt on top of the NHL SOG Poisson base."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df


FEATURES = [
    "opp_d10_sf_allowed_per_game",
    "pace_matchup_index",
]

THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}
ALPHAS = np.array([0.01, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], dtype=float)


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


def _split_df(df: pd.DataFrame, test_game_days: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return train, test, train_dates, test_dates


def _combined_metric(scored: pd.DataFrame, kind: str) -> Dict[str, Any]:
    probs = pd.concat(
        [
            scored[f"p_{kind}_over_{str(line).replace('.', '_')}"]
            for line in THRESHOLDS
        ],
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate an opponent-only tilt on top of the NHL SOG Poisson base.")
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

    X_train = train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    y_train = pd.to_numeric(train["target_log1p_residual"], errors="coerce").fillna(0.0)
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", RidgeCV(alphas=ALPHAS)),
        ]
    )
    model.fit(X_train, y_train)

    scored = test.copy()
    scored["lambda_base"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    X_test = scored[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    scored["opponent_tilt"] = model.predict(X_test)
    scored["lambda_opponent_tilt"] = np.expm1(np.log1p(scored["lambda_base"]) + scored["opponent_tilt"]).clip(lower=0.0)

    for line, threshold in THRESHOLDS.items():
        base_col = f"p_poisson_over_{str(line).replace('.', '_')}"
        tilt_col = f"p_tilt_over_{str(line).replace('.', '_')}"
        scored[base_col] = scored["lambda_base"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[tilt_col] = scored["lambda_opponent_tilt"].apply(lambda v: _poisson_tail(float(v), threshold))

    ridge = model.named_steps["ridge"]
    coef_rows = [
        {"feature": name, "coefficient": _round(float(coef), 6)}
        for name, coef in zip(FEATURES, ridge.coef_)
    ]
    coef_rows.sort(key=lambda row: abs(row["coefficient"] or 0.0), reverse=True)

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
        "model": {
            "type": "ridge_opponent_tilt",
            "alpha": _round(float(ridge.alpha_), 6),
            "features": coef_rows,
        },
        "overall": {
            "poisson": _combined_metric(scored, "poisson"),
            "opponent_tilt": _combined_metric(scored, "tilt"),
        },
        "by_line": {},
    }

    for line, threshold in THRESHOLDS.items():
        base_col = f"p_poisson_over_{str(line).replace('.', '_')}"
        tilt_col = f"p_tilt_over_{str(line).replace('.', '_')}"
        result["by_line"][str(line)] = {
            "poisson": _metric_rows(scored, base_col, threshold),
            "opponent_tilt": _metric_rows(scored, tilt_col, threshold),
            "poisson_by_expected_bucket": _bucket_stats(scored, base_col, threshold),
            "opponent_tilt_by_expected_bucket": _bucket_stats(scored, tilt_col, threshold),
        }

    if args.write_scored_csv:
        out_path = Path(args.write_scored_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scored.to_csv(out_path, index=False)
        result["scored_csv"] = str(out_path)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
