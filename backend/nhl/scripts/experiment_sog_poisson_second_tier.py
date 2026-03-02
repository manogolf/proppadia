#!/usr/bin/env python3
"""Train and evaluate a simple second-tier NHL SOG correction on top of Poisson."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df


FEATURES = [
    "attempts_d10_per60",
    "d5_minus_d10",
    "d20_minus_d10",
    "role_pp_share",
    "toi_trend_3v10",
    "d10_toi_cv",
]

LINES = (1.5, 2.5, 3.5)
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


@dataclass
class Split:
    train: pd.DataFrame
    test: pd.DataFrame
    train_dates: List[str]
    test_dates: List[str]


def _split_df(df: pd.DataFrame, test_game_days: int) -> Split:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return Split(train=train, test=test, train_dates=train_dates, test_dates=test_dates)


def _fit_and_score(df: pd.DataFrame, test_game_days: int) -> Dict[str, Any]:
    split = _split_df(df, test_game_days)

    X_train = split.train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    y_train = pd.to_numeric(split.train["target_log1p_residual"], errors="coerce").fillna(0.0)

    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", RidgeCV(alphas=ALPHAS)),
        ]
    )
    model.fit(X_train, y_train)

    X_test = split.test[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    correction = pd.Series(model.predict(X_test), index=split.test.index, dtype=float)

    scored = split.test.copy()
    scored["poisson_lambda"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["correction_pred"] = correction
    scored["lambda_corrected"] = np.expm1(np.log1p(scored["poisson_lambda"]) + scored["correction_pred"]).clip(lower=0.0)

    for line, threshold in THRESHOLDS.items():
        base_col = f"p_base_over_{str(line).replace('.', '_')}"
        corr_col = f"p_corr_over_{str(line).replace('.', '_')}"
        scored[base_col] = scored["poisson_lambda"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[corr_col] = scored["lambda_corrected"].apply(lambda v: _poisson_tail(float(v), threshold))

    ridge = model.named_steps["ridge"]
    coef_rows = []
    for name, coef in zip(FEATURES, ridge.coef_):
        coef_rows.append({"feature": name, "coefficient": _round(float(coef), 6)})
    coef_rows.sort(key=lambda row: abs(row["coefficient"] or 0.0), reverse=True)

    result: Dict[str, Any] = {
        "ok": True,
        "rows": {
            "train": int(len(split.train)),
            "test": int(len(split.test)),
        },
        "dates": {
            "train_min": split.train_dates[0],
            "train_max": split.train_dates[-1],
            "test_min": split.test_dates[0],
            "test_max": split.test_dates[-1],
            "test_game_days": int(test_game_days),
        },
        "model": {
            "type": "ridge_log1p_residual",
            "alpha": _round(float(ridge.alpha_), 6),
            "features": coef_rows,
        },
        "overall": {},
        "by_line": {},
    }

    for line, threshold in THRESHOLDS.items():
        base_col = f"p_base_over_{str(line).replace('.', '_')}"
        corr_col = f"p_corr_over_{str(line).replace('.', '_')}"
        result["by_line"][str(line)] = {
            "poisson": _metric_rows(scored, base_col, threshold),
            "corrected": _metric_rows(scored, corr_col, threshold),
            "poisson_by_expected_bucket": _bucket_stats(scored, base_col, threshold),
            "corrected_by_expected_bucket": _bucket_stats(scored, corr_col, threshold),
        }

    expanded_rows: List[pd.DataFrame] = []
    for line, threshold in THRESHOLDS.items():
        base_col = f"p_base_over_{str(line).replace('.', '_')}"
        corr_col = f"p_corr_over_{str(line).replace('.', '_')}"
        tmp = scored[["shots_on_goal", base_col, corr_col]].copy()
        tmp["threshold"] = threshold
        expanded_rows.append(tmp)

    # Build explicit combined metrics to avoid column-name confusion.
    def _combined_metric(prob_col: str) -> Dict[str, Any]:
        probs = pd.concat(
            [scored[f"p_base_over_{str(line).replace('.', '_')}"] if prob_col == "poisson"
             else scored[f"p_corr_over_{str(line).replace('.', '_')}"] for line in THRESHOLDS],
            ignore_index=True,
        )
        ys = pd.concat(
            [(pd.to_numeric(scored["shots_on_goal"], errors="coerce") >= threshold).astype(int) for threshold in THRESHOLDS.values()],
            ignore_index=True,
        )
        n = int(len(probs))
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

    result["overall"] = {
        "poisson": _combined_metric("poisson"),
        "corrected": _combined_metric("corrected"),
    }
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Experiment with a second-tier correction on top of the NHL SOG Poisson base.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None, help="Optional prebuilt dataset CSV.")
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)

    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    result = _fit_and_score(df, args.test_game_days)
    if args.write_scored_csv:
        # Re-run fit once to persist holdout rows with corrected lambdas/probabilities.
        split = _split_df(df, args.test_game_days)
        X_train = split.train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        y_train = pd.to_numeric(split.train["target_log1p_residual"], errors="coerce").fillna(0.0)
        model = Pipeline([("scaler", StandardScaler()), ("ridge", RidgeCV(alphas=ALPHAS))])
        model.fit(X_train, y_train)
        X_test = split.test[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        scored = split.test.copy()
        scored["correction_pred"] = model.predict(X_test)
        scored["lambda_corrected"] = np.expm1(np.log1p(pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)) + scored["correction_pred"]).clip(lower=0.0)
        for line, threshold in THRESHOLDS.items():
            scored[f"p_base_over_{str(line).replace('.', '_')}"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0).apply(lambda v: _poisson_tail(float(v), threshold))
            scored[f"p_corr_over_{str(line).replace('.', '_')}"] = scored["lambda_corrected"].apply(lambda v: _poisson_tail(float(v), threshold))
        out_path = Path(args.write_scored_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scored.to_csv(out_path, index=False)
        result["scored_csv"] = str(out_path)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
