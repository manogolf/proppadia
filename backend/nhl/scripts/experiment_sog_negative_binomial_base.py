#!/usr/bin/env python3
"""Evaluate equal-ground Poisson and Negative Binomial NHL SOG candidates vs the live Poisson base."""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import nbinom, poisson

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_position_defense_base import _combined_metric, _metric_rows, _split_df


THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}
NB_ALPHA_GRID = (0.10, 0.20, 0.35, 0.50, 0.75, 1.00, 1.50)
FEATURES = [
    "log_d10_sog_per60",
    "log_attempts_d10_per60",
    "d5_minus_d10",
    "d20_minus_d10",
    "role_pp_share",
    "toi_trend_3v10",
    "d10_toi_cv",
    "log_opp_d10_sf_per60",
    "log_opp_d10_sa_per60",
    "pace_matchup_index",
    "log_goalie_d10_shots_faced_per60",
    "projected_goalie_d10_save_pct",
    "is_home_num",
    "is_defenseman",
]


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


def _nb_tail(mu: float, alpha: float, threshold: int) -> float:
    if not math.isfinite(mu) or mu < 0 or not math.isfinite(alpha) or alpha <= 0:
        return float("nan")
    r = 1.0 / alpha
    p = r / (r + mu)
    cutoff = max(0, threshold - 1)
    return max(0.0, min(1.0, 1.0 - float(nbinom.cdf(cutoff, r, p))))


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["log_d10_sog_per60"] = np.log1p(pd.to_numeric(out["d10_sog_per60"], errors="coerce").clip(lower=0.0))
    out["log_attempts_d10_per60"] = np.log1p(pd.to_numeric(out["attempts_d10_per60"], errors="coerce").clip(lower=0.0))
    out["d5_minus_d10"] = pd.to_numeric(out["d5_sog_per60"], errors="coerce") - pd.to_numeric(out["d10_sog_per60"], errors="coerce")
    out["d20_minus_d10"] = pd.to_numeric(out["d20_sog_per60"], errors="coerce") - pd.to_numeric(out["d10_sog_per60"], errors="coerce")
    out["role_pp_share"] = pd.to_numeric(out["role_pp_share"], errors="coerce")
    out["toi_trend_3v10"] = pd.to_numeric(out["toi_trend_3v10"], errors="coerce")
    out["d10_toi_cv"] = pd.to_numeric(out["d10_toi_cv"], errors="coerce")
    out["log_opp_d10_sf_per60"] = np.log1p(pd.to_numeric(out["opp_d10_sf_per60"], errors="coerce").clip(lower=0.0))
    out["log_opp_d10_sa_per60"] = np.log1p(pd.to_numeric(out["opp_d10_sa_per60"], errors="coerce").clip(lower=0.0))
    out["pace_matchup_index"] = pd.to_numeric(out["pace_matchup_index"], errors="coerce")
    out["log_goalie_d10_shots_faced_per60"] = np.log1p(pd.to_numeric(out["projected_goalie_d10_shots_faced_per60"], errors="coerce").clip(lower=0.0))
    out["projected_goalie_d10_save_pct"] = pd.to_numeric(out["projected_goalie_d10_save_pct"], errors="coerce")
    out["is_home_num"] = pd.to_numeric(out["is_home"], errors="coerce").fillna(0.0)
    out["is_defenseman"] = (out["position_raw"].astype(str).str.strip().str.upper() == "D").astype(float)
    out["offset_log_minutes"] = np.log(pd.to_numeric(out["d10_toi_min_avg"], errors="coerce").clip(lower=1e-6) / 60.0)
    for col in FEATURES:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        med = float(out[col].median()) if out[col].notna().any() else 0.0
        out[col] = out[col].fillna(med)
    return out


def _zscore_fit(train: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    means = train[FEATURES].mean()
    stds = train[FEATURES].std(ddof=0).replace(0.0, 1.0).fillna(1.0)
    return means, stds


def _apply_zscore(df: pd.DataFrame, means: pd.Series, stds: pd.Series) -> pd.DataFrame:
    out = df.copy()
    out[FEATURES] = (out[FEATURES] - means) / stds
    return out


def _fit_poisson(train: pd.DataFrame):
    X = sm.add_constant(train[FEATURES], has_constant="add").astype(float)
    y = pd.to_numeric(train["shots_on_goal"], errors="coerce").astype(float)
    model = sm.GLM(y, X, family=sm.families.Poisson(), offset=train["offset_log_minutes"].astype(float))
    return model.fit(maxiter=200, tol=1e-8)


def _fit_nb(train: pd.DataFrame, alpha: float):
    X = sm.add_constant(train[FEATURES], has_constant="add").astype(float)
    y = pd.to_numeric(train["shots_on_goal"], errors="coerce").astype(float)
    model = sm.GLM(
        y,
        X,
        family=sm.families.NegativeBinomial(alpha=alpha),
        offset=train["offset_log_minutes"].astype(float),
    )
    return model.fit(maxiter=200, tol=1e-8)


def _score_probs(df: pd.DataFrame, mu_col: str, prefix: str, nb_alpha: float | None = None) -> pd.DataFrame:
    scored = df.copy()
    scored[f"mu_{prefix}"] = pd.to_numeric(scored[mu_col], errors="coerce").clip(lower=0.0)
    for line, threshold in THRESHOLDS.items():
        col = f"p_{prefix}_over_{str(line).replace('.', '_')}"
        if nb_alpha is None:
            scored[col] = scored[f"mu_{prefix}"].apply(lambda v: _poisson_tail(float(v), threshold))
        else:
            scored[col] = scored[f"mu_{prefix}"].apply(lambda v: _nb_tail(float(v), nb_alpha, threshold))
    return scored


def _combined_for_prefix(scored: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    tmp = scored.copy()
    for line in THRESHOLDS:
        tmp[f"p_{prefix}_over_{str(line).replace('.', '_')}"] = pd.to_numeric(
            tmp[f"p_{prefix}_over_{str(line).replace('.', '_')}"], errors="coerce"
        )
    return _combined_metric(tmp.rename(columns={f"p_{prefix}_over_{str(line).replace('.', '_')}": f"p_{prefix}_over_{str(line).replace('.', '_')}" for line in THRESHOLDS}), prefix)


def _evaluate(scored: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    out = {
        "overall": _combined_for_prefix(scored, prefix),
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        col = f"p_{prefix}_over_{str(line).replace('.', '_')}"
        out["by_line"][str(line)] = _metric_rows(scored, col, threshold)
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


def _fit_best_nb(train: pd.DataFrame, validation_days: int) -> tuple[float, Dict[str, Any]]:
    inner_train, val, inner_dates, val_dates = _split_df(train, validation_days)
    best_alpha = NB_ALPHA_GRID[0]
    best_score = float("inf")
    grid_rows: List[Dict[str, Any]] = []
    for alpha in NB_ALPHA_GRID:
        res = _fit_nb(inner_train, float(alpha))
        Xv = sm.add_constant(val[FEATURES], has_constant="add").astype(float)
        mu = np.clip(res.predict(Xv, offset=val["offset_log_minutes"].astype(float)), 1e-6, None)
        tmp = val.copy()
        tmp["mu_nb"] = mu
        tmp = _score_probs(tmp, "mu_nb", "nb", nb_alpha=float(alpha))
        brier = float(_combined_for_prefix(tmp, "nb")["brier"])
        grid_rows.append({"alpha": float(alpha), "brier": _round(brier, 6)})
        if brier < best_score:
            best_score = brier
            best_alpha = float(alpha)
    meta = {
        "validation_days": int(validation_days),
        "validation_from": min(val_dates),
        "validation_to": max(val_dates),
        "grid": grid_rows,
        "best_alpha": best_alpha,
        "best_brier": _round(best_score, 6),
    }
    return best_alpha, meta


def analyze(df: pd.DataFrame, test_game_days: int, validation_days: int) -> Dict[str, Any]:
    work = _build_features(df)
    train, test, train_dates, test_dates = _split_df(work, test_game_days)
    means, stds = _zscore_fit(train)
    train = _apply_zscore(train, means, stds)
    test = _apply_zscore(test, means, stds)

    best_nb_alpha, nb_meta = _fit_best_nb(train, validation_days)

    pois = _fit_poisson(train)
    nb = _fit_nb(train, best_nb_alpha)

    X_test = sm.add_constant(test[FEATURES], has_constant="add").astype(float)
    test = test.copy()
    test["mu_base"] = pd.to_numeric(test["lambda_base"], errors="coerce").clip(lower=0.0)
    test["mu_poisson_glm"] = np.clip(pois.predict(X_test, offset=test["offset_log_minutes"].astype(float)), 1e-6, None)
    test["mu_nb_glm"] = np.clip(nb.predict(X_test, offset=test["offset_log_minutes"].astype(float)), 1e-6, None)

    test = _score_probs(test, "mu_base", "base")
    test = _score_probs(test, "mu_poisson_glm", "poisson_glm")
    test = _score_probs(test, "mu_nb_glm", "nb_glm", nb_alpha=best_nb_alpha)

    result = {
        "ok": True,
        "rows": {"train": int(len(train)), "test": int(len(test))},
        "dates": {
            "train_min": min(train_dates),
            "train_max": max(train_dates),
            "test_min": min(test_dates),
            "test_max": max(test_dates),
            "test_game_days": int(test_game_days),
        },
        "nb_tuning": nb_meta,
        "features": FEATURES,
        "base": _evaluate(test, "base"),
        "poisson_glm": _evaluate(test, "poisson_glm"),
        "negative_binomial_glm": _evaluate(test, "nb_glm"),
        "top_n": {},
    }
    for line in THRESHOLDS:
        line_key = str(line)
        result["top_n"][line_key] = {}
        for n in (5, 10, 20):
            result["top_n"][line_key][str(n)] = {
                "base": _topn_by_date(test, "base", line, n),
                "poisson_glm": _topn_by_date(test, "poisson_glm", line, n),
                "negative_binomial_glm": _topn_by_date(test, "nb_glm", line, n),
            }
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate equal-ground Poisson and Negative Binomial NHL SOG candidates.")
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

    print(json.dumps(analyze(df, args.test_game_days, args.validation_days), indent=2))


if __name__ == "__main__":
    main()
