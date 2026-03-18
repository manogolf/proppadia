#!/usr/bin/env python3
"""Train/test NHL SOG market-prior residual models (per line) on out-of-time holdout."""

from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss


LINES: dict[float, int] = {1.5: 2, 2.5: 3, 3.5: 4}
SHOT_MARKETS = {"player_shots_on_goal", "player_shots_on_goal_alternate"}


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


def _norm_name(s: Any) -> str:
    if not isinstance(s, str):
        s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\\s]", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s


def _short_key(norm: str) -> str:
    parts = norm.split()
    if not parts:
        return ""
    return f"{parts[0][0]} {parts[-1]}"


def _pick_market_player_name(outcome: dict[str, Any]) -> str | None:
    name = outcome.get("name")
    desc = outcome.get("description")
    part = outcome.get("participant")
    name_s = name.strip() if isinstance(name, str) else ""
    desc_s = desc.strip() if isinstance(desc, str) else ""
    part_s = part.strip() if isinstance(part, str) else ""
    if name_s.lower() in ("over", "under"):
        return desc_s or part_s or None
    if desc_s.lower() in ("over", "under"):
        return name_s or part_s or None
    return desc_s or part_s or (name_s or None)


def _outcome_side(outcome: dict[str, Any]) -> str | None:
    for key in ("name", "description", "label", "type"):
        val = outcome.get(key)
        if isinstance(val, str):
            s = val.strip().lower()
            if s in ("over", "under"):
                return s
    return None


def _american_to_prob(price: Any) -> float:
    try:
        a = float(price)
    except Exception:
        return float("nan")
    if not math.isfinite(a) or a == 0:
        return float("nan")
    if a > 0:
        return 100.0 / (a + 100.0)
    return (-a) / ((-a) + 100.0)


def _logit(p: pd.Series) -> pd.Series:
    q = pd.to_numeric(p, errors="coerce").clip(1e-6, 1 - 1e-6)
    return np.log(q / (1.0 - q))


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam**k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _metric(probs: pd.Series, y: pd.Series) -> dict[str, Any]:
    p = pd.to_numeric(probs, errors="coerce")
    t = pd.to_numeric(y, errors="coerce")
    mask = p.notna() & t.notna()
    p = p[mask].astype(float)
    t = t[mask].astype(int)
    if p.empty:
        return {
            "n": 0,
            "avg_p": None,
            "hit_rate": None,
            "gap": None,
            "brier": None,
            "logloss": None,
            "accuracy_50": None,
        }
    brier = float(((p - t) ** 2).mean())
    ll = float(log_loss(t, p, labels=[0, 1]))
    acc = float(((p >= 0.5).astype(int) == t).mean())
    return {
        "n": int(len(p)),
        "avg_p": _round(float(p.mean())),
        "hit_rate": _round(float(t.mean())),
        "gap": _round(float(p.mean() - t.mean())),
        "brier": _round(brier),
        "logloss": _round(ll),
        "accuracy_50": _round(acc),
    }


def _price_to_profit_per_unit(price: float) -> float:
    if price > 0:
        return price / 100.0
    return 100.0 / abs(price)


def _edge_policy_roi(
    probs: pd.Series,
    market_probs: pd.Series,
    prices: pd.Series,
    y: pd.Series,
    edge_threshold: float,
) -> dict[str, Any]:
    p = pd.to_numeric(probs, errors="coerce")
    m = pd.to_numeric(market_probs, errors="coerce")
    pr = pd.to_numeric(prices, errors="coerce")
    t = pd.to_numeric(y, errors="coerce")
    mask = p.notna() & m.notna() & pr.notna() & t.notna()
    if not mask.any():
        return {"bets": 0, "wins": 0, "losses": 0, "roi": None}
    p = p[mask].astype(float)
    m = m[mask].astype(float)
    pr = pr[mask].astype(float)
    t = t[mask].astype(int)
    bet_mask = (p - m) > float(edge_threshold)
    if not bet_mask.any():
        return {"bets": 0, "wins": 0, "losses": 0, "roi": None}
    pb = p[bet_mask]
    prb = pr[bet_mask]
    tb = t[bet_mask]
    wins = int((tb == 1).sum())
    losses = int((tb == 0).sum())
    profits = np.where(tb.values == 1, [_price_to_profit_per_unit(x) for x in prb.values], -1.0)
    roi = float(np.mean(profits))
    return {"bets": int(len(pb)), "wins": wins, "losses": losses, "roi": _round(roi)}


def _load_market_rows(
    odds_root: Path,
    from_date: str | None,
    to_date: str | None,
    bookmaker_key: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day_dir in sorted(odds_root.glob("20??-??-??")):
        day = day_dir.name
        if from_date and day < str(from_date):
            continue
        if to_date and day > str(to_date):
            continue
        # Read both snapshots when available:
        # - odds_latest_compatible.json (legacy normalized)
        # - odds_latest.json (raw daily snapshot)
        #
        # This prevents date holes when only one of the two artifacts is present.
        events_sources: list[list[Any]] = []
        for fname in ("odds_latest_compatible.json", "odds_latest.json"):
            fp = day_dir / fname
            if not fp.exists():
                continue
            try:
                events = json.loads(fp.read_text())
            except Exception:
                continue
            if isinstance(events, list):
                events_sources.append(events)

        if not events_sources:
            continue

        for events in events_sources:
            for ev in events:
                books = ev.get("bookmakers", [])
                if not isinstance(books, list):
                    continue
                for book in books:
                    if str(book.get("key", "")).strip() != bookmaker_key:
                        continue
                    markets = book.get("markets", [])
                    if not isinstance(markets, list):
                        continue
                    for market in markets:
                        market_key = str(market.get("key", "")).strip()
                        if market_key not in SHOT_MARKETS:
                            continue
                        outcomes = market.get("outcomes", [])
                        if not isinstance(outcomes, list):
                            continue
                        for outcome in outcomes:
                            if not isinstance(outcome, dict):
                                continue
                            if _outcome_side(outcome) != "over":
                                continue
                            name = _pick_market_player_name(outcome)
                            if not name:
                                continue
                            line = pd.to_numeric(outcome.get("point"), errors="coerce")
                            price = pd.to_numeric(outcome.get("price"), errors="coerce")
                            if pd.isna(line) or pd.isna(price):
                                continue
                            key = _short_key(_norm_name(name))
                            if not key:
                                continue
                            rows.append(
                                {
                                    "game_date": day,
                                    "player_key": key,
                                    "line": float(line),
                                    "price_over": float(price),
                                    "bookmaker": bookmaker_key,
                                    "market_key": market_key,
                                }
                            )
    if not rows:
        return pd.DataFrame(
            columns=["game_date", "player_key", "line", "price_over", "bookmaker", "market_key", "p_mkt"]
        )
    df = pd.DataFrame(rows)
    df = (
        df.groupby(["game_date", "player_key", "line"], as_index=False)
        .agg(price_over=("price_over", "median"))
        .copy()
    )
    df["p_mkt"] = df["price_over"].map(_american_to_prob)
    return df


def _load_feature_rows(dataset_csv: Path, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    usecols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "shots_on_goal",
        "lambda_base",
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "toi_trend_3v10",
        "d10_toi_cv",
        "pace_matchup_index",
        "is_home",
    ]
    df = pd.read_csv(dataset_csv, usecols=usecols)
    df["game_date"] = df["game_date"].astype(str)
    if from_date:
        df = df[df["game_date"] >= str(from_date)]
    if to_date:
        df = df[df["game_date"] <= str(to_date)]
    df["player_key"] = df["player_name"].map(_norm_name).map(_short_key)
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    for c in [
        "shots_on_goal",
        "lambda_base",
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "toi_trend_3v10",
        "d10_toi_cv",
        "pace_matchup_index",
        "is_home",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # NHL player appears once per day; enforce a single row per player/day for merge robustness.
    df = (
        df.sort_values(["game_date", "player_id", "game_id"])
        .dropna(subset=["player_key"])
        .drop_duplicates(["game_date", "player_key"], keep="last")
        .reset_index(drop=True)
    )
    return df


def _prepare_matched(
    dataset_csv: Path,
    odds_root: Path,
    bookmaker_key: str,
    from_date: str | None,
    to_date: str | None,
) -> pd.DataFrame:
    feats = _load_feature_rows(dataset_csv, from_date, to_date)
    market = _load_market_rows(odds_root, from_date, to_date, bookmaker_key)
    df = market.merge(feats, on=["game_date", "player_key"], how="inner")
    df = df[df["line"].isin(list(LINES.keys()))].copy()
    for line, threshold in LINES.items():
        key = str(line).replace(".", "_")
        mask = df["line"] == float(line)
        df.loc[mask, "y_over"] = (df.loc[mask, "shots_on_goal"] >= threshold).astype(int)
        df.loc[mask, "p_base"] = df.loc[mask, "lambda_base"].apply(lambda v: _poisson_tail(float(v), threshold))
        df.loc[mask, f"hit_over_{key}"] = df.loc[mask, "y_over"]
    df["market_logit"] = _logit(df["p_mkt"])
    df["base_logit"] = _logit(df["p_base"])
    return df.reset_index(drop=True)


def _fit_line_model(
    line_df: pd.DataFrame,
    test_game_days: int,
    edge_threshold: float,
) -> Tuple[dict[str, Any], pd.DataFrame]:
    dates = sorted(line_df["game_date"].dropna().astype(str).unique().tolist())
    if len(dates) <= int(test_game_days):
        raise ValueError(
            f"line={line_df['line'].iloc[0]} needs > {test_game_days} distinct dates; found {len(dates)}"
        )
    test_dates = set(dates[-int(test_game_days) :])
    train = line_df[~line_df["game_date"].isin(test_dates)].copy()
    test = line_df[line_df["game_date"].isin(test_dates)].copy()

    feature_cols = [
        "market_logit",
        "base_logit",
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "toi_trend_3v10",
        "d10_toi_cv",
        "pace_matchup_index",
        "is_home",
    ]

    for col in feature_cols:
        med = float(pd.to_numeric(train[col], errors="coerce").median()) if train[col].notna().any() else 0.0
        train[col] = pd.to_numeric(train[col], errors="coerce").fillna(med)
        test[col] = pd.to_numeric(test[col], errors="coerce").fillna(med)

    y_train = pd.to_numeric(train["y_over"], errors="coerce").astype(int)
    y_test = pd.to_numeric(test["y_over"], errors="coerce").astype(int)

    model = LogisticRegression(max_iter=2000, C=1.0, solver="lbfgs")
    model.fit(train[feature_cols], y_train)
    p_model = pd.Series(model.predict_proba(test[feature_cols])[:, 1], index=test.index)

    out = test.copy()
    out["p_model"] = p_model
    out["edge_model_vs_market"] = out["p_model"] - out["p_mkt"]
    out["edge_base_vs_market"] = out["p_base"] - out["p_mkt"]

    summary = {
        "rows": {"train": int(len(train)), "test": int(len(test))},
        "dates": {
            "train_min": min(train["game_date"]) if len(train) else None,
            "train_max": max(train["game_date"]) if len(train) else None,
            "test_min": min(test["game_date"]) if len(test) else None,
            "test_max": max(test["game_date"]) if len(test) else None,
            "distinct_test_dates": int(test["game_date"].nunique()),
        },
        "metrics": {
            "market_only": _metric(out["p_mkt"], y_test),
            "base_only": _metric(out["p_base"], y_test),
            "residual_model": _metric(out["p_model"], y_test),
        },
        "edge_policy_over": {
            "edge_threshold": float(edge_threshold),
            "market_only": _edge_policy_roi(out["p_mkt"], out["p_mkt"], out["price_over"], y_test, edge_threshold),
            "base_only": _edge_policy_roi(out["p_base"], out["p_mkt"], out["price_over"], y_test, edge_threshold),
            "residual_model": _edge_policy_roi(out["p_model"], out["p_mkt"], out["price_over"], y_test, edge_threshold),
        },
        "coefficients": {
            "intercept": _round(float(model.intercept_[0]), 6),
            "weights": {k: _round(float(v), 6) for k, v in zip(feature_cols, model.coef_[0].tolist())},
        },
    }
    return summary, out


def run_experiment(
    dataset_csv: Path,
    odds_root: Path,
    bookmaker_key: str,
    from_date: str | None,
    to_date: str | None,
    test_game_days: int,
    edge_threshold: float,
) -> Tuple[dict[str, Any], pd.DataFrame]:
    matched = _prepare_matched(dataset_csv, odds_root, bookmaker_key, from_date, to_date)
    if matched.empty:
        raise ValueError("No matched market + feature rows were found.")

    line_out: dict[str, Any] = {}
    holdouts: list[pd.DataFrame] = []
    for line in sorted(LINES):
        sub = matched[matched["line"] == float(line)].copy()
        if sub.empty:
            line_out[str(line)] = {"error": "no rows"}
            continue
        try:
            summary, hold = _fit_line_model(sub, test_game_days, edge_threshold)
            line_out[str(line)] = summary
            holdouts.append(hold)
        except Exception as exc:
            line_out[str(line)] = {"error": str(exc)}

    holdout_df = pd.concat(holdouts, ignore_index=True) if holdouts else pd.DataFrame()

    out = {
        "ok": True,
        "bookmaker": bookmaker_key,
        "inputs": {
            "dataset_csv": str(dataset_csv),
            "odds_root": str(odds_root),
            "from_date": from_date,
            "to_date": to_date,
            "test_game_days": int(test_game_days),
            "edge_threshold": float(edge_threshold),
        },
        "matched_rows": int(len(matched)),
        "matched_dates": int(matched["game_date"].nunique()),
        "coverage_by_line": {
            str(line): {
                "rows": int((matched["line"] == line).sum()),
                "dates": int(matched.loc[matched["line"] == line, "game_date"].nunique()),
            }
            for line in sorted(LINES)
        },
        "by_line": line_out,
    }
    return out, holdout_df


def main() -> None:
    ap = argparse.ArgumentParser(description="Train/test market-prior residual SOG models by line.")
    ap.add_argument(
        "--dataset-csv",
        default="backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv",
    )
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--test-game-days", type=int, default=7)
    ap.add_argument("--edge-threshold", type=float, default=0.0)
    ap.add_argument("--out-json", default="tmp/nhl_sog_market_residual_logit.json")
    ap.add_argument("--out-holdout-csv", default="tmp/nhl_sog_market_residual_logit_holdout.csv")
    args = ap.parse_args()

    result, holdout = run_experiment(
        dataset_csv=Path(args.dataset_csv),
        odds_root=Path(args.odds_root),
        bookmaker_key=str(args.bookmaker),
        from_date=args.from_date,
        to_date=args.to_date,
        test_game_days=int(args.test_game_days),
        edge_threshold=float(args.edge_threshold),
    )

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, indent=2))

    out_csv = Path(args.out_holdout_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if holdout.empty:
        out_csv.write_text("")
    else:
        cols = [
            "game_date",
            "line",
            "player_id",
            "player_name",
            "shots_on_goal",
            "y_over",
            "price_over",
            "p_mkt",
            "p_base",
            "p_model",
            "edge_base_vs_market",
            "edge_model_vs_market",
            "d10_sog_per60",
            "attempts_d10_per60",
            "d10_toi_min_avg",
            "role_pp_share",
            "toi_trend_3v10",
            "d10_toi_cv",
            "pace_matchup_index",
            "is_home",
        ]
        keep = [c for c in cols if c in holdout.columns]
        holdout[keep].sort_values(["game_date", "line", "player_name"]).to_csv(out_csv, index=False)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
