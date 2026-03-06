#!/usr/bin/env python3
"""Month-by-month reconciliation of NHL SOG Poisson base vs BetOnline market."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from backend.nhl.scripts.experiment_sog_market_residual_logit import (
    LINES,
    _american_to_prob,
    _prepare_matched,
)


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


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


def _prob_to_fair_american(p: float) -> int | None:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def _roi_over_threshold(probs: pd.Series, prices: pd.Series, y: pd.Series, threshold: float) -> dict[str, Any]:
    p = pd.to_numeric(probs, errors="coerce")
    pr = pd.to_numeric(prices, errors="coerce")
    t = pd.to_numeric(y, errors="coerce")
    mask = p.notna() & pr.notna() & t.notna()
    if not mask.any():
        return {"bets": 0, "wins": 0, "losses": 0, "roi": None}
    p = p[mask].astype(float)
    pr = pr[mask].astype(float)
    t = t[mask].astype(int)

    take = p >= float(threshold)
    if not take.any():
        return {"bets": 0, "wins": 0, "losses": 0, "roi": None}

    prb = pr[take]
    tb = t[take]
    wins = int((tb == 1).sum())
    losses = int((tb == 0).sum())
    profits = [(_price_to_profit_per_unit(px) if yi == 1 else -1.0) for px, yi in zip(prb.values, tb.values)]
    roi = float(sum(profits) / len(profits))
    return {"bets": int(len(prb)), "wins": wins, "losses": losses, "roi": _round(roi)}


def _roi_edge_vs_market(
    probs: pd.Series, market_probs: pd.Series, prices: pd.Series, y: pd.Series, edge_threshold: float
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

    take = (p - m) > float(edge_threshold)
    if not take.any():
        return {"bets": 0, "wins": 0, "losses": 0, "roi": None}

    prb = pr[take]
    tb = t[take]
    wins = int((tb == 1).sum())
    losses = int((tb == 0).sum())
    profits = [(_price_to_profit_per_unit(px) if yi == 1 else -1.0) for px, yi in zip(prb.values, tb.values)]
    roi = float(sum(profits) / len(profits))
    return {"bets": int(len(prb)), "wins": wins, "losses": losses, "roi": _round(roi)}


def _wl_at_threshold(probs: pd.Series, y: pd.Series, threshold: float = 0.5) -> dict[str, int]:
    p = pd.to_numeric(probs, errors="coerce")
    t = pd.to_numeric(y, errors="coerce")
    mask = p.notna() & t.notna()
    if not mask.any():
        return {"wins": 0, "losses": 0}
    p = p[mask].astype(float)
    t = t[mask].astype(int)
    picks = (p >= float(threshold)).astype(int)
    wins = int((picks == t).sum())
    losses = int((picks != t).sum())
    return {"wins": wins, "losses": losses}


def _flatten_monthly(df: pd.DataFrame, over_threshold: float, edge_threshold: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if df.empty:
        return pd.DataFrame()

    for month, g in df.groupby("month"):
        for line in sorted(LINES):
            sub = g[g["line"] == float(line)].copy()
            if sub.empty:
                continue
            y = pd.to_numeric(sub["y_over"], errors="coerce")
            base = pd.to_numeric(sub["p_base"], errors="coerce")
            mkt = pd.to_numeric(sub["p_mkt"], errors="coerce")
            pr = pd.to_numeric(sub["price_over"], errors="coerce")
            m_base = _metric(base, y)
            m_mkt = _metric(mkt, y)
            wl_base = _wl_at_threshold(base, y, 0.5)
            wl_mkt = _wl_at_threshold(mkt, y, 0.5)
            roi_base_50 = _roi_over_threshold(base, pr, y, over_threshold)
            roi_mkt_50 = _roi_over_threshold(mkt, pr, y, over_threshold)
            roi_base_edge = _roi_edge_vs_market(base, mkt, pr, y, edge_threshold)

            rows.append(
                {
                    "month": month,
                    "line": float(line),
                    "rows": int(len(sub)),
                    "base_brier": m_base["brier"],
                    "mkt_brier": m_mkt["brier"],
                    "delta_brier_base_minus_mkt": _round((m_base["brier"] or 0.0) - (m_mkt["brier"] or 0.0)),
                    "base_logloss": m_base["logloss"],
                    "mkt_logloss": m_mkt["logloss"],
                    "delta_logloss_base_minus_mkt": _round((m_base["logloss"] or 0.0) - (m_mkt["logloss"] or 0.0)),
                    "base_acc50": m_base["accuracy_50"],
                    "mkt_acc50": m_mkt["accuracy_50"],
                    "base_avg_p": m_base["avg_p"],
                    "mkt_avg_p": m_mkt["avg_p"],
                    "hit_rate": m_base["hit_rate"],
                    "model_wins": wl_base["wins"],
                    "model_losses": wl_base["losses"],
                    "market_wins": wl_mkt["wins"],
                    "market_losses": wl_mkt["losses"],
                    "base_roi_over50": roi_base_50["roi"],
                    "base_bets_over50": roi_base_50["bets"],
                    "mkt_roi_over50": roi_mkt_50["roi"],
                    "mkt_bets_over50": roi_mkt_50["bets"],
                    "base_roi_edge": roi_base_edge["roi"],
                    "base_bets_edge": roi_base_edge["bets"],
                }
            )

        # Month overall, weighted across lines
        y_all = pd.to_numeric(g["y_over"], errors="coerce")
        base_all = pd.to_numeric(g["p_base"], errors="coerce")
        mkt_all = pd.to_numeric(g["p_mkt"], errors="coerce")
        pr_all = pd.to_numeric(g["price_over"], errors="coerce")
        m_base = _metric(base_all, y_all)
        m_mkt = _metric(mkt_all, y_all)
        wl_base = _wl_at_threshold(base_all, y_all, 0.5)
        wl_mkt = _wl_at_threshold(mkt_all, y_all, 0.5)
        roi_base_50 = _roi_over_threshold(base_all, pr_all, y_all, over_threshold)
        roi_mkt_50 = _roi_over_threshold(mkt_all, pr_all, y_all, over_threshold)
        roi_base_edge = _roi_edge_vs_market(base_all, mkt_all, pr_all, y_all, edge_threshold)
        rows.append(
            {
                "month": month,
                "line": "all",
                "rows": int(len(g)),
                "base_brier": m_base["brier"],
                "mkt_brier": m_mkt["brier"],
                "delta_brier_base_minus_mkt": _round((m_base["brier"] or 0.0) - (m_mkt["brier"] or 0.0)),
                "base_logloss": m_base["logloss"],
                "mkt_logloss": m_mkt["logloss"],
                "delta_logloss_base_minus_mkt": _round((m_base["logloss"] or 0.0) - (m_mkt["logloss"] or 0.0)),
                "base_acc50": m_base["accuracy_50"],
                "mkt_acc50": m_mkt["accuracy_50"],
                "base_avg_p": m_base["avg_p"],
                "mkt_avg_p": m_mkt["avg_p"],
                "hit_rate": m_base["hit_rate"],
                "model_wins": wl_base["wins"],
                "model_losses": wl_base["losses"],
                "market_wins": wl_mkt["wins"],
                "market_losses": wl_mkt["losses"],
                "base_roi_over50": roi_base_50["roi"],
                "base_bets_over50": roi_base_50["bets"],
                "mkt_roi_over50": roi_mkt_50["roi"],
                "mkt_bets_over50": roi_mkt_50["bets"],
                "base_roi_edge": roi_base_edge["roi"],
                "base_bets_edge": roi_base_edge["bets"],
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    line_order = {"1.5": 1, "2.5": 2, "3.5": 3, "all": 9}
    out["line_sort"] = out["line"].astype(str).map(line_order).fillna(99)
    out = out.sort_values(["month", "line_sort"]).drop(columns=["line_sort"]).reset_index(drop=True)
    return out


def _apply_publish_policy(df: pd.DataFrame, *, keep_line_0_5: bool = False) -> pd.DataFrame:
    """
    Mirror book-upload publishability guardrails:
      - default drop line 0.5
      - require valid endpoint-safe probabilities (0,1) for base and market
      - require fair-odds conversion sanity (not percent-like)
    """
    out = df.copy()
    if not keep_line_0_5:
        out = out[out["line"] != 0.5]

    out = out[
        pd.to_numeric(out["p_base"], errors="coerce").between(0.0, 1.0, inclusive="neither")
        & pd.to_numeric(out["p_mkt"], errors="coerce").between(0.0, 1.0, inclusive="neither")
    ].copy()

    # Same endpoint/fair-odds safety from book upload path.
    fair_over = pd.to_numeric(out["p_base"], errors="coerce").map(_prob_to_fair_american)
    out = out[fair_over.notna()].copy()
    fair_over = fair_over.loc[out.index].astype(int)
    out = out[(fair_over <= -100) | (fair_over >= 100)].copy()
    return out.reset_index(drop=True)


def _summary(monthly: pd.DataFrame, matched: pd.DataFrame, bookmaker: str, label: str) -> dict[str, Any]:
    if monthly.empty or matched.empty:
        return {"ok": True, "scope": label, "rows": 0, "months": 0, "bookmaker": bookmaker}

    overall = monthly[monthly["line"].astype(str) == "all"].copy()
    return {
        "ok": True,
        "scope": label,
        "bookmaker": bookmaker,
        "rows_matched": int(len(matched)),
        "dates_matched": int(matched["game_date"].nunique()),
        "months": int(overall["month"].nunique()),
        "month_range": {
            "min": str(overall["month"].min()) if not overall.empty else None,
            "max": str(overall["month"].max()) if not overall.empty else None,
        },
        "overall_rows": int(overall["rows"].sum()) if not overall.empty else 0,
        "base_vs_market": {
            "avg_delta_brier": _round(float(overall["delta_brier_base_minus_mkt"].mean())),
            "avg_delta_logloss": _round(float(overall["delta_logloss_base_minus_mkt"].mean())),
            "avg_base_acc50": _round(float(overall["base_acc50"].mean())),
            "avg_mkt_acc50": _round(float(overall["mkt_acc50"].mean())),
            "avg_base_roi_over50": _round(float(overall["base_roi_over50"].dropna().mean()))
            if overall["base_roi_over50"].notna().any()
            else None,
            "avg_mkt_roi_over50": _round(float(overall["mkt_roi_over50"].dropna().mean()))
            if overall["mkt_roi_over50"].notna().any()
            else None,
            "avg_base_roi_edge": _round(float(overall["base_roi_edge"].dropna().mean()))
            if overall["base_roi_edge"].notna().any()
            else None,
        },
    }


def _build_row_report(matched_all: pd.DataFrame, matched_publishable: pd.DataFrame) -> pd.DataFrame:
    """Build human-readable per-row reconciliation report."""
    df = matched_all.copy()
    if df.empty:
        return df

    key_cols = ["game_date", "player_key", "line"]
    pub_keys = matched_publishable[key_cols].drop_duplicates().copy()
    pub_keys["publishable"] = True
    df = df.merge(pub_keys, on=key_cols, how="left")
    df["publishable"] = df["publishable"].fillna(False).astype(bool)

    df["actual_sog"] = pd.to_numeric(df["shots_on_goal"], errors="coerce")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["p_base"] = pd.to_numeric(df["p_base"], errors="coerce")
    df["p_mkt"] = pd.to_numeric(df["p_mkt"], errors="coerce")
    df["price_over"] = pd.to_numeric(df["price_over"], errors="coerce")

    df["actual_result"] = np.where(df["y_over"] == 1, "over", "under")
    df["model_pick"] = np.where(df["p_base"] >= 0.5, "over", "under")
    df["market_pick"] = np.where(df["p_mkt"] >= 0.5, "over", "under")
    df["model_wl"] = np.where(df["model_pick"] == df["actual_result"], "W", "L")
    df["market_wl"] = np.where(df["market_pick"] == df["actual_result"], "W", "L")
    df["edge_base_vs_market"] = df["p_base"] - df["p_mkt"]

    # Line-target integer (>=2 for 1.5, >=3 for 2.5, >=4 for 3.5)
    df["line_target"] = df["line"].map({1.5: 2, 2.5: 3, 3.5: 4})

    cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "player_key",
        "line",
        "line_target",
        "actual_sog",
        "actual_result",
        "price_over",
        "p_mkt",
        "market_pick",
        "market_wl",
        "p_base",
        "model_pick",
        "model_wl",
        "edge_base_vs_market",
        "publishable",
    ]
    keep = [c for c in cols if c in df.columns]
    return df[keep].sort_values(["game_date", "line", "player_name"]).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Reconcile NHL SOG base predictions vs BetOnline by month.")
    ap.add_argument(
        "--dataset-csv",
        default="backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv",
    )
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--from-date", default="2025-10-07")
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--over-threshold", type=float, default=0.5, help="Over bet threshold for ROI policy.")
    ap.add_argument("--edge-threshold", type=float, default=0.0, help="Edge threshold: bet when p_base-p_mkt > x.")
    ap.add_argument(
        "--keep-line-0-5",
        action="store_true",
        help="Keep line 0.5 in publishable scope (default mirrors book upload: drop it).",
    )
    ap.add_argument("--out-csv", default="tmp/nhl_sog_base_vs_betonline_monthly.csv")
    ap.add_argument("--out-json", default="tmp/nhl_sog_base_vs_betonline_monthly.json")
    ap.add_argument(
        "--out-rows-csv",
        default="tmp/nhl_sog_base_vs_betonline_rows.csv",
        help="Per-row reconciliation output with model/book side + W/L.",
    )
    args = ap.parse_args()

    matched = _prepare_matched(
        dataset_csv=Path(args.dataset_csv),
        odds_root=Path(args.odds_root),
        bookmaker_key=str(args.bookmaker),
        from_date=args.from_date,
        to_date=args.to_date,
    )
    if matched.empty:
        raise SystemExit("No matched market+feature rows found.")

    matched_all = matched.copy()
    matched_all["month"] = matched_all["game_date"].astype(str).str.slice(0, 7)
    monthly_all = _flatten_monthly(matched_all, float(args.over_threshold), float(args.edge_threshold))

    matched_publishable = _apply_publish_policy(matched_all, keep_line_0_5=bool(args.keep_line_0_5))
    matched_publishable["month"] = matched_publishable["game_date"].astype(str).str.slice(0, 7)
    monthly_publishable = _flatten_monthly(
        matched_publishable, float(args.over_threshold), float(args.edge_threshold)
    )

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    monthly_all.to_csv(out_csv, index=False)

    publishable_csv = out_csv.with_name(out_csv.stem + "_publishable.csv")
    monthly_publishable.to_csv(publishable_csv, index=False)

    row_report = _build_row_report(matched_all, matched_publishable)
    out_rows_csv = Path(args.out_rows_csv)
    out_rows_csv.parent.mkdir(parents=True, exist_ok=True)
    row_report.to_csv(out_rows_csv, index=False)

    out = {
        "ok": True,
        "bookmaker": str(args.bookmaker),
        "all_matched": _summary(monthly_all, matched_all, str(args.bookmaker), "all_matched"),
        "publishable": _summary(monthly_publishable, matched_publishable, str(args.bookmaker), "publishable"),
    }
    out["inputs"] = {
        "dataset_csv": str(args.dataset_csv),
        "odds_root": str(args.odds_root),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "over_threshold": float(args.over_threshold),
        "edge_threshold": float(args.edge_threshold),
        "keep_line_0_5": bool(args.keep_line_0_5),
    }
    out["monthly_csv_all_matched"] = str(out_csv)
    out["monthly_rows_all_matched"] = int(len(monthly_all))
    out["monthly_csv_publishable"] = str(publishable_csv)
    out["monthly_rows_publishable"] = int(len(monthly_publishable))
    out["rows_csv"] = str(out_rows_csv)
    out["rows_csv_count"] = int(len(row_report))
    out["row_counts"] = {
        "matched_all": int(len(matched_all)),
        "matched_publishable": int(len(matched_publishable)),
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(out, indent=2))

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
