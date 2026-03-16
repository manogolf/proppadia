#!/usr/bin/env python3
"""Backtest NHL SOG selection using primary-market two-sided executable prices.

This script enforces:
  - primary market only (default: player_shots_on_goal)
  - explicit two-sided quotes (both over and under present)
  - no synthetic under prices
  - executed PnL from exact side price with optional slippage scenarios

Selection policy is read from walk-forward threshold history:
  select if ev_side >= min_ev AND edge_side >= min_gap for (date, segment)
"""

from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _to_bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y"}


def _norm_line(v: Any) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(x):
        return str(v)
    return f"{float(x):.1f}"


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


def _is_reasonable_american_price(a: Any) -> bool:
    try:
        v = float(a)
    except Exception:
        return False
    if not math.isfinite(v) or v == 0:
        return False
    return abs(v) >= 100


def _load_rows(path: Path, from_date: str, to_date: str) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    df["publishable"] = df["publishable"].map(_to_bool)
    df = df[df["publishable"]].copy()

    df["game_date"] = df["game_date"].astype(str)
    if from_date:
        df = df[df["game_date"] >= str(from_date)].copy()
    if to_date:
        df = df[df["game_date"] <= str(to_date)].copy()

    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["p_base"] = pd.to_numeric(df["p_base"], errors="coerce")
    df["p_mkt"] = pd.to_numeric(df["p_mkt"], errors="coerce")
    df["model_pick"] = df["model_pick"].astype(str).str.lower().str.strip()
    df["model_wl"] = df["model_wl"].astype(str).str.upper().str.strip()
    df = df[df["model_pick"].isin(["over", "under"]) & df["model_wl"].isin(["W", "L"])].copy()

    df["is_win"] = (df["model_wl"] == "W").astype(int)
    df["line_key"] = df["line"].map(_norm_line)
    df["segment"] = df["model_pick"] + ":" + df["line_key"]

    over_mask = df["model_pick"] == "over"
    df["model_side_prob"] = np.where(over_mask, df["p_base"], 1.0 - df["p_base"])
    df["market_side_prob"] = np.where(over_mask, df["p_mkt"], 1.0 - df["p_mkt"])
    df = df[
        df["model_side_prob"].between(0.0, 1.0, inclusive="neither")
        & df["market_side_prob"].between(0.0, 1.0, inclusive="neither")
    ].copy()

    df["ev_side"] = (df["model_side_prob"] / df["market_side_prob"]) - 1.0
    df["edge_side"] = df["model_side_prob"] - df["market_side_prob"]
    return df.reset_index(drop=True)


def _load_threshold_map(path: Path) -> dict[tuple[str, str], tuple[float, float]]:
    df = pd.read_csv(path).copy()
    df["reopt_date"] = df["reopt_date"].astype(str)
    df["segment"] = df["segment"].astype(str)
    return {
        (row.reopt_date, row.segment): (float(row.min_ev), float(row.min_gap))
        for row in df.itertuples(index=False)
    }


def _load_primary_two_sided_prices(
    odds_root: Path,
    dates: list[str],
    bookmaker: str,
    market_key: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in dates:
        fp_compat = odds_root / day / "odds_latest_compatible.json"
        fp_latest = odds_root / day / "odds_latest.json"
        fp = fp_compat if fp_compat.exists() else fp_latest
        if not fp.exists():
            continue
        try:
            events = json.loads(fp.read_text())
        except Exception:
            continue
        if not isinstance(events, list):
            continue

        for event in events:
            books = event.get("bookmakers", [])
            if not isinstance(books, list):
                continue
            for book in books:
                if str(book.get("key", "")).strip() != bookmaker:
                    continue
                markets = book.get("markets", [])
                if not isinstance(markets, list):
                    continue
                for market in markets:
                    if str(market.get("key", "")).strip() != market_key:
                        continue
                    outcomes = market.get("outcomes", [])
                    if not isinstance(outcomes, list):
                        continue
                    for outcome in outcomes:
                        if not isinstance(outcome, dict):
                            continue
                        side = _outcome_side(outcome)
                        if side not in {"over", "under"}:
                            continue
                        name = _pick_market_player_name(outcome)
                        if not name:
                            continue
                        key = _short_key(_norm_name(name))
                        if not key:
                            continue
                        line = pd.to_numeric(outcome.get("point"), errors="coerce")
                        price = pd.to_numeric(outcome.get("price"), errors="coerce")
                        if pd.isna(line) or pd.isna(price):
                            continue
                        if not _is_reasonable_american_price(price):
                            continue
                        rows.append(
                            {
                                "game_date": day,
                                "player_key": key,
                                "line": float(line),
                                "side": side,
                                "price": float(price),
                            }
                        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "game_date",
                "player_key",
                "line",
                "price_over",
                "price_under",
                "price_side",
                "model_pick",
            ]
        )

    raw = pd.DataFrame(rows)
    med = (
        raw.groupby(["game_date", "player_key", "line", "side"], as_index=False)
        .agg(price=("price", "median"))
        .copy()
    )
    pivot = (
        med.pivot_table(
            index=["game_date", "player_key", "line"],
            columns="side",
            values="price",
            aggfunc="median",
        )
        .reset_index()
        .rename(columns={"over": "price_over", "under": "price_under"})
    )
    for need in ("price_over", "price_under"):
        if need not in pivot.columns:
            pivot[need] = pd.NA

    pivot = pivot[pivot["price_over"].notna() & pivot["price_under"].notna()].copy()
    if pivot.empty:
        return pd.DataFrame(
            columns=[
                "game_date",
                "player_key",
                "line",
                "price_over",
                "price_under",
                "price_side",
                "model_pick",
            ]
        )

    over_rows = pivot.copy()
    over_rows["model_pick"] = "over"
    over_rows["price_side"] = pd.to_numeric(over_rows["price_over"], errors="coerce")

    under_rows = pivot.copy()
    under_rows["model_pick"] = "under"
    under_rows["price_side"] = pd.to_numeric(under_rows["price_under"], errors="coerce")

    out = pd.concat([over_rows, under_rows], ignore_index=True)
    return out.reset_index(drop=True)


def _apply_slippage(price: float, slippage_cents: float) -> float:
    p = float(price)
    s = float(max(0.0, slippage_cents))
    if p > 0:
        return max(100.0, p - s)
    return p - s


def _profit_units(american_price: float, is_win: int) -> float:
    p = float(american_price)
    if p > 0:
        win_profit = p / 100.0
    else:
        win_profit = 100.0 / abs(p)
    return win_profit if int(is_win) == 1 else -1.0


def _summarize_strategy(df: pd.DataFrame, slippage_cents: float) -> dict[str, Any]:
    if df.empty:
        return {
            "bets": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "roi": None,
            "profit_units": None,
        }
    adj = pd.to_numeric(df["price_side"], errors="coerce").map(lambda p: _apply_slippage(float(p), slippage_cents))
    y = pd.to_numeric(df["is_win"], errors="coerce").astype(int)
    pnl = np.array([_profit_units(px, yi) for px, yi in zip(adj.values, y.values)], dtype=float)
    return {
        "bets": int(len(df)),
        "wins": int(y.sum()),
        "losses": int(len(df) - y.sum()),
        "win_rate": float(y.mean()),
        "roi": float(pnl.mean()),
        "profit_units": float(pnl.sum()),
        "avg_price": float(adj.mean()),
    }


def _split_slippage_grid(s: str) -> list[float]:
    out: list[float] = []
    for tok in str(s).split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(float(tok))
    if not out:
        return [0.0]
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest primary two-sided executable NHL SOG strategy.")
    ap.add_argument("--rows-csv", default="tmp/nhl_sog_base_vs_betonline_rows.csv")
    ap.add_argument("--threshold-history-csv", default="tmp/nhl_sog_walkforward_threshold_history.csv")
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--market-key", default="player_shots_on_goal")
    ap.add_argument("--warmup-days", type=int, default=30)
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--slippage-cents-grid", default="0,5,10,20")
    ap.add_argument("--out-selected-csv", default="tmp/nhl_sog_primary_two_sided_selected.csv")
    ap.add_argument("--out-summary-json", default="tmp/nhl_sog_primary_two_sided_summary.json")
    args = ap.parse_args()

    rows = _load_rows(Path(args.rows_csv), from_date=str(args.from_date), to_date=str(args.to_date))
    if rows.empty:
        raise SystemExit("No rows after row-level filtering.")

    dates = sorted(rows["game_date"].dropna().astype(str).unique().tolist())
    if len(dates) <= int(args.warmup_days):
        raise SystemExit(f"Not enough dates for warmup={args.warmup_days}; have={len(dates)}")
    oot_dates = dates[int(args.warmup_days) :]
    rows = rows[rows["game_date"].isin(oot_dates)].copy()

    thr_map = _load_threshold_map(Path(args.threshold_history_csv))
    rows["min_ev"] = [thr_map.get((d, s), (np.nan, np.nan))[0] for d, s in zip(rows["game_date"], rows["segment"])]
    rows["min_gap"] = [thr_map.get((d, s), (np.nan, np.nan))[1] for d, s in zip(rows["game_date"], rows["segment"])]
    rows = rows.dropna(subset=["min_ev", "min_gap"]).copy()
    rows["selected"] = (rows["ev_side"] >= rows["min_ev"]) & (rows["edge_side"] >= rows["min_gap"])

    prices = _load_primary_two_sided_prices(
        odds_root=Path(args.odds_root),
        dates=oot_dates,
        bookmaker=str(args.bookmaker),
        market_key=str(args.market_key),
    )
    if prices.empty:
        raise SystemExit("No two-sided primary prices found in odds history for requested dates.")

    merged = rows.merge(
        prices,
        on=["game_date", "player_key", "line", "model_pick"],
        how="left",
    )
    merged["has_exec_price"] = merged["price_side"].notna()
    selected_exec = merged[merged["selected"] & merged["has_exec_price"]].copy()

    summary: dict[str, Any] = {
        "config": {
            "rows_csv": str(args.rows_csv),
            "threshold_history_csv": str(args.threshold_history_csv),
            "odds_root": str(args.odds_root),
            "bookmaker": str(args.bookmaker),
            "market_key": str(args.market_key),
            "warmup_days": int(args.warmup_days),
            "from_date": (str(args.from_date) or None),
            "to_date": (str(args.to_date) or None),
            "slippage_cents_grid": _split_slippage_grid(args.slippage_cents_grid),
        },
        "coverage": {
            "distinct_dates_total_after_filters": int(len(dates)),
            "distinct_dates_oot": int(len(oot_dates)),
            "oot_start": oot_dates[0],
            "oot_end": oot_dates[-1],
            "rows_oot_total": int(len(rows)),
            "rows_oot_selected_by_policy": int(rows["selected"].sum()),
            "rows_oot_selected_with_exec_price": int(len(selected_exec)),
            "selected_exec_match_rate": (
                float(len(selected_exec) / max(1, int(rows["selected"].sum())))
            ),
            "exec_price_available_rows_any_side": int(merged["has_exec_price"].sum()),
        },
        "scenarios": {},
    }

    # Strategy summaries by slippage.
    for slip in _split_slippage_grid(args.slippage_cents_grid):
        summary["scenarios"][f"slippage_{int(slip)}c"] = _summarize_strategy(selected_exec, slippage_cents=slip)

    # Slice diagnostics.
    by_side: dict[str, Any] = {}
    for side, sub in selected_exec.groupby("model_pick", dropna=False):
        by_side[str(side)] = _summarize_strategy(sub, slippage_cents=0.0)
    summary["by_side_slippage_0c"] = by_side

    by_line: dict[str, Any] = {}
    for line, sub in selected_exec.groupby("line_key", dropna=False):
        by_line[str(line)] = _summarize_strategy(sub, slippage_cents=0.0)
    summary["by_line_slippage_0c"] = by_line

    by_month: dict[str, Any] = {}
    if not selected_exec.empty:
        tmp = selected_exec.copy()
        tmp["month"] = tmp["game_date"].astype(str).str.slice(0, 7)
        for month, sub in tmp.groupby("month", dropna=False):
            by_month[str(month)] = _summarize_strategy(sub, slippage_cents=0.0)
    summary["by_month_slippage_0c"] = by_month

    out_sel = Path(args.out_selected_csv)
    out_sel.parent.mkdir(parents=True, exist_ok=True)
    keep_cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "player_key",
        "line",
        "line_key",
        "model_pick",
        "model_wl",
        "is_win",
        "p_base",
        "p_mkt",
        "model_side_prob",
        "market_side_prob",
        "ev_side",
        "edge_side",
        "min_ev",
        "min_gap",
        "price_over",
        "price_under",
        "price_side",
    ]
    keep = [c for c in keep_cols if c in selected_exec.columns]
    selected_exec[keep].sort_values(["game_date", "game_id", "player_id", "line", "model_pick"]).to_csv(out_sel, index=False)

    out_sum = Path(args.out_summary_json)
    out_sum.parent.mkdir(parents=True, exist_ok=True)
    out_sum.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

