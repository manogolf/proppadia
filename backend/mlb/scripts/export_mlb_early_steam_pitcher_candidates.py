#!/usr/bin/env python3
"""Export daily MLB early-steam pitcher-market candidates.

Examples:
  python -m backend.mlb.scripts.export_mlb_early_steam_pitcher_candidates \
    --rows-csv tmp/mlb_early_steam_multiday_results.csv

  python -m backend.mlb.scripts.export_mlb_early_steam_pitcher_candidates \
    --rows-csv tmp/mlb_early_steam_2026-04-30_results.csv \
    --date 2026-04-30 \
    --out-csv tmp/mlb_early_steam_pitcher_candidates_2026-04-30.csv \
    --out-summary-csv tmp/mlb_early_steam_pitcher_candidates_2026-04-30_summary.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


PITCHER_MARKETS = {"pitcher_outs", "pitcher_strikeouts"}
MARKET_ALIASES = {
    "outs_recorded": "pitcher_outs",
    "outs recorded": "pitcher_outs",
    "pitcher outs": "pitcher_outs",
    "pitching outs": "pitcher_outs",
    "strikeouts_pitching": "pitcher_strikeouts",
    "pitcher strikeouts": "pitcher_strikeouts",
}
PROP_ALIASES = {
    "pitcher_outs": "outs_recorded",
    "outs recorded": "outs_recorded",
    "pitcher outs": "outs_recorded",
    "pitching outs": "outs_recorded",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher strikeouts": "strikeouts_pitching",
}
DEFAULT_MIN_IMP_MOVE = 0.02
DEFAULT_MAX_IMP_MOVE = 0.05

CANDIDATE_BASE_COLUMNS = [
    "date",
    "game_id",
    "player_name",
    "market_key",
    "prop_type",
    "side",
    "line",
    "bookmaker_key",
    "first_price",
    "second_price",
    "imp_move_early",
]


def _norm_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _norm_lower(value: Any) -> str:
    return _norm_text(value).lower()


def _norm_market_key(value: Any) -> str:
    text = _norm_lower(value).replace("_", " ")
    return MARKET_ALIASES.get(text, MARKET_ALIASES.get(_norm_lower(value), _norm_lower(value)))


def _norm_prop_type(value: Any, market_key: Any = "") -> str:
    text = _norm_lower(value).replace("_", " ")
    if text:
        return PROP_ALIASES.get(text, PROP_ALIASES.get(_norm_lower(value), _norm_lower(value)))
    return PROP_ALIASES.get(_norm_lower(market_key), _norm_lower(market_key))


def _resolve_col(df: pd.DataFrame, names: Sequence[str]) -> str:
    columns = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in names:
        key = str(name).strip().lower()
        if key in columns:
            return columns[key]
    return ""


def _first_existing_numeric(df: pd.DataFrame, names: Iterable[str]) -> pd.Series:
    col = _resolve_col(df, list(names))
    if col:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype="float64")


def _date_series(df: pd.DataFrame) -> tuple[str, pd.Series]:
    date_col = _resolve_col(df, ["date", "game_date", "slate_date"])
    if not date_col:
        raise SystemExit("Input rows must include one of: date, game_date, slate_date")
    dates = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string")
    return date_col, dates


def _default_date(dates: pd.Series) -> str:
    valid = sorted(d for d in dates.dropna().astype(str).unique() if d and d != "<NA>")
    if not valid:
        raise SystemExit("Could not infer export date from input rows.")
    return valid[-1]


def _selected_side_price(df: pd.DataFrame, side: pd.Series) -> pd.Series:
    explicit = _first_existing_numeric(
        df,
        [
            "second_price",
            "second_odds",
            "current_price",
            "current_odds",
            "selected_american_odds",
            "price",
            "odds",
        ],
    )
    if explicit.notna().any():
        return explicit

    over_col = _resolve_col(df, ["price_over_american"])
    under_col = _resolve_col(df, ["price_under_american"])
    over = pd.to_numeric(df[over_col], errors="coerce") if over_col else pd.Series(np.nan, index=df.index)
    under = pd.to_numeric(df[under_col], errors="coerce") if under_col else pd.Series(np.nan, index=df.index)
    return pd.Series(
        np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)),
        index=df.index,
        dtype="float64",
    )


def _selected_first_price(df: pd.DataFrame) -> pd.Series:
    return _first_existing_numeric(
        df,
        [
            "first_price",
            "first_odds",
            "opening_price",
            "opening_odds",
            "initial_price",
            "initial_odds",
        ],
    )


def _source_metrics(
    df: pd.DataFrame,
    *,
    export_date: str,
) -> dict[str, float | int]:
    _, dates = _date_series(df)
    market_col = _resolve_col(df, ["market_key", "market"])
    date_mask = dates.astype(str).eq(str(export_date))
    markets = df[market_col].map(_norm_market_key) if market_col else pd.Series([""] * len(df), index=df.index)
    source_rows = int(date_mask.sum())
    pitcher_market_source_rows = int((date_mask & markets.isin(PITCHER_MARKETS)).sum())
    return {
        "source_rows": source_rows,
        "pitcher_market_source_rows": pitcher_market_source_rows,
    }


def _build_candidates(
    df: pd.DataFrame,
    *,
    export_date: str,
    min_imp_move: float,
    max_imp_move: float,
) -> pd.DataFrame:
    _, dates = _date_series(df)
    work = df.copy()
    work["date"] = dates

    market_col = _resolve_col(work, ["market_key", "market"])
    if not market_col:
        raise SystemExit("Input rows must include market_key or market.")
    side_col = _resolve_col(work, ["side", "selected_side"])
    if not side_col:
        raise SystemExit("Input rows must include side or selected_side.")
    imp_col = _resolve_col(work, ["imp_move_early"])
    if not imp_col:
        raise SystemExit("Input rows must include imp_move_early.")

    work["market_key"] = work[market_col].map(_norm_market_key)
    work["side"] = work[side_col].map(_norm_lower)
    work["imp_move_early"] = pd.to_numeric(work[imp_col], errors="coerce")
    work["line"] = pd.to_numeric(work[_resolve_col(work, ["line"])], errors="coerce") if _resolve_col(work, ["line"]) else np.nan
    work["first_price"] = _selected_first_price(work)
    work["second_price"] = _selected_side_price(work, work["side"])

    prop_col = _resolve_col(work, ["prop_type"])
    book_col = _resolve_col(work, ["bookmaker_key", "book"])
    game_col = _resolve_col(work, ["game_id"])
    player_col = _resolve_col(work, ["player_name", "player"])
    work["prop_type"] = work.apply(lambda r: _norm_prop_type(r.get(prop_col) if prop_col else "", r.get("market_key")), axis=1)
    work["bookmaker_key"] = work[book_col].map(_norm_lower) if book_col else ""
    work["game_id"] = work[game_col] if game_col else ""
    work["player_name"] = work[player_col].map(_norm_text) if player_col else ""

    mask = (
        work["date"].astype(str).eq(str(export_date))
        & work["market_key"].isin(PITCHER_MARKETS)
        & work["imp_move_early"].between(float(min_imp_move), float(max_imp_move), inclusive="both")
    )
    candidates = work.loc[mask].copy()

    output_cols = list(CANDIDATE_BASE_COLUMNS)
    for optional in ["outcome", "pnl"]:
        real = _resolve_col(candidates, [optional])
        if real:
            if real != optional:
                candidates[optional] = candidates[real]
            output_cols.append(optional)

    for col in output_cols:
        if col not in candidates.columns:
            candidates[col] = np.nan

    return candidates[output_cols].sort_values(
        ["date", "market_key", "side", "line", "bookmaker_key", "player_name"],
        na_position="last",
    )


def _build_summary(candidates: pd.DataFrame, *, source_metrics: dict[str, float | int]) -> pd.DataFrame:
    work = candidates.copy()
    has_outcome = "outcome" in work.columns
    has_pnl = "pnl" in work.columns

    if has_outcome:
        outcome = work["outcome"].map(_norm_lower)
        work["__win"] = outcome.eq("win")
        work["__loss"] = outcome.eq("loss")
    else:
        work["__win"] = False
        work["__loss"] = False
    work["__pnl"] = pd.to_numeric(work["pnl"], errors="coerce") if has_pnl else np.nan
    work["imp_move_early"] = pd.to_numeric(work["imp_move_early"], errors="coerce")

    group_cols = ["market_key", "side", "line", "bookmaker_key"]
    if work.empty:
        summary = pd.DataFrame(
            columns=[
                *group_cols,
                "bets",
                "wins",
                "resolved_losses",
                "profit",
                "avg_imp_move_early",
            ]
        )
    else:
        summary = (
            work.groupby(group_cols, dropna=False)
            .agg(
                bets=("market_key", "size"),
                wins=("__win", "sum"),
                resolved_losses=("__loss", "sum"),
                profit=("__pnl", "sum"),
                avg_imp_move_early=("imp_move_early", "mean"),
            )
            .reset_index()
        )
    resolved = summary["wins"] + summary["resolved_losses"]
    summary["win_rate"] = np.where(resolved > 0, summary["wins"] / resolved, np.nan)
    summary["roi"] = np.where(summary["bets"] > 0, summary["profit"] / summary["bets"], np.nan)
    source_rows = int(source_metrics.get("source_rows", 0) or 0)
    pitcher_source_rows = int(source_metrics.get("pitcher_market_source_rows", 0) or 0)
    candidates_count = int(len(candidates))
    summary["source_rows"] = source_rows
    summary["pitcher_market_source_rows"] = pitcher_source_rows
    summary["candidates"] = candidates_count
    summary["candidate_rate"] = candidates_count / source_rows if source_rows else np.nan
    summary["pitcher_candidate_rate"] = candidates_count / pitcher_source_rows if pitcher_source_rows else np.nan
    summary = summary.drop(columns=["resolved_losses"])
    return summary[
        [
            "source_rows",
            "pitcher_market_source_rows",
            "candidates",
            "candidate_rate",
            "pitcher_candidate_rate",
            "market_key",
            "side",
            "line",
            "bookmaker_key",
            "bets",
            "wins",
            "win_rate",
            "profit",
            "roi",
            "avg_imp_move_early",
        ]
    ].sort_values(["market_key", "side", "line", "bookmaker_key"], na_position="last")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows-csv", default="tmp/mlb_early_steam_multiday_results.csv")
    ap.add_argument("--date", default="", help="Slate/export date. Defaults to latest date in rows.")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-summary-csv", default="")
    ap.add_argument("--min-imp-move", type=float, default=DEFAULT_MIN_IMP_MOVE)
    ap.add_argument("--max-imp-move", type=float, default=DEFAULT_MAX_IMP_MOVE)
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv)
    if not rows_csv.exists():
        raise SystemExit(f"Input rows CSV not found: {rows_csv}")

    df = pd.read_csv(rows_csv)
    _, dates = _date_series(df)
    export_date = _norm_text(args.date) or _default_date(dates)

    out_csv = Path(args.out_csv or f"tmp/mlb_early_steam_pitcher_candidates_{export_date}.csv")
    out_summary_csv = Path(
        args.out_summary_csv or f"tmp/mlb_early_steam_pitcher_candidates_{export_date}_summary.csv"
    )

    candidates = _build_candidates(
        df,
        export_date=export_date,
        min_imp_move=args.min_imp_move,
        max_imp_move=args.max_imp_move,
    )
    metrics = _source_metrics(df, export_date=export_date)
    summary = _build_summary(candidates, source_metrics=metrics)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(out_csv, index=False)
    summary.to_csv(out_summary_csv, index=False)

    resolved = 0
    wins = 0
    profit = np.nan
    roi = np.nan
    if "outcome" in candidates.columns:
        outcomes = candidates["outcome"].map(_norm_lower)
        resolved = int(outcomes.isin({"win", "loss"}).sum())
        wins = int(outcomes.eq("win").sum())
    if "pnl" in candidates.columns:
        pnl = pd.to_numeric(candidates["pnl"], errors="coerce")
        profit = float(pnl.sum()) if pnl.notna().any() else np.nan
        roi = profit / len(candidates) if len(candidates) else np.nan

    first_price_note = ""
    if "first_price" in candidates.columns and candidates["first_price"].isna().all() and len(candidates):
        first_price_note = " first_price=not_available_in_source"

    print(
        "[mlb-early-steam-pitcher-candidates] "
        f"date={export_date} source_rows={len(df)} candidates={len(candidates)} "
        f"date_source_rows={metrics['source_rows']} "
        f"pitcher_market_source_rows={metrics['pitcher_market_source_rows']} "
        f"candidate_rate={(len(candidates) / metrics['source_rows']) if metrics['source_rows'] else 'NA'} "
        f"pitcher_candidate_rate={(len(candidates) / metrics['pitcher_market_source_rows']) if metrics['pitcher_market_source_rows'] else 'NA'} "
        f"summary_rows={len(summary)} resolved={resolved} wins={wins} "
        f"profit={profit if not np.isnan(profit) else 'NA'} roi={roi if not np.isnan(roi) else 'NA'} "
        f"out_csv={out_csv} out_summary_csv={out_summary_csv}{first_price_note}"
    )

    if not summary.empty:
        printable = summary.copy()
        printable["roi"] = printable["roi"].map(lambda v: "" if pd.isna(v) else f"{v:.3f}")
        printable["win_rate"] = printable["win_rate"].map(lambda v: "" if pd.isna(v) else f"{v:.3f}")
        print("[mlb-early-steam-pitcher-candidates] summary")
        print(printable.to_string(index=False))


if __name__ == "__main__":
    main()
