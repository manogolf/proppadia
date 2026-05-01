#!/usr/bin/env python3
"""Summarize MLB reconcile/result rows by market and prop type.

Examples:
  python -m backend.mlb.scripts.report_mlb_reconcile_by_market \
    --rows-csv tmp/mlb_reconcile_rows_2026-04-30_full_slate_mixedbook.csv \
    --out-csv tmp/mlb_reconcile_by_market_2026-04-30.csv

  python -m backend.mlb.scripts.report_mlb_reconcile_by_market \
    --rows-csv tmp/mlb_early_steam_multiday_results.csv \
    --include-side \
    --out-csv tmp/mlb_early_steam_by_market.csv
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Any, Iterable, List, Sequence

import numpy as np
import pandas as pd


PROP_FAMILY = {
    "hits": "contact",
    "doubles": "power_low_frequency",
    "triples": "power_low_frequency",
    "home_runs": "power_low_frequency",
    "total_bases": "power_low_frequency",
    "singles": "contact",
    "walks": "contact",
    "strikeouts_batting": "contact",
    "hits_runs_rbis": "run_production",
    "runs_rbis": "run_production",
    "runs_scored": "run_production",
    "rbis": "run_production",
    "strikeouts_pitching": "pitching",
    "outs_recorded": "pitching",
    "earned_runs": "pitching",
    "hits_allowed": "pitching",
    "walks_allowed": "pitching",
}

RESOLVED = {"win", "loss", "push"}


def _norm_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _norm_lower(value: Any) -> str:
    return _norm_text(value).lower()


def _resolve_col(df: pd.DataFrame, names: Sequence[str]) -> str:
    lower = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in names:
        key = str(name).strip().lower()
        if key in lower:
            return lower[key]
    return ""


def _expand_inputs(patterns: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for raw in patterns:
        text = _norm_text(raw)
        if not text:
            continue
        matches = sorted(glob.glob(text))
        if matches:
            paths.extend(Path(m).expanduser() for m in matches)
        else:
            paths.append(Path(text).expanduser())
    return list(dict.fromkeys(paths))


def _date_range_label(values: pd.Series) -> str:
    dates = pd.to_datetime(values, errors="coerce").dropna()
    if dates.empty:
        return ""
    lo = dates.min().date().isoformat()
    hi = dates.max().date().isoformat()
    return lo if lo == hi else f"{lo}..{hi}"


def _selected_side(df: pd.DataFrame, scope: str) -> pd.Series:
    side_col = _resolve_col(df, ["side", "selected_side"])
    if side_col:
        return df[side_col].map(_norm_lower)
    if scope == "full_slate_model_pick":
        pick_col = _resolve_col(df, ["model_pick_side"])
        if pick_col:
            return df[pick_col].map(_norm_lower)
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _selected_price(df: pd.DataFrame, side: pd.Series) -> pd.Series:
    price_col = _resolve_col(df, ["price", "odds", "bet_odds", "selected_american_odds"])
    if price_col:
        return pd.to_numeric(df[price_col], errors="coerce")
    over_col = _resolve_col(df, ["price_over_american"])
    under_col = _resolve_col(df, ["price_under_american"])
    over = pd.to_numeric(df[over_col], errors="coerce") if over_col else pd.Series(np.nan, index=df.index)
    under = pd.to_numeric(df[under_col], errors="coerce") if under_col else pd.Series(np.nan, index=df.index)
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=df.index)


def _selected_implied(df: pd.DataFrame, side: pd.Series) -> pd.Series:
    implied_col = _resolve_col(df, ["implied_probability", "implied_prob", "market_implied"])
    if implied_col:
        return pd.to_numeric(df[implied_col], errors="coerce")
    over_col = _resolve_col(df, ["implied_over", "implied_over_novig"])
    under_col = _resolve_col(df, ["implied_under", "implied_under_novig"])
    over = pd.to_numeric(df[over_col], errors="coerce") if over_col else pd.Series(np.nan, index=df.index)
    under = pd.to_numeric(df[under_col], errors="coerce") if under_col else pd.Series(np.nan, index=df.index)
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=df.index)


def _report_scope(df: pd.DataFrame) -> str:
    if _resolve_col(df, ["outcome"]) and _resolve_col(df, ["pnl"]):
        return "side_specific"
    if _resolve_col(df, ["actual_model_pick_outcome"]) and _resolve_col(df, ["pnl_model_pick_1u"]):
        return "full_slate_model_pick"
    raise SystemExit(
        "Could not determine report scope. Expected either outcome/pnl columns or "
        "actual_model_pick_outcome/pnl_model_pick_1u columns."
    )


def _prepare(df: pd.DataFrame, *, source_path: Path) -> pd.DataFrame:
    out = df.copy()
    out["source_file"] = str(source_path)
    scope = _report_scope(out)
    out["report_scope"] = scope

    if scope == "side_specific":
        outcome_col = _resolve_col(out, ["outcome"])
        pnl_col = _resolve_col(out, ["pnl"])
    else:
        outcome_col = _resolve_col(out, ["actual_model_pick_outcome"])
        pnl_col = _resolve_col(out, ["pnl_model_pick_1u"])

    out["__outcome"] = out[outcome_col].map(_norm_lower)
    out["__pnl"] = pd.to_numeric(out[pnl_col], errors="coerce")
    out["__side"] = _selected_side(out, scope)
    out["__price"] = _selected_price(out, out["__side"])
    out["__market_implied"] = _selected_implied(out, out["__side"])

    prop_col = _resolve_col(out, ["prop_type"])
    market_col = _resolve_col(out, ["market_key", "market"])
    date_col = _resolve_col(out, ["game_date", "date", "slate_date"])
    out["prop_type"] = out[prop_col].map(_norm_lower) if prop_col else ""
    out["market_key"] = out[market_col].map(_norm_lower) if market_col else out["prop_type"]
    out["prop_family"] = out["prop_type"].map(lambda p: PROP_FAMILY.get(str(p), "other"))
    out["__date"] = out[date_col] if date_col else ""

    prob_col = _resolve_col(out, ["model_pick_prob", "model_prob", "model_probability"])
    out["__model_pick_prob"] = pd.to_numeric(out[prob_col], errors="coerce") if prob_col else np.nan
    for col in ["imp_move_early", "imp_move"]:
        real = _resolve_col(out, [col])
        out[f"__{col}"] = pd.to_numeric(out[real], errors="coerce") if real else np.nan

    line_col = _resolve_col(out, ["line"])
    book_col = _resolve_col(out, ["bookmaker_key", "book"])
    out["__line"] = pd.to_numeric(out[line_col], errors="coerce") if line_col else np.nan
    out["__bookmaker_key"] = out[book_col].map(_norm_lower) if book_col else ""
    return out[out["__outcome"].isin(RESOLVED)].copy()


def build_report(
    rows: pd.DataFrame,
    *,
    include_side: bool,
    include_line: bool,
    include_book: bool,
    min_bets: int,
) -> pd.DataFrame:
    group_cols = ["report_scope", "market_key", "prop_type", "prop_family"]
    if include_side:
        rows["side"] = rows["__side"]
        group_cols.append("side")
    if include_line:
        rows["line"] = rows["__line"]
        group_cols.append("line")
    if include_book:
        rows["bookmaker_key"] = rows["__bookmaker_key"]
        group_cols.append("bookmaker_key")

    agg = (
        rows.groupby(group_cols, dropna=False)
        .agg(
            game_date=("__date", _date_range_label),
            bets=("__outcome", "size"),
            wins=("__outcome", lambda s: int((s == "win").sum())),
            losses=("__outcome", lambda s: int((s == "loss").sum())),
            pushes=("__outcome", lambda s: int((s == "push").sum())),
            profit_units=("__pnl", "sum"),
            avg_model_pick_prob=("__model_pick_prob", "mean"),
            avg_market_implied=("__market_implied", "mean"),
            avg_price=("__price", "mean"),
            avg_imp_move_early=("__imp_move_early", "mean"),
            avg_imp_move=("__imp_move", "mean"),
        )
        .reset_index()
    )
    wl = agg["wins"] + agg["losses"]
    agg["win_rate"] = np.where(wl > 0, agg["wins"] / wl, np.nan)
    agg["roi"] = np.where(agg["bets"] > 0, agg["profit_units"] / agg["bets"], np.nan)
    agg = agg[agg["bets"] >= int(min_bets)].copy()

    required_order = [
        "report_scope",
        "game_date",
        "market_key",
        "prop_type",
        "prop_family",
        "side",
        "line",
        "bookmaker_key",
        "bets",
        "wins",
        "losses",
        "win_rate",
        "profit_units",
        "roi",
        "avg_model_pick_prob",
        "avg_market_implied",
        "avg_price",
        "avg_imp_move_early",
        "avg_imp_move",
    ]
    out_cols = [c for c in required_order if c in agg.columns]
    return agg[out_cols].sort_values(["roi", "bets"], ascending=[False, False])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows-csv", action="append", required=True, help="Input reconcile/result CSV path or glob. Repeatable.")
    ap.add_argument("--out-csv", default="tmp/mlb_reconcile_by_market_report.csv")
    ap.add_argument("--date", default="", help="Optional game_date/date/slate_date filter.")
    ap.add_argument("--min-bets", type=int, default=1)
    ap.add_argument("--include-side", action="store_true")
    ap.add_argument("--include-line", action="store_true")
    ap.add_argument("--include-book", action="store_true")
    args = ap.parse_args()

    paths = _expand_inputs(args.rows_csv)
    if not paths:
        raise SystemExit("No --rows-csv inputs matched.")

    frames = []
    for path in paths:
        if not path.exists():
            raise SystemExit(f"Input not found: {path}")
        frames.append(_prepare(pd.read_csv(path), source_path=path))
    rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if args.date:
        target = pd.Timestamp(args.date).date().isoformat()
        dates = pd.to_datetime(rows["__date"], errors="coerce").dt.date.astype("string")
        rows = rows[dates == target].copy()

    report = build_report(
        rows,
        include_side=bool(args.include_side),
        include_line=bool(args.include_line),
        include_book=bool(args.include_book),
        min_bets=int(args.min_bets),
    )

    out_csv = Path(args.out_csv).expanduser()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(out_csv, index=False)

    print(f"[mlb-reconcile-by-market] inputs={len(paths)} rows={len(rows)} groups={len(report)} out_csv={out_csv}")
    printable = report[report["bets"] >= 10].copy()
    if printable.empty:
        print("[mlb-reconcile-by-market] no markets with bets >= 10")
    else:
        cols = [c for c in ["market_key", "prop_type", "side", "bets", "wins", "losses", "win_rate", "profit_units", "roi"] if c in printable.columns]
        print("[mlb-reconcile-by-market] top markets by ROI (bets >= 10)")
        print(printable.sort_values(["roi", "bets"], ascending=[False, False]).head(10)[cols].to_string(index=False))
        print("[mlb-reconcile-by-market] bottom markets by ROI (bets >= 10)")
        print(printable.sort_values(["roi", "bets"], ascending=[True, False]).head(10)[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
