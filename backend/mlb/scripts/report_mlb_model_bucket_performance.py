#!/usr/bin/env python3
"""Report MLB model-pick performance by prop and diagnostic buckets.

Reads outcome-backed full-slate reconcile rows only. CSV outputs only; no DB
writes and no model logic changes.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd


RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
ACTIVE_PROPS_CSV = Path("backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv")
PROP_ROLLING_SUMMARY_CSV = Path("backend/mlb/exports/model_performance/prop_rolling_summary.csv")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_performance/bucket_performance.csv")
DEFAULT_SUMMARY_CSV = Path("backend/mlb/exports/model_performance/bucket_performance_summary.csv")

WIN_LOSS_PUSH = {"win", "loss", "push"}


def _parse_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except Exception as exc:
        raise SystemExit(f"{flag} must be YYYY-MM-DD, got {value!r}") from exc


def _sample_flag(bets: Any) -> str:
    n = int(bets or 0)
    if n >= 75:
        return "strong_sample"
    if n >= 25:
        return "usable"
    return "low_sample"


def _load_active_props(path: Path) -> list[str]:
    if not path.exists():
        return []
    df = pd.read_csv(path, usecols=lambda c: c == "prop_type")
    if "prop_type" not in df.columns:
        return []
    return sorted(
        {
            str(v).strip().lower()
            for v in df["prop_type"].dropna().tolist()
            if str(v).strip()
        }
    )


def _load_prop_status(path: Path) -> pd.DataFrame:
    cols = ["prop_type", "status", "suggested_action"]
    if not path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(path, usecols=lambda c: c in cols)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    df["prop_type"] = df["prop_type"].astype(str).str.lower().str.strip()
    return df[cols].drop_duplicates("prop_type", keep="last")


def _available_reconcile_files() -> list[Path]:
    return sorted(RECONCILE_ROOT.glob("20??-??-??/reconcile_rows.csv"))


def _resolved_row_count(path: Path) -> int:
    try:
        df = pd.read_csv(
            path,
            usecols=lambda c: c in {"actual_model_pick_outcome", "pnl_model_pick_1u"},
            low_memory=False,
        )
    except Exception:
        return 0
    outcome = df.get("actual_model_pick_outcome", pd.Series(dtype=str)).astype(str).str.lower().str.strip()
    pnl = pd.to_numeric(df.get("pnl_model_pick_1u", pd.Series(dtype=float)), errors="coerce")
    return int(outcome.isin(WIN_LOSS_PUSH).sum() if pnl.notna().any() else 0)


def _default_date_window() -> tuple[date, date]:
    usable: list[date] = []
    for path in _available_reconcile_files():
        if _resolved_row_count(path) <= 0:
            continue
        usable.append(_parse_date(path.parent.name, "date folder"))
    if not usable:
        raise SystemExit("no outcome-backed reconcile_rows.csv files found")
    selected = sorted(usable)[-14:]
    return selected[0], selected[-1]


def _paths_for_window(start: date, end: date) -> list[Path]:
    out = []
    for path in _available_reconcile_files():
        try:
            day = _parse_date(path.parent.name, "date folder")
        except SystemExit:
            continue
        if start <= day <= end:
            out.append(path)
    return out


def _selected_price(df: pd.DataFrame) -> pd.Series:
    side = df["model_pick_side"].astype(str).str.lower().str.strip()
    over = pd.to_numeric(df.get("price_over_american"), errors="coerce")
    under = pd.to_numeric(df.get("price_under_american"), errors="coerce")
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=df.index)


def _selected_implied(df: pd.DataFrame) -> pd.Series:
    side = df["model_pick_side"].astype(str).str.lower().str.strip()
    over = pd.to_numeric(df.get("implied_over_novig"), errors="coerce").fillna(
        pd.to_numeric(df.get("implied_over"), errors="coerce")
    )
    under = pd.to_numeric(df.get("implied_under_novig"), errors="coerce").fillna(
        pd.to_numeric(df.get("implied_under"), errors="coerce")
    )
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=df.index)


def _bucket_price(price: Any) -> str:
    p = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(p):
        return "unknown"
    if p <= -200:
        return "<= -200"
    if p <= -150:
        return "-200 to -150"
    if p <= -110:
        return "-150 to -110"
    if p <= 100:
        return "-110 to +100"
    if p <= 150:
        return "+100 to +150"
    return "+150+"


def _bucket_prob(prob: Any) -> str:
    p = pd.to_numeric(pd.Series([prob]), errors="coerce").iloc[0]
    if pd.isna(p):
        return "unknown"
    if p < 0.50:
        return "<0.50"
    if p < 0.55:
        return "0.50-0.55"
    if p < 0.60:
        return "0.55-0.60"
    if p < 0.65:
        return "0.60-0.65"
    return "0.65+"


def _bucket_edge(edge: Any) -> str:
    e = pd.to_numeric(pd.Series([edge]), errors="coerce").iloc[0]
    if pd.isna(e):
        return "unknown"
    if e < 0:
        return "<0"
    if e < 0.02:
        return "0-0.02"
    if e < 0.05:
        return "0.02-0.05"
    if e < 0.10:
        return "0.05-0.10"
    return "0.10+"


def _bucket_line(line: Any) -> str:
    n = pd.to_numeric(pd.Series([line]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "unknown"
    if float(n).is_integer():
        return f"line={int(n)}"
    return f"line={float(n):g}"


def _load_rows(paths: list[Path], active_props: list[str]) -> pd.DataFrame:
    usecols = {
        "game_date",
        "prop_type",
        "line",
        "model_pick_side",
        "model_pick_prob",
        "actual_model_pick_outcome",
        "pnl_model_pick_1u",
        "price_over_american",
        "price_under_american",
        "implied_over",
        "implied_under",
        "implied_over_novig",
        "implied_under_novig",
    }
    parts = []
    for path in paths:
        try:
            df = pd.read_csv(path, usecols=lambda c: c in usecols, low_memory=False)
        except Exception as exc:
            print(f"[mlb-bucket-performance] skip path={path} reason=read_error:{type(exc).__name__}")
            continue
        if df.empty:
            continue
        df["source_file"] = str(path)
        parts.append(df)
    if not parts:
        return pd.DataFrame()

    rows = pd.concat(parts, ignore_index=True)
    rows["date"] = pd.to_datetime(rows["game_date"], errors="coerce").dt.date
    rows["prop_type"] = rows["prop_type"].astype(str).str.lower().str.strip()
    if active_props:
        rows = rows[rows["prop_type"].isin(active_props)].copy()
    rows["model_pick_side"] = rows["model_pick_side"].astype(str).str.lower().str.strip()
    rows["actual_model_pick_outcome"] = rows["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
    rows["pnl_model_pick_1u"] = pd.to_numeric(rows["pnl_model_pick_1u"], errors="coerce")
    rows = rows[
        rows["date"].notna()
        & rows["actual_model_pick_outcome"].isin(WIN_LOSS_PUSH)
        & rows["pnl_model_pick_1u"].notna()
    ].copy()
    if rows.empty:
        return rows

    rows["selected_price"] = _selected_price(rows)
    rows["model_pick_prob"] = pd.to_numeric(rows.get("model_pick_prob"), errors="coerce")
    rows["selected_implied_probability"] = _selected_implied(rows)
    rows["edge"] = rows["model_pick_prob"] - rows["selected_implied_probability"]
    rows["price_bucket"] = rows["selected_price"].map(_bucket_price)
    rows["model_prob_bucket"] = rows["model_pick_prob"].map(_bucket_prob)
    rows["implied_edge_bucket"] = rows["edge"].map(_bucket_edge)
    rows["line"] = pd.to_numeric(rows["line"], errors="coerce")
    rows["line_bucket"] = rows["line"].map(_bucket_line)
    rows["wins"] = rows["actual_model_pick_outcome"].eq("win").astype(int)
    rows["losses"] = rows["actual_model_pick_outcome"].eq("loss").astype(int)
    return rows


def _summarize_group(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    wins = int(group["wins"].sum())
    losses = int(group["losses"].sum())
    decided = wins + losses
    profit = float(pd.to_numeric(group["pnl_model_pick_1u"], errors="coerce").sum())
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "win_rate": float(wins / decided) if decided else np.nan,
        "profit_units": profit,
        "roi": float(profit / bets) if bets else np.nan,
        "avg_price": float(pd.to_numeric(group["selected_price"], errors="coerce").mean()),
        "avg_model_prob": float(pd.to_numeric(group["model_pick_prob"], errors="coerce").mean()),
        "avg_edge": float(pd.to_numeric(group["edge"], errors="coerce").mean()),
        "sample_size_flag": _sample_flag(bets),
    }


def _bucket_rows(rows: pd.DataFrame, prop_status: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    dimensions = [
        ("prop_type", ["prop_type"]),
        ("model_pick_side", ["prop_type", "model_pick_side"]),
        ("price_bucket", ["prop_type", "price_bucket"]),
        ("model_prob_bucket", ["prop_type", "model_prob_bucket"]),
        ("line_bucket", ["prop_type", "line_bucket"]),
        ("implied_edge_bucket", ["prop_type", "implied_edge_bucket"]),
    ]
    out_rows: list[dict[str, Any]] = []
    for dimension, group_cols in dimensions:
        for keys, group in rows.groupby(group_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            record: dict[str, Any] = {
                "from_date": start.isoformat(),
                "to_date": end.isoformat(),
                "source_type": "full_slate_model_pick",
                "dimension": dimension,
                "prop_type": str(keys[0]),
                "bucket": "all" if len(keys) == 1 else str(keys[1]),
            }
            record.update(_summarize_group(group))
            out_rows.append(record)
    out = pd.DataFrame(out_rows)
    if not prop_status.empty and not out.empty:
        out = out.merge(
            prop_status.rename(columns={"status": "prop_status", "suggested_action": "prop_suggested_action"}),
            on="prop_type",
            how="left",
        )
    return out


def _build_summary(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame()
    rows = []
    usable = detail[detail["bets"].ge(25)].copy()
    overall_by_prop = (
        detail[detail["dimension"].eq("prop_type") & detail["bucket"].eq("all")]
        .set_index("prop_type")["roi"]
        .to_dict()
    )
    for (prop, dimension), group in detail.groupby(["prop_type", "dimension"], dropna=False):
        use = usable[(usable["prop_type"].eq(prop)) & (usable["dimension"].eq(dimension))].copy()
        base_roi = overall_by_prop.get(prop, np.nan)
        if use.empty:
            best = group.sort_values(["bets", "roi"], ascending=[False, False]).head(1)
            worst = best
        else:
            best = use.sort_values(["roi", "bets"], ascending=[False, False]).head(1)
            worst = use.sort_values(["roi", "bets"], ascending=[True, False]).head(1)
        best_row = best.iloc[0]
        worst_row = worst.iloc[0]
        rows.append(
            {
                "prop_type": prop,
                "source_type": "full_slate_model_pick",
                "dimension": dimension,
                "prop_status": best_row.get("prop_status"),
                "prop_suggested_action": best_row.get("prop_suggested_action"),
                "total_bets_in_dimension": int(group["bets"].sum()),
                "usable_bucket_count": int(group["bets"].ge(25).sum()),
                "strong_sample_bucket_count": int(group["bets"].ge(75).sum()),
                "overall_roi": base_roi,
                "best_bucket": best_row["bucket"],
                "best_bucket_bets": int(best_row["bets"]),
                "best_bucket_roi": best_row["roi"],
                "best_bucket_win_rate": best_row["win_rate"],
                "worst_bucket": worst_row["bucket"],
                "worst_bucket_bets": int(worst_row["bets"]),
                "worst_bucket_roi": worst_row["roi"],
                "worst_bucket_win_rate": worst_row["win_rate"],
                "roi_spread_best_minus_worst": best_row["roi"] - worst_row["roi"],
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(["prop_status", "prop_type", "dimension"], na_position="last").reset_index(drop=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Report MLB model-pick bucket performance by prop.")
    ap.add_argument("--from-date")
    ap.add_argument("--to-date")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--summary-csv", default=str(DEFAULT_SUMMARY_CSV))
    ap.add_argument("--active-props-csv", default=str(ACTIVE_PROPS_CSV))
    ap.add_argument("--prop-rolling-summary-csv", default=str(PROP_ROLLING_SUMMARY_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            raise SystemExit("--from-date and --to-date must be provided together")
        start = _parse_date(args.from_date, "--from-date")
        end = _parse_date(args.to_date, "--to-date")
    else:
        start, end = _default_date_window()
    if end < start:
        raise SystemExit("--to-date is before --from-date")

    active_props = _load_active_props(Path(args.active_props_csv))
    prop_status = _load_prop_status(Path(args.prop_rolling_summary_csv))
    paths = _paths_for_window(start, end)
    rows = _load_rows(paths, active_props)
    if rows.empty:
        raise SystemExit(f"no resolved full-slate model-pick rows found for {start} to {end}")

    detail = _bucket_rows(rows, prop_status, start, end)
    summary = _build_summary(detail)

    out_csv = Path(args.out_csv)
    summary_csv = Path(args.summary_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    detail.to_csv(out_csv, index=False)
    summary.to_csv(summary_csv, index=False)

    print(
        f"[mlb-bucket-performance] source_type=full_slate_model_pick "
        f"from_date={start} to_date={end} files={len(paths)} rows={len(rows)} "
        f"detail_rows={len(detail)} summary_rows={len(summary)}"
    )
    print(f"[mlb-bucket-performance] out_csv={out_csv}")
    print(f"[mlb-bucket-performance] summary_csv={summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
