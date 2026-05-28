#!/usr/bin/env python3
"""Summarize daily hits lane selector results after outcomes exist."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _bool_win(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "win", "won"})


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _norm_name(value: Any) -> str:
    return " ".join(_clean(value).lower().split())


def _line_key(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return None
    return float(round(float(val), 3))


def _metric(df: pd.DataFrame, group: str, value: str) -> dict[str, Any]:
    bets = int(len(df))
    wins = int(df["actual_win_bool"].sum()) if bets else 0
    units = float(pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    return {
        "group": group,
        "value": value,
        "bets": bets,
        "wins": wins,
        "win_rate": float(wins / bets) if bets else None,
        "units": units,
        "roi": float(units / bets) if bets else None,
    }


def _default_input(date_value: str) -> Path:
    dated = DEFAULT_ROOT / date_value / f"hits_lane_selector_{date_value}.csv"
    if dated.exists():
        return dated
    return DEFAULT_ROOT / f"hits_lane_selector_{date_value}.csv"


def _default_out(date_value: str) -> Path:
    return DEFAULT_ROOT / date_value / f"hits_lane_selector_{date_value}_results_summary.json"


def _profit_from_price(price: Any, win: Any) -> float | None:
    if pd.isna(win):
        return None
    px = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(px):
        return None
    if not bool(win):
        return -1.0
    if px > 0:
        return float(px / 100.0)
    if px < 0:
        return float(100.0 / abs(px))
    return None


def _filter_two_sided_valid_prices(df: pd.DataFrame) -> pd.DataFrame:
    required = {"price_over_american", "price_under_american"}
    if df.empty or not required.issubset(df.columns):
        return df
    over = pd.to_numeric(df["price_over_american"], errors="coerce")
    under = pd.to_numeric(df["price_under_american"], errors="coerce")
    mask = over.notna() & under.notna() & over.abs().ge(100) & under.abs().ge(100)
    if "book_count_two_sided" in df.columns:
        mask &= pd.to_numeric(df["book_count_two_sided"], errors="coerce").fillna(0).ge(2)
    return df.loc[mask].copy()


def _attach_reconcile_results(work: pd.DataFrame, date_value: str, reconcile_root: Path) -> pd.DataFrame:
    if work["pnl"].notna().all():
        return work
    reconcile_csv = reconcile_root / date_value / "reconcile_rows.csv"
    if not reconcile_csv.exists():
        return work
    rec = pd.read_csv(reconcile_csv, low_memory=False)
    required = {"game_date", "player_name", "prop_type", "line", "actual_over_outcome", "actual_under_outcome"}
    if not required.issubset(rec.columns):
        return work
    rec = _filter_two_sided_valid_prices(rec)
    for df in (work, rec):
        date_col = "date" if df is work else "game_date"
        df["date_norm"] = df[date_col].map(_date_key)
        df["player_name_norm"] = df[("player" if df is work and "player" in df.columns else "player_name")].map(_norm_name)
        df["prop_type_norm"] = df["prop_type"].astype(str).str.strip().str.lower()
        df["line_norm"] = df["line"].map(_line_key)
    rec = rec.drop_duplicates(["date_norm", "player_name_norm", "prop_type_norm", "line_norm"], keep="first")
    merged = work.merge(
        rec[
            [
                "date_norm",
                "player_name_norm",
                "prop_type_norm",
                "line_norm",
                "price_over_american",
                "price_under_american",
                "actual_over_outcome",
                "actual_under_outcome",
                "pnl_over_1u",
                "pnl_under_1u",
            ]
        ],
        on=["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
        how="left",
    )
    side = merged["side"].astype(str).str.strip().str.lower() if "side" in merged.columns else pd.Series("", index=merged.index)
    outcome = pd.Series(
        pd.NA,
        index=merged.index,
        dtype="object",
    )
    outcome = outcome.where(~side.eq("over"), merged.get("actual_over_outcome"))
    outcome = outcome.where(~side.eq("under"), merged.get("actual_under_outcome"))
    outcome_norm = outcome.astype(str).str.strip().str.lower()
    resolved_win = outcome_norm.eq("win")
    resolved_known = outcome_norm.isin({"win", "loss"})
    existing_win_missing = merged["actual_win"].isna() | merged["actual_win"].astype(str).str.lower().isin({"", "nan", "none", "null", "<na>"})
    merged.loc[existing_win_missing & resolved_known, "actual_win"] = resolved_win[existing_win_missing & resolved_known]

    pnl_side = np.where(
        side.eq("over"),
        pd.to_numeric(merged.get("pnl_over_1u"), errors="coerce"),
        pd.to_numeric(merged.get("pnl_under_1u"), errors="coerce"),
    )
    pnl_side = pd.Series(pnl_side, index=merged.index)
    pnl_missing = pd.to_numeric(merged["pnl"], errors="coerce").isna()
    merged.loc[pnl_missing, "pnl"] = pnl_side[pnl_missing]
    still_missing = pd.to_numeric(merged["pnl"], errors="coerce").isna() & resolved_known
    if still_missing.any():
        odds = np.where(
            side.eq("over"),
            pd.to_numeric(merged.get("price_over_american"), errors="coerce"),
            pd.to_numeric(merged.get("price_under_american"), errors="coerce"),
        )
        fallback = [_profit_from_price(px, win) for px, win in zip(odds, resolved_win)]
        merged.loc[still_missing, "pnl"] = pd.Series(fallback, index=merged.index)[still_missing]
    return merged


def run(args: argparse.Namespace) -> dict[str, Any]:
    date_value = _date_key(args.date)
    input_csv = Path(args.input_csv) if args.input_csv else _default_input(date_value)
    out_json = Path(args.out_json) if args.out_json else _default_out(date_value)
    if not input_csv.exists():
        raise SystemExit(f"Missing selector CSV: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)
    required = {"source_lane"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{input_csv} missing required columns: {missing}")

    work = df.copy()
    if "pnl" not in work.columns:
        work["pnl"] = pd.NA
    if "actual_win" not in work.columns:
        work["actual_win"] = pd.NA
    work = _attach_reconcile_results(work, date_value, Path(args.reconcile_root))
    work["pnl_num"] = pd.to_numeric(work["pnl"], errors="coerce")
    work["actual_win_bool"] = _bool_win(work["actual_win"])
    resolved = work[work["pnl_num"].notna()].copy()
    missing_outcome = work[work["pnl_num"].isna()].copy()

    metrics = [_metric(resolved, "overall", "resolved")]
    for lane, group in resolved.groupby("source_lane", dropna=False):
        metrics.append(_metric(group, "by_lane", str(lane)))

    summary = {
        "date": date_value,
        "input_csv": str(input_csv),
        "reconcile_root": str(args.reconcile_root),
        "out_json": str(out_json),
        "selected_rows": int(len(work)),
        "rows_with_resolved_pnl": int(len(resolved)),
        "missing_outcome_rows": int(len(missing_outcome)),
        "missing_outcome_by_lane": {
            str(k): int(v) for k, v in missing_outcome.groupby("source_lane", dropna=False).size().to_dict().items()
        },
        "metrics": metrics,
    }
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare daily hits lane selector to resolved results.")
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD.")
    parser.add_argument("--input-csv", default="")
    parser.add_argument("--out-json", default="")
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    overall = next((m for m in summary["metrics"] if m["group"] == "overall"), {})
    print(f"Wrote {summary['out_json']}")
    print(
        "selected={selected_rows} resolved={rows_with_resolved_pnl} missing={missing_outcome_rows} "
        "win_rate={win_rate:.4f} roi={roi:.4f} units={units:.2f}".format(
            win_rate=overall.get("win_rate") or 0.0,
            roi=overall.get("roi") or 0.0,
            units=overall.get("units") or 0.0,
            **summary,
        )
    )


if __name__ == "__main__":
    main()
