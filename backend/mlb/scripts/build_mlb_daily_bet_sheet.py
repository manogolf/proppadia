#!/usr/bin/env python3
"""Build a daily MLB tool-ready bet sheet from side-matrix rows and lane history."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


def _today_et() -> str:
    return pd.Timestamp.now(tz=ET).date().isoformat()


def _bucket_ten(odds: float) -> str | None:
    if pd.isna(odds):
        return None
    o = int(round(float(odds)))
    if o >= 201:
        return ">=+201"
    if 101 <= o <= 200:
        low = ((o - 101) // 10) * 10 + 101
        return f"+{low}..+{low + 9}"
    if -99 <= o <= 100:
        return "-99..+100"
    if -299 <= o <= -100:
        abs_o = abs(o)
        low_abs = ((abs_o - 100) // 10) * 10 + 100
        return f"-{low_abs + 9}..-{low_abs}"
    return "<=-300"


def _split_csv(raw: str) -> list[str]:
    return [x.strip() for x in str(raw or "").split(",") if x.strip()]


def _pnl_and_side_for_selection(df: pd.DataFrame, selection: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    pick = df["model_pick_side"].astype(str).str.lower().str.strip()
    pnl_over = pd.to_numeric(df["pnl_over_1u"], errors="coerce")
    pnl_under = pd.to_numeric(df["pnl_under_1u"], errors="coerce")
    odds_over = pd.to_numeric(df["price_over_american"], errors="coerce")
    odds_under = pd.to_numeric(df["price_under_american"], errors="coerce")

    if selection == "model":
        side = np.where(pick.eq("over"), "over", np.where(pick.eq("under"), "under", None))
        pnl = pd.to_numeric(df["pnl_model_pick_1u"], errors="coerce")
        odds = np.where(pick.eq("over"), odds_over, np.where(pick.eq("under"), odds_under, np.nan))
        return (
            pd.Series(side, index=df.index),
            pd.Series(pnl, index=df.index),
            pd.Series(odds, index=df.index),
        )

    side = np.where(pick.eq("over"), "under", np.where(pick.eq("under"), "over", None))
    pnl = np.where(pick.eq("over"), pnl_under, np.where(pick.eq("under"), pnl_over, np.nan))
    odds = np.where(pick.eq("over"), odds_under, np.where(pick.eq("under"), odds_over, np.nan))
    return (
        pd.Series(side, index=df.index),
        pd.Series(pnl, index=df.index),
        pd.Series(odds, index=df.index),
    )


def _safe_mean(values: Iterable[float]) -> float | None:
    series = pd.to_numeric(pd.Series(list(values)), errors="coerce")
    if series.dropna().empty:
        return None
    return float(series.mean())


def main() -> int:
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("production_wager_and_staking_output")
    ap = argparse.ArgumentParser(description="Build daily tool-ready bet sheet from historical lane stats.")
    ap.add_argument("--slate-date", default=os.environ.get("MLB_DATE", "") or _today_et(), help="YYYY-MM-DD (ET)")
    ap.add_argument("--history-rows-csv", default="tmp/mlb_red_mode_rows.csv")
    ap.add_argument("--details-csv", required=True, help="Side-matrix details CSV for slate date")
    ap.add_argument("--upload-csv", required=True, help="Tool-ready side-matrix upload CSV for slate date")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--selection", choices=["model", "fade"], default="fade")
    ap.add_argument("--prop-types", default="total_bases", help="Comma-separated prop types")
    ap.add_argument("--required-side", default="over", choices=["over", "under", "any"])
    ap.add_argument("--required-pick-type", default="fade", choices=["model", "fade", "any"])
    ap.add_argument("--min-lane-rows", type=int, default=20)
    ap.add_argument("--min-lane-roi-pct", type=float, default=6.0)
    ap.add_argument("--out-upload-csv", required=True)
    ap.add_argument("--out-details-csv", required=True)
    ap.add_argument("--out-summary-json", required=True)
    ap.add_argument("--fail-if-empty", action="store_true")
    args = ap.parse_args()

    prop_types = set(_split_csv(args.prop_types))
    slate_date = str(args.slate_date).strip()
    if not slate_date:
        raise SystemExit("slate-date is required")

    history = pd.read_csv(args.history_rows_csv)
    history = history[
        history["bookmaker_key"].astype(str).str.lower().eq(str(args.bookmaker).strip().lower())
        & history["price_over_american"].notna()
        & history["price_under_american"].notna()
    ].copy()
    history = history.reset_index(drop=True)
    history["game_date"] = pd.to_datetime(history["game_date"], errors="coerce").dt.date.astype(str)
    history = history[history["game_date"] < slate_date].copy()

    selected_side, selected_pnl, selected_odds = _pnl_and_side_for_selection(history, args.selection)
    history["selected_side"] = selected_side
    history["selected_pnl_1u"] = pd.to_numeric(selected_pnl, errors="coerce")
    history["selected_odds"] = pd.to_numeric(selected_odds, errors="coerce")
    history["selected_bucket"] = history["selected_odds"].map(_bucket_ten)
    history = history[history["selected_pnl_1u"].notna()].copy()

    if prop_types:
        history = history[history["prop_type"].astype(str).isin(prop_types)].copy()
    if args.required_side != "any":
        history = history[history["selected_side"].astype(str).eq(args.required_side)].copy()

    lane = (
        history.groupby(["prop_type", "selected_side", "selected_bucket"], dropna=False)["selected_pnl_1u"]
        .agg(rows="count", roi="mean", pnl="sum")
        .reset_index()
    )
    lane["roi_pct"] = lane["roi"] * 100.0
    lane_ok = lane[
        (lane["rows"] >= int(args.min_lane_rows)) & (lane["roi_pct"] >= float(args.min_lane_roi_pct))
    ].copy()

    details = pd.read_csv(args.details_csv)
    upload = pd.read_csv(args.upload_csv)
    if len(details) != len(upload):
        raise SystemExit(
            f"row mismatch between details ({len(details)}) and upload ({len(upload)}); cannot safely index-match"
        )

    details["prop_type"] = details["prop_type"].astype(str)
    details["pick_type"] = details["pick_type"].astype(str)
    details["selected_side"] = details["selected_side"].astype(str)
    details["selected_bucket"] = details["selected_bucket"].astype(str)

    mask = details["prop_type"].isin(prop_types) if prop_types else pd.Series(True, index=details.index)
    if args.required_pick_type != "any":
        mask = mask & details["pick_type"].eq(args.required_pick_type)
    if args.required_side != "any":
        mask = mask & details["selected_side"].eq(args.required_side)
    mask = mask & details["selected_bucket"].isin(set(lane_ok["selected_bucket"].astype(str)))

    out_upload = upload[mask].copy()
    out_details = details[mask].copy()

    lane_meta = lane_ok[
        ["prop_type", "selected_side", "selected_bucket", "rows", "roi_pct"]
    ].drop_duplicates(subset=["prop_type", "selected_side", "selected_bucket"])
    out_details = out_details.merge(
        lane_meta,
        on=["prop_type", "selected_side", "selected_bucket"],
        how="left",
        validate="many_to_one",
    )
    out_details = out_details.rename(columns={"rows": "lane_rows_hist", "roi_pct": "lane_roi_hist_pct"})

    out_upload_path = Path(args.out_upload_csv)
    out_details_path = Path(args.out_details_csv)
    out_summary_path = Path(args.out_summary_json)
    out_upload_path.parent.mkdir(parents=True, exist_ok=True)
    out_details_path.parent.mkdir(parents=True, exist_ok=True)
    out_summary_path.parent.mkdir(parents=True, exist_ok=True)
    out_upload.to_csv(out_upload_path, index=False)
    out_details.to_csv(out_details_path, index=False)

    summary = {
        "ok": True,
        "slate_date": slate_date,
        "selection": args.selection,
        "bookmaker": args.bookmaker,
        "prop_types": sorted(prop_types),
        "required_side": args.required_side,
        "required_pick_type": args.required_pick_type,
        "min_lane_rows": int(args.min_lane_rows),
        "min_lane_roi_pct": float(args.min_lane_roi_pct),
        "history": {
            "rows_considered": int(len(history)),
            "game_date_min": (None if history.empty else str(history["game_date"].min())),
            "game_date_max": (None if history.empty else str(history["game_date"].max())),
            "lane_count_total": int(len(lane)),
            "lane_count_qualified": int(len(lane_ok)),
        },
        "sheet": {
            "rows": int(len(out_upload)),
            "buckets": sorted(out_details["selected_bucket"].dropna().astype(str).unique().tolist()),
            "avg_hist_lane_roi_pct": _safe_mean(out_details.get("lane_roi_hist_pct", [])),
            "avg_hist_lane_rows": _safe_mean(out_details.get("lane_rows_hist", [])),
            "avg_selected_market_odds": _safe_mean(out_details.get("selected_market_odds", [])),
        },
        "outputs": {
            "upload_csv": str(out_upload_path),
            "details_csv": str(out_details_path),
            "summary_json": str(out_summary_path),
        },
    }
    out_summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))

    if args.fail_if_empty and len(out_upload) == 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
