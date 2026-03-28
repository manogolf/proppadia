#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


def _is_win_loss(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin({"win", "loss"})


def _safe_float(value: float | int | np.floating | None) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return float(value)


def _build_fade_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    pick = out["model_pick_side"].astype(str).str.lower().str.strip()
    over = pick.eq("over")
    under = pick.eq("under")
    out["fade_pnl_1u"] = np.where(over, out["pnl_under_1u"], np.where(under, out["pnl_over_1u"], np.nan))
    out["fade_outcome"] = np.where(
        over,
        out["actual_under_outcome"].astype(str).str.lower(),
        np.where(under, out["actual_over_outcome"].astype(str).str.lower(), None),
    )
    out["paired_for_fade"] = out["pnl_model_pick_1u"].notna() & out["fade_pnl_1u"].notna()
    return out


def _summary_block(df: pd.DataFrame, *, min_bets_alert: int) -> dict[str, object]:
    model = df["pnl_model_pick_1u"]
    fade = df["fade_pnl_1u"]

    model_wl = df[_is_win_loss(df["actual_model_pick_outcome"])]
    fade_wl = df[_is_win_loss(df["fade_outcome"])]

    model_roi = _safe_float(model.mean()) if model.notna().any() else None
    fade_roi = _safe_float(fade.mean()) if fade.notna().any() else None
    delta = None
    if model_roi is not None and fade_roi is not None:
        delta = float(fade_roi - model_roi)

    model_bets = int(model.notna().sum())
    fade_bets = int(fade.notna().sum())
    paired_bets = int(df["paired_for_fade"].sum())

    model_win_rate = _safe_float((model_wl["actual_model_pick_outcome"].astype(str).str.lower() == "win").mean()) if len(model_wl) else None
    fade_win_rate = _safe_float((fade_wl["fade_outcome"].astype(str).str.lower() == "win").mean()) if len(fade_wl) else None

    alert = bool(paired_bets >= int(min_bets_alert) and delta is not None and delta > 0)
    return {
        "model_bets": model_bets,
        "fade_bets": fade_bets,
        "paired_bets": paired_bets,
        "model_roi_1u": model_roi,
        "fade_roi_1u": fade_roi,
        "delta_fade_minus_model_1u": delta,
        "model_win_rate": model_win_rate,
        "fade_win_rate": fade_win_rate,
        "fade_beating_model_alert": alert,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Report MLB model-pick ROI vs opposite-side fade ROI.")
    ap.add_argument("--rows-csv", required=True, help="Reconcile rows CSV with pnl_model_pick_1u/pnl_over_1u/pnl_under_1u.")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_model_vs_fade_summary.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_model_vs_fade_by_prop.csv")
    ap.add_argument("--min-bets-alert", type=int, default=30)
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    if not rows_csv.exists():
        raise FileNotFoundError(f"rows csv not found: {rows_csv}")

    out_json = Path(args.out_json).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    usecols = [
        "game_date",
        "prop_type",
        "model_pick_side",
        "actual_model_pick_outcome",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_model_pick_1u",
        "pnl_over_1u",
        "pnl_under_1u",
    ]
    try:
        df = pd.read_csv(rows_csv, usecols=usecols, low_memory=False)
    except EmptyDataError:
        payload = {
            "rows_csv": str(rows_csv),
            "window": {"game_date_min": None, "game_date_max": None},
            "counts": {
                "rows_input": 0,
                "rows_with_model_pnl": 0,
                "rows_with_fade_pnl": 0,
                "rows_paired_for_fade": 0,
            },
            "overall": {
                "model_bets": 0,
                "fade_bets": 0,
                "paired_bets": 0,
                "model_roi_1u": None,
                "fade_roi_1u": None,
                "delta_fade_minus_model_1u": None,
                "model_win_rate": None,
                "fade_win_rate": None,
                "fade_beating_model_alert": False,
                "status": "no_data",
            },
            "outputs": {
                "by_prop_csv": str(out_csv),
                "summary_json": str(out_json),
            },
        }
        pd.DataFrame().to_csv(out_csv, index=False)
        out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0
    df = _build_fade_columns(df)

    game_date = pd.to_datetime(df["game_date"], errors="coerce")
    date_min = game_date.min()
    date_max = game_date.max()

    paired = df[df["paired_for_fade"]].copy()
    overall = _summary_block(paired, min_bets_alert=int(args.min_bets_alert))

    prop_rows = []
    for prop, g in paired.groupby("prop_type", dropna=False):
        block = _summary_block(g, min_bets_alert=int(args.min_bets_alert))
        block["prop_type"] = str(prop)
        prop_rows.append(block)

    by_prop = pd.DataFrame(prop_rows)
    if not by_prop.empty:
        by_prop = by_prop.sort_values(["delta_fade_minus_model_1u", "paired_bets"], ascending=[False, False]).reset_index(drop=True)
    by_prop.to_csv(out_csv, index=False)

    payload = {
        "rows_csv": str(rows_csv),
        "window": {
            "game_date_min": None if pd.isna(date_min) else str(date_min.date()),
            "game_date_max": None if pd.isna(date_max) else str(date_max.date()),
        },
        "counts": {
            "rows_input": int(len(df)),
            "rows_with_model_pnl": int(df["pnl_model_pick_1u"].notna().sum()),
            "rows_with_fade_pnl": int(df["fade_pnl_1u"].notna().sum()),
            "rows_paired_for_fade": int(paired.shape[0]),
        },
        "overall": overall,
        "outputs": {
            "by_prop_csv": str(out_csv),
            "summary_json": str(out_json),
        },
    }

    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
