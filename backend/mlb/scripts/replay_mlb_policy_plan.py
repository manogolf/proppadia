#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from backend.mlb.shared.policy_plan import load_policy_plan, score_policy_plan_rows


def _parse_list(raw: str) -> list[str]:
    return [str(x).strip().lower() for x in str(raw or "").split(",") if str(x).strip()]


def _add_slices(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    out = out[out["game_date"].notna()].copy()
    out["season"] = out["game_date"].dt.year.astype(int)
    out = out[out["season"].isin([2024, 2025])].copy()

    date_map: dict[tuple[int, object], str] = {}
    for season, g in out.groupby("season"):
        dates = sorted(pd.Series(g["game_date"].dt.date).dropna().unique())
        for label, arr in zip(["early", "mid", "late"], np.array_split(np.array(dates, dtype=object), 3)):
            for d in arr.tolist():
                date_map[(int(season), d)] = label

    out["slice"] = [date_map.get((int(s), d)) for s, d in zip(out["season"], out["game_date"].dt.date)]
    out = out[out["slice"].notna()].copy()
    out["slice_id"] = out["season"].astype(str) + "_" + out["slice"].astype(str)
    out["game_month"] = out["game_date"].dt.to_period("M").astype(str)
    return out


def _summarize(df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            [
                {
                    "scenario": scenario,
                    "bets": 0,
                    "roi_1u": np.nan,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": np.nan,
                    "avg_side_price": np.nan,
                }
            ]
        )
    wl = df[df["side_outcome"].isin(["win", "loss"])].copy()
    wins = int((wl["side_outcome"] == "win").sum())
    losses = int((wl["side_outcome"] == "loss").sum())
    win_rate = float(wins / (wins + losses)) if (wins + losses) else np.nan
    return pd.DataFrame(
        [
            {
                "scenario": scenario,
                "bets": int(df["side_pnl_1u"].notna().sum()),
                "roi_1u": float(np.nanmean(df["side_pnl_1u"])),
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "avg_side_price": float(np.nanmean(df["side_price_american"])),
            }
        ]
    )


def _rollup_slice(df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["slice_id", "bets", "roi_1u", "win_rate", "scenario"])

    rows = []
    for sid, g in df.groupby("slice_id", sort=True):
        wl = g[g["side_outcome"].isin(["win", "loss"])]
        win_rate = float((wl["side_outcome"] == "win").mean()) if len(wl) else np.nan
        rows.append(
            {
                "slice_id": str(sid),
                "bets": int(g["side_pnl_1u"].notna().sum()),
                "roi_1u": float(np.nanmean(g["side_pnl_1u"])),
                "win_rate": win_rate,
                "scenario": scenario,
            }
        )
    return pd.DataFrame(rows).sort_values("slice_id").reset_index(drop=True)


def _rollup_prop_slice(df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["prop_type", "slice_id", "bets", "roi_1u", "scenario"])
    rows = []
    for (prop, sid), g in df.groupby(["prop_type", "slice_id"], sort=True):
        rows.append(
            {
                "prop_type": str(prop),
                "slice_id": str(sid),
                "bets": int(g["side_pnl_1u"].notna().sum()),
                "roi_1u": float(np.nanmean(g["side_pnl_1u"])),
                "scenario": scenario,
            }
        )
    return pd.DataFrame(rows).sort_values(["prop_type", "slice_id"]).reset_index(drop=True)


def _rollup_month(df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["game_month", "bets", "roi_1u", "scenario"])
    rows = []
    for month, g in df.groupby("game_month", sort=True):
        rows.append(
            {
                "game_month": str(month),
                "bets": int(g["side_pnl_1u"].notna().sum()),
                "roi_1u": float(np.nanmean(g["side_pnl_1u"])),
                "scenario": scenario,
            }
        )
    return pd.DataFrame(rows).sort_values("game_month").reset_index(drop=True)


def _monitor_fragile_lanes(
    df: pd.DataFrame,
    *,
    props: Iterable[str],
    min_bets_alert: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    monitor_props = [str(p).strip().lower() for p in props if str(p).strip()]
    cols = [
        "prop_type",
        "bets_all",
        "roi_all",
        "bets_30d",
        "roi_30d",
        "bets_14d",
        "roi_14d",
        "bets_7d",
        "roi_7d",
        "min_slice_roi",
        "status",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols), {"alerts": [], "monitor_props": monitor_props}

    max_date = pd.to_datetime(df["game_date"], errors="coerce").max()
    rows = []
    alerts: list[dict[str, object]] = []
    for prop in monitor_props:
        g = df[df["prop_type"].astype(str).str.lower() == prop].copy()
        if g.empty:
            rows.append(
                {
                    "prop_type": prop,
                    "bets_all": 0,
                    "roi_all": np.nan,
                    "bets_30d": 0,
                    "roi_30d": np.nan,
                    "bets_14d": 0,
                    "roi_14d": np.nan,
                    "bets_7d": 0,
                    "roi_7d": np.nan,
                    "min_slice_roi": np.nan,
                    "status": "no_data",
                }
            )
            continue

        def _window(days: int) -> tuple[int, float]:
            start = max_date - timedelta(days=days)
            w = g[pd.to_datetime(g["game_date"]) >= start]
            return int(w["side_pnl_1u"].notna().sum()), float(np.nanmean(w["side_pnl_1u"])) if len(w) else np.nan

        bets_30d, roi_30d = _window(30)
        bets_14d, roi_14d = _window(14)
        bets_7d, roi_7d = _window(7)
        slice_min = float(g.groupby("slice_id")["side_pnl_1u"].mean().min()) if "slice_id" in g.columns else np.nan
        bets_all = int(g["side_pnl_1u"].notna().sum())
        roi_all = float(np.nanmean(g["side_pnl_1u"]))

        status = "ok"
        if bets_all >= int(min_bets_alert) and roi_all < 0:
            status = "alert_negative_roi"
            alerts.append(
                {
                    "prop_type": prop,
                    "bets_all": bets_all,
                    "roi_all": roi_all,
                    "min_slice_roi": slice_min,
                }
            )

        rows.append(
            {
                "prop_type": prop,
                "bets_all": bets_all,
                "roi_all": roi_all,
                "bets_30d": bets_30d,
                "roi_30d": roi_30d,
                "bets_14d": bets_14d,
                "roi_14d": roi_14d,
                "bets_7d": bets_7d,
                "roi_7d": roi_7d,
                "min_slice_roi": slice_min,
                "status": status,
            }
        )

    return pd.DataFrame(rows, columns=cols), {"alerts": alerts, "monitor_props": monitor_props}


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay MLB policy-plan selection on reconcile rows.")
    ap.add_argument(
        "--rows-csv",
        default="tmp/mlb_reconcile_rows_2024_2025_prod11_allbooks_noncollapsed.csv",
        help="Reconcile rows CSV (all-books non-collapsed recommended).",
    )
    ap.add_argument(
        "--policy-plan-csv",
        default="backend/mlb/config/policy/all11_forward_plan_pass4.csv",
        help="Per-prop policy plan CSV.",
    )
    ap.add_argument(
        "--out-dir",
        default="tmp/analysis/mlb_baseline_readiness_pack/pass4_execution_replay",
        help="Output directory for replay artifacts.",
    )
    ap.add_argument(
        "--allow-one-sided",
        action="store_true",
        help="Allow one-sided prices in policy scoring (default requires two-sided).",
    )
    ap.add_argument(
        "--monitor-props",
        default="doubles,walks_allowed",
        help="Comma-separated props for fragile-lane monitor outputs.",
    )
    ap.add_argument("--monitor-min-bets-alert", type=int, default=30)
    ap.add_argument(
        "--compare-summary-csv",
        default="tmp/analysis/mlb_baseline_readiness_pack/all11_forward_replay_summary.csv",
        help="Optional reference summary CSV for parity delta.",
    )
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    if not rows_csv.exists():
        raise FileNotFoundError(f"rows csv not found: {rows_csv}")
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    usecols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "bookmaker_key",
        "price_over_american",
        "price_under_american",
        "implied_over_novig",
        "implied_under_novig",
        "model_prob_over",
        "model_prob_under",
        "pnl_over_1u",
        "pnl_under_1u",
        "actual_over_outcome",
        "actual_under_outcome",
    ]
    rows = pd.read_csv(rows_csv, usecols=usecols, low_memory=False)
    plan = load_policy_plan(args.policy_plan_csv, include_actions=("enable",))
    scored = score_policy_plan_rows(
        rows,
        plan,
        require_two_sided=not bool(args.allow_one_sided),
    )
    selected = scored[scored["pass_policy"]].copy() if not scored.empty else scored

    if not selected.empty:
        side_over = selected["plan_side"].eq("over")
        selected["side_pnl_1u"] = np.where(side_over, selected["pnl_over_1u"], selected["pnl_under_1u"])
        selected["side_outcome"] = np.where(
            side_over,
            selected["actual_over_outcome"].astype(str),
            selected["actual_under_outcome"].astype(str),
        )
    else:
        selected["side_pnl_1u"] = np.nan
        selected["side_outcome"] = np.nan

    selected = _add_slices(selected)
    summary_df = _summarize(selected, scenario="pass4_baseline")
    slice_df = _rollup_slice(selected, scenario="pass4_baseline")
    prop_slice_df = _rollup_prop_slice(selected, scenario="pass4_baseline")
    month_df = _rollup_month(selected, scenario="pass4_baseline")
    monitor_df, monitor_meta = _monitor_fragile_lanes(
        selected,
        props=_parse_list(args.monitor_props),
        min_bets_alert=int(args.monitor_min_bets_alert),
    )

    selected_csv = out_dir / "selected_rows.csv"
    summary_csv = out_dir / "summary.csv"
    slice_csv = out_dir / "slice_rollup.csv"
    prop_slice_csv = out_dir / "prop_slice_rollup.csv"
    month_csv = out_dir / "month_rollup.csv"
    monitor_csv = out_dir / "fragile_lane_monitor.csv"
    report_json = out_dir / "report.json"

    selected.to_csv(selected_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    slice_df.to_csv(slice_csv, index=False)
    prop_slice_df.to_csv(prop_slice_csv, index=False)
    month_df.to_csv(month_csv, index=False)
    monitor_df.to_csv(monitor_csv, index=False)

    parity: dict[str, object] = {}
    ref_path = Path(str(args.compare_summary_csv or "")).expanduser()
    if ref_path.exists():
        ref = pd.read_csv(ref_path)
        if not ref.empty and not summary_df.empty:
            ref_row = ref.sort_values("bets", ascending=False).iloc[0]
            cur_row = summary_df.iloc[0]
            parity = {
                "reference_file": str(ref_path),
                "reference_scenario": str(ref_row.get("scenario")),
                "reference_roi_1u": float(ref_row.get("roi_1u")),
                "replay_roi_1u": float(cur_row.get("roi_1u")),
                "delta_roi_1u": float(cur_row.get("roi_1u") - ref_row.get("roi_1u")),
                "reference_bets": int(ref_row.get("bets")),
                "replay_bets": int(cur_row.get("bets")),
                "delta_bets": int(cur_row.get("bets") - ref_row.get("bets")),
            }

    report = {
        "rows_csv": str(rows_csv),
        "policy_plan_csv": str(Path(args.policy_plan_csv).expanduser()),
        "counts": {
            "rows_input": int(len(rows)),
            "rows_scored": int(len(scored)),
            "rows_selected": int(len(selected)),
        },
        "monitor": monitor_meta,
        "parity": parity,
        "outputs": {
            "selected_rows_csv": str(selected_csv),
            "summary_csv": str(summary_csv),
            "slice_rollup_csv": str(slice_csv),
            "prop_slice_rollup_csv": str(prop_slice_csv),
            "month_rollup_csv": str(month_csv),
            "fragile_lane_monitor_csv": str(monitor_csv),
        },
    }
    report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

