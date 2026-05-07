#!/usr/bin/env python3
"""Build daily and rolling MLB model performance by prop.

Default source is full-slate model-pick outcome artifacts so every active prop
with resolved outcomes is represented. Ops paired model-vs-fade remains
available as an explicit source mode.

No DB writes. Outputs CSV only.
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd


DAILY_COLUMNS = [
    "date",
    "prop_type",
    "source_type",
    "model_bets",
    "fade_bets",
    "paired_bets",
    "model_win_rate",
    "fade_win_rate",
    "model_roi_1u",
    "fade_roi_1u",
    "delta_fade_minus_model_1u",
    "fade_beating_model_alert",
    "missing_reason",
]

SUMMARY_COLUMNS = [
    "prop_type",
    "source_type",
    "latest_date",
    "rolling_3d_model_win_rate",
    "rolling_3d_model_roi",
    "rolling_7d_model_win_rate",
    "rolling_7d_model_roi",
    "rolling_14d_model_win_rate",
    "rolling_14d_model_roi",
    "recent_bets_7d",
    "recent_bets_14d",
    "status",
    "suggested_action",
    "missing_reason",
]

DEFAULT_ACTIVE_PROPS_CSV = Path("backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv")
FULL_SLATE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")


def _parse_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except Exception as exc:
        raise SystemExit(f"{flag} must be YYYY-MM-DD, got {value!r}") from exc


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit(f"--to-date {end.isoformat()} is before --from-date {start.isoformat()}")
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _run_ops_aligned_model_vs_fade(day: str, work_dir: Path) -> tuple[Optional[pd.DataFrame], str]:
    day_dir = work_dir / day
    day_dir.mkdir(parents=True, exist_ok=True)
    rows_csv = day_dir / "reconcile_rows.csv"
    summary_json = day_dir / "model_vs_fade_summary.json"
    by_prop_csv = day_dir / "model_vs_fade_by_prop.csv"
    reconcile_summary = day_dir / "reconcile_summary.json"

    cmd = [
        "make",
        "mlb-post-grade-fade-check",
        f"MLB_RECONCILE_FROM_DATE={day}",
        f"MLB_RECONCILE_TO_DATE={day}",
        "MLB_RECONCILE_BOOKMAKER=betonlineag",
        "MLB_RECONCILE_ODDS_FILENAME=odds_latest_compatible.json",
        f"MLB_RECONCILE_ROWS_OUT_CSV={rows_csv}",
        f"MLB_RECONCILE_SUMMARY_OUT_JSON={reconcile_summary}",
        "MLB_RECONCILE_REQUIRE_OUTCOMES=1",
        "MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN=1",
        f"MLB_MODEL_VS_FADE_OUT_JSON={summary_json}",
        f"MLB_MODEL_VS_FADE_OUT_CSV={by_prop_csv}",
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        reason = "make_failed"
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        if detail:
            reason = f"{reason}:{detail[-1][:180]}"
        return None, reason
    if not by_prop_csv.exists():
        return None, "missing_model_vs_fade_by_prop"

    try:
        df = pd.read_csv(by_prop_csv)
    except Exception as exc:
        return None, f"read_error:{type(exc).__name__}"
    if df.empty:
        return None, "empty_model_vs_fade_by_prop"
    return df, "ok"


def _read_full_slate_by_prop(day: str) -> tuple[Optional[pd.DataFrame], str]:
    by_prop_csv = FULL_SLATE_ROOT / day / "full_slate_by_prop.csv"
    if by_prop_csv.exists():
        try:
            df = pd.read_csv(by_prop_csv)
        except Exception as exc:
            return None, f"read_error:{type(exc).__name__}"
        if df.empty:
            return None, "empty_full_slate_by_prop"
        rows = pd.DataFrame(
            {
                "date": day,
                "prop_type": df["prop_type"].astype(str).str.lower().str.strip(),
                "source_type": "full_slate_model_pick",
                "model_bets": pd.to_numeric(df.get("rows"), errors="coerce"),
                "fade_bets": np.nan,
                "paired_bets": np.nan,
                "model_win_rate": pd.to_numeric(df.get("model_win_rate"), errors="coerce"),
                "fade_win_rate": np.nan,
                "model_roi_1u": pd.to_numeric(df.get("model_roi"), errors="coerce"),
                "fade_roi_1u": np.nan,
                "delta_fade_minus_model_1u": np.nan,
                "fade_beating_model_alert": np.nan,
                "missing_reason": "",
            }
        )
        return rows, "ok"

    rows_csv = FULL_SLATE_ROOT / day / "reconcile_rows.csv"
    if not rows_csv.exists():
        return None, "missing_full_slate_by_prop_and_reconcile_rows"
    try:
        df = pd.read_csv(rows_csv, low_memory=False)
    except Exception as exc:
        return None, f"read_error:{type(exc).__name__}"
    required = {"prop_type", "actual_model_pick_outcome", "pnl_model_pick_1u"}
    if not required.issubset(df.columns):
        return None, "reconcile_rows_missing_required_columns"
    rows = df.copy()
    rows["prop_type"] = rows["prop_type"].astype(str).str.lower().str.strip()
    rows["actual_model_pick_outcome"] = rows["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
    rows["pnl_model_pick_1u"] = pd.to_numeric(rows["pnl_model_pick_1u"], errors="coerce")
    rows = rows[rows["actual_model_pick_outcome"].isin({"win", "loss", "push"}) & rows["pnl_model_pick_1u"].notna()]
    if rows.empty:
        return None, "no_resolved_full_slate_rows"
    rows["wins"] = rows["actual_model_pick_outcome"].eq("win").astype(int)
    rows["losses"] = rows["actual_model_pick_outcome"].eq("loss").astype(int)
    grouped = rows.groupby("prop_type", as_index=False).agg(
        model_bets=("prop_type", "size"),
        wins=("wins", "sum"),
        losses=("losses", "sum"),
        pnl=("pnl_model_pick_1u", "sum"),
    )
    decided = grouped["wins"] + grouped["losses"]
    grouped["model_win_rate"] = grouped["wins"] / decided.replace(0, np.nan)
    grouped["model_roi_1u"] = grouped["pnl"] / grouped["model_bets"].replace(0, np.nan)
    grouped.insert(0, "date", day)
    grouped["source_type"] = "full_slate_model_pick"
    grouped["fade_bets"] = np.nan
    grouped["paired_bets"] = np.nan
    grouped["fade_win_rate"] = np.nan
    grouped["fade_roi_1u"] = np.nan
    grouped["delta_fade_minus_model_1u"] = np.nan
    grouped["fade_beating_model_alert"] = np.nan
    grouped["missing_reason"] = ""
    return grouped[DAILY_COLUMNS], "ok"


def _load_active_props(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"active prop universe csv not found: {path}")
    df = pd.read_csv(path, usecols=lambda c: c == "prop_type")
    if "prop_type" not in df.columns:
        raise ValueError(f"active prop universe csv missing prop_type column: {path}")
    props = sorted(
        {
            str(p).strip().lower()
            for p in df["prop_type"].dropna().tolist()
            if str(p).strip()
        }
    )
    if not props:
        raise ValueError(f"active prop universe csv has no prop_type values: {path}")
    return props


def _daily_rows_for_date(day: str, by_prop: pd.DataFrame, active_props: list[str], source_type: str) -> pd.DataFrame:
    rows = by_prop.copy()
    if "prop_type" in rows.columns:
        rows["prop_type"] = rows["prop_type"].astype(str).str.lower().str.strip()
    if "source_type" not in rows.columns:
        rows["source_type"] = source_type
    if "date" not in rows.columns:
        rows.insert(0, "date", day)
    else:
        rows["date"] = day
    for col in DAILY_COLUMNS:
        if col not in rows.columns:
            rows[col] = np.nan
    rows["missing_reason"] = rows["missing_reason"].fillna("")

    present = set(rows["prop_type"].dropna().astype(str))
    missing_rows = []
    for prop in active_props:
        if prop in present:
            continue
        missing_rows.append(
            {
                "date": day,
                "prop_type": prop,
                "source_type": source_type,
                "model_bets": 0,
                "fade_bets": 0 if source_type == "ops_paired_model_vs_fade" else np.nan,
                "paired_bets": 0 if source_type == "ops_paired_model_vs_fade" else np.nan,
                "model_win_rate": np.nan,
                "fade_win_rate": np.nan,
                "model_roi_1u": np.nan,
                "fade_roi_1u": np.nan,
                "delta_fade_minus_model_1u": np.nan,
                "fade_beating_model_alert": False,
                "missing_reason": (
                    "not_present_in_full_slate_by_prop; no resolved full-slate model-pick rows"
                    if source_type == "full_slate_model_pick"
                    else (
                        "not_present_in_ops_model_vs_fade_by_prop; "
                        "zero paired two-sided model/fade rows in selected ops-aligned source, "
                        "or prop absent from selected reconcile rows"
                    )
                ),
            }
        )
    if missing_rows:
        rows = pd.concat([rows, pd.DataFrame(missing_rows)], ignore_index=True)
    rows = rows[rows["prop_type"].isin(active_props)].copy()
    return rows[DAILY_COLUMNS].sort_values(["date", "prop_type"]).reset_index(drop=True)


def _weighted_rate(group: pd.DataFrame, value_col: str, weight_col: str = "model_bets") -> float:
    vals = pd.to_numeric(group[value_col], errors="coerce")
    weights = pd.to_numeric(group[weight_col], errors="coerce").fillna(0.0)
    mask = vals.notna() & weights.gt(0)
    if not mask.any():
        return np.nan
    return float((vals[mask] * weights[mask]).sum() / weights[mask].sum())


def _rolling_for_prop(prop_df: pd.DataFrame, *, end_date: date, days: int) -> tuple[float, float, int]:
    start = end_date - timedelta(days=days - 1)
    dates = pd.to_datetime(prop_df["date"], errors="coerce").dt.date
    window = prop_df[(dates >= start) & (dates <= end_date)].copy()
    if window.empty:
        return np.nan, np.nan, 0
    win_rate = _weighted_rate(window, "model_win_rate")
    roi = _weighted_rate(window, "model_roi_1u")
    bets = int(pd.to_numeric(window["model_bets"], errors="coerce").fillna(0).sum())
    return win_rate, roi, bets


def _classify(rolling_7d_win_rate: float, rolling_7d_roi: float, recent_bets_7d: int) -> tuple[str, str]:
    if int(recent_bets_7d or 0) <= 0:
        return "no_recent_data", "investigate_or_wait"
    wr = rolling_7d_win_rate
    roi = rolling_7d_roi
    if pd.isna(wr) or pd.isna(roi):
        return "insufficient_data", "investigate"
    if roi < -0.10 and wr < 0.48:
        return "critical", "investigate"
    if roi < -0.05 or wr < 0.50:
        if roi < -0.05:
            return "watch", "reduce_exposure"
        return "watch", "tighten_threshold"
    if roi > 0 and wr >= 0.53:
        return "strong", "keep_normal"
    if -0.05 <= roi <= 0.05:
        return "neutral", "keep_normal"
    return "neutral", "keep_normal"


def _build_summary(daily: pd.DataFrame, *, end_date: date, active_props: list[str], source_type: str) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for prop in active_props:
        group = daily[daily["prop_type"].astype(str).str.lower().str.strip().eq(prop)].copy()
        latest = pd.to_datetime(group["date"], errors="coerce").dt.date.max()
        r3_wr, r3_roi, _ = _rolling_for_prop(group, end_date=end_date, days=3)
        r7_wr, r7_roi, r7_bets = _rolling_for_prop(group, end_date=end_date, days=7)
        r14_wr, r14_roi, r14_bets = _rolling_for_prop(group, end_date=end_date, days=14)
        status, action = _classify(r7_wr, r7_roi, r7_bets)
        missing_reasons = []
        if "missing_reason" in group.columns:
            missing_reasons = sorted(
                {
                    str(v).strip()
                    for v in group["missing_reason"].dropna().tolist()
                    if str(v).strip()
                }
            )
        if r7_bets <= 0 and not missing_reasons:
            missing_reasons = ["no model-vs-fade rows in recent 7d window"]
        rows.append(
            {
                "prop_type": prop,
                "source_type": source_type,
                "latest_date": latest.isoformat() if latest else "",
                "rolling_3d_model_win_rate": r3_wr,
                "rolling_3d_model_roi": r3_roi,
                "rolling_7d_model_win_rate": r7_wr,
                "rolling_7d_model_roi": r7_roi,
                "rolling_14d_model_win_rate": r14_wr,
                "rolling_14d_model_roi": r14_roi,
                "recent_bets_7d": r7_bets,
                "recent_bets_14d": r14_bets,
                "status": status,
                "suggested_action": action,
                "missing_reason": "; ".join(missing_reasons),
            }
        )
    out = pd.DataFrame(rows, columns=SUMMARY_COLUMNS)
    if not out.empty:
        out = out.sort_values(["status", "rolling_7d_model_roi", "recent_bets_7d"], ascending=[True, True, False])
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Report MLB daily and rolling model performance by prop.")
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument("--out-csv", default="backend/mlb/exports/model_performance/prop_daily_performance.csv")
    ap.add_argument("--summary-csv", default="backend/mlb/exports/model_performance/prop_rolling_summary.csv")
    ap.add_argument("--active-props-csv", default=str(DEFAULT_ACTIVE_PROPS_CSV))
    ap.add_argument(
        "--source-type",
        choices=["full_slate_model_pick", "ops_paired_model_vs_fade"],
        default="full_slate_model_pick",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    start = _parse_date(args.from_date, "--from-date")
    end = _parse_date(args.to_date, "--to-date")
    active_props = _load_active_props(Path(args.active_props_csv))
    work_dir = Path("tmp/analysis/model_performance_by_prop_daily")
    daily_parts: list[pd.DataFrame] = []

    for day in _date_range(start, end):
        day_s = day.isoformat()
        if args.source_type == "ops_paired_model_vs_fade":
            by_prop, status = _run_ops_aligned_model_vs_fade(day_s, work_dir)
        else:
            by_prop, status = _read_full_slate_by_prop(day_s)
        if by_prop is None:
            print(f"[mlb-model-perf-by-prop] date={day_s} status=skipped reason={status}")
            continue
        date_rows = _daily_rows_for_date(day_s, by_prop, active_props, args.source_type)
        daily_parts.append(date_rows)
        source_props = by_prop["prop_type"].nunique() if "prop_type" in by_prop.columns else 0
        print(
            f"[mlb-model-perf-by-prop] date={day_s} status=ok source_type={args.source_type} "
            f"source_props={source_props} output_props={len(date_rows)}"
        )

    daily = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame(columns=DAILY_COLUMNS)
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out_csv, index=False)

    summary = _build_summary(daily, end_date=end, active_props=active_props, source_type=args.source_type)
    summary_csv = Path(args.summary_csv)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_csv, index=False)

    print(f"[mlb-model-perf-by-prop] out_csv={out_csv} rows={len(daily)}")
    print(f"[mlb-model-perf-by-prop] summary_csv={summary_csv} rows={len(summary)}")
    if not summary.empty:
        print("[mlb-model-perf-by-prop] status_counts=" + summary["status"].value_counts().to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
