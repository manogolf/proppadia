#!/usr/bin/env python3
"""Append daily MLB post-grade analysis metrics and render simple trend charts.

Inputs:
- tmp/analysis/mlb_model_vs_fade_summary.json
- tmp/analysis/mlb_all_available_summary.json
- tmp/analysis/mlb_all_available_by_prop.csv
- optional: backend/mlb/data/processed/mlb_book_upload.csv

Outputs:
- artifacts/mlb_postgrade_daily_tracker.csv
- artifacts/mlb_postgrade_by_prop_daily_tracker.csv
- artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json
- artifacts/analysis/mlb/mlb_postgrade_alerts_history.jsonl
- artifacts/analysis/mlb/mlb_postgrade_dashboard.png
- artifacts/analysis/mlb/mlb_postgrade_roi.png
- artifacts/analysis/mlb/mlb_postgrade_winrate.png
- artifacts/analysis/mlb/mlb_postgrade_volume.png
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


ET = ZoneInfo("America/New_York")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing json input: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected json object in {path}")
    return payload


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        out = int(value)
        return out
    except Exception:
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _extract_report_date(
    *,
    explicit_date: str,
    model_payload: dict[str, Any],
    all_payload: dict[str, Any],
) -> str:
    if explicit_date.strip():
        return explicit_date.strip()

    candidates = [
        model_payload.get("window", {}).get("game_date_max"),
        model_payload.get("window", {}).get("game_date_min"),
        all_payload.get("window", {}).get("game_date_max"),
        all_payload.get("window", {}).get("game_date_min"),
    ]
    for c in candidates:
        text = str(c or "").strip()
        if text:
            return text
    return datetime.now(ET).date().isoformat()


def _extract_book_upload_metrics(book_upload_csv: Path) -> dict[str, int]:
    out = {
        "book_upload_rows": 0,
        "book_upload_over_rows": 0,
        "book_upload_under_rows": 0,
        "book_upload_top40_over_rows": 0,
        "book_upload_top40_under_rows": 0,
    }
    if not book_upload_csv.exists():
        return out

    df = pd.read_csv(book_upload_csv, low_memory=False)
    if df.empty or "SIDE" not in df.columns:
        return out

    side = df["SIDE"].astype(str).str.lower().str.strip()
    out["book_upload_rows"] = int(len(df))
    out["book_upload_over_rows"] = int(side.eq("over").sum())
    out["book_upload_under_rows"] = int(side.eq("under").sum())

    top40 = side.head(40)
    out["book_upload_top40_over_rows"] = int(top40.eq("over").sum())
    out["book_upload_top40_under_rows"] = int(top40.eq("under").sum())
    return out


def _extract_all_available_by_prop(by_prop_csv: Path) -> pd.DataFrame:
    if not by_prop_csv.exists():
        return pd.DataFrame(columns=["prop_type", "rows", "model_rows", "model_win_rate_pct"])
    try:
        df = pd.read_csv(by_prop_csv, low_memory=False)
    except Exception:
        return pd.DataFrame(columns=["prop_type", "rows", "model_rows", "model_win_rate_pct"])
    if df.empty:
        return pd.DataFrame(columns=["prop_type", "rows", "model_rows", "model_win_rate_pct"])

    out = df.copy()
    for col in ("prop_type", "rows", "model_rows", "model_win_rate_pct"):
        if col not in out.columns:
            out[col] = pd.NA
    out["prop_type"] = out["prop_type"].astype(str).str.lower().str.strip()
    out["rows"] = pd.to_numeric(out["rows"], errors="coerce")
    out["model_rows"] = pd.to_numeric(out["model_rows"], errors="coerce")
    out["model_win_rate_pct"] = pd.to_numeric(out["model_win_rate_pct"], errors="coerce")
    out = out.dropna(subset=["prop_type"])
    out = out[out["prop_type"] != ""]
    return out[["prop_type", "rows", "model_rows", "model_win_rate_pct"]].copy()


def _build_row(
    *,
    report_date: str,
    model_payload: dict[str, Any],
    all_payload: dict[str, Any],
    book_metrics: dict[str, int],
) -> dict[str, Any]:
    model_overall = model_payload.get("overall", {}) or {}
    model_counts = model_payload.get("counts", {}) or {}
    all_overall = all_payload.get("overall", {}) or {}
    all_counts = all_payload.get("counts", {}) or {}

    model_win_rate = _to_float(model_overall.get("model_win_rate"))
    fade_win_rate = _to_float(model_overall.get("fade_win_rate"))

    return {
        "report_date": report_date,
        "captured_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "model_bets": _to_int(model_overall.get("model_bets")) or 0,
        "fade_bets": _to_int(model_overall.get("fade_bets")) or 0,
        "paired_bets": _to_int(model_overall.get("paired_bets")) or 0,
        "model_roi_1u": _to_float(model_overall.get("model_roi_1u")),
        "fade_roi_1u": _to_float(model_overall.get("fade_roi_1u")),
        "delta_fade_minus_model_1u": _to_float(model_overall.get("delta_fade_minus_model_1u")),
        "model_win_rate_pct": None if model_win_rate is None else round(100.0 * model_win_rate, 2),
        "fade_win_rate_pct": None if fade_win_rate is None else round(100.0 * fade_win_rate, 2),
        "fade_beating_model_alert": bool(model_overall.get("fade_beating_model_alert")),
        "rows_input": _to_int(model_counts.get("rows_input")) or 0,
        "rows_with_model_pnl": _to_int(model_counts.get("rows_with_model_pnl")) or 0,
        "rows_paired_for_fade": _to_int(model_counts.get("rows_paired_for_fade")) or 0,
        "all_rows_input": _to_int(all_counts.get("rows_input")) or 0,
        "all_rows_resolved_any": _to_int(all_counts.get("rows_resolved_any")) or 0,
        "all_rows_resolved_two_sided": _to_int(all_counts.get("rows_resolved_two_sided")) or 0,
        "all_rows_with_model_pick_result": _to_int(all_counts.get("rows_with_model_pick_result")) or 0,
        "all_model_win_rate_pct": _to_float(all_overall.get("model_win_rate_pct")),
        **book_metrics,
    }


def _upsert_daily_row(out_csv: Path, row: dict[str, Any]) -> pd.DataFrame:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    row_df = pd.DataFrame([row])
    if out_csv.exists():
        try:
            prev = pd.read_csv(out_csv, low_memory=False)
        except Exception:
            prev = pd.DataFrame()
    else:
        prev = pd.DataFrame()

    if not prev.empty and "report_date" in prev.columns:
        prev = prev[prev["report_date"].astype(str) != str(row["report_date"])]

    merged = pd.concat([prev, row_df], ignore_index=True)
    if "report_date" in merged.columns:
        merged["_date_sort"] = pd.to_datetime(merged["report_date"], errors="coerce")
        merged = merged.sort_values("_date_sort", ascending=True, kind="mergesort").drop(columns=["_date_sort"])
    merged.to_csv(out_csv, index=False)
    return merged


def _upsert_prop_rows(out_csv: Path, *, report_date: str, by_prop: pd.DataFrame) -> pd.DataFrame:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists():
        try:
            prev = pd.read_csv(out_csv, low_memory=False)
        except Exception:
            prev = pd.DataFrame()
    else:
        prev = pd.DataFrame()

    if not prev.empty and "report_date" in prev.columns:
        prev = prev[prev["report_date"].astype(str) != str(report_date)]

    if by_prop.empty:
        merged = prev.copy()
    else:
        add = by_prop.copy()
        add["report_date"] = report_date
        add["captured_at_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        add = add[["report_date", "captured_at_utc", "prop_type", "rows", "model_rows", "model_win_rate_pct"]]
        merged = pd.concat([prev, add], ignore_index=True)

    if not merged.empty and "report_date" in merged.columns:
        merged["_date_sort"] = pd.to_datetime(merged["report_date"], errors="coerce")
        merged = merged.sort_values(["_date_sort", "prop_type"], ascending=[True, True], kind="mergesort").drop(
            columns=["_date_sort"]
        )
    merged.to_csv(out_csv, index=False)
    return merged


def _window_delta(series: pd.Series, *, window: int) -> tuple[float | None, float | None, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna().tolist()
    if window <= 0 or len(values) < (2 * window):
        return None, None, None
    prev = values[-(2 * window) : -window]
    last = values[-window:]
    prev_mean = float(pd.Series(prev).mean()) if prev else None
    last_mean = float(pd.Series(last).mean()) if last else None
    if prev_mean is None or last_mean is None:
        return None, prev_mean, last_mean
    return float(last_mean - prev_mean), prev_mean, last_mean


def _compute_alerts(
    *,
    report_date: str,
    history: pd.DataFrame,
    prop_history: pd.DataFrame,
    fade_min_paired_bets: int,
    roi_min_paired_bets: int,
    roi_breach_threshold: float,
    overall_drop_window_days: int,
    overall_drop_threshold_pct: float,
    prop_drop_window_days: int,
    prop_drop_threshold_pct: float,
    prop_drop_min_model_rows: int,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    if history.empty:
        return alerts

    daily = history.copy()
    daily["report_date"] = pd.to_datetime(daily["report_date"], errors="coerce")
    daily = daily.sort_values("report_date")
    latest = daily.iloc[-1]

    paired_bets = _to_int(latest.get("paired_bets")) or 0
    model_roi = _to_float(latest.get("model_roi_1u"))
    fade_beating = _to_bool(latest.get("fade_beating_model_alert"))

    if fade_beating and paired_bets >= int(fade_min_paired_bets):
        alerts.append(
            {
                "code": "fade_beating_model",
                "severity": "critical",
                "message": "Fade is beating model on paired bets.",
                "report_date": report_date,
                "paired_bets": paired_bets,
                "model_roi_1u": model_roi,
                "fade_roi_1u": _to_float(latest.get("fade_roi_1u")),
            }
        )

    if model_roi is not None and paired_bets >= int(roi_min_paired_bets) and model_roi <= float(roi_breach_threshold):
        alerts.append(
            {
                "code": "model_roi_breach",
                "severity": "critical",
                "message": "Model ROI breached threshold on paired bets.",
                "report_date": report_date,
                "paired_bets": paired_bets,
                "model_roi_1u": model_roi,
                "roi_breach_threshold": float(roi_breach_threshold),
            }
        )

    overall_delta, overall_prev, overall_last = _window_delta(
        pd.to_numeric(daily.get("all_model_win_rate_pct"), errors="coerce"),
        window=int(overall_drop_window_days),
    )
    if overall_delta is not None and overall_delta <= (-1.0 * float(overall_drop_threshold_pct)):
        alerts.append(
            {
                "code": "overall_model_win_rate_drop",
                "severity": "warning",
                "message": "Overall model win rate dropped across recent windows.",
                "report_date": report_date,
                "window_days": int(overall_drop_window_days),
                "prev_window_avg_pct": round(float(overall_prev), 2),
                "last_window_avg_pct": round(float(overall_last), 2),
                "delta_pct": round(float(overall_delta), 2),
                "drop_threshold_pct": float(overall_drop_threshold_pct),
            }
        )

    if not prop_history.empty:
        ph = prop_history.copy()
        ph["report_date"] = pd.to_datetime(ph["report_date"], errors="coerce")
        ph["model_rows"] = pd.to_numeric(ph["model_rows"], errors="coerce")
        ph["model_win_rate_pct"] = pd.to_numeric(ph["model_win_rate_pct"], errors="coerce")
        ph = ph.dropna(subset=["report_date", "prop_type", "model_win_rate_pct"])

        for prop, g in ph.groupby("prop_type", dropna=False):
            g = g.sort_values("report_date")
            if int(prop_drop_min_model_rows) > 0:
                g = g[g["model_rows"].fillna(0) >= int(prop_drop_min_model_rows)]
            if g.empty:
                continue
            delta, prev, last = _window_delta(g["model_win_rate_pct"], window=int(prop_drop_window_days))
            if delta is None:
                continue
            if delta <= (-1.0 * float(prop_drop_threshold_pct)):
                alerts.append(
                    {
                        "code": "prop_model_win_rate_drop",
                        "severity": "warning",
                        "message": "Prop-level model win rate dropped across recent windows.",
                        "report_date": report_date,
                        "prop_type": str(prop),
                        "window_days": int(prop_drop_window_days),
                        "prev_window_avg_pct": round(float(prev), 2),
                        "last_window_avg_pct": round(float(last), 2),
                        "delta_pct": round(float(delta), 2),
                        "drop_threshold_pct": float(prop_drop_threshold_pct),
                        "min_model_rows_per_day": int(prop_drop_min_model_rows),
                    }
                )
    return alerts


def _write_alert_outputs(
    *,
    alerts_out_json: Path,
    alerts_history_jsonl: Path,
    payload: dict[str, Any],
) -> None:
    alerts_out_json.parent.mkdir(parents=True, exist_ok=True)
    alerts_history_jsonl.parent.mkdir(parents=True, exist_ok=True)
    alerts_out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines: list[str] = []
    if alerts_history_jsonl.exists():
        lines = alerts_history_jsonl.read_text(encoding="utf-8").splitlines()

    target_date = str(payload.get("report_date", "")).strip()
    fresh: list[str] = []
    for ln in lines:
        text = ln.strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except Exception:
            fresh.append(text)
            continue
        if str(obj.get("report_date", "")).strip() == target_date:
            continue
        fresh.append(text)
    fresh.append(json.dumps(payload, separators=(",", ":")))
    alerts_history_jsonl.write_text("\n".join(fresh) + "\n", encoding="utf-8")


def _maybe_build_charts(history: pd.DataFrame, charts_dir: Path) -> tuple[list[str], str | None]:
    if history.empty:
        return [], "no_history_rows"
    # Keep matplotlib cache in a writable project path to avoid runtime crashes
    # when home-level cache dirs are unavailable/unwritable in ops shells.
    mpl_cache = Path("artifacts/analysis/mlb/.matplotlib_cache").expanduser()
    mpl_cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_cache))
    # Force a headless-safe backend for shell/cron execution.
    os.environ.setdefault("MPLBACKEND", "Agg")
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return [], "matplotlib_missing"

    charts_dir.mkdir(parents=True, exist_ok=True)
    work = history.copy()
    work["report_date"] = pd.to_datetime(work["report_date"], errors="coerce")
    work = work.dropna(subset=["report_date"]).sort_values("report_date")
    if work.empty:
        return [], "no_valid_dates"

    x = work["report_date"]

    roi_png = charts_dir / "mlb_postgrade_roi.png"
    wr_png = charts_dir / "mlb_postgrade_winrate.png"
    vol_png = charts_dir / "mlb_postgrade_volume.png"
    dash_png = charts_dir / "mlb_postgrade_dashboard.png"

    # ROI chart
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(x, pd.to_numeric(work["model_roi_1u"], errors="coerce"), marker="o", label="Model ROI (1u)")
    ax.plot(x, pd.to_numeric(work["fade_roi_1u"], errors="coerce"), marker="o", label="Fade ROI (1u)")
    ax.axhline(0.0, color="#999999", linewidth=1, linestyle="--")
    ax.set_title("MLB Post-Grade ROI Trend")
    ax.set_ylabel("ROI per bet")
    ax.legend(loc="best")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(roi_png, dpi=140)
    plt.close(fig)

    # Win-rate chart
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(x, pd.to_numeric(work["model_win_rate_pct"], errors="coerce"), marker="o", label="Model Win Rate %")
    ax.plot(x, pd.to_numeric(work["fade_win_rate_pct"], errors="coerce"), marker="o", label="Fade Win Rate %")
    ax.plot(x, pd.to_numeric(work["all_model_win_rate_pct"], errors="coerce"), marker="o", label="All-Available Model Win Rate %")
    ax.axhline(50.0, color="#999999", linewidth=1, linestyle="--")
    ax.set_title("MLB Post-Grade Win-Rate Trend")
    ax.set_ylabel("Win Rate %")
    ax.legend(loc="best")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(wr_png, dpi=140)
    plt.close(fig)

    # Volume chart
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(x, pd.to_numeric(work["paired_bets"], errors="coerce"), marker="o", label="Model vs Fade Paired Bets")
    ax.plot(
        x,
        pd.to_numeric(work["all_rows_resolved_two_sided"], errors="coerce"),
        marker="o",
        label="All-Available Two-Sided Resolved Rows",
    )
    ax.set_title("MLB Post-Grade Volume Trend")
    ax.set_ylabel("Count")
    ax.legend(loc="best")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(vol_png, dpi=140)
    plt.close(fig)

    # Combined dashboard
    fig, axs = plt.subplots(3, 1, figsize=(11, 11), sharex=True)
    axs[0].plot(x, pd.to_numeric(work["model_roi_1u"], errors="coerce"), marker="o", label="Model ROI")
    axs[0].plot(x, pd.to_numeric(work["fade_roi_1u"], errors="coerce"), marker="o", label="Fade ROI")
    axs[0].axhline(0.0, color="#999999", linewidth=1, linestyle="--")
    axs[0].set_ylabel("ROI")
    axs[0].legend(loc="best")
    axs[0].grid(alpha=0.25)

    axs[1].plot(x, pd.to_numeric(work["model_win_rate_pct"], errors="coerce"), marker="o", label="Model %")
    axs[1].plot(x, pd.to_numeric(work["fade_win_rate_pct"], errors="coerce"), marker="o", label="Fade %")
    axs[1].plot(x, pd.to_numeric(work["all_model_win_rate_pct"], errors="coerce"), marker="o", label="All-Avail Model %")
    axs[1].axhline(50.0, color="#999999", linewidth=1, linestyle="--")
    axs[1].set_ylabel("Win %")
    axs[1].legend(loc="best")
    axs[1].grid(alpha=0.25)

    axs[2].plot(x, pd.to_numeric(work["paired_bets"], errors="coerce"), marker="o", label="Paired Bets")
    axs[2].plot(
        x,
        pd.to_numeric(work["all_rows_resolved_two_sided"], errors="coerce"),
        marker="o",
        label="Two-Sided Resolved",
    )
    axs[2].set_ylabel("Count")
    axs[2].legend(loc="best")
    axs[2].grid(alpha=0.25)

    axs[0].set_title("MLB Post-Grade Tracker Dashboard")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(dash_png, dpi=140)
    plt.close(fig)

    return [str(dash_png), str(roi_png), str(wr_png), str(vol_png)], None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append MLB daily post-grade metrics and render trend charts.")
    ap.add_argument("--date", default="", help="Report date (YYYY-MM-DD). Default: inferred from summary windows.")
    ap.add_argument("--model-vs-fade-summary-json", default="tmp/analysis/mlb_model_vs_fade_summary.json")
    ap.add_argument("--all-available-summary-json", default="tmp/analysis/mlb_all_available_summary.json")
    ap.add_argument("--all-available-by-prop-csv", default="tmp/analysis/mlb_all_available_by_prop.csv")
    ap.add_argument("--book-upload-csv", default="backend/mlb/data/processed/mlb_book_upload.csv")
    ap.add_argument("--out-csv", default="artifacts/mlb_postgrade_daily_tracker.csv")
    ap.add_argument("--out-by-prop-csv", default="artifacts/mlb_postgrade_by_prop_daily_tracker.csv")
    ap.add_argument("--charts-dir", default="artifacts/analysis/mlb")
    ap.add_argument("--alerts-out-json", default="artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json")
    ap.add_argument("--alerts-history-jsonl", default="artifacts/analysis/mlb/mlb_postgrade_alerts_history.jsonl")
    ap.add_argument("--alert-fade-min-paired-bets", type=int, default=30)
    ap.add_argument("--alert-roi-min-paired-bets", type=int, default=30)
    ap.add_argument("--alert-roi-breach-threshold", type=float, default=-0.08)
    ap.add_argument("--alert-overall-drop-window-days", type=int, default=3)
    ap.add_argument("--alert-overall-drop-threshold-pct", type=float, default=5.0)
    ap.add_argument("--alert-prop-drop-window-days", type=int, default=3)
    ap.add_argument("--alert-prop-drop-threshold-pct", type=float, default=8.0)
    ap.add_argument("--alert-prop-drop-min-model-rows", type=int, default=20)
    ap.add_argument("--alerts-strict", action="store_true", help="Exit non-zero when critical alerts are present.")
    ap.add_argument("--skip-charts", action="store_true")
    args = ap.parse_args(argv)

    model_path = Path(args.model_vs_fade_summary_json).expanduser()
    all_path = Path(args.all_available_summary_json).expanduser()
    all_by_prop_path = Path(args.all_available_by_prop_csv).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_by_prop_csv = Path(args.out_by_prop_csv).expanduser()
    charts_dir = Path(args.charts_dir).expanduser()
    alerts_out_json = Path(args.alerts_out_json).expanduser()
    alerts_history_jsonl = Path(args.alerts_history_jsonl).expanduser()
    book_upload_csv = Path(args.book_upload_csv).expanduser()

    model_payload = _load_json(model_path)
    all_payload = _load_json(all_path)
    all_by_prop = _extract_all_available_by_prop(all_by_prop_path)
    report_date = _extract_report_date(
        explicit_date=str(args.date or ""),
        model_payload=model_payload,
        all_payload=all_payload,
    )
    book_metrics = _extract_book_upload_metrics(book_upload_csv)

    row = _build_row(
        report_date=report_date,
        model_payload=model_payload,
        all_payload=all_payload,
        book_metrics=book_metrics,
    )
    history = _upsert_daily_row(out_csv, row)
    prop_history = _upsert_prop_rows(out_by_prop_csv, report_date=report_date, by_prop=all_by_prop)
    if args.skip_charts:
        charts, chart_warning = [], "charts_skipped_by_flag"
    else:
        charts, chart_warning = _maybe_build_charts(history, charts_dir)

    alerts = _compute_alerts(
        report_date=report_date,
        history=history,
        prop_history=prop_history,
        fade_min_paired_bets=int(args.alert_fade_min_paired_bets),
        roi_min_paired_bets=int(args.alert_roi_min_paired_bets),
        roi_breach_threshold=float(args.alert_roi_breach_threshold),
        overall_drop_window_days=int(args.alert_overall_drop_window_days),
        overall_drop_threshold_pct=float(args.alert_overall_drop_threshold_pct),
        prop_drop_window_days=int(args.alert_prop_drop_window_days),
        prop_drop_threshold_pct=float(args.alert_prop_drop_threshold_pct),
        prop_drop_min_model_rows=int(args.alert_prop_drop_min_model_rows),
    )
    critical_count = sum(1 for a in alerts if str(a.get("severity", "")).lower() == "critical")
    warning_count = sum(1 for a in alerts if str(a.get("severity", "")).lower() == "warning")
    alerts_payload = {
        "captured_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "status": "alert" if alerts else "ok",
        "report_date": report_date,
        "tracker_csv": str(out_csv),
        "by_prop_tracker_csv": str(out_by_prop_csv),
        "alerts_count": len(alerts),
        "critical_count": critical_count,
        "warning_count": warning_count,
        "thresholds": {
            "fade_min_paired_bets": int(args.alert_fade_min_paired_bets),
            "roi_min_paired_bets": int(args.alert_roi_min_paired_bets),
            "roi_breach_threshold": float(args.alert_roi_breach_threshold),
            "overall_drop_window_days": int(args.alert_overall_drop_window_days),
            "overall_drop_threshold_pct": float(args.alert_overall_drop_threshold_pct),
            "prop_drop_window_days": int(args.alert_prop_drop_window_days),
            "prop_drop_threshold_pct": float(args.alert_prop_drop_threshold_pct),
            "prop_drop_min_model_rows": int(args.alert_prop_drop_min_model_rows),
        },
        "alerts": alerts,
    }
    _write_alert_outputs(
        alerts_out_json=alerts_out_json,
        alerts_history_jsonl=alerts_history_jsonl,
        payload=alerts_payload,
    )

    summary = {
        "status": "ok" if (critical_count == 0 or not args.alerts_strict) else "fail",
        "report_date": report_date,
        "tracker_csv": str(out_csv),
        "by_prop_tracker_csv": str(out_by_prop_csv),
        "rows_in_tracker": int(len(history)),
        "rows_in_prop_tracker": int(len(prop_history)),
        "charts": charts,
        "chart_warning": chart_warning,
        "alerts_out_json": str(alerts_out_json),
        "alerts_history_jsonl": str(alerts_history_jsonl),
        "alerts_count": len(alerts),
        "critical_count": critical_count,
        "warning_count": warning_count,
        "alerts_strict": bool(args.alerts_strict),
    }
    print(json.dumps(summary, indent=2))
    if args.alerts_strict and critical_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
