#!/usr/bin/env python3
"""Build MLB prop regime validation artifacts from historical model-pick rows.

This is an analysis/artifact builder only. It does not touch model training,
prediction generation, upload generation, calibration, odds ingestion, or UI.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Iterable, Optional

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.scripts.add_prop_regime_context_fields import add_context_fields


ACTIVE_PROPS = [
    "hits",
    "total_bases",
    "strikeouts_batting",
    "earned_runs",
    "doubles",
    "hits_allowed",
    "strikeouts_pitching",
    "walks",
    "hits_runs_rbis",
    "runs_scored",
    "walks_allowed",
    "rbis",
]

PROP_FAMILY = {
    "hits": "contact",
    "doubles": "power_low_frequency",
    "total_bases": "power_low_frequency",
    "walks": "contact",
    "strikeouts_batting": "contact",
    "hits_runs_rbis": "run_production",
    "runs_scored": "run_production",
    "rbis": "run_production",
    "strikeouts_pitching": "pitching",
    "earned_runs": "pitching",
    "hits_allowed": "pitching",
    "walks_allowed": "pitching",
}

DEFAULT_RECONCILE_CSVS = [
    Path("tmp/mlb_reconcile_rows_historical_bestbook_2024.csv"),
    Path("tmp/mlb_reconcile_rows_historical_bestbook_2025.csv"),
    Path("tmp/mlb_base_vs_market_rows_anybook_full.csv"),
]
DEFAULT_EXECUTION_CSV = Path("artifacts/analysis/mlb/execution_vs_model/extended_clean/execution_vs_model.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/prop_regime_validation")
DEFAULT_DEPLOY_CSV = Path("backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv")

WIN_LOSS_PUSH = {"win", "loss", "push"}
CHUNK_SIZE = 150_000
REGIME_MIN_ROWS = 25


def _norm_text(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"", "nan", "none", "null"}:
        return ""
    return text


def _safe_pct(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value) * 100:.2f}%"


def _fmt_float(value: object, digits: int = 4) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.{digits}f}".rstrip("0").rstrip(".")


def _markdown_table(df: pd.DataFrame, pct_cols: Iterable[str] = ()) -> str:
    if df.empty:
        return "(none)"
    pct_cols = set(pct_cols)
    rows = ["| " + " | ".join(df.columns) + " |"]
    rows.append("| " + " | ".join(["---"] * len(df.columns)) + " |")
    for _, row in df.iterrows():
        vals = []
        for col in df.columns:
            value = row[col]
            if pd.isna(value):
                text = ""
            elif col in pct_cols:
                text = _safe_pct(value)
            elif isinstance(value, float):
                text = _fmt_float(value)
            else:
                text = str(value)
            vals.append(text.replace("|", "\\|"))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join(rows)


def _sample_confidence(rows: float) -> str:
    rows = float(rows or 0)
    if rows >= 50:
        return "HIGH"
    if rows >= 25:
        return "MEDIUM"
    if rows >= 10:
        return "LOW"
    return "INSUFFICIENT"


def _regime_from_roi(roi: object, rows: object, min_rows: int = 25) -> str:
    rows = float(rows or 0)
    if rows < min_rows or pd.isna(roi):
        return "INSUFFICIENT"
    roi = float(roi)
    if roi > 0.05:
        return "HOT"
    if roi < -0.05:
        return "COLD"
    return "NEUTRAL"


def _trend_direction(recent_roi: object, prior_roi: object, recent_rows: object, prior_rows: object) -> str:
    recent_rows = float(recent_rows or 0)
    prior_rows = float(prior_rows or 0)
    if recent_rows < REGIME_MIN_ROWS or prior_rows < REGIME_MIN_ROWS or pd.isna(recent_roi) or pd.isna(prior_roi):
        return "INSUFFICIENT"
    recent_roi = float(recent_roi)
    prior_roi = float(prior_roi)
    delta = recent_roi - prior_roi
    if delta >= 0.05:
        return "IMPROVING"
    if delta <= -0.05 and recent_roi < 0 and prior_roi < 0:
        return "DETERIORATING"
    if delta <= -0.05:
        return "COOLING"
    return "FLAT"


def _execution_regime(roi: object, bets: object, rolling_3d_roi: object = np.nan) -> str:
    bets = float(bets or 0)
    if bets <= 0 or pd.isna(roi):
        return "INSUFFICIENT"
    roi = float(roi)
    if roi > 0.05:
        return "HOT" if bets >= 50 else "SOFT HOT"
    if roi < -0.05:
        return "COLD"
    if roi < 0 and not pd.isna(rolling_3d_roi) and float(rolling_3d_roi) < 0:
        return "COOLING"
    return "NEUTRAL"


def _board_usage(long_term: str, recent: str, execution: str) -> str:
    if long_term == "COLD" and recent in {"COLD", "INSUFFICIENT"} and execution in {"COLD", "COOLING"}:
        return "exclude_or_deemphasize"
    if long_term == "COLD" and (recent in {"HOT"} or execution in {"HOT", "SOFT HOT"}):
        return "watch_low_exposure"
    if long_term in {"HOT", "NEUTRAL"} and (recent in {"HOT"} or execution in {"HOT", "SOFT HOT"}):
        return "active_monitor"
    return "monitor_only"


def _select_adaptive_window(row: pd.Series, prefix: str = "rolling") -> dict[str, object]:
    for window in (7, 14, 30):
        rows = row.get(f"{prefix}_{window}d_rows", 0.0)
        roi = row.get(f"{prefix}_{window}d_roi_proxy", np.nan)
        if float(rows or 0) >= REGIME_MIN_ROWS and not pd.isna(roi):
            return {
                "window_days": window,
                "rows": rows,
                "roi": roi,
                "prior_rows": row.get(f"prior_{window}d_rows", 0.0),
                "prior_roi": row.get(f"prior_{window}d_roi_proxy", np.nan),
            }
    return {"window_days": np.nan, "rows": 0.0, "roi": np.nan, "prior_rows": 0.0, "prior_roi": np.nan}


def _read_reconcile_metrics(paths: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    usecols = [
        "game_date",
        "prop_type",
        "model_pick_side",
        "model_pick_prob",
        "actual_over_outcome",
        "actual_under_outcome",
        "actual_model_pick_outcome",
        "pnl_model_pick_1u",
        "implied_over",
        "implied_under",
        "implied_over_novig",
        "implied_under_novig",
    ]
    daily_parts: list[pd.DataFrame] = []
    row_parts: list[pd.DataFrame] = []
    source_counts: dict[str, object] = {"input_files": [], "raw_rows": 0, "usable_rows": 0}

    for path in paths:
        if not path.exists():
            source_counts["input_files"].append({"path": str(path), "exists": False, "raw_rows": 0, "usable_rows": 0})
            continue
        raw_rows = 0
        usable_rows = 0
        for chunk in pd.read_csv(path, usecols=lambda c: c in usecols, chunksize=CHUNK_SIZE, low_memory=False):
            raw_rows += len(chunk)
            chunk = chunk.copy()
            if "prop_type" not in chunk or "actual_model_pick_outcome" not in chunk:
                continue
            chunk["prop_type"] = chunk["prop_type"].map(_norm_text)
            chunk = chunk[chunk["prop_type"].isin(ACTIVE_PROPS)]
            chunk["actual_model_pick_outcome"] = chunk["actual_model_pick_outcome"].map(_norm_text)
            chunk = chunk[chunk["actual_model_pick_outcome"].isin(WIN_LOSS_PUSH)]
            if chunk.empty:
                continue
            usable_rows += len(chunk)
            chunk["date"] = pd.to_datetime(chunk["game_date"], errors="coerce").dt.date
            chunk = chunk[chunk["date"].notna()]
            chunk["model_pick_side"] = chunk["model_pick_side"].map(_norm_text)
            chunk["wins"] = chunk["actual_model_pick_outcome"].eq("win").astype(int)
            chunk["losses"] = chunk["actual_model_pick_outcome"].eq("loss").astype(int)
            chunk["pushes"] = chunk["actual_model_pick_outcome"].eq("push").astype(int)
            chunk["pnl"] = pd.to_numeric(chunk["pnl_model_pick_1u"], errors="coerce").fillna(0.0)
            chunk["model_pick_prob"] = pd.to_numeric(chunk["model_pick_prob"], errors="coerce")
            over_imp = pd.to_numeric(chunk.get("implied_over_novig"), errors="coerce").fillna(
                pd.to_numeric(chunk.get("implied_over"), errors="coerce")
            )
            under_imp = pd.to_numeric(chunk.get("implied_under_novig"), errors="coerce").fillna(
                pd.to_numeric(chunk.get("implied_under"), errors="coerce")
            )
            chunk["selected_implied_probability"] = np.where(
                chunk["model_pick_side"].eq("over"),
                over_imp,
                np.where(chunk["model_pick_side"].eq("under"), under_imp, np.nan),
            )
            chunk["edge"] = chunk["model_pick_prob"] - chunk["selected_implied_probability"]
            chunk["false_over"] = (chunk["losses"].eq(1) & chunk["model_pick_side"].eq("over")).astype(int)
            chunk["false_under"] = (chunk["losses"].eq(1) & chunk["model_pick_side"].eq("under")).astype(int)
            chunk["side_outcome_mismatch"] = (
                np.where(
                    chunk["model_pick_side"].eq("over"),
                    chunk["actual_over_outcome"].map(_norm_text),
                    np.where(chunk["model_pick_side"].eq("under"), chunk["actual_under_outcome"].map(_norm_text), ""),
                )
                != chunk["actual_model_pick_outcome"]
            )
            daily_parts.append(
                chunk.groupby(["date", "prop_type"], as_index=False).agg(
                    rows=("prop_type", "size"),
                    wins=("wins", "sum"),
                    losses=("losses", "sum"),
                    pushes=("pushes", "sum"),
                    pnl=("pnl", "sum"),
                    avg_model_probability=("model_pick_prob", "mean"),
                    avg_implied_probability=("selected_implied_probability", "mean"),
                    avg_edge=("edge", "mean"),
                    false_over=("false_over", "sum"),
                    false_under=("false_under", "sum"),
                    side_outcome_mismatches=("side_outcome_mismatch", "sum"),
                )
            )
            row_parts.append(
                chunk[
                    [
                        "date",
                        "prop_type",
                        "wins",
                        "losses",
                        "pushes",
                        "pnl",
                        "false_over",
                        "false_under",
                        "side_outcome_mismatch",
                    ]
                ]
            )
        source_counts["raw_rows"] += raw_rows
        source_counts["usable_rows"] += usable_rows
        source_counts["input_files"].append(
            {"path": str(path), "exists": True, "raw_rows": raw_rows, "usable_rows": usable_rows}
        )

    if not daily_parts:
        raise RuntimeError("no usable historical reconcile rows found")

    daily = pd.concat(daily_parts, ignore_index=True)
    daily = daily.groupby(["date", "prop_type"], as_index=False).agg(
        rows=("rows", "sum"),
        wins=("wins", "sum"),
        losses=("losses", "sum"),
        pushes=("pushes", "sum"),
        pnl=("pnl", "sum"),
        avg_model_probability=("avg_model_probability", "mean"),
        avg_implied_probability=("avg_implied_probability", "mean"),
        avg_edge=("avg_edge", "mean"),
        false_over=("false_over", "sum"),
        false_under=("false_under", "sum"),
        side_outcome_mismatches=("side_outcome_mismatches", "sum"),
    )
    daily["model_pick_win_rate"] = daily["wins"] / (daily["wins"] + daily["losses"]).replace(0, np.nan)
    daily["roi_proxy"] = daily["pnl"] / daily["rows"].replace(0, np.nan)
    daily["prop_family"] = daily["prop_type"].map(PROP_FAMILY)

    rows = pd.concat(row_parts, ignore_index=True)
    return daily, rows, source_counts


def _build_rolling(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = pd.to_datetime(daily["date"]).min().date()
    end = pd.to_datetime(daily["date"]).max().date()
    dates = pd.date_range(start, end, freq="D").date
    grid = pd.MultiIndex.from_product([dates, ACTIVE_PROPS], names=["date", "prop_type"]).to_frame(index=False)
    merged = grid.merge(daily, on=["date", "prop_type"], how="left")
    merged["prop_family"] = merged["prop_type"].map(PROP_FAMILY)
    for col in ["rows", "wins", "losses", "pushes", "pnl", "false_over", "false_under", "side_outcome_mismatches"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    for col in ["roi_proxy", "model_pick_win_rate", "avg_model_probability", "avg_implied_probability", "avg_edge"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged["active_day"] = merged["rows"] > 0

    parts: list[pd.DataFrame] = []
    for prop, group in merged.groupby("prop_type", sort=False):
        group = group.sort_values("date").copy()
        active_roi = group["roi_proxy"].where(group["active_day"])
        group["roi_volatility_last5"] = active_roi.rolling(5, min_periods=2).std()
        for window in (3, 7, 14, 30):
            prefix = f"rolling_{window}d"
            group[f"{prefix}_rows"] = group["rows"].rolling(window, min_periods=1).sum()
            group[f"{prefix}_wins"] = group["wins"].rolling(window, min_periods=1).sum()
            group[f"{prefix}_losses"] = group["losses"].rolling(window, min_periods=1).sum()
            group[f"{prefix}_pnl"] = group["pnl"].rolling(window, min_periods=1).sum()
            group[f"{prefix}_roi_proxy"] = group[f"{prefix}_pnl"] / group[f"{prefix}_rows"].replace(0, np.nan)
            decided = group[f"{prefix}_wins"] + group[f"{prefix}_losses"]
            group[f"{prefix}_model_pick_win_rate"] = group[f"{prefix}_wins"] / decided.replace(0, np.nan)
            group[f"{prefix}_avg_model_probability"] = group["avg_model_probability"].rolling(window, min_periods=1).mean()
            group[f"{prefix}_avg_implied_probability"] = group["avg_implied_probability"].rolling(window, min_periods=1).mean()
            group[f"{prefix}_avg_edge"] = group["avg_edge"].rolling(window, min_periods=1).mean()
            group[f"{prefix}_false_over"] = group["false_over"].rolling(window, min_periods=1).sum()
            group[f"{prefix}_false_under"] = group["false_under"].rolling(window, min_periods=1).sum()
            group[f"prior_{window}d_rows"] = group[f"{prefix}_rows"].shift(window)
            group[f"prior_{window}d_wins"] = group[f"{prefix}_wins"].shift(window)
            group[f"prior_{window}d_losses"] = group[f"{prefix}_losses"].shift(window)
            group[f"prior_{window}d_pnl"] = group[f"{prefix}_pnl"].shift(window)
            group[f"prior_{window}d_roi_proxy"] = group[f"{prefix}_roi_proxy"].shift(window)
            prior_decided = group[f"prior_{window}d_wins"] + group[f"prior_{window}d_losses"]
            group[f"prior_{window}d_model_pick_win_rate"] = group[f"prior_{window}d_wins"] / prior_decided.replace(0, np.nan)
        parts.append(group)
    rolling = pd.concat(parts, ignore_index=True)
    latest_usable = (
        daily.groupby("prop_type", as_index=False)
        .agg(
            latest_usable_date=("date", "max"),
            total_usable_rows=("rows", "sum"),
        )
    )
    rolling = rolling.merge(latest_usable, on="prop_type", how="left")
    latest = rolling[rolling["date"].eq(end)].copy()
    return rolling, latest


def _read_execution_metrics(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    if "prop_type_norm" not in df or "date_norm" not in df:
        return pd.DataFrame(), pd.DataFrame()
    df = df.copy()
    df["prop_type"] = df["prop_type_norm"].map(_norm_text)
    df = df[df["prop_type"].isin(ACTIVE_PROPS)]
    if "matched_reconcile" in df.columns:
        df = df[df["matched_reconcile"].astype(str).str.lower().isin({"true", "1", "yes"})]
    df["date"] = pd.to_datetime(df["date_norm"], errors="coerce").dt.date
    df = df[df["date"].notna()]
    df["bet_result"] = df["bet_result"].map(_norm_text)
    df["wins"] = df["bet_result"].eq("win").astype(int)
    df["losses"] = df["bet_result"].eq("loss").astype(int)
    df["pushes"] = df["bet_result"].eq("push").astype(int)
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily = df.groupby(["date", "prop_type"], as_index=False).agg(
        bets=("prop_type", "size"),
        wins=("wins", "sum"),
        losses=("losses", "sum"),
        pushes=("pushes", "sum"),
        pnl=("pnl", "sum"),
    )
    daily["roi"] = daily["pnl"] / daily["bets"].replace(0, np.nan)
    daily["win_rate"] = daily["wins"] / (daily["wins"] + daily["losses"]).replace(0, np.nan)

    start = pd.to_datetime(daily["date"]).min().date()
    end = pd.to_datetime(daily["date"]).max().date()
    dates = pd.date_range(start, end, freq="D").date
    grid = pd.MultiIndex.from_product([dates, ACTIVE_PROPS], names=["date", "prop_type"]).to_frame(index=False)
    rolling = grid.merge(daily, on=["date", "prop_type"], how="left")
    for col in ["bets", "wins", "losses", "pushes", "pnl"]:
        rolling[col] = pd.to_numeric(rolling[col], errors="coerce").fillna(0.0)
    for prop, idx in rolling.groupby("prop_type").groups.items():
        group = rolling.loc[idx].sort_values("date")
        for window in (3, 7):
            rolling.loc[group.index, f"rolling_{window}d_bets"] = group["bets"].rolling(window, min_periods=1).sum()
            rolling.loc[group.index, f"rolling_{window}d_pnl"] = group["pnl"].rolling(window, min_periods=1).sum()
            rolling.loc[group.index, f"rolling_{window}d_roi"] = (
                rolling.loc[group.index, f"rolling_{window}d_pnl"]
                / rolling.loc[group.index, f"rolling_{window}d_bets"].replace(0, np.nan)
            )
    latest = rolling[rolling["date"].eq(end)].copy()
    overall = df.groupby("prop_type", as_index=False).agg(
        exec_bets=("prop_type", "size"),
        exec_wins=("wins", "sum"),
        exec_losses=("losses", "sum"),
        exec_pushes=("pushes", "sum"),
        exec_pnl=("pnl", "sum"),
    )
    return latest, overall


def _build_outputs(
    daily: pd.DataFrame,
    rolling_latest: pd.DataFrame,
    row_metrics: pd.DataFrame,
    execution_latest: pd.DataFrame,
    execution_overall: pd.DataFrame,
    source_counts: dict[str, object],
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    latest_date = pd.to_datetime(rolling_latest["date"]).max().date()
    overlap_start = pd.to_datetime("2026-03-27").date()
    overlap_end = min(latest_date, pd.to_datetime("2026-04-27").date())
    overlap = row_metrics[(row_metrics["date"] >= overlap_start) & (row_metrics["date"] <= overlap_end)].copy()
    db_overlap = overlap.groupby("prop_type", as_index=False).agg(
        db_rows=("prop_type", "size"),
        db_wins=("wins", "sum"),
        db_losses=("losses", "sum"),
        db_pushes=("pushes", "sum"),
        db_pnl=("pnl", "sum"),
        db_false_over=("false_over", "sum"),
        db_false_under=("false_under", "sum"),
        db_side_outcome_mismatches=("side_outcome_mismatch", "sum"),
    )
    db_overlap["db_decided"] = db_overlap["db_wins"] + db_overlap["db_losses"]
    db_overlap["db_win_rate"] = db_overlap["db_wins"] / db_overlap["db_decided"].replace(0, np.nan)
    db_overlap["db_roi"] = db_overlap["db_pnl"] / db_overlap["db_rows"].replace(0, np.nan)
    db_overlap["db_overlap_regime"] = db_overlap.apply(
        lambda r: _regime_from_roi(r["db_roi"], r["db_rows"], min_rows=25),
        axis=1,
    )

    all_props = pd.DataFrame({"prop_type": ACTIVE_PROPS})
    exec_overall = all_props.merge(execution_overall, on="prop_type", how="left")
    for col in ["exec_bets", "exec_wins", "exec_losses", "exec_pushes", "exec_pnl"]:
        exec_overall[col] = pd.to_numeric(exec_overall[col], errors="coerce").fillna(0.0)
    exec_overall["exec_decided"] = exec_overall["exec_wins"] + exec_overall["exec_losses"]
    exec_overall["exec_win_rate"] = exec_overall["exec_wins"] / exec_overall["exec_decided"].replace(0, np.nan)
    exec_overall["exec_roi"] = exec_overall["exec_pnl"] / exec_overall["exec_bets"].replace(0, np.nan)
    exec_overall["exec_overlap_regime"] = exec_overall.apply(
        lambda r: _execution_regime(r["exec_roi"], r["exec_bets"]),
        axis=1,
    )

    db_vs_exec = all_props.merge(db_overlap, on="prop_type", how="left").merge(exec_overall, on="prop_type", how="left")
    for col in ["db_rows", "db_wins", "db_losses", "db_pushes", "db_pnl", "db_false_over", "db_false_under", "db_decided"]:
        db_vs_exec[col] = pd.to_numeric(db_vs_exec[col], errors="coerce").fillna(0.0)
    db_vs_exec["db_overlap_regime"] = db_vs_exec["db_overlap_regime"].fillna("INSUFFICIENT")

    latest = all_props.merge(rolling_latest, on="prop_type", how="left")
    latest = latest.merge(db_vs_exec, on="prop_type", how="left", suffixes=("", "_overlap"))
    latest = latest.merge(
        execution_latest[
            [
                "prop_type",
                "rolling_3d_bets",
                "rolling_3d_roi",
                "rolling_7d_bets",
                "rolling_7d_roi",
            ]
        ]
        if not execution_latest.empty
        else pd.DataFrame(columns=["prop_type", "rolling_3d_bets", "rolling_3d_roi", "rolling_7d_bets", "rolling_7d_roi"]),
        on="prop_type",
        how="left",
    )

    combined_rows = []
    for _, row in latest.iterrows():
        prop = row["prop_type"]
        long_rows = row.get("rolling_30d_rows", 0.0)
        long_roi = row.get("rolling_30d_roi_proxy", np.nan)
        recent = _select_adaptive_window(row)
        user_exec_bets_7d = row.get("rolling_7d_bets", np.nan)
        user_exec_roi_7d = row.get("rolling_7d_roi", np.nan)
        user_exec_roi_3d = row.get("rolling_3d_roi", np.nan)
        long_regime = _regime_from_roi(long_roi, long_rows, min_rows=25)
        recent_regime = _regime_from_roi(recent["roi"], recent["rows"], min_rows=REGIME_MIN_ROWS)
        trend_metric_delta = (
            float(recent["roi"]) - float(recent["prior_roi"])
            if not pd.isna(recent["roi"]) and not pd.isna(recent["prior_roi"])
            else np.nan
        )
        trend_direction = _trend_direction(
            recent_roi=recent["roi"],
            prior_roi=recent["prior_roi"],
            recent_rows=recent["rows"],
            prior_rows=recent["prior_rows"],
        )
        user_execution_regime = _execution_regime(user_exec_roi_7d, user_exec_bets_7d, user_exec_roi_3d)
        combined_rows.append(
            {
                "prop_type": prop,
                "prop_family": PROP_FAMILY.get(prop, "other"),
                "latest_usable_date": row.get("latest_usable_date"),
                "total_usable_rows": row.get("total_usable_rows", 0.0),
                "long_term_regime": long_regime,
                "long_term_confidence": _sample_confidence(long_rows),
                "long_term_actionability": "monitor-only",
                "long_term_30d_rows": long_rows,
                "long_term_30d_roi": long_roi,
                "db_recent_7d_rows": row.get("rolling_7d_rows", 0.0),
                "db_recent_7d_roi": row.get("rolling_7d_roi_proxy", np.nan),
                "db_recent_14d_rows": row.get("rolling_14d_rows", 0.0),
                "db_recent_14d_roi": row.get("rolling_14d_roi_proxy", np.nan),
                "db_recent_30d_rows": row.get("rolling_30d_rows", 0.0),
                "db_recent_30d_roi": row.get("rolling_30d_roi_proxy", np.nan),
                "recent_window_days": recent["window_days"],
                "recent_window_rows": recent["rows"],
                "recent_window_roi": recent["roi"],
                "recent_regime": recent_regime,
                "recent_confidence": _sample_confidence(recent["rows"]),
                "trend_regime": trend_direction,
                "trend_direction": trend_direction,
                "trend_window_days": recent["window_days"],
                "trend_window_rows": recent["rows"],
                "trend_window_roi": recent["roi"],
                "trend_metric_recent": recent["roi"],
                "trend_metric_prior": recent["prior_roi"],
                "trend_metric_delta": trend_metric_delta,
                "trend_prior_window_days": recent["window_days"],
                "trend_prior_sample_rows": recent["prior_rows"],
                "execution_regime": trend_direction,
                "user_execution_regime": user_execution_regime,
                "confidence_score": (float(long_rows or 0) / 50.0) * min(1.0, abs(float(long_roi)) / 0.10)
                if not pd.isna(long_roi)
                else 0.0,
                "execution_7d_bets": user_exec_bets_7d,
                "execution_7d_roi": user_exec_roi_7d,
                "user_execution_7d_bets": user_exec_bets_7d,
                "user_execution_7d_roi": user_exec_roi_7d,
                "days_in_current_regime": np.nan,
                "regime_stability": row.get("roi_volatility_last5", np.nan),
                "db_rows": row.get("db_rows", 0.0),
                "db_win_rate": row.get("db_win_rate", np.nan),
                "db_roi": row.get("db_roi", np.nan),
                "exec_bets": row.get("exec_bets", 0.0),
                "exec_win_rate": row.get("exec_win_rate", np.nan),
                "exec_roi": row.get("exec_roi", np.nan),
                "user_exec_bets": row.get("exec_bets", 0.0),
                "user_exec_win_rate": row.get("exec_win_rate", np.nan),
                "user_exec_roi": row.get("exec_roi", np.nan),
                "db_overlap_regime": row.get("db_overlap_regime", "INSUFFICIENT"),
                "exec_overlap_regime": row.get("exec_overlap_regime", "INSUFFICIENT"),
                "user_exec_overlap_regime": row.get("exec_overlap_regime", "INSUFFICIENT"),
                "recommended_board_usage": _board_usage(long_regime, recent_regime, trend_direction),
            }
        )
    combined = add_context_fields(pd.DataFrame(combined_rows))
    freshness = combined[
        [
            "prop_type",
            "latest_usable_date",
            "total_usable_rows",
            "db_recent_7d_rows",
            "db_recent_7d_roi",
            "db_recent_14d_rows",
            "db_recent_14d_roi",
            "db_recent_30d_rows",
            "db_recent_30d_roi",
            "recent_window_days",
            "recent_window_rows",
            "recent_window_roi",
            "trend_window_days",
            "trend_window_rows",
            "trend_window_roi",
            "trend_metric_recent",
            "trend_metric_prior",
            "trend_metric_delta",
            "trend_prior_window_days",
            "trend_prior_sample_rows",
        ]
    ].copy()

    md_lines = [
        "# Prop Regime Validation: DB Monitor vs Execution Trend",
        "",
        f"Historical model-pick source rows loaded: `{source_counts['raw_rows']}` raw, `{source_counts['usable_rows']}` usable active-prop rows.",
        f"Latest historical model-pick date: `{latest_date}`",
        f"Overlap window: `{overlap_start}` to `{overlap_end}`",
        "",
        "## Source Files",
        "",
    ]
    for item in source_counts["input_files"]:
        md_lines.append(
            f"- `{item['path']}`: exists={item['exists']}, raw_rows={item['raw_rows']}, usable_rows={item['usable_rows']}"
        )
    md_lines.extend(
        [
            "",
            "## DB Monitor Logic Validation",
            "",
            "- model_pick_side selects the side for both outcome and odds: over uses over-side outcome/implied probability; under uses under-side outcome/implied probability.",
            "- actual_model_pick_outcome is used as the model-pick result; no over+under side symmetry is counted.",
            "- Each reconcile row contributes exactly one model-pick outcome and one model-pick PnL.",
            "",
            "## Source Freshness",
            "",
            _markdown_table(
                freshness,
                pct_cols={
                    "db_recent_7d_roi",
                    "db_recent_14d_roi",
                    "db_recent_30d_roi",
                    "recent_window_roi",
                    "trend_window_roi",
                    "trend_metric_recent",
                    "trend_metric_prior",
                    "trend_metric_delta",
                },
            ),
            "",
            "## Overlap Comparison",
            "",
            _markdown_table(
                db_vs_exec[
                    [
                        "prop_type",
                        "db_rows",
                        "db_win_rate",
                        "db_roi",
                        "db_overlap_regime",
                        "exec_bets",
                        "exec_win_rate",
                        "exec_roi",
                        "exec_overlap_regime",
                    ]
                ],
                pct_cols={"db_win_rate", "db_roi", "exec_win_rate", "exec_roi"},
            ),
            "",
            "## Combined Signal",
            "",
            _markdown_table(
                combined[
                    [
                        "prop_type",
                        "prop_family",
                        "latest_usable_date",
                        "long_term_regime",
                        "recent_regime",
                        "execution_regime",
                        "regime_context_label",
                        "long_term_30d_rows",
                        "long_term_30d_roi",
                        "recent_window_days",
                        "recent_window_rows",
                        "recent_window_roi",
                        "trend_window_days",
                        "trend_window_rows",
                        "trend_window_roi",
                        "trend_metric_prior",
                        "trend_metric_delta",
                        "trend_prior_sample_rows",
                        "db_recent_7d_roi",
                        "execution_7d_bets",
                        "execution_7d_roi",
                        "user_execution_regime",
                        "db_rows",
                        "db_roi",
                        "exec_bets",
                        "exec_roi",
                    ]
                ],
                pct_cols={
                    "long_term_30d_roi",
                    "recent_window_roi",
                    "trend_window_roi",
                    "trend_metric_prior",
                    "trend_metric_delta",
                    "db_recent_7d_roi",
                    "execution_7d_roi",
                    "db_roi",
                    "exec_roi",
                },
            ),
            "",
            "## Files",
            "",
            "- `prop_regime_validation.md`",
            "- `prop_regime_db_vs_execution.csv`",
            "- `prop_regime_combined_signal.csv`",
            "- `prop_regime_source_freshness.csv`",
            "- `prop_regime_context_validation_summary.csv`",
            "- `prop_regime_context_validation_summary.md`",
            "",
        ]
    )
    return db_vs_exec, combined, freshness, "\n".join(md_lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reconcile-csv", action="append", dest="reconcile_csvs")
    parser.add_argument("--execution-csv", default=str(DEFAULT_EXECUTION_CSV))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--deploy-csv", default=str(DEFAULT_DEPLOY_CSV))
    args = parser.parse_args(argv)

    reconcile_csvs = [Path(p) for p in args.reconcile_csvs] if args.reconcile_csvs else DEFAULT_RECONCILE_CSVS
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily, row_metrics, source_counts = _read_reconcile_metrics(reconcile_csvs)
    _rolling, rolling_latest = _build_rolling(daily)
    execution_latest, execution_overall = _read_execution_metrics(Path(args.execution_csv))
    db_vs_exec, combined, freshness, validation_md = _build_outputs(
        daily=daily,
        rolling_latest=rolling_latest,
        row_metrics=row_metrics,
        execution_latest=execution_latest,
        execution_overall=execution_overall,
        source_counts=source_counts,
    )

    db_vs_exec.to_csv(out_dir / "prop_regime_db_vs_execution.csv", index=False)
    combined.to_csv(out_dir / "prop_regime_combined_signal.csv", index=False)
    deploy_csv = Path(args.deploy_csv)
    deploy_csv.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(deploy_csv, index=False)
    freshness.to_csv(out_dir / "prop_regime_source_freshness.csv", index=False)
    (out_dir / "prop_regime_validation.md").write_text(validation_md, encoding="utf-8")

    summary_cols = [
        "prop_type",
        "latest_usable_date",
        "long_term_regime",
        "recent_regime",
        "execution_regime",
        "recent_window_days",
        "recent_window_rows",
        "trend_window_days",
        "trend_window_rows",
        "trend_prior_window_days",
        "trend_prior_sample_rows",
        "trend_metric_recent",
        "trend_metric_prior",
        "trend_metric_delta",
        "regime_context_score",
        "regime_context_label",
        "regime_context_explanation",
    ]
    summary = combined[summary_cols].copy()
    summary.to_csv(out_dir / "prop_regime_context_validation_summary.csv", index=False)
    summary_md = "\n".join(
        [
            "# Prop Regime Context Validation Summary",
            "",
            "User-facing labels describe signal environment only. They are not calls to action.",
            "",
            _markdown_table(summary),
            "",
        ]
    )
    (out_dir / "prop_regime_context_validation_summary.md").write_text(summary_md, encoding="utf-8")

    props = sorted(combined["prop_type"].astype(str).tolist())
    print(f"[prop-regime-validation] wrote {out_dir / 'prop_regime_combined_signal.csv'}")
    print(f"[prop-regime-validation] wrote {deploy_csv}")
    print(f"[prop-regime-validation] active props: {','.join(props)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
