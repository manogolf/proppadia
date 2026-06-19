#!/usr/bin/env python3
"""Audit outcome-backed V2/QC hits over 0.5 performance."""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


POPULATIONS = (
    "v2_hits_o05_only",
    "qc_hits_o05_only",
    "overlap_hits_o05",
)
WINDOWS = (
    ("full_history", None),
    ("last_30", 30),
    ("last_14", 14),
    ("last_7", 7),
)
KEY_COLS = ["date", "player_id", "prop_type", "line", "side"]
OUTPUT_COLUMNS = [
    "date",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "price",
    "result",
    "units",
    "v2_present",
    "qc_present",
    "population",
    "placed_available",
    "placed_source_category",
    "placed_price",
    "placed_result",
    "placed_units",
    "source_lane_v2",
    "source_lane_qc",
    "rank_score_v2",
    "rank_score_qc",
    "time_of_day_bucket",
    "game_day_of_week",
]


@dataclass(frozen=True)
class Inputs:
    lanes_root: Path
    execution_root: Path
    actual_reconcile_root: Path
    output_dir: Path


def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _as_date(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value)[:10]


def _norm_side(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _norm_prop(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _norm_line(value: object) -> float:
    try:
        return float(value)
    except Exception:
        return math.nan


def _truthy(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(bool)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    lowered = series.astype(str).str.strip().str.lower()
    return lowered.isin({"1", "true", "t", "yes", "y"})


def _date_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and len(p.name) == 10)


def _load_lane_file(date_dir: Path, base_name: str) -> Path | None:
    dated = date_dir.name
    canonical = date_dir / f"{base_name}.csv"
    if canonical.exists():
        return canonical
    timestamped = sorted(date_dir.glob(f"{base_name}__*.csv"))
    return timestamped[-1] if timestamped else None


def _filter_hits_over_05(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    for col in KEY_COLS:
        if col not in work.columns:
            work[col] = pd.NA
    work["date"] = work["date"].map(_as_date)
    work["prop_type"] = work["prop_type"].map(_norm_prop)
    work["side"] = work["side"].map(_norm_side)
    work["line"] = work["line"].map(_norm_line)
    mask = (
        (work["prop_type"] == "hits")
        & (work["side"] == "over")
        & (work["line"].round(3) == 0.5)
    )
    if "selected_flag" in work.columns:
        mask &= _truthy(work["selected_flag"])
    return work.loc[mask].copy()


def _dedupe_candidates(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=KEY_COLS)
    keep = KEY_COLS.copy()
    for col in [
        "player_name",
        "player",
        "team",
        "opponent",
        "source_lane",
        "rank_score",
        "score",
        "time_of_day_bucket",
        "game_day_of_week",
    ]:
        if col in df.columns and col not in keep:
            keep.append(col)
    out = df[keep].copy()
    if "player_name" not in out.columns and "player" in out.columns:
        out["player_name"] = out["player"]
    if "rank_score" not in out.columns and "score" in out.columns:
        out["rank_score"] = out["score"]
    rename = {
        col: f"{col}_{suffix}"
        for col in ["source_lane", "rank_score", "score"]
        if col in out.columns
    }
    out = out.rename(columns=rename)
    return out.drop_duplicates(KEY_COLS, keep="last")


def load_lane_candidates(lanes_root: Path, kind: str) -> pd.DataFrame:
    stem = (
        "hits_lane_selector_{date}_ranking_upload_input"
        if kind == "v2"
        else "quick_card_hits_{date}"
    )
    frames: list[pd.DataFrame] = []
    for date_dir in _date_dirs(lanes_root):
        base_name = stem.format(date=date_dir.name)
        path = _load_lane_file(date_dir, base_name)
        if not path:
            continue
        df = _read_csv(path, low_memory=False)
        filtered = _filter_hits_over_05(df)
        if filtered.empty:
            continue
        filtered["lane_source_file"] = str(path)
        frames.append(filtered)
    if not frames:
        return pd.DataFrame(columns=KEY_COLS)
    return _dedupe_candidates(pd.concat(frames, ignore_index=True), kind)


def load_outcomes(execution_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(execution_root.glob("*/reconcile_rows.csv")):
        df = _read_csv(path, low_memory=False)
        if df.empty:
            continue
        date_col = "slate_date" if "slate_date" in df.columns else "game_date"
        required = {date_col, "player_id", "player_name", "prop_type", "line", "actual_over_outcome", "pnl_over_1u"}
        if not required.issubset(df.columns):
            continue
        work = df.copy()
        work["date"] = work[date_col].map(_as_date)
        work["prop_type"] = work["prop_type"].map(_norm_prop)
        work["side"] = "over"
        work["line"] = work["line"].map(_norm_line)
        mask = (
            (work["prop_type"] == "hits")
            & (work["line"].round(3) == 0.5)
            & work["actual_over_outcome"].notna()
        )
        work = work.loc[mask].copy()
        if work.empty:
            continue
        price_col = "price_over_american" if "price_over_american" in work.columns else "market_price_over"
        work["price"] = pd.to_numeric(work.get(price_col), errors="coerce")
        work["result"] = work["actual_over_outcome"].astype(str).str.lower()
        work["units"] = pd.to_numeric(work["pnl_over_1u"], errors="coerce")
        for col in ["team", "opponent", "time_of_day_bucket", "game_day_of_week"]:
            if col not in work.columns:
                work[col] = pd.NA
        frames.append(
            work[
                [
                    "date",
                    "player_id",
                    "player_name",
                    "team",
                    "opponent",
                    "prop_type",
                    "line",
                    "side",
                    "price",
                    "result",
                    "units",
                    "time_of_day_bucket",
                    "game_day_of_week",
                ]
            ]
        )
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    return out.drop_duplicates(KEY_COLS, keep="last")


def load_placed(actual_reconcile_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(actual_reconcile_root.glob("*/actual_wagers_by_source_*.csv")):
        df = _read_csv(path, low_memory=False)
        if df.empty:
            continue
        required = {"row_type", "date", "player_id", "prop_type", "line", "side"}
        if not required.issubset(df.columns):
            continue
        work = df.copy()
        work = work[work["row_type"].astype(str).str.lower().eq("actual_wager")].copy()
        work["date"] = work["date"].map(_as_date)
        work["prop_type"] = work["prop_type"].map(_norm_prop)
        work["side"] = work["side"].map(_norm_side)
        work["line"] = work["line"].map(_norm_line)
        work["player_id"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
        mask = (
            (work["prop_type"] == "hits")
            & (work["side"] == "over")
            & (work["line"].round(3) == 0.5)
            & work["player_id"].notna()
        )
        work = work.loc[mask].copy()
        if work.empty:
            continue
        for col in ["source_category", "price", "result", "units"]:
            if col not in work.columns:
                work[col] = pd.NA
        work["placed_price"] = pd.to_numeric(work["price"], errors="coerce")
        work["placed_result"] = work["result"].astype(str).str.lower()
        work["placed_units"] = pd.to_numeric(work["units"], errors="coerce")
        work["placed_available"] = True
        frames.append(
            work[
                KEY_COLS
                + [
                    "placed_available",
                    "source_category",
                    "placed_price",
                    "placed_result",
                    "placed_units",
                ]
            ].rename(columns={"source_category": "placed_source_category"})
        )
    if not frames:
        return pd.DataFrame(columns=KEY_COLS + ["placed_available"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(KEY_COLS, keep="last")


def build_rows(inputs: Inputs) -> pd.DataFrame:
    outcomes = load_outcomes(inputs.execution_root)
    v2 = load_lane_candidates(inputs.lanes_root, "v2")
    qc = load_lane_candidates(inputs.lanes_root, "qc")
    placed = load_placed(inputs.actual_reconcile_root)

    lane = outcomes.merge(v2, on=KEY_COLS, how="left", suffixes=("", "_v2ctx"))
    lane = lane.merge(qc, on=KEY_COLS, how="left", suffixes=("", "_qcctx"))
    lane["v2_present"] = lane[[c for c in lane.columns if c == "source_lane_v2"]].notna().any(axis=1)
    lane["qc_present"] = lane[[c for c in lane.columns if c == "source_lane_qc"]].notna().any(axis=1)
    lane = lane[lane["v2_present"] | lane["qc_present"]].copy()

    def classify(row: pd.Series) -> str:
        if bool(row["v2_present"]) and bool(row["qc_present"]):
            return "overlap_hits_o05"
        if bool(row["v2_present"]):
            return "v2_hits_o05_only"
        return "qc_hits_o05_only"

    lane["population"] = lane.apply(classify, axis=1)
    if not placed.empty:
        lane = lane.merge(placed, on=KEY_COLS, how="left")
    else:
        lane["placed_available"] = False
    lane["placed_available"] = lane["placed_available"].eq(True)
    for col in OUTPUT_COLUMNS:
        if col not in lane.columns:
            lane[col] = pd.NA
    return lane[OUTPUT_COLUMNS].copy()


def _window_mask(df: pd.DataFrame, days: int | None, latest_date: pd.Timestamp) -> pd.Series:
    dates = pd.to_datetime(df["date"], errors="coerce")
    if days is None:
        return dates.notna()
    start = latest_date - pd.Timedelta(days=days - 1)
    return (dates >= start) & (dates <= latest_date)


def _metrics(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {
            "bets": 0,
            "resolved": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "wr": math.nan,
            "roi": math.nan,
            "units": 0.0,
            "avg_odds": math.nan,
        }
    result = df["result"].astype(str).str.lower()
    wins = int(result.eq("win").sum())
    losses = int(result.eq("loss").sum())
    pushes = int(result.eq("push").sum())
    resolved = wins + losses + pushes
    decisions = wins + losses
    units = float(pd.to_numeric(df["units"], errors="coerce").fillna(0).sum())
    return {
        "bets": int(len(df)),
        "resolved": int(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / decisions if decisions else math.nan,
        "roi": units / resolved if resolved else math.nan,
        "units": units,
        "avg_odds": float(pd.to_numeric(df["price"], errors="coerce").mean()),
    }


def _drift_flag(full_roi: float, recent_roi: float, recent_resolved: int) -> str:
    if recent_resolved == 0:
        return "no_recent_rows"
    if recent_resolved < 10:
        return "small_sample"
    if pd.isna(full_roi) or pd.isna(recent_roi):
        return "unknown"
    if recent_roi < 0 and recent_roi < full_roi - 0.10:
        return "negative_recent_drift"
    if recent_roi < full_roi - 0.10:
        return "cooling"
    if recent_roi > full_roi + 0.15:
        return "improving"
    return "stable"


def summarize(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame()
    latest_date = pd.to_datetime(rows["date"], errors="coerce").max()
    full_roi_by_pop: dict[str, float] = {}
    last7_by_pop: dict[str, dict[str, object]] = {}
    records: list[dict[str, object]] = []
    for pop in POPULATIONS:
        pop_df = rows[rows["population"].eq(pop)].copy()
        full_roi_by_pop[pop] = _metrics(pop_df)["roi"]
        last7_mask = _window_mask(pop_df, 7, latest_date) if not pop_df.empty else pd.Series([], dtype=bool)
        last7_by_pop[pop] = _metrics(pop_df.loc[last7_mask]) if not pop_df.empty else _metrics(pop_df)

    for pop in POPULATIONS:
        pop_df = rows[rows["population"].eq(pop)].copy()
        for window, days in WINDOWS:
            win_df = pop_df.loc[_window_mask(pop_df, days, latest_date)].copy() if not pop_df.empty else pop_df
            for placed_status, part in (
                ("all", win_df),
                ("placed", win_df[win_df["placed_available"]]),
                ("unplaced", win_df[~win_df["placed_available"]]),
            ):
                metrics = _metrics(part)
                latest_qualifying = part["date"].max() if not part.empty else ""
                latest_rows = int(part["date"].eq(latest_date.strftime("%Y-%m-%d")).sum()) if not part.empty else 0
                record = {
                    "population": pop,
                    "window": window,
                    "placed_status": placed_status,
                    **metrics,
                    "latest_qualifying_date": latest_qualifying,
                    "latest_completed_slate": latest_date.strftime("%Y-%m-%d"),
                    "latest_completed_slate_rows": latest_rows,
                    "placed_rows": int(part["placed_available"].sum()) if not part.empty else 0,
                    "unplaced_rows": int((~part["placed_available"]).sum()) if not part.empty else 0,
                    "sample_warning": "small_sample" if metrics["resolved"] and metrics["resolved"] < 20 else ("no_rows" if metrics["resolved"] == 0 else "ok"),
                }
                if placed_status == "all":
                    l7 = last7_by_pop[pop]
                    record["drift_flag"] = _drift_flag(full_roi_by_pop[pop], l7["roi"], int(l7["resolved"]))
                else:
                    record["drift_flag"] = ""
                records.append(record)
    return pd.DataFrame(records)


def _fmt_pct(value: object) -> str:
    try:
        if pd.isna(value):
            return "n/a"
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "n/a"


def _fmt_num(value: object, digits: int = 2) -> str:
    try:
        if pd.isna(value):
            return "n/a"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _load_user_o15_watch() -> pd.DataFrame:
    path = Path("artifacts/analysis/mlb/user_over_15_filter_watch.csv")
    if not path.exists():
        return pd.DataFrame()
    df = _read_csv(path)
    if df.empty:
        return df
    cols = {c.lower(): c for c in df.columns}
    if "group" in cols:
        return df[df[cols["group"]].astype(str).str.contains("user|proxy", case=False, na=False)].copy()
    if "segment" in cols:
        return df[df[cols["segment"]].astype(str).str.contains("user|proxy", case=False, na=False)].copy()
    if "population" in cols:
        return df[df[cols["population"]].astype(str).str.contains("user|proxy", case=False, na=False)].copy()
    return df


def _composition_table(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame()
    latest_date = pd.to_datetime(rows["date"], errors="coerce").max()
    records: list[dict[str, object]] = []
    for pop in POPULATIONS:
        pop_df = rows[rows["population"].eq(pop)].copy()
        for window, days in WINDOWS:
            part = pop_df.loc[_window_mask(pop_df, days, latest_date)].copy() if not pop_df.empty else pop_df
            if part.empty:
                records.append(
                    {
                        "population": pop,
                        "window": window,
                        "rows": 0,
                        "placed_share": math.nan,
                        "avg_odds": math.nan,
                        "avg_v2_score": math.nan,
                        "avg_qc_score": math.nan,
                        "late_share": math.nan,
                        "evening_share": math.nan,
                    }
                )
                continue
            tod = part["time_of_day_bucket"].astype(str).str.lower()
            records.append(
                {
                    "population": pop,
                    "window": window,
                    "rows": int(len(part)),
                    "placed_share": float(part["placed_available"].mean()),
                    "avg_odds": float(pd.to_numeric(part["price"], errors="coerce").mean()),
                    "avg_v2_score": float(pd.to_numeric(part["rank_score_v2"], errors="coerce").mean()),
                    "avg_qc_score": float(pd.to_numeric(part["rank_score_qc"], errors="coerce").mean()),
                    "late_share": float(tod.eq("late").mean()),
                    "evening_share": float(tod.eq("evening").mean()),
                }
            )
    return pd.DataFrame(records)


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    work = df.fillna("n/a").astype(str)
    headers = list(work.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("\n", " ") for col in headers) + " |")
    return "\n".join(lines)


def write_report(rows: pd.DataFrame, summary: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "hits_o05_v2_summary.csv"
    report_path = output_dir / "hits_o05_v2_audit.md"
    detail_path = output_dir / "hits_o05_v2_detail_rows.csv"
    summary.to_csv(summary_path, index=False)
    rows.to_csv(detail_path, index=False)

    latest = rows["date"].max() if not rows.empty else "n/a"
    lines: list[str] = [
        "# V2 Hits Over 0.5 Audit",
        "",
        f"- Latest completed slate in audit: `{latest}`",
        f"- Outcome-backed qualifying rows: `{len(rows)}`",
        "- Populations are reconstructed from lane outputs and joined to execution-vs-model outcomes by date/player/prop/line/side.",
        "- `V2 only` and `QC only` exclude exact V2/QC agreement; `overlap` requires both V2 and QC membership for the same hits over 0.5 row.",
        "- Placed splits use actual-wager reconcile matches where a parsed row is available; unplaced rows use full-slate execution reconcile pricing/outcomes.",
        "",
        "## Summary",
        "",
    ]

    view = summary[summary["placed_status"].eq("all")].copy()
    view = view[["population", "window", "bets", "resolved", "wr", "roi", "units", "avg_odds", "latest_qualifying_date", "latest_completed_slate_rows", "drift_flag"]]
    if not view.empty:
        display = view.copy()
        display["wr"] = display["wr"].map(_fmt_pct)
        display["roi"] = display["roi"].map(_fmt_pct)
        display["units"] = display["units"].map(lambda v: _fmt_num(v, 2))
        display["avg_odds"] = display["avg_odds"].map(lambda v: _fmt_num(v, 1))
        lines.append(_markdown_table(display))
    else:
        lines.append("_No qualifying outcome-backed hits over 0.5 lane rows found._")

    lines.extend(["", "## Placed vs Unplaced", ""])
    placed_view = summary[summary["placed_status"].ne("all")].copy()
    if not placed_view.empty:
        placed_view = placed_view[
            ["population", "window", "placed_status", "bets", "resolved", "wr", "roi", "units", "avg_odds", "latest_qualifying_date"]
        ]
        placed_view["wr"] = placed_view["wr"].map(_fmt_pct)
        placed_view["roi"] = placed_view["roi"].map(_fmt_pct)
        placed_view["units"] = placed_view["units"].map(lambda v: _fmt_num(v, 2))
        placed_view["avg_odds"] = placed_view["avg_odds"].map(lambda v: _fmt_num(v, 1))
        lines.append(_markdown_table(placed_view))
    else:
        lines.append("_No placed/unplaced split available._")

    latest_rows = rows[rows["date"].eq(latest)].copy() if latest != "n/a" else pd.DataFrame()
    lines.extend(["", "## Latest Slate Composition", ""])
    if not latest_rows.empty:
        comp = latest_rows.groupby("population", dropna=False).agg(
            rows=("date", "size"),
            placed_rows=("placed_available", "sum"),
            avg_odds=("price", "mean"),
            avg_v2_score=("rank_score_v2", "mean"),
            avg_qc_score=("rank_score_qc", "mean"),
        ).reset_index()
        comp["avg_odds"] = comp["avg_odds"].map(lambda v: _fmt_num(v, 1))
        comp["avg_v2_score"] = comp["avg_v2_score"].map(lambda v: _fmt_num(v, 4))
        comp["avg_qc_score"] = comp["avg_qc_score"].map(lambda v: _fmt_num(v, 4))
        lines.append(_markdown_table(comp))
    else:
        lines.append("_No latest-slate qualifying rows._")

    lines.extend(["", "## Composition Changes", ""])
    composition = _composition_table(rows)
    if not composition.empty:
        comp_view = composition[composition["window"].isin(["full_history", "last_30", "last_14", "last_7"])].copy()
        comp_view["placed_share"] = comp_view["placed_share"].map(_fmt_pct)
        comp_view["late_share"] = comp_view["late_share"].map(_fmt_pct)
        comp_view["evening_share"] = comp_view["evening_share"].map(_fmt_pct)
        comp_view["avg_odds"] = comp_view["avg_odds"].map(lambda v: _fmt_num(v, 1))
        comp_view["avg_v2_score"] = comp_view["avg_v2_score"].map(lambda v: _fmt_num(v, 4))
        comp_view["avg_qc_score"] = comp_view["avg_qc_score"].map(lambda v: _fmt_num(v, 4))
        lines.append(_markdown_table(comp_view))
    else:
        lines.append("_No composition rows available._")

    user_watch = _load_user_o15_watch()
    lines.extend(["", "## User Over 1.5 Watch Reference", ""])
    if not user_watch.empty:
        keep = [c for c in user_watch.columns if c in {"group", "segment", "population", "window", "rows", "bets", "resolved", "wr", "roi", "units", "recommendation"}]
        if keep:
            ref = user_watch[keep].head(12).copy()
            for col in ["wr", "roi"]:
                if col in ref.columns:
                    ref[col] = ref[col].map(_fmt_pct)
            if "units" in ref.columns:
                ref["units"] = ref["units"].map(lambda v: _fmt_num(v, 2))
            lines.append(_markdown_table(ref))
        else:
            lines.append(f"- Watch artifact present: `artifacts/analysis/mlb/user_over_15_filter_watch.csv`")
    else:
        lines.append("- User over 1.5 watch artifact was not available for comparison.")

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This audit is descriptive only. It does not change model logic, thresholds, ranking/QC selection, upload generation, grading, wager matching, or overlap logic.",
            f"- Detail rows: `{detail_path}`",
            f"- Summary CSV: `{summary_path}`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")
    return report_path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes/today")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--actual-reconcile-root", default="backend/mlb/exports/model_v2/reconcile")
    ap.add_argument("--output-dir", default="artifacts/analysis/mlb/hits_o05_v2_audit")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    inputs = Inputs(
        lanes_root=Path(args.lanes_root),
        execution_root=Path(args.execution_root),
        actual_reconcile_root=Path(args.actual_reconcile_root),
        output_dir=Path(args.output_dir),
    )
    rows = build_rows(inputs)
    summary = summarize(rows)
    report_path = write_report(rows, summary, inputs.output_dir)
    latest = rows["date"].max() if not rows.empty else "n/a"
    print(f"hits_o05_v2_audit_rows={len(rows)}")
    print(f"latest_completed_slate={latest}")
    if not summary.empty:
        all_rows = summary[(summary["window"] == "last_7") & (summary["placed_status"] == "all")]
        for _, row in all_rows.iterrows():
            print(
                f"{row['population']} last_7 resolved={row['resolved']} "
                f"wr={_fmt_pct(row['wr'])} roi={_fmt_pct(row['roi'])} units={_fmt_num(row['units'])}"
            )
    print(f"report={report_path}")
    print(f"summary={inputs.output_dir / 'hits_o05_v2_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
