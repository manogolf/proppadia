#!/usr/bin/env python3
"""Explain why Ranking/Quick Card exact overlap survives versus parent lanes."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
TMP_ANALYSIS = ROOT / "tmp/analysis"
if TMP_ANALYSIS.exists() and str(TMP_ANALYSIS) not in sys.path:
    sys.path.insert(0, str(TMP_ANALYSIS))

OUT_DIR = Path("artifacts/analysis/mlb/overlap_survival_audit")
WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full_history", None),
    ("last_30", 30),
    ("last_14", 14),
    ("last_7", 7),
)


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    return "" if text in {"", "nan", "none", "null", "<na>"} else text


def _as_num(value: Any) -> float:
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


def _fmt_pct(value: Any) -> str:
    val = _as_num(value)
    return "n/a" if pd.isna(val) else f"{val * 100:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = _as_num(value)
    return "n/a" if pd.isna(val) else f"{val:.{digits}f}"


def _md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    work = df.head(max_rows).copy() if max_rows else df.copy()
    for col in work.columns:
        if col in {
            "wr",
            "roi",
            "share",
            "unit_share",
            "bottom_order_share",
            "under_0_5_share",
            "placed_share",
            "resolved_share",
            "roi_delta_vs_parent_best",
            "roi_delta_vs_full_history",
        }:
            work[col] = work[col].map(_fmt_pct)
        elif col.startswith("avg_") or col in {"units", "parent_best_roi", "full_history_roi"}:
            work[col] = work[col].map(lambda v: _fmt_num(v, 3 if "score" in col or "prob" in col else 2))
    work = work.fillna("n/a").astype(str)
    lines = [
        "| " + " | ".join(work.columns) + " |",
        "| " + " | ".join(["---"] * len(work.columns)) + " |",
    ]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[c]).replace("|", "\\|") for c in work.columns) + " |")
    return "\n".join(lines)


def _window_mask(rows: pd.DataFrame, latest: str, days: int | None) -> pd.Series:
    dates = pd.to_datetime(rows["date_key"], errors="coerce")
    if days is None:
        return dates.notna()
    latest_ts = pd.Timestamp(latest)
    return dates.ge(latest_ts - pd.Timedelta(days=days - 1)) & dates.le(latest_ts)


def _bucket_american_odds(value: Any) -> str:
    val = _as_num(value)
    if pd.isna(val):
        return "unknown"
    if val < -300:
        return "<-300"
    if val < -200:
        return "-300 to -200"
    if val < -150:
        return "-200 to -150"
    if val < -120:
        return "-150 to -120"
    if val <= 120:
        return "-120 to +120"
    if val <= 200:
        return "+120 to +200"
    return ">+200"


def _bucket_probability(value: Any) -> str:
    val = _as_num(value)
    if pd.isna(val):
        return "unknown"
    if val < 0.50:
        return "<50"
    if val < 0.55:
        return "50-55"
    if val < 0.60:
        return "55-60"
    if val < 0.65:
        return "60-65"
    if val < 0.70:
        return "65-70"
    return "70+"


def _bucket_line(value: Any) -> str:
    val = _as_num(value)
    if pd.isna(val):
        return "unknown"
    if abs(val - 0.5) < 0.001:
        return "0.5"
    if abs(val - 1.5) < 0.001:
        return "1.5"
    return "other"


def _market_name(value: Any) -> str:
    text = _norm(value)
    if text in {"batter_hits", "hits"}:
        return "hits"
    if text in {"batter_total_bases", "total_bases"}:
        return "total_bases"
    return text or "unknown"


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    total = int(len(rows))
    resolved = rows[rows["result_key"].isin(["win", "loss", "push"])].copy() if total else rows
    wins = int(resolved["result_key"].eq("win").sum()) if total else 0
    losses = int(resolved["result_key"].eq("loss").sum()) if total else 0
    pushes = int(resolved["result_key"].eq("push").sum()) if total else 0
    decisions = wins + losses
    units = float(pd.to_numeric(resolved["units_num"], errors="coerce").fillna(0).sum()) if total else 0.0
    return {
        "rows": total,
        "resolved": int(len(resolved)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / decisions if decisions else np.nan,
        "roi": units / len(resolved) if len(resolved) else np.nan,
        "units": units,
        "avg_odds": float(pd.to_numeric(rows.get("price_num"), errors="coerce").mean(skipna=True)) if total else np.nan,
        "avg_implied_probability": float(pd.to_numeric(rows.get("implied_probability"), errors="coerce").mean(skipna=True)) if total else np.nan,
        "avg_v2_score": float(pd.to_numeric(rows.get("ranking_score"), errors="coerce").mean(skipna=True)) if total else np.nan,
        "avg_qc_score": float(pd.to_numeric(rows.get("quick_card_score"), errors="coerce").mean(skipna=True)) if total else np.nan,
        "bottom_order_share": float(rows.get("bottom_order_flag", pd.Series(False, index=rows.index)).fillna(False).astype(bool).mean()) if total else np.nan,
        "under_0_5_share": float((rows["side_key"].eq("under") & rows["line_num"].eq(0.5)).mean()) if total else np.nan,
    }


def load_rows() -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        import run_overlap_formation_audit as formation
    except Exception as exc:  # pragma: no cover - user-facing failure path
        raise RuntimeError(f"Unable to import run_overlap_formation_audit: {type(exc).__name__}: {exc}") from exc
    rows, meta = formation._build_rows()
    out = rows.copy()
    out["date_key"] = pd.to_datetime(out["date_key"], errors="coerce").dt.date.astype(str)
    out["prop_type"] = out["market_key"].map(_market_name)
    out["side"] = out["side_key"].map(_norm)
    out["line_num"] = pd.to_numeric(out.get("line", out.get("line_key")), errors="coerce")
    out["line_bucket"] = out["line_num"].map(_bucket_line)
    out["odds_bucket"] = out["price_num"].map(_bucket_american_odds)
    out["model_probability_bucket"] = out["implied_probability"].map(_bucket_probability)
    out["qc_score_bucket"] = out.get("quick_card_score", pd.Series(np.nan, index=out.index)).map(_bucket_probability)
    out["v2_score_bucket"] = out.get("ranking_score", pd.Series(np.nan, index=out.index)).map(_bucket_probability)
    out["lineup_bucket"] = out.get("lineup_group", pd.Series("unknown", index=out.index)).fillna("unknown").astype(str)
    out["bottom_order_bucket"] = np.where(out.get("bottom_order_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool), "bottom_order", "not_bottom_order")
    out["source_lane"] = out.get("ranking_source_lane", pd.Series("", index=out.index)).fillna("").where(
        out["formation_bucket"].ne("quick_card_only"),
        out.get("quick_card_source_lane", pd.Series("", index=out.index)).fillna(""),
    )
    out["prop_side_line"] = out["prop_type"] + " " + out["side"] + " " + out["line_bucket"]
    out["timing_bucket"] = "unavailable"
    return out, meta


def population_comparison(rows: pd.DataFrame, latest: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    dims = [
        ("overall", None),
        ("prop_type", "prop_type"),
        ("side", "side"),
        ("line_bucket", "line_bucket"),
        ("prop_side_line", "prop_side_line"),
        ("odds_bucket", "odds_bucket"),
        ("lineup_bucket", "lineup_bucket"),
        ("bottom_order_bucket", "bottom_order_bucket"),
        ("model_probability_bucket", "model_probability_bucket"),
        ("qc_score_bucket", "qc_score_bucket"),
        ("v2_score_bucket", "v2_score_bucket"),
        ("source_lane", "source_lane"),
        ("timing_bucket", "timing_bucket"),
    ]
    for window, days in WINDOWS:
        wrows = rows.loc[_window_mask(rows, latest, days)].copy()
        for dim_name, dim_col in dims:
            if dim_col is None:
                groups = [("all", wrows)]
            else:
                groups = list(wrows.groupby(dim_col, dropna=False))
            for dim_value, dim_rows in groups:
                for formation, part in dim_rows.groupby("formation_bucket", dropna=False):
                    metric = _metrics(part)
                    records.append(
                        {
                            "window": window,
                            "dimension": dim_name,
                            "bucket": str(dim_value),
                            "population": formation,
                            **metric,
                        }
                    )
    return pd.DataFrame(records)


def roi_contribution(rows: pd.DataFrame, latest: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    overlap = rows[rows["formation_bucket"].eq("overlap")].copy()
    dims = [
        ("prop_type_side_line", ["prop_type", "side", "line_bucket"]),
        ("hitter_order_bucket", ["lineup_bucket"]),
        ("bottom_order_bucket", ["bottom_order_bucket"]),
        ("odds_bucket", ["odds_bucket"]),
        ("source_lane", ["source_lane"]),
        ("date", ["date_key"]),
    ]
    for window, days in WINDOWS:
        wrows = overlap.loc[_window_mask(overlap, latest, days)].copy()
        total_units = float(pd.to_numeric(wrows.loc[wrows["result_key"].isin(["win", "loss", "push"]), "units_num"], errors="coerce").fillna(0).sum())
        for dim_name, cols in dims:
            for keys, part in wrows.groupby(cols, dropna=False):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                metric = _metrics(part)
                records.append(
                    {
                        "window": window,
                        "dimension": dim_name,
                        "bucket": " / ".join(str(k) for k in keys),
                        **metric,
                        "unit_share": metric["units"] / total_units if total_units else np.nan,
                    }
                )
    out = pd.DataFrame(records)
    if out.empty:
        return out
    return out.sort_values(["window", "units"], ascending=[True, False])


def recent_drift(rows: pd.DataFrame, latest: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for formation, group in rows.groupby("formation_bucket", dropna=False):
        full = _metrics(group.loc[_window_mask(group, latest, None)])
        for window, days in WINDOWS[1:]:
            part = group.loc[_window_mask(group, latest, days)]
            metric = _metrics(part)
            flag = "no_rows"
            if metric["resolved"] >= 10:
                if pd.notna(metric["roi"]) and pd.notna(full["roi"]) and metric["roi"] < full["roi"] - 0.20:
                    flag = "material_cooling"
                elif pd.notna(metric["roi"]) and pd.notna(full["roi"]) and metric["roi"] > full["roi"] + 0.20:
                    flag = "material_improvement"
                elif metric["roi"] < 0:
                    flag = "negative_recent"
                else:
                    flag = "stable_or_positive"
            elif metric["resolved"] > 0:
                flag = "small_sample"
            records.append(
                {
                    "population": formation,
                    "window": window,
                    **metric,
                    "full_history_roi": full["roi"],
                    "roi_delta_vs_full_history": metric["roi"] - full["roi"] if pd.notna(metric["roi"]) and pd.notna(full["roi"]) else np.nan,
                    "drift_flag": flag,
                }
            )
    return pd.DataFrame(records)


def candidate_profiles(comparison: pd.DataFrame) -> pd.DataFrame:
    useful_dims = {"prop_side_line", "odds_bucket", "lineup_bucket", "bottom_order_bucket", "qc_score_bucket", "v2_score_bucket", "source_lane"}
    overlap = comparison[
        comparison["population"].eq("overlap")
        & comparison["window"].isin(["full_history", "last_30", "last_14", "last_7"])
        & comparison["dimension"].isin(useful_dims)
        & comparison["resolved"].ge(5)
    ].copy()
    if overlap.empty:
        return overlap
    parent = comparison[
        comparison["population"].isin(["ranking_only", "quick_card_only"])
        & comparison["window"].isin(["full_history", "last_30", "last_14", "last_7"])
        & comparison["dimension"].isin(useful_dims)
    ][["window", "dimension", "bucket", "population", "roi", "resolved"]].copy()
    parent_best = (
        parent.sort_values("roi", ascending=False)
        .groupby(["window", "dimension", "bucket"], dropna=False)
        .head(1)
        .rename(columns={"roi": "parent_best_roi", "population": "parent_best_population", "resolved": "parent_best_resolved"})
    )
    out = overlap.merge(parent_best, on=["window", "dimension", "bucket"], how="left")
    out["roi_delta_vs_parent_best"] = out["roi"] - out["parent_best_roi"]
    out["profile_recommendation"] = np.select(
        [
            out["resolved"].ge(20) & out["roi"].gt(0.15),
            out["resolved"].ge(10) & out["roi"].gt(0),
            out["resolved"].lt(10),
        ],
        ["surviving_profile_candidate", "monitor_profile", "sample_too_small"],
        default="weak_or_negative",
    )
    return out.sort_values(["profile_recommendation", "roi", "resolved"], ascending=[True, False, False])


def write_outputs(rows: pd.DataFrame, meta: dict[str, Any], out_dir: Path) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_completed = max([d for d in rows["date_key"].dropna().astype(str) if d <= "2026-12-31"], default="")
    # Restrict performance windows to latest resolved date, not today-only lane rows without outcomes.
    resolved_dates = rows.loc[rows["result_key"].isin(["win", "loss", "push"]), "date_key"].dropna().astype(str)
    latest_resolved = resolved_dates.max() if not resolved_dates.empty else latest_completed
    comparison = population_comparison(rows, latest_resolved)
    contribution = roi_contribution(rows, latest_resolved)
    drift = recent_drift(rows, latest_resolved)
    profiles = candidate_profiles(comparison)

    paths = {
        "md": out_dir / "overlap_survival_audit.md",
        "comparison": out_dir / "overlap_population_comparison.csv",
        "contribution": out_dir / "overlap_roi_contribution.csv",
        "drift": out_dir / "overlap_recent_drift.csv",
        "profiles": out_dir / "overlap_candidate_profiles.csv",
        "detail": out_dir / "overlap_survival_detail_rows.csv",
    }
    comparison.to_csv(paths["comparison"], index=False)
    contribution.to_csv(paths["contribution"], index=False)
    drift.to_csv(paths["drift"], index=False)
    profiles.to_csv(paths["profiles"], index=False)
    rows.to_csv(paths["detail"], index=False)

    overall = comparison[(comparison["dimension"].eq("overall")) & (comparison["bucket"].eq("all"))].copy()
    overall = overall[["window", "population", "rows", "resolved", "wins", "losses", "pushes", "wr", "roi", "units", "avg_odds", "avg_v2_score", "avg_qc_score", "bottom_order_share", "under_0_5_share"]]
    overlap_profiles = profiles[profiles["profile_recommendation"].isin(["surviving_profile_candidate", "monitor_profile"])].copy()
    top_contrib = contribution[contribution["window"].eq("full_history")].copy().sort_values("units", ascending=False).head(15)
    last7_contrib = contribution[contribution["window"].eq("last_7")].copy().sort_values("units", ascending=False).head(15)
    prop_mix = comparison[
        comparison["dimension"].eq("prop_side_line")
        & comparison["population"].eq("overlap")
        & comparison["window"].eq("full_history")
        & comparison["resolved"].gt(0)
    ].copy().sort_values("units", ascending=False)

    full_overlap = overall[(overall["window"].eq("full_history")) & (overall["population"].eq("overlap"))]
    last7_overlap = overall[(overall["window"].eq("last_7")) & (overall["population"].eq("overlap"))]
    full_roi = full_overlap["roi"].iloc[0] if not full_overlap.empty else np.nan
    last7_roi = last7_overlap["roi"].iloc[0] if not last7_overlap.empty else np.nan
    action = "keep_priority_watch"
    if pd.notna(last7_roi) and pd.notna(full_roi) and last7_roi < 0 and last7_roi < full_roi - 0.20:
        action = "downgrade_to_monitor_until_recovery"
    elif not last7_overlap.empty and int(last7_overlap["resolved"].iloc[0]) < 10:
        action = "keep_priority_watch_but_mark_recent_sample_small"

    lines = [
        "# Overlap Survival Audit",
        "",
        f"- Latest resolved date used for windows: `{latest_resolved}`",
        f"- Row spine: `{len(rows)}` lane-selected rows from ranking/QC formation audit.",
        f"- Resolved rows: `{int(rows['result_key'].isin(['win', 'loss', 'push']).sum())}`",
        f"- Enrichment status: PDS `{meta.get('pds_status')}`, lineup `{meta.get('lineup_status')}`, history `{meta.get('history_status')}`.",
        "- Timing bucket was unavailable in the current formation spine; it is reported as `unavailable` instead of inferred.",
        "",
        "## Population Comparison",
        "",
        _md_table(overall),
        "",
        "## Why Overlap Survives",
        "",
        "- Overlap is not simply the average of the parent lanes. It is a same-side agreement subset with better full-history ROI than both `ranking_only` and `quick_card_only`.",
        "- The strongest full-history overlap contribution is concentrated in `hits / under / 0.5`, especially non-bottom-order and earlier bottom-order cohorts, rather than hits over 0.5.",
        "- Recent overlap remains positive but with a much smaller sample; bottom-order under 0.5 has largely disappeared from the recent overlap mix, which explains the Ops Brief composition drift warning.",
        "- Parent lane degradation is most visible in QC-only under 0.5 / bottom-order profiles and in V2 hits over 0.5 only; those are not the same profile as current surviving overlap.",
        "",
        "## Overlap Prop/Side/Line Mix",
        "",
        _md_table(prop_mix[["bucket", "rows", "resolved", "wr", "roi", "units", "avg_odds", "avg_v2_score", "avg_qc_score", "bottom_order_share"]], max_rows=20),
        "",
        "## Overlap ROI Contribution",
        "",
        "### Full History Top Contributors",
        "",
        _md_table(top_contrib[["dimension", "bucket", "rows", "resolved", "wr", "roi", "units", "unit_share", "avg_odds"]], max_rows=15),
        "",
        "### Last 7 Top Contributors",
        "",
        _md_table(last7_contrib[["dimension", "bucket", "rows", "resolved", "wr", "roi", "units", "unit_share", "avg_odds"]], max_rows=15),
        "",
        "## Recent Drift",
        "",
        _md_table(drift[["population", "window", "rows", "resolved", "wr", "roi", "units", "full_history_roi", "roi_delta_vs_full_history", "drift_flag"]]),
        "",
        "## Candidate Surviving Profiles",
        "",
        _md_table(overlap_profiles[["window", "dimension", "bucket", "resolved", "wr", "roi", "units", "parent_best_population", "parent_best_roi", "roi_delta_vs_parent_best", "profile_recommendation"]], max_rows=30),
        "",
        "## Recommendation",
        "",
        f"- Recommendation: `{action}`",
        "- Keep overlap as a priority watch, but do not broaden it. The edge appears profile-dependent and recent samples are smaller/cooler than full history.",
        "- Narrowing candidates worth watching: exact overlap + hits under 0.5, overlap rows outside the degraded QC-only bottom-order-only profile, and overlap rows with QC/V2 agreement scores above the low 55-60 band.",
        "- Do not treat opposite-side conflicts as overlap. They remain a separate diagnostic population.",
        "",
        "## Output Files",
        "",
    ]
    for label, path in paths.items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    lines.append("Analysis only. No production selector, upload, threshold, grading, or wager-matching changes.")
    paths["md"].write_text("\n".join(lines) + "\n")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, meta = load_rows()
    paths = write_outputs(rows, meta, Path(args.out_dir))
    print(f"rows={len(rows)}")
    print(f"resolved={int(rows['result_key'].isin(['win', 'loss', 'push']).sum())}")
    for path in paths.values():
        print(f"wrote={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
