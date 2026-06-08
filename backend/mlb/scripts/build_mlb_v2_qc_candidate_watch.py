#!/usr/bin/env python3
"""Build a visibility-only daily watch card for v2 / Quick Card candidate slices."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/v2_qc_diagnostics")
DEFAULT_V2_ROWS = Path("artifacts/analysis/mlb/v2_health_check/v2_health_check_selected_side_only_rows.csv")
DEFAULT_QC_ROWS = Path("artifacts/analysis/mlb/v2_health_check/quick_card_selected_side_deduped_rows.csv")
DEFAULT_RECONCILE_ROOT = Path("backend/mlb/exports/model_v2/reconcile")
DEFAULT_RANKING_DIAG_ROOT = Path("backend/mlb/exports/model_v2/upload")


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _num(value: Any) -> pd.Series:
    return pd.to_numeric(value, errors="coerce")


def _price_bucket(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "missing"
    if val >= 100:
        return "+100+"
    if -149 <= val <= -100:
        return "-100 to -149"
    if -199 <= val <= -150:
        return "-150 to -199"
    if val <= -200:
        return "-200 or shorter"
    return "other"


def _line_bucket(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "missing"
    if abs(float(val) - 0.5) < 1e-9:
        return "0.5"
    if abs(float(val) - 1.5) < 1e-9:
        return "1.5"
    if float(val) >= 2.5:
        return "2.5+"
    return f"{float(val):.1f}"


def _prob_bucket(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "missing"
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
    if val < 0.75:
        return "70-75"
    return "75+"


def _fmt_pct(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "n/a"
    return f"{val * 100:.1f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "n/a"
    return f"{val:.{digits}f}"


def _md_table(df: pd.DataFrame, cols: list[str], n: int | None = None) -> str:
    work = df[cols].copy()
    if n is not None:
        work = work.head(n)
    for col in work.columns:
        if col.startswith("roi") or col == "wr":
            work[col] = work[col].map(_fmt_pct)
        elif col in {
            "units",
            "avg_odds",
            "median_odds",
            "book_coverage",
            "best_day_units",
            "worst_day_units",
            "roi_without_best_day",
            "roi_without_best_2_days",
            "roi_without_worst_day",
            "last_7_roi",
            "last_14_roi",
            "prior_7_roi",
            "roi_trend_delta",
        }:
            if col.startswith("roi") or col.endswith("_roi") or col == "roi_trend_delta":
                work[col] = work[col].map(_fmt_pct)
            else:
                work[col] = work[col].map(lambda v: _fmt_num(v, 2))
    work = work.fillna("n/a").astype(str)
    lines = [
        "| " + " | ".join(work.columns) + " |",
        "| " + " | ".join(["---"] * len(work.columns)) + " |",
    ]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in work.columns) + " |")
    return "\n".join(lines)


def _latest_actual_wagers_date(root: Path) -> str:
    dates = []
    for path in root.glob("*/actual_wagers_by_source_*.csv"):
        dates.append(_date_key(path.parent.name))
    return max([d for d in dates if d], default="")


def _counts_by_date(rows: pd.DataFrame, date_col: str = "date") -> dict[str, int]:
    if rows.empty or date_col not in rows.columns:
        return {}
    dates = rows[date_col].map(_date_key)
    return {str(k): int(v) for k, v in dates[dates.ne("")].value_counts().sort_index().items()}


def _load_reconcile_source_rows(root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames = []
    files = []
    for path in sorted(root.glob("*/actual_wagers_by_source_*.csv")):
        try:
            df = pd.read_csv(path, low_memory=False)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["reconcile_source_file"] = str(path)
        df["reconcile_file_date"] = _date_key(path.parent.name)
        frames.append(df)
        files.append(str(path))
    if not frames:
        return pd.DataFrame(), {"files_loaded": [], "dates_found_by_source": {}}
    rows = pd.concat(frames, ignore_index=True, sort=False)
    dates_found = {}
    if "source_category" in rows.columns:
        for source, group in rows.groupby("source_category", dropna=False):
            dates_found[str(source)] = _counts_by_date(group)
    return rows, {"files_loaded": files, "dates_found_by_source": dates_found}


def _prepare_base_rows(raw: pd.DataFrame, source: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    diagnostics: dict[str, Any] = {
        "source": source,
        "rows_loaded_by_date": {},
        "rows_after_each_filter_by_date": {},
        "exclusion_counts_by_reason": {},
        "exclusion_counts_by_date": {},
    }
    if raw.empty:
        return raw.copy(), diagnostics
    out = raw.copy()
    out["source_category"] = source
    out["date"] = out["date"].map(_date_key)
    diagnostics["rows_loaded_by_date"] = _counts_by_date(raw)
    diagnostics["rows_after_each_filter_by_date"]["raw"] = diagnostics["rows_loaded_by_date"]

    valid_date = out["date"].ne("")
    diagnostics["exclusion_counts_by_reason"]["invalid_date"] = int((~valid_date).sum())
    out = out[valid_date].copy()
    diagnostics["rows_after_each_filter_by_date"]["valid_date"] = _counts_by_date(out)

    out["result_key"] = out.get("result_key", out.get("result", "")).astype(str).str.lower()
    resolved_result = out["result_key"].isin(["win", "loss", "push"])
    excluded_unresolved = out[~resolved_result].copy()
    diagnostics["exclusion_counts_by_reason"]["unresolved_result"] = int(len(excluded_unresolved))
    diagnostics["exclusion_counts_by_date"]["unresolved_result"] = _counts_by_date(excluded_unresolved)
    out = out[resolved_result].copy()
    diagnostics["rows_after_each_filter_by_date"]["resolved_result"] = _counts_by_date(out)

    out["price_num"] = pd.to_numeric(out.get("price_num", out.get("price", np.nan)), errors="coerce")
    out["units_num"] = pd.to_numeric(out.get("units_num", out.get("units", np.nan)), errors="coerce")
    out["side_key"] = out.get("side_key", out.get("side", "")).astype(str).str.lower().str.strip()
    out["line_num"] = pd.to_numeric(out.get("line", np.nan), errors="coerce")
    out["line_bucket"] = out["line_num"].map(_line_bucket)
    out["price_bucket"] = out["price_num"].map(_price_bucket)
    out["book_coverage"] = pd.to_numeric(out.get("book_count_two_sided", np.nan), errors="coerce")
    out["week"] = pd.to_datetime(out["date"], errors="coerce").dt.to_period("W-SUN").astype(str)
    env = out.get("environment_alignment", pd.Series("", index=out.index)).astype(str)
    out["hostile_split"] = np.where(
        env.eq("hostile_environment"),
        "hostile",
        np.where(env.eq("neutral_or_mixed"), "non_hostile", "missing"),
    )
    out["player_name_norm"] = out.get("player_name", "").astype(str).str.lower().str.strip()
    out["line_key"] = out["line_num"].round(3).astype(str)
    out["side_key2"] = out["side_key"]
    rows_before_dedup = int(len(out))
    if "wager_id" in out.columns:
        wager_key = out["wager_id"].astype(str).replace({"nan": "", "": ""})
        with_wager = out[wager_key.ne("")].drop_duplicates(["source_category", "wager_id"], keep="last")
        without_wager = out[wager_key.eq("")]
        keys = [
            col
            for col in ["source_category", "date", "player_id", "player_name_norm", "prop_type", "line_key", "side_key2", "price_num", "row_type"]
            if col in out.columns
        ]
        without_wager = without_wager.drop_duplicates(keys, keep="last")
        out = pd.concat([with_wager, without_wager], ignore_index=True, sort=False)
    diagnostics["exclusion_counts_by_reason"]["duplicate_dropped"] = int(rows_before_dedup - len(out))
    diagnostics["rows_after_each_filter_by_date"]["deduped"] = _counts_by_date(out)
    return out, diagnostics


def _load_static_base_rows(path: Path, source: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.exists():
        return pd.DataFrame(), {"source": source, "path": str(path), "missing": True}
    try:
        raw = pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), {"source": source, "path": str(path), "empty_file": True}
    rows, diagnostics = _prepare_base_rows(raw, source)
    diagnostics["path"] = str(path)
    return rows, diagnostics


def _load_ranking_probs(root: Path) -> pd.DataFrame:
    parts = []
    for path in sorted(root.glob("*/ranking_tool_upload_diagnostics_*.csv")):
        parent_date = path.parent.name
        expected_name = f"ranking_tool_upload_diagnostics_{parent_date}.csv"
        if path.name != expected_name:
            continue
        df = pd.read_csv(path, low_memory=False)
        if df.empty or "player_name" not in df.columns:
            continue
        df["date"] = df["date"].map(_date_key)
        df["player_name_norm"] = df["player_name"].astype(str).str.lower().str.strip()
        df["line_key"] = pd.to_numeric(df.get("line"), errors="coerce").round(3).astype(str)
        df["side_key2"] = df.get("side", "").astype(str).str.lower().str.strip()
        df["ranking_expected_prob"] = pd.to_numeric(df.get("empirical_win_pct", df.get("uploaded_win_value")), errors="coerce")
        parts.append(df[["date", "player_name_norm", "line_key", "side_key2", "ranking_expected_prob"]])
    if not parts:
        return pd.DataFrame(columns=["date", "player_name_norm", "line_key", "side_key2", "ranking_expected_prob"])
    return pd.concat(parts, ignore_index=True).drop_duplicates(["date", "player_name_norm", "line_key", "side_key2"], keep="last")


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(rows))
    wins = int(rows["result_key"].eq("win").sum()) if bets else 0
    losses = int(rows["result_key"].eq("loss").sum()) if bets else 0
    pushes = int(rows["result_key"].eq("push").sum()) if bets else 0
    decisions = wins + losses
    units = float(rows["units_num"].sum(skipna=True)) if bets else 0.0
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / decisions if decisions else np.nan,
        "roi": units / bets if bets else np.nan,
        "units": units,
        "avg_odds": float(rows["price_num"].mean(skipna=True)) if bets else np.nan,
        "median_odds": float(rows["price_num"].median(skipna=True)) if bets else np.nan,
        "book_coverage": float(rows["book_coverage"].mean(skipna=True)) if bets else np.nan,
    }


def _daily(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["date", "bets", "wins", "losses", "pushes", "wr", "roi", "units"])
    out = rows.groupby("date", dropna=False).apply(lambda g: pd.Series(_metrics(g)), include_groups=False).reset_index()
    out = out.sort_values("date").reset_index(drop=True)
    for col in ["bets", "wins", "losses", "pushes"]:
        out[f"cumulative_{col}"] = pd.to_numeric(out[col], errors="coerce").fillna(0).cumsum().astype(int)
    out["cumulative_units"] = pd.to_numeric(out["units"], errors="coerce").fillna(0).cumsum()
    out["cumulative_roi"] = out["cumulative_units"] / out["cumulative_bets"].replace(0, np.nan)
    out["cumulative_wr"] = out["cumulative_wins"] / (out["cumulative_wins"] + out["cumulative_losses"]).replace(0, np.nan)
    return out


def _max_drawdown(daily: pd.DataFrame) -> float:
    if daily.empty:
        return 0.0
    curve = pd.to_numeric(daily["units"], errors="coerce").fillna(0).cumsum()
    return float((curve.cummax() - curve).max(skipna=True))


def _candidate_summary(name: str, rows: pd.DataFrame, latest_date: str) -> dict[str, Any]:
    daily = _daily(rows)
    overall = _metrics(rows)
    last_date = pd.Timestamp(latest_date) if latest_date else pd.NaT
    last7 = rows[pd.to_datetime(rows["date"], errors="coerce").ge(last_date - pd.Timedelta(days=6))] if latest_date else rows.iloc[0:0]
    last14 = rows[pd.to_datetime(rows["date"], errors="coerce").ge(last_date - pd.Timedelta(days=13))] if latest_date else rows.iloc[0:0]
    prior7 = rows[
        pd.to_datetime(rows["date"], errors="coerce").between(last_date - pd.Timedelta(days=13), last_date - pd.Timedelta(days=7), inclusive="both")
    ] if latest_date else rows.iloc[0:0]
    best = daily.sort_values("units", ascending=False).head(1)
    worst = daily.sort_values("units", ascending=True).head(1)
    best_days = daily.sort_values("units", ascending=False).head(2)["date"].astype(str).tolist()
    best_day = str(best["date"].iloc[0]) if not best.empty else ""
    worst_day = str(worst["date"].iloc[0]) if not worst.empty else ""
    without_best = rows[~rows["date"].eq(best_day)] if best_day else rows
    without_best2 = rows[~rows["date"].isin(best_days)] if best_days else rows
    without_worst = rows[~rows["date"].eq(worst_day)] if worst_day else rows
    last7_m = _metrics(last7)
    prior7_m = _metrics(prior7)
    return {
        "candidate": name,
        **overall,
        "last_7_bets": last7_m["bets"],
        "last_7_roi": last7_m["roi"],
        "last_7_units": last7_m["units"],
        "last_14_bets": _metrics(last14)["bets"],
        "last_14_roi": _metrics(last14)["roi"],
        "last_14_units": _metrics(last14)["units"],
        "prior_7_roi": prior7_m["roi"],
        "roi_trend_delta": last7_m["roi"] - prior7_m["roi"] if pd.notna(last7_m["roi"]) and pd.notna(prior7_m["roi"]) else np.nan,
        "positive_roi_days": int(pd.to_numeric(daily["roi"], errors="coerce").gt(0).sum()) if not daily.empty else 0,
        "negative_roi_days": int(pd.to_numeric(daily["roi"], errors="coerce").lt(0).sum()) if not daily.empty else 0,
        "best_day": best_day,
        "best_day_units": float(best["units"].iloc[0]) if not best.empty else 0.0,
        "worst_day": worst_day,
        "worst_day_units": float(worst["units"].iloc[0]) if not worst.empty else 0.0,
        "roi_without_best_day": _metrics(without_best)["roi"],
        "roi_without_best_2_days": _metrics(without_best2)["roi"],
        "roi_without_worst_day": _metrics(without_worst)["roi"],
        "max_drawdown_units": _max_drawdown(daily),
    }


def _build_candidates(v2: pd.DataFrame, qc: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "quick_card under 0.5": qc[qc["side_key"].eq("under") & qc["line_bucket"].eq("0.5")].copy(),
        "quick_card 0.5 +100+": qc[qc["line_bucket"].eq("0.5") & qc["price_bucket"].eq("+100+")].copy(),
        "quick_card under 0.5 +100+": qc[
            qc["side_key"].eq("under") & qc["line_bucket"].eq("0.5") & qc["price_bucket"].eq("+100+")
        ].copy(),
        "quick_card +100+ all lines": qc[qc["price_bucket"].eq("+100+")].copy(),
        "v2 baseline": v2.copy(),
        "v2 excluding 65-70": v2[~v2["prob_bucket"].eq("65-70")].copy(),
        "v2 70%+": v2[v2["prob_bucket"].isin(["70-75", "75+"])].copy(),
        "v2 75%+": v2[v2["prob_bucket"].eq("75+")].copy(),
    }


def build_watch(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    reconcile_raw, reconcile_diag = _load_reconcile_source_rows(Path(args.reconcile_root))
    input_mode = "reconcile_actual_wagers"
    source_diagnostics: dict[str, Any] = {"reconcile": reconcile_diag}
    if reconcile_raw.empty or "source_category" not in reconcile_raw.columns:
        input_mode = "static_health_check_fallback"
        v2, v2_diag = _load_static_base_rows(Path(args.v2_rows), "v2_ranking")
        qc, qc_diag = _load_static_base_rows(Path(args.quick_card_rows), "quick_card")
    else:
        v2_raw = reconcile_raw[reconcile_raw["source_category"].astype(str).eq("v2_ranking")].copy()
        qc_raw = reconcile_raw[reconcile_raw["source_category"].astype(str).eq("quick_card")].copy()
        v2, v2_diag = _prepare_base_rows(v2_raw, "v2_ranking")
        qc, qc_diag = _prepare_base_rows(qc_raw, "quick_card")
    source_diagnostics["v2_ranking"] = v2_diag
    source_diagnostics["quick_card"] = qc_diag

    ranking_probs = _load_ranking_probs(Path(args.ranking_diag_root))
    if not v2.empty:
        v2 = v2.merge(ranking_probs, on=["date", "player_name_norm", "line_key", "side_key2"], how="left")
        direct_prob = pd.to_numeric(v2.get("upload_win_prob_for_row_side", pd.Series(np.nan, index=v2.index)), errors="coerce")
        v2["expected_prob"] = direct_prob.combine_first(pd.to_numeric(v2.get("ranking_expected_prob"), errors="coerce"))
        v2["prob_bucket"] = v2["expected_prob"].map(_prob_bucket)

    latest_reconcile = _latest_actual_wagers_date(Path(args.reconcile_root))
    source_candidate_dates = [
        d
        for source_diag in [v2_diag, qc_diag]
        for d in source_diag.get("rows_loaded_by_date", {}).keys()
        if d
    ]
    latest_candidate_before_filters = max(source_candidate_dates, default="")
    latest_included = max([d for d in pd.concat([v2.get("date", pd.Series(dtype=str)), qc.get("date", pd.Series(dtype=str))]).dropna().astype(str) if d], default="")
    stale = bool(latest_reconcile and latest_included and latest_included < latest_reconcile)

    candidates = _build_candidates(v2, qc)
    summary_rows = [_candidate_summary(name, rows, latest_included) for name, rows in candidates.items()]
    summary = pd.DataFrame(summary_rows)

    csv_rows = []
    for name, rows in candidates.items():
        daily = _daily(rows)
        for _, row in daily.iterrows():
            payload = row.to_dict()
            payload.update({"candidate": name, "scope": "daily", "period": row.get("date")})
            csv_rows.append(payload)
        for scope, frame in {
            "cumulative": rows,
            "last_7_days": rows[pd.to_datetime(rows["date"], errors="coerce").ge(pd.Timestamp(latest_included) - pd.Timedelta(days=6))] if latest_included else rows.iloc[0:0],
            "last_14_days": rows[pd.to_datetime(rows["date"], errors="coerce").ge(pd.Timestamp(latest_included) - pd.Timedelta(days=13))] if latest_included else rows.iloc[0:0],
        }.items():
            payload = _metrics(frame)
            payload.update({"candidate": name, "scope": scope, "period": latest_included})
            csv_rows.append(payload)
    watch_csv = pd.DataFrame(csv_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "daily_candidate_watch.csv"
    json_path = out_dir / "daily_candidate_watch.json"
    md_path = out_dir / "daily_candidate_watch.md"
    watch_csv.to_csv(csv_path, index=False)

    stable = summary[summary["roi_without_best_2_days"].notna()].sort_values(["roi_without_best_2_days", "bets"], ascending=[False, False]).head(1)
    dependent = summary.assign(outlier_delta=(summary["roi"] - summary["roi_without_best_2_days"]).abs()).sort_values("outlier_delta", ascending=False).head(1)
    improving = summary[summary["roi_trend_delta"].notna()].sort_values("roi_trend_delta", ascending=False).head(1)
    deteriorating = summary[summary["roi_trend_delta"].notna()].sort_values("roi_trend_delta", ascending=True).head(1)

    metadata = {
        "input_mode": input_mode,
        "latest_reconcile_date_found": latest_reconcile,
        "latest_candidate_date_before_filters": latest_candidate_before_filters,
        "latest_date_included": latest_included,
        "latest_candidate_date_after_filters": latest_included,
        "stale": stale,
        "dates_found_by_source": reconcile_diag.get("dates_found_by_source", {}),
        "rows_loaded_by_date": {
            "v2_ranking": v2_diag.get("rows_loaded_by_date", {}),
            "quick_card": qc_diag.get("rows_loaded_by_date", {}),
        },
        "rows_after_each_filter_by_date": {
            "v2_ranking": v2_diag.get("rows_after_each_filter_by_date", {}),
            "quick_card": qc_diag.get("rows_after_each_filter_by_date", {}),
        },
        "exclusion_counts_by_reason": {
            "v2_ranking": v2_diag.get("exclusion_counts_by_reason", {}),
            "quick_card": qc_diag.get("exclusion_counts_by_reason", {}),
        },
        "exclusion_counts_by_date": {
            "v2_ranking": v2_diag.get("exclusion_counts_by_date", {}),
            "quick_card": qc_diag.get("exclusion_counts_by_date", {}),
        },
        "source_diagnostics": source_diagnostics,
        "rows": {"v2": int(len(v2)), "quick_card": int(len(qc)), "watch_csv": int(len(watch_csv))},
        "outputs": {"csv": str(csv_path), "json": str(json_path), "md": str(md_path)},
        "most_stable_candidate": stable.iloc[0].to_dict() if not stable.empty else {},
        "most_outlier_dependent_candidate": dependent.iloc[0].to_dict() if not dependent.empty else {},
        "most_improving_candidate": improving.iloc[0].to_dict() if not improving.empty else {},
        "most_deteriorating_candidate": deteriorating.iloc[0].to_dict() if not deteriorating.empty else {},
        "candidates": summary.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")

    lines = [
        "# Daily Candidate Watch",
        "",
        "Visibility only. No production rules, thresholds, uploads, model outputs, or selection logic changed.",
        "",
        f"- Latest reconcile date found: `{latest_reconcile or 'none'}`",
        f"- Input mode: `{input_mode}`",
        f"- Latest candidate date before filters: `{latest_candidate_before_filters or 'none'}`",
        f"- Latest date included: `{latest_included or 'none'}`",
        f"- Stale: `{str(stale).lower()}`",
        f"- V2 rows by latest filter: `{v2_diag.get('rows_after_each_filter_by_date', {}).get('deduped', {})}`",
        f"- Quick Card rows by latest filter: `{qc_diag.get('rows_after_each_filter_by_date', {}).get('deduped', {})}`",
        "",
        "## Candidate Summary",
        "",
        _md_table(
            summary,
            [
                "candidate",
                "bets",
                "wins",
                "losses",
                "pushes",
                "wr",
                "roi",
                "units",
                "last_7_bets",
                "last_7_roi",
                "last_14_bets",
                "last_14_roi",
                "roi_without_best_2_days",
                "max_drawdown_units",
            ],
        ),
        "",
        "## Stability Answers",
        "",
        f"- Most stable: `{stable.iloc[0]['candidate'] if not stable.empty else 'n/a'}` by ROI with best two days removed.",
        f"- Most outlier-dependent: `{dependent.iloc[0]['candidate'] if not dependent.empty else 'n/a'}` by largest ROI drop when best two days are removed.",
        f"- Most improving: `{improving.iloc[0]['candidate'] if not improving.empty else 'n/a'}` by last-7 ROI minus prior-7 ROI.",
        f"- Most deteriorating: `{deteriorating.iloc[0]['candidate'] if not deteriorating.empty else 'n/a'}` by last-7 ROI minus prior-7 ROI.",
        "",
        "## Daily Rows",
        "",
        _md_table(watch_csv[watch_csv["scope"].eq("daily")].sort_values(["candidate", "period"]), ["candidate", "period", "bets", "wins", "losses", "pushes", "wr", "roi", "units", "cumulative_roi"], 80),
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily v2 / Quick Card candidate watch artifacts.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--v2-rows", default=str(DEFAULT_V2_ROWS))
    parser.add_argument("--quick-card-rows", default=str(DEFAULT_QC_ROWS))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--ranking-diag-root", default=str(DEFAULT_RANKING_DIAG_ROOT))
    return parser.parse_args()


def main() -> int:
    metadata = build_watch(parse_args())
    outputs = metadata.get("outputs", {})
    print(f"Wrote {outputs.get('csv')}")
    print(f"Wrote {outputs.get('json')}")
    print(f"Wrote {outputs.get('md')}")
    print(
        "freshness "
        f"latest_reconcile={metadata.get('latest_reconcile_date_found') or 'none'} "
        f"latest_candidate_before_filters={metadata.get('latest_candidate_date_before_filters') or 'none'} "
        f"latest_included={metadata.get('latest_date_included') or 'none'} "
        f"stale={metadata.get('stale')} "
        f"input_mode={metadata.get('input_mode')}"
    )
    print(
        "filter_counts "
        f"v2={json.dumps(metadata.get('exclusion_counts_by_reason', {}).get('v2_ranking', {}), sort_keys=True)} "
        f"quick_card={json.dumps(metadata.get('exclusion_counts_by_reason', {}).get('quick_card', {}), sort_keys=True)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
