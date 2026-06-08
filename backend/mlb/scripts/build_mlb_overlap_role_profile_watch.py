#!/usr/bin/env python3
"""Build visibility-only overlap role profile watch artifacts."""

from __future__ import annotations

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import build_mlb_ranking_qc_overlap_watch as overlap_watch

DEFAULT_LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
DEFAULT_RECON_ROOT = Path("backend/mlb/exports/model_v2/reconcile")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/v2_qc_diagnostics")
DEFAULT_LINEUP_CONTEXT_DIR = Path("artifacts/analysis/mlb/research_gap_analysis")
LINEUP_CONTEXT_GLOB = "lineup_context_diagnostics_20??-??-??_20??-??-??.csv"
LOW_D15_AB_THRESHOLD = 3.0
LOW_D15_PA_THRESHOLD = 3.0


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(dt) else pd.Timestamp(dt).date().isoformat()


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    return "" if text in {"", "nan", "none", "null", "<na>"} else text


def _id_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return str(int(val)) if pd.notna(val) else ""


def _line_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "" if pd.isna(val) else f"{float(val):.3f}".rstrip("0").rstrip(".")


def _fmt_pct(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "n/a" if pd.isna(val) else f"{val * 100:.1f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "n/a" if pd.isna(val) else f"{val:.{digits}f}"


def _md_table(df: pd.DataFrame, cols: list[str], n: int | None = None) -> str:
    if df.empty:
        return "No rows."
    work = df[cols].copy()
    if n is not None:
        work = work.head(n)
    for col in work.columns:
        if col in {"wr", "roi", "last_7_roi", "last_14_roi", "roi_without_best_day", "roi_without_best_2_days"}:
            work[col] = work[col].map(_fmt_pct)
        elif col in {"units", "avg_odds", "best_day_units", "worst_day_units", "last_7_units", "last_14_units"}:
            work[col] = work[col].map(_fmt_num)
    work = work.fillna("n/a").astype(str)
    lines = ["| " + " | ".join(work.columns) + " |", "| " + " | ".join(["---"] * len(work.columns)) + " |"]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in work.columns) + " |")
    return "\n".join(lines)


def _db_url() -> str:
    return os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or ""


def _load_perf(recon_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(recon_root.glob("*/actual_wagers_by_source_*.csv")):
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue
        df = df[df.get("source_category", pd.Series("", index=df.index)).isin(["v2_ranking", "quick_card"])].copy()
        if df.empty:
            continue
        df["date"] = df["date"].map(_date_key)
        df["date_key"] = df["date"]
        df["player_id_key"] = df.get("player_id", pd.Series("", index=df.index)).map(_id_key)
        df["player_name_key"] = df.get("player_name", pd.Series("", index=df.index)).map(_norm)
        df["player_key"] = df["player_id_key"].where(df["player_id_key"].ne(""), df["player_name_key"])
        df["market_key"] = df.get("prop_type", pd.Series("", index=df.index)).map(_norm)
        df["line_key"] = df.get("line", pd.Series("", index=df.index)).map(_line_key)
        df["side_key"] = df.get("side", pd.Series("", index=df.index)).map(_norm)
        df["result_key"] = df.get("result", pd.Series("", index=df.index)).map(_norm)
        df["units_num"] = pd.to_numeric(df.get("units"), errors="coerce")
        df["price_num"] = pd.to_numeric(df.get("price"), errors="coerce")
        df["game_id_key"] = df.get("game_id", pd.Series("", index=df.index)).map(_id_key)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out[out["result_key"].isin(["win", "loss", "push"])].copy()
    keys = ["source_category", "date_key", "player_key", "market_key", "line_key", "side_key"]
    return out.drop_duplicates(keys, keep="last")


def _selection_perf(perf: pd.DataFrame, lane_root: Path) -> pd.DataFrame:
    if perf.empty:
        return pd.DataFrame()
    ranking, quick = overlap_watch.load_selections(lane_root)
    ranking, quick = overlap_watch.classify_overlap(ranking, quick)
    keys = ["source_category", "date_key", "player_key", "market_key", "line_key", "side_key"]
    r = ranking[ranking.get("exact_overlap", pd.Series(False, index=ranking.index))].copy()
    q = quick[quick.get("exact_overlap", pd.Series(False, index=quick.index))].copy()
    r["source_category"] = "v2_ranking"
    q["source_category"] = "quick_card"
    base = pd.concat([r[keys], q[keys]], ignore_index=True, sort=False).drop_duplicates(keys)
    joined = base.merge(perf, on=keys, how="inner", suffixes=("", "_perf"))
    return joined[joined["result_key"].isin(["win", "loss", "push"])].copy()


def _load_pds(rows: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    url = _db_url()
    if not url:
        return pd.DataFrame(), "db_url_missing"
    try:
        from sqlalchemy import create_engine, text
    except Exception as exc:  # pragma: no cover
        return pd.DataFrame(), f"sqlalchemy_unavailable:{type(exc).__name__}"
    player_ids = sorted({int(v) for v in pd.to_numeric(rows.get("player_id"), errors="coerce").dropna().unique()})
    dates = sorted({d for d in rows.get("date_key", pd.Series(dtype=str)).dropna().astype(str) if d})
    if not player_ids or not dates:
        return pd.DataFrame(), "missing_player_ids_or_dates"
    start_date = min(dates)
    end_date = max(dates)
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            pds = pd.read_sql(
                text(
                    """
                    SELECT
                      player_id,
                      game_id,
                      game_date,
                      d7_plate_appearances,
                      d15_plate_appearances,
                      d30_plate_appearances,
                      d15_at_bats
                    FROM mlb.player_derived_stats
                    WHERE player_id = ANY(:player_ids)
                      AND game_date BETWEEN :start_date AND :end_date
                    """
                ),
                conn,
                params={"player_ids": player_ids, "start_date": start_date, "end_date": end_date},
            )
    except Exception as exc:
        return pd.DataFrame(), f"db_query_failed:{type(exc).__name__}:{exc}"
    if pds.empty:
        return pds, "db_query_empty"
    pds["date_key"] = pds["game_date"].map(_date_key)
    pds["player_id_key"] = pds["player_id"].map(_id_key)
    pds["game_id_key"] = pds["game_id"].map(_id_key)
    return pds.drop_duplicates(["date_key", "player_id_key", "game_id_key"], keep="last"), "db_query_ok"


def _join_pds(rows: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    pds, status = _load_pds(rows)
    out = rows.copy()
    if not pds.empty:
        out = out.merge(
            pds[
                [
                    "date_key",
                    "player_id_key",
                    "game_id_key",
                    "d15_at_bats",
                    "d7_plate_appearances",
                    "d15_plate_appearances",
                    "d30_plate_appearances",
                ]
            ],
            on=["date_key", "player_id_key", "game_id_key"],
            how="left",
        )
        missing = out["d15_at_bats"].isna()
        if missing.any():
            fallback = pds.drop_duplicates(["date_key", "player_id_key"], keep="last")[
                ["date_key", "player_id_key", "d15_at_bats", "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"]
            ]
            out = out.merge(fallback, on=["date_key", "player_id_key"], how="left", suffixes=("", "_fallback"))
            for col in ["d15_at_bats", "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"]:
                out[col] = out[col].combine_first(out[f"{col}_fallback"])
            out = out.drop(columns=[c for c in out.columns if c.endswith("_fallback")])
    for col in ["d15_at_bats", "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"]:
        if col not in out.columns:
            out[col] = np.nan
    return out, status


def _latest_lineup_context(root: Path) -> Path | None:
    candidates = sorted(root.glob(LINEUP_CONTEXT_GLOB), key=lambda p: (p.stat().st_mtime, p.name))
    return candidates[-1] if candidates else None


def _join_lineup_context(rows: pd.DataFrame, root: Path) -> tuple[pd.DataFrame, str, str]:
    path = _latest_lineup_context(root)
    out = rows.copy()
    if path is None:
        return out, "lineup_context_missing", ""
    ctx = pd.read_csv(path, low_memory=False)
    if ctx.empty:
        return out, "lineup_context_empty", str(path)
    ctx["date_key"] = ctx.get("game_date", pd.Series("", index=ctx.index)).map(_date_key)
    ctx["game_id_key"] = ctx.get("game_id", pd.Series("", index=ctx.index)).map(_id_key)
    ctx["player_id_key"] = ctx.get("player_id", pd.Series("", index=ctx.index)).map(_id_key)
    keep = ["date_key", "game_id_key", "player_id_key", "lineup_slot", "lineup_group", "started_flag", "actual_ab"]
    ctx = ctx[[c for c in keep if c in ctx.columns]].drop_duplicates(["date_key", "game_id_key", "player_id_key"], keep="last")
    out = out.merge(ctx, on=["date_key", "game_id_key", "player_id_key"], how="left")
    return out, "lineup_context_joined", str(path)


def _load_player_stats_history(rows: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    url = _db_url()
    if not url:
        return pd.DataFrame(), "db_url_missing"
    try:
        from sqlalchemy import create_engine, text
    except Exception as exc:  # pragma: no cover
        return pd.DataFrame(), f"sqlalchemy_unavailable:{type(exc).__name__}"
    player_ids = sorted({int(v) for v in pd.to_numeric(rows.get("player_id"), errors="coerce").dropna().unique()})
    dates = pd.to_datetime(rows["date_key"], errors="coerce").dropna()
    if not player_ids or dates.empty:
        return pd.DataFrame(), "missing_player_ids_or_dates"
    start = (dates.min().date() - timedelta(days=45)).isoformat()
    end = dates.max().date().isoformat()
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            hist = pd.read_sql(
                text(
                    """
                    SELECT player_id, game_date, position, at_bats
                    FROM mlb.player_stats
                    WHERE player_id = ANY(:player_ids)
                      AND game_date BETWEEN :start_date AND :end_date
                      AND COALESCE(position, '') <> 'P'
                    """
                ),
                conn,
                params={"player_ids": player_ids, "start_date": start, "end_date": end},
            )
    except Exception as exc:
        return pd.DataFrame(), f"db_query_failed:{type(exc).__name__}:{exc}"
    if hist.empty:
        return hist, "db_query_empty"
    hist["game_date"] = pd.to_datetime(hist["game_date"], errors="coerce")
    hist["player_id_key"] = hist["player_id"].map(_id_key)
    hist["at_bats"] = pd.to_numeric(hist["at_bats"], errors="coerce").fillna(0)
    hist["start_proxy"] = hist["at_bats"].ge(3)
    hist["pinch_hit_proxy"] = hist["at_bats"].between(0, 1, inclusive="both")
    return hist.dropna(subset=["game_date"]).sort_values(["player_id_key", "game_date"]), "db_query_ok"


def _add_role_features(rows: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    hist, status = _load_player_stats_history(rows)
    out = rows.copy()
    for col in ["appearances15", "appearances30", "ab15", "ab30", "starts_proxy15", "starts_proxy30", "start_proxy_rate15", "start_proxy_rate30", "pinch_hit_proxy_rate15"]:
        out[col] = np.nan
    if hist.empty:
        return out, status
    grouped = {pid: grp.reset_index(drop=True) for pid, grp in hist.groupby("player_id_key", sort=False)}
    feature_rows: list[dict[str, Any]] = []
    for idx, row in out.iterrows():
        pid = row.get("player_id_key", "")
        current_date = pd.to_datetime(row.get("date_key"), errors="coerce")
        feat: dict[str, Any] = {"_row_index": idx}
        if not pid or pd.isna(current_date) or pid not in grouped:
            feature_rows.append(feat)
            continue
        prior = grouped[pid][grouped[pid]["game_date"] < current_date]
        for days in [15, 30]:
            window = prior[prior["game_date"] >= current_date - pd.Timedelta(days=days)]
            apps = int(len(window))
            starts = int(window["start_proxy"].sum()) if apps else 0
            feat[f"appearances{days}"] = apps
            feat[f"ab{days}"] = float(window["at_bats"].sum()) if apps else 0.0
            feat[f"starts_proxy{days}"] = starts
            feat[f"start_proxy_rate{days}"] = starts / apps if apps else np.nan
        window15 = prior[prior["game_date"] >= current_date - pd.Timedelta(days=15)]
        feat["pinch_hit_proxy_rate15"] = float(window15["pinch_hit_proxy"].mean()) if len(window15) else np.nan
        feature_rows.append(feat)
    features = pd.DataFrame(feature_rows).set_index("_row_index")
    for col in features.columns:
        out.loc[features.index, col] = features[col]
    return out, status


def _role_bucket(row: pd.Series) -> str:
    started_raw = _norm(row.get("started_flag", ""))
    started = started_raw in {"true", "1", "yes"}
    lineup_slot = pd.to_numeric(pd.Series([row.get("lineup_slot")]), errors="coerce").iloc[0]
    has_lineup_context = pd.notna(lineup_slot) or started_raw in {"true", "false", "1", "0", "yes", "no"}
    current_starter = started or (pd.notna(lineup_slot) and 1 <= float(lineup_slot) <= 9)
    apps30 = pd.to_numeric(pd.Series([row.get("appearances30")]), errors="coerce").iloc[0]
    starts30 = pd.to_numeric(pd.Series([row.get("starts_proxy30")]), errors="coerce").iloc[0]
    start_rate30 = pd.to_numeric(pd.Series([row.get("start_proxy_rate30")]), errors="coerce").iloc[0]
    start_rate15 = pd.to_numeric(pd.Series([row.get("start_proxy_rate15")]), errors="coerce").iloc[0]
    pinch_rate15 = pd.to_numeric(pd.Series([row.get("pinch_hit_proxy_rate15")]), errors="coerce").iloc[0]
    ab30 = pd.to_numeric(pd.Series([row.get("ab30")]), errors="coerce").iloc[0]
    if has_lineup_context and not current_starter:
        return "bench_or_non_start"
    if pd.isna(apps30) or apps30 == 0:
        return "unknown_role_history"
    if apps30 <= 5 or (pd.notna(ab30) and ab30 <= 15) or (pd.notna(starts30) and starts30 <= 2):
        return "spot_starter_low_start_rate"
    if (pd.notna(start_rate15) and start_rate15 < 0.35) or (pd.notna(start_rate30) and start_rate30 < 0.45):
        return "spot_starter_low_start_rate"
    if (pd.notna(start_rate30) and start_rate30 < 0.65) or (pd.notna(pinch_rate15) and pinch_rate15 >= 0.35):
        return "part_time_platoon"
    if pd.notna(start_rate30) and start_rate30 < 0.85:
        return "semi_regular"
    return "stable_starter"


UNSTABLE_BUCKETS = {"bench_or_non_start", "spot_starter_low_start_rate", "part_time_platoon", "unknown_role_history"}


def _add_profile_flags(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["d15_at_bats"] = pd.to_numeric(out.get("d15_at_bats"), errors="coerce")
    out["d15_plate_appearances"] = pd.to_numeric(out.get("d15_plate_appearances"), errors="coerce")
    out["low_d15_ab"] = out["d15_at_bats"].notna() & out["d15_at_bats"].le(LOW_D15_AB_THRESHOLD)
    out["low_d15_pa"] = out["d15_plate_appearances"].notna() & out["d15_plate_appearances"].le(LOW_D15_PA_THRESHOLD)
    out["under_0_5"] = out["side_key"].eq("under") & out["line_key"].eq("0.5")
    out["role_stability_bucket"] = out.apply(_role_bucket, axis=1)
    out["unstable_role"] = out["role_stability_bucket"].isin(UNSTABLE_BUCKETS)
    return out


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(rows))
    wins = int(rows["result_key"].eq("win").sum()) if bets else 0
    losses = int(rows["result_key"].eq("loss").sum()) if bets else 0
    pushes = int(rows["result_key"].eq("push").sum()) if bets else 0
    decisions = wins + losses
    units = float(pd.to_numeric(rows.get("units_num"), errors="coerce").sum(skipna=True)) if bets else 0.0
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / decisions if decisions else np.nan,
        "roi": units / bets if bets else np.nan,
        "units": units,
        "avg_odds": float(pd.to_numeric(rows.get("price_num"), errors="coerce").mean(skipna=True)) if bets else np.nan,
    }


def _daily(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["date", "bets", "wins", "losses", "pushes", "wr", "roi", "units"])
    return rows.groupby("date_key", dropna=False).apply(lambda g: pd.Series(_metrics(g)), include_groups=False).reset_index().rename(columns={"date_key": "date"}).sort_values("date")


def _slice_summary(name: str, rows: pd.DataFrame, latest_date: str, latest_reconcile: str) -> dict[str, Any]:
    daily = _daily(rows)
    latest_ts = pd.Timestamp(latest_date) if latest_date else pd.NaT
    last7 = rows[pd.to_datetime(rows["date_key"], errors="coerce").ge(latest_ts - pd.Timedelta(days=6))] if latest_date else rows.iloc[0:0]
    last14 = rows[pd.to_datetime(rows["date_key"], errors="coerce").ge(latest_ts - pd.Timedelta(days=13))] if latest_date else rows.iloc[0:0]
    best = daily.sort_values("units", ascending=False).head(1)
    worst = daily.sort_values("units", ascending=True).head(1)
    best_day = str(best["date"].iloc[0]) if not best.empty else ""
    best_days = daily.sort_values("units", ascending=False).head(2)["date"].astype(str).tolist()
    without_best = rows[~rows["date_key"].eq(best_day)] if best_day else rows
    without_best2 = rows[~rows["date_key"].isin(best_days)] if best_days else rows
    last7_m = _metrics(last7)
    last14_m = _metrics(last14)
    dates = sorted([d for d in rows.get("date_key", pd.Series(dtype=str)).dropna().astype(str).unique() if d])
    return {
        "slice": name,
        **_metrics(rows),
        "date_count": len(dates),
        "first_date": dates[0] if dates else "",
        "last_date": dates[-1] if dates else "",
        "date_coverage": f"{dates[0]}..{dates[-1]}" if dates else "",
        "last_7_bets": last7_m["bets"],
        "last_7_roi": last7_m["roi"],
        "last_7_units": last7_m["units"],
        "last_14_bets": last14_m["bets"],
        "last_14_roi": last14_m["roi"],
        "last_14_units": last14_m["units"],
        "best_day": best_day,
        "best_day_units": float(best["units"].iloc[0]) if not best.empty else 0.0,
        "worst_day": str(worst["date"].iloc[0]) if not worst.empty else "",
        "worst_day_units": float(worst["units"].iloc[0]) if not worst.empty else 0.0,
        "roi_without_best_day": _metrics(without_best)["roi"],
        "roi_without_best_2_days": _metrics(without_best2)["roi"],
        "latest_included_date": latest_date,
        "stale": bool(latest_reconcile and latest_date and latest_date < latest_reconcile),
    }


def _build_slices(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "all exact ranking/QC overlaps": rows.copy(),
        "exact overlap + under 0.5": rows[rows["under_0_5"]].copy(),
        "exact overlap + low d15 AB": rows[rows["low_d15_ab"]].copy(),
        "exact overlap + low d15 PA": rows[rows["low_d15_pa"]].copy(),
        "exact overlap + unstable role": rows[rows["unstable_role"]].copy(),
        "exact overlap + under 0.5 + low d15 AB": rows[rows["under_0_5"] & rows["low_d15_ab"]].copy(),
        "exact overlap + under 0.5 + low d15 PA": rows[rows["under_0_5"] & rows["low_d15_pa"]].copy(),
        "exact overlap + under 0.5 + low d15 AB + unstable role": rows[
            rows["under_0_5"] & rows["low_d15_ab"] & rows["unstable_role"]
        ].copy(),
        "exact overlap + under 0.5 + low d15 PA + unstable role": rows[
            rows["under_0_5"] & rows["low_d15_pa"] & rows["unstable_role"]
        ].copy(),
    }


def _comparison_recommendation(summary: pd.DataFrame) -> tuple[str, str]:
    def row(name: str) -> pd.Series | None:
        hit = summary[summary["slice"].eq(name)]
        return hit.iloc[0] if not hit.empty else None

    ab = row("exact overlap + under 0.5 + low d15 AB")
    pa = row("exact overlap + under 0.5 + low d15 PA")
    pa_role = row("exact overlap + under 0.5 + low d15 PA + unstable role")
    if ab is None or pa is None:
        return "Add PA watch alongside existing watch", "AB and PA comparison rows were not both available."
    sample_delta = int(pa.get("bets", 0) or 0) - int(ab.get("bets", 0) or 0)
    roi_delta = float(pa.get("roi", np.nan)) - float(ab.get("roi", np.nan))
    if abs(sample_delta) <= 10 and abs(roi_delta) <= 0.03:
        recommendation = "Add PA watch alongside existing watch"
        rationale = "PA and AB profiles are materially similar, so PA is useful confirmation without enough separation to replace the current practical AB watch yet. Keep the AB profile as the primary incumbent and monitor PA beside it."
    elif float(pa.get("roi", np.nan)) >= float(ab.get("roi", np.nan)) and int(pa.get("bets", 0) or 0) >= int(ab.get("bets", 0) or 0) * 0.85:
        recommendation = "Add PA watch alongside existing watch"
        rationale = "PA is competitive with AB and better aligned with the research explanation, but side-by-side monitoring should run before replacing the existing watch."
    else:
        recommendation = "Keep current watch unchanged"
        rationale = "The existing AB watch currently has the better practical sample/return tradeoff; PA should remain a companion diagnostic until it proves superior prospectively."
    if pa_role is not None and int(pa_role.get("bets", 0) or 0) < int(pa.get("bets", 0) or 0):
        rationale += " The PA + unstable-role slice is narrower, so it remains supporting context rather than the primary watch profile."
    return recommendation, rationale


def _latest_reconcile_date(recon_root: Path) -> str:
    return max([_date_key(path.parent.name) for path in recon_root.glob("*/actual_wagers_by_source_*.csv")], default="")


def build_watch(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    recon_root = Path(args.reconcile_root)
    rows = _selection_perf(_load_perf(recon_root), Path(args.lane_root))
    rows, pds_status = _join_pds(rows)
    rows, lineup_status, lineup_path = _join_lineup_context(rows, Path(args.lineup_context_dir))
    rows, history_status = _add_role_features(rows)
    rows = _add_profile_flags(rows)

    latest_reconcile = _latest_reconcile_date(recon_root)
    latest_included = max([d for d in rows.get("date_key", pd.Series(dtype=str)).dropna().astype(str) if d], default="")
    slices = _build_slices(rows)
    summary = pd.DataFrame([_slice_summary(name, frame, latest_included, latest_reconcile) for name, frame in slices.items()])
    recommendation, recommendation_rationale = _comparison_recommendation(summary)

    csv_rows: list[dict[str, Any]] = []
    for name, frame in slices.items():
        daily = _daily(frame)
        for _, row in daily.iterrows():
            payload = row.to_dict()
            payload.update({"slice": name, "scope": "daily", "period": row.get("date")})
            csv_rows.append(payload)
        payload = _metrics(frame)
        payload.update({"slice": name, "scope": "cumulative", "period": latest_included})
        csv_rows.append(payload)
    watch_csv = pd.DataFrame(csv_rows)

    csv_path = out_dir / "overlap_role_profile_watch.csv"
    json_path = out_dir / "overlap_role_profile_watch.json"
    md_path = out_dir / "overlap_role_profile_watch.md"
    watch_csv.to_csv(csv_path, index=False)
    metadata = {
        "latest_reconcile_date_found": latest_reconcile,
        "latest_included_date": latest_included,
        "stale": bool(latest_reconcile and latest_included and latest_included < latest_reconcile),
        "pds_status": pds_status,
        "lineup_context_status": lineup_status,
        "lineup_context_csv": lineup_path,
        "player_stats_history_status": history_status,
        "resolved_exact_overlap_rows": int(len(rows)),
        "low_d15_ab_threshold": LOW_D15_AB_THRESHOLD,
        "low_d15_pa_threshold": LOW_D15_PA_THRESHOLD,
        "recommendation": recommendation,
        "recommendation_rationale": recommendation_rationale,
        "outputs": {"csv": str(csv_path), "json": str(json_path), "md": str(md_path)},
        "slices": summary.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")

    lines = [
        "# Overlap Role Profile Watch",
        "",
        "Visibility only. Tracks the research profile without changing production rules, filters, thresholds, uploads, or models.",
        "",
        f"- Latest reconcile date found: `{latest_reconcile or 'none'}`",
        f"- Latest included date: `{latest_included or 'none'}`",
        f"- Stale: `{str(metadata['stale']).lower()}`",
        f"- PDS status: `{pds_status}`",
        f"- Lineup context status: `{lineup_status}`",
        f"- Player stats history status: `{history_status}`",
        "",
        "## Slice Summary",
        "",
        _md_table(
            summary,
            [
                "slice",
                "bets",
                "wins",
                "losses",
                "pushes",
                "wr",
                "roi",
                "units",
                "avg_odds",
                "date_count",
                "date_coverage",
                "last_7_bets",
                "last_7_roi",
                "last_14_bets",
                "last_14_roi",
                "best_day",
                "best_day_units",
                "worst_day",
                "worst_day_units",
                "roi_without_best_day",
                "roi_without_best_2_days",
                "latest_included_date",
                "stale",
            ],
        ),
        "",
        "## AB vs PA Recommendation",
        "",
        f"Recommendation: **{recommendation}**.",
        "",
        recommendation_rationale,
        "",
        _md_table(
            summary[
                summary["slice"].isin(
                    [
                        "exact overlap + under 0.5 + low d15 AB",
                        "exact overlap + under 0.5 + low d15 PA",
                        "exact overlap + under 0.5 + low d15 PA + unstable role",
                    ]
                )
            ],
            ["slice", "bets", "wins", "losses", "pushes", "wr", "roi", "units", "date_count", "date_coverage", "latest_included_date"],
        ),
        "",
        "## Notes",
        "",
        f"- `low d15 AB` is `d15_at_bats <= {LOW_D15_AB_THRESHOLD:g}` from `mlb.player_derived_stats`, matching the current practical watch definition.",
        f"- `low d15 PA` is `d15_plate_appearances <= {LOW_D15_PA_THRESHOLD:g}` from `mlb.player_derived_stats`, added as a PA-backed companion profile.",
        "- `unstable role` uses the same descriptive role buckets from the player opportunity stability audit: bench/non-start, spot/low-start-rate, part-time/platoon, or unknown role history.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build overlap role profile watch artifacts.")
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECON_ROOT))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--lineup-context-dir", default=str(DEFAULT_LINEUP_CONTEXT_DIR))
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
        f"latest_included={metadata.get('latest_included_date') or 'none'} "
        f"stale={metadata.get('stale')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
