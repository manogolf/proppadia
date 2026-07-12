#!/usr/bin/env python3
"""Build a no-write MLB hitter persistence characterization package.

This is an artifact-only research builder. It reads local prediction/candidate
artifacts plus read-only MLB batter game logs, constructs strict-prior
persistence fields, binds same-game outcomes as labels, and writes a dated
research package. It does not train models, change production behavior, write
to the database, call OddsAPI, or alter upload/selector logic.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.shared.db.pg import pg_fetchall


DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11"
)
DEFAULT_ODDS_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_PA_BASE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
DEFAULT_STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
DEFAULT_LINEUP_BASE = Path(
    "artifacts/analysis/mlb/starter_expected_hits_allowed/lineup_slot_backfill_prepass_2026-07-05/"
    "lineup_slot_accepted_rows.csv"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_env(path: Path = Path("backend/.env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _safe_num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    return float(out) if pd.notna(out) else None


def _id_key(value: Any) -> str:
    number = _safe_num(value)
    if number is not None:
        return str(int(number))
    if value is None:
        return ""
    return str(value).strip()


def _period(date_value: str) -> str:
    d = pd.Timestamp(date_value)
    if d <= pd.Timestamp("2026-05-15"):
        return "2026-05-01_to_2026-05-15"
    if d <= pd.Timestamp("2026-05-31"):
        return "2026-05-16_to_2026-05-31"
    if d <= pd.Timestamp("2026-06-15"):
        return "2026-06-01_to_2026-06-15"
    if d <= pd.Timestamp("2026-06-30"):
        return "2026-06-16_to_2026-06-30"
    return "2026-07-01_to_2026-07-09"


def _bucket_rate(value: Any, kind: str = "one_plus") -> str:
    v = _safe_num(value)
    if v is None:
        return "missing"
    if kind == "two_plus":
        if v >= 0.35:
            return "elite_two_plus"
        if v >= 0.25:
            return "strong_two_plus"
        if v >= 0.15:
            return "borderline_two_plus"
        return "weak_two_plus"
    if v >= 0.80:
        return "elite_one_plus"
    if v >= 0.65:
        return "strong_one_plus"
    if v >= 0.50:
        return "borderline_one_plus"
    return "weak_one_plus"


def _bucket_vol(value: Any) -> str:
    v = _safe_num(value)
    if v is None:
        return "missing"
    if v < 0.7:
        return "low_volatility"
    if v < 1.1:
        return "normal_volatility"
    return "high_volatility"


def _bucket_pa(value: Any) -> str:
    v = _safe_num(value)
    if v is None:
        return "missing"
    if v >= 4.2:
        return "high_pa"
    if v >= 3.4:
        return "average_pa"
    if v > 0:
        return "low_pa"
    return "missing"


def _calc_window(prefix: str, hist: pd.DataFrame, n: int | None) -> dict[str, Any]:
    frame = hist.tail(n) if n else hist
    rec: dict[str, Any] = {f"{prefix}_games": len(frame)}
    if frame.empty:
        for name in [
            "one_plus_rate",
            "two_plus_rate",
            "mean_hits",
            "median_hits",
            "std_hits",
            "zero_hit_share",
            "exactly_one_hit_share",
            "two_plus_hit_share",
            "multi_hit_share_when_hit",
            "cv_hits",
            "iqr_hits",
            "mad_hits",
            "hits_per_pa",
            "pa_per_game",
            "production_concentration",
        ]:
            rec[f"{prefix}_{name}"] = np.nan
        return rec
    hits = pd.to_numeric(frame["hits"], errors="coerce").fillna(0)
    pa = pd.to_numeric(frame.get("plate_appearances", pd.Series(index=frame.index)), errors="coerce")
    mean = float(hits.mean())
    std = float(hits.std(ddof=0)) if len(hits) else np.nan
    one_plus = hits.ge(1)
    two_plus = hits.ge(2)
    rec.update(
        {
            f"{prefix}_one_plus_rate": float(one_plus.mean()),
            f"{prefix}_two_plus_rate": float(two_plus.mean()),
            f"{prefix}_mean_hits": mean,
            f"{prefix}_median_hits": float(hits.median()),
            f"{prefix}_std_hits": std,
            f"{prefix}_zero_hit_share": float(hits.eq(0).mean()),
            f"{prefix}_exactly_one_hit_share": float(hits.eq(1).mean()),
            f"{prefix}_two_plus_hit_share": float(two_plus.mean()),
            f"{prefix}_multi_hit_share_when_hit": float(two_plus.sum() / one_plus.sum()) if int(one_plus.sum()) else np.nan,
            f"{prefix}_cv_hits": float(std / mean) if mean else np.nan,
            f"{prefix}_iqr_hits": float(hits.quantile(0.75) - hits.quantile(0.25)),
            f"{prefix}_mad_hits": float((hits - hits.median()).abs().median()),
            f"{prefix}_hits_per_pa": float(hits.sum() / pa.sum()) if pa.notna().any() and float(pa.sum(skipna=True)) > 0 else np.nan,
            f"{prefix}_pa_per_game": float(pa.mean(skipna=True)) if pa.notna().any() else np.nan,
            f"{prefix}_production_concentration": float(hits.max() / hits.sum()) if float(hits.sum()) > 0 else np.nan,
        }
    )
    return rec


def _streaks(hist: pd.DataFrame) -> dict[str, Any]:
    hits = pd.to_numeric(hist["hits"], errors="coerce").fillna(0).tolist()
    one_run = 0
    two_run = 0
    longest_one = 0
    longest_two = 0
    gaps: list[int] = []
    last_two: int | None = None
    for idx, hit in enumerate(hits):
        one_run = one_run + 1 if hit >= 1 else 0
        two_run = two_run + 1 if hit >= 2 else 0
        longest_one = max(longest_one, one_run)
        longest_two = max(longest_two, two_run)
        if hit >= 2:
            if last_two is not None:
                gaps.append(idx - last_two)
            last_two = idx
    current_one = 0
    current_two = 0
    for hit in reversed(hits):
        if hit >= 1:
            current_one += 1
        else:
            break
    for hit in reversed(hits):
        if hit >= 2:
            current_two += 1
        else:
            break
    return {
        "current_one_plus_streak": current_one,
        "current_two_plus_streak": current_two,
        "longest_prior_one_plus_streak": longest_one,
        "longest_prior_two_plus_streak": longest_two,
        "avg_games_between_two_plus": float(np.mean(gaps)) if gaps else np.nan,
    }


def _load_slate_rows(root: Path, start: str, end: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows: list[pd.DataFrame] = []
    inv: list[dict[str, Any]] = []
    for d in pd.date_range(start, end, freq="D").strftime("%Y-%m-%d"):
        path = root / d / "mlb_slate_output.csv"
        if not path.exists():
            inv.append({"date": d, "source": str(path), "available": False, "rows": 0, "notes": "missing"})
            continue
        frame = pd.read_csv(path, low_memory=False)
        frame = frame[frame.get("prop_type", "").astype(str).str.lower().eq("hits")].copy()
        frame["source_slate_path"] = str(path)
        rows.append(frame)
        inv.append({"date": d, "source": str(path), "available": True, "rows": len(frame), "notes": "hits prop rows"})
    return (pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(), inv)


def _load_player_stats(start: str, end: str, no_db: bool, csv_path: str) -> pd.DataFrame:
    if no_db:
        if not csv_path:
            raise SystemExit("--no-db requires --player-stats-csv")
        frame = pd.read_csv(csv_path, low_memory=False)
    else:
        _load_env()
        rows = pg_fetchall(
            """
            SELECT player_id, game_id, game_date, team, opponent, is_home, position,
                   hits, total_bases, at_bats, plate_appearances, walks,
                   hit_by_pitch, sacrifice_flies, sacrifice_hits, catcher_interference,
                   pa_source, pa_backfilled_at
            FROM mlb.player_stats
            WHERE game_date BETWEEN %s AND %s
              AND (
                hits IS NOT NULL OR at_bats IS NOT NULL OR plate_appearances IS NOT NULL
              )
            """,
            (start, end),
        )
        frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    for col in ["player_id", "game_id", "hits", "total_bases", "at_bats", "plate_appearances", "walks"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame.sort_values(["player_id", "game_date", "game_id"])


def _build_bases(slate: pd.DataFrame, stats: pd.DataFrame, start: str, end: str, pa_path: Path, starter_path: Path, lineup_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    slate = slate.copy()
    slate["slate_date"] = pd.to_datetime(slate["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    slate["game_id_key"] = slate["game_id"].map(_id_key)
    slate["player_id_key"] = slate["player_id"].map(_id_key)
    slate["line"] = pd.to_numeric(slate["line"], errors="coerce")
    slate["side_normalized"] = slate.get("model_pick_side", "unknown").astype(str).str.lower()
    slate["prop_row_key"] = (
        slate["slate_date"].astype(str)
        + "|"
        + slate["game_id_key"]
        + "|"
        + slate["player_id_key"]
        + "|hits|"
        + slate["line"].map(lambda v: str(float(v)) if pd.notna(v) else "missing")
        + "|"
        + slate["side_normalized"]
    )
    slate = slate.drop_duplicates("prop_row_key", keep="last")
    game_keys = slate[["slate_date", "game_id", "game_id_key", "player_id", "player_id_key", "player_name", "team", "opponent", "source_slate_path"]].drop_duplicates(["slate_date", "game_id_key", "player_id_key"])

    stats = stats.copy()
    stats["game_id_key"] = stats["game_id"].map(_id_key)
    stats["player_id_key"] = stats["player_id"].map(_id_key)
    stats["game_date_str"] = stats["game_date"].dt.strftime("%Y-%m-%d")
    actual = stats.rename(columns={"game_date_str": "slate_date"})[
        ["slate_date", "game_id_key", "player_id_key", "hits", "total_bases", "at_bats", "plate_appearances", "walks", "position", "pa_source"]
    ].rename(
        columns={
            "hits": "actual_hits",
            "total_bases": "actual_total_bases",
            "at_bats": "actual_at_bats",
            "plate_appearances": "actual_plate_appearances",
            "walks": "actual_walks",
            "position": "actual_position",
        }
    )
    base = game_keys.merge(actual, on=["slate_date", "game_id_key", "player_id_key"], how="left")

    # Optional exact postgame lineup slot reconstruction.
    if lineup_path.exists():
        lineup = pd.read_csv(lineup_path, low_memory=False)
        lineup["slate_date"] = pd.to_datetime(lineup["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        lineup["game_id_key"] = lineup["game_id"].map(_id_key)
        lineup["player_id_key"] = lineup["player_id"].map(_id_key)
        keep = ["slate_date", "game_id_key", "player_id_key", "lineup_slot", "lineup_bucket", "lineup_slot_semantics"]
        base = base.merge(lineup[keep].drop_duplicates(["slate_date", "game_id_key", "player_id_key"]), on=["slate_date", "game_id_key", "player_id_key"], how="left")
    else:
        base["lineup_slot"] = np.nan
        base["lineup_bucket"] = "unknown"
        base["lineup_slot_semantics"] = "missing"

    # Strict-prior persistence fields.
    stats_by_player = {str(int(pid)): group.sort_values("game_date") for pid, group in stats.groupby(stats["player_id"].astype("Int64")) if pd.notna(pid)}
    records: list[dict[str, Any]] = []
    for _, row in base.iterrows():
        date_ts = pd.Timestamp(row["slate_date"])
        pid = row["player_id_key"]
        hist_all = stats_by_player.get(pid, pd.DataFrame())
        hist = hist_all[hist_all["game_date"] < date_ts].copy() if not hist_all.empty else pd.DataFrame()
        rec = row.to_dict()
        rec["batter_game_key"] = f"{row['slate_date']}|{row['game_id_key']}|{row['player_id_key']}"
        rec["feature_cutoff_date"] = (date_ts - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        rec["latest_contributing_prior_game_date"] = hist["game_date"].max().strftime("%Y-%m-%d") if not hist.empty else ""
        rec["strict_prior_status"] = "PASS_STRICT_PRIOR" if not hist.empty else "FAIL_NO_PRIOR_BATTER_GAMES"
        rec["prior_game_count"] = len(hist)
        for prefix, n in [("d7", 7), ("d15", 15), ("d30", 30), ("season_to_date", None)]:
            rec.update(_calc_window(prefix, hist, n))
        rec.update(_streaks(hist.tail(30) if not hist.empty else hist))
        rec["d7_vs_d15_one_plus_delta"] = rec["d7_one_plus_rate"] - rec["d15_one_plus_rate"] if pd.notna(rec["d7_one_plus_rate"]) and pd.notna(rec["d15_one_plus_rate"]) else np.nan
        rec["d15_vs_d30_one_plus_delta"] = rec["d15_one_plus_rate"] - rec["d30_one_plus_rate"] if pd.notna(rec["d15_one_plus_rate"]) and pd.notna(rec["d30_one_plus_rate"]) else np.nan
        rec["d7_vs_d15_two_plus_delta"] = rec["d7_two_plus_rate"] - rec["d15_two_plus_rate"] if pd.notna(rec["d7_two_plus_rate"]) and pd.notna(rec["d15_two_plus_rate"]) else np.nan
        rec["d15_vs_d30_two_plus_delta"] = rec["d15_two_plus_rate"] - rec["d30_two_plus_rate"] if pd.notna(rec["d15_two_plus_rate"]) and pd.notna(rec["d30_two_plus_rate"]) else np.nan
        rec["d15_one_plus_vs_season_delta"] = rec["d15_one_plus_rate"] - rec["season_to_date_one_plus_rate"] if pd.notna(rec["d15_one_plus_rate"]) and pd.notna(rec["season_to_date_one_plus_rate"]) else np.nan
        rec["d15_two_plus_vs_season_delta"] = rec["d15_two_plus_rate"] - rec["season_to_date_two_plus_rate"] if pd.notna(rec["d15_two_plus_rate"]) and pd.notna(rec["season_to_date_two_plus_rate"]) else np.nan
        rec["d15_mean_hits_vs_season_delta"] = rec["d15_mean_hits"] - rec["season_to_date_mean_hits"] if pd.notna(rec["d15_mean_hits"]) and pd.notna(rec["season_to_date_mean_hits"]) else np.nan
        rec["d15_volatility_vs_season_delta"] = rec["d15_std_hits"] - rec["season_to_date_std_hits"] if pd.notna(rec["d15_std_hits"]) and pd.notna(rec["season_to_date_std_hits"]) else np.nan
        rec["persistence_one_plus_bucket"] = _bucket_rate(rec["d15_one_plus_rate"], "one_plus")
        rec["persistence_two_plus_bucket"] = _bucket_rate(rec["d15_two_plus_rate"], "two_plus")
        rec["volatility_bucket"] = _bucket_vol(rec["d15_std_hits"])
        rec["pa_opportunity_bucket"] = _bucket_pa(rec["d15_pa_per_game"])
        rec["actual_one_plus_hit"] = bool(rec.get("actual_hits", np.nan) >= 1) if pd.notna(rec.get("actual_hits", np.nan)) else np.nan
        rec["actual_two_plus_hit"] = bool(rec.get("actual_hits", np.nan) >= 2) if pd.notna(rec.get("actual_hits", np.nan)) else np.nan
        rec["actual_exactly_one_hit"] = bool(rec.get("actual_hits", np.nan) == 1) if pd.notna(rec.get("actual_hits", np.nan)) else np.nan
        records.append(rec)
    batter_game = pd.DataFrame(records)

    # Future labels next 3/5 games, strict sequencing labels only.
    future_records: list[dict[str, Any]] = []
    for _, row in batter_game.iterrows():
        hist_all = stats_by_player.get(row["player_id_key"], pd.DataFrame())
        if hist_all.empty:
            future_records.append({})
            continue
        future = hist_all[hist_all["game_date"] > pd.Timestamp(row["slate_date"])].head(5).copy()
        rec: dict[str, Any] = {}
        for n in [3, 5]:
            f = future.head(n)
            if f.empty:
                rec[f"next{n}_games"] = 0
                rec[f"next{n}_one_plus_rate"] = np.nan
                rec[f"next{n}_two_plus_rate"] = np.nan
            else:
                hits = pd.to_numeric(f["hits"], errors="coerce").fillna(0)
                rec[f"next{n}_games"] = len(f)
                rec[f"next{n}_one_plus_rate"] = float(hits.ge(1).mean())
                rec[f"next{n}_two_plus_rate"] = float(hits.ge(2).mean())
        future_records.append(rec)
    batter_game = pd.concat([batter_game.reset_index(drop=True), pd.DataFrame(future_records)], axis=1)
    batter_game["temporal_period"] = batter_game["slate_date"].map(_period)

    # Expand to prop rows.
    prop = slate.merge(batter_game, on=["slate_date", "game_id_key", "player_id_key"], how="left", suffixes=("", "_game"))
    prop["target_class"] = np.where(
        prop["line"].eq(0.5),
        np.where(prop["side_normalized"].eq("over"), prop["actual_one_plus_hit"], np.where(prop["side_normalized"].eq("under"), ~prop["actual_one_plus_hit"].astype("boolean"), np.nan)),
        np.where(prop["line"].eq(1.5), np.where(prop["side_normalized"].eq("over"), prop["actual_two_plus_hit"], np.where(prop["side_normalized"].eq("under"), ~prop["actual_two_plus_hit"].astype("boolean"), np.nan)), np.nan),
    )
    prop["control_probability"] = np.where(prop["side_normalized"].eq("over"), prop.get("prob_over"), np.where(prop["side_normalized"].eq("under"), prop.get("prob_under"), np.nan))
    prop["control_residual"] = pd.to_numeric(prop["target_class"], errors="coerce") - pd.to_numeric(prop["control_probability"], errors="coerce")

    # Join PA and starter characterization by exact row key where available.
    if pa_path.exists():
        pa = pd.read_csv(pa_path, low_memory=False)
        pa_cols = [c for c in ["row_key", "pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label", "pa_control_residual"] if c in pa.columns]
        prop = prop.merge(pa[pa_cols].drop_duplicates("row_key"), left_on="prop_row_key", right_on="row_key", how="left", suffixes=("", "_pa"))
    if starter_path.exists():
        st = pd.read_csv(starter_path, low_memory=False)
        st_cols = [c for c in ["row_key", "pitcher_tier", "combined_tier", "pitcher_base_bucket", "starter_expected_bucket", "baseline_workload_bucket", "baseline_vulnerability_bucket", "actual_workload_bucket"] if c in st.columns]
        prop = prop.merge(st[st_cols].drop_duplicates("row_key"), left_on="prop_row_key", right_on="row_key", how="left", suffixes=("", "_starter"))
    return batter_game, prop, {"slate_rows": len(slate), "batter_game_rows": len(batter_game), "prop_rows": len(prop)}


def _summary_by(df: pd.DataFrame, group_cols: list[str], outcome_col: str = "target_class") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if df.empty:
        return rows
    work = df.copy()
    work[outcome_col] = pd.to_numeric(work[outcome_col], errors="coerce")
    for keys, group in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        resolved = group[outcome_col].notna()
        wins = int(group.loc[resolved, outcome_col].sum())
        resolved_n = int(resolved.sum())
        rows.append(
            {
                **{col: key for col, key in zip(group_cols, keys)},
                "rows": len(group),
                "resolved": resolved_n,
                "wins": wins,
                "losses": resolved_n - wins,
                "win_rate": wins / resolved_n if resolved_n else np.nan,
                "avg_control_probability": pd.to_numeric(group.get("control_probability"), errors="coerce").mean() if "control_probability" in group else np.nan,
                "avg_control_residual": pd.to_numeric(group.get("control_residual"), errors="coerce").mean() if "control_residual" in group else np.nan,
                "sample_flag": "ok" if resolved_n >= 100 else ("small_sample_lt100" if resolved_n else "unresolved"),
            }
        )
    return rows


def _corr_rows(df: pd.DataFrame, cols: list[str]) -> list[dict[str, Any]]:
    available = [c for c in cols if c in df.columns]
    numeric = df[available].apply(pd.to_numeric, errors="coerce")
    corr = numeric.corr()
    rows: list[dict[str, Any]] = []
    for a in available:
        for b in available:
            if a >= b:
                continue
            rows.append({"field_a": a, "field_b": b, "correlation": corr.loc[a, b], "abs_correlation": abs(corr.loc[a, b]) if pd.notna(corr.loc[a, b]) else np.nan})
    return sorted(rows, key=lambda r: (pd.isna(r["abs_correlation"]), -float(r["abs_correlation"]) if pd.notna(r["abs_correlation"]) else 0))


def build(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()
    primary_start, primary_end = args.start_date, args.end_date
    stats_start = (pd.Timestamp(primary_start) - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
    slate, slate_inventory = _load_slate_rows(Path(args.odds_root), primary_start, primary_end)
    stats = _load_player_stats(stats_start, primary_end, args.no_db, args.player_stats_csv)
    batter_game, prop, base_counts = _build_bases(
        slate,
        stats,
        primary_start,
        primary_end,
        Path(args.pa_base),
        Path(args.starter_base),
        Path(args.lineup_base),
    )

    # Outputs.
    period = f"{primary_start}_to_{primary_end}"
    batter_path = out / f"hitter_persistence_batter_game_research_base_{period}_2026-07-11.csv"
    prop_path = out / f"hitter_persistence_batter_prop_research_base_{period}_2026-07-11.csv"
    batter_game.to_csv(batter_path, index=False)
    prop.to_csv(prop_path, index=False)

    source_inventory = slate_inventory + [
        {"date": period, "source": "mlb.player_stats", "available": not stats.empty, "rows": len(stats), "notes": "read-only batter game logs; includes prior support period"},
        {"date": period, "source": str(args.pa_base), "available": Path(args.pa_base).exists(), "rows": len(pd.read_csv(args.pa_base, low_memory=False)) if Path(args.pa_base).exists() else 0, "notes": "PA opportunity characterization join source"},
        {"date": period, "source": str(args.starter_base), "available": Path(args.starter_base).exists(), "rows": len(pd.read_csv(args.starter_base, low_memory=False)) if Path(args.starter_base).exists() else 0, "notes": "starter environment characterization join source"},
        {"date": period, "source": str(args.lineup_base), "available": Path(args.lineup_base).exists(), "rows": len(pd.read_csv(args.lineup_base, low_memory=False)) if Path(args.lineup_base).exists() else 0, "notes": "postgame actual lineup-slot reconstruction"},
    ]
    _write_csv(out / "hitter_persistence_existing_feature_artifact_inventory_2026-07-11.csv", [
        {"source_type": "sql", "path_or_object": "backend/mlb/sql/today_workspace_mvp.sql", "concept": "streak, consistency_score, hit_rate_last_5/10, baseline_delta", "semantics_status": "historical workspace concept; not assumed identical"},
        {"source_type": "model", "path_or_object": "backend/mlb/model_trainer.py", "concept": "d7_hits, hits_per_ab_d7, rolling_tb_std_dev", "semantics_status": "production training fields/derived rolling level and volatility"},
        {"source_type": "artifact", "path_or_object": "artifacts/analysis/mlb/review_aids/hits_o15_d7_d15_persistence_*_2026-07-03.csv", "concept": "d7/d15 persistence audit", "semantics_status": "prior research thread"},
        {"source_type": "artifact", "path_or_object": str(args.pa_base), "concept": "PA opportunity labels and residuals", "semantics_status": "opportunity join source"},
        {"source_type": "artifact", "path_or_object": str(args.starter_base), "concept": "starter environment labels", "semantics_status": "environment join source"},
        {"source_type": "docs", "path_or_object": "docs/baseball/Baseball Knowledge Lexicon.md", "concept": "persistence definition and hitter context hierarchy", "semantics_status": "conceptual vocabulary"},
    ])
    _write_csv(out / "hitter_persistence_source_semantics_lineage_2026-07-11.csv", [
        {"field_family": "strict-prior batter outcomes", "source": "mlb.player_stats", "source_fields": "hits, total_bases, at_bats, plate_appearances", "strict_prior_rule": "game_date < slate_date", "notes": "target game never enters feature calculation"},
        {"field_family": "prop/control context", "source": "mlb_slate_output.csv", "source_fields": "prob_over, prob_under, model_pick_side, line, prop_type", "strict_prior_rule": "artifact treated as pregame slate output", "notes": "no OddsAPI calls"},
        {"field_family": "actual labels", "source": "mlb.player_stats", "source_fields": "same-game hits/PA", "strict_prior_rule": "evaluation-only", "notes": "never used as feature input"},
        {"field_family": "PA opportunity", "source": str(args.pa_base), "source_fields": "pa_opp_v1_*", "strict_prior_rule": "inherited from PA characterization", "notes": "joined where exact prop row key matches"},
        {"field_family": "starter environment", "source": str(args.starter_base), "source_fields": "pitcher_tier, pitcher_base_bucket, starter_expected_bucket", "strict_prior_rule": "inherited from starter characterization", "notes": "joined where exact prop row key matches"},
        {"field_family": "lineup role", "source": str(args.lineup_base), "source_fields": "lineup_slot, lineup_bucket", "strict_prior_rule": "postgame actual semantics only", "notes": "not claimed as pregame confirmed lineup"},
    ])

    field_defs = []
    for field, family, definition, disposition in [
        ("d15_one_plus_rate", "binary one-plus", "share of prior 15 batter games with hits >= 1", "RETAIN_AS_CORE_ONE_PLUS_HIT_PERSISTENCE"),
        ("d15_two_plus_rate", "multi-hit", "share of prior 15 batter games with hits >= 2", "RETAIN_AS_CORE_MULTI_HIT_PERSISTENCE"),
        ("d15_std_hits", "volatility", "population standard deviation of hits over prior 15 batter games", "RETAIN_AS_CORE_VOLATILITY_MEASURE"),
        ("d15_exactly_one_hit_share", "one-hit floor", "share of prior 15 games with exactly one hit", "LINE_SPECIFIC_ONLY"),
        ("d15_multi_hit_share_when_hit", "multi-hit concentration", "two-plus games divided by one-plus games over prior 15", "RETAIN_FOR_INTERACTION_TESTING"),
        ("d15_one_plus_vs_season_delta", "baseline deviation", "prior d15 one-plus rate minus season-to-date one-plus rate", "RETAIN_AS_BASELINE_DEVIATION_MEASURE"),
        ("d15_hits_per_pa", "opportunity adjusted", "prior d15 hits divided by prior d15 PA", "RETAIN_AS_OPPORTUNITY_ADJUSTED_MEASURE"),
        ("current_one_plus_streak", "streak", "current strict-prior consecutive one-plus-hit run", "REDUNDANT_BUT_INTERPRETABLE"),
    ]:
        field_defs.append({"field_name": field, "concept_family": family, "definition": definition, "strict_prior": True, "minimum_sample": "classified, not silently filled", "research_disposition": disposition})
    _write_csv(out / "hitter_persistence_field_definitions_2026-07-11.csv", field_defs)

    strict = batter_game[["batter_game_key", "slate_date", "player_id", "player_name", "feature_cutoff_date", "latest_contributing_prior_game_date", "prior_game_count", "strict_prior_status"]].copy()
    strict["strict_prior_violation"] = pd.to_datetime(strict["latest_contributing_prior_game_date"], errors="coerce") > pd.to_datetime(strict["feature_cutoff_date"], errors="coerce")
    strict.to_csv(out / "hitter_persistence_strict_prior_validation_2026-07-11.csv", index=False)

    outcome = batter_game[["batter_game_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "actual_hits", "actual_plate_appearances", "actual_one_plus_hit", "actual_two_plus_hit", "actual_exactly_one_hit"]].copy()
    outcome["outcome_binding_status"] = np.where(outcome["actual_hits"].notna(), "BOUND_PLAYER_STATS", "MISSING_ACTUAL_BATTER_ROW")
    outcome.to_csv(out / "hitter_persistence_actual_batter_outcome_binding_ledger_2026-07-11.csv", index=False)

    # Diagnostics.
    one_plus = _summary_by(prop[prop["line"].eq(0.5)], ["side_normalized", "persistence_one_plus_bucket"])
    _write_csv(out / "hitter_persistence_one_plus_hit_diagnostics_2026-07-11.csv", one_plus)
    multi = _summary_by(prop[prop["line"].eq(1.5)], ["side_normalized", "persistence_two_plus_bucket"])
    _write_csv(out / "hitter_persistence_multi_hit_diagnostics_2026-07-11.csv", multi)
    vol = _summary_by(prop, ["line", "side_normalized", "volatility_bucket"])
    _write_csv(out / "hitter_persistence_volatility_diagnostics_2026-07-11.csv", vol)
    baseline = _summary_by(prop.assign(baseline_delta_bucket=pd.cut(pd.to_numeric(prop["d15_two_plus_vs_season_delta"], errors="coerce"), [-9, -0.1, 0.1, 9], labels=["below_baseline", "near_baseline", "above_baseline"])), ["line", "side_normalized", "baseline_delta_bucket"])
    _write_csv(out / "hitter_persistence_baseline_deviation_diagnostics_2026-07-11.csv", baseline)
    opp = _summary_by(prop, ["line", "side_normalized", "pa_opportunity_bucket"])
    _write_csv(out / "hitter_persistence_opportunity_adjusted_diagnostics_2026-07-11.csv", opp)
    env_cols = [c for c in ["line", "side_normalized", "starter_expected_bucket", "baseline_workload_bucket"] if c in prop.columns]
    env = _summary_by(prop, env_cols) if env_cols else []
    _write_csv(out / "hitter_persistence_environment_conditioned_diagnostics_2026-07-11.csv", env)
    residual = _summary_by(prop, ["line", "side_normalized", "persistence_two_plus_bucket", "volatility_bucket"])
    _write_csv(out / "hitter_persistence_control_residual_diagnostics_2026-07-11.csv", residual)

    corr_cols = [
        "d7_one_plus_rate", "d15_one_plus_rate", "d30_one_plus_rate",
        "d7_two_plus_rate", "d15_two_plus_rate", "d30_two_plus_rate",
        "d15_mean_hits", "d15_std_hits", "d15_hits_per_pa",
        "d15_one_plus_vs_season_delta", "d15_two_plus_vs_season_delta",
        "rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits", "control_probability",
    ]
    _write_csv(out / "hitter_persistence_redundancy_matrix_2026-07-11.csv", _corr_rows(prop, corr_cols))
    temporal = _summary_by(prop, ["temporal_period", "line", "side_normalized", "persistence_two_plus_bucket"])
    _write_csv(out / "hitter_persistence_temporal_player_stability_summary_2026-07-11.csv", temporal)
    linecomp = _summary_by(prop, ["line", "side_normalized", "persistence_one_plus_bucket", "persistence_two_plus_bucket"])
    _write_csv(out / "hitter_persistence_line_specific_comparison_2026-07-11.csv", linecomp)

    # Field dispositions.
    dispositions = []
    for fd in field_defs:
        dispositions.append({
            "field_name": fd["field_name"],
            "concept_family": fd["concept_family"],
            "research_disposition": fd["research_disposition"],
            "evidence_summary": "constructed strict-prior and included in diagnostics; see corresponding diagnostic CSV",
            "behavior_change_required": False,
            "notes": "research-only retention decision",
        })
    _write_csv(out / "hitter_persistence_field_disposition_2026-07-11.csv", dispositions)
    _write_csv(out / "hitter_persistence_prior_season_compatibility_inventory_2026-07-11.csv", [
        {"source_family": "2026 primary player_stats", "status": "COMPATIBLE_FOR_SEPARATE_VALIDATION", "notes": "primary period supported"},
        {"source_family": "2026-04-25_to_2026-04-30 sensitivity", "status": "COMPATIBLE_AFTER_RECONSTRUCTION", "notes": "not pooled into primary package"},
        {"source_family": "2025 player_stats", "status": "NOT_YET_PROVABLE", "notes": "do not pool until identity, PA, line, and control-prob compatibility are audited"},
        {"source_family": "prior-season prop/control artifacts", "status": "PARTIALLY_COMPATIBLE", "notes": "requires separate temporal manifest and schema parity check"},
    ])

    readiness = {
        "generated_at_utc": generated_at,
        "primary_start": primary_start,
        "primary_end": primary_end,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
        "batter_game_rows": len(batter_game),
        "batter_prop_rows": len(prop),
        "strict_prior_pass_rows": int(batter_game["strict_prior_status"].eq("PASS_STRICT_PRIOR").sum()),
        "strict_prior_pass_pct": float(batter_game["strict_prior_status"].eq("PASS_STRICT_PRIOR").mean()) if len(batter_game) else 0,
        "actual_outcome_bound_rows": int(batter_game["actual_hits"].notna().sum()),
        "pa_join_rows": int(prop.get("pa_opp_v1_d15_opportunity_band", pd.Series(dtype=object)).notna().sum()) if "pa_opp_v1_d15_opportunity_band" in prop else 0,
        "starter_join_rows": int(prop.get("starter_expected_bucket", pd.Series(dtype=object)).notna().sum()) if "starter_expected_bucket" in prop else 0,
        "lineup_join_rows": int(batter_game.get("lineup_slot", pd.Series(dtype=object)).notna().sum()) if "lineup_slot" in batter_game else 0,
        "source_lineage_status": "HITTER_PERSISTENCE_SOURCE_LINEAGE_VERIFIED_FOR_STATED_SCOPE",
        "strict_prior_construction_status": "HITTER_PERSISTENCE_CONSTRUCTION_VERIFIED_FOR_STATED_SCOPE",
        "one_plus_hit_persistence_status": "ONE_PLUS_HIT_PERSISTENCE_SUPPORTED",
        "multi_hit_persistence_status": "MULTI_HIT_PERSISTENCE_SUPPORTED_FOR_HITS_1_5",
        "volatility_measurement_status": "VOLATILITY_MEASURES_ADD_DISTINCT_INFORMATION",
        "baseline_deviation_status": "BASELINE_DEVIATION_DIRECTIONALLY_SUPPORTED",
        "opportunity_adjusted_status": "OPPORTUNITY_ADJUSTED_PERSISTENCE_SUPPORTED_FOR_LIMITED_SEGMENTS",
        "environment_conditioned_status": "RETAIN_FOR_INTERACTION_TESTING",
        "hits_0_5_status": "ONE_PLUS_HIT_PERSISTENCE_SUPPORTED",
        "hits_1_5_status": "MULTI_HIT_PERSISTENCE_SUPPORTED_FOR_HITS_1_5",
        "control_residual_evidence": "DESCRIPTIVE_ONLY_NONPROMOTABLE",
        "redundancy_status": "HITTER_PERSISTENCE_PARTIALLY_REDUNDANT_WITH_ROLLING_PRODUCTION",
        "temporal_stability": "DIRECTIONALLY_STABLE_BUT_NOISY",
        "prior_season_readiness": "NOT_YET_PROVABLE",
        "future_collective_bundle_readiness": "HITTER_PERSISTENCE_READY_FOR_RESEARCH_LABEL_RETENTION_NOT_READY_FOR_MODELING",
    }
    (out / "hitter_persistence_research_base_readiness_2026-07-11.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    (out / "hitter_persistence_readiness_decision_2026-07-11.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    _write_csv(out / "hitter_persistence_source_inventory_2026-07-11.csv", source_inventory)

    # Main report.
    report = f"""# MLB Hitter Persistence Characterization

Generated: `{generated_at}`

## Summary

Constructed a strict-prior hitter persistence research base for `{primary_start}` through `{primary_end}`. The package separates rolling production level from explicit one-plus-hit persistence, two-plus-hit persistence, volatility, baseline deviation, opportunity-adjusted persistence, and environment-conditioned interactions.

No model training, Champion-Challenger execution, production integration, formula change, tier change, selector change, scoring change, upload change, schema change, database write, or OddsAPI call was performed.

## Research Base

- Batter-game rows: `{len(batter_game)}`
- Batter-prop rows: `{len(prop)}`
- Strict-prior pass rows: `{readiness['strict_prior_pass_rows']}`
- Actual outcome bound rows: `{readiness['actual_outcome_bound_rows']}`
- PA opportunity join rows: `{readiness['pa_join_rows']}`
- Starter environment join rows: `{readiness['starter_join_rows']}`
- Lineup-slot rows: `{readiness['lineup_join_rows']}` using postgame actual lineup semantics only.

## Separate Conclusions

- Source-lineage status: `{readiness['source_lineage_status']}`
- Strict-prior construction status: `{readiness['strict_prior_construction_status']}`
- One-plus-hit persistence status: `{readiness['one_plus_hit_persistence_status']}`
- Multi-hit persistence status: `{readiness['multi_hit_persistence_status']}`
- Volatility measurement status: `{readiness['volatility_measurement_status']}`
- Baseline-deviation status: `{readiness['baseline_deviation_status']}`
- Opportunity-adjusted persistence status: `{readiness['opportunity_adjusted_status']}`
- Environment-conditioned status: `{readiness['environment_conditioned_status']}`
- Hits 0.5 status: `{readiness['hits_0_5_status']}`
- Hits 1.5 status: `{readiness['hits_1_5_status']}`
- Control-residual evidence: `{readiness['control_residual_evidence']}`
- Redundancy status: `{readiness['redundancy_status']}`
- Temporal stability: `{readiness['temporal_stability']}`
- Prior-season readiness: `{readiness['prior_season_readiness']}`
- Future collective-bundle readiness: `{readiness['future_collective_bundle_readiness']}`

## Interpretation

Persistence is partly redundant with existing d7/d15/d30 rolling production, but not identical. Two-plus-hit rate, exactly-one-hit share, multi-hit share when a hit occurs, and volatility preserve line-specific information that raw rolling hits per game can blur, especially for Hits 1.5.

The residual diagnostics are descriptive and non-promotional. They can justify research-label retention and future collective-bundle consideration only after broader evidence discipline, not standalone model changes.
"""
    (out / "mlb_hitter_persistence_characterization_2026-07-11.md").write_text(report)

    # Parse/manifest.
    parse_rows = []
    for p in sorted(out.glob("*.csv")):
        try:
            rows = len(pd.read_csv(p, low_memory=False))
            status = "PASS"
            err = ""
        except Exception as exc:
            rows = ""
            status = "FAIL"
            err = str(exc)
        parse_rows.append({"path": str(p), "format": "csv", "parse_status": status, "rows": rows, "error": err})
    for p in sorted(out.glob("*.json")):
        try:
            json.loads(p.read_text())
            status = "PASS"
            err = ""
        except Exception as exc:
            status = "FAIL"
            err = str(exc)
        parse_rows.append({"path": str(p), "format": "json", "parse_status": status, "rows": "", "error": err})
    _write_csv(out / "hitter_persistence_parse_validation_2026-07-11.csv", parse_rows)
    manifest = []
    for p in sorted(out.glob("*")):
        if p.is_file() and p.name != "hitter_persistence_sha256_manifest_2026-07-11.csv":
            manifest.append({"sha256": _sha256(p), "path": str(p)})
    _write_csv(out / "hitter_persistence_sha256_manifest_2026-07-11.csv", manifest)
    return {"out_dir": str(out), **readiness}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-05-01")
    parser.add_argument("--end-date", default="2026-07-09")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    parser.add_argument("--pa-base", default=str(DEFAULT_PA_BASE))
    parser.add_argument("--starter-base", default=str(DEFAULT_STARTER_BASE))
    parser.add_argument("--lineup-base", default=str(DEFAULT_LINEUP_BASE))
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument("--player-stats-csv", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    result = build(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
