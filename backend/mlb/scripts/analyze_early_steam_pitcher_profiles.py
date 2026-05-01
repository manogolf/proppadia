#!/usr/bin/env python3
"""Join early-steam pitcher candidates to prior pybaseball pitcher profiles.

CSV-only analysis. No database writes.
"""

from __future__ import annotations

import argparse
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PITCHER_MARKETS = {"pitcher_outs", "pitcher_strikeouts"}
DEFAULT_EARLY_STEAM = "tmp/mlb_early_steam_multiday_results.csv"
DEFAULT_PITCHER_LOGS = "tmp/pitcher_game_logs_pybaseball_2026-04-16_to_2026-05-01.csv"
DEFAULT_OUT = "tmp/mlb_early_steam_pitcher_profile_analysis.csv"
DEFAULT_SUMMARY = "tmp/mlb_early_steam_pitcher_profile_summary.csv"
DEFAULT_STABLE_SUMMARY = "tmp/mlb_early_steam_pitcher_profile_stable_summary.csv"


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _norm_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(",", " ")
    keep = [ch if ch.isalnum() or ch.isspace() else " " for ch in text]
    return " ".join("".join(keep).split())


def _name_key(value: Any) -> str:
    norm = _norm_name(value)
    parts = norm.split()
    if len(parts) == 2:
        # Pybaseball often emits "Last, First"; after comma removal that becomes "last first".
        return f"{parts[1]} {parts[0]}"
    return norm


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date.astype("string")


def _bucket_days_rest(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "unknown"
    if pd.isna(v):
        return "unknown"
    if v <= 3:
        return "<=3"
    if v == 4:
        return "4"
    if v == 5:
        return "5"
    if v == 6:
        return "6"
    return "7+"


def _bucket_days_rest_coarse(value: Any) -> str:
    bucket = _bucket_days_rest(value)
    if bucket in {"<=3", "4"}:
        return "short"
    if bucket in {"5", "6"}:
        return "normal"
    if bucket == "7+":
        return "long"
    return "unknown"


def _bucket_outs(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "unknown"
    if pd.isna(v):
        return "unknown"
    if v < 45:
        return "<45"
    if v < 51:
        return "45-50"
    if v < 57:
        return "51-56"
    return "57+"


def _bucket_outs_coarse(value: Any) -> str:
    bucket = _bucket_outs(value)
    if bucket == "<45":
        return "low"
    if bucket in {"45-50", "51-56"}:
        return "typical"
    if bucket == "57+":
        return "high"
    return "unknown"


def _bucket_strikeouts(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "unknown"
    if pd.isna(v):
        return "unknown"
    if v < 12:
        return "<12"
    if v < 18:
        return "12-17"
    if v < 24:
        return "18-23"
    return "24+"


def _bucket_strikeouts_coarse(value: Any) -> str:
    bucket = _bucket_strikeouts(value)
    if bucket == "<12":
        return "low"
    if bucket in {"12-17", "18-23"}:
        return "typical"
    if bucket == "24+":
        return "high"
    return "unknown"


def _bucket_line(row: pd.Series) -> str:
    try:
        line = float(row.get("line"))
    except Exception:
        return "unknown"
    if pd.isna(line):
        return "unknown"
    market = _clean_text(row.get("market_key")).lower()
    if market == "pitcher_outs":
        if line <= 15.5:
            return "low"
        if line <= 17.5:
            return "standard"
        return "high"
    if market == "pitcher_strikeouts":
        if line <= 3.5:
            return "low"
        if line <= 5.5:
            return "standard"
        return "high"
    return "unknown"


def _load_candidates(path: Path, *, min_imp_move: float, max_imp_move: float) -> pd.DataFrame:
    df = pd.read_csv(path)
    date_col = "date" if "date" in df.columns else "game_date"
    if date_col not in df.columns:
        raise SystemExit("Early-steam rows must include date or game_date.")
    for col in ["market_key", "imp_move_early"]:
        if col not in df.columns:
            raise SystemExit(f"Early-steam rows missing required column: {col}")

    work = df.copy()
    work["date"] = _to_date(work[date_col])
    work["market_key"] = work["market_key"].map(lambda v: _clean_text(v).lower())
    work["imp_move_early"] = pd.to_numeric(work["imp_move_early"], errors="coerce")
    work = work[
        work["market_key"].isin(PITCHER_MARKETS)
        & work["imp_move_early"].between(float(min_imp_move), float(max_imp_move), inclusive="both")
    ].copy()
    work["side"] = work.get("side", "").map(lambda v: _clean_text(v).lower()) if "side" in work.columns else ""
    work["line"] = pd.to_numeric(work.get("line", np.nan), errors="coerce")
    if "player_id" in work.columns:
        work["candidate_mlbam_id"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    else:
        work["candidate_mlbam_id"] = pd.Series([pd.NA] * len(work), dtype="Int64")
    name_source = "player_name" if "player_name" in work.columns else "player"
    work["candidate_name_key"] = work[name_source].map(_norm_name) if name_source in work.columns else ""
    return work


def _load_pitcher_logs(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"game_date", "pitcher_mlbam_id", "player_name"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"Pitcher logs missing required columns: {missing}")
    logs = df.copy()
    logs["game_date"] = _to_date(logs["game_date"])
    logs["pitcher_mlbam_id"] = pd.to_numeric(logs["pitcher_mlbam_id"], errors="coerce").astype("Int64")
    logs["pitcher_name_key"] = logs["player_name"].map(_name_key)
    logs["is_starter"] = logs.get("is_starter", False).astype(str).str.lower().isin({"true", "1", "yes"})
    for col in [
        "days_rest",
        "last_3_starts_strikeouts",
        "last_3_starts_outs",
        "last_5_starts_era",
        "hits_allowed",
        "walks",
        "home_runs_allowed",
    ]:
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce")
        else:
            logs[col] = np.nan
    return logs.sort_values(["pitcher_mlbam_id", "pitcher_name_key", "game_date"])


def _recent_average(logs: pd.DataFrame, idx: int, col: str, window: int = 3) -> float:
    prior = logs.iloc[max(0, idx - window) : idx]
    if prior.empty:
        return np.nan
    return float(pd.to_numeric(prior[col], errors="coerce").mean())


def _profile_lookup(logs: pd.DataFrame) -> dict[tuple[str, Any], pd.DataFrame]:
    lookup: dict[tuple[str, Any], pd.DataFrame] = {}
    for pid, group in logs.dropna(subset=["pitcher_mlbam_id"]).groupby("pitcher_mlbam_id"):
        lookup[("id", int(pid))] = group.sort_values("game_date").reset_index(drop=True)
    for name, group in logs[logs["pitcher_name_key"].ne("")].groupby("pitcher_name_key"):
        lookup[("name", name)] = group.sort_values("game_date").reset_index(drop=True)
    return lookup


def _latest_prior_profile(
    lookup: dict[tuple[str, Any], pd.DataFrame],
    *,
    pitcher_id: Any,
    name_key: str,
    candidate_date: str,
) -> dict[str, Any]:
    date_value = pd.to_datetime(candidate_date, errors="coerce")
    if pd.isna(date_value):
        return {}

    groups = []
    try:
        if pd.notna(pitcher_id):
            groups.append(("id", lookup.get(("id", int(pitcher_id)))))
    except Exception:
        pass
    if name_key:
        groups.append(("name", lookup.get(("name", name_key))))

    for match_type, group in groups:
        if group is None or group.empty:
            continue
        dates = pd.to_datetime(group["game_date"], errors="coerce")
        prior = group[dates < date_value].copy()
        if prior.empty:
            continue
        idx = prior.index[-1]
        row = group.loc[idx]
        return {
            "profile_match_type": match_type,
            "profile_game_date": row.get("game_date"),
            "profile_pitcher_mlbam_id": row.get("pitcher_mlbam_id"),
            "profile_player_name": row.get("player_name"),
            "days_rest": row.get("days_rest"),
            "last_3_starts_strikeouts": row.get("last_3_starts_strikeouts"),
            "last_3_starts_outs": row.get("last_3_starts_outs"),
            "last_5_starts_era": row.get("last_5_starts_era"),
            "recent_hits_allowed_avg": _recent_average(group, idx, "hits_allowed"),
            "recent_walks_avg": _recent_average(group, idx, "walks"),
            "recent_hr_allowed_avg": _recent_average(group, idx, "home_runs_allowed"),
            "suspected_starter": bool(row.get("is_starter")),
        }
    return {}


def build_analysis(candidates: pd.DataFrame, logs: pd.DataFrame) -> pd.DataFrame:
    lookup = _profile_lookup(logs)
    profile_rows = []
    for _, row in candidates.iterrows():
        profile_rows.append(
            _latest_prior_profile(
                lookup,
                pitcher_id=row.get("candidate_mlbam_id"),
                name_key=str(row.get("candidate_name_key") or ""),
                candidate_date=str(row.get("date") or ""),
            )
        )
    profiles = pd.DataFrame(profile_rows, index=candidates.index)
    out = pd.concat([candidates.reset_index(drop=True), profiles.reset_index(drop=True)], axis=1)
    out["profile_matched"] = out["profile_match_type"].notna() if "profile_match_type" in out.columns else False
    out["days_rest_bucket"] = out.get("days_rest", np.nan).map(_bucket_days_rest)
    out["last_3_starts_outs_bucket"] = out.get("last_3_starts_outs", np.nan).map(_bucket_outs)
    out["last_3_starts_strikeouts_bucket"] = out.get("last_3_starts_strikeouts", np.nan).map(_bucket_strikeouts)
    out["days_rest_group"] = out.get("days_rest", np.nan).map(_bucket_days_rest_coarse)
    out["last_3_starts_outs_group"] = out.get("last_3_starts_outs", np.nan).map(_bucket_outs_coarse)
    out["last_3_starts_strikeouts_group"] = out.get("last_3_starts_strikeouts", np.nan).map(_bucket_strikeouts_coarse)
    out["line_bucket"] = out.apply(_bucket_line, axis=1)
    return out


def _with_outcomes(analysis: pd.DataFrame) -> pd.DataFrame:
    work = analysis.copy()
    outcome = work.get("outcome", pd.Series([""] * len(work), index=work.index)).map(lambda v: _clean_text(v).lower())
    work["__win"] = outcome.eq("win")
    work["__loss"] = outcome.eq("loss")
    work["__pnl"] = pd.to_numeric(work.get("pnl", np.nan), errors="coerce")
    work["imp_move_early"] = pd.to_numeric(work.get("imp_move_early", np.nan), errors="coerce")
    return work


def _summarize_groups(work: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    metric_cols = ["bets", "wins", "win_rate", "profit", "roi", "avg_imp_move_early", "matched_profiles"]
    if work.empty:
        return pd.DataFrame(columns=group_cols + metric_cols)
    summary = (
        work.groupby(group_cols, dropna=False)
        .agg(
            bets=("market_key", "size"),
            wins=("__win", "sum"),
            losses=("__loss", "sum"),
            profit=("__pnl", "sum"),
            avg_imp_move_early=("imp_move_early", "mean"),
            matched_profiles=("profile_matched", "sum"),
        )
        .reset_index()
    )
    resolved = summary["wins"] + summary["losses"]
    summary["win_rate"] = np.where(resolved > 0, summary["wins"] / resolved, np.nan)
    summary["roi"] = np.where(summary["bets"] > 0, summary["profit"] / summary["bets"], np.nan)
    return summary.drop(columns=["losses"])[group_cols + metric_cols]


def build_summary(analysis: pd.DataFrame) -> pd.DataFrame:
    work = _with_outcomes(analysis)
    group_cols = [
        "market_key",
        "side",
        "line",
        "days_rest_bucket",
        "last_3_starts_outs_bucket",
        "last_3_starts_strikeouts_bucket",
    ]
    return _summarize_groups(work, group_cols)


def build_stable_summary(analysis: pd.DataFrame, *, min_bets: int) -> pd.DataFrame:
    work = _with_outcomes(analysis)
    segment_specs = [
        ("market_side", ["market_key", "side"]),
        ("market_side_line_bucket", ["market_key", "side", "line_bucket"]),
        ("market_side_rest", ["market_key", "side", "days_rest_group"]),
        ("market_side_recent_outs", ["market_key", "side", "last_3_starts_outs_group"]),
        ("market_side_recent_ks", ["market_key", "side", "last_3_starts_strikeouts_group"]),
        (
            "market_side_profile",
            [
                "market_key",
                "side",
                "days_rest_group",
                "last_3_starts_outs_group",
                "last_3_starts_strikeouts_group",
            ],
        ),
    ]
    frames = []
    for family, cols in segment_specs:
        summary = _summarize_groups(work, cols)
        if summary.empty:
            continue
        summary.insert(0, "segment_family", family)
        summary["segment_key"] = summary[cols].astype(str).agg("|".join, axis=1)
        for col in [
            "line_bucket",
            "days_rest_group",
            "last_3_starts_outs_group",
            "last_3_starts_strikeouts_group",
        ]:
            if col not in summary.columns:
                summary[col] = ""
        frames.append(summary)
    base_cols = [
        "segment_family",
        "segment_key",
        "market_key",
        "side",
        "line_bucket",
        "days_rest_group",
        "last_3_starts_outs_group",
        "last_3_starts_strikeouts_group",
        "bets",
        "wins",
        "win_rate",
        "profit",
        "roi",
        "avg_imp_move_early",
        "matched_profiles",
        "is_stable",
    ]
    if not frames:
        return pd.DataFrame(columns=base_cols)
    out = pd.concat(frames, ignore_index=True, sort=False)
    out["is_stable"] = pd.to_numeric(out["bets"], errors="coerce").fillna(0).ge(int(min_bets))
    out = out[base_cols]
    return out.sort_values(["is_stable", "bets", "roi"], ascending=[False, False, False], na_position="last")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--early-steam-csv", default=DEFAULT_EARLY_STEAM)
    parser.add_argument("--pitcher-logs-csv", default=DEFAULT_PITCHER_LOGS)
    parser.add_argument("--out-csv", default=DEFAULT_OUT)
    parser.add_argument("--summary-csv", default=DEFAULT_SUMMARY)
    parser.add_argument("--stable-summary-csv", default=DEFAULT_STABLE_SUMMARY)
    parser.add_argument("--stable-min-bets", type=int, default=5)
    parser.add_argument("--min-imp-move", type=float, default=0.02)
    parser.add_argument("--max-imp-move", type=float, default=0.05)
    args = parser.parse_args()

    early_path = Path(args.early_steam_csv)
    logs_path = Path(args.pitcher_logs_csv)
    if not early_path.exists():
        raise SystemExit(f"Early-steam CSV not found: {early_path}")
    if not logs_path.exists():
        raise SystemExit(f"Pitcher logs CSV not found: {logs_path}")

    candidates = _load_candidates(early_path, min_imp_move=args.min_imp_move, max_imp_move=args.max_imp_move)
    logs = _load_pitcher_logs(logs_path)
    analysis = build_analysis(candidates, logs)
    summary = build_summary(analysis)
    stable_summary = build_stable_summary(analysis, min_bets=args.stable_min_bets)

    out_csv = Path(args.out_csv)
    summary_csv = Path(args.summary_csv)
    stable_summary_csv = Path(args.stable_summary_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    stable_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    analysis.to_csv(out_csv, index=False)
    summary.to_csv(summary_csv, index=False)
    stable_summary.to_csv(stable_summary_csv, index=False)

    matched = int(analysis["profile_matched"].sum()) if "profile_matched" in analysis.columns else 0
    unmatched = int(len(analysis) - matched)
    print(
        "[early-steam-pitcher-profiles] "
        f"candidates={len(candidates)} matched={matched} unmatched={unmatched} "
        f"out_csv={out_csv} summary_csv={summary_csv} stable_summary_csv={stable_summary_csv}"
    )
    stable = stable_summary[stable_summary["is_stable"]] if not stable_summary.empty else stable_summary
    if not stable.empty:
        print(f"[early-steam-pitcher-profiles] stable segments min_bets={args.stable_min_bets}")
        print(stable.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
