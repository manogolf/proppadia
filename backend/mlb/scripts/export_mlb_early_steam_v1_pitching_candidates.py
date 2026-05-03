#!/usr/bin/env python3
"""Export V1 early-steam pitching candidates with workload-volatility filter.

CSV-only. Joins early-steam rows to prior pitcher game logs to compute
last_3_starts_outs_std, then applies the V1 pitching signal filter.
"""

from __future__ import annotations

import argparse
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PITCHER_MARKETS = {"pitcher_strikeouts", "pitcher_outs"}
DEFAULT_ROWS_CSV = "tmp/mlb_early_steam_multiday_results.csv"
DEFAULT_PITCHER_LOGS_CSV = "tmp/pitcher_game_logs_pybaseball_2026-03-15_to_2026-05-01.csv"
DEFAULT_OUT_CSV = "tmp/mlb_early_steam_v1_pitching_candidates.csv"
RAW_OUT_COLUMNS = [
    "date",
    "player_name",
    "market_key",
    "side",
    "line",
    "bookmaker_key",
    "price",
    "first_price",
    "second_price",
    "imp_move_early",
    "last_3_starts_outs_std",
]
RENAME_COLUMNS = {
    "price": "current_price",
    "first_price": "early_price",
    "second_price": "signal_price",
    "imp_move_early": "implied_move",
    "last_3_starts_outs_std": "workload_volatility",
}
OUT_COLUMNS = [
    "date",
    "player_name",
    "market_key",
    "side",
    "line",
    "bookmaker_key",
    "current_price",
    "early_price",
    "signal_price",
    "implied_move",
    "workload_volatility",
]
DEDUP_COLUMNS = ["date", "player_name", "market_key", "side", "line"]


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


def _pitcher_log_name_key(value: Any) -> str:
    parts = _norm_name(value).split()
    if len(parts) == 2:
        # Pybaseball often emits "Last, First"; after comma removal that becomes "last first".
        return f"{parts[1]} {parts[0]}"
    return " ".join(parts)


def _date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date.astype("string")


def _numeric(series: pd.Series | Any, index: pd.Index) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.Series(np.nan, index=index, dtype="float64")


def _selected_side_price(df: pd.DataFrame) -> pd.Series:
    if "second_price" in df.columns and pd.to_numeric(df["second_price"], errors="coerce").notna().any():
        return pd.to_numeric(df["second_price"], errors="coerce")
    over = _numeric(df["price_over_american"], df.index) if "price_over_american" in df.columns else pd.Series(np.nan, index=df.index)
    under = _numeric(df["price_under_american"], df.index) if "price_under_american" in df.columns else pd.Series(np.nan, index=df.index)
    side = df["side"].astype(str).str.lower()
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=df.index)


def _load_pitcher_logs(path: Path) -> dict[tuple[str, Any], pd.DataFrame]:
    df = pd.read_csv(path, low_memory=False)
    required = {"game_date", "pitcher_mlbam_id", "player_name", "outs_recorded"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"Pitcher logs missing required columns: {missing}")
    logs = df.copy()
    logs["game_date"] = _date_series(logs["game_date"])
    logs["pitcher_mlbam_id"] = pd.to_numeric(logs["pitcher_mlbam_id"], errors="coerce").astype("Int64")
    logs["pitcher_name_key"] = logs["player_name"].map(_pitcher_log_name_key)
    logs["outs_recorded"] = pd.to_numeric(logs["outs_recorded"], errors="coerce")
    logs = logs.sort_values(["pitcher_mlbam_id", "pitcher_name_key", "game_date"])

    lookup: dict[tuple[str, Any], pd.DataFrame] = {}
    for pid, group in logs.dropna(subset=["pitcher_mlbam_id"]).groupby("pitcher_mlbam_id"):
        lookup[("id", int(pid))] = group.sort_values("game_date").reset_index(drop=True)
    for name, group in logs[logs["pitcher_name_key"].ne("")].groupby("pitcher_name_key"):
        lookup[("name", name)] = group.sort_values("game_date").reset_index(drop=True)
    return lookup


def _prior_outs_std(group: pd.DataFrame, end_idx_exclusive: int) -> float:
    values = pd.to_numeric(group.iloc[max(0, end_idx_exclusive - 3) : end_idx_exclusive]["outs_recorded"], errors="coerce").dropna()
    if values.empty:
        return np.nan
    return float(values.std(ddof=0))


def _lookup_outs_std(lookup: dict[tuple[str, Any], pd.DataFrame], row: pd.Series) -> float:
    candidate_date = pd.to_datetime(row.get("date"), errors="coerce")
    if pd.isna(candidate_date):
        return np.nan

    groups: list[pd.DataFrame | None] = []
    try:
        pitcher_id = row.get("candidate_mlbam_id")
        if pd.notna(pitcher_id):
            groups.append(lookup.get(("id", int(pitcher_id))))
    except Exception:
        pass
    name_key = str(row.get("candidate_name_key") or "")
    if name_key:
        groups.append(lookup.get(("name", name_key)))

    for group in groups:
        if group is None or group.empty:
            continue
        dates = pd.to_datetime(group["game_date"], errors="coerce")
        prior = group[dates < candidate_date]
        if prior.empty:
            continue
        return _prior_outs_std(group, int(prior.index[-1]) + 1)
    return np.nan


def build_candidates(
    rows: pd.DataFrame,
    *,
    lookup: dict[tuple[str, Any], pd.DataFrame],
    export_date: str,
    min_imp_move: float,
    max_imp_move: float,
    min_outs_std: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_col = "date" if "date" in rows.columns else "game_date"
    if date_col not in rows.columns:
        raise SystemExit("Rows CSV must include date or game_date.")
    missing = sorted({"market_key", "side", "imp_move_early"} - set(rows.columns))
    if missing:
        raise SystemExit(f"Rows CSV missing required columns: {missing}")

    work = rows.copy()
    work["date"] = _date_series(work[date_col])
    if export_date:
        work = work[work["date"].astype(str).eq(str(export_date))].copy()
    work["market_key"] = work["market_key"].map(lambda v: _clean_text(v).lower())
    work["side"] = work["side"].map(lambda v: _clean_text(v).lower())
    work["line"] = pd.to_numeric(work.get("line", np.nan), errors="coerce")
    work["imp_move_early"] = pd.to_numeric(work["imp_move_early"], errors="coerce")
    work = work[
        work["market_key"].isin(PITCHER_MARKETS)
        & work["imp_move_early"].between(float(min_imp_move), float(max_imp_move), inclusive="both")
    ].copy()

    if work.empty:
        for col in RAW_OUT_COLUMNS:
            if col not in work.columns:
                work[col] = np.nan
        return work[RAW_OUT_COLUMNS], work

    if "player_id" in work.columns:
        work["candidate_mlbam_id"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    else:
        work["candidate_mlbam_id"] = pd.Series([pd.NA] * len(work), dtype="Int64")
    name_col = "player_name" if "player_name" in work.columns else "player"
    work["player_name"] = work[name_col].map(_clean_text) if name_col in work.columns else ""
    work["candidate_name_key"] = work["player_name"].map(_norm_name)
    work["bookmaker_key"] = work["bookmaker_key"].map(lambda v: _clean_text(v).lower()) if "bookmaker_key" in work.columns else ""
    work["first_price"] = pd.to_numeric(work.get("first_price", np.nan), errors="coerce")
    work["second_price"] = pd.to_numeric(work.get("second_price", np.nan), errors="coerce")
    work["price"] = _selected_side_price(work)
    work["last_3_starts_outs_std"] = work.apply(lambda row: _lookup_outs_std(lookup, row), axis=1)

    filtered = work[pd.to_numeric(work["last_3_starts_outs_std"], errors="coerce").ge(float(min_outs_std))].copy()
    return filtered[RAW_OUT_COLUMNS].sort_values(["date", "market_key", "side", "line", "player_name"]), filtered


def finalize_export(candidates: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    for col in RAW_OUT_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan
    for col in ["line", "price", "first_price", "second_price", "imp_move_early", "last_3_starts_outs_std"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = (
        out.sort_values(
            ["date", "player_name", "market_key", "side", "line", "price", "bookmaker_key"],
            ascending=[True, True, True, True, True, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=DEDUP_COLUMNS, keep="first")
        .rename(columns=RENAME_COLUMNS)
        .sort_values(["market_key", "player_name", "side", "line"], na_position="last")
        .reset_index(drop=True)
    )
    return out[OUT_COLUMNS]


def canonical_wagers_path(export_date: str) -> Path | None:
    if not export_date:
        return None
    return Path("backend/mlb/exports/v1_wagers") / str(export_date) / "wagers.csv"


def dated_rows_fallback_path(export_date: str) -> Path | None:
    if not export_date:
        return None
    path = Path("tmp") / f"mlb_early_steam_rows_{export_date}.csv"
    return path if path.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-csv", default=DEFAULT_ROWS_CSV)
    parser.add_argument("--pitcher-logs-csv", default=DEFAULT_PITCHER_LOGS_CSV)
    parser.add_argument("--date", default="")
    parser.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    parser.add_argument("--min-imp-move", type=float, default=0.02)
    parser.add_argument("--max-imp-move", type=float, default=0.05)
    parser.add_argument("--min-outs-std", type=float, default=2.0)
    args = parser.parse_args()

    rows_path = Path(args.rows_csv)
    logs_path = Path(args.pitcher_logs_csv)
    if not rows_path.exists():
        raise SystemExit(f"Rows CSV not found: {rows_path}")
    if not logs_path.exists():
        raise SystemExit(f"Pitcher logs CSV not found: {logs_path}")

    rows = pd.read_csv(rows_path, low_memory=False)
    lookup = _load_pitcher_logs(logs_path)
    export_date = str(args.date or "")
    rows_source = rows_path
    candidates, filtered = build_candidates(
        rows,
        lookup=lookup,
        export_date=export_date,
        min_imp_move=float(args.min_imp_move),
        max_imp_move=float(args.max_imp_move),
        min_outs_std=float(args.min_outs_std),
    )
    fallback_rows_path = dated_rows_fallback_path(export_date)
    if candidates.empty and fallback_rows_path is not None and fallback_rows_path != rows_path:
        rows_source = fallback_rows_path
        rows = pd.read_csv(fallback_rows_path, low_memory=False)
        candidates, filtered = build_candidates(
            rows,
            lookup=lookup,
            export_date=export_date,
            min_imp_move=float(args.min_imp_move),
            max_imp_move=float(args.max_imp_move),
            min_outs_std=float(args.min_outs_std),
        )
    out = finalize_export(candidates)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    canonical_csv = canonical_wagers_path(str(args.date or ""))
    if canonical_csv is not None and canonical_csv != out_csv:
        canonical_csv.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(canonical_csv, index=False)

    bets = int(len(out))
    raw_bets = int(len(filtered))
    win_rate = np.nan
    if "outcome" in filtered.columns and raw_bets:
        outcome = filtered["outcome"].map(lambda v: _clean_text(v).lower())
        resolved = outcome.isin(["win", "loss"])
        if int(resolved.sum()) > 0:
            win_rate = float(outcome.eq("win").sum() / resolved.sum())
    print(
        "[early-steam-v1-pitching-candidates] "
        f"date={args.date or 'all'} bets={bets} raw_candidates={raw_bets} "
        f"win_rate={'NA' if pd.isna(win_rate) else f'{win_rate:.3f}'} "
        f"rows_csv={rows_source} "
        f"out_csv={out_csv}"
        + (f" canonical_csv={canonical_csv}" if canonical_csv is not None and canonical_csv != out_csv else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
