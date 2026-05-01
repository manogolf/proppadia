#!/usr/bin/env python3
"""Build CSV-only pitcher game logs from pybaseball Statcast data.

This is a fast historical backbone for pitcher prop modeling/reporting. It does
not write to the database and does not change model/reconcile code.

Notes:
- Uses pybaseball Statcast pitch-level data and aggregates to pitcher-game rows.
- MLBAM pitcher IDs come directly from Statcast.
- Chadwick Register is used as a sanity check only; IDs are not overridden.
- Earned runs are not available from Statcast pitch rows, so `earned_runs` is
  emitted as blank until a Retrosheet/Chadwick game-log source is joined.

Example:
  python -m backend.mlb.scripts.build_pitcher_game_logs_pybaseball \
    --start-year 2024 \
    --end-year 2024 \
    --chadwick-register-csv backend/mlb/data/raw/retrosheet/chadwick_register/people.csv \
    --out-csv tmp/pitcher_game_logs_pybaseball.csv
"""

from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import numpy as np
import pandas as pd


DEFAULT_REGISTER = "backend/mlb/data/raw/retrosheet/chadwick_register/people.csv"
DEFAULT_OUT = "tmp/pitcher_game_logs_pybaseball.csv"

HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
NON_BF_EVENTS = {
    "caught_stealing_2b",
    "caught_stealing_3b",
    "caught_stealing_home",
    "pickoff_1b",
    "pickoff_2b",
    "pickoff_3b",
    "pickoff_caught_stealing_2b",
    "pickoff_caught_stealing_3b",
    "pickoff_caught_stealing_home",
    "stolen_base_2b",
    "stolen_base_3b",
    "stolen_base_home",
}
EVENT_OUTS = {
    "field_out": 1,
    "force_out": 1,
    "fielders_choice_out": 1,
    "sac_bunt": 1,
    "sac_fly": 1,
    "strikeout": 1,
    "strikeout_double_play": 2,
    "double_play": 2,
    "grounded_into_double_play": 2,
    "triple_play": 3,
}
OUTPUT_COLUMNS = [
    "game_date",
    "game_pk",
    "pitcher_mlbam_id",
    "chadwick_key_retro",
    "chadwick_mlbam_match",
    "player_name",
    "team",
    "opponent",
    "is_starter",
    "innings_pitched",
    "outs_recorded",
    "strikeouts",
    "walks",
    "hits_allowed",
    "earned_runs",
    "home_runs_allowed",
    "batters_faced",
    "season",
    "days_rest",
    "last_3_starts_strikeouts",
    "last_3_starts_outs",
    "last_5_starts_era",
]


def _import_pybaseball():
    os.environ.setdefault("PYBASEBALL_CACHE", "tmp/pybaseball_cache")
    os.environ.setdefault("MPLCONFIGDIR", "tmp/matplotlib_cache")
    try:
        from pybaseball import cache, statcast  # type: ignore
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "pybaseball is not installed. Install dependencies first, e.g. "
            "`pip install -r requirements.txt` or `.venv/bin/python -m pip install pybaseball`."
        ) from exc
    return cache, statcast


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _year_bounds(start_year: int, end_year: int) -> tuple[date, date]:
    if start_year > end_year:
        raise SystemExit("--start-year must be <= --end-year")
    start = date(int(start_year), 1, 1)
    end = date(int(end_year), 12, 31)
    today = date.today()
    if end > today:
        end = today
    return start, end


def _date_bounds(args: argparse.Namespace) -> tuple[date, date]:
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date must be provided together")
        start = date.fromisoformat(str(args.start_date))
        end = date.fromisoformat(str(args.end_date))
        if start > end:
            raise SystemExit("--start-date must be <= --end-date")
        today = date.today()
        return start, min(end, today)
    return _year_bounds(args.start_year, args.end_year)


def _chunks(start: date, end: date, days: int) -> Iterable[tuple[date, date]]:
    cur = start
    step = max(1, int(days))
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=step - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _load_chadwick_map(path: Path) -> Mapping[int, str]:
    if not path.exists():
        print(f"[pitcher-pybaseball] Chadwick Register not found; continuing without mapping check: {path}")
        return {}
    df = pd.read_csv(path, dtype=str, usecols=lambda c: c in {"key_mlbam", "key_retro"})
    if not {"key_mlbam", "key_retro"}.issubset(df.columns):
        print(f"[pitcher-pybaseball] Chadwick Register missing key_mlbam/key_retro; continuing: {path}")
        return {}
    work = df.dropna(subset=["key_mlbam", "key_retro"]).copy()
    work["key_mlbam_int"] = pd.to_numeric(work["key_mlbam"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["key_mlbam_int"])
    return {
        int(row["key_mlbam_int"]): str(row["key_retro"]).strip()
        for _, row in work.iterrows()
        if str(row["key_retro"]).strip()
    }


def _ip_from_outs(outs: pd.Series) -> pd.Series:
    values = pd.to_numeric(outs, errors="coerce")
    whole = np.floor(values / 3)
    rem = values % 3
    return whole + (rem / 10.0)


def _safe_col(df: pd.DataFrame, col: str, default: Any = np.nan) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def _pitching_team(row: pd.Series) -> str:
    topbot = _clean_text(row.get("inning_topbot")).lower()
    if topbot == "top":
        return _clean_text(row.get("home_team")).upper()
    if topbot == "bot":
        return _clean_text(row.get("away_team")).upper()
    return ""


def _opponent_team(row: pd.Series) -> str:
    topbot = _clean_text(row.get("inning_topbot")).lower()
    if topbot == "top":
        return _clean_text(row.get("away_team")).upper()
    if topbot == "bot":
        return _clean_text(row.get("home_team")).upper()
    return ""


def _starter_keys(pa: pd.DataFrame) -> set[tuple[Any, Any]]:
    if pa.empty:
        return set()
    work = pa.copy()
    work["inning_num"] = pd.to_numeric(_safe_col(work, "inning"), errors="coerce")
    work["at_bat_num"] = pd.to_numeric(_safe_col(work, "at_bat_number"), errors="coerce")
    work = work.sort_values(["game_pk", "team", "inning_num", "at_bat_num", "pitch_number"])
    first = work.groupby(["game_pk", "team"], dropna=False).head(1)
    return {(row["game_pk"], row["pitcher"]) for _, row in first.iterrows()}


def _aggregate_statcast(raw: pd.DataFrame, *, chadwick_map: Mapping[int, str]) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = raw.copy()
    required = {"game_date", "game_pk", "pitcher", "events", "at_bat_number", "pitch_number"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"pybaseball Statcast output missing required columns: {missing}")

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date.astype("string")
    df["pitcher"] = pd.to_numeric(df["pitcher"], errors="coerce").astype("Int64")
    df["pitch_number"] = pd.to_numeric(df["pitch_number"], errors="coerce")
    df["team"] = df.apply(_pitching_team, axis=1)
    df["opponent"] = df.apply(_opponent_team, axis=1)
    df["events_norm"] = _safe_col(df, "events", "").fillna("").astype(str).str.strip().str.lower()

    event_rows = df[df["events_norm"].ne("")].copy()
    event_rows = event_rows.sort_values(["game_pk", "at_bat_number", "pitch_number"])
    pa = event_rows.groupby(["game_pk", "at_bat_number", "pitcher"], dropna=False).tail(1).copy()
    pa = pa[~pa["events_norm"].isin(NON_BF_EVENTS)].copy()

    if pa.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    pa["bf"] = 1
    pa["strikeout"] = pa["events_norm"].isin(STRIKEOUT_EVENTS).astype(int)
    pa["walk"] = pa["events_norm"].isin(WALK_EVENTS).astype(int)
    pa["hit"] = pa["events_norm"].isin(HIT_EVENTS).astype(int)
    pa["home_run"] = pa["events_norm"].eq("home_run").astype(int)
    pa["outs"] = pa["events_norm"].map(lambda ev: EVENT_OUTS.get(str(ev), 0)).astype(int)
    starter_keys = _starter_keys(pa)

    name_col = "player_name" if "player_name" in pa.columns else ""
    grouped = (
        pa.groupby(["game_date", "game_pk", "pitcher", "team", "opponent"], dropna=False)
        .agg(
            player_name=(name_col, "first") if name_col else ("pitcher", "first"),
            batters_faced=("bf", "sum"),
            strikeouts=("strikeout", "sum"),
            walks=("walk", "sum"),
            hits_allowed=("hit", "sum"),
            home_runs_allowed=("home_run", "sum"),
            outs_recorded=("outs", "sum"),
        )
        .reset_index()
    )
    grouped = grouped.rename(columns={"pitcher": "pitcher_mlbam_id"})
    grouped["pitcher_mlbam_id"] = pd.to_numeric(grouped["pitcher_mlbam_id"], errors="coerce").astype("Int64")
    grouped["season"] = pd.to_datetime(grouped["game_date"], errors="coerce").dt.year.astype("Int64")
    grouped["innings_pitched"] = _ip_from_outs(grouped["outs_recorded"])
    grouped["earned_runs"] = np.nan
    grouped["is_starter"] = grouped.apply(
        lambda r: (r["game_pk"], r["pitcher_mlbam_id"]) in starter_keys,
        axis=1,
    )
    grouped["chadwick_key_retro"] = grouped["pitcher_mlbam_id"].map(
        lambda v: chadwick_map.get(int(v)) if pd.notna(v) else ""
    )
    grouped["chadwick_mlbam_match"] = grouped["chadwick_key_retro"].fillna("").astype(str).ne("")
    return grouped


def _add_rolling_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["game_date_dt"] = pd.to_datetime(out["game_date"], errors="coerce")
    out = out.sort_values(["pitcher_mlbam_id", "game_date_dt", "game_pk"]).reset_index(drop=True)
    out["days_rest"] = out.groupby("pitcher_mlbam_id")["game_date_dt"].diff().dt.days
    out["last_3_starts_strikeouts"] = np.nan
    out["last_3_starts_outs"] = np.nan
    out["last_5_starts_era"] = np.nan

    for _pid, idx in out[out["is_starter"].eq(True)].groupby("pitcher_mlbam_id").groups.items():
        starts = out.loc[list(idx)].sort_values(["game_date_dt", "game_pk"]).copy()
        last3_k = starts["strikeouts"].shift(1).rolling(3, min_periods=1).sum()
        last3_outs = starts["outs_recorded"].shift(1).rolling(3, min_periods=1).sum()
        # Earned runs are unavailable in Statcast; this stays NaN until a game-log ER source is joined.
        last5_er = pd.to_numeric(starts["earned_runs"], errors="coerce").shift(1).rolling(5, min_periods=1).sum()
        last5_outs = starts["outs_recorded"].shift(1).rolling(5, min_periods=1).sum()
        last5_era = np.where(last5_outs > 0, last5_er * 27.0 / last5_outs, np.nan)
        out.loc[starts.index, "last_3_starts_strikeouts"] = last3_k.values
        out.loc[starts.index, "last_3_starts_outs"] = last3_outs.values
        out.loc[starts.index, "last_5_starts_era"] = last5_era

    return out.drop(columns=["game_date_dt"])


def _missing_fields_count(df: pd.DataFrame) -> dict[str, int]:
    fields = [
        "game_date",
        "pitcher_mlbam_id",
        "player_name",
        "team",
        "opponent",
        "innings_pitched",
        "outs_recorded",
        "strikeouts",
        "walks",
        "hits_allowed",
        "earned_runs",
        "home_runs_allowed",
        "batters_faced",
        "season",
    ]
    return {field: int(df[field].isna().sum()) if field in df.columns else len(df) for field in fields}


def build_dataset(
    *,
    start: date,
    end: date,
    chadwick_register_csv: Path,
    chunk_days: int,
) -> pd.DataFrame:
    cache, statcast = _import_pybaseball()
    try:
        cache.enable()
    except Exception:
        pass

    chadwick_map = _load_chadwick_map(chadwick_register_csv)
    frames: list[pd.DataFrame] = []
    for chunk_start, chunk_end in _chunks(start, end, chunk_days):
        start_s = chunk_start.isoformat()
        end_s = chunk_end.isoformat()
        print(f"[pitcher-pybaseball] fetching statcast {start_s}..{end_s}")
        raw = statcast(start_dt=start_s, end_dt=end_s, verbose=False, parallel=True)
        if raw is None or raw.empty:
            continue
        agg = _aggregate_statcast(raw, chadwick_map=chadwick_map)
        if not agg.empty:
            frames.append(agg)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["game_date", "game_pk", "pitcher_mlbam_id"], keep="last")
    out = _add_rolling_fields(out)
    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan
    return out[OUTPUT_COLUMNS].sort_values(["game_date", "team", "player_name"], na_position="last")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--start-date", default="", help="Optional YYYY-MM-DD override for smoke tests.")
    parser.add_argument("--end-date", default="", help="Optional YYYY-MM-DD override for smoke tests.")
    parser.add_argument("--chadwick-register-csv", default=DEFAULT_REGISTER)
    parser.add_argument("--out-csv", default=DEFAULT_OUT)
    parser.add_argument("--chunk-days", type=int, default=7)
    args = parser.parse_args()

    start, end = _date_bounds(args)
    out = build_dataset(
        start=start,
        end=end,
        chadwick_register_csv=Path(args.chadwick_register_csv),
        chunk_days=args.chunk_days,
    )
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    rows = int(len(out))
    unique_pitchers = int(out["pitcher_mlbam_id"].nunique()) if rows else 0
    avg_games = float(rows / unique_pitchers) if unique_pitchers else 0.0
    missing = _missing_fields_count(out)
    chadwick_matches = int(out["chadwick_mlbam_match"].sum()) if rows and "chadwick_mlbam_match" in out.columns else 0

    print(
        "[pitcher-pybaseball] "
        f"rows={rows} unique_pitchers={unique_pitchers} games_per_pitcher_avg={avg_games:.2f} "
        f"chadwick_mlbam_matches={chadwick_matches} out_csv={out_csv}"
    )
    print("[pitcher-pybaseball] missing_fields_count")
    for field, count in missing.items():
        print(f"  {field}: {count}")
    if rows and out["earned_runs"].isna().all():
        print("[pitcher-pybaseball] note: earned_runs unavailable from Statcast aggregate; column left blank.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
