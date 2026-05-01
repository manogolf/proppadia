#!/usr/bin/env python3
"""Audit coverage of pybaseball pitcher-game-log CSV output.

Read-only utility. No database writes.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_IN = "tmp/pitcher_game_logs_pybaseball_smoke.csv"
DEFAULT_OUT = "tmp/pitcher_game_logs_pybaseball_audit.csv"


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([pd.NA] * len(df), index=df.index)


def _nonempty(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], dtype=bool)
    text = series.fillna("").astype(str).str.strip()
    return text.ne("") & ~text.str.lower().isin({"nan", "none", "null", "<na>"})


def _nonnull_count(df: pd.DataFrame, name: str) -> int:
    return int(_nonempty(_col(df, name)).sum())


def _numeric(df: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(_col(df, name), errors="coerce")


def _metric(rows: list[dict[str, Any]], metric: str, value: Any, group: str = "coverage") -> None:
    rows.append({"group": group, "metric": metric, "value": value})


def _add_describe(rows: list[dict[str, Any]], df: pd.DataFrame, name: str) -> None:
    values = _numeric(df, name).dropna()
    if values.empty:
        for stat in ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]:
            _metric(rows, f"{name}_{stat}", pd.NA, "distribution")
        return
    desc = values.describe()
    for stat in ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]:
        _metric(rows, f"{name}_{stat}", desc.get(stat, pd.NA), "distribution")


def build_audit(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    total_rows = int(len(df))
    unique_pitchers = int(_numeric(df, "pitcher_mlbam_id").dropna().nunique())
    key_retro_col = "chadwick_key_retro" if "chadwick_key_retro" in df.columns else "key_retro"
    chadwick_match_col = "chadwick_mlbam_match"

    _metric(rows, "total_rows", total_rows)
    _metric(rows, "unique_pitchers", unique_pitchers)
    _metric(rows, "rows_with_pitcher_mlbam_id", _nonnull_count(df, "pitcher_mlbam_id"))
    _metric(rows, "rows_with_key_retro", _nonnull_count(df, key_retro_col))

    if chadwick_match_col in df.columns:
        match = _col(df, chadwick_match_col).astype(str).str.lower().isin({"true", "1", "yes"})
        _metric(rows, "missing_chadwick_match_count", int((~match).sum()))
    else:
        _metric(rows, "missing_chadwick_match_count", total_rows - _nonnull_count(df, key_retro_col))

    for field in [
        "outs_recorded",
        "strikeouts",
        "walks",
        "hits_allowed",
        "home_runs_allowed",
        "days_rest",
        "pitch_count",
    ]:
        _metric(rows, f"rows_with_{field}", _nonnull_count(df, field))

    _metric(rows, "rows_missing_earned_runs", int(_numeric(df, "earned_runs").isna().sum()))

    if "is_starter" in df.columns:
        starters = _col(df, "is_starter").astype(str).str.lower().isin({"true", "1", "yes"})
        _metric(rows, "suspected_starters_count", int(starters.sum()))
    else:
        _metric(rows, "suspected_starters_count", int((_numeric(df, "outs_recorded") >= 9).sum()))

    if "game_date" in df.columns:
        by_date = df.groupby("game_date", dropna=False).agg(
            rows=("game_date", "size"),
            pitchers=("pitcher_mlbam_id", lambda s: int(pd.to_numeric(s, errors="coerce").dropna().nunique())),
        )
        for game_date, row in by_date.reset_index().iterrows():
            label = str(row["game_date"])
            _metric(rows, f"rows_per_game_date:{label}", int(row["rows"]), "by_date")
            _metric(rows, f"pitchers_per_game_date:{label}", int(row["pitchers"]), "by_date")

    for field in ["outs_recorded", "strikeouts", "walks", "hits_allowed"]:
        _add_describe(rows, df, field)

    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=DEFAULT_IN)
    parser.add_argument("--out-csv", default=DEFAULT_OUT)
    args = parser.parse_args()

    in_csv = Path(args.csv)
    if not in_csv.exists():
        raise SystemExit(f"Pitcher game-log CSV not found: {in_csv}")

    df = pd.read_csv(in_csv)
    audit = build_audit(df)
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(out_csv, index=False)

    print(f"[pitcher-pybaseball-audit] csv={in_csv}")
    for _, row in audit[audit["group"].eq("coverage")].iterrows():
        print(f"{row['metric']}: {row['value']}")
    print("[pitcher-pybaseball-audit] by_date")
    by_date = audit[audit["group"].eq("by_date")]
    print(by_date.to_string(index=False) if not by_date.empty else "none")
    print("[pitcher-pybaseball-audit] distribution")
    dist = audit[audit["group"].eq("distribution")]
    print(dist.to_string(index=False) if not dist.empty else "none")
    print(f"[pitcher-pybaseball-audit] out_csv={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
