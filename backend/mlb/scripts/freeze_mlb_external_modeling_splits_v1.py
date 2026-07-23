#!/usr/bin/env python3
"""Freeze date-based game populations; never reads outcomes or model performance."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
NORM = ROOT / "backend/mlb/data/external/normalized/v1"


def load_games() -> pd.DataFrame:
    frames = [pq.ParquetFile(p).read().to_pandas() for p in sorted((NORM / "games").glob("season=*/*.parquet"))]
    games = pd.concat(frames, ignore_index=True).drop_duplicates("game_pk")
    games["game_date"] = pd.to_datetime(games.game_date, errors="coerce")
    return games


def split_name(date: pd.Timestamp) -> str:
    if pd.isna(date): return "INELIGIBLE_MISSING_DATE"
    if date.year <= 2024: return "DEVELOPMENT_2022_2024"
    if date.year == 2025: return "VALIDATION_2025"
    if date <= pd.Timestamp("2026-06-30"): return "PROTECTED_HOLDOUT_2026_TO_06_30"
    if date <= pd.Timestamp("2026-07-21"): return "FINAL_RECENT_UNTOUCHED_2026_07_01_07_21"
    return "INELIGIBLE_OUTSIDE_FROZEN_RANGE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(); args.output_dir.mkdir(parents=True, exist_ok=True)
    games = load_games()
    games = games[games.game_type.fillna("R").eq("R")].copy()
    games["split"] = games.game_date.map(split_name)
    games["strict_prior_cutoff"] = "source_game_start < target_game_start"
    for name, frame in games.groupby("split"):
        frame.sort_values(["game_date", "game_pk"]).to_csv(args.output_dir / f"split_{name.lower()}.csv", index=False)
    games.groupby("split", dropna=False).agg(games=("game_pk", "nunique"), first_date=("game_date", "min"),
                                              last_date=("game_date", "max")).reset_index().to_csv(
        args.output_dir / "chronological_split_summary.csv", index=False)
    print(games.split.value_counts().to_json())


if __name__ == "__main__":
    main()
