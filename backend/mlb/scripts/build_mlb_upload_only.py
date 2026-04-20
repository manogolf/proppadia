#!/usr/bin/env python3
"""
Build strict MLB upload-only CSV from a rich internal production candidate CSV.

Default input:
  tmp/analysis/mlb_over_prod_candidates_edge015.csv

Default output:
  <input_stem>_upload_only.csv (same directory as input)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REQUIRED_SOURCE_FIELDS = [
    "game_date",
    "home_team_code",
    "away_team_code",
    "market_key",
    "player_id",
    "line",
    "bet_side",
    "model_fair_over_american",
]

UPLOAD_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "DOUBLEHEADER",
    "SECTION",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]


def _build_output_path(input_path: Path, output_arg: str) -> Path:
    if output_arg:
        return Path(output_arg).expanduser().resolve()
    return input_path.resolve().with_name(f"{input_path.stem}_upload_only.csv")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Create strict upload-only MLB CSV from rich internal candidate CSV."
    )
    ap.add_argument(
        "--in-csv",
        default="tmp/analysis/mlb_over_prod_candidates_edge015.csv",
        help="Path to rich internal MLB candidate CSV.",
    )
    ap.add_argument(
        "--out-csv",
        default="",
        help="Output path for upload-only CSV (default: <input_stem>_upload_only.csv).",
    )
    ap.add_argument(
        "--league",
        default="MLB",
        help="LEAGUE value for upload rows (default: MLB).",
    )
    ap.add_argument(
        "--section",
        default="player_prop",
        help="SECTION value for upload rows (default: player_prop).",
    )
    args = ap.parse_args()

    in_path = Path(args.in_csv).expanduser().resolve()
    out_path = _build_output_path(in_path, args.out_csv)

    if not in_path.exists():
        raise FileNotFoundError(f"missing input CSV: {in_path}")

    src = pd.read_csv(in_path)
    missing = [c for c in REQUIRED_SOURCE_FIELDS if c not in src.columns]
    if missing:
        raise ValueError(f"missing required source fields: {missing}")

    out = pd.DataFrame(
        {
            "LEAGUE": args.league,
            "DATE": src["game_date"],
            "HOME": src["home_team_code"],
            "AWAY": src["away_team_code"],
            "DOUBLEHEADER": "",
            "SECTION": args.section,
            "MARKET": src["market_key"],
            "SELECTOR": src["player_id"],
            "POINT": src["line"],
            "SIDE": src["bet_side"],
            "WIN %": src["model_fair_over_american"],
        }
    )[UPLOAD_COLUMNS]

    row_count_in = len(src)
    row_count_out = len(out)
    counts_match = row_count_in == row_count_out
    if not counts_match:
        raise RuntimeError(
            f"row count mismatch: in={row_count_in} out={row_count_out}"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"input_path={in_path}")
    print(f"output_path={out_path}")
    print(f"row_count_in={row_count_in}")
    print(f"row_count_out={row_count_out}")
    print(f"row_counts_match={counts_match}")


if __name__ == "__main__":
    main()
