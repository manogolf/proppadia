#!/usr/bin/env python3
"""Rebuild the exact documented MLB/Retrosheet player crosswalk only."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "backend/mlb/data/raw/retrosheet/chadwick_register/people.csv"


def sha256(path: Path) -> str:
    h = hashlib.sha256(path.read_bytes())
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = pd.read_csv(SOURCE, dtype=str, low_memory=False)
    mapped = raw[raw.key_mlbam.notna() & raw.key_retro.notna()][
        ["key_mlbam", "key_retro", "key_person", "name_first", "name_last"]
    ].copy()
    mapped["mlb_player_id"] = pd.to_numeric(mapped.key_mlbam, errors="coerce").astype("Int64")
    mapped = mapped[mapped.mlb_player_id.notna()].copy()
    mlb_conflicts = mapped.groupby("mlb_player_id").key_retro.nunique()
    retro_conflicts = mapped.groupby("key_retro").mlb_player_id.nunique()
    mapped["crosswalk_status"] = "EXACT_DOCUMENTED"
    mapped.loc[mapped.mlb_player_id.isin(mlb_conflicts[mlb_conflicts > 1].index), "crosswalk_status"] = "ONE_TO_MANY_CONFLICT"
    mapped.loc[mapped.key_retro.isin(retro_conflicts[retro_conflicts > 1].index), "crosswalk_status"] = "MANY_TO_ONE_CONFLICT"
    mapped["evidence_path"] = str(SOURCE.relative_to(ROOT))
    mapped["evidence_sha256"] = sha256(SOURCE)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    mapped.sort_values(["mlb_player_id", "key_retro"]).to_csv(args.output, index=False)
    print(mapped.crosswalk_status.value_counts(dropna=False).to_json())


if __name__ == "__main__":
    main()
