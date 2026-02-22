#!/usr/bin/env python3
"""Attach names to SOG/Saves prediction outputs when possible.

Compatibility utility for legacy workflow step.
Writes:
  - backend/nhl/data/processed/sog_with_names.csv
  - backend/nhl/data/processed/saves_with_names.csv
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


SOG_IN = Path("backend/nhl/data/processed/sog_predictions.csv")
SAVES_IN = Path("backend/nhl/data/processed/saves_predictions.csv")
SOG_OUT = Path("backend/nhl/data/processed/sog_with_names.csv")
SAVES_OUT = Path("backend/nhl/data/processed/saves_with_names.csv")


def load_names() -> pd.DataFrame:
    # Prefer current daily export names if present.
    daily_names = sorted(Path("backend/nhl/exports/daily/names").glob("names_*.csv"))
    candidates = list(reversed(daily_names))
    candidates.append(Path("backend/nhl/data/external/roster_names.csv"))

    for path in candidates:
        if not path.exists():
            continue
        try:
            names = pd.read_csv(path)
        except Exception:
            continue
        if "nhl_id" in names.columns and "player_id" not in names.columns:
            names = names.rename(columns={"nhl_id": "player_id"})
        if "player_id" in names.columns and "full_name" in names.columns:
            names["player_id"] = pd.to_numeric(names["player_id"], errors="coerce").astype(
                "Int64"
            )
            cols = ["player_id", "full_name"]
            if "team" in names.columns:
                cols.append("team")
            return names[cols].dropna(subset=["player_id"]).drop_duplicates("player_id")

    return pd.DataFrame(columns=["player_id", "full_name"])


def attach_one(pred_path: Path, out_path: Path, names: pd.DataFrame) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not pred_path.exists():
        pd.DataFrame().to_csv(out_path, index=False)
        print(f"[attach_names] wrote empty {out_path} (missing input {pred_path})")
        return

    df = pd.read_csv(pred_path)
    if "player_id" not in df.columns:
        df.to_csv(out_path, index=False)
        print(f"[attach_names] copied {pred_path} (no player_id column)")
        return

    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    merged = df.merge(names, on="player_id", how="left", suffixes=("", "_name"))
    merged.to_csv(out_path, index=False)
    matched = int(merged["full_name"].notna().sum()) if "full_name" in merged.columns else 0
    print(f"[attach_names] wrote {out_path} rows={len(merged)} matched_names={matched}")


def main() -> int:
    names = load_names()
    attach_one(SOG_IN, SOG_OUT, names)
    attach_one(SAVES_IN, SAVES_OUT, names)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
