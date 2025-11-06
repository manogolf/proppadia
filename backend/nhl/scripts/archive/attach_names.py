#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

NAMES_CSV = Path("backend/nhl/data/external/roster_names.csv")
SOG_IN    = Path("backend/nhl/data/processed/sog_predictions.csv")
SAVES_IN  = Path("backend/nhl/data/processed/saves_predictions.csv")
SOG_OUT   = Path("backend/nhl/data/processed/sog_with_names.csv")
SAVES_OUT = Path("backend/nhl/data/processed/saves_with_names.csv")

def load_names(path: Path) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"Missing names CSV: {path}")
    names = pd.read_csv(path)

    # Normalize expected columns: nhl_id -> player_id, team_abbr -> team
    colmap = {}
    if "nhl_id" in names.columns and "player_id" not in names.columns:
        colmap["nhl_id"] = "player_id"
    if "team_abbr" in names.columns and "team" not in names.columns:
        colmap["team_abbr"] = "team"
    if colmap:
        names = names.rename(columns=colmap)

    # Minimal schema check
    need = {"player_id", "full_name"}
    missing = need - set(names.columns)
    if missing:
        sys.exit(f"{path} is missing required columns: {sorted(missing)}")

    # Coerce types
    names["player_id"] = pd.to_numeric(names["player_id"], errors="coerce").astype("Int64")
    if "team" in names.columns:
        names["team"] = names["team"].astype(str)

    return names[["player_id", "full_name"] + (["team"] if "team" in names.columns else [])].dropna(subset=["player_id"])

def attach(pred_path: Path, out_path: Path, names: pd.DataFrame):
    if not pred_path.exists():
        print(f"skip: {pred_path} not found")
        return
    df = pd.read_csv(pred_path)
    if "player_id" not in df.columns:
        sys.exit(f"{pred_path} has no 'player_id' column")

    # Coerce to align types for merge
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")

    m = df.merge(names, on="player_id", how="left")
    # Column order: full_name, team, game_id, p_over_* … then the rest
    pcols = [c for c in m.columns if c.startswith("p_over_")]
    lead  = (["full_name"] if "full_name" in m.columns else []) \
            + (["team"] if "team" in m.columns else []) \
            + (["game_id"] if "game_id" in m.columns else []) \
            + pcols
    rest  = [c for c in m.columns if c not in lead]
    m = m[lead + rest]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.to_csv(out_path, index=False)
    missing_names = m["full_name"].isna().sum() if "full_name" in m.columns else len(m)
    print(f"✅ wrote {out_path}  rows={len(m)}  pcols={len(pcols)}  missing_names={missing_names}")

def main():
    names = load_names(NAMES_CSV)
    attach(SOG_IN,   SOG_OUT,   names)
    attach(SAVES_IN, SAVES_OUT, names)

if __name__ == "__main__":
    main()
