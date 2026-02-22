#!/usr/bin/env python3
"""
prepare_points_training.py
- Reads features CSV (canonical path)
- Fetches y_points (goals+assists) from DB for each (player_id, game_id)
- Adds aliases: d10_pp_min_avg -> d10_pp_toi_min_avg (if needed)
- Adds interactions: d10_sog_avg__x__d10_pp_toi_min_avg, d10_attempts_avg__x__d10_pp_toi_min_avg
- Fills NaNs in feature columns with 0.0 (keeps target strict)
- Writes back to the same canonical CSV

Usage:
  SUPABASE_DB_URL=... python backend/nhl/scripts/prepare_points_training.py \
    --csv backend/nhl/data/processed/points_training.csv
"""
from __future__ import annotations
import argparse, os, sys
import pandas as pd
import psycopg
from psycopg.rows import dict_row

def get_db_url() -> str:
    url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        print("FATAL: set SUPABASE_DB_URL", file=sys.stderr); sys.exit(2)
    # ensure ssl
    if "?sslmode=" not in url and "&sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

def fetch_y_points_map(db_url: str, pairs: list[tuple[int,int]]) -> dict[tuple[int,int], int]:
    # batch fetch y_points for the given (player_id, game_id) pairs
    out: dict[tuple[int,int], int] = {}
    if not pairs:
        return out
    pid_list = [int(p) for p,_ in pairs]
    gid_list = [int(g) for _,g in pairs]
    with psycopg.connect(db_url, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.player_id::bigint AS player_id,
                       s.game_id::bigint   AS game_id,
                       COALESCE(s.goals,0) + COALESCE(s.assists,0) AS y_points
                FROM nhl.skater_game_logs_raw s
                WHERE s.player_id = ANY(%s) AND s.game_id = ANY(%s)
            """, (pid_list, gid_list))
            for r in cur.fetchall():
                out[(int(r["player_id"]), int(r["game_id"]))] = int(r["y_points"])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Canonical features CSV to augment in-place.")
    args = ap.parse_args()

    path = args.csv
    df = pd.read_csv(path)
    # ensure required id keys exist
    for key in ("player_id","game_id"):
        if key not in df.columns:
            print(f"FATAL: input missing column {key}", file=sys.stderr); sys.exit(2)

    # 1) fetch y_points for all pairs
    pairs = list({(int(r.player_id), int(r.game_id)) for r in df.itertuples(index=False)})
    ymap = fetch_y_points_map(get_db_url(), pairs)

    # 2) attach y_points
    df["y_points"] = [
        ymap.get((int(r.player_id), int(r.game_id))) if pd.notna(r.player_id) and pd.notna(r.game_id) else None
        for r in df.itertuples(index=False)
    ]
    # drop rows missing target
    before = len(df)
    df = df.dropna(subset=["y_points"])
    df["y_points"] = df["y_points"].astype(int)
    after = len(df)
    if after == 0:
        print("FATAL: no rows with y_points matched from DB. Check ids/date range.", file=sys.stderr); sys.exit(2)
    print(f"Matched target for {after}/{before} rows.", file=sys.stderr)

    # 3) alias old PP column name -> new
    if "d10_pp_toi_min_avg" not in df.columns and "d10_pp_min_avg" in df.columns:
        df["d10_pp_toi_min_avg"] = df["d10_pp_min_avg"]

    # 4) interactions
    if "d10_sog_avg" in df.columns and "d10_pp_toi_min_avg" in df.columns:
        df["d10_sog_avg__x__d10_pp_toi_min_avg"] = pd.to_numeric(df["d10_sog_avg"], errors="coerce") * \
                                                   pd.to_numeric(df["d10_pp_toi_min_avg"], errors="coerce")
    if "d10_attempts_avg" in df.columns and "d10_pp_toi_min_avg" in df.columns:
        df["d10_attempts_avg__x__d10_pp_toi_min_avg"] = pd.to_numeric(df["d10_attempts_avg"], errors="coerce") * \
                                                        pd.to_numeric(df["d10_pp_toi_min_avg"], errors="coerce")

    # 5) safe fill of common features
    SAFE_FILL = [
        "d5_points_avg","d10_points_avg","d10_sog_avg","d10_attempts_avg","d10_toi_min_avg",
        "d10_pp_toi_min_avg","d10_sog_avg__x__d10_pp_toi_min_avg","d10_attempts_avg__x__d10_pp_toi_min_avg",
        "team_d10_sf_per60","opp_d10_sf_per60","pace_matchup_index"
    ]
    for c in SAFE_FILL:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # 6) write back in place
    df.to_csv(path, index=False)
    print(f"✅ Augmented and saved: {path} (rows={len(df)}, cols={len(df.columns)})")

if __name__ == "__main__":
    main()
