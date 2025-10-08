#!/usr/bin/env python3
"""
Pooler-friendly loader: inserts CSV rows into stage tables (no COPY/PREPARE),
then calls your existing upsert functions.

Fix: map CSV headers with dots (e.g., p_over_2.5) to stage columns with underscores (p_over_2_5).
"""

import argparse, csv, json
from pathlib import Path

def read_model_index(d: Path):
    idx = json.loads((d / "MODEL_INDEX.json").read_text())
    fh = d / "FEATURE_HASH.txt"
    if fh.exists():
        idx["feature_hash"] = fh.read_text().strip()
    idx.setdefault("feature_hash", idx.get("feature_hash", ""))
    return idx

def to_float(x):
    if x is None or x == "": return None
    try: return float(x)
    except: return None

def load_sog_rows(csv_path: str):
    # Map dotted headers to underscore stage columns
    map_keys = {
        "p_over_0.5": "p_over_0_5", "p_over_0_5": "p_over_0_5",
        "p_over_1.5": "p_over_1_5", "p_over_1_5": "p_over_1_5",
        "p_over_2.5": "p_over_2_5", "p_over_2_5": "p_over_2_5",
        "p_over_3.5": "p_over_3_5", "p_over_3_5": "p_over_3_5",
    }
    out = []
    with open(csv_path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rec = {
                "player_id": row.get("player_id"),
                "game_id":   row.get("game_id"),
                "p_over_0_5": to_float(row.get("p_over_0.5") or row.get("p_over_0_5")),
                "p_over_1_5": to_float(row.get("p_over_1.5") or row.get("p_over_1_5")),
                "p_over_2_5": to_float(row.get("p_over_2.5") or row.get("p_over_2_5")),
                "p_over_3_5": to_float(row.get("p_over_3.5") or row.get("p_over_3_5")),
            }
            out.append(rec)
    cols = ["player_id","game_id","p_over_0_5","p_over_1_5","p_over_2_5","p_over_3_5"]
    return out, cols

def load_saves_rows(csv_path: str):
    out = []
    with open(csv_path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rec = {
                "player_id":  row.get("player_id"),
                "game_id":    row.get("game_id"),
                "p_over_24_5": to_float(row.get("p_over_24.5") or row.get("p_over_24_5")),
                "p_over_28_5": to_float(row.get("p_over_28.5") or row.get("p_over_28_5")),
            }
            out.append(rec)
    cols = ["player_id","game_id","p_over_24_5","p_over_28_5"]
    return out, cols

def batched(it, n=1000):
    batch = []
    for x in it:
        batch.append(x)
        if len(batch) >= n:
            yield batch; batch = []
    if batch: yield batch

def insert_stage(cur, table, rows, cols):
    cols_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
    data = [[r.get(c) for c in cols] for r in rows]
    cur.executemany(sql, data)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--db-url", required=True)  # Use POOLER URI (session pooler is fine)
    ap.add_argument("--sog-csv")
    ap.add_argument("--saves-csv")
    ap.add_argument("--sog-model-dir")
    ap.add_argument("--saves-model-dir")
    args = ap.parse_args()

    proj = Path(args.project)
    sog_dir   = Path(args.sog_model_dir) if args.sog_model_dir else proj / "models" / "latest" / "shots_on_goal"
    saves_dir = Path(args.saves_model_dir) if args.saves_model_dir else proj / "models" / "latest" / "goalie_saves"

    sog_idx   = read_model_index(sog_dir)   if args.sog_csv else None
    saves_idx = read_model_index(saves_dir) if args.saves_csv else None

    import psycopg2
    conn = psycopg2.connect(args.db_url)
    with conn:
        with conn.cursor() as cur:
            if args.sog_csv:
                print("🧹 Truncating nhl.predictions_sog_stage …")
                cur.execute("TRUNCATE nhl.predictions_sog_stage")
                rows, cols = load_sog_rows(args.sog_csv)
                print(f"📥 Inserting {len(rows)} SOG stage rows …")
                for b in batched(rows, 1000):
                    insert_stage(cur, "nhl.predictions_sog_stage", b, cols)
                print("🚀 Upserting SOG to nhl.predictions …")
                cur.execute("""
                    SELECT nhl.load_sog_predictions_from_stage(
                      p_model_family  => %s,
                      p_model_params  => %s::jsonb,
                      p_feature_hash  => %s,
                      p_model_version => %s
                    )
                """, (sog_idx["family"], json.dumps(sog_idx.get("params", {})),
                      sog_idx["feature_hash"], "latest/shots_on_goal"))
                print("✅", cur.fetchone())

            if args.saves_csv:
                print("🧹 Truncating nhl.predictions_saves_stage …")
                cur.execute("TRUNCATE nhl.predictions_saves_stage")
                rows, cols = load_saves_rows(args.saves_csv)
                print(f"📥 Inserting {len(rows)} Saves stage rows …")
                for b in batched(rows, 1000):
                    insert_stage(cur, "nhl.predictions_saves_stage", b, cols)
                print("🚀 Upserting Saves to nhl.predictions …")
                cur.execute("""
                    SELECT nhl.load_saves_predictions_from_stage(
                      p_model_family  => %s,
                      p_model_params  => %s::jsonb,
                      p_feature_hash  => %s,
                      p_model_version => %s
                    )
                """, (saves_idx["family"], json.dumps(saves_idx.get("params", {})),
                      saves_idx["feature_hash"], "latest/goalie_saves"))
                print("✅", cur.fetchone())

    print("🎉 Done. Upserts complete via pooler.")

if __name__ == "__main__":
    main()
