#!/usr/bin/env python3
"""
load_predictions_to_db.py — bulk-load SOG & Goalie Saves CSVs into Supabase and upsert into nhl.predictions.

What it does (ONLY if --db-url is provided):
  1) Connects to Postgres
  2) TRUNCATEs staging tables:
       - nhl.predictions_sog_stage
       - nhl.predictions_saves_stage
  3) COPYs your CSVs into staging (keeps only the expected columns)
  4) Calls your existing loader functions to UPSERT into nhl.predictions using each model’s metadata
     read from MODEL_INDEX.json in the model directories.

Args:
  --project         Project root (contains models/, features/, data/)
  --db-url          Postgres connection URL (e.g., postgresql://user:pass@host:port/db?sslmode=require)
  --sog-csv         Path to SOG predictions CSV (columns include p_over_0.5, p_over_1.5, p_over_2.5, p_over_3.5)
  --saves-csv       Path to Saves predictions CSV (columns include p_over_24.5, p_over_28.5)
  --sog-model-dir   Override path to SOG model dir (default: {project}/models/latest/shots_on_goal)
  --saves-model-dir Override path to Saves model dir (default: {project}/models/latest/goalie_saves)

Example:
  python scripts/load_predictions_to_db.py \
    --project /Users/jerrystrain/Projects/Proppadia/nhl-props \
    --db-url "postgresql://USER:PASSWORD@HOST:PORT/DBNAME?sslmode=require" \
    --sog-csv /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/sog_predictions.csv \
    --saves-csv /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/saves_predictions.csv
"""
import argparse, json, os, re, sys, csv, tempfile
from pathlib import Path

def read_model_index(model_dir: Path):
    idx_path = model_dir / "MODEL_INDEX.json"
    if not idx_path.exists():
        sys.exit(f"Missing {idx_path}")
    idx = json.loads(idx_path.read_text())
    fh = (model_dir / "FEATURE_HASH.txt")
    if fh.exists():
        idx["feature_hash"] = fh.read_text().strip()
    if "feature_hash" not in idx:
        # fall back to index JSON if present there
        idx["feature_hash"] = idx.get("feature_hash", "")
    return idx

def ensure_psycopg2():
    try:
        import psycopg2  # noqa
    except Exception:
        import subprocess
        print("🔧 Installing psycopg2-binary...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-qU", "psycopg2-binary"])
        import psycopg2  # noqa

def quote_ident(col: str) -> str:
    # quote if not a simple identifier
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col):
        return col
    return '"' + col.replace('"', '""') + '"'

def project_csv(src_path: Path, keep_cols: list[str]) -> Path:
    """Return path to a temp CSV containing only keep_cols (header preserved)."""
    tmpdir = Path(tempfile.mkdtemp())
    outp = tmpdir / ("projected_" + src_path.name)
    with open(src_path, "r", newline="") as fin, open(outp, "w", newline="") as fout:
        r = csv.DictReader(fin)
        w = csv.DictWriter(fout, fieldnames=keep_cols)
        w.writeheader()
        for row in r:
            w.writerow({c: row.get(c, "") for c in keep_cols})
    return outp

def copy_csv(conn, table: str, csv_path: Path, columns: list[str]):
    import psycopg2
    cols_sql = ", ".join(quote_ident(c) for c in columns)
    sql_trunc = f"TRUNCATE {table};"
    sql_copy  = f"COPY {table} ({cols_sql}) FROM STDIN WITH CSV HEADER"
    with conn.cursor() as cur, open(csv_path, "r") as f:
        print(f"🧹 {sql_trunc}")
        cur.execute(sql_trunc)
        print(f"📥 COPY {csv_path} → {table} ({cols_sql})")
        cur.copy_expert(sql_copy, f)
    conn.commit()

def run_loader(conn, sql: str, tag: str):
    with conn.cursor() as cur:
        print(f"🚀 {tag}: {sql.strip()}")
        cur.execute(sql)
        row = cur.fetchone()
        print(f"✅ {tag} result: {row}")
    conn.commit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--db-url", required=True)
    ap.add_argument("--sog-csv", default=None)
    ap.add_argument("--saves-csv", default=None)
    ap.add_argument("--sog-model-dir", default=None)
    ap.add_argument("--saves-model-dir", default=None)
    args = ap.parse_args()

    proj = Path(args.project).resolve()
    sog_model_dir   = Path(args.sog_model_dir) if args.sog_model_dir else (proj / "models" / "latest" / "shots_on_goal")
    saves_model_dir = Path(args.saves_model_dir) if args.saves_model_dir else (proj / "models" / "latest" / "goalie_saves")

    if not args.sog_csv and not args.saves_csv:
        sys.exit("Provide at least one of --sog-csv or --saves-csv")

    # Read model metadata (for loader params)
    sog_idx = read_model_index(sog_model_dir) if args.sog_csv else None
    saves_idx = read_model_index(saves_model_dir) if args.saves_csv else None

    ensure_psycopg2()
    import psycopg2
    print("🔌 Connecting to DB…")
    conn = psycopg2.connect(args.db_url)

    try:
        # SOG
        if args.sog_csv:
            sog_cols = ["player_id","game_id","p_over_0.5","p_over_1.5","p_over_2.5","p_over_3.5"]
            sog_csv_p = Path(args.sog_csv)
            sog_tmp = project_csv(sog_csv_p, sog_cols)
            copy_csv(conn, "nhl.predictions_sog_stage", sog_tmp, sog_cols)
            sog_params_json = json.dumps(sog_idx.get("params", {}))
            run_loader(conn, f"""
                SELECT nhl.load_sog_predictions_from_stage(
                  p_model_family  => '{sog_idx["family"]}',
                  p_model_params  => '{sog_params_json}'::jsonb,
                  p_feature_hash  => '{sog_idx["feature_hash"]}',
                  p_model_version => 'latest/shots_on_goal'
                );
            """, tag="load_sog_predictions_from_stage")

        # SAVES
        if args.saves_csv:
            saves_cols = ["player_id","game_id","p_over_24.5","p_over_28.5"]
            saves_csv_p = Path(args.saves_csv)
            saves_tmp = project_csv(saves_csv_p, saves_cols)
            copy_csv(conn, "nhl.predictions_saves_stage", saves_tmp, saves_cols)
            saves_params_json = json.dumps(saves_idx.get("params", {}))
            run_loader(conn, f"""
                SELECT nhl.load_saves_predictions_from_stage(
                  p_model_family  => '{saves_idx["family"]}',
                  p_model_params  => '{saves_params_json}'::jsonb,
                  p_feature_hash  => '{saves_idx["feature_hash"]}',
                  p_model_version => 'latest/goalie_saves'
                );
            """, tag="load_saves_predictions_from_stage")

        # Clear staging after successful loads
        with conn.cursor() as cur:
            print("🧼 Clearing staging tables…")
            cur.execute("TRUNCATE nhl.predictions_sog_stage;")
            cur.execute("TRUNCATE nhl.predictions_saves_stage;")
        conn.commit()
        print("🎉 Done. Predictions upserted to nhl.predictions and staging cleared.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
