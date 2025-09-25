#!/usr/bin/env python3
"""
run_daily_slate.py — one-shot runner to score today's NHL slate and load to Supabase.

What it does (in order):
1) Scores SOG for lines 0.5,1.5,2.5,3.5 with your latest model
2) Scores Goalie Saves for lines 24.5,28.5 with your latest model
3) If a Postgres URL is provided, truncates staging tables, bulk-loads the CSVs,
   and calls the existing loader functions to upsert into nhl.predictions.

Usage (replace the CSVs with your daily feature files when you have them):
  python scripts/run_daily_slate.py \
    --project /Users/jerrystrain/Projects/Proppadia/nhl-props \
    --sog-csv /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/nhl_sog_training.csv \
    --saves-csv /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/nhl_saves_training.csv \
    --db-url "postgresql://USER:PASSWORD@HOST:PORT/DBNAME?sslmode=require"

Notes:
- If --db-url is omitted, this script just writes the CSVs; you can import via Table Editor and run the loaders manually.
- Requires your existing score script at scripts/score_nhl_props.py
- Installs psycopg2-binary on first run if needed (for DB loading).
"""

import argparse, json, os, sys, subprocess, shlex, tempfile
from pathlib import Path

# ---------- helpers ----------

def sh(cmd, env=None):
    print(f"$ {cmd}")
    proc = subprocess.run(cmd, shell=True, env=env)
    if proc.returncode != 0:
        sys.exit(proc.returncode)

def read_model_index(model_dir: Path):
    idx = json.loads((model_dir / "MODEL_INDEX.json").read_text())
    fh_txt = (model_dir / "FEATURE_HASH.txt")
    if fh_txt.exists():
        idx["feature_hash"] = fh_txt.read_text().strip()
    return idx

def ensure_psycopg():
    try:
        import psycopg2  # noqa
    except Exception:
        print("🔧 Installing psycopg2-binary...")
        sh(f'{sys.executable} -m pip install -qU psycopg2-binary')
        import psycopg2  # noqa

def copy_csv(conn, table, csv_path, columns):
    import psycopg2
    with conn.cursor() as cur, open(csv_path, "r") as f:
        cur.execute(f"TRUNCATE {table};")
        cols = ", ".join(columns)
        sql = f"COPY {table} ({cols}) FROM STDIN WITH CSV HEADER"
        cur.copy_expert(sql, f)

def run_loader(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        print("⬆️ loader result:", row)
    conn.commit()

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="Project root (contains scripts/, models/, data/)")
    ap.add_argument("--sog-csv", required=True, help="CSV with SOG features for the slate")
    ap.add_argument("--saves-csv", required=True, help="CSV with Saves features for the slate")
    ap.add_argument("--db-url", default=os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL"),
                    help="Postgres connection URL (optional). If present, will load to DB.")
    args = ap.parse_args()

    proj = Path(args.project).resolve()
    score_py = proj / "scripts" / "score_nhl_props.py"
    if not score_py.exists():
        sys.exit(f"Missing scorer: {score_py}")

    # Models
    model_sog_dir   = proj / "models" / "latest" / "shots_on_goal"
    model_saves_dir = proj / "models" / "latest" / "goalie_saves"
    if not model_sog_dir.exists() or not model_saves_dir.exists():
        sys.exit("Missing latest models. Train first.")

    # Read model metadata (family, params, feature_hash)
    sog_idx   = read_model_index(model_sog_dir)
    saves_idx = read_model_index(model_saves_dir)

    # Outputs
    out_sog   = proj / "data" / "processed" / "sog_predictions.csv"
    out_saves = proj / "data" / "processed" / "saves_predictions.csv"

    # 1) Score SOG for 0.5/1.5/2.5/3.5
    sog_lines = "0.5,1.5,2.5,3.5"
    cmd_sog = f"""
    {shlex.quote(sys.executable)} {shlex.quote(str(score_py))} \
      --model-dir {shlex.quote(str(model_sog_dir))} \
      --csv {shlex.quote(args.sog_csv)} \
      --feature-json {shlex.quote(str(proj / "features" / "feature_metadata_nhl.json"))} \
      --feature-key shots_on_goal \
      --line {sog_lines} \
      --out {shlex.quote(str(out_sog))}
    """
    sh(cmd_sog)

    # 2) Score Goalie Saves for 24.5/28.5
    saves_lines = "24.5,28.5"
    cmd_saves = f"""
    {shlex.quote(sys.executable)} {shlex.quote(str(score_py))} \
      --model-dir {shlex.quote(str(model_saves_dir))} \
      --csv {shlex.quote(args.saves_csv)} \
      --feature-json {shlex.quote(str(proj / "features" / "feature_metadata_nhl.json"))} \
      --feature-key goalie_saves \
      --line {saves_lines} \
      --out {shlex.quote(str(out_saves))}
    """
    sh(cmd_saves)

    print(f"✅ Wrote: {out_sog}")
    print(f"✅ Wrote: {out_saves}")

    # 3) Optional: load to DB (if URL provided)
    if args.db_url:
        ensure_psycopg()
        import psycopg2, csv

        # Map CSVs to staging schemas
        sog_cols = ["player_id","game_id","p_over_0.5","p_over_1.5","p_over_2.5","p_over_3.5"]
        saves_cols = ["player_id","game_id","p_over_24.5","p_over_28.5"]

        # Create filtered temp CSVs with only staging columns (in case the scorer wrote extra columns)
        def project_csv(src_path, cols, dest_path):
            with open(src_path, "r") as fin, open(dest_path, "w", newline="") as fout:
                r = csv.DictReader(fin)
                w = csv.DictWriter(fout, fieldnames=cols)
                w.writeheader()
                for row in r:
                    w.writerow({c: row.get(c, "") for c in cols})

        with tempfile.TemporaryDirectory() as td:
            tmp_sog   = Path(td) / "sog_stage.csv"
            tmp_saves = Path(td) / "saves_stage.csv"
            project_csv(out_sog, sog_cols, tmp_sog)
            project_csv(out_saves, saves_cols, tmp_saves)

            print("🔌 Connecting to DB…")
            conn = psycopg2.connect(args.db_url)

            # COPY into stage (truncate first to avoid dup-on-conflict issues)
            copy_csv(conn, "nhl.predictions_sog_stage", str(tmp_sog), sog_cols)
            copy_csv(conn, "nhl.predictions_saves_stage", str(tmp_saves), saves_cols)

            # Run loaders (SOG uses family/params/feature_hash from its model index; Saves likewise)
            sog_params_json = json.dumps(sog_idx.get("params", {}))
            saves_params_json = json.dumps(saves_idx.get("params", {}))

            run_loader(conn, f"""
                SELECT nhl.load_sog_predictions_from_stage(
                  p_model_family  => '{sog_idx["family"]}',
                  p_model_params  => '{sog_params_json}'::jsonb,
                  p_feature_hash  => '{sog_idx["feature_hash"]}',
                  p_model_version => 'latest/shots_on_goal'
                );
            """)

            run_loader(conn, f"""
                SELECT nhl.load_saves_predictions_from_stage(
                  p_model_family  => '{saves_idx["family"]}',
                  p_model_params  => '{saves_params_json}'::jsonb,
                  p_feature_hash  => '{saves_idx["feature_hash"]}',
                  p_model_version => 'latest/goalie_saves'
                );
            """)

            # Clear stage
            with conn.cursor() as cur:
                cur.execute("TRUNCATE nhl.predictions_sog_stage; TRUNCATE nhl.predictions_saves_stage;")
            conn.commit()
            conn.close()

        print("✅ Upserted predictions to nhl.predictions and cleared staging.")
    else:
        print("ℹ️ No --db-url provided. CSVs are ready to import:")
        print(f"   SOG  → {out_sog}")
        print(f"   SAVES→ {out_saves}")

if __name__ == "__main__":
    main()
