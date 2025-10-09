#  backend/nhl/scripts/run_daily_slate.py
"""
run_daily_slate.py — score today's NHL slate and (optionally) load to Supabase.

What it does:
1) Scores SOG for lines 0.5,1.5,2.5,3.5
2) Scores Goalie Saves for lines 24.5,28.5
3) If a Postgres URL is provided, COPYs the results into stage tables and calls loaders.

Usage (CI example):
  python backend/nhl/scripts/run_daily_slate.py \
    --project nhl \
    --sog-csv exports/train_nhl_sog_v2.csv \
    --saves-csv exports/train_goalie_saves_v2.csv \
    --db-url "$SUPABASE_DB_URL"

Notes:
- Paths are resolved relative to this script (backend/nhl as BASE).
- Installs psycopg2-binary on first run if needed for COPY.
"""

from __future__ import annotations

import argparse, json, os, sys, subprocess, tempfile, csv
from pathlib import Path

# -------- Paths --------
HERE = Path(__file__).resolve().parent                  # backend/nhl/scripts
BASE = HERE.parent                                      # backend/nhl

MODEL_SOG_DIR   = BASE / "models" / "latest" / "shots_on_goal"
MODEL_SAVES_DIR = BASE / "models" / "latest" / "goalie_saves"
FEATURE_JSON    = BASE / "features" / "feature_metadata_nhl.json"
OUT_SOG         = BASE / "data" / "processed" / "sog_predictions.csv"
OUT_SAVES       = BASE / "data" / "processed" / "saves_predictions.csv"

# -------- Helpers --------
def read_model_index(model_dir: Path) -> dict:
    idx = json.loads((model_dir / "MODEL_INDEX.json").read_text())
    fh = model_dir / "FEATURE_HASH.txt"
    if fh.exists():
        idx["feature_hash"] = fh.read_text().strip()
    return idx

def ensure_psycopg2():
    try:
        import psycopg2  # noqa: F401
    except Exception:
        print("🔧 Installing psycopg2-binary…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-qU", "psycopg2-binary"])
        import psycopg2  # noqa: F401

def copy_csv(conn, table: str, path: Path, cols: list[str]) -> None:
    """
    COPY a CSV into table using explicit column list.
    Quotes identifiers so names like p_over_0_5 work.
    """
    cols_sql = ", ".join([f'"{c}"' for c in cols])
    sql = f"COPY {table} ({cols_sql}) FROM STDIN WITH CSV HEADER"
    with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(sql, f)

def run_loader(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        print("⬆️ loader result:", cur.fetchone())
    conn.commit()

def project_csv_allow_dotted(src: Path, dest: Path, wanted_cols: list[str]) -> None:
    """
    Write a CSV containing only wanted_cols, accepting either underscore
    or dotted variants in the source (e.g., p_over_0_5 or p_over_0.5).
    """
    def resolve_val(row: dict, col: str):
        v = row.get(col)
        if v is not None:
            return v
        # try dotted <-> underscore variants
        if col.endswith("_5"):
            dotted = col[:-2] + ".5"
            v = row.get(dotted)
            if v is not None:
                return v
        if ".5" in col:
            underscored = col.replace(".5", "_5")
            v = row.get(underscored)
            if v is not None:
                return v
        return ""

    with open(src, "r", newline="") as fin, open(dest, "w", newline="") as fout:
        r = csv.DictReader(fin)
        w = csv.DictWriter(fout, fieldnames=wanted_cols)
        w.writeheader()
        for row in r:
            w.writerow({c: resolve_val(row, c) for c in wanted_cols})

# -------- Main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="Project label (e.g., nhl). Label only; not a path.")
    ap.add_argument("--sog-csv", required=True, help="CSV with SOG features for the slate (exported earlier).")
    ap.add_argument("--saves-csv", required=True, help="CSV with Saves features for the slate (exported earlier).")
    default_scorer = BASE / "scripts" / "score_nhl_props.py"
    ap.add_argument("--scorer", default=str(default_scorer), help="Path to scorer script.")
    ap.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL"),
        help="Postgres URL (optional). If present, will load stage tables and run loaders.",
    )
    args = ap.parse_args()

    # insert this check immediately after args are parsed
    scorer_path = Path(args.scorer).resolve()
    if not scorer_path.exists():
        sys.exit(f"Missing scorer: {scorer_path}")

    # Sanity checks
    if not MODEL_SOG_DIR.exists() or not MODEL_SAVES_DIR.exists():
        sys.exit("Missing latest models under backend/nhl/models/latest/(shots_on_goal|goalie_saves). Train first.")
    if not FEATURE_JSON.exists():
        sys.exit(f"Missing feature metadata: {FEATURE_JSON}")

    # Read model metadata (family, params, feature_hash)
    sog_idx   = read_model_index(MODEL_SOG_DIR)
    saves_idx = read_model_index(MODEL_SAVES_DIR)

    # 1) Score SOG (0.5/1.5/2.5/3.5)
    sog_cmd = [
        sys.executable, str(scorer_path),
        "--model-dir", str(MODEL_SOG_DIR),
        "--csv", args.sog_csv,
        "--feature-json", str(FEATURE_JSON),
        "--feature-key", "shots_on_goal",
        "--line", "0.5,1.5,2.5,3.5",
        "--out", str(OUT_SOG),
    ]
    print("▶ Scoring SOG:", " ".join(sog_cmd))
    subprocess.check_call(sog_cmd)

    # 2) Score SAVES (24.5/28.5)
    saves_cmd = [
        sys.executable, str(scorer_path),
        "--model-dir", str(MODEL_SAVES_DIR),
        "--csv", args.saves_csv,
        "--feature-json", str(FEATURE_JSON),
        "--feature-key", "goalie_saves",
        "--line", "24.5,28.5",
        "--out", str(OUT_SAVES),
    ]
    print("▶ Scoring SAVES:", " ".join(saves_cmd))
    subprocess.check_call(saves_cmd)

    print(f"✅ Wrote: {OUT_SOG}")
    print(f"✅ Wrote: {OUT_SAVES}")

    # 3) Optional: load to DB
    if not args.db_url:
        print("ℹ️ No --db-url provided. CSVs are ready to import via Table Editor:")
        print(f"   SOG   → {OUT_SOG}")
        print(f"   SAVES → {OUT_SAVES}")
        return

    ensure_psycopg2()
    import psycopg2  # now safe to import

    # Use underscore column names expected by stage tables
    sog_cols   = ["player_id", "game_id", "p_over_0_5",  "p_over_1_5",  "p_over_2_5",  "p_over_3_5"]
    saves_cols = ["player_id", "game_id", "p_over_24_5", "p_over_28_5"]

    with tempfile.TemporaryDirectory() as td:
        tmp_sog   = Path(td) / "sog_stage.csv"
        tmp_saves = Path(td) / "saves_stage.csv"

        # Normalize possible dotted/underscore variants from scorer output
        project_csv_allow_dotted(OUT_SOG,   tmp_sog,   sog_cols)
        project_csv_allow_dotted(OUT_SAVES, tmp_saves, saves_cols)

        print("🔌 Connecting to DB…")
        conn = psycopg2.connect(args.db_url)

        # COPY into stage
        copy_csv(conn, "nhl.predictions_sog_stage",   tmp_sog,   sog_cols)
        copy_csv(conn, "nhl.predictions_saves_stage", tmp_saves, saves_cols)

        # Run loaders
        run_loader(conn, f"""
            SELECT nhl.load_sog_predictions_from_stage(
              p_model_family  => %s,
              p_model_params  => %s::jsonb,
              p_feature_hash  => %s,
              p_model_version => %s
            );
        """,)  # type: ignore

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT nhl.load_sog_predictions_from_stage(
                  p_model_family  => %s,
                  p_model_params  => %s::jsonb,
                  p_feature_hash  => %s,
                  p_model_version => %s
                );
                """,
                (sog_idx["family"], json.dumps(sog_idx.get("params", {})), sog_idx["feature_hash"], "latest/shots_on_goal"),
            )
            cur.execute(
                """
                SELECT nhl.load_saves_predictions_from_stage(
                  p_model_family  => %s,
                  p_model_params  => %s::jsonb,
                  p_feature_hash  => %s,
                  p_model_version => %s
                );
                """,
                (saves_idx["family"], json.dumps(saves_idx.get("params", {})), saves_idx["feature_hash"], "latest/goalie_saves"),
            )
        conn.commit()

        # Clear stage
        with conn.cursor() as cur:
            cur.execute("TRUNCATE nhl.predictions_sog_stage; TRUNCATE nhl.predictions_saves_stage;")
        conn.commit()
        conn.close()

    print("✅ Upserted predictions to nhl.predictions and cleared staging.")

if __name__ == "__main__":
    main()
