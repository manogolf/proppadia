#  backend/nhl/scripts/run_daily_slate.py
"""
Score today's NHL slate and (optionally) load to Supabase.

What it does:
1) Scores SOG for lines 0.5,1.5,2.5,3.5
2) Scores Goalie Saves for lines 24.5,28.5
3) If a Postgres URL is provided, loads via nhl.load_predictions_stage(...) and then
   finalizes with nhl.load_sog_predictions_from_stage(...) / nhl.load_saves_predictions_from_stage(...)

Usage:
  python backend/nhl/scripts/run_daily_slate.py \
    --project nhl \
    --sog-csv exports/train_nhl_sog_v2.csv \
    --saves-csv exports/train_goalie_saves_v2.csv \
    --db-url "$SUPABASE_DB_URL"
"""

from __future__ import annotations

import argparse, json, os, sys, subprocess
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
def db_connect(dsn: str):
    """Try psycopg (v3) first, then psycopg2 as a fallback. Returns an open connection."""
    try:
        import psycopg  # v3
        return psycopg.connect(dsn)
    except Exception:
        import psycopg2  # v2
        return psycopg2.connect(dsn)

def read_model_index(model_dir: Path) -> dict:
    idx = json.loads((model_dir / "MODEL_INDEX.json").read_text())
    fh = model_dir / "FEATURE_HASH.txt"
    if fh.exists():
        idx["feature_hash"] = fh.read_text().strip()
    return idx

def get_model_family(model_dir: Path | str) -> str:
    p = Path(model_dir) / "MODEL_INDEX.json"
    with open(p, "r") as f:
        return json.load(f)["family"]

def run_loader(conn, sql: str, params: tuple | list):
    """Execute a parameterized SQL call and commit."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()

# -------- Main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="Project label (e.g., nhl).")
    ap.add_argument("--sog-csv", required=True, help="CSV with SOG features for the slate.")
    ap.add_argument("--saves-csv", required=True, help="CSV with Saves features for the slate.")
    default_scorer = BASE / "scripts" / "score_nhl_props.py"
    ap.add_argument("--scorer", default=str(default_scorer), help="Path to scorer script.")
    ap.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL"),
        help="Postgres URL (optional). If present, will stage & finalize predictions.",
    )
    args = ap.parse_args()

    scorer_path = Path(args.scorer).resolve()
    if not scorer_path.exists():
        sys.exit(f"Missing scorer: {scorer_path}")

    # Sanity checks
    if not MODEL_SOG_DIR.exists() or not MODEL_SAVES_DIR.exists():
        sys.exit("Missing latest models under backend/nhl/models/latest/(shots_on_goal|goalie_saves). Train first.")
    if not FEATURE_JSON.exists():
        sys.exit(f"Missing feature metadata: {FEATURE_JSON}")

    # Read model metadata (for finalize step)
    sog_idx   = read_model_index(MODEL_SOG_DIR)
    saves_idx = read_model_index(MODEL_SAVES_DIR)

    # -------- 1) Score SOG (0.5/1.5/2.5/3.5) --------
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

    # -------- 2) Score SAVES (24.5/28.5) --------
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

    # -------- 3) Optional DB load & finalize --------
    if not args.db_url:
        print("ℹ️ No --db-url provided. CSVs are ready to import manually:")
        print(f"   SOG   → {OUT_SOG}")
        print(f"   SAVES → {OUT_SAVES}")
        return

    sog_family   = get_model_family(MODEL_SOG_DIR)
    saves_family = get_model_family(MODEL_SAVES_DIR)

    print("🔌 Connecting to DB…")
    conn = db_connect(args.db_url)

    try:
        # 3a) Stage both CSVs via unified function
        run_loader(
            conn,
            """
            SELECT nhl.load_predictions_stage(
                p_project         => %s,
                p_model_family    => %s,
                p_prop_key        => %s,
                p_predictions_csv => %s
            );
            """,
            (args.project, sog_family, "shots_on_goal", str(OUT_SOG)),
        )
        run_loader(
            conn,
            """
            SELECT nhl.load_predictions_stage(
                p_project         => %s,
                p_model_family    => %s,
                p_prop_key        => %s,
                p_predictions_csv => %s
            );
            """,
            (args.project, saves_family, "goalie_saves", str(OUT_SAVES)),
        )
        print("✅ Staged SOG & SAVES via nhl.load_predictions_stage")

        # 3b) Finalize from stage into nhl.predictions
        run_loader(
            conn,
            """
            SELECT nhl.load_sog_predictions_from_stage(
              p_model_family  => %s,
              p_model_params  => %s::jsonb,
              p_feature_hash  => %s,
              p_model_version => %s
            );
            """,
            (
                sog_idx["family"],
                json.dumps(sog_idx.get("params", {})),
                sog_idx.get("feature_hash"),
                "latest/shots_on_goal",
            ),
        )
        run_loader(
            conn,
            """
            SELECT nhl.load_saves_predictions_from_stage(
              p_model_family  => %s,
              p_model_params  => %s::jsonb,
              p_feature_hash  => %s,
              p_model_version => %s
            );
            """,
            (
                saves_idx["family"],
                json.dumps(saves_idx.get("params", {})),
                saves_idx.get("feature_hash"),
                "latest/goalie_saves",
            ),
        )
        print("✅ Finalized predictions from stage")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
