# backend/nhl/scripts/export_sog_denali_book_upload.py
"""
Export SOG predictions into a CSV formatted for the book-value upload tool.

**IMPORTANT CHANGE (2025-12-07)**

This script now uses the Random Forest probabilities directly:

    p_over_rf_0_5, p_over_rf_1_5, p_over_rf_2_5, p_over_rf_3_5

instead of the Denali-blended / calibrated p_over_* columns. The goal is to
restore realistic spread between players while we treat Denali calibration as
experimental.

Inputs
------
1) Wide predictions CSV:
     backend/nhl/data/processed/sog_predictions.csv

   Expected columns include (at minimum):
     - player_id
     - game_id
     - p_over_rf_0_5, p_over_rf_1_5, p_over_rf_2_5, p_over_rf_3_5

2) Postgres (Supabase) DB, via $SUPABASE_DB_URL, with:
     nhl.games(game_id, game_date, home_team_code, away_team_code)

Output
------
Writes:

  backend/nhl/data/processed/sog_denali_book_upload.csv

with columns:

  LEAGUE, DATE, HOME, AWAY, DOUBLEHEADER, SECTION, MARKET,
  SELECTOR, POINT, SIDE, WIN %

Where:
  - LEAGUE       = "NHL"
  - DATE         = game_date as YYYYMMDD
  - HOME         = home_team_code (e.g. ANA)
  - AWAY         = away_team_code (e.g. NJD)
  - DOUBLEHEADER = "" (blank)
  - SECTION      = "player_prop"
  - MARKET       = "player-shots_onGoal-ou"
  - SELECTOR     = player_id (NHL stats ID)
  - POINT        = line (0.5, 1.5, 2.5, 3.5, ...)
  - SIDE         = "over"
  - WIN %        = RF probability * 100, as a percentage
                   (e.g. p=0.573 → 57.3)

Usage
-----
  python backend/nhl/scripts/export_sog_denali_book_upload.py
"""

import os
import sys
from pathlib import Path
from typing import Optional, List

import pandas as pd
import psycopg


# ----------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

# This file lives at: backend/nhl/scripts/export_sog_denali_book_upload.py
# BASE_DIR should be the repo's backend/ directory.
BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "nhl" / "data" / "processed"

# NOTE: now reading the wide RF/LR prediction file, not the calibrated one.
PRED_CSV = DATA_DIR / "sog_predictions.csv"

OUT_CSV = DATA_DIR / "sog_denali_book_upload.csv"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def prob_to_win_prob(prob: float) -> Optional[float]:
    """
    Convert probability to the book's expected WIN % format:
    - Input: p in [0,1]
    - Output: p in [0,1] (e.g. 0.3206), or None if invalid.
    """
    try:
        p = float(prob)
    except (TypeError, ValueError):
        return None
    if not (0.0 <= p <= 1.0):
        return None
    return p


def load_predictions(path: Path) -> pd.DataFrame:
    """
    Load wide SOG predictions and ensure we have the RF columns we need.
    """
    if not path.exists():
        print(f"ERROR: Input CSV not found: {path}", file=sys.stderr)
        sys.exit(1)

    print(f"Using BASE_DIR={BASE_DIR}")
    print(f"Reading RF/LR SOG predictions from: {path}")

    df = pd.read_csv(path)

    required = {"player_id", "game_id"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        print(f"ERROR: Missing required columns in {path}: {missing}", file=sys.stderr)
        sys.exit(1)

    # We now expect RF-based probabilities
    prob_cols = [c for c in df.columns if c.startswith("p_over_rf_")]
    if not prob_cols:
        print(
            f"ERROR: No p_over_rf_* columns found in {path}. "
            f"Expected RF probabilities like p_over_rf_1_5.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Found RF probability columns: {prob_cols}")
    return df


def melt_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert wide RF predictions into long form:
      player_id, game_id, line, prob_over

    where columns p_over_rf_0_5, p_over_rf_1_5, ... become rows.
    """
    prob_cols = [c for c in df.columns if c.startswith("p_over_rf_")]
    id_cols = ["player_id", "game_id"]

    long_df = df.melt(
        id_vars=id_cols,
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )

    # Drop missing
    long_df = long_df.dropna(subset=["prob_over"])

    # Parse line from column name: p_over_rf_0_5 -> 0.5, p_over_rf_2_5 -> 2.5
    def col_to_line(col: str) -> float:
        # remove prefix
        s = col.replace("p_over_rf_", "")
        # convert 0_5 -> 0.5
        s = s.replace("_", ".")
        try:
            return float(s)
        except ValueError:
            return float("nan")

    long_df["line"] = long_df["prob_col"].apply(col_to_line)
    long_df = long_df.dropna(subset=["line"])

    # Clean types
    long_df["player_id"] = long_df["player_id"].astype("int64")
    long_df["game_id"] = long_df["game_id"].astype("int64")
    long_df["line"] = long_df["line"].astype(float)
    long_df["prob_over"] = long_df["prob_over"].astype(float)

    # Only keep the canonical lines you actually care about
    keep_lines = {0.5, 1.5, 2.5, 3.5}
    long_df = long_df[long_df["line"].isin(keep_lines)]

    print(
        f"Built long-form SOG rows from RF probabilities: {len(long_df)} "
        f"(player, game, line, prob_over)"
    )
    return long_df


def get_db_conn() -> psycopg.Connection:
    url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        print(
            "ERROR: SUPABASE_DB_URL (or DATABASE_URL) not set in environment.",
            file=sys.stderr,
        )
        sys.exit(1)
    return psycopg.connect(url)


def fetch_games(conn: psycopg.Connection, game_ids: List[int]) -> pd.DataFrame:
    """
    Fetch game_date, home_team_code, away_team_code for the given game_ids.
    """
    if not game_ids:
        return pd.DataFrame(
            columns=["game_id", "game_date", "home_team_code", "away_team_code"]
        )

    _sql = """
        SELECT
          game_id,
          game_date,
          home_team_code,
          away_team_code
        FROM nhl.games
        WHERE game_id = ANY(%s)
    """

    games = pd.read_sql(_sql, conn, params=(game_ids,))
    return games


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> None:
    # --- determine slate date (ET) ---
    # Priority:
    #  1) --slate-date CLI arg
    #  2) SLATE_DATE env
    #  3) ET today
    import argparse
    from datetime import datetime
    import pytz

    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--strict", action="store_true", help="Fail if predictions contain game_ids not on slate-date.")
    args = ap.parse_args()

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = args.slate_date or os.environ.get("SLATE_DATE") or et_today

    print(f"[book_upload] slate_date (ET) = {slate_date}")

    df_wide = load_predictions(PRED_CSV)
    df_long = melt_to_long(df_wide)

    unique_game_ids = sorted(df_long["game_id"].unique().tolist())
    print(f"Fetching game metadata for {len(unique_game_ids)} unique game_ids...")

    with get_db_conn() as conn:
        games = fetch_games(conn, unique_game_ids)

    if games.empty:
        print("WARNING: No matching rows in nhl.games for these game_ids. Output will be empty.", file=sys.stderr)
        return

    merged = df_long.merge(games, on="game_id", how="left")
    merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])

    if merged.empty:
        print("No rows after joining with nhl.games; nothing to write.")
        return

    # --- guardrail: filter to slate date ---
    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target = pd.to_datetime(slate_date).date()

    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().unique().tolist()})
    print(f"[book_upload] dates present in predictions after join: {dates_present}")

    before = len(merged)
    merged = merged[merged["game_date"] == target]
    after = len(merged)

    if after == 0:
        msg = (
            f"ERROR: after filtering to slate_date={slate_date}, zero rows remain.\n"
            f"Dates present were: {dates_present}\n"
            f"This usually means {PRED_CSV} is stale or built for a different SLATE_DATE."
        )
        print(msg, file=sys.stderr)
        sys.exit(1)

    if after < before:
        msg = (
            f"[book_upload] WARNING: filtered out {before - after} rows not on slate_date={slate_date} "
            f"(kept {after})."
        )
        if args.strict:
            print("ERROR: " + msg, file=sys.stderr)
            sys.exit(1)
        print(msg)

    # --- build upload rows ---
    rows = []
    for _, row in merged.iterrows():
        win_prob = prob_to_win_prob(row["prob_over"])
        if win_prob is None:
            continue

        date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")

        rows.append(
            {
                "LEAGUE": "NHL",
                "DATE": date_str,
                "HOME": row["home_team_code"],
                "AWAY": row["away_team_code"],
                "DOUBLEHEADER": "",
                "SECTION": "player_prop",
                "MARKET": "player-shots_onGoal-ou",
                "SELECTOR": int(row["player_id"]),
                "POINT": row["line"],
                "SIDE": "over",
                "WIN %": round(win_prob, 4),
            }
        )

    out_df = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False)
    print(f"Wrote {len(out_df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
