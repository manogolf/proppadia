#!/usr/bin/env python3
"""
Load Phoenix SOG predictions into nhl.predictions.

Inputs:
  --pred-csv   Path to sog_predictions.csv from score_sog_phoenix.py
  --db-url     Postgres connection string (SUPABASE_DB_URL)
  --slate-date (optional) for logging/debug; not required for insert

Assumptions (adjust if your schema differs):

Table: nhl.predictions
  - project      text
  - prop_type    text          -- 'sog', 'points', 'saves', etc.
  - player_id    bigint
  - game_id      bigint
  - line         numeric
  - prob_over    double precision
  - model_name   text          -- which model produced this (e.g. 'sog_phoenix_lr')
  - created_at   timestamptz   -- defaults to now()

Unique index (recommended):
  UNIQUE (project, prop_type, player_id, game_id, line)

If your column names differ, tweak the INSERT below accordingly.
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred-csv", required=True)
    ap.add_argument("--db-url", required=False, default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"))
    ap.add_argument("--prop-type", default="sog")
    ap.add_argument("--project", default="nhl")
    ap.add_argument("--slate-date", default=None)
    args = ap.parse_args()

    if not args.db_url:
        raise SystemExit("FATAL: --db-url or SUPABASE_DB_URL/DATABASE_URL is required")

    pred_path = Path(args.pred_csv)
    if not pred_path.exists():
        raise SystemExit(f"FATAL: predictions CSV not found: {pred_path}")

    df = pd.read_csv(pred_path)
    if df.empty:
        print(f"ℹ️ No SOG predictions in {pred_path}; nothing to load.")
        return

    # We ultimately want one row per (player, game, line, model)
    required_cols = {"player_id", "game_id", "line", "prob_over", "model"}
    missing = required_cols - set(df.columns)

    if missing:
        # Try to adapt from wide Denali format:
        #   player_id, game_id, ..., p_over_lr_0_5, p_over_rf_0_5, p_over_0_5, ...
        #
        # We *only* use the blended p_over_* columns for loading, not the lr/rf
        # components. So we look for columns like p_over_0_5, p_over_1_5, etc.
        wide_blend_cols = [
            c
            for c in df.columns
            if c.startswith("p_over_")
            and "_lr_" not in c
            and "_rf_" not in c
        ]

        if wide_blend_cols:
            base_cols = [
                c
                for c in [
                    "player_id",
                    "game_id",
                    "team_id",
                    "opponent_id",
                    "is_home",
                    "game_date",
                ]
                if c in df.columns
            ]

            long_rows = []
            for _, row in df.iterrows():
                base = {k: row[k] for k in base_cols}
                for col in wide_blend_cols:
                    val = row[col]
                    if pd.isna(val):
                        continue

                    # col like "p_over_2_5" -> suffix "2_5" -> line 2.5
                    suffix = col[len("p_over_"):]  # e.g. "2_5"
                    try:
                        line_val = float(suffix.replace("_", "."))
                    except ValueError:
                        # If somehow a non-line column sneaks in, skip it
                        continue

                    long_rows.append(
                        {
                            **base,
                            "line": line_val,
                            "model": "denali_blend",
                            "prob_over": float(val),
                        }
                    )

            if not long_rows:
                raise SystemExit(
                    f"FATAL: could not build long-format predictions from wide p_over_* "
                    f"columns in {pred_path}"
                )

            df = pd.DataFrame(long_rows)
            # Recompute required/missing after reshaping
            missing = required_cols - set(df.columns)

    if missing:
        raise SystemExit(
            f"FATAL: missing required columns in {pred_path}: {sorted(missing)}"
        )

    # Clean / coerce types
    df = df.copy()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["prob_over"] = pd.to_numeric(df["prob_over"], errors="coerce")

    df = df.dropna(subset=["player_id", "game_id", "line", "prob_over"])
    if df.empty:
        print("ℹ️ All prediction rows invalid after type coercion; nothing to load.")
        return

    rows = [
        (
            int(r.player_id),
            int(r.game_id),
            args.prop_type,      # becomes nhl.predictions.prop (e.g. "sog")
            float(r.line),
            float(r.prob_over),  # becomes p_over
            str(r.model),        # becomes model_family (e.g. "sog_phoenix_lr")
            "phoenix_v2",        # model_version tag – pick any label you like
        )
        for r in df.itertuples(index=False)
    ]

    print(f"→ Loading {len(rows)} SOG predictions into nhl.predictions")

    # Adjust column names to match nhl.predictions schema.
    # We treat prop_type as "prop", prob_over as "p_over",
    # model_name as "model_family", and hard-code a simple model_version.
    insert_sql = """
    INSERT INTO nhl.predictions (
      player_id,
      game_id,
      prop,
      line,
      p_over,
      model_family,
      model_version
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (player_id, game_id, prop, line)
    DO UPDATE SET
      p_over       = EXCLUDED.p_over,
      model_family = EXCLUDED.model_family,
      model_version= EXCLUDED.model_version,
      updated_at   = now();
    """

    with psycopg.connect(args.db_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()

    print("✅ Load complete.")


if __name__ == "__main__":
    main()
