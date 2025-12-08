#!/usr/bin/env python
"""
backtest_sog_denali.py

Export Denali SOG backtest data for calibration.

It pulls rows from nhl.predictions + skater logs, and writes a CSV:

    player_id,game_id,game_date,line,p_over_raw,y_over

Where:
  - p_over_raw = the model's p_over from nhl.predictions
  - y_over     = 1 if shots_on_goal > line, else 0

Usage example:

  python backend/nhl/scripts/backtest_sog_denali.py \
    --dsn "$SUPABASE_DB_URL" \
    --model-family shots_on_goal_denali \
    --model-version latest/shots_on_goal_denali \
    --start 2023-10-10 \
    --end   2025-12-05 \
    --out   backend/nhl/data/processed/sog_calibration_training_denali.csv

Notes:
  - DSN defaults to $SUPABASE_DB_URL if --dsn is omitted.
  - Adjust --model-family / --model-version to match how Denali
    rows are recorded in nhl.predictions.
"""

import argparse
import os
import subprocess
import sys
import textwrap


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Export Denali SOG backtest data for calibration.")
    ap.add_argument(
        "--dsn",
        help="Postgres DSN (defaults to $SUPABASE_DB_URL)",
        default=os.environ.get("SUPABASE_DB_URL", ""),
    )
    ap.add_argument(
        "--model-family",
        required=True,
        help="nhl.predictions.model_family value for Denali SOG (e.g. 'shots_on_goal_denali' or similar)",
    )
    ap.add_argument(
        "--model-version",
        required=True,
        help="nhl.predictions.model_version value for Denali SOG (e.g. 'latest/shots_on_goal_denali')",
    )
    ap.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    ap.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD, inclusive)",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output CSV path (e.g. backend/nhl/data/processed/sog_calibration_training_denali.csv)",
    )
    return ap.parse_args()


def build_sql(model_family: str, model_version: str, start_date: str, end_date: str) -> str:
    """
    Build the COPY SQL that pulls:
      player_id, game_id, game_date, line, p_over_raw, y_over
    for SOG Denali predictions with labels from skater logs.
    """
    # Basic sanity escaping – these should be simple strings, not raw SQL fragments.
    mf = model_family.replace("'", "''")
    mv = model_version.replace("'", "''")
    sd = start_date.replace("'", "''")
    ed = end_date.replace("'", "''")

    sql = textwrap.dedent(
        f"""
        COPY (
          SELECT
            p.player_id,
            p.game_id,
            g.game_date::date AS game_date,
            p.line::numeric    AS line,
            p.p_over::float8   AS p_over_raw,
            CASE
              WHEN l.shots_on_goal > p.line THEN 1
              ELSE 0
            END                AS y_over
          FROM nhl.predictions p
          JOIN nhl.games g
            ON g.game_id = p.game_id
          JOIN nhl.skater_game_logs_raw l
            ON l.player_id = p.player_id
           AND l.game_id   = p.game_id
          WHERE
            p.prop = 'shots_on_goal'
            AND p.model_family = '{mf}'
            AND p.model_version = '{mv}'
            AND g.game_date BETWEEN DATE '{sd}' AND DATE '{ed}'
        ) TO STDOUT WITH CSV HEADER;
        """
    ).strip()
    return sql


def main() -> None:
    args = parse_args()

    if not args.dsn:
        print("FATAL: No DSN provided and $SUPABASE_DB_URL is not set.", file=sys.stderr)
        sys.exit(1)

    sql = build_sql(
        model_family=args.model_family,
        model_version=args.model_version,
        start_date=args.start,
        end_date=args.end,
    )

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    cmd = [
        "psql",
        args.dsn,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]

    print("▶ Running backtest export with command:")
    print("  " + " ".join(cmd))
    print(f"  → writing to {args.out}")

    try:
        with open(args.out, "w", encoding="utf-8") as fh:
            proc = subprocess.run(
                cmd,
                stdout=fh,
                stderr=subprocess.PIPE,
                text=True,
            )
    except FileNotFoundError:
        print("FATAL: psql not found on PATH. Install psql or adjust PATH.", file=sys.stderr)
        sys.exit(1)

    if proc.returncode != 0:
        print("FATAL: psql failed:", file=sys.stderr)
        print(proc.stderr, file=sys.stderr)
        # Remove partial file if any
        try:
            os.remove(args.out)
        except OSError:
            pass
        sys.exit(proc.returncode)

    print("✅ Backtest export complete.")
    print(f"   Output: {args.out}")


if __name__ == "__main__":
    main()
