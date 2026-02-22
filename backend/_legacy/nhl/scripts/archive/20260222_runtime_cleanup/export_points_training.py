# backend/nhl/scripts/export_points_training.py
#!/usr/bin/env python3
"""
Export NHL skater points training set (last N years) to CSV using Python/Pandas.

Outputs columns (one row per player-game with a known label):
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  d5_points_avg, d10_points_avg, d10_sog_avg, d10_attempts_avg,
  d10_toi_min_avg, d10_pp_min_avg, d10_team_pp_min, opp_d5_goals_allowed_avg,
  y_points

Run:
  python backend/nhl/scripts/export_points_training.py \
    --db "$SUPABASE_DB_URL" \
    --years 3 \
    --out exports/train_nhl_points_history.csv
"""

from __future__ import annotations

import argparse
import os
import sys
import math
import pandas as pd
import numpy as np
import psycopg
from psycopg.rows import dict_row
from datetime import timedelta
from datetime import date
from dateutil.relativedelta import relativedelta

# ---------- CLI ----------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=False, default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"),
                    help="Postgres connection URL")
    ap.add_argument("--years", type=int, default=3, help="How many trailing years to include")
    ap.add_argument("--out", required=True, help="Output CSV path")
    return ap.parse_args()

# ---------- SQL ----------
LOGS_SQL = """
WITH base AS (
  SELECT
    l.player_id,
    l.game_id,
    l.team_id,
    (l.team_id = g.home_team_id) AS is_home,
    CASE
      WHEN l.team_id = g.home_team_id THEN g.away_team_id
      WHEN l.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    g.game_date::date AS game_date,
    COALESCE(sp.points,
             COALESCE(l.points, COALESCE(l.goals,0) + COALESCE(l.assists,0)))::int AS points,
    COALESCE(l.shots_on_goal, 0)::float  AS sog,
    COALESCE(l.shot_attempts, 0)::float  AS attempts,
    NULLIF(l.toi_minutes, 0)::float      AS toi_min,
    NULLIF(l.pp_toi_minutes, 0)::float   AS pp_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  LEFT JOIN nhl.skater_points_raw sp
    ON sp.player_id = l.player_id AND sp.game_id = l.game_id
  WHERE g.game_date >= %(cutoff)s
)
SELECT * FROM base
ORDER BY game_date, player_id, game_id;
"""

TEAM_GOALS_SQL = """
WITH team_goals AS (
  SELECT
    g.game_id,
    g.game_date::date AS game_date,
    t.team_id,
    COALESCE(
      (SELECT SUM(sp.goals)::int
         FROM nhl.skater_points_raw sp
        WHERE sp.game_id = g.game_id AND sp.team_id = t.team_id),
      (SELECT SUM(COALESCE(l.goals,0))::int
         FROM nhl.skater_game_logs_raw l
        WHERE l.game_id = g.game_id AND l.team_id = t.team_id),
      0
    ) AS goals_for
  FROM nhl.games g
  JOIN LATERAL (VALUES (g.home_team_id),(g.away_team_id)) AS t(team_id) ON TRUE
  WHERE g.game_date >= %(cutoff)s
)
SELECT * FROM team_goals
ORDER BY game_date, game_id, team_id;
"""

# ---------- Helpers ----------
def rolling_mean_prior(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling mean excluding the current row (left-closed window).
    """
    return series.rolling(window=window, min_periods=1).mean().shift(1)

def summarize_classes(df: pd.DataFrame) -> None:
    for line in (0.5, 1.5):
        if line <= 0.5:
            y = (df["y_points"] > 0).astype(int)
        else:
            y = (df["y_points"] > 1).astype(int)
        pos = int(y.sum())
        total = int(y.shape[0])
        print(f"{line}: {{'pos': {pos}, 'neg': {total - pos}, 'total': {total}}}")

# ---------- Main ----------
def main():
    args = parse_args()
    if not args.db:
        print("Missing --db or SUPABASE_DB_URL/DATABASE_URL", file=sys.stderr)
        sys.exit(2)

    # Ensure sslmode in URL
    db_url = args.db
    if "?sslmode=" not in db_url and "&sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"

    # Params for the two queries
    cutoff = date.today() - relativedelta(years=args.years)     # for LOGS_SQL
    years_interval = f"{max(1, args.years)} years"              # for TEAM_GOALS_SQL

    with psycopg.connect(db_url, row_factory=dict_row, autocommit=False) as conn:
        # LOGS_SQL
        with conn.cursor() as cur, conn.transaction():
            cur.execute("SET LOCAL statement_timeout = '900s'")
            cur.execute(LOGS_SQL, {"cutoff": cutoff})
            logs_rows = cur.fetchall()

        # TEAM_GOALS_SQL
        with conn.cursor() as cur, conn.transaction():
            cur.execute("SET LOCAL statement_timeout = '900s'")
            cur.execute(TEAM_GOALS_SQL, {"cutoff": cutoff})
            tg_rows = cur.fetchall()

    if not logs_rows:
        print("No rows returned from skater logs; nothing to export.", file=sys.stderr)
        sys.exit(1)

    logs = pd.DataFrame(logs_rows)
    tg = pd.DataFrame(tg_rows) if tg_rows else pd.DataFrame(columns=["game_id","game_date","team_id","goals_for"])

    # Types / sort
    for col in ("player_id","game_id","team_id","opponent_id"):
        if col in logs.columns:
            logs[col] = pd.to_numeric(logs[col], errors="coerce").astype("Int64")
    logs["is_home"] = logs["is_home"].astype(bool)
    logs["game_date"] = pd.to_datetime(logs["game_date"]).dt.date
    logs = logs.sort_values(["player_id","game_date","game_id"])

    # --- Player-level rolling (exclude current game) ---
    grp = logs.groupby("player_id", group_keys=False)

    logs["d5_points_avg"]     = grp["points"].apply(lambda s: rolling_mean_prior(s.astype(float), 5))
    logs["d10_points_avg"]    = grp["points"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))
    logs["d10_sog_avg"]       = grp["sog"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))
    logs["d10_attempts_avg"]  = grp["attempts"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))
    logs["d10_toi_min_avg"]   = grp["toi_min"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))
    logs["d10_pp_min_avg"]    = grp["pp_min"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))

    # --- Team-level PP minutes (exclude current game) ---
    tgrp = logs.sort_values(["team_id","game_date","game_id"]).groupby("team_id", group_keys=False)
    logs["d10_team_pp_min"] = tgrp["pp_min"].apply(lambda s: rolling_mean_prior(s.astype(float), 10))

    # --- Opponent 5-game goals-allowed (exclude current game) ---
    if not tg.empty:
        tg = tg.copy()
        tg["game_date"] = pd.to_datetime(tg["game_date"]).dt.date
        # pair each (game_id, team_id) with opponent goals_for for that game
        opp = tg.merge(tg, on="game_id", suffixes=("", "_opp"))
        opp = opp[opp["team_id"] != opp["team_id_opp"]][
            ["game_id", "team_id", "game_date", "goals_for_opp"]
        ].rename(columns={"goals_for_opp": "opp_goals_for"})
        opp = opp.sort_values(["team_id","game_date","game_id"])
        ogrp = opp.groupby("team_id", group_keys=False)
        opp["opp_d5_goals_allowed_avg"] = ogrp["opp_goals_for"].apply(
            lambda s: rolling_mean_prior(s.astype(float), 5)
        )
        # merge back to logs on (team_id, game_id)
        logs = logs.merge(
            opp[["team_id","game_id","opp_d5_goals_allowed_avg"]],
            on=["team_id","game_id"], how="left"
        )
    else:
        logs["opp_d5_goals_allowed_avg"] = np.nan

    # Label
    logs = logs.rename(columns={"points": "y_points"})

    # Keep rows with known label
    before = len(logs)
    logs = logs[logs["y_points"].notna()].copy()

    # Deduplicate (player_id, game_id)
    logs = logs.sort_values(["player_id","game_date","game_id"])
    logs = logs.drop_duplicates(subset=["player_id","game_id"], keep="last")

    # Final column order
    cols = [
        "player_id", "game_id", "team_id", "opponent_id",
        "is_home", "game_date",
        "d5_points_avg", "d10_points_avg", "d10_sog_avg", "d10_attempts_avg",
        "d10_toi_min_avg", "d10_pp_min_avg",
        "d10_team_pp_min", "opp_d5_goals_allowed_avg",
        "y_points"
    ]
    for c in cols:
        if c not in logs.columns:
            logs[c] = np.nan

    out_df = logs[cols].copy()

    # Minimal NA handling: keep PP mins NaN (legit “no PP history”), zero-fill others that are splits/attempts/time
    for c in ["d5_points_avg","d10_points_avg","d10_sog_avg","d10_attempts_avg","d10_toi_min_avg",
              "d10_team_pp_min","opp_d5_goals_allowed_avg"]:
        out_df[c] = out_df[c].astype(float).fillna(0.0)
    # Keep d10_pp_min_avg as-is (can be NaN)
    out_df["d10_pp_min_avg"] = out_df["d10_pp_min_avg"].astype(float)

    # Cast & write
    out_df["is_home"] = out_df["is_home"].astype(int)
    out_df.to_csv(args.out, index=False)
    print(f"✅ Wrote {len(out_df):,} rows to {args.out}")
    summarize_classes(out_df)

if __name__ == "__main__":
    # Pandas display tweaks (optional)
    pd.set_option("display.width", 180)
    pd.set_option("display.max_columns", 50)
    main()
