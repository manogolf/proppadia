#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")  # p_over_1_5 etc.

def prob_cols(df: pd.DataFrame) -> list[tuple[str, float]]:
    out = []
    for c in df.columns:
        m = PCOL_RE.match(c)
        if not m:
            continue
        whole = int(m.group(1))
        half = int(m.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((c, line))
    out.sort(key=lambda x: x[1])
    return out

def melt_wide(df: pd.DataFrame) -> pd.DataFrame:
    cols = prob_cols(df)
    if not cols:
        raise SystemExit("No p_over_* cols found.")
    keep = ["player_id", "game_id"] + [c for c, _ in cols]
    df = df[keep].copy()

    long = df.melt(
        id_vars=["player_id", "game_id"],
        var_name="prob_col",
        value_name="p_raw",
    )
    mp = {c: line for c, line in cols}
    long["line"] = long["prob_col"].map(mp).astype(float)
    long.drop(columns=["prob_col"], inplace=True)

    long["player_id"] = pd.to_numeric(long["player_id"], errors="coerce")
    long["game_id"] = pd.to_numeric(long["game_id"], errors="coerce")
    long["p_raw"] = pd.to_numeric(long["p_raw"], errors="coerce")
    long = long.dropna(subset=["player_id", "game_id", "line", "p_raw"])
    long["player_id"] = long["player_id"].astype(int)
    long["game_id"] = long["game_id"].astype(int)
    long["p_raw"] = long["p_raw"].astype(float)
    return long

def logloss(y: np.ndarray, p: np.ndarray) -> float:
    eps = 1e-15
    p = np.clip(p, eps, 1 - eps)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())

def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))

def auc(y: np.ndarray, p: np.ndarray) -> float:
    # simple AUC (no sklearn dependency) using rank method
    # returns nan if all one class
    y = y.astype(int)
    if y.min() == y.max():
        return float("nan")
    order = np.argsort(p)
    y_sorted = y[order]
    n_pos = y_sorted.sum()
    n_neg = len(y_sorted) - n_pos
    ranks = np.arange(1, len(y_sorted) + 1)
    sum_ranks_pos = ranks[y_sorted == 1].sum()
    return float((sum_ranks_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred-wide", required=True, help="Wide calibrated preds CSV (p_over_* cols)")
    ap.add_argument("--date", required=True, help="Game date to evaluate (YYYY-MM-DD), completed day")
    ap.add_argument("--db-url", required=True, help="SUPABASE_DB_URL")
    ap.add_argument("--actuals-table", default="nhl.skater_game_logs_raw", help="Actuals source table")
    ap.add_argument("--lines", default="1.5,2.5,3.5", help="Comma list of lines to score")
    args = ap.parse_args()

    pred_path = Path(args.pred_wide)
    dfw = pd.read_csv(pred_path)
    long = melt_wide(dfw)

    # Join games to filter to args.date via game_id
    game_ids = sorted(long["game_id"].unique().tolist())
    if not game_ids:
        raise SystemExit("No game_ids in predictions.")

    lines = [float(x.strip()) for x in args.lines.split(",") if x.strip()]
    long = long[long["line"].isin(lines)].copy()

    sql_games = """
      SELECT game_id::bigint AS game_id, game_date::date AS game_date
      FROM nhl.games
      WHERE game_id = ANY(%s::bigint[])
    """

    sql_actuals = f"""
      SELECT
        l.player_id::bigint AS player_id,
        l.game_id::bigint   AS game_id,
        COALESCE(NULLIF(BTRIM(l.shots_on_goal::text), ''), '0')::int AS sog
      FROM {args.actuals_table} l
      WHERE l.game_id = ANY(%s::bigint[])
    """

    with psycopg2.connect(args.db_url) as conn:
        games = pd.read_sql(sql_games, conn, params=(game_ids,))
        actuals = pd.read_sql(sql_actuals, conn, params=(game_ids,))

    # Filter to date
    games["game_date"] = pd.to_datetime(games["game_date"]).dt.date
    target = pd.to_datetime(args.date).date()
    games = games[games["game_date"] == target].copy()

    if games.empty:
        raise SystemExit(f"No games found for {args.date} in nhl.games for these game_ids.")

    long = long.merge(games[["game_id"]], on="game_id", how="inner")
    m = long.merge(actuals, on=["game_id", "player_id"], how="inner")

    if m.empty:
        raise SystemExit(
            f"Joined 0 rows with actuals for {args.date}. "
            f"Likely actuals not populated in {args.actuals_table} yet."
        )

    print(f"[probe] joined rows={len(m)} unique_games={m['game_id'].nunique()} date={args.date}")

    # Evaluate per line
    for line in sorted(m["line"].unique()):
        mm = m[m["line"] == line].copy()
        # y = indicator OVER line: sog > line
        y = (mm["sog"].astype(float).values > float(line)).astype(int)

        p_over_as_is = mm["p_raw"].astype(float).values
        p_over_if_flipped = 1.0 - p_over_as_is

        # compute metrics
        for name, p in [
            ("as_is (treat p_raw as P(OVER))", p_over_as_is),
            ("flipped (treat p_raw as P(UNDER))", p_over_if_flipped),
        ]:
            ll = logloss(y, p)
            br = brier(y, p)
            au = auc(y, p)
            print(f"line={line:.1f}  {name:34s}  logloss={ll:.4f}  brier={br:.4f}  auc={au:.4f}  n={len(y)}")

if __name__ == "__main__":
    main()
