#!/usr/bin/env python3
import os, sys, re, math, argparse
import pandas as pd
import numpy as np

def american_to_breakeven_p(odds):
    o = float(odds)
    if o >= 0:
        return 100.0 / (o + 100.0)
    else:
        return abs(o) / (abs(o) + 100.0)

def breakeven_to_american(p):
    p = float(p)
    if p <= 0 or p >= 1:
        return np.nan
    if p > 0.5:
        return -round(100 * p / (1 - p))
    else:
        return round(100 * (1 - p) / p)

def implied_ev_per_dollar(p, odds):
    # EV on $1 stake: p * win_return - (1-p) * 1
    # american -> decimal return
    o = float(odds)
    if o >= 0:
        ret = o / 100.0
    else:
        ret = 100.0 / abs(o)
    return p * ret - (1 - p)

def pick_prob(scores_row, market, side, line):
    # find column like p_over_2.5 or p_over_28.5
    col = f"p_over_{line:g}"
    if col not in scores_row.index:
        return None
    p_over = float(scores_row[col])
    return p_over if side == "over" else (1.0 - p_over)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scoresheet", required=True, help="CSV with p_over_* columns")
    ap.add_argument("--odds", required=True, help="CSV of odds (see intake format)")
    ap.add_argument("--market", choices=["sog","saves","all"], default="all")
    ap.add_argument("--min-edge", type=float, default=0.02, help="min (model_p - break_even_p)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Load scoresheet (must contain p_over_* columns produced by your scorer)
    S = pd.read_csv(args.scoresheet)
    # Normalize keys
    key_cols = [c for c in ["player_id","game_id","player_name","market"] if c in S.columns]
    if "market" not in S.columns:
        # If your combined scoresheet doesn’t have market, infer: SOG if it has p_over_2.5; SAVES if it has p_over_24.5 (crude but works if split files are used separately)
        if any(c.startswith("p_over_2.") or c == "p_over_2.5" for c in S.columns):
            S["market"] = "sog"
        else:
            S["market"] = "saves"

    # Load odds
    O = pd.read_csv(args.odds)
    O["market"] = O["market"].str.lower()
    O["side"]   = O["side"].str.lower()

    if args.market != "all":
        S = S[S["market"] == args.market]
        O = O[O["market"] == args.market]

    # Merge strategy:
    # 1) Try strict on (player_id, game_id, market). If player_id missing, fallback to (player_name, game_id, market).
    have_pid = "player_id" in S.columns and "player_id" in O.columns
    have_gid = "game_id" in S.columns and "game_id" in O.columns

    if have_pid and have_gid:
        M = O.merge(S, on=["player_id","game_id","market"], how="left", suffixes=("","_s"))
    else:
        # fallback on name+game_id
        if "player_name" not in O.columns:
            print("odds CSV missing player_name and player_id — need one for matching", file=sys.stderr)
            sys.exit(2)
        if "player_name" not in S.columns:
            # try to build from known columns
            if "full_name" in S.columns:
                S = S.rename(columns={"full_name":"player_name"})
            else:
                print("scoresheet missing player_name/full_name and player_id; cannot match", file=sys.stderr)
                sys.exit(2)
        M = O.merge(S, on=["player_name","game_id","market"], how="left", suffixes=("","_s"))

    # Compute model probability for this specific line/side
    probs = []
    missing_line = 0
    for idx, row in M.iterrows():
        p = pick_prob(row, row["market"], row["side"], float(row["line"]))
        if p is None or np.isnan(p):
            probs.append(np.nan)
            missing_line += 1
        else:
            probs.append(p)
    M["model_prob"] = probs

    # Break-even, edge, fair odds, EV
    M["breakeven_p"] = M["american_odds"].apply(american_to_breakeven_p)
    M["edge"]        = M["model_prob"] - M["breakeven_p"]
    M["fair_american"] = M["model_prob"].apply(breakeven_to_american)
    M["ev_per_$1"]   = M.apply(lambda r: implied_ev_per_dollar(r["model_prob"], r["american_odds"]), axis=1)

    # Filter: keep rows with a valid prob and edge threshold
    M = M[~M["model_prob"].isna()]
    M = M[M["edge"] >= args.min_edge]

    # Nice ordering
    keep_cols = [c for c in ["player_id","player_name","game_id","market","side","line","book","american_odds",
                             "model_prob","breakeven_p","edge","fair_american","ev_per_$1","ts"] if c in M.columns]
    M = M.sort_values(["market","edge","model_prob"], ascending=[True, False, False])[keep_cols]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    M.to_csv(args.out, index=False)

    print(f"✅ Wrote bettable list to: {args.out}")
    if missing_line:
        print(f"ℹ️ Skipped {missing_line} odds rows with lines not present in scoresheet columns.", file=sys.stderr)

if __name__ == "__main__":
    main()
