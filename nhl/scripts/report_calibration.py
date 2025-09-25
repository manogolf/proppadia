#!/usr/bin/env python3
"""
report_calibration.py — join predictions with labels and print calibration by decile.

Inputs:
  --preds   path to predictions CSV (e.g., sog_predictions.csv)
            expected cols: player_id, game_id, p_over_2.5, p_over_3.5, ...
  --labels  path to labels CSV (e.g., nhl_sog_training.csv)
            expected cols: player_id, game_id, shots_on_goal
  --lines   comma-separated lines to evaluate (e.g., 2.5,3.5)
  --out     path to write decile table CSV (default: ./calibration.csv)

Usage example (from your venv):
  python scripts/report_calibration.py \
    --preds /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/sog_predictions.csv \
    --labels /Users/jerrystrain/Projects/Proppadia/nhl-props/data/processed/nhl_sog_training.csv \
    --lines 2.5,3.5 \
    --out /Users/jerrystrain/Projects/Proppadia/nhl-props/reports/metrics/sog_calibration.csv
"""
import argparse, os, sys
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, log_loss

def brier(y, p): return float(np.mean((p - y) ** 2))

def make_deciles(p):
    # Quantile bins; duplicates='drop' handles small samples
    return pd.qcut(p, q=10, labels=range(1,11), duplicates="drop")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preds", required=True)
    ap.add_argument("--labels", required=True)
    ap.add_argument("--lines", required=True)
    ap.add_argument("--out", default="calibration.csv")
    args = ap.parse_args()

    preds = pd.read_csv(args.preds)
    labels = pd.read_csv(args.labels)

    required_pred_keys = {"player_id","game_id"}
    required_label_keys = {"player_id","game_id","shots_on_goal"}
    if not required_pred_keys.issubset(preds.columns):
        sys.exit(f"Preds missing {required_pred_keys - set(preds.columns)}")
    if not required_label_keys.issubset(labels.columns):
        sys.exit(f"Labels missing {required_label_keys - set(labels.columns)}")

    df = preds.merge(labels[["player_id","game_id","shots_on_goal"]], on=["player_id","game_id"], how="inner")
    if df.empty:
        sys.exit("No rows after merge on (player_id, game_id). Check inputs.")

    # Monotonic sanity across provided lines
    line_vals = [float(x) for x in args.lines.split(",") if x.strip()]
    line_vals = sorted(line_vals)
    mono_viol = 0
    if len(line_vals) >= 2:
        for i in range(len(line_vals)-1):
            L1, L2 = line_vals[i], line_vals[i+1]
            c1, c2 = f"p_over_{L1}", f"p_over_{L2}"
            if c1 in df.columns and c2 in df.columns:
                mono_viol += int((df[c1] < df[c2]).sum())
        if (len(line_vals) >= 2) and (df.shape[0] > 0):
            print(f"Monotonic check: violations where p_over_L1 < p_over_L2 across adjacent lines = {mono_viol} / {df.shape[0]*(len(line_vals)-1)}")

    # Per-line calibration & metrics
    rows = []
    for L in line_vals:
        col = f"p_over_{L}"
        if col not in df.columns:
            print(f"Skipping line {L}: column '{col}' not found in predictions.")
            continue
        y = (df["shots_on_goal"] > L).astype(int).values
        p = df[col].clip(1e-6, 1-1e-6).values

        bs = brier(y, p)
        try:
            ll = log_loss(y, p, labels=[0,1])
        except ValueError:
            ll = np.nan
        auc = np.nan
        if y.min() != y.max():
            try:
                auc = roc_auc_score(y, p)
            except Exception:
                auc = np.nan

        # Decile table
        try:
            df_dec = df[[col, "shots_on_goal"]].copy()
            df_dec["y"] = (df_dec["shots_on_goal"] > L).astype(int)
            df_dec["decile"] = make_deciles(df_dec[col])
            cal = df_dec.groupby("decile", as_index=False).agg(
                n=("y","size"),
                pred_mean=(col,"mean"),
                hit_rate=("y","mean")
            )
            cal["abs_err"] = (cal["pred_mean"] - cal["hit_rate"]).abs()
            # Expected Calibration Error (ECE)
            N = cal["n"].sum()
            ece = float((cal["n"] / N * cal["abs_err"]).sum()) if N > 0 else np.nan
        except Exception:
            cal = pd.DataFrame(columns=["decile","n","pred_mean","hit_rate","abs_err"])
            ece = np.nan

        print(f"\n=== Line {L} ===")
        print(f"Brier: {bs:.4f} | LogLoss: {ll if np.isnan(ll) else f'{ll:.4f}'} | AUC: {auc if np.isnan(auc) else f'{auc:.3f}'} | ECE: {ece if np.isnan(ece) else f'{ece:.3f}'}")
        print(cal.to_string(index=False))

        cal.insert(0, "line", L)
        rows.append(cal)

    if rows:
        out_df = pd.concat(rows, ignore_index=True)
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        out_df.to_csv(args.out, index=False)
        print(f"\n✅ Wrote decile table to: {args.out}")

if __name__ == "__main__":
    main()
