# ============================
# FILE: backend/nhl/scripts/score_points_poisson.py
# ============================
#!/usr/bin/env python3
"""
Scores today's slate with the trained Poisson/NB model.
Inputs:
  --features-csv  exports/train_nhl_points_v2.csv (from export_points.sql for SLATE_DATE)
  --model-dir     models_out/nhl/points/<version>/
  --odds-json     nhl/site/data/odds_nhl_playerprops_today.json (to infer active point lines)
Outputs:
  backend/nhl/data/processed/points_predictions.csv
"""
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import pandas as pd
import numpy as np
import joblib
from scipy.stats import poisson

def extract_lines(odds_json_path: Path) -> list[float]:
    try:
        obj = json.loads(odds_json_path.read_text())
    except Exception:
        return [0.5, 1.5, 2.5]  # sensible defaults
    lines = set()
    def walk(x):
        if isinstance(x, dict):
            if x.get("key") == "player_points":
                for o in x.get("outcomes",[]) or []:
                    if "point" in o and o["point"] is not None:
                        try: lines.add(float(o["point"]))
                        except: pass
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
    walk(obj)
    return sorted(lines) or [0.5,1.5,2.5]

def cdf_geq_poisson(k: int, lam: float) -> float:
    # P[X >= k] = 1 - CDF(k-1)
    return 1.0 - poisson.cdf(k-1, lam)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-csv", required=True)
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--odds-json", default="nhl/site/data/odds_nhl_playerprops_today.json")
    ap.add_argument("--out", default="backend/nhl/data/processed/points_predictions.csv")
    args = ap.parse_args()

    meta = joblib.load(Path(args.model_dir)/"model.joblib")
    params = np.array(meta["params"], dtype=float)
    feats = meta["features"]
    has_const = meta.get("has_const", True)

    df = pd.read_csv(args.features_csv)
    X = df[feats].astype(float).fillna(0.0).values
    if has_const:
        X = np.column_stack([np.ones(len(X)), X])
    eta = X @ params       # linear predictor
    lam = np.exp(eta)      # Poisson/NB mean
    df["lambda_hat"] = lam

    # produce per-line probabilities
    lines = extract_lines(Path(args.odds_json))
    for L in lines:
        k = math.ceil(L)
        df[f"p_over_{str(L).replace('.','_')}"] = [cdf_geq_poisson(k, l) for l in lam]

    keep = ["player_id","game_id","team_id","opponent_id","is_home","game_date","lambda_hat"] + [c for c in df.columns if c.startswith("p_over_")]
    df[keep].to_csv(args.out, index=False)
    print(f"✅ Wrote {args.out}  rows={len(df)}  lines={lines}")

if __name__ == "__main__":
    main()

