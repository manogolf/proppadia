#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import statsmodels.api as sm
import joblib

DEF_FEATURES = [
    "d5_points_avg","d10_points_avg","d10_sog_avg",
    "d10_attempts_avg","d10_toi_min_avg","d10_pp_min_avg","is_home"
]

def main():
    ap = argparse.ArgumentParser(description="Train Poisson/NB for NHL Points (goals+assists).")
    ap.add_argument("--train-csv", required=True)
    ap.add_argument("--outdir", default="models_out/nhl/points")
    ap.add_argument("--features", nargs="*", default=DEF_FEATURES)
    args = ap.parse_args()

    df = pd.read_csv(args.train_csv)
    df = df.replace([np.inf,-np.inf], np.nan).dropna(subset=args.features+["points"])
    y = df["points"].astype(float).values
    X = df[args.features].astype(float).values
    X = sm.add_constant(X, has_constant="add")

    # Poisson GLM
    pois = sm.GLM(y, X, family=sm.families.Poisson())
    pois_res = pois.fit(maxiter=200, method="newton")

    # Overdispersion check via Pearson residuals variance
    pearson = pois_res.resid_pearson
    od_ratio = float(np.var(pearson))
    use_nb = od_ratio > 1.6

    nb_res = None
    if use_nb:
        nb = sm.GLM(y, X, family=sm.families.NegativeBinomial())
        nb_res = nb.fit(maxiter=400, method="newton")

    best_name, best_model = ("poisson", pois_res)
    if nb_res is not None and nb_res.aic < pois_res.aic:
        best_name, best_model = ("neg_binom", nb_res)

    outdir = Path(args.outdir) / datetime.utcnow().strftime("v%Y%m%d")
    outdir.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"params": best_model.params, "features": args.features, "has_const": True, "family": best_name},
        outdir/"model.joblib"
    )
    (outdir/"FEATURES.json").write_text(json.dumps(args.features, indent=2))
    (outdir/"MODEL_META.json").write_text(json.dumps({
        "family": best_name,
        "aic": float(best_model.aic),
        "overdispersion_ratio": od_ratio,
        "trained_from": str(Path(args.train_csv).resolve())
    }, indent=2))

    print(f"✅ Trained {best_name} — AIC {best_model.aic:.1f} (overdispersion={od_ratio:.2f})")
    print(f"   Saved to {outdir}")

if __name__ == "__main__":
    main()
