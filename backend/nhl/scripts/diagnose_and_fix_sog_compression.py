#!/usr/bin/env python3
"""
diagnose_and_fix_sog_compression.py

Goal
----
1) Diagnose whether your current SOG pricing is "compressed" (model odds clustered in a narrow band)
   compared to market spread (e.g., 1.5/2.5/3.5 lines all around +100…+140 or +350…+450).
2) Provide a concrete "solve" path: build a simple Poisson λ (expected SOG) model from your
   existing historical calibration/training CSV, then compute P(over line) from λ. This
   immediately creates proper line-dependent spread, even before any calibration.

What it expects in your repo (defaults; override with flags):
- Historical training rows (must include shots_on_goal target and the feature columns):
    backend/nhl/data/processed/sog_calibration_training_denali.csv
- Today's pregame features (Denali export used by your current model):
    backend/nhl/exports/train_nhl_sog_denali.csv
- Feature list json:
    backend/nhl/features/feature_metadata_nhl.json
  It tries keys in order: shots_on_goal_denali, shots_on_goal

Optional:
- A "with market" CSV containing at least player_id,game_id,line,market_odds,model_odds (or similar).
  If you pass it, the script will quantify compression vs market directly.

Outputs:
- A CSV with λ-based probabilities and implied fair odds:
    backend/nhl/data/processed/sog_lambda_odds_debug.csv

Usage
-----
ppb
python backend/nhl/scripts/diagnose_and_fix_sog_compression.py \
  --slate-features backend/nhl/exports/train_nhl_sog_denali.csv \
  --train backend/nhl/data/processed/sog_calibration_training_denali.csv \
  --feature-metadata backend/nhl/features/feature_metadata_nhl.json \
  --out backend/nhl/data/processed/sog_lambda_odds_debug.csv

If you have a merged market/model CSV:
  --with-market nhl/site/data/sog_with_market.csv

Notes
-----
- This uses sklearn's PoissonRegressor to predict λ. If sklearn isn't installed in your venv, install:
    pip install scikit-learn
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from sklearn.linear_model import PoissonRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception as e:
    raise SystemExit(
        "Missing scikit-learn. Install in your venv: pip install scikit-learn\n"
        f"Original import error: {e}"
    )


# -----------------------------
# odds helpers
# -----------------------------
def prob_to_american(p: float) -> int | None:
    """Convert probability to American odds (rounded). Returns None if p out of (0,1)."""
    if not (0.0 < p < 1.0):
        return None
    # American odds:
    # p = 0.5 => +100
    # favorites (p>0.5): negative odds
    if p >= 0.5:
        odds = - (p / (1 - p)) * 100
    else:
        odds = ((1 - p) / p) * 100
    return int(round(odds))


def poisson_p_over(lmbda: float, line: float) -> float:
    """
    Compute P(SOG > line) where line is a half-integer like 0.5, 1.5, 2.5, 3.5.
    For half-integers, P(SOG > 1.5) == P(SOG >= 2) == 1 - P(SOG <= 1).
    """
    if lmbda < 0:
        return 0.0
    k = int(math.floor(line + 1e-9))  # 1.5 -> 1, 2.5 -> 2, etc.
    # P(X <= k) for Poisson:
    # sum_{i=0..k} e^-λ λ^i / i!
    cdf = 0.0
    term = math.exp(-lmbda)
    cdf += term  # i=0
    for i in range(1, k + 1):
        term *= lmbda / i
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))


def describe_compression(series: pd.Series, name: str) -> dict:
    s = series.dropna().astype(float)
    if len(s) == 0:
        return {"name": name, "n": 0}
    return {
        "name": name,
        "n": int(len(s)),
        "min": float(s.min()),
        "p05": float(s.quantile(0.05)),
        "p25": float(s.quantile(0.25)),
        "p50": float(s.quantile(0.50)),
        "p75": float(s.quantile(0.75)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
        "std": float(s.std(ddof=0)),
    }


def print_desc(d: dict) -> None:
    if d.get("n", 0) == 0:
        print(f"  {d['name']}: n=0")
        return
    print(
        f"  {d['name']}: n={d['n']} "
        f"min={d['min']:.4f} p05={d['p05']:.4f} p25={d['p25']:.4f} "
        f"p50={d['p50']:.4f} p75={d['p75']:.4f} p95={d['p95']:.4f} "
        f"max={d['max']:.4f} std={d['std']:.4f}"
    )


# -----------------------------
# main
# -----------------------------
def load_feature_list(feature_metadata_path: Path) -> list[str]:
    meta = json.loads(feature_metadata_path.read_text())
    for key in ("shots_on_goal_denali", "shots_on_goal"):
        if key in meta and isinstance(meta[key], list) and meta[key]:
            return [str(x) for x in meta[key]]
    raise SystemExit(
        f"No shots_on_goal feature list found in {feature_metadata_path}. "
        "Expected key shots_on_goal_denali or shots_on_goal."
    )


def ensure_cols(df: pd.DataFrame, cols: Iterable[str], who: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{who} is missing required columns: {missing[:30]}" + (" ..." if len(missing) > 30 else ""))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", default="backend/nhl/data/processed/sog_calibration_training_denali.csv")
    ap.add_argument("--slate-features", default="backend/nhl/exports/train_nhl_sog_denali.csv")
    ap.add_argument("--feature-metadata", default="backend/nhl/features/feature_metadata_nhl.json")
    ap.add_argument("--out", default="backend/nhl/data/processed/sog_lambda_odds_debug.csv")
    ap.add_argument(
        "--with-market",
        default=None,
        help="Optional CSV to quantify compression vs market directly. "
             "Should include line + market odds and model odds columns.",
    )
    ap.add_argument("--lines", default="0.5,1.5,2.5,3.5")
    ap.add_argument("--max-train-rows", type=int, default=200000)
    args = ap.parse_args()

    train_path = Path(args.train)
    slate_path = Path(args.slate_features)
    meta_path = Path(args.feature_metadata)
    out_path = Path(args.out)

    lines = [float(x.strip()) for x in args.lines.split(",") if x.strip()]

    print("▶ Loading feature list from:", meta_path)
    feat_cols = load_feature_list(meta_path)
    print("  features:", len(feat_cols))

    print("\n▶ Loading training data:", train_path)
    train = pd.read_csv(train_path)
    # target expectation: shots_on_goal exists (historical)
    if "shots_on_goal" not in train.columns:
        raise SystemExit("Training CSV must include shots_on_goal column as the count target.")
    ensure_cols(train, feat_cols, "Training CSV")

    # Basic cleaning
    train = train.replace([float("inf"), float("-inf")], pd.NA)
    # keep rows with non-null target
    train = train.dropna(subset=["shots_on_goal"])
    # fill feature nulls with 0 (consistent with your COALESCE-heavy SQL exports)
    train[feat_cols] = train[feat_cols].fillna(0.0)

    if len(train) > args.max_train_rows:
        train = train.sample(args.max_train_rows, random_state=7)

    X = train[feat_cols].astype(float)
    y = train["shots_on_goal"].astype(float).clip(lower=0.0)

    print(f"  train rows used: {len(train)}")
    print(f"  y mean={y.mean():.3f}  y std={y.std(ddof=0):.3f}")

    print("\n▶ Fitting Poisson λ model (PoissonRegressor)...")
    # Scaling helps stability; PoissonRegressor in sklearn expects reasonably scaled inputs.
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("pois", PoissonRegressor(alpha=1e-4, max_iter=2000)),
        ]
    )
    model.fit(X, y)
    print("  fitted.")

    print("\n▶ Loading slate features:", slate_path)
    slate = pd.read_csv(slate_path)
    ensure_cols(slate, ["player_id", "game_id"], "Slate features CSV")
    ensure_cols(slate, feat_cols, "Slate features CSV")
    slate = slate.replace([float("inf"), float("-inf")], pd.NA)
    slate[feat_cols] = slate[feat_cols].fillna(0.0)

    Xs = slate[feat_cols].astype(float)
    lam = model.predict(Xs)
    # clip to sane values
    lam = pd.Series(lam).clip(lower=0.0, upper=12.0)

    out = slate[["player_id", "game_id"]].copy()
    if "team_id" in slate.columns:
        out["team_id"] = slate["team_id"]
    if "opponent_id" in slate.columns:
        out["opponent_id"] = slate["opponent_id"]
    if "is_home" in slate.columns:
        out["is_home"] = slate["is_home"]

    out["lambda_sog"] = lam

    # Compute probabilities and implied odds for each line
    for L in lines:
        p = out["lambda_sog"].apply(lambda z: poisson_p_over(float(z), L))
        out[f"p_over_{L}"] = p
        out[f"fair_american_{L}"] = p.apply(prob_to_american)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print("\n✅ wrote:", out_path)

    # Diagnose "spread" (compression) of λ-based probabilities on the slate
    print("\n▶ Slate spread (λ-based) by line:")
    for L in lines:
        print_desc(describe_compression(out[f"p_over_{L}"], f"p_over_{L}"))

    # Optional: quantify compression vs market/model odds if you have a merged file
    if args.with_market:
        wm_path = Path(args.with_market)
        print("\n▶ Loading with-market CSV:", wm_path)
        wm = pd.read_csv(wm_path)

        # Heuristics for column names (you can rename in the CSV if needed)
        # Required: line and some market/model odds columns
        # Common patterns: line, market_american, model_american, market_odds, model_odds
        line_col = "line" if "line" in wm.columns else None
        if not line_col:
            raise SystemExit("with-market CSV must include a 'line' column.")
        # try to find odds columns
        market_col = next((c for c in ["market_american", "market_odds", "market"] if c in wm.columns), None)
        model_col = next((c for c in ["model_american", "model_odds", "model"] if c in wm.columns), None)
        if not market_col or not model_col:
            raise SystemExit(
                "with-market CSV must include market/model odds columns. "
                "Tried market_american/market_odds/market and model_american/model_odds/model."
            )

        # convert odds to implied probabilities
        def american_to_prob(a: float) -> float:
            a = float(a)
            if a == 0:
                return float("nan")
            if a < 0:
                return (-a) / ((-a) + 100.0)
            return 100.0 / (a + 100.0)

        wm["p_market"] = wm[market_col].apply(american_to_prob)
        wm["p_model"] = wm[model_col].apply(american_to_prob)

        print("\n▶ Compression check vs market/model (implied prob spread):")
        for L in sorted(wm[line_col].dropna().unique()):
            sub = wm[wm[line_col] == L]
            print(f"  line={L}")
            print_desc(describe_compression(sub["p_market"], "p_market"))
            print_desc(describe_compression(sub["p_model"], "p_model"))

        # If wm includes player_id/game_id, we can compare λ-based probabilities too
        if {"player_id", "game_id"}.issubset(wm.columns):
            merged = wm.merge(out, on=["player_id", "game_id"], how="left")
            for L in lines:
                col = f"p_over_{L}"
                if col in merged.columns:
                    print(f"\n▶ line={L} λ-based vs market (quick correlation on overlaps):")
                    m = merged[merged[line_col] == L].dropna(subset=["p_market", col])
                    if len(m) >= 20:
                        corr = float(m["p_market"].corr(m[col]))
                        print(f"  n={len(m)} corr(p_market, {col})={corr:.3f}")
                    else:
                        print(f"  n={len(m)} (not enough overlap rows for correlation)")

    print("\nNext action if λ-based spread looks healthy:")
    print("  - Use lambda_sog + Poisson to price 0.5/1.5/2.5/3.5, then (optionally) calibrate p_over_line.")
    print("  - If you still want isotonic, calibrate p_over_line produced from λ, not the compressed classifier output.")


if __name__ == "__main__":
    main()
