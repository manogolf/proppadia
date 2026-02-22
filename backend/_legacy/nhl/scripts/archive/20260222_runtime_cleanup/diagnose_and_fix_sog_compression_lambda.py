#!/usr/bin/env python3
"""
diagnose_and_fix_sog_compression_lambda.py

Diagnose + fix SOG "small square" compression by switching from
binary over-probability to a Poisson mean λ (expected SOG), then
pricing P(over line) from λ.

Uses:
- Historical training CSV used by your Denali SOG models:
    backend/nhl/exports/train_sog_denali.csv
- Today's slate features:
    backend/nhl/exports/train_nhl_sog_denali.csv
- Feature list:
    backend/nhl/features/feature_metadata_nhl.json (key: shots_on_goal_denali)

Output:
- backend/nhl/data/processed/sog_lambda_odds_debug.csv

Run:
ppb
python backend/nhl/scripts/diagnose_and_fix_sog_compression_lambda.py \
  --train backend/nhl/exports/train_sog_denali.csv \
  --slate-features backend/nhl/exports/train_nhl_sog_denali.csv \
  --feature-metadata backend/nhl/features/feature_metadata_nhl.json \
  --out backend/nhl/data/processed/sog_lambda_odds_debug.csv
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

def tf_to_int(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()

    allowed = {"t","f","true","false","1","0","1.0","0.0"}
    bad_vals = sorted(set(s[~s.isin(list(allowed))].unique().tolist()))
    if bad_vals:
        raise SystemExit(f"Unexpected non-boolean values found in flag column: {bad_vals[:10]}")

    mapping = {
        "t": 1.0, "true": 1.0, "1": 1.0, "1.0": 1.0,
        "f": 0.0, "false": 0.0, "0": 0.0, "0.0": 0.0,
    }
    return s.map(mapping).astype(float)

def load_feature_list(feature_metadata_path: Path) -> list[str]:
    meta = json.loads(feature_metadata_path.read_text())
    if "shots_on_goal_denali" in meta and isinstance(meta["shots_on_goal_denali"], list):
        return [str(x) for x in meta["shots_on_goal_denali"]]
    if "shots_on_goal" in meta and isinstance(meta["shots_on_goal"], list):
        return [str(x) for x in meta["shots_on_goal"]]
    raise SystemExit(
        f"No shots_on_goal_denali (or shots_on_goal) list found in {feature_metadata_path}"
    )


def ensure_cols(df: pd.DataFrame, cols: Iterable[str], who: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(
            f"{who} missing required columns (showing up to 30): {missing[:30]}"
            + (" ..." if len(missing) > 30 else "")
        )


def prob_to_american(p: float) -> int | None:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        odds = - (p / (1.0 - p)) * 100.0
    else:
        odds = ((1.0 - p) / p) * 100.0
    return int(round(odds))


def poisson_p_over(lmbda: float, line: float) -> float:
    """
    For half-integer lines:
      P(over 1.5) == P(X >= 2) == 1 - P(X <= 1)
    """
    if lmbda <= 0:
        return 0.0
    k = int(math.floor(line + 1e-9))  # 1.5->1, 2.5->2, 3.5->3
    cdf = 0.0
    term = math.exp(-lmbda)
    cdf += term  # i=0
    for i in range(1, k + 1):
        term *= lmbda / i
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))


def describe(series: pd.Series, name: str) -> str:
    s = series.dropna().astype(float)
    if len(s) == 0:
        return f"{name}: n=0"
    q = s.quantile([0.05, 0.25, 0.50, 0.75, 0.95]).to_dict()
    return (
        f"{name}: n={len(s)} "
        f"min={s.min():.4f} p05={q[0.05]:.4f} p25={q[0.25]:.4f} "
        f"p50={q[0.50]:.4f} p75={q[0.75]:.4f} p95={q[0.95]:.4f} "
        f"max={s.max():.4f} std={s.std(ddof=0):.4f}"
    )


def detect_target_col(df: pd.DataFrame) -> str:
    # Common historical count targets
    candidates = [
        "shots_on_goal",
        "sog",
        "shots",
        "shots_on_goal_final",
        "sog_final",
    ]
    for c in candidates:
        if c in df.columns:
            return c

    # helpful fallback
    sog_like = [c for c in df.columns if ("sog" in c.lower() or "shot" in c.lower())]
    raise SystemExit(
        "Could not find a SOG count target column in training CSV.\n"
        f"Tried: {candidates}\n"
        f"SOG/shot-like columns found: {sog_like[:50]}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", default="backend/nhl/exports/train_sog_denali.csv")
    ap.add_argument("--slate-features", default="backend/nhl/exports/train_nhl_sog_denali.csv")
    ap.add_argument("--feature-metadata", default="backend/nhl/features/feature_metadata_nhl.json")
    ap.add_argument("--out", default="backend/nhl/data/processed/sog_lambda_odds_debug.csv")
    ap.add_argument("--lines", default="0.5,1.5,2.5,3.5")
    ap.add_argument("--max-train-rows", type=int, default=250000)
    args = ap.parse_args()

    train_path = Path(args.train)
    slate_path = Path(args.slate_features)
    meta_path = Path(args.feature_metadata)
    out_path = Path(args.out)
    lines = [float(x.strip()) for x in args.lines.split(",") if x.strip()]

    print("▶ Loading feature list:", meta_path)
    feat_cols = load_feature_list(meta_path)
    print("  features:", len(feat_cols))

    print("\n▶ Loading training CSV:", train_path)
    train = pd.read_csv(train_path)
    target_col = detect_target_col(train)
    print(f"  target_col={target_col!r}")
    ensure_cols(train, feat_cols, "Training CSV")

    # Clean + fill (match your COALESCE-heavy export behavior)
    train = train.replace([float("inf"), float("-inf")], pd.NA)
    train = train.dropna(subset=[target_col])
    train[feat_cols] = train[feat_cols].fillna(0.0)

    if len(train) > args.max_train_rows:
        train = train.sample(args.max_train_rows, random_state=7)

    for c in ["is_home", "b2b_flag", "hot_last5_flag"]:
        if c in train.columns:
            train[c] = tf_to_int(train[c])
   
    X = train[feat_cols].astype(float)
    y = train[target_col].astype(float).clip(lower=0.0)

    print(f"  train rows used: {len(train)}")
    print(f"  y mean={y.mean():.3f} std={y.std(ddof=0):.3f}")

    print("\n▶ Fitting λ model (nonlinear Poisson boosting)...")

    # Train-time sanity (tells us whether the target supports high-SOG tails)
    print("  y quantiles:", y.quantile([0.50, 0.75, 0.90, 0.95, 0.99]).to_dict())

    try:
        # Nonlinear, handles interactions automatically, supports Poisson loss directly.
        from sklearn.ensemble import HistGradientBoostingRegressor

        model = HistGradientBoostingRegressor(
            loss="poisson",
            max_depth=6,
            learning_rate=0.06,
            max_iter=400,
            min_samples_leaf=40,
            l2_regularization=0.0,
            random_state=7,
        )

        model.fit(X, y)
        print("  fitted: HistGradientBoostingRegressor(loss='poisson')")

    except Exception as e:
        # Fallback that still captures nonlinearity well (not Poisson-loss, but works fine for λ).
        print(f"  ⚠️ Poisson boosting unavailable ({e}); falling back to RandomForestRegressor")
        from sklearn.ensemble import RandomForestRegressor

        model = RandomForestRegressor(
            n_estimators=500,
            max_depth=16,
            min_samples_leaf=25,
            n_jobs=-1,
            random_state=7,
        )
        model.fit(X, y)
        print("  fitted: RandomForestRegressor")

    # Optional: quick in-sample λ sanity (we're not trying to evaluate here, just see the range)
    lam_train = pd.Series(model.predict(X)).clip(lower=0.0, upper=20.0)
    print("  λ_train quantiles:", lam_train.quantile([0.50, 0.75, 0.90, 0.95, 0.99]).to_dict())
    print("  λ_train max:", float(lam_train.max()))
    print("\n▶ Loading slate features:", slate_path)
    slate = pd.read_csv(slate_path)
    ensure_cols(slate, ["player_id", "game_id"], "Slate CSV")
    ensure_cols(slate, feat_cols, "Slate CSV")
    slate = slate.replace([float("inf"), float("-inf")], pd.NA)
    slate[feat_cols] = slate[feat_cols].fillna(0.0)

    # ⬇️ convert only the known t/f flag columns
    for c in ["is_home", "b2b_flag", "hot_last5_flag"]:
        if c in slate.columns:
            slate[c] = tf_to_int(slate[c])

    lam = pd.Series(model.predict(slate[feat_cols].astype(float))).clip(lower=0.0, upper=12.0)

    out = slate[["player_id", "game_id"]].copy()
    for c in ["team_id", "opponent_id", "is_home", "game_date", "season", "full_name", "team_code"]:
        if c in slate.columns:
            out[c] = slate[c]
    out["lambda_sog"] = lam

    for L in lines:
        p = out["lambda_sog"].apply(lambda z: poisson_p_over(float(z), L))
        out[f"p_over_{L}"] = p
        out[f"fair_american_{L}"] = p.apply(prob_to_american)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print("\n✅ wrote:", out_path)

    # Print spread diagnostics — this is what should "unsqueeze the square"
    print("\n▶ Slate spread diagnostics")
    print(describe(out["lambda_sog"], "lambda_sog"))
    for L in lines:
        print(describe(out[f"p_over_{L}"], f"p_over_{L}"))

    print("\nWhat to look for:")
    print("- If p_over_1.5 / p_over_2.5 / p_over_3.5 have wide p05..p95 ranges, λ pricing fixes compression.")
    print("- If they’re still tight, the features themselves aren’t differentiating enough (rare given your list).")

    # --- debug: why slate λ is low? (no merges) ---
    KEY_COLS = [
        "attempts_d10_per60",
        "d10_sog_per60",
        "d20_sog_per60",
        "pace_matchup_index",
        "role_pp_share",
        "szn_toi_per_game_5on5",
        "szn_toi_per_game_pp",
        "szn_shifts_per_game_5on5",
        "season_5on5_icetime_per_game",
        "season_5on5_shifts_per_game",
        "last10_team_sog_share",
    ]

    print("\n▶ Top slate by lambda_sog (key feature values)")
    show_cols = [c for c in ["player_id", "game_id", "lambda_sog"] + KEY_COLS if c in out.columns]
    print(out.sort_values("lambda_sog", ascending=False)[show_cols].head(15).to_string(index=False))

    print("\n▶ Compare slate vs train feature ranges (same columns)")
    for c in KEY_COLS:
        if c in train.columns and c in slate.columns:
            t = pd.to_numeric(train[c], errors="coerce")
            s = pd.to_numeric(slate[c], errors="coerce")
            print(f"  {c}")
            print(f"    train p50={t.quantile(0.50):.3f} p90={t.quantile(0.90):.3f} p99={t.quantile(0.99):.3f}")
            print(f"    slate p50={s.quantile(0.50):.3f} p90={s.quantile(0.90):.3f} p99={s.quantile(0.99):.3f}")

if __name__ == "__main__":
    main()
