#!/usr/bin/env python3
# backend/nhl/scripts/score_points_poisson.py
import argparse, math, os, glob, json
import pandas as pd

try:
    import joblib
except Exception:
    joblib = None


def poisson_tail_over(mu: float, line: float) -> float:
    """
    P(X > line) for X~Poisson(mu). For half-lines (e.g., 0.5, 1.5),
    this is 1 - CDF(floor(line)). Compute CDF iteratively for stability.
    """
    if mu is None or math.isnan(mu) or mu < 0:
        return float("nan")
    kmax = int(math.floor(line))
    term = math.exp(-mu)  # k=0
    cdf = term
    for k in range(1, kmax + 1):
        term = term * (mu / k)
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))


def find_model_file(model_dir: str):
    if not model_dir or not os.path.isdir(model_dir):
        return None
    for pat in ("*.joblib", "*.pkl"):
        files = sorted(glob.glob(os.path.join(model_dir, pat)))
        if files:
            return files[-1]
    return None


def predict_mu_with_model(df: pd.DataFrame, model_path: str):
    """
    Load a scikit-learn style regressor (joblib/pickle).
    Assumes model.predict(X) returns the Poisson mean (mu) or expected value.
    Auto-select numeric feature columns; drop obvious identifiers/meta.
    """
    if not model_path or not os.path.exists(model_path) or joblib is None:
        return None
    try:
        model = joblib.load(model_path)
    except Exception:
        return None

    drop_like = {
        "player_id", "game_id", "team_id", "opponent_id", "is_home",
        "full_name", "player", "player_name", "team", "line",
        "price_over", "p_over_mkt", "fair_over", "game_date"
    }
    num_cols = [c for c, dt in df.dtypes.items() if pd.api.types.is_numeric_dtype(dt)]
    feat_cols = [c for c in num_cols if c not in drop_like]
    if not feat_cols:
        return None

    try:
        mu_hat = model.predict(df[feat_cols])
        mu_hat = pd.to_numeric(mu_hat, errors="coerce")
        return mu_hat
    except Exception:
        return None


def choose_mu_proxy(df: pd.DataFrame):
    """
    Fallback if no model or mu column:
    Prefer d10_points_avg, else d5_points_avg, else zeros.
    """
    if "d10_points_avg" in df.columns:
        return pd.to_numeric(df["d10_points_avg"], errors="coerce").fillna(0.0)
    if "d5_points_avg" in df.columns:
        return pd.to_numeric(df["d5_points_avg"], errors="coerce").fillna(0.0)
    return pd.Series([0.0] * len(df), index=df.index, dtype="float64")


def main():
    ap = argparse.ArgumentParser(description="Score NHL points and emit wide p_over_* columns.")
    ap.add_argument("--features-csv", required=True, help="exports/train_nhl_points_v2.csv")
    ap.add_argument("--model-dir", required=False, default="", help="models_out/nhl/points/v*/ (optional)")
    ap.add_argument("--odds-json", required=False, help="unused here; kept for CLI symmetry")
    ap.add_argument("--out", required=True, help="backend/nhl/data/processed/points_predictions.csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    df = pd.read_csv(args.features_csv)

    # Ensure identifier columns exist (don’t fail if missing)
    for col in ["player_id", "game_id", "team_id", "full_name", "line"]:
        if col not in df.columns:
            df[col] = ""

    # 1) Use existing mu if present
    mu = None
    for mu_name in ["mu", "pois_mu", "lambda", "lam"]:
        if mu_name in df.columns:
            mu = pd.to_numeric(df[mu_name], errors="coerce")
            break

    # 2) Else try model
    if mu is None or mu.isna().all():
        mdl_file = find_model_file(args.model_dir)
        mu_pred = predict_mu_with_model(df, mdl_file) if mdl_file else None
        if mu_pred is not None:
            mu = pd.to_numeric(mu_pred, errors="coerce")

    # 3) Else fallback proxy
    if mu is None or mu.isna().all():
        mu = choose_mu_proxy(df)

    # Wide probs at the common posted lines (0.5, 1.5)
    p_over_0_5 = mu.apply(lambda m: poisson_tail_over(m, 0.5))
    p_over_1_5 = mu.apply(lambda m: poisson_tail_over(m, 1.5))

    # Long-form p_over for the row’s own line if present
    def p_over_for_row(row):
        try:
            ln = float(row.get("line"))
        except Exception:
            return float("nan")
        return poisson_tail_over(row["_mu_"], ln)

    out = df.copy()
    out["_mu_"] = mu
    out["p_over_0_5"] = p_over_0_5
    out["p_over_1_5"] = p_over_1_5
    out["p_over"] = out.apply(p_over_for_row, axis=1)

    keep = [
        "player_id", "game_id", "team_id", "full_name", "line",
        "p_over", "p_over_0_5", "p_over_1_5"
    ]
    passthru = [c for c in ["price_over", "p_over_mkt", "fair_over", "game_date"] if c in out.columns]
    out = out[keep + passthru]

    out.to_csv(args.out, index=False)
    print(json.dumps({
        "rows": int(len(out)),
        "has_mu": bool(mu.notna().any()),
        "model_used": bool(find_model_file(args.model_dir)),
        "out": args.out
    }, indent=2))


if __name__ == "__main__":
    main()
