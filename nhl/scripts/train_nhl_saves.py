#!/usr/bin/env python3
"""
NHL — Train Goalie Saves (Poisson vs Negative Binomial) with temporal CV and optional per-line isotonic calibration.

Save as: scripts/train_nhl_saves.py

This mirrors your SOG trainer, but targets goalie SAVES.
- Expects a CSV with one row per goalie-game and a count label column (default: `saves`)
- Uses features listed under the `goalie_saves` key in your feature_metadata_nhl.json
- Temporal CV selects the best family/hyperparams by mean Brier across --eval-lines
- Optional per-line isotonic calibration on a small "calibration" window before holdout,
  with guardrails (only kept if it improves LL or ECE; requires basic class balance)

Example run (after you’ve prepared a CSV like data/processed/nhl_saves_training.csv):
  python scripts/train_nhl_saves.py \
    --csv data/processed/nhl_saves_training.csv \
    --date-col game_date \
    --label-col saves \
    --feature-json features/feature_metadata_nhl.json \
    --feature-key goalie_saves \
    --out-dir models/latest/goalie_saves \
    --eval-lines 24.5,28.5 \
    --n-folds 5 \
    --holdout-days 7 \
    --calibration-days 14
"""
import argparse, os, json, hashlib, warnings, math
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

import numpy as np
import pandas as pd
import pandas.api.types as ptypes
from scipy.stats import nbinom, poisson
from sklearn.linear_model import PoissonRegressor
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss, roc_auc_score
import statsmodels.api as sm

warnings.filterwarnings("ignore", category=FutureWarning)

# ----------------------------- Utilities ----------------------------- #

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def brier_score(y_true_prob: np.ndarray, y_pred_prob: np.ndarray) -> float:
    return float(np.mean((y_pred_prob - y_true_prob) ** 2))

def is_bool_or_binary(s: pd.Series) -> bool:
    if ptypes.is_bool_dtype(s):
        return True
    vals = pd.Series(s.dropna().unique())
    if len(vals) <= 2:
        try:
            as_int = pd.to_numeric(vals, errors="coerce").dropna().astype(int)
            return set(as_int.unique()).issubset({0, 1})
        except Exception:
            return False
    return False

def winsorize_numeric(s: pd.Series, p_low=0.01, p_high=0.99) -> pd.Series:
    lo, hi = s.quantile(p_low), s.quantile(p_high)
    return s.clip(lo, hi)

def prepare_features(df: pd.DataFrame, feature_list: List[str]) -> pd.DataFrame:
    """Cast numerics, handle booleans, one-hot categoricals, return numeric frame."""
    X = df[feature_list].copy()
    for c in X.columns:
        col = X[c]
        if is_bool_or_binary(col):
            X[c] = pd.to_numeric(col, errors="coerce").fillna(0).astype(float)
        elif ptypes.is_numeric_dtype(col):
            X[c] = pd.to_numeric(col, errors="coerce").astype(float)
            X[c] = winsorize_numeric(X[c]).fillna(X[c].median())
        else:
            X[c] = col.astype(str).fillna("other")
    cat_cols = [c for c in X.columns if not ptypes.is_numeric_dtype(X[c])]
    if cat_cols:
        X = pd.get_dummies(X, columns=cat_cols, dummy_na=False)
    # Ensure numeric dtype
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    return X

def make_temporal_folds(df: pd.DataFrame, date_col: str, n_folds: int, gap_days:int=2) -> List[Tuple[np.ndarray, np.ndarray]]:
    df_sorted = df.sort_values(date_col).reset_index(drop=True)
    dates = pd.to_datetime(df_sorted[date_col]).values.astype("datetime64[D]")
    unique_days = np.unique(dates)
    if len(unique_days) < max(3, n_folds + 2):
        return []
    edges = np.linspace(0, len(unique_days), n_folds + 1).astype(int)
    folds = []
    for k in range(n_folds):
        start, end = edges[k], edges[k+1]
        if end - start < 3:
            continue
        val_days = unique_days[start:end]
        train_days = unique_days[: max(0, start - gap_days)]
        tr_idx = df_sorted.index[np.isin(dates, train_days)]
        va_idx = df_sorted.index[np.isin(dates, val_days)]
        if len(tr_idx)==0 or len(va_idx)==0:
            continue
        folds.append((tr_idx.values, va_idx.values))
    return folds

def prob_over_poisson(mu: np.ndarray, line: float) -> np.ndarray:
    k = int(math.floor(line))
    return 1.0 - poisson.cdf(k, mu)

def prob_over_nb(mu: np.ndarray, alpha: float, line: float) -> np.ndarray:
    a = max(alpha, 1e-8)
    r = 1.0 / a
    p = r / (r + mu)
    k = int(math.floor(line))
    return 1.0 - nbinom.cdf(k, r, p)

def compute_ece(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> float:
    p = np.clip(p_pred, 1e-6, 1 - 1e-6)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.digitize(p, bins) - 1
    ece, N = 0.0, len(y_true)
    for b in range(n_bins):
        mask = (idx == b)
        if not np.any(mask): continue
        p_mean = float(p[mask].mean())
        y_mean = float(y_true[mask].mean())
        ece += (mask.sum() / N) * abs(p_mean - y_mean)
    return float(ece)

@dataclass
class CVResult:
    family: str
    params: Dict
    mean_brier: float
    mean_logloss: float
    mean_auc: float

# ----------------------------- Core Training ----------------------------- #

def run_cv_and_select(
    df_train_core: pd.DataFrame,
    date_col: str,
    label_col: str,
    features: List[str],
    eval_lines: List[float],
    n_folds: int,
    alpha_grid_poisson: List[float],
    alpha_grid_nb: List[float],
) -> Tuple[CVResult, Dict]:
    X_raw = prepare_features(df_train_core, features)
    y = df_train_core[label_col].astype(int).values
    folds = make_temporal_folds(df_train_core, date_col, n_folds, gap_days=2)
    if not folds:
        raise ValueError("Temporal CV failed to create folds. Check date coverage.")

    # Poisson grid
    poi_results = []
    for a in alpha_grid_poisson:
        briers, lls, aucs = [], [], []
        for tr_idx, va_idx in folds:
            Xt, Xv = X_raw.iloc[tr_idx], X_raw.iloc[va_idx]
            yt, yv = y[tr_idx], y[va_idx]
            model = PoissonRegressor(alpha=a, fit_intercept=True, max_iter=1000, tol=1e-7)
            model.fit(Xt, yt)
            mu_val = np.clip(model.predict(Xv), 1e-6, None)
            b_line, ll_line, auc_line = [], [], []
            for L in eval_lines:
                y_over = (yv > L).astype(int)
                p_over = np.clip(prob_over_poisson(mu_val, L), 1e-6, 1 - 1e-6)
                b_line.append(brier_score(y_over, p_over))
                try: ll_line.append(log_loss(y_over, p_over, labels=[0,1]))
                except ValueError: ll_line.append(np.nan)
                if y_over.min() != y_over.max():
                    auc_line.append(roc_auc_score(y_over, p_over))
            briers.append(np.nanmean(b_line)); lls.append(np.nanmean(ll_line)); aucs.append(np.nanmean(auc_line) if auc_line else np.nan)
        poi_results.append(CVResult("poisson", {"alpha": a}, float(np.nanmean(briers)), float(np.nanmean(lls)), float(np.nanmean(aucs))))

    # Negative Binomial grid (statsmodels GLM)
    nb_results = []
    for disp_alpha in alpha_grid_nb:
        briers, lls, aucs = [], [], []
        for tr_idx, va_idx in folds:
            Xt, Xv = X_raw.iloc[tr_idx], X_raw.iloc[va_idx]
            yt, yv = y[tr_idx], y[va_idx]
            Xt_sm = sm.add_constant(Xt, has_constant="add")
            Xv_sm = sm.add_constant(Xv, has_constant="add")
            fam = sm.families.NegativeBinomial(alpha=disp_alpha)
            try:
                nb_model = sm.GLM(yt, Xt_sm, family=fam)
                nb_res = nb_model.fit(maxiter=200, tol=1e-8)
            except Exception:
                nb_model = sm.GLM(yt, Xt_sm, family=sm.families.Poisson())
                nb_res = nb_model.fit(maxiter=200, tol=1e-8)
            mu_val = np.clip(nb_res.predict(Xv_sm), 1e-6, None)
            b_line, ll_line, auc_line = [], [], []
            for L in eval_lines:
                y_over = (yv > L).astype(int)
                p_over = np.clip(prob_over_nb(mu_val, disp_alpha, L), 1e-6, 1 - 1e-6)
                b_line.append(brier_score(y_over, p_over))
                try: ll_line.append(log_loss(y_over, p_over, labels=[0,1]))
                except ValueError: ll_line.append(np.nan)
                if y_over.min() != y_over.max():
                    auc_line.append(roc_auc_score(y_over, p_over))
            briers.append(np.nanmean(b_line)); lls.append(np.nanmean(ll_line)); aucs.append(np.nanmean(auc_line) if auc_line else np.nan)
        nb_results.append(CVResult("neg_binomial", {"disp_alpha": disp_alpha}, float(np.nanmean(briers)), float(np.nanmean(lls)), float(np.nanmean(aucs))))

    all_results = poi_results + nb_results
    best = min(all_results, key=lambda r: r.mean_brier)
    diagnostics = {"folds": len(folds), "poisson": [r.__dict__ for r in poi_results], "neg_binomial": [r.__dict__ for r in nb_results]}
    return best, diagnostics

def fit_winner(family: str, params: Dict, df_fit: pd.DataFrame, features: List[str], label_col: str):
    X = prepare_features(df_fit, features)
    y = df_fit[label_col].astype(int).values
    if family == "poisson":
        model = PoissonRegressor(alpha=params["alpha"], fit_intercept=True, max_iter=2000, tol=1e-8)
        model.fit(X, y)
        artifact = {"coef": model.coef_.tolist(), "intercept": float(model.intercept_), "feature_order": list(X.columns)}
        return ("poisson", model, artifact)
    else:
        Xt_sm = sm.add_constant(X, has_constant="add")
        fam = sm.families.NegativeBinomial(alpha=params["disp_alpha"])
        nb_model = sm.GLM(y, Xt_sm, family=fam)
        nb_res = nb_model.fit(maxiter=200, tol=1e-8)
        artifact = {"params": nb_res.params.tolist(), "feature_order_with_const": list(Xt_sm.columns)}
        return ("neg_binomial", nb_res, artifact)

def predict_family(model_obj, family: str, X: pd.DataFrame, nb_alpha: Optional[float], line: float) -> np.ndarray:
    if family == "poisson":
        mu = np.clip(model_obj.predict(X), 1e-6, None)
        return np.clip(prob_over_poisson(mu, line), 1e-6, 1-1e-6)
    else:
        X_sm = sm.add_constant(X, has_constant="add")
        mu = np.clip(model_obj.predict(X_sm), 1e-6, None)
        return np.clip(prob_over_nb(mu, nb_alpha, line), 1e-6, 1-1e-6)

def fit_isotonic_per_line(
    model_obj, family: str, params: Dict,
    df_cal: pd.DataFrame, features: List[str], label_col: str, eval_lines: List[float],
    min_per_class: int = 10,   # tolerate smaller windows; still guarded by improvement test
    improve_tol: float = 0.005
) -> Dict[str, Dict]:
    """Fit isotonic on calibration window; keep per-line only if it improves LL or ECE."""
    if df_cal is None or df_cal.empty:
        return {}
    Xc = prepare_features(df_cal, features)
    yc = df_cal[label_col].astype(int).values
    out = {}
    for L in eval_lines:
        y_over = (yc > L).astype(int)
        n_pos, n_neg = int(y_over.sum()), int(len(y_over) - y_over.sum())
        if n_pos < min_per_class or n_neg < min_per_class:
            print(f"ℹ️  Skipping calibration for line {L}: insufficient class balance (pos={n_pos}, neg={n_neg}, need ≥{min_per_class} each).")
            continue
        p_raw = predict_family(model_obj, family, Xc, params.get("disp_alpha"), L)
        p_raw = np.clip(p_raw, 1e-6, 1-1e-6)
        raw_ll  = float(log_loss(y_over, p_raw, labels=[0,1]))
        raw_ece = compute_ece(y_over, p_raw, n_bins=10)
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0, increasing=True)
        iso.fit(p_raw, y_over)
        grid_x = np.linspace(0.0, 1.0, 101)
        grid_y = iso.predict(grid_x)
        p_cal  = np.clip(np.interp(p_raw, grid_x, grid_y), 1e-6, 1-1e-6)
        cal_ll  = float(log_loss(y_over, p_cal, labels=[0,1]))
        cal_ece = compute_ece(y_over, p_cal, n_bins=10)
        if (raw_ll - cal_ll) >= improve_tol or (raw_ece - cal_ece) >= improve_tol:
            out[str(L)] = {
                "method": "isotonic",
                "grid_x": grid_x.tolist(),
                "grid_y": grid_y.tolist(),
                "n": int(len(y_over)),
                "diagnostics": {
                    "raw_ll": raw_ll, "cal_ll": cal_ll,
                    "raw_ece": raw_ece, "cal_ece": cal_ece,
                    "pos": n_pos, "neg": n_neg
                }
            }
            print(f"✅ Keeping calibration for line {L}: LL {raw_ll:.3f}→{cal_ll:.3f}, ECE {raw_ece:.3f}→{cal_ece:.3f}")
        else:
            print(f"↩️  Dropping calibration for line {L}: no improvement (LL {raw_ll:.3f}→{cal_ll:.3f}, ECE {raw_ece:.3f}→{cal_ece:.3f}).")
    return out

def eval_lines_metrics(y_true_counts: np.ndarray, p_map: Dict[float, np.ndarray]) -> Dict[str, Dict]:
    metrics = {}
    for L, p_over in p_map.items():
        y_over = (y_true_counts > L).astype(int)
        p = np.clip(p_over, 1e-6, 1-1e-6)
        m = {"brier": float(brier_score(y_over, p))}
        try: m["log_loss"] = float(log_loss(y_over, p, labels=[0,1]))
        except ValueError: m["log_loss"] = float("nan")
        if y_over.min() != y_over.max():
            try: m["auc"] = float(roc_auc_score(y_over, p))
            except Exception: m["auc"] = None
        else:
            m["auc"] = None
        metrics[str(L)] = m
    return metrics

def save_artifacts(
    out_dir: str,
    feature_key: str,
    best: CVResult,
    feature_hash: str,
    eval_lines: List[float],
    holdout_days: int,
    metrics_holdout_raw: Dict[str, Dict],
    metrics_holdout_cal: Optional[Dict[str, Dict]],
    model_subartifact: Dict,
    calibration_payload: Optional[Dict],
):
    os.makedirs(out_dir, exist_ok=True)
    model_artifact = {
        "family": best.family,
        "params": best.params,
        "feature_key": feature_key,
        "eval_lines": eval_lines,
        "calibration": calibration_payload or {},
    }
    model_artifact.update({"sklearn_poisson": model_subartifact} if best.family=="poisson" else {"statsmodels_nb": model_subartifact})
    with open(os.path.join(out_dir, "MODEL_ARTIFACT.json"), "w") as f:
        json.dump(model_artifact, f, indent=2)

    model_index = {
        "prop": feature_key,
        "family": best.family,
        "params": best.params,
        "feature_hash": feature_hash,
        "feature_key": feature_key,
        "eval_lines": eval_lines,
        "holdout_days": holdout_days,
        "metrics_holdout": metrics_holdout_raw,
        "metrics_holdout_calibrated": metrics_holdout_cal or {},
        "artifact_files": ["MODEL_ARTIFACT.json"],
    }
    with open(os.path.join(out_dir, "MODEL_INDEX.json"), "w") as f:
        json.dump(model_index, f, indent=2)

    with open(os.path.join(out_dir, "FEATURE_HASH.txt"), "w") as f:
        f.write(feature_hash)

    print(f"✅ Saved model to: {out_dir}")
    print(json.dumps(model_index, indent=2))

# ----------------------------- CLI ----------------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--date-col", default="game_date")
    ap.add_argument("--label-col", default="saves")
    ap.add_argument("--feature-json", required=True)
    ap.add_argument("--feature-key", default="goalie_saves")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--eval-lines", default="24.5,28.5")
    ap.add_argument("--n-folds", type=int, default=5)
    ap.add_argument("--holdout-days", type=int, default=14)
    ap.add_argument("--calibration-days", type=int, default=7)
    ap.add_argument("--alpha-grid-poisson", default="0.0,0.0001,0.001,0.01,0.1")
    ap.add_argument("--alpha-grid-nb", default="0.2,0.5,1.0,2.0")
    args = ap.parse_args()

    # Load data
    df = pd.read_csv(args.csv)
    if args.date_col not in df.columns:
        raise ValueError(f"Missing date column: {args.date_col}")
    if args.label_col not in df.columns:
        raise ValueError(f"Missing label column: {args.label_col}")
    df[args.date_col] = pd.to_datetime(df[args.date_col])

    # Load feature registry
    with open(args.feature_json, "r") as f:
        feat_meta = json.load(f)
    if args.feature_key not in feat_meta:
        raise ValueError(f"feature_key '{args.feature_key}' not found in {args.feature_json}")
    # original registry from JSON
    raw_features = feat_meta[args.feature_key]
    all_cols = set(df.columns)

    # minimal columns we truly require
    required = {"is_home", "rest_days", "b2b_flag"}
    # if the spec includes start_prob for goalies, make it required too
    if "start_prob" in raw_features:
        required.add("start_prob")

    missing_required = [c for c in required if c not in all_cols]
    if missing_required:
        raise ValueError(f"Missing REQUIRED features: {missing_required}")

    # keep only features that actually exist in the CSV; warn about the rest
    used_features = [c for c in raw_features if c in all_cols]
    skipped = [c for c in raw_features if c not in all_cols]
    if skipped:
        print(f"[warn] Skipping missing optional features: {skipped}")

    # from here on, train with the gated list
    features = used_features
    feature_hash = sha256_str(json.dumps(features, sort_keys=True))

    eval_lines = [float(x) for x in args.eval_lines.split(",") if x.strip()]
    alpha_grid_poisson = [float(x) for x in args.alpha_grid_poisson.split(",") if x.strip()]
    alpha_grid_nb = [float(x) for x in args.alpha_grid_nb.split(",") if x.strip()]

    # Temporal windows: TRAIN_CORE | CAL | HOLDOUT
    max_date = df[args.date_col].max()
    holdout_start = max_date - pd.Timedelta(days=args.holdout_days)
    calib_start = holdout_start - pd.Timedelta(days=args.calibration_days)

    df_calib = df[(df[args.date_col] > calib_start) & (df[args.date_col] <= holdout_start)].copy()
    df_holdout = df[df[args.date_col] > holdout_start].copy()
    df_train_core = df[df[args.date_col] <= calib_start].copy()
    if df_train_core.empty:
        raise ValueError("No data in TRAIN_CORE after applying calibration window. Reduce --calibration-days or --holdout-days.")

    # CV selection
    best, diagnostics = run_cv_and_select(
        df_train_core=df_train_core,
        date_col=args.date_col,
        label_col=args.label_col,
        features=features,
        eval_lines=eval_lines,
        n_folds=args.n_folds,
        alpha_grid_poisson=alpha_grid_poisson,
        alpha_grid_nb=alpha_grid_nb,
    )
    print("📊 CV Diagnostics:")
    print(json.dumps(diagnostics, indent=2))
    print(f"🏆 Winner: {best.family} with params={best.params} | mean_brier={best.mean_brier:.6f}")

    # Fit winner on TRAIN_CORE
    fam, model_obj, subartifact = fit_winner(best.family, best.params, df_train_core, features, args.label_col)

    # Per-line isotonic on CAL (guarded by balance + improvement)
    calibration_payload = fit_isotonic_per_line(
        model_obj=model_obj,
        family=fam,
        params=best.params,
        df_cal=df_calib,
        features=features,
        label_col=args.label_col,
        eval_lines=eval_lines,
        min_per_class=10,        # tolerant but safe
        improve_tol=0.005
    ) if not df_calib.empty else {}
    if calibration_payload:
        print(f"✅ Fitted isotonic calibrators for lines: {list(calibration_payload.keys())}")
    else:
        print("ℹ️ Skipped calibration (no df_calib rows or guardrails dropped all lines).")

    # Holdout eval (raw + calibrated if present)
    metrics_holdout_raw, metrics_holdout_cal = {}, {}
    if not df_holdout.empty:
        Xh = prepare_features(df_holdout, features)
        yh = df_holdout[args.label_col].astype(int).values
        raw_map = {L: predict_family(model_obj, fam, Xh, best.params.get("disp_alpha"), L) for L in eval_lines}
        metrics_holdout_raw = eval_lines_metrics(yh, raw_map)
        if calibration_payload:
            cal_map = {}
            for L in eval_lines:
                key = str(L)
                cal = calibration_payload.get(key)
                if cal:
                    gx = np.array(cal["grid_x"], dtype=float)
                    gy = np.array(cal["grid_y"], dtype=float)
                    cal_map[L] = np.clip(np.interp(raw_map[L], gx, gy), 1e-6, 1-1e-6)
            if cal_map:
                metrics_holdout_cal = eval_lines_metrics(yh, cal_map)

    # Save artifacts
    save_artifacts(
        out_dir=args.out_dir,
        feature_key=args.feature_key,
        best=best,
        feature_hash=feature_hash,
        eval_lines=eval_lines,
        holdout_days=args.holdout_days,
        metrics_holdout_raw=metrics_holdout_raw,
        metrics_holdout_cal=metrics_holdout_cal,
        model_subartifact=subartifact,
        calibration_payload=calibration_payload,
    )

    print("✅ Completed. Model index and artifacts written.")
    print(f"🔗 {os.path.join(args.out_dir, 'MODEL_INDEX.json')}")

if __name__ == "__main__":
    main()
