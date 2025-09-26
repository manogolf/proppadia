# backend/scripts/modeling/train_models.py
import os, json, math, argparse, time
from pathlib import Path
from typing import List, Tuple
from .feature_sql import build_training_sql

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score
import joblib

# --------- Config ---------
DEFAULT_PROPS = [
    "doubles","earned_runs","hits","hits_allowed","hits_runs_rbis","home_runs",
    "outs_recorded","rbis","runs_rbis","runs_scored","singles","stolen_bases",
    "strikeouts_batting","strikeouts_pitching","total_bases","triples","walks","walks_allowed",
]

EXCLUDE_KEYS = {
    "player_id","team_id","game_id","game_date","prop_type","prop_source",
    "over_under","result","outcome","status","created_at","updated_at",
    # optional bookkeeping that might exist in your MVs:
    "rn"
}

# Sample weights by source (keep weighting in code)
SOURCE_WEIGHTS = {
    "user_added": 10.0,
    "mlb_api": 1.0,
}

OUT_DIR = Path(os.getenv("MODELS_DIR", "models_out")).resolve()
OUT_LATEST = OUT_DIR / "latest"
OUT_ARCHIVE = OUT_DIR / "archive"

# --------------------------------------------

def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    # SQLAlchemy URL tweak for Postgres if needed
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

def _mv_name(prop: str) -> str:
    return f"public.training_features_{prop}_enriched"

def _load_prop_df(engine, prop: str, days_back: int = None, limit: int = None) -> pd.DataFrame:
    """
    Load training rows for a prop. By default, use the dynamic SQL builder that
    joins MT + (PDS/BVP) on the fly and selects ONLY the features listed in
    feature_metadata.json for the model. Keep the y_over label logic exactly as before.

    To fall back to your old per-prop MVs, set TRAIN_DATA_MODE=mv.
    """
    mode = os.getenv("TRAIN_DATA_MODE", "dynamic").lower()

    if mode == "dynamic":
        # Choose one model’s feature set to define the columns (both LR/RF will use the same X)
        # You can flip to "logistic_regression" if you prefer that feature list instead.
        model_name = "random_forest"

        # Build the SELECT; it returns the columns named exactly as in feature_metadata.json,
        # plus: prop_value, result, status, over_under, game_date, prop_source, etc., and y_over.
        sql = build_training_sql(
            engine,
            prop_type=prop,
            model_name=model_name,
            days_back=days_back,
            limit=limit,
        )

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        # If the builder didn’t compute the label, compute it here (kept identical to your logic)
        if "y_over" not in df.columns:
            df["y_over"] = np.where(df["result"] > df["prop_value"], 1,
                             np.where(df["result"] < df["prop_value"], 0, np.nan))

        # drop pushes/NaN labels, match your original behavior
        df = df[~df["y_over"].isna()].copy()
        df["y_over"] = df["y_over"].astype(int)
        return df

    # ---------- fallback to your existing MV path (unchanged) ----------
    base_sql = f"""
        SELECT *,
               CASE
                 WHEN result > prop_value THEN 1
                 WHEN result < prop_value THEN 0
                 ELSE NULL
               END AS y_over
        FROM { _mv_name(prop) }
        WHERE (status IS NULL OR status IN ('resolved','final','settled'))
    """
    clauses = []
    params = {}
    if days_back is not None and days_back > 0:
        clauses.append("game_date >= CURRENT_DATE - INTERVAL ':days days'")
        params["days"] = days_back
    if clauses:
        base_sql += " AND " + " AND ".join(clauses)
    if limit:
        base_sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        df = pd.read_sql(text(base_sql), conn, params=params)
    df = df[~df["y_over"].isna()].copy()
    df["y_over"] = df["y_over"].astype(int)
    return df

def _feature_columns(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    """
    Build raw feature list from columns present in the MV:
    - base: all cols except EXCLUDE_KEYS and the label y_over
    - add isna__<base> for every base feature (numeric & string allowed)
    - ensure categorical raw cols exist: streak_type, time_of_day_bucket
    Returns (raw_input_columns, num_cols_for_preproc, cat_cols_for_preproc).
    """
    cols = [c for c in df.columns if c not in EXCLUDE_KEYS and c != "y_over"]
    # Guarantee our two known categoricals are present (create empty if missing)
    if "streak_type" not in cols:
        df["streak_type"] = "none"
        cols.append("streak_type")
    if "time_of_day_bucket" not in cols:
        df["time_of_day_bucket"] = "unknown"
        cols.append("time_of_day_bucket")

    # Build input schema expected by predictor: base + isna__base + raw categoricals
    base = [c for c in cols if c not in ("streak_type","time_of_day_bucket")]
    input_columns = []
    input_columns.extend(base)
    input_columns.extend([f"isna__{c}" for c in base])
    input_columns.extend(["streak_type","time_of_day_bucket"])

    # Preproc split for sklearn (on the *raw* input columns)
    # Treat base (non-cats) as numeric; we’ll impute to 0 and scale.
    num_cols = base + [f"isna__{c}" for c in base]
    cat_cols = ["streak_type","time_of_day_bucket"]

    return input_columns, num_cols, cat_cols

def _materialize_training_matrix(df: pd.DataFrame, input_columns: List[str]) -> pd.DataFrame:
    """
    Produce the 2D table the pipelines expect at fit time.
    - Build isna__ columns
    - Keep raw categoricals
    - Coerce numeric to float where possible, else 0.0
    """
    X = pd.DataFrame(index=df.index)
    # raw categoricals pass through
    for col in ("streak_type", "time_of_day_bucket"):
        if col in df.columns:
            X[col] = df[col].astype(str)
        else:
            X[col] = "none" if col == "streak_type" else "unknown"

    # base features = everything except cats & excluded & label
    base = [c for c in input_columns if not c.startswith("isna__") and c not in ("streak_type","time_of_day_bucket")]
    for col in base:
        v = df[col] if col in df.columns else 0.0
        X[col] = pd.to_numeric(v, errors="coerce").fillna(0.0).astype(float)

    # missingness flags
    for col in base:
        isna = df[col].isna() if col in df.columns else pd.Series(True, index=df.index)
        # also treat empty string as missing for strings
        if col in df.columns and df[col].dtype == object:
            isna = isna | (df[col].astype(str).str.len() == 0)
        X[f"isna__{col}"] = isna.astype(float)

    # order the columns exactly
    X = X[[c for c in input_columns]]
    return X

def _sample_weights(df: pd.DataFrame) -> np.ndarray:
    src = df.get("prop_source")
    if src is None:
        return np.ones(len(df), dtype=float)
    return src.map(lambda s: SOURCE_WEIGHTS.get(str(s), 1.0)).to_numpy(dtype=float)

def _split_by_time(df: pd.DataFrame, test_frac: float = 0.2) -> Tuple[np.ndarray,np.ndarray]:
    # time-aware split if game_date exists; else stratified random
    if "game_date" in df.columns:
        df2 = df.sort_values("game_date").reset_index(drop=True)
        n = len(df2)
        cut = max(1, int(n * (1.0 - test_frac)))
        idx_train = df2.index[:cut].to_numpy()
        idx_val   = df2.index[cut:].to_numpy()
        return idx_train, idx_val
    else:
        idx = np.arange(len(df))
        np.random.shuffle(idx)
        cut = max(1, int(len(df) * (1.0 - test_frac)))
        return idx[:cut], idx[cut:]

def _fit_models(X, y, num_cols, cat_cols, w):
    num_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler(with_mean=False)),
    ])

    # sklearn >= 1.2 uses `sparse_output`; older uses `sparse`
    try:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse=False)

    cat_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
        ("ohe", ohe),
    ])

    pre = ColumnTransformer([
        ("num", num_pipe, num_cols),
        ("cat", cat_pipe, cat_cols),
    ], remainder="drop")  # you can omit sparse_threshold now

    lr = Pipeline([
        ("pre", pre),
        ("clf", LogisticRegression(max_iter=2000)),
    ])
    rf = Pipeline([
        ("pre", pre),
        ("clf", RandomForestClassifier(
            n_estimators=600, max_depth=None, random_state=42,
            class_weight="balanced_subsample", n_jobs=-1)),
    ])

    lr.fit(X, y, clf__sample_weight=w)
    rf.fit(X, y, clf__sample_weight=w)
    return lr, rf

def _evaluate(models, X_val, y_val) -> dict:
    out = {}
    for name, m in models.items():
        try:
            proba = m.predict_proba(X_val)[:,1]
            out[f"{name}_auc"] = float(roc_auc_score(y_val, proba))
        except Exception:
            out[f"{name}_auc"] = None
    return out

def _save_artifact(prop: str, lr, rf, input_columns: List[str], meta_extra: dict):
    OUT_LATEST.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)

    artifact = {
        "best": None,   # (optional) keep for future best-of logic
        "lr": lr,
        "rf": rf,
        "meta": {
            "prop_type": prop,
            "input_columns": list(input_columns),
            **meta_extra
        }
    }
    # latest
    joblib.dump(artifact, OUT_LATEST / f"{prop}.joblib", compress=3)
    # archive by timestamp
    ts = time.strftime("%Y%m%d-%H%M%S")
    (OUT_ARCHIVE / prop).mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, OUT_ARCHIVE / prop / f"{prop}-{ts}.joblib", compress=3)

def _update_model_index(prop: str, meta: dict):
    idx_path = OUT_LATEST / "MODEL_INDEX.json"
    if idx_path.exists():
        data = json.loads(idx_path.read_text())
    else:
        data = {}
    data[prop] = meta
    idx_path.write_text(json.dumps(data, indent=2))

def retrain_prop(engine, prop: str, days_back: int = None, limit: int = None) -> dict:
    df = _load_prop_df(engine, prop, days_back=days_back, limit=limit)
    if df.empty:
        raise RuntimeError(f"No rows for prop '{prop}' after filtering")

    input_columns, num_cols, cat_cols = _feature_columns(df)
    X = _materialize_training_matrix(df, input_columns)
    y = df["y_over"].to_numpy(dtype=int)
    w = _sample_weights(df)

    idx_tr, idx_va = _split_by_time(df)
    X_tr, X_va = X.iloc[idx_tr], X.iloc[idx_va]
    y_tr, y_va = y[idx_tr], y[idx_va]
    w_tr = w[idx_tr]

    lr, rf = _fit_models(X_tr, y_tr, num_cols, cat_cols, w_tr)
    metrics = _evaluate({"lr": lr, "rf": rf}, X_va, y_va)

    # save
    meta = {
        "val_metrics": metrics,
        "n_train": int(len(X_tr)),
        "n_val": int(len(X_va)),
        "features_num": [c for c in input_columns if c.startswith("isna__") or (c not in ("streak_type","time_of_day_bucket"))],
        "features_cat": ["streak_type","time_of_day_bucket"],
    }
    _save_artifact(prop, lr, rf, input_columns, meta)
    _update_model_index(prop, meta)
    return meta

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--props", help="comma-separated list, or 'all'", default="all")
    ap.add_argument("--days-back", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    props = DEFAULT_PROPS if args.props == "all" else [p.strip() for p in args.props.split(",") if p.strip()]
    engine = create_engine(_db_url())

    OUT_LATEST.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)

    summary = {}
    for p in props:
        print(f"=== Retraining {p} ===")
        try:
            meta = retrain_prop(engine, p, days_back=args.days_back, limit=args.limit)
            print(f"✅ {p}: {meta['val_metrics']}")
            summary[p] = {"ok": True, **meta}
        except Exception as e:
            print(f"❌ {p}: {e}")
            summary[p] = {"ok": False, "error": str(e)}

    (OUT_LATEST / "TRAIN_SUMMARY.json").write_text(json.dumps(summary, indent=2))
    print("Done. Wrote:", OUT_LATEST)

if __name__ == "__main__":
    main()
