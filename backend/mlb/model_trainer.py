# backend/scripts/model_trainer.py
"""
Train and save per-prop models (LogReg + RandomForest) to local filesystem.

- Primary source: training_examples_v1 (if exists)
- Fallback: model_training_props + merge player_derived_stats for requested features
- Target: outcome ('win'→1, 'loss'→0)
- Saves models to: $MODELS_DIR/{latest,archive} (default /var/data/proppadia/models)
- Embeds exact feature lists used into joblib meta (features_num/features_cat)

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY  (or SUPABASE_ANON_KEY for read-only)
  MODELS_DIR (optional, default /var/data/proppadia/models)
"""
from __future__ import annotations


import os, io, json
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import joblib

from supabase import create_client, Client
from backend.shared.db.pg import pg_fetchall

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score
from pandas.api.types import is_numeric_dtype

# ---- .env (optional) ---------------------------------------------------------
try:
    from dotenv import load_dotenv
    for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if p.exists():
            load_dotenv(p, override=False)
except Exception:
    pass

# ---- Config ------------------------------------------------------------------
DEFAULT_DAYS_BACK = 365
DEFAULT_ROW_LIMIT = 50_000

PROP_TYPES = [
    "doubles","earned_runs","hits","hits_allowed","hits_runs_rbis","home_runs",
    "outs_recorded","rbis","runs_rbis","runs_scored","singles","stolen_bases",
    "strikeouts_batting","strikeouts_pitching","total_bases","triples","walks",
    "walks_allowed",
]

# Props that are pitcher-centric (used to drop d7_* windows)
PITCHING_PROPS = {
    "hits_allowed",
    "earned_runs",
    "walks_allowed",
    "strikeouts_pitching",
    "outs_recorded",
}


MODELS_DIR  = Path(os.environ.get("MODELS_DIR", "/var/data/proppadia/models")).resolve()
LATEST_DIR  = MODELS_DIR / "latest"
ARCHIVE_DIR = MODELS_DIR / "archive"

# Feature spec JSON (same sources your registry uses)
FEATURE_JSON_CANDIDATES = [
    Path(os.environ["FEATURE_JSON"]) if os.getenv("FEATURE_JSON") else None,
    Path(__file__).resolve().parents[2] / "backend" / "mlb" / "modeling" / "feature_metadata.json",
    Path(__file__).resolve().parents[2] / "backend" / "mlb" / "modeling" / "feature_metadata_backup.json",
    Path(__file__).resolve().parents[2] / "backend" / "scripts" / "modeling" / "feature_metadata.json",
    Path(__file__).resolve().parents[2] / "backend" / "scripts" / "modeling" / "feature_metadata_backup.json",
]
FEATURE_JSON_CANDIDATES = [p for p in FEATURE_JSON_CANDIDATES if p]

# OneHotEncoder kw compat
try:
    _ = OneHotEncoder(sparse_output=True, handle_unknown="ignore")
    _ONEHOT_KW = dict(sparse_output=True, handle_unknown="ignore")
except TypeError:
    _ONEHOT_KW = dict(sparse=True, handle_unknown="ignore")

def _debug_feature_paths():
    print("Feature JSON search paths:")
    for p in FEATURE_JSON_CANDIDATES:
        print(" -", p, "✓" if p.exists() else "✗")

# before first use of load_feature_spec():
_debug_feature_paths()

# Single source of truth for the training view (public schema via PostgREST)
# Training data source selection:
# - reconcile_csv (default): market+outcome rows CSV + player_derived_stats join
# - base_merge: model_training_props + player_derived_stats join
# - view: explicit FEATURE_VIEW relation via Supabase table API
TRAIN_FEATURE_SOURCE = str(os.environ.get("TRAIN_FEATURE_SOURCE", "reconcile_csv")).strip().lower()
FEATURE_VIEW = os.environ.get("FEATURE_VIEW", "").strip()
RECONCILE_ROWS_CSV = str(
    os.environ.get("MLB_TRAIN_RECONCILE_ROWS_CSV")
    or os.environ.get("MLB_RECONCILE_ROWS_OUT_CSV")
    or (Path(__file__).resolve().parents[2] / "tmp" / "mlb_base_vs_market_rows.csv")
)
RECONCILE_REQUIRE_TWO_SIDED = str(os.environ.get("MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
RECONCILE_ALLOW_MISSING_PRICE_PROPS = {
    p.strip().lower()
    for p in str(os.environ.get("MLB_TRAIN_RECONCILE_ALLOW_MISSING_PRICE_PROPS", "runs_rbis")).split(",")
    if p.strip()
}
RECONCILE_FALLBACK_BASE_MERGE = str(
    os.environ.get("MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

# ---- Training thresholds (class balance) -------------------------------------
MIN_CLASS_COUNT = int(os.getenv("MIN_CLASS_COUNT", "100"))
MIN_MINORITY_PCT = float(os.getenv("MIN_MINORITY_PCT", "0.10"))
try:
    import json as _json
    MIN_CLASS_COUNT_BY_PROP = _json.loads(os.getenv("MIN_CLASS_COUNT_BY_PROP", "{}"))
    MIN_MINORITY_PCT_BY_PROP = _json.loads(os.getenv("MIN_MINORITY_PCT_BY_PROP", "{}"))
except Exception:
    MIN_CLASS_COUNT_BY_PROP = {}
    MIN_MINORITY_PCT_BY_PROP = {}


# ---- Utilities ---------------------------------------------------------------
def _atomic_write_bytes(path: Path, blob: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(blob)
    os.replace(tmp, path)

def _supabase_client() -> Optional[Client]:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

def _load_feature_spec() -> Dict[str, Any]:
    for p in FEATURE_JSON_CANDIDATES:
        try:
            if p and p.exists():
                return json.loads(p.read_text())
        except Exception:
            continue
    return {}

def _chunked(xs: List[Any], n: int) -> List[List[Any]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def _pg_data(resp):
    """Normalize PostgREST response to a list of rows across supabase-py versions."""
    # supabase-py v2 returns object with .data
    if hasattr(resp, "data"):
        return resp.data or []
    # some versions return dict
    if isinstance(resp, dict):
        return resp.get("data", []) or []
    # some return list directly
    if isinstance(resp, list):
        return resp
    return []


# ---- Data access -------------------------------------------------------------
def _fetch_from_view(sb: Optional[Client], prop_type: str, days_back: int, limit: int, cols: List[str]) -> Optional[pd.DataFrame]:
    """Use consolidated feature view/table (joined, de-duped, backfills applied)."""
    if not FEATURE_VIEW or sb is None:
        return None
    since_date = (datetime.utcnow() - timedelta(days=days_back)).date().isoformat()
    try:
        q = (
            sb.table(FEATURE_VIEW)
              .select("*")                      # tolerate per-prop columns
              .eq("prop_type", prop_type)
              .gte("game_date", since_date)
        )
        # prefer range for broader client compat
        if hasattr(q, "range") and isinstance(limit, int):
            q = q.range(0, max(0, limit - 1))
        else:
            q = q.limit(limit)
        resp = q.execute()                      # ← ensure we actually execute
        rows = _pg_data(resp)
        print(f"[trainer] source=view:{FEATURE_VIEW} prop={prop_type} rows={len(rows)}")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[trainer] view fetch failed ({FEATURE_VIEW}) for {prop_type}: {e}")
        return None


def _fetch_base_rows_pg(prop_type: str, since_date: str, limit: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT *
    FROM mlb.model_training_props
    WHERE lower(trim(prop_type)) = lower(trim(%s))
      AND line IS NOT NULL
      AND prop_value IS NOT NULL
      AND game_date::date >= %s::date
      AND lower(trim(coalesce(outcome, ''))) IN ('win', 'loss')
    ORDER BY game_date DESC
    LIMIT %s
    """
    return pg_fetchall(sql, (prop_type, since_date, int(limit)))


def _fetch_base_and_merge(sb: Optional[Client], prop_type: str, days_back: int, limit: int, feat_cols: List[str]) -> pd.DataFrame:
    """Fallback: model_training_props + join derived features by (player_id, game_id)."""
    since_date = (datetime.utcnow() - timedelta(days=days_back)).date().isoformat()
    if sb is not None:
        resp = (
            sb.schema("mlb").table("model_training_props")
              .select("*")
              .eq("prop_type", prop_type)
              .not_.is_("line", "null")
              .not_.is_("prop_value", "null")
              .gte("game_date", since_date)
              .order("game_date", desc=True)
              .limit(limit)
              .execute()
        )
        rows = resp.data or []
        rows = [r for r in rows if r.get("outcome") in ("win", "loss")]
    else:
        rows = _fetch_base_rows_pg(prop_type, since_date, limit)

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"[trainer] source=fallback:base empty prop={prop_type}")
        return df

    df = _add_time_features(df)
    df = _merge_derived_features(sb, df, feat_cols)
    print(f"[trainer] source=fallback:base+merge prop={prop_type} rows={len(df)}")
    return df


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "game_date" not in out.columns:
        return out
    try:
        dt = pd.to_datetime(out["game_date"])
    except Exception:
        dt = pd.to_datetime(out["game_date"], errors="coerce")
    hour = getattr(dt.dt, "hour", pd.Series([None] * len(out)))
    bucket = np.where(hour < 12, "morning", np.where(hour < 18, "afternoon", "night"))
    dow = dt.dt.day_name().str[:3]
    out["time_of_day_bucket"] = bucket
    out["game_day_of_week"] = dow
    return out


def _merge_derived_features(sb: Optional[Client], df: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for k in ("player_id", "game_id"):
        if k in out.columns:
            out[k] = pd.to_numeric(out[k], errors="coerce")

    # merge in derived features for the exact games we have
    pairs = out[["player_id", "game_id"]].dropna().drop_duplicates() if {"player_id", "game_id"}.issubset(out.columns) else pd.DataFrame()
    game_ids = pairs["game_id"].astype(str).tolist()

    derived_frames: List[pd.DataFrame] = []
    for chunk in _chunked(game_ids, 1000):
        if sb is not None:
            r = (
                sb.schema("mlb").table("player_derived_stats")
                .select("*")
                .in_("game_id", chunk)
                .execute()
            )
            part = _pg_data(r)
        else:
            placeholders = ",".join(["%s"] * len(chunk))
            sql = f"SELECT * FROM mlb.player_derived_stats WHERE game_id IN ({placeholders})"
            part = pg_fetchall(sql, tuple(chunk))
        if part:
            derived_frames.append(pd.DataFrame(part))
    if derived_frames:
        derived = pd.concat(derived_frames, ignore_index=True)
    else:
        derived = pd.DataFrame(columns=["player_id","game_id"])

    # coerce keys on derived as well
    for k in ("player_id", "game_id"):
        if k in derived.columns:
            derived[k] = pd.to_numeric(derived[k], errors="coerce")

    if {"player_id", "game_id"}.issubset(out.columns):
        out = out.merge(derived, on=["player_id", "game_id"], how="left", suffixes=("", "_der"))

    # ensure all requested features exist
    for f in feat_cols:
        if f not in out.columns:
            out[f] = np.nan
    return out


def _fetch_reconcile_and_merge(sb: Optional[Client], prop_type: str, days_back: int, limit: int, feat_cols: List[str]) -> pd.DataFrame:
    """
    Preferred source: row-level reconcile CSV (market lines + realized outcomes),
    then merge player_derived_stats for model features.
    """
    path = Path(RECONCILE_ROWS_CSV).expanduser()
    if not path.exists():
        print(f"[trainer] source=reconcile_csv missing file: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[trainer] source=reconcile_csv read failed ({path}): {e}")
        return pd.DataFrame()
    if df.empty:
        print(f"[trainer] source=reconcile_csv empty file: {path}")
        return df

    since_date = pd.Timestamp((datetime.utcnow() - timedelta(days=days_back)).date())
    out = df.copy()
    prop_series = out["prop_type"] if "prop_type" in out.columns else pd.Series([""] * len(out), index=out.index)
    out["prop_type"] = prop_series.astype(str).str.strip().str.lower()
    out = out[out["prop_type"] == str(prop_type).strip().lower()]
    if out.empty:
        print(f"[trainer] source=reconcile_csv no rows for prop={prop_type}")
        return out

    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
        out = out[out["game_date"] >= since_date]

    out["line"] = pd.to_numeric(out.get("line"), errors="coerce")
    out = out[out["line"].notna()]

    if RECONCILE_REQUIRE_TWO_SIDED and str(prop_type).strip().lower() not in RECONCILE_ALLOW_MISSING_PRICE_PROPS:
        px_o = pd.to_numeric(out.get("price_over_american"), errors="coerce")
        px_u = pd.to_numeric(out.get("price_under_american"), errors="coerce")
        out = out[px_o.notna() & px_u.notna()]

    over_outcome_series = (
        out["actual_over_outcome"]
        if "actual_over_outcome" in out.columns
        else pd.Series([""] * len(out), index=out.index)
    )
    over_outcome = over_outcome_series.astype(str).str.strip().str.lower()
    out = out[over_outcome.isin({"win", "loss"})].copy()
    if out.empty:
        print(f"[trainer] source=reconcile_csv no labeled rows for prop={prop_type}")
        return out
    out["y"] = (over_outcome == "win").astype("Int64")

    # Keep compatibility with legacy preprocessing fallbacks.
    out["prop_value"] = out["line"]

    # De-duplicate repeated records for same player/game/line.
    dedupe_cols = [c for c in ("player_id", "game_id", "line") if c in out.columns]
    if dedupe_cols:
        out = out.drop_duplicates(subset=dedupe_cols, keep="first")

    if "game_date" in out.columns:
        out = out.sort_values("game_date", ascending=False, na_position="last")
    if int(limit) > 0:
        out = out.head(int(limit))

    out = _add_time_features(out)
    out = _merge_derived_features(sb, out, feat_cols)
    print(f"[trainer] source=reconcile_csv prop={prop_type} rows={len(out)} file={path}")
    return out


def fetch_training_rows(sb: Optional[Client], prop_type: str, days_back: int, limit: int, feat_cols: List[str]) -> pd.DataFrame:
    if TRAIN_FEATURE_SOURCE in {"reconcile", "reconcile_csv"}:
        df = _fetch_reconcile_and_merge(sb, prop_type, days_back, limit, feat_cols)
        if not df.empty:
            return df
        if not RECONCILE_FALLBACK_BASE_MERGE:
            print(f"[trainer] source=reconcile_csv strict mode; no fallback for prop={prop_type}")
            return df
        print(f"[trainer] source=reconcile_csv fallback->base_merge prop={prop_type}")
    if TRAIN_FEATURE_SOURCE == "view":
        df = _fetch_from_view(sb, prop_type, days_back, limit, feat_cols)
        if df is not None:
            return df
    return _fetch_base_and_merge(sb, prop_type, days_back, limit, feat_cols)


# ---- Preprocessing / pipelines ----------------------------------------------
def _prep_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = df.copy()

    # --- choose label source (strict) ---
    # Training target must be "actual over side" (1=over, 0=under), not generic win/loss.
    label_source = None
    if "y" in df.columns:
        y = pd.to_numeric(df["y"], errors="coerce")
        y = y.where(y.isin([0, 1]))
        df["y"] = y.astype("Int64")
        label_source = "provided(y)"
    elif {"over_under", "outcome"}.issubset(df.columns):
        ou = df["over_under"].astype(str).str.strip().str.lower()
        oc = df["outcome"].astype(str).str.strip().str.lower()
        y = pd.Series([pd.NA] * len(df), dtype="Int64")
        # If the pick was OVER and it won, actual side was over.
        y[(ou == "over") & (oc == "win")] = 1
        y[(ou == "over") & (oc == "loss")] = 0
        # If the pick was UNDER and it won, actual side was under.
        y[(ou == "under") & (oc == "win")] = 0
        y[(ou == "under") & (oc == "loss")] = 1
        df["y"] = y
        label_source = "derived(over_under+outcome)"
    elif {"result", "prop_value"}.issubset(df.columns):
        # derive: OVER wins if actual result > line
        r = pd.to_numeric(df["result"], errors="coerce")
        pv = pd.to_numeric(df["prop_value"], errors="coerce")
        df["y"] = (r > pv).astype("Int64")
        label_source = "derived(result>prop_value)"
    elif "status" in df.columns and df["status"].isin(["win", "loss"]).any():
        df["y"] = (df["status"] == "win").astype(int)
        label_source = "status"
    else:
        df["y"] = pd.Series([pd.NA] * len(df), dtype="Int64")
        label_source = "none"

    # drop unlabeled
    df = df[df["y"].notna()].copy()
    df["y"] = df["y"].astype(int)

    # log the label source early
    try:
        cnt = df["y"].value_counts().to_dict()
        print(f"[trainer] label_source={label_source} y_counts={cnt}")
    except Exception:
        pass

    # coerce binary flags
    for col in ("is_home","is_pitcher"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # sample weights: uniform (no source-based overweighting)
    w = np.ones(len(df), dtype="float64")
    df["sample_weight"] = w
    return df


def build_pipeline(num_cols: List[str], cat_cols: List[str]):
    num_transform = Pipeline([("imputer", SimpleImputer(strategy="median"))])
    cat_transform = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(**_ONEHOT_KW)),
    ])
    pre = ColumnTransformer(
        transformers=[
            ("num", num_transform, num_cols),
            ("cat", cat_transform, cat_cols),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )
    lr = LogisticRegression(max_iter=1000)
    rf = RandomForestClassifier(n_estimators=300, max_depth=None, n_jobs=-1, random_state=42, 
    class_weight="balanced"
    )
    lr_cal = CalibratedClassifierCV(lr, method="isotonic", cv=3)
    pipe_lr = Pipeline([("pre", pre), ("clf", lr_cal)])
    pipe_rf = Pipeline([("pre", pre), ("clf", rf)])
    return pipe_lr, pipe_rf


# ---- Trainer -----------------------------------------------------------------
def train_models_for_prop(prop_type: str, *, days_back=DEFAULT_DAYS_BACK, limit=DEFAULT_ROW_LIMIT, quiet=True):
    sb = _supabase_client()
    if sb is None and not quiet:
        print("[trainer] SUPABASE_URL/key not set; using DATABASE_URL/SUPABASE_DB_URL via direct Postgres fallback.")

    # 1) expected features from repo JSON (same universe as prediction)
    spec_all = _load_feature_spec()
    spec = spec_all.get(prop_type) or {}
    feat_list: List[str] = (
        spec.get("random_forest")
        or spec.get("rf")
        or spec.get("logistic_regression")
        or spec.get("lr")
        or spec.get("features")
        or []
    )
    if not feat_list:
        if not quiet:
            print(f"⏭️  {prop_type}: no feature list in feature_metadata.json; skipping.")
        return None

    # 2) fetch rows (view or fallback merge)
    df = fetch_training_rows(sb, prop_type, days_back, limit, feat_list)
    if df.empty:
        if not quiet:
            print(f"⏭️  {prop_type}: no training rows.")
        return None

    # 3) prep labels/weights
    df = _prep_frame(df)
    if df.empty or df["y"].nunique() < 2:
        if not quiet:
            print(f"⏭️  {prop_type}: target has a single class or no labeled rows; skipping.")
        return None

    # ⬇️ Insert the class-balance guard here
    threshold = int(MIN_CLASS_COUNT_BY_PROP.get(prop_type, MIN_CLASS_COUNT))
    minority_threshold = float(MIN_MINORITY_PCT_BY_PROP.get(prop_type, MIN_MINORITY_PCT))
    pos = int((df["y"] == 1).sum())
    neg = int((df["y"] == 0).sum())
    total = max(1, pos + neg)
    minority_rate = min(pos, neg) / float(total)
    if pos < threshold or neg < threshold:
        if not quiet:
            print(f"⏭️  {prop_type}: too few positives/negatives "
                f"(pos={pos}, neg={neg}, threshold={threshold}); skipping.")
        return None
    if minority_rate < minority_threshold:
        if not quiet:
            print(
                f"⏭️  {prop_type}: label imbalance too high "
                f"(pos={pos}, neg={neg}, minority_rate={minority_rate:.3f}, "
                f"min_minority_pct={minority_threshold:.3f}); skipping."
            )
        return None

    # --- Feature availability policy + pitching-specific trim ---
    # 1) Drop only truly all-NaN features present in the frame
    all_nan = [c for c in set(feat_list) if c in df.columns and df[c].isna().all()]
    if all_nan and not quiet:
        print(f"ℹ️  {prop_type}: dropping all-NaN features: {sorted(all_nan)}")
    feat_list = [c for c in feat_list if c not in all_nan]

    # 2) For pitching props, drop d7* windows (rotation → weak coverage/signal)
    if prop_type in PITCHING_PROPS:
        drop_d7 = [c for c in feat_list if c.startswith("d7_") or c == "rolling_result_avg_7"]
        if drop_d7 and not quiet:
            print(f"ℹ️  {prop_type}: dropping pitcher d7-window features: {sorted(drop_d7)}")
        feat_list = [c for c in feat_list if c not in drop_d7]

    # (Informational) coverage log only — do NOT drop low-coverage columns
    if not quiet:
        cov = []
        for c in feat_list:
            if c in df.columns:
                cov.append((c, float(df[c].notna().mean())))
        cov.sort(key=lambda x: x[1])
        low = [f"{c}:{int(p*100)}%" for c,p in cov if p < 0.60]
        if low:
            print("ℹ️  {0}: low-coverage kept → {1}".format(prop_type, ", ".join(low[:12]) + (" ..." if len(low)>12 else "")))

    # 3) determine num/cat AFTER pruning
    ALWAYS_CAT = {"time_of_day_bucket","game_day_of_week"}
    num_used = [c for c in feat_list if c in df.columns and (is_numeric_dtype(df[c]) and c not in ALWAYS_CAT)]
    cat_used = [c for c in feat_list if c in df.columns and (not is_numeric_dtype(df[c]) or c in ALWAYS_CAT)]

    # 4) add missingness indicators for numeric features that have NaNs
    miss_inds = []
    for c in num_used:
        if df[c].isna().any():
            mcol = f"isna__{c}"
            df[mcol] = df[c].isna().astype(int)
            miss_inds.append(mcol)
    num_used = num_used + miss_inds

    cols_used = num_used + cat_used
    if not cols_used:
        if not quiet:
            print(f"⏭️  {prop_type}: no usable features after pruning; skipping.")
        return None

    # Coverage hint
    expected = set(feat_list)
    used = set(cols_used)
    if not quiet and expected:
        cov = len(used & expected) / max(1, len(expected))
        if cov < 0.6:
            print(f"⚠️  {prop_type}: feature coverage {cov:.0%} ({len(used & expected)}/{len(expected)})")

    # 5) stratified split (ensures both classes in val)
    # --- time-based holdout first, with stratified fallback if needed ---
    if "game_date" in df.columns:
        df = df.sort_values("game_date")
    else:
        df = df.sort_values("game_id")

    split = int(len(df) * 0.8)
    train_df, val_df = df.iloc[:split], df.iloc[split:]

    # ensure both classes exist in val; otherwise fallback to stratified
    if train_df["y"].nunique() < 2 or val_df["y"].nunique() < 2:
        from sklearn.model_selection import StratifiedShuffleSplit
        sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        (train_idx, val_idx), = sss.split(df[cols_used], df["y"])
        train_df, val_df = df.iloc[train_idx], df.iloc[val_idx]

    X_tr, y_tr, w_tr = train_df[cols_used], train_df["y"], train_df["sample_weight"]
    X_v,  y_v,  w_v  =  val_df[cols_used],  val_df["y"],  val_df["sample_weight"]

    # 6) build pipelines (this DEFINES pipe_lr / pipe_rf) and fit
    pipe_lr, pipe_rf = build_pipeline(num_used, cat_used)

    pipe_lr.fit(X_tr, y_tr, clf__sample_weight=w_tr)
    pipe_rf.fit(X_tr, y_tr, clf__sample_weight=w_tr)

    # 7) AUC — report both unweighted and weighted, then use weighted for selection/meta
    proba_lr = pipe_lr.predict_proba(X_v)[:, 1]
    proba_rf = pipe_rf.predict_proba(X_v)[:, 1]
    pos_rate = float(np.mean(y_v))

    def safe_auc(y, p, w=None):
        try:
            return roc_auc_score(y, p) if w is None else roc_auc_score(y, p, sample_weight=w)
        except Exception:
            return np.nan

    auc_lr_uw = safe_auc(y_v, proba_lr, w=None)
    auc_lr_w  = safe_auc(y_v, proba_lr, w_v)
    auc_rf_uw = safe_auc(y_v, proba_rf, w=None)
    auc_rf_w  = safe_auc(y_v, proba_rf, w_v)

    # keep weighted versions as the canonical ones for model selection & metadata
    auc_lr = auc_lr_w
    auc_rf = auc_rf_w

    def _w_from_auc(v: float) -> float:
        try:
            if np.isnan(v):
                return 0.0
            return max(float(v) - 0.5, 0.0)
        except Exception:
            return 0.0

    w_lr = _w_from_auc(auc_lr)
    w_rf = _w_from_auc(auc_rf)
    if (w_lr + w_rf) > 0:
        blend_val = ((proba_lr * w_lr) + (proba_rf * w_rf)) / (w_lr + w_rf)
    else:
        blend_val = (proba_lr + proba_rf) / 2.0

    # Tune per-prop decision threshold on validation to avoid one-size 0.5 cutoff.
    # Weighted accuracy is used to align with training-source weighting.
    yv = y_v.to_numpy(dtype=int)
    wv = np.asarray(w_v, dtype=float)
    if wv.size != len(yv):
        wv = np.ones(len(yv), dtype=float)
    thresholds = [round(x, 2) for x in np.arange(0.35, 0.66, 0.01)]
    best_thr = 0.5
    best_score = -1.0
    for thr in thresholds:
        pred = (blend_val >= thr).astype(int)
        denom = float(wv.sum()) if float(wv.sum()) > 0 else float(len(yv))
        score = float(((pred == yv).astype(float) * wv).sum()) / max(1e-9, denom)
        if score > best_score or (abs(score - best_score) < 1e-12 and abs(thr - 0.5) < abs(best_thr - 0.5)):
            best_score = score
            best_thr = thr

    if not quiet:
        fmt = lambda x: "NaN" if np.isnan(x) else f"{x:.3f}"
        print(
            f"📈 {prop_type}  AUC — "
            f"LR: {fmt(auc_lr_uw)} (uw) / {fmt(auc_lr_w)} (w);  "
            f"RF: {fmt(auc_rf_uw)} (uw) / {fmt(auc_rf_w)} (w);  "
            f"pos_rate={pos_rate:.3f}, n_val={len(y_v)}, decision_threshold={best_thr:.2f}, val_wacc={best_score:.3f}"
        )

    best_model = pipe_rf if (auc_rf >= (auc_lr if not np.isnan(auc_lr) else -1)) else pipe_lr

    # 6) serialize with exact lists we used
    payload = {
        "best": best_model,
        "lr": pipe_lr,
        "rf": pipe_rf,
        "meta": {
            "prop_type": prop_type,
            "trained_at": datetime.utcnow().isoformat(),
            "days_back": days_back,
            "limit": limit,
            "auc_lr": float(auc_lr) if not np.isnan(auc_lr) else None,
            "auc_rf": float(auc_rf) if not np.isnan(auc_rf) else None,
            "decision_threshold": float(best_thr),
            "val_weighted_accuracy": float(best_score),
            "features_num": num_used,
            "features_cat": cat_used,
        },
    }
    buf = io.BytesIO()
    joblib.dump(payload, buf, compress=3)
    model_bytes = buf.getvalue()

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    latest_path  = (LATEST_DIR / f"{prop_type}.joblib").resolve()
    archive_path = (ARCHIVE_DIR / prop_type / f"{prop_type}-{ts}.joblib").resolve()

    _atomic_write_bytes(latest_path, model_bytes)
    _atomic_write_bytes(archive_path, model_bytes)

    # 7) update MODEL_INDEX.json
    index_path = (LATEST_DIR / "MODEL_INDEX.json").resolve()
    index: Dict[str, Any] = {}
    if index_path.exists():
        try:
            index = json.loads(index_path.read_text())
            if not isinstance(index, dict):
                index = {}
        except Exception:
            index = {}
    index[prop_type] = {
        "prop_type": prop_type,
        "trained_at": datetime.utcnow().isoformat(),
        "file": latest_path.name,
        "auc_lr": None if np.isnan(auc_lr) else float(auc_lr),
        "auc_rf": None if np.isnan(auc_rf) else float(auc_rf),
        "decision_threshold": float(best_thr),
        "val_weighted_accuracy": float(best_score),
        "rows": int(len(df)),
        "features_num": num_used,
        "features_cat": cat_used,
    }
    _atomic_write_bytes(index_path, json.dumps(index, indent=2).encode("utf-8"))

    if not quiet:
        print(f"✅ {prop_type}: wrote latest → {latest_path}")
        print(f"📦 archived copy → {archive_path}")

    return {
        "prop_type": prop_type,
        "auc_lr": auc_lr,
        "auc_rf": auc_rf,
        "latest_path": str(latest_path),
        "archive_path": str(archive_path),
        "rows": int(len(df)),
    }


# ---- CLI ---------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--prop", help="Single prop type to train (default: all)", default=None)
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--limit", type=int, default=DEFAULT_ROW_LIMIT)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    props = [args.prop] if args.prop else PROP_TYPES
    results = []
    trained = skipped = 0

    for p in props:
        try:
            r = train_models_for_prop(p, days_back=args.days_back, limit=args.limit, quiet=args.quiet)
            if r:
                trained += 1
                results.append(r)
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            if not args.quiet:
                print(f"❌ {p}: {e}")

    print(json.dumps({"trained": trained, "skipped": skipped, "props": props, "results": results}, indent=2))
    sys.exit(0)
