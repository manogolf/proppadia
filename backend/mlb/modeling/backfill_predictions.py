# File: backend/scripts/modeling/backfill_predictions.py
"""
Backfills model predictions into the `model_training_props` table.

- Loads RF + LR models from persistent disk (MODEL_DIR)
- Builds feature vectors with build_feature_vector(...)
- Blends probabilities (RF+LR) to set predicted_outcome + confidence_score
- Updates only predicted_outcome, confidence_score, was_correct, prediction_timestamp
- Touches only mlb_api, resolved rows where predicted_outcome IS NULL

NOTE: No Supabase Storage calls. Disk-only models.
"""
from __future__ import annotations


import os
import sys
import json
import importlib
import importlib.util
from time import perf_counter
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
import traceback

import pandas as pd
import numpy as np
import joblib
from dotenv import load_dotenv
from supabase import create_client

# ──────────────────────────────────────────────────────────────────────────────
# Env & clients
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

MODEL_DIR = os.getenv("MODEL_DIR", "/var/data/models")
Path(MODEL_DIR).mkdir(parents=True, exist_ok=True)

BATCH_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "500"))
ENV_PROP_TYPES = os.getenv("PROP_TYPES")
ENV_PROP_TYPES = [p.strip() for p in ENV_PROP_TYPES.split(",")] if ENV_PROP_TYPES else None

# --- prediction guard knobs ---
OVERLAP_MIN  = int(os.getenv("BACKFILL_OVERLAP_MIN", "10"))     # min cols overlapping training schema
MIN_NNZ      = int(os.getenv("BACKFILL_MIN_NNZ", "3"))           # min nonzero cols after alignment
COIN_EPSILON = float(os.getenv("BACKFILL_COIN_EPSILON", "0.01")) # skip |p-0.5| < eps

# Map DB prop_type -> model folder/file prefix on disk
PROP_TYPE_ALIASES = {
    "rbis": "rbis",
    "rbi": "rbis",
    "runs": "runs_scored",
}

# ──────────────────────────────────────────────────────────────────────────────
# Pandas truthiness shim (avoid legacy 'if series:' errors)
# ──────────────────────────────────────────────────────────────────────────────
def _enable_pandas_truthiness_compat():
    try:
        from pandas.core.generic import NDFrame
        def _ndframe_bool(self):
            try:
                if getattr(self, "empty", False):
                    return False
                try:
                    a = self.any()
                except Exception:
                    a = getattr(self, "values", self).any()
                return bool(getattr(a, "item", lambda: a)())
            except Exception:
                return False
        NDFrame.__bool__ = _ndframe_bool  # type: ignore[attr-defined]
        NDFrame.__nonzero__ = _ndframe_bool  # type: ignore[attr-defined]
    except Exception as e:
        print(f"⚠️ Pandas truthiness compat patch failed: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Scalar normalization helpers
# ──────────────────────────────────────────────────────────────────────────────
def _scalarize(x):
    if isinstance(x, pd.Series):
        return _scalarize(x.iloc[0] if not x.empty else None)
    if isinstance(x, pd.DataFrame):
        return _scalarize(x.iloc[0, 0] if not x.empty else None)
    if isinstance(x, np.ndarray):
        return _scalarize(x.flat[0]) if x.size else None
    if isinstance(x, np.generic):
        return x.item()
    if isinstance(x, (list, tuple)) and len(x) == 1:
        return _scalarize(x[0])
    return x

def _normalize_row(row) -> dict:
    if hasattr(row, "to_dict"):
        row = row.to_dict()
    elif isinstance(row, pd.Series):
        row = row.to_dict()
    elif not isinstance(row, dict):
        row = dict(row)
    out = {}
    for k, v in row.items():
        if isinstance(v, dict):
            out[k] = {kk: _scalarize(vv) for kk, vv in v.items()}
        else:
            out[k] = _scalarize(v)
    for k in ("id","player_id","game_id","team","player_name","outcome","prop_type"):
        if k in out:
            out[k] = _scalarize(out[k])
    return out

def to_plain_scalars(d: dict) -> dict:
    s = pd.DataFrame([d]).iloc[0]
    out = {}
    for k, v in s.items():
        if isinstance(v, pd.Series):
            out[k] = v.iloc[0] if not v.empty else None
        elif isinstance(v, pd.DataFrame):
            out[k] = v.iloc[0, 0] if not v.empty else None
        elif isinstance(v, (list, tuple)) and len(v) == 1:
            out[k] = v[0]
        elif isinstance(v, np.generic):
            out[k] = v.item()
        else:
            out[k] = v
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Model I/O (disk-only) + cache
# ──────────────────────────────────────────────────────────────────────────────
def model_type_for(db_type: str) -> str:
    return PROP_TYPE_ALIASES.get(db_type, db_type)

def _model_filename(model_prop_type: str, kind: str) -> str:
    return f"{model_prop_type}_{'random_forest' if kind == 'rf' else 'logistic_regression'}.pkl"

def _model_path(model_prop_type: str, kind: str) -> str:
    folder = os.path.join(MODEL_DIR, model_prop_type)
    Path(folder).mkdir(parents=True, exist_ok=True)
    return os.path.join(folder, _model_filename(model_prop_type, kind))

def models_available(model_prop_type: str) -> tuple[bool, list[str]]:
    missing = []
    rf_path = _model_path(model_prop_type, "rf")
    lr_path = _model_path(model_prop_type, "lr")
    if not os.path.exists(rf_path): missing.append(rf_path)
    if not os.path.exists(lr_path): missing.append(lr_path)
    return (len(missing) == 0, missing)

_MODEL_CACHE: dict[str, tuple[object, object]] = {}

def load_models(model_prop_type: str):
    if model_prop_type in _MODEL_CACHE:
        return _MODEL_CACHE[model_prop_type]
    ok, missing = models_available(model_prop_type)
    if not ok:
        raise FileNotFoundError(f"Models missing for {model_prop_type}: {', '.join(missing)}")
    rf = joblib.load(_model_path(model_prop_type, "rf"))
    lr = joblib.load(_model_path(model_prop_type, "lr"))
    _MODEL_CACHE[model_prop_type] = (rf, lr)
    return rf, lr

# ──────────────────────────────────────────────────────────────────────────────
# Feature metadata (per-model schemas from /var/data/models/feature_metadata.json)
# ──────────────────────────────────────────────────────────────────────────────
_FEATURE_META: dict | None = None

def _load_feature_meta() -> dict:
    global _FEATURE_META
    if _FEATURE_META is not None:
        return _FEATURE_META
    path = os.path.join(MODEL_DIR, "feature_metadata.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"feature_metadata.json not found at {path}")
    with open(path, "r") as f:
        meta = json.load(f)
    if not isinstance(meta, dict) or not meta:
        raise ValueError("feature_metadata.json is empty or invalid")
    _FEATURE_META = meta
    return _FEATURE_META

def _expected_columns_pair_from_meta(prop_type: str) -> tuple[list[str], list[str]]:
    """
    Return (rf_cols, lr_cols). Supports:
      { "prop": ["..."] }
      { "prop": {"columns":[...] } }
      { "prop": {"random_forest":[...], "logistic_regression":[...] } }
      { "columns":[...] }  # global fallback
    """
    meta = _load_feature_meta()
    entry = meta.get(prop_type, meta.get("columns"))

    if isinstance(entry, list):
        return list(entry), list(entry)

    if isinstance(entry, dict):
        if isinstance(entry.get("columns"), list):
            cols = list(entry["columns"])
            return cols, cols
        rf = entry.get("random_forest")
        lr = entry.get("logistic_regression")
        if isinstance(rf, list) and isinstance(lr, list):
            return list(rf), list(lr)
        if isinstance(rf, list):
            return list(rf), list(rf)
        if isinstance(lr, list):
            return list(lr), list(lr)
        raise ValueError(f"No usable columns in feature_metadata.json for '{prop_type}'. Keys: {list(entry.keys())}")

    raise ValueError(f"No columns listed in feature_metadata.json for '{prop_type}'")

# ──────────────────────────────────────────────────────────────────────────────
# Numeric coercion / sparsity helpers
# ──────────────────────────────────────────────────────────────────────────────
def _coerce_numeric_fill(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == object:
            try: df[c] = pd.to_numeric(df[c], errors="coerce")
            except Exception: pass
    return df.replace([np.inf, -np.inf], 0).fillna(0)

def _nnz_cols(df: pd.DataFrame) -> int:
    if df.empty: return 0
    arr = np.nan_to_num(df.to_numpy(copy=False), nan=0.0, posinf=0.0, neginf=0.0)
    return int((arr != 0).any(axis=0).sum())

def _overlap(cols: list[str] | pd.Index, expected: list[str]) -> int:
    es = set(expected)
    return sum(1 for c in cols if c in es)

def _as_one_row_df(X):
    if isinstance(X, pd.DataFrame): return X
    if isinstance(X, pd.Series):    return X.to_frame().T
    if isinstance(X, dict):         return pd.DataFrame([X])
    if isinstance(X, np.ndarray):   return pd.DataFrame([X]) if X.ndim == 1 else pd.DataFrame(X)
    if X is None:                   return pd.DataFrame()
    return pd.DataFrame([X])

def _actual_label_from_row(row: dict):
    """
    Compute the true 'over'/'under'/'push' from resolved data.
    Prefers corrected_result, falls back to result; compares to line/prop_value.
    Returns 'over'|'under'|'push' or None if unavailable.
    """
    result = row.get("corrected_result", row.get("result"))
    line = row.get("line", row.get("prop_value", row.get("line_value")))
    try:
        r = None if result is None else float(result)
        l = None if line   is None else float(line)
    except Exception:
        return None
    if r is None or l is None: return None
    if r > l:  return "over"
    if r < l:  return "under"
    return "push"

# ──────────────────────────────────────────────────────────────────────────────
# Robust, lazy import of build_feature_vector
# ──────────────────────────────────────────────────────────────────────────────
def _load_build_feature_vector():
    """Return build_feature_vector(DataFrame) -> (X, y?) with resilient imports."""
    try:
        mod = importlib.import_module("backend.scripts.modeling.build_feature_vector")
        return getattr(mod, "build_feature_vector")
    except Exception:
        pass

    modeling_dir = Path(__file__).resolve().parent
    if str(modeling_dir) not in sys.path:
        sys.path.insert(0, str(modeling_dir))
    try:
        mod = importlib.import_module("build_feature_vector")
        return getattr(mod, "build_feature_vector")
    except Exception:
        pass

    bfv_path = modeling_dir / "build_feature_vector.py"
    if bfv_path.exists():
        spec = importlib.util.spec_from_file_location("build_feature_vector_fallback", bfv_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            sys.modules["build_feature_vector_fallback"] = mod
            spec.loader.exec_module(mod)  # type: ignore
            return getattr(mod, "build_feature_vector")

    raise ImportError("Could not import build_feature_vector; check local imports in that file.")

# ──────────────────────────────────────────────────────────────────────────────
# Prediction (blend RF + LR) — disk-only models
# ──────────────────────────────────────────────────────────────────────────────
_PRINTED_ALIGN = set()
_DEBUG_NNZ = defaultdict(int)

def predict(model_prop_type: str, row: dict) -> tuple[str, float]:
    build_feature_vector = _load_build_feature_vector()
    rf_model, lr_model = load_models(model_prop_type)

    # Build features
    row = to_plain_scalars(row)
    bfv_out = build_feature_vector(pd.DataFrame([row]))
    X = bfv_out[0] if isinstance(bfv_out, tuple) else bfv_out
    if X is None:
        raise ValueError("build_feature_vector returned None")

    X = _as_one_row_df(X)

    # Per-model schema from feature_metadata.json
    rf_cols, lr_cols = _expected_columns_pair_from_meta(model_prop_type)

    # One-time alignment visibility
    if model_prop_type not in _PRINTED_ALIGN:
        extra_rf   = [c for c in X.columns if c not in rf_cols]
        missing_rf = [c for c in rf_cols    if c not in X.columns]
        extra_lr   = [c for c in X.columns if c not in lr_cols]
        missing_lr = [c for c in lr_cols    if c not in X.columns]
        if extra_rf or missing_rf or extra_lr or missing_lr:
            print(
              f"🧩 Aligning {model_prop_type} — "
              f"RF(miss:{len(missing_rf)} extra:{len(extra_rf)} -> {len(rf_cols)}), "
              f"LR(miss:{len(missing_lr)} extra:{len(extra_lr)} -> {len(lr_cols)})"
            )
        _PRINTED_ALIGN.add(model_prop_type)

    # Align & coerce
    X_rf = _coerce_numeric_fill(X.reindex(columns=rf_cols, fill_value=0))
    X_lr = _coerce_numeric_fill(X.reindex(columns=lr_cols, fill_value=0))

    # Guards: schema overlap + sparsity
    ov_rf = _overlap(list(X.columns), rf_cols)
    ov_lr = _overlap(list(X.columns), lr_cols)
    if ov_rf < OVERLAP_MIN and ov_lr < OVERLAP_MIN:
        raise ValueError(f"insufficient feature overlap (rf:{ov_rf} lr:{ov_lr} min:{OVERLAP_MIN})")

    nnz_rf = _nnz_cols(X_rf)
    nnz_lr = _nnz_cols(X_lr)
    if nnz_rf < MIN_NNZ and nnz_lr < MIN_NNZ:
        raise ValueError("no usable features after alignment")

    if _DEBUG_NNZ[model_prop_type] < 3:
        print(f"   ↳ nonzero cols — RF:{nnz_rf}/{X_rf.shape[1]}  LR:{nnz_lr}/{X_lr.shape[1]}")
        _DEBUG_NNZ[model_prop_type] += 1

    # Score
    rf_prob = float(rf_model.predict_proba(X_rf)[0][1])
    lr_prob = float(lr_model.predict_proba(X_lr)[0][1])
    avg_prob = (rf_prob + lr_prob) / 2.0

    # Skip near-coinflips unless disabled
    if abs(avg_prob - 0.5) < COIN_EPSILON:
        raise ValueError(f"near-coinflip {avg_prob:.3f}; skipping write")

    pred = "over" if avg_prob >= 0.5 else "under"
    return pred, avg_prob

# ──────────────────────────────────────────────────────────────────────────────
# Summary tracking & printout
# ──────────────────────────────────────────────────────────────────────────────
SUMMARY = defaultdict(lambda: {
    "batches": 0,
    "fetched": 0,
    "attempted": 0,
    "updated": 0,
    "skipped": 0,
    "no_features": 0,
    "model_errors": 0,
    "errors": 0,
})

def print_summary(summary: dict, started_at: float) -> None:
    elapsed = perf_counter() - started_at
    print("\n" + "=" * 72)
    print("📈 Backfill Predictions — Run Summary")
    print(f"⏱️  Elapsed: {elapsed:0.1f}s")
    print("-" * 72)
    hdr = f"{'prop_type':20} {'batches':7} {'fetched':7} {'attempt':8} {'updated':8} {'skipped':8} {'no_feat':7} {'model_err':9} {'errors':7}"
    print(hdr)
    print("-" * 72)
    totals = {k: 0 for k in ["batches","fetched","attempted","updated","skipped","no_features","model_errors","errors"]}
    for ptype in sorted(summary.keys()):
        m = summary[ptype]
        print(f"{ptype:20} {m['batches']:7d} {m['fetched']:7d} {m['attempted']:8d} {m['updated']:8d} {m['skipped']:8d} {m['no_features']:7d} {m['model_errors']:9d} {m['errors']:7d}")
        for k in totals:
            totals[k] += m[k]
    print("-" * 72)
    print(f"{'TOTAL':20} {totals['batches']:7d} {totals['fetched']:7d} {totals['attempted']:8d} {totals['updated']:8d} {totals['skipped']:8d} {totals['no_features']:7d} {totals['model_errors']:9d} {totals['errors']:7d}")
    print("=" * 72 + "\n")

# ──────────────────────────────────────────────────────────────────────────────
# Batch processing
# ──────────────────────────────────────────────────────────────────────────────
def process_batch(db_prop_type: str, model_prop_type: str, batch_size: int = BATCH_SIZE) -> int:
    response = (
        supabase.table("model_training_props")
        .select("*")
        .eq("prop_type", db_prop_type)
        .eq("prop_source", "mlb_api")
        .is_("predicted_outcome", None)
        .eq("status", "resolved")
        .limit(batch_size)
        .execute()
    )

    rows = response.data or []
    SUMMARY[db_prop_type]["batches"] += 1
    SUMMARY[db_prop_type]["fetched"] += len(rows)

    print(f"📊 {db_prop_type}: Fetched {len(rows)} pending rows")
    if not rows:
        return 0

    updates = 0
    for row in rows:
        SUMMARY[db_prop_type]["attempted"] += 1
        try:
            row_dict = to_plain_scalars(_normalize_row(row))

            # extra sanity
            for k, v in list(row_dict.items()):
                if isinstance(v, (pd.Series, pd.DataFrame)):
                    row_dict[k] = to_plain_scalars({k: v}).get(k)
            for k, v in row_dict.items():
                if isinstance(v, (pd.Series, pd.DataFrame)):
                    raise TypeError(f"{k} remained a {type(v).__name__}")

            # ensure transformer knows the DB prop type
            row_dict.setdefault("prop_type", db_prop_type)

            pid = str(row_dict.get("player_id"))
            gid = str(row_dict.get("game_id"))
            team = str(row_dict.get("team"))
            print(f"🔍 player_id={pid}, game_id={gid}, team={team}")

            prediction, prob = predict(model_prop_type, row_dict)
            if prediction is None:
                SUMMARY[db_prop_type]["skipped"] += 1
                continue

            actual = _actual_label_from_row(row_dict)
            was_correct = (prediction == actual) if actual in ("over", "under") else None
            timestamp = datetime.now(timezone.utc).isoformat()

            supabase.table("model_training_props").update({
                "predicted_outcome": prediction,
                "confidence_score": float(prob),
                "was_correct": was_correct,
                "prediction_timestamp": timestamp,
            }).eq("id", row_dict["id"]).execute()

            print(f"✅ {row_dict.get('player_name')} → {prediction} ({prob:.3f}) | Correct? {was_correct}")
            updates += 1
            SUMMARY[db_prop_type]["updated"] += 1

        except Exception as e:
            traceback.print_exc()
            msg = str(e).lower()
            if ("near-coinflip" in msg
                or "no usable features" in msg
                or "insufficient feature overlap" in msg):
                SUMMARY[db_prop_type]["skipped"] += 1
            elif "model" in msg and ("missing" in msg or "no such file" in msg or "file not found" in msg or "invalid load key" in msg):
                SUMMARY[db_prop_type]["model_errors"] += 1
            else:
                SUMMARY[db_prop_type]["errors"] += 1
            print(f"❌ Failed on row {row.get('id') if isinstance(row, dict) else row}: {e}")

    return updates

def fetch_pending_prop_types() -> list[str]:
    resp = (
        supabase.table("model_training_props")
        .select("prop_type")
        .eq("prop_source", "mlb_api")
        .eq("status", "resolved")
        .is_("predicted_outcome", None)
        .limit(2000)
        .execute()
    )
    rows = resp.data or []
    return sorted({r.get("prop_type") for r in rows if r.get("prop_type")})

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
_enable_pandas_truthiness_compat()

def main():
    started_at = perf_counter()

    print("📆 Starting batch prediction loop")
    print(json.dumps({"model_dir": MODEL_DIR, "batch_size": BATCH_SIZE, "prop_types_env": ENV_PROP_TYPES}, indent=2))

    db_prop_types = ENV_PROP_TYPES or fetch_pending_prop_types()
    if not db_prop_types:
        print("✅ No pending rows. Nothing to do.")
        print_summary(SUMMARY, started_at)
        return

    print("🧰 Model inventory (disk):")
    for db_pt in db_prop_types:
        mt = model_type_for(db_pt)
        ok, missing = models_available(mt)
        if ok:
            print(f"  • {db_pt} (model: {mt}): OK")
        else:
            print(f"  • {db_pt} (model: {mt}): MISSING ({'; '.join(missing)})")

    for db_pt in db_prop_types:
        mt = model_type_for(db_pt)
        ok, missing = models_available(mt)
        if not ok:
            SUMMARY[db_pt]["model_errors"] += 1
            print(f"⏭️  {db_pt}: Skipping — models missing for '{mt}':\n     " + "\n     ".join(missing))
            continue

        batch_num = 0
        while True:
            batch_num += 1
            print(f"🔁 {db_pt} | Batch {batch_num}")
            updates = process_batch(db_pt, mt)
            if updates == 0:
                print(f"✅ {db_pt}: No more pending predictions.")
                break

    print("✅ All prop types processed.")
    print_summary(SUMMARY, started_at)

if __name__ == "__main__":
    main()
