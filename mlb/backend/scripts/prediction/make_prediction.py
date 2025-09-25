# backend/scripts/prediction/make_prediction.py

import joblib
import os, json
import sys
import numpy as np
import pandas as pd
import math
import re

from typing import Dict, Any, List, Optional
from pathlib import Path
from backend.app.services.model_registry import (
    canonicalize_prop_type,
    load_model,
    get_expected_features,
)

# (optional) previously unused: _missing_re = re.compile(r"columns are missing:\s*\{([^}]*)\}")
# Make sure the repo root is on sys.path (…/project/src)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

    # Columns that are identifiers/provenance, not model features
_EXCLUDE_KEYS = {
    "player_id", "team_id", "game_id", "game_date",
    "prop_type", "over_under", "prop_value",
    "prop_source", "created_at", "updated_at", "ingested_at",
}

def _parse_missing_columns(msg: str) -> List[str]:
    # extract 'colname' items from error string
    return re.findall(r"'([^']+)'", msg or "")

def _augment_df_with_missing(X: pd.DataFrame, features: Dict[str, Any], missing: List[str]) -> pd.DataFrame:
    """Add missing columns the pipeline asked for, with sensible defaults."""
    X = X.copy()
    for col in missing:
        if col in X.columns:
            continue
        if col.startswith("isna__"):
            base = col.split("__", 1)[1]
            v = features.get(base, None)
            X[col] = 1.0 if (v is None or v == "" or (isinstance(v, float) and math.isnan(v))) else 0.0
        elif col == "streak_type":
            v = features.get("streak_type")
            if v is None:
                hot = features.get("streak_type_hot")
                cold = features.get("streak_type_cold")
                if hot in (1, True, "1", "true"): v = "hot"
                elif cold in (1, True, "1", "true"): v = "cold"
                else: v = "none"
            X[col] = str(v)
        else:
            # numeric default
            val = features.get(col, 0.0)
            try:
                val = float(val)
            except Exception:
                val = 0.0
            X[col] = val
    return X

def _p_retry_missing(model, X: pd.DataFrame, features: Dict[str, Any]) -> Optional[float]:
    """Score; if the model complains about missing columns, augment and retry once."""
    if model is None:
        return None
    # first attempt
    try:
        if hasattr(model, "predict_proba"):
            return float(model.predict_proba(X)[0][1])
        if hasattr(model, "predict"):
                y2 = model.predict(X2)
                return float(np.ravel(y2)[0])    
    except Exception as e:
        missing = _parse_missing_columns(str(e))
        if not missing:
            print(f"[predict] {type(model).__name__} failed: {e}", file=sys.stderr, flush=True)
            return None
        X2 = _augment_df_with_missing(X, features, missing)
        try:
            if hasattr(model, "predict_proba"):
                return float(model.predict_proba(X2)[0][1])
            if hasattr(model, "predict"):
                return float(np.ravel(model.predict(X2)[0]))
        except Exception as e2:
            print(f"[predict] {type(model).__name__} retry failed: {e2}", file=sys.stderr, flush=True)
            return None
    return None

def _columns_from_features_dict(features: Dict[str, Any]) -> List[str]:
    """
    Infer the model input columns from the enriched features (MV row):
      - include all base numeric/string features (minus IDs/provenance)
      - add isna__<base> for each base
      - ensure 'streak_type' exists (categorical expected by the pipeline)
    """
    base = [k for k in features.keys() if k not in _EXCLUDE_KEYS]
    cols = set(base)
    for b in base:
        cols.add(f"isna__{b}")
    cols.add("streak_type")
    # Deterministic order (pipeline uses names; order won't matter, but keep stable)
    return sorted(cols)


DEBUG = os.getenv("DEBUG_PREDICT") not in (None, "", "0", "false", "False")

def _is_missing(v) -> bool:
    return v is None or v == "" or (isinstance(v, float) and math.isnan(v))

def _vectorize(features: Dict[str, Any], feature_list: List[str]) -> pd.DataFrame:
    """
    Build a 1-row DataFrame whose columns exactly match `feature_list`.
    Special handling:
      - 'isna__<base>' columns are generated from missingness of `<base>`
      - 'streak_type' remains a string category (default 'none')
      - everything else coerced to float with fallback 0.0
    """
    row: Dict[str, Any] = {}
    for col in feature_list:
        if col.startswith("isna__"):
            base = col.split("__", 1)[1]
            v = features.get(base, None)
            row[col] = 1.0 if _is_missing(v) else 0.0
        elif col == "streak_type":
            v = features.get("streak_type", None)
            # if caller passed streak_type_hot/cold flags, synthesize a label
            if v is None:
                hot = features.get("streak_type_hot")
                cold = features.get("streak_type_cold")
                if hot in (1, True, "1", "true"): v = "hot"
                elif cold in (1, True, "1", "true"): v = "cold"
                else: v = "none"
            row[col] = str(v)
        else:
            v = features.get(col, 0)
            try:
                row[col] = float(v)
            except Exception:
                row[col] = 0.0
    return pd.DataFrame([row], columns=feature_list)

def _input_columns_for(prop: str) -> list[str] | None:
    """
    Prefer the input column list stored in the model artifact's meta.
    This list matches what the pipeline expects (e.g., 'isna__*', raw categoricals).
    """
    try:
        p = Path("/var/data/models/latest") / f"{prop}.joblib"
        if p.exists():
            obj = joblib.load(p)
            meta = obj.get("meta") if isinstance(obj, dict) else None
            if meta:
                # try a few common keys
                for key in ("input_columns", "expected_input_columns", "features_in", "expected_columns"):
                    cols = meta.get(key)
                    if cols:
                        return list(cols)
    except Exception:
        pass
    try:
        # last resort (older artifacts). may not include isna__/categoricals
        return get_expected_features(prop, prefer="random_forest")
    except Exception:
        return None

def _p(model, X) -> Optional[float]:
    if model is None:
        return None
    try:
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X)
            return float(proba[0][1])
        if hasattr(model, "predict"):
            y = model.predict(X)
            return float(np.ravel(y)[0])
    except Exception as e:  # <-- bind as e
        # helpful log so we see column/schema issues instead of silent 0.5s
        print(f"[predict] {type(model).__name__} failed: {e}", file=sys.stderr, flush=True)
        return None
    return None

def _blend(a: Optional[float], b: Optional[float]) -> float:
    xs = [x for x in (a, b) if x is not None]
    return sum(xs) / len(xs) if xs else 0.5


def _load_artifact_meta(prop: str) -> dict:
    """Read /var/data/models/latest/{prop}.joblib and return its meta dict."""
    try:
        p = Path("/var/data/models/latest") / f"{prop}.joblib"
        obj = joblib.load(p)
        if isinstance(obj, dict):
            return obj.get("meta") or {}
    except Exception:
        pass
    return {}

def _auc_for(prop: str, algo: str) -> Optional[float]:
    """
    Try a few common keys to find AUC in the artifact meta.
    algo in {"logistic_regression","random_forest"}.
    """
    meta = _load_artifact_meta(prop)
    if not meta:
        return None

    # common key patterns
    if algo == "logistic_regression":
        for k in ("auc_lr", "lr_auc"):
            if k in meta: 
                try: return float(meta[k])
                except: pass
        try: return float((meta.get("metrics", {}).get("lr", {}) or {}).get("auc"))
        except: pass
    elif algo == "random_forest":
        for k in ("auc_rf", "rf_auc"):
            if k in meta: 
                try: return float(meta[k])
                except: pass
        try: return float((meta.get("metrics", {}).get("rf", {}) or {}).get("auc"))
        except: pass

    # very generic fallbacks if present
    for k in ("valid_auc", "auc"):
        if k in meta:
            try: return float(meta[k])
            except: pass
    return None

def _weight_from_auc(auc: Optional[float]) -> Optional[float]:
    """Map AUC to a non-negative weight; 0.5→0, better than random > 0."""
    if auc is None:
        return None
    try:
        return max(float(auc) - 0.5, 0.0)
    except Exception:
        return None

def _blend_weighted(values: list[float], weights: list[float]) -> Optional[float]:
    """Weighted mean with renormalization; returns None if nothing valid."""
    if not values or not weights or len(values) != len(weights):
        return None
    # keep only (v,w) where both are valid and w>0
    pairs = [(v, w) for v, w in zip(values, weights) if v is not None and w is not None and w > 0]
    if not pairs:
        # If all weights are 0/None, fall back to plain average of non-None values
        vals = [v for v in values if v is not None]
        return sum(vals) / len(vals) if vals else None
    num = sum(v * w for v, w in pairs)
    den = sum(w for _, w in pairs)
    return (num / den) if den > 0 else None


def predict(*, prop_type: str, features: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry for in-process import."""
    prop = canonicalize_prop_type(prop_type)

    # 1) expected columns (prefer artifact meta if present; else infer from DB-enriched features)
    feat_cols = _input_columns_for(prop) or _columns_from_features_dict(features)
    if not feat_cols:
        feat_cols = get_expected_features(prop, prefer="random_forest") or []

    # 2) strictly-filtered DF in correct order (no extra cols!)
    X = _vectorize(features, feat_cols)

    # 3) load models (disk-first, supabase fallback if configured)
    lr = rf = None
    try:
        lr = load_model(prop, "logistic_regression")
    except Exception:
        pass
    try:
        rf = load_model(prop, "random_forest")
    except Exception:
        pass
    if not (lr or rf):
        raise RuntimeError(f"No models available for prop_type '{prop}'")

    # 4) predict (retry-aware if available) + AUC-weighted blend
    if "_p_retry_missing" in globals():
        p_lr = _p_retry_missing(lr, X, features)
        p_rf = _p_retry_missing(rf, X, features)
    else:
        p_lr = _p(lr, X)
        p_rf = _p(rf, X)

    if p_lr is None and p_rf is None:
        raise RuntimeError(f"both models failed to score for prop={prop}")

    # fetch AUCs -> weights
    auc_lr = _auc_for(prop, "logistic_regression")
    auc_rf = _auc_for(prop, "random_forest")
    w_lr = _weight_from_auc(auc_lr)
    w_rf = _weight_from_auc(auc_rf)

    # weighted blend with robust fallbacks
    p_over = _blend_weighted([p_lr, p_rf], [w_lr, w_rf])
    if p_over is None:
        # last resort (shouldn’t happen): equal-blend of what we have
        p_over = _blend(p_lr, p_rf)

    # clamp
    p_over = max(0.0, min(1.0, p_over))

    return {
        "prop_type": prop,
        "probability_over": p_over,
        "probability": p_over,
        "probability_under": 1.0 - p_over,
        "components": {"lr": p_lr, "rf": p_rf},
        "blend": {
            "strategy": "auc_weighted",
            "weights": {"lr": w_lr, "rf": w_rf},
            "aucs": {"lr": auc_lr, "rf": auc_rf},
        },
        "feature_count": len(feat_cols),
        "used_features": feat_cols,
        "model": "blend_auc(lr,rf)",
    }

# Subprocess mode: read stdin JSON and print JSON to stdout.
def make_prediction(*, prop_type: str, features: Dict[str, Any]) -> Dict[str, Any]:
    # alias for older call-site names
    return predict(prop_type=prop_type, features=features)

if __name__ == "__main__":
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        out = predict(
            prop_type=payload.get("prop_type") or payload.get("propType"),
            features=payload.get("features") or {},
        )
        sys.stdout.write(json.dumps(out))
    except Exception as e:
        sys.stderr.write(str(e))
        sys.exit(1)
