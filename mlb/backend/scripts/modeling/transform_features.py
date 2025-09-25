# backend/scripts/modeling/transform_features.py

import os
import json
import pandas as pd
import numpy as np

MODEL_DIR = os.getenv("MODEL_DIR", "/var/data/models")
_FEATURE_META = None

import os, json, numpy as np, pandas as pd

MODEL_DIR = os.getenv("MODEL_DIR", "/var/data/models")
_FEATURE_META = None

def _load_feature_meta():
    global _FEATURE_META
    if _FEATURE_META is None:
        with open(os.path.join(MODEL_DIR, "feature_metadata.json"), "r") as f:
            _FEATURE_META = json.load(f)
    return _FEATURE_META

def _expected_columns_pair_from_meta(prop_type: str) -> tuple[list[str], list[str]]:
    meta = _load_feature_meta()
    entry = meta.get(prop_type, meta.get("columns"))
    if isinstance(entry, list):
        return list(entry), list(entry)
    if isinstance(entry, dict):
        if isinstance(entry.get("columns"), list):
            cols = list(entry["columns"]); return cols, cols
        rf = entry.get("random_forest"); lr = entry.get("logistic_regression")
        if isinstance(rf, list) and isinstance(lr, list): return list(rf), list(lr)
        if isinstance(rf, list): return list(rf), list(rf)
        if isinstance(lr, list): return list(lr), list(lr)
    return [], []

def _safe_setcol(df: pd.DataFrame, col: str, value):
    if col in df.columns: df[col] = value
    else: df.loc[:, col] = value

def _coerce_numeric_fill(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == object:
            try: df[c] = pd.to_numeric(df[c], errors="coerce")
            except Exception: pass
    return df.replace([np.inf, -np.inf], 0).fillna(0)

def transform_features(df: pd.DataFrame, debug: bool = False, *args, **kwargs) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    if df.empty:
        return df

    prop_type = None
    if "prop_type" in df.columns:
        v = df["prop_type"].iloc[0]
        prop_type = str(v) if v is not None else None

    # Pre-create the union of expected columns for this prop
    rf_cols, lr_cols = _expected_columns_pair_from_meta(prop_type or "")
    expected = sorted(set(rf_cols) | set(lr_cols))
    for col in expected:
        if col not in df.columns:
            _safe_setcol(df, col, 0)

    # Final cleanup
    df = _coerce_numeric_fill(df)
    return df

   
