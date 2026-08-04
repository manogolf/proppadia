"""Model registry helpers for MLB prediction runtime."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib

_PROP_ALIASES = {
    "rbi": "rbis",
    "runs": "runs_scored",
}


def canonicalize_prop_type(prop_type: str) -> str:
    p = (
        str(prop_type or "")
        .strip()
        .lower()
        .replace("(", "")
        .replace(")", "")
        .replace(" + ", "_")
        .replace(" ", "_")
        .strip("_")
    )
    return _PROP_ALIASES.get(p, p)


def _models_root() -> Path:
    configured = os.getenv("MODEL_DIR")
    if configured:
        return Path(configured)
    return Path("/var/data/proppadia/models")


def _latest_artifact_path(prop: str) -> Path:
    return _models_root() / "latest" / f"{prop}.joblib"


def _legacy_model_path(prop: str, algo: str) -> Path:
    suffix = "random_forest" if algo == "random_forest" else "logistic_regression"
    return _models_root() / prop / f"{prop}_{suffix}.pkl"


def _alt_model_paths(prop: str) -> List[Path]:
    root = _models_root()
    # Legacy MLB regressor bundles (batter/pitcher trees).
    return [
        root / "batter" / prop / f"{prop}_poisson_v1.joblib",
        root / "batter" / prop / f"{prop}.joblib",
        root / "batter" / prop / "zip_lambda.joblib",
        root / "pitcher" / prop / f"{prop}.joblib",
        root / prop / f"{prop}.joblib",
    ]


def _feature_metadata_candidates() -> List[Path]:
    return [
        _models_root() / "feature_metadata.json",
        # repo/backend/mlb/modeling/*
        Path(__file__).resolve().parents[2] / "mlb" / "modeling" / "feature_metadata.json",
        Path(__file__).resolve().parents[2] / "mlb" / "modeling" / "feature_metadata_backup.json",
        # legacy repo/mlb/modeling/*
        Path(__file__).resolve().parents[3] / "mlb" / "modeling" / "feature_metadata.json",
        Path(__file__).resolve().parents[3] / "mlb" / "modeling" / "feature_metadata_backup.json",
    ]


def load_model(prop_type: str, algo: str):
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("production_model_load")
    prop = canonicalize_prop_type(prop_type)
    alg = str(algo or "").strip().lower()
    if alg not in {"random_forest", "logistic_regression"}:
        raise ValueError("algo must be random_forest or logistic_regression")

    latest_path = _latest_artifact_path(prop)
    if latest_path.exists():
        obj = joblib.load(latest_path)
        if isinstance(obj, dict):
            model = obj.get(alg)
            if model is not None:
                return model
            # Support bundled aliases used by backup/promotion artifacts.
            if alg == "random_forest" and obj.get("rf") is not None:
                return obj.get("rf")
            if alg == "logistic_regression" and obj.get("lr") is not None:
                return obj.get("lr")
            models = obj.get("models") if isinstance(obj.get("models"), dict) else None
            if models and models.get(alg) is not None:
                return models.get(alg)
            if models:
                if alg == "random_forest" and models.get("rf") is not None:
                    return models.get("rf")
                if alg == "logistic_regression" and models.get("lr") is not None:
                    return models.get("lr")

    legacy_path = _legacy_model_path(prop, alg)
    if legacy_path.exists():
        return joblib.load(legacy_path)

    for alt in _alt_model_paths(prop):
        if alt.exists():
            return joblib.load(alt)

    raise FileNotFoundError(f"model not found for prop={prop} algo={alg}")


def get_expected_features(prop_type: str, prefer: str = "random_forest") -> Optional[List[str]]:
    prop = canonicalize_prop_type(prop_type)
    pref = str(prefer or "").strip().lower()
    for candidate in _feature_metadata_candidates():
        if not candidate.exists():
            continue
        try:
            meta = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        entry: Any = meta.get(prop) if isinstance(meta, dict) else None
        if entry is None and isinstance(meta, dict):
            entry = meta.get("columns")
        if isinstance(entry, list):
            return [str(c) for c in entry]
        if isinstance(entry, dict):
            if isinstance(entry.get("columns"), list):
                return [str(c) for c in entry.get("columns")]
            preferred = entry.get(pref)
            if isinstance(preferred, list):
                return [str(c) for c in preferred]
            # fallback to either algo list
            for k in ("random_forest", "logistic_regression"):
                if isinstance(entry.get(k), list):
                    return [str(c) for c in entry.get(k)]
    return None
