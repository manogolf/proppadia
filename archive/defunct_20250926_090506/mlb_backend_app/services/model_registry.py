# backend/app/services/model_registry.py

"""
Model registry utilities:
- Load fitted models from disk (with optional Supabase fallback).
- Resolve expected features for vectorization (repo JSON first).
- Canonicalize prop types.

Feature order preference (highest → lowest):
  1) Repo JSON (backend/scripts/modeling/feature_metadata.json or *_backup.json)
  2) Model meta inside joblib ("features_num"+"features_cat")
  3) latest/MODEL_INDEX.json (written by trainer)
"""
# in backend/app/services/model_registry.py
from __future__ import annotations
import os, json
from pathlib import Path
import os, json, threading, requests
import logging
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from joblib import load as joblib_load

log = logging.getLogger(__name__)

# ── Optional Supabase client (only if env vars exist) ─────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
_supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        _supabase = None  # don’t crash if the lib/env isn’t available

# ── Paths ─────────────────────────────────────────────────────────────────────
MODELS_DIR = Path(os.getenv("MODELS_DIR") or os.getenv("MODEL_DIR") or "/var/data/models").resolve()
MODEL_DIR = MODELS_DIR  # back-compat alias

# Do not probe env/paths for v2; leave empty to avoid accidental loads.
_FEATURE_JSON_CANDIDATES: list = []

def _latest_index_path() -> Path:
    return MODELS_DIR / "latest" / "MODEL_INDEX.json"

# Add just below: def _latest_index_path() -> Path: ...
def _latest_root() -> Path:
    return (MODELS_DIR / "latest").resolve()

def _read_latest_index_dict() -> dict:
    p = _latest_index_path()
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.warning("MODEL_INDEX.json not readable at %s: %s", p, e)
        return {}

def resolve_feature_spec_path(prop_name: str) -> Path:
    """
    Resolve the features.json path for a prop using:
      1) MODEL_INDEX.json 'features' entry (absolute or relative to latest/)
      2) latest/<prop>/features.json
      3) latest/<prop>/<prop>_features.json
    """
    root = _latest_root()
    idx = _read_latest_index_dict()
    entry = idx.get(prop_name) or {}

    candidates = []

    # 1) Path from index ('features': "total_bases/features.json")
    feat = entry.get("features")
    if isinstance(feat, str) and feat.strip():
        p = Path(feat)
        candidates.append(p if p.is_absolute() else (root / p))

    # 2–3) Common local names alongside the model
    prop_dir = root / prop_name
    candidates += [
        prop_dir / "features.json",
        prop_dir / f"{prop_name}_features.json",
    ]

    log.info("[features] %s candidates: %s", prop_name, [str(p) for p in candidates])

    for p in candidates:
        if p.exists():
            log.info("[features] %s resolved -> %s (%d bytes)", prop_name, p, p.stat().st_size)
            return p

    raise FileNotFoundError(
        f"Features file not found for '{prop_name}'. Tried: " + ", ".join(str(p) for p in candidates)
    )

# ── Caches ────────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_MODEL_CACHE: Dict[Tuple[str, str], Any] = {}     # (prop_type, algo) -> fitted Pipeline
_FEATURE_META: Optional[Dict[str, Any]] = None
_CANON: Optional[Dict[str, str]] = None

# ── Canonicalization ──────────────────────────────────────────────────────────
def _canonical_map() -> Dict[str, str]:
    global _CANON
    if _CANON is not None:
        return _CANON
    canon = {
        "hits":"hits","singles":"singles","doubles":"doubles","triples":"triples",
        "home_runs":"home_runs","rbis":"rbis","runs_scored":"runs_scored","walks":"walks",
        "strikeouts_batting":"strikeouts_batting","total_bases":"total_bases","stolen_bases":"stolen_bases",
        "hits_runs_rbis":"hits_runs_rbis","runs_rbis":"runs_rbis",
        "strikeouts_pitching":"strikeouts_pitching","walks_allowed":"walks_allowed",
        "earned_runs":"earned_runs","hits_allowed":"hits_allowed","outs_recorded":"outs_recorded",
    }
    aliases = {
        "hr":"home_runs","home run":"home_runs",
        "runs+rbi":"runs_rbis","runs rbis":"runs_rbis","runs rbi":"runs_rbis",
        "h+r+rbi":"hits_runs_rbis","hrr":"hits_runs_rbis","hrrr":"hits_runs_rbis",
    }
    _CANON = {**{k: k for k in canon}, **{k.lower(): v for k, v in aliases.items()}}
    return _CANON

def canonicalize_prop_type(s: str) -> str:
    key = (s or "").strip().lower()
    m = _canonical_map()
    if key in m:            # alias → canonical
        return m[key]
    if key in m.values():   # already canonical
        return key
    raise ValueError(f"Unknown prop_type '{s}'")

# ── Feature metadata (repo JSON → model meta → index) ─────────────────────────
# ── Feature metadata (deprecated in v2) ────────────────────────────────────────
_FEATURE_META: Dict[str, Any] | None = {}

def _load_feature_metadata_repo() -> Dict[str, Any]:
    """
    DEPRECATED in v2: per-prop features are loaded by the predict route.
    Keep this a safe no-op so any legacy import doesn't crash.
    """
    return {}

def _features_from_model_meta(prop: str, prefer: str) -> List[str]:
    """Try reading features from the joblib payload's meta."""
    for p in _disk_candidates(prop, prefer):
        if p.exists():
            try:
                obj = joblib_load(str(p))
                if isinstance(obj, dict):
                    meta = obj.get("meta") or {}
                    num = meta.get("features_num") or []
                    cat = meta.get("features_cat") or []
                    if num or cat:
                        return list(dict.fromkeys(list(num) + list(cat)))
            except Exception:
                continue
    return []

def _features_from_index(prop: str) -> List[str]:
    idx_path = _latest_index_path()
    if idx_path.exists():
        try:
            idx = json.loads(idx_path.read_text())
            entry = idx.get(prop)
            if entry:
                num = entry.get("features_num") or []
                cat = entry.get("features_cat") or []
                if num or cat:
                    return list(dict.fromkeys(list(num) + list(cat)))
        except Exception:
            pass
    return []

def get_expected_features(prop_type: str, prefer: str = "random_forest") -> List[str]:
    """Return ordered feature list used by prediction vectorizer."""
    prop = canonicalize_prop_type(prop_type)

    # 1) Repo JSON (source of truth)
    repo = _load_feature_metadata_repo().get(prop)
    if repo:
        # accept multiple common keys
        for key in (prefer, "rf" if prefer == "random_forest" else None, "logistic_regression", "lr", "features"):
            if not key:
                continue
            feats = repo.get(key)
            if feats:
                return list(dict.fromkeys(list(feats)))

    # 2) Model meta inside joblib
    feats = _features_from_model_meta(prop, prefer)
    if feats:
        return feats

    # 3) MODEL_INDEX.json (written by trainer)
    feats = _features_from_index(prop)
    if feats:
        return feats
    
        # 3b) MODEL_INDEX.json may specify a *path* ('features') to a JSON list
    try:
        p = resolve_feature_spec_path(prop)
        feats = json.loads(p.read_text())
        if isinstance(feats, list) and feats:
            return list(dict.fromkeys(feats))
    except Exception:
        pass

    # 4) Last resort: empty → caller should 0-fill
    return []

# ── Supabase fallback download (only if disk-miss) ────────────────────────────
def _download_from_supabase(bucket: str, path: str) -> bytes:
    if not _supabase:
        raise RuntimeError("Supabase client not available for fallback download.")
    res = _supabase.storage.from_(bucket).create_signed_url(path, 3600)
    url = res["signedURL"]
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

# Optional pin to a specific snapshot (e.g., MODEL_TAG=20250813T200000Z)
MODEL_TAG = os.getenv("MODEL_TAG")

# ── Disk search order (pinned → latest → legacy) ──────────────────────────────
def _disk_candidates(prop: str, algo: str) -> List[Path]:
    """
    1) If MODEL_TAG: /var/data/models/archive/<prop>/<prop>-<TAG>.joblib
    2) /var/data/models/latest/<prop>.joblib
    3) /var/data/models/<prop>/latest.joblib
    4) /var/data/models/<prop>/<algo>.joblib
    5) Legacy PKL under /var/data/models/<prop>/:
         - <prop>_<algo>.pkl
         - <algo>.pkl
    """
    base = MODELS_DIR
    if MODEL_TAG:
        return [(base / "archive" / prop / f"{prop}-{MODEL_TAG}.joblib").resolve()]
    return [
        (base / "latest" / f"{prop}.joblib").resolve(),
        (base / prop / "latest.joblib").resolve(),
        (base / prop / f"{algo}.joblib").resolve(),
        (base / prop / f"{prop}_{algo}.pkl").resolve(),
        (base / prop / f"{algo}.pkl").resolve(),
    ]

# ── Unwrap + fitted check ─────────────────────────────────────────────────────
def _unwrap_model(obj: Any, algo: str):
    """Return an estimator from a loaded joblib object."""
    # already an estimator?
    if hasattr(obj, "predict") or hasattr(obj, "predict_proba"):
        return obj
    if isinstance(obj, dict):
        if algo == "random_forest" and obj.get("rf") is not None:
            return obj["rf"]
        if algo == "logistic_regression" and obj.get("lr") is not None:
            return obj["lr"]
        if obj.get("best") is not None:
            return obj["best"]
    return None

def _looks_fitted(pipeline) -> bool:
    """Heuristic: estimator has learned attributes."""
    try:
        clf = getattr(pipeline, "named_steps", {}).get("clf", None)
        if clf is None:
            return False
        return any(hasattr(clf, attr) for attr in ("classes_", "n_features_in_"))
    except Exception:
        return False

# ── Public: load_model ────────────────────────────────────────────────────────
def load_model(prop: str, algo: str | None = None):
    """
    DEPRECATED in v2. Models are resolved in routes/api/predict.py.
    This stub exists to avoid crashes if legacy code imports it.
    """
    raise RuntimeError(
        "model_registry.load_model is deprecated in v2. "
        "Use /api/predict (which auto-discovers per-prop models/features)."
    )
