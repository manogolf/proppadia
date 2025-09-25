#  ml/feature_utils.py

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List
import os, json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --- logging (optional but helpful) ---
log = logging.getLogger("precompute")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# --- one shared HTTP session with retries/backoff ---
_SESSION = requests.Session()
_RETRY = Retry(
    total=3,
    backoff_factor=0.6,               # 0.6s, 1.2s, 1.8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
    respect_retry_after_header=True,
)
_ADAPTER = HTTPAdapter(max_retries=_RETRY, pool_connections=20, pool_maxsize=20)
_SESSION.mount("https://", _ADAPTER)
_SESSION.headers.update({"User-Agent": "Proppadia-Precompute/1.0"})

_DEFAULT_TIMEOUT = (3.05, 10)  # (connect, read) seconds

def _get(url: str, timeout=_DEFAULT_TIMEOUT):
    """GET JSON with sane timeouts/retries; return None on failure."""
    try:
        resp = _SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.Timeout:
        log.warning("Timeout fetching %s", url)
        return None
    except requests.RequestException as e:
        log.warning("HTTP error fetching %s: %s", url, e)
        return None
    except ValueError:
        log.warning("Non-JSON response from %s", url)
        return None

# --- update _people_stats to guard None ---
def _people_stats(person_id, group="hitting", types=None):
    types = types or ["last7", "last15", "last30"]
    # build your URL exactly as before...
    url = f"https://statsapi.mlb.com/api/v1/people/{person_id}/stats?group={group}&stats={','.join(types)}"
    data = _get(url)
    # Always return a safe shape so callers don't hang on bad data
    return data or {"stats": []}

def _models_root() -> Path:
    env = os.getenv("MODELS_ROOT") or os.getenv("MODELS_DIR") or os.getenv("MODEL_DIR")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parents[1] / "models"

def _prop_folders(prop: str) -> List[Path]:
    root = _models_root()
    return [
        root / "batter" / prop,
        root / "pitcher" / prop,
        root / prop,
    ]


from pathlib import Path
import os

def _models_root() -> Path:
    """
    Resolve the models root. Honors env overrides for CI/cron.
    """
    env = os.getenv("MODELS_ROOT") or os.getenv("MODELS_DIR") or os.getenv("MODEL_DIR")
    if env:
        return Path(env).resolve()
    # this file lives in ml/, models live in ml/models
    return Path(__file__).resolve().parent / "models"

def _prop_folders(prop: str) -> list[Path]:
    root = _models_root()
    return [root / "batter" / prop, root / "pitcher" / prop, root / prop]


def _models_root() -> Path:
    """
    Resolve the models root. Honors env overrides for CI/cron.
    """
    env = os.getenv("MODELS_ROOT") or os.getenv("MODELS_DIR") or os.getenv("MODEL_DIR")
    if env:
        return Path(env).resolve()
    # this file lives in ml/, models live in ml/models
    return Path(__file__).resolve().parent / "models"

def _prop_folders(prop: str) -> list[Path]:
    root = _models_root()
    return [root / "batter" / prop, root / "pitcher" / prop, root / prop]

def features_path_for(prop: str) -> Path:
    """
    Find the per-prop features JSON with robust fallbacks:
      - FEATURE_META_PATH_<prop> or FEATURE_META_PATH (env overrides)
      - <ROOT>/{batter,pitcher}/<prop>/{features_<prop>_<tag>.json | <prop>_features_<tag>.json}
      - <ROOT>/{batter,pitcher}/<prop>/{features_<prop>.json | <prop>_features.json}
      - fallback globs in the prop folder(s), preferring *_<tag>.json
      - skip calibrator files
    """
    # explicit overrides
    env = os.getenv(f"FEATURE_META_PATH_{prop}") or os.getenv("FEATURE_META_PATH")
    if env:
        p = Path(env).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Feature meta file not found: {p}")
        return p

    tag = os.getenv("FEATURE_SET_TAG", "v1")
    tried: list[str] = []

    # exact preferred names
    for folder in _prop_folders(prop):
        for name in (
            f"features_{prop}_{tag}.json",
            f"{prop}_features_{tag}.json",
            f"features_{prop}.json",
            f"{prop}_features.json",
        ):
            p = folder / name
            tried.append(str(p))
            if p.exists():
                return p

    # fallback: glob in priority order, filter out calibrator files
    def collect(folder: Path) -> list[Path]:
        if not folder.exists():
            return []
        pats = (
            f"{prop}_features_*.json",
            f"features_{prop}_*.json",
            "*features*.json",
            "*.json",
        )
        out: list[Path] = []
        for pat in pats:
            tried.append(str(folder / pat))
            out.extend(folder.glob(pat))
        # avoid grabbing calibrator files
        out = [x for x in out if "calibrator" not in x.name and "calibrators" not in x.name]
        return out

    def pick_best(cands: list[Path]) -> Path | None:
        if not cands:
            return None
        # 1 prefer files containing _{tag}.
        tagged = [c for c in cands if f"_{tag}." in c.name]
        if tagged:
            return sorted(tagged)[-1]
        # 2 prefer names that include the prop in a common pattern
        pref = [c for c in cands if f"{prop}_features" in c.name or f"features_{prop}" in c.name]
        if pref:
            return sorted(pref)[-1]
        # 3 otherwise, last available feature-like json
        feats = [c for c in cands if "feature" in c.name]
        if feats:
            return sorted(feats)[-1]
        return sorted(cands)[-1]

    for folder in _prop_folders(prop):
        best = pick_best(collect(folder))
        if best:
            return best

    raise FileNotFoundError(
        f"No features file for '{prop}'. Tried: {', '.join(tried)}."
    )

def load_feature_names(prop: str) -> List[str]:
    p = features_path_for(prop)
    data = json.loads(p.read_text())
    if isinstance(data, dict):
        for k in ("feature_names", "features", "ordered_feature_names", "columns"):
            v = data.get(k)
            if isinstance(v, list):
                return list(v)
        if prop in data and isinstance(data[prop], dict):
            v = data[prop].get("columns")
            if isinstance(v, list):
                return list(v)
        raise ValueError(f"Could not find a list of features in {p}")
    elif isinstance(data, list):
        return list(data)
    else:
        raise ValueError(f"Unsupported feature meta format in {p}")

def _coerce_scalar(v: Any) -> float:
    if v is None: return 0.0
    if isinstance(v, bool): return 1.0 if v else 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().lower()
    if s in {"true","t","yes","y"}: return 1.0
    if s in {"false","f","no","n"}: return 0.0
    try: return float(s)
    except Exception: return 0.0

def vector_from_features(features: Dict[str, Any], ordered_names: List[str]) -> List[float]:
    return [_coerce_scalar(features.get(name)) for name in ordered_names]
