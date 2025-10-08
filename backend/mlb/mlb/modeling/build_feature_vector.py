# backend/scripts/modeling/build_feature_vector.py
from __future__ import annotations
from typing import Any, Dict, List
# Reuse the shared per-prop logic from ml/
from ml.feature_utils import load_feature_names, vector_from_features

# --- Back-compat no-ops (so old imports don't crash) ---
FEATURE_NAMES: List[str] = []  # not used in v2
META_PATH = None               # not used in v2

def build_feature_vector_for(prop: str, features: Dict[str, Any]) -> List[float]:
    """
    v2: Build a feature vector for a specific prop using that prop's
    features_<prop>_v1.json file colocated with the model.
    """
    names = load_feature_names(prop)
    return vector_from_features(features, names)

# Optional convenience if some legacy code calls "build_feature_vector"
# without a prop. We keep it explicit in v2 (require prop).
def build_feature_vector(features: Dict[str, Any], *, prop: str | None = None) -> List[float]:
    if not prop:
        raise ValueError("build_feature_vector now requires 'prop' in v2.")
    return build_feature_vector_for(prop, features)
