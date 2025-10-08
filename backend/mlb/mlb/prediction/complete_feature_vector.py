# File: backend/scripts/prediction/complete_feature_vector.py

import yaml
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
FEATURE_SPEC_PATH = os.path.join(PROJECT_ROOT, "model_features.yaml")

_feature_spec_cache = None

def load_feature_spec():
    global _feature_spec_cache
    if _feature_spec_cache is not None:
        return _feature_spec_cache

    with open(FEATURE_SPEC_PATH, "r") as f:
        spec = yaml.safe_load(f)

    _feature_spec_cache = spec.get("features", {})
    return _feature_spec_cache

def complete_feature_vector(input_features: dict, prop_type: str) -> dict:
    """
    Fill missing features with default values based on the model_features.yaml spec.
    """
    spec = load_feature_spec()
    completed = {}

    for feature_name, feature_info in spec.items():
        if feature_name in input_features:
            completed[feature_name] = input_features[feature_name]
            continue

        ftype = feature_info.get("type")
        required = feature_info.get("required", False)

        # Choose defaults based on type
        if ftype == "numeric":
            completed[feature_name] = 0.0
        elif ftype == "binary":
            completed[feature_name] = 0
        elif ftype == "categorical":
            completed[feature_name] = "Unknown"
        elif ftype == "time":
            completed[feature_name] = "00:00"  # fallback time bucket
        else:
            completed[feature_name] = None  # Fallback for untyped or unknown

    return completed
