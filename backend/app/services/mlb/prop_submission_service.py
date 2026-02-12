"""MLB prepare/predict/add prop application services."""

from __future__ import annotations

from typing import Any, Dict

from backend.domains.mlb.prop_workflow import add_prop_from_commit, predict_prop, prepare_prop


def prepare_prop_submission(payload: Dict[str, Any]) -> Dict[str, Any]:
    features = prepare_prop(payload)
    warnings = features.pop("_warnings", None)
    out = {"ok": True, "features": features}
    if warnings:
        out["warnings"] = warnings
    return out


def predict_prepared_prop(payload: Dict[str, Any]) -> Dict[str, Any]:
    prop_type = str(payload.get("prop_type") or "").strip()
    features = payload.get("features") or {}
    if not isinstance(features, dict):
        raise ValueError("features must be an object")
    return predict_prop(prop_type=prop_type, features=features)


def add_prop(payload: Dict[str, Any]) -> Dict[str, Any]:
    commit_token = str(payload.get("commit_token") or "").strip()
    if not commit_token:
        raise ValueError("commit_token is required")
    prop_source = str(payload.get("prop_source") or "user_added").strip() or "user_added"
    return add_prop_from_commit(commit_token=commit_token, prop_source=prop_source)
