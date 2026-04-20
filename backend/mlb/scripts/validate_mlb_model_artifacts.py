#!/usr/bin/env python3
"""Validate MLB model artifacts for runtime compatibility."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Sequence

import joblib
from sklearn.exceptions import NotFittedError
from sklearn.utils.validation import check_is_fitted

from backend.app.services.model_registry import (
    canonicalize_prop_type,
    get_expected_features,
    load_model,
)


def _artifact_latest_dir() -> Path:
    root = Path(str(os.environ.get("MODEL_DIR", "/var/data/proppadia/models")))
    return root / "latest"


def _load_artifact_meta(prop_type: str) -> Dict[str, Any]:
    path = _artifact_latest_dir() / f"{prop_type}.joblib"
    try:
        obj = joblib.load(path)
        if isinstance(obj, dict):
            meta = obj.get("meta")
            if isinstance(meta, dict):
                return meta
    except Exception:
        pass
    return {}


def _feature_names(model: Any) -> List[str]:
    names = getattr(model, "feature_names_in_", None)
    if names is not None:
        return [str(x) for x in list(names)]
    steps = getattr(model, "named_steps", None)
    if isinstance(steps, dict):
        for step in reversed(list(steps.values())):
            step_names = getattr(step, "feature_names_in_", None)
            if step_names is not None:
                return [str(x) for x in list(step_names)]
    return []


def _validate_prop(
    *,
    prop_type: str,
    min_overlap_pct: float,
    require_fitted: bool,
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "prop_type": prop_type,
        "ok": True,
        "failures": [],
        "models": {},
    }
    artifact_meta = _load_artifact_meta(prop_type)
    expected = list(artifact_meta.get("input_columns") or [])
    expected_source = "artifact_meta_input_columns"
    if not expected:
        expected = get_expected_features(prop_type) or []
        expected_source = "feature_registry"
    row["expected_features_source"] = expected_source
    if artifact_meta:
        row["artifact_training_profile"] = artifact_meta.get("training_profile")
    row["expected_features_count"] = len(expected)
    if not expected:
        row["ok"] = False
        row["failures"].append("missing_expected_features")
    expected_set = set(expected)

    for algo in ("logistic_regression", "random_forest"):
        m_row: Dict[str, Any] = {"algo": algo, "ok": True, "failures": []}
        try:
            model = load_model(prop_type, algo)
            m_row["model_class"] = type(model).__name__
        except Exception as e:
            m_row["ok"] = False
            m_row["failures"].append(f"load_failed:{e}")
            row["ok"] = False
            row["failures"].append(f"{algo}:load_failed")
            row["models"][algo] = m_row
            continue

        if require_fitted:
            try:
                check_is_fitted(model)
            except NotFittedError as e:
                m_row["ok"] = False
                m_row["failures"].append(f"not_fitted:{e}")
            except Exception as e:
                m_row["ok"] = False
                m_row["failures"].append(f"fitted_check_failed:{e}")

        model_features = _feature_names(model)
        m_row["model_features_count"] = len(model_features)
        if expected and model_features:
            model_set = set(model_features)
            overlap = len(expected_set & model_set)
            overlap_pct = (100.0 * overlap / max(1, len(expected_set)))
            m_row["expected_overlap_count"] = overlap
            m_row["expected_overlap_pct"] = round(overlap_pct, 2)
            if overlap_pct < float(min_overlap_pct):
                m_row["ok"] = False
                m_row["failures"].append(
                    f"feature_overlap_below_threshold:{round(overlap_pct,2)}<{float(min_overlap_pct)}"
                )
        elif expected:
            m_row["ok"] = False
            m_row["failures"].append("model_feature_names_unavailable")

        if not m_row["ok"]:
            row["ok"] = False
            row["failures"].append(f"{algo}:invalid")
        row["models"][algo] = m_row

    row["status"] = "pass" if row["ok"] else "fail"
    return row


def validate(
    *,
    prop_types: Sequence[str],
    min_overlap_pct: float,
    require_fitted: bool,
) -> Dict[str, Any]:
    rows = [
        _validate_prop(
            prop_type=canonicalize_prop_type(p),
            min_overlap_pct=min_overlap_pct,
            require_fitted=require_fitted,
        )
        for p in prop_types
    ]
    failures = [r["prop_type"] for r in rows if not r["ok"]]
    return {
        "ok": not failures,
        "status": "pass" if not failures else "fail",
        "prop_types": [canonicalize_prop_type(p) for p in prop_types],
        "min_feature_overlap_pct": float(min_overlap_pct),
        "require_fitted": bool(require_fitted),
        "failed_props": failures,
        "rows": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate MLB model artifacts for runtime compatibility.")
    ap.add_argument(
        "--prop-types",
        default="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis",
    )
    ap.add_argument("--min-feature-overlap-pct", type=float, default=70.0)
    ap.add_argument("--allow-unfitted", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    props = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    payload = validate(
        prop_types=props,
        min_overlap_pct=float(args.min_feature_overlap_pct),
        require_fitted=not bool(args.allow_unfitted),
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
