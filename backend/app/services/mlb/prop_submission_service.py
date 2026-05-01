"""MLB prepare/predict/add prop application services."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List

from backend.domains.mlb.prop_workflow import add_prop_from_commit, predict_prop, prepare_prop
from backend.domains.mlb.repository.prop_repository import (
    count_model_training_prop_history_rows,
    count_prop_history_rows,
    fetch_model_training_prop_history_rows,
    fetch_prop_history_rows,
    fetch_streak_dashboard_rows,
)

logger = logging.getLogger(__name__)


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
    user_id = str(payload.get("user_id") or "").strip() or None
    return add_prop_from_commit(commit_token=commit_token, prop_source=prop_source, user_id=user_id)


def _to_json_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def get_prop_history(payload: Dict[str, Any]) -> Dict[str, Any]:
    limit = int(payload.get("limit") or 50)
    offset = int(payload.get("offset") or 0)
    user_id = str(payload.get("user_id") or "").strip() or None
    from_date = str(payload.get("from_date") or "").strip() or None
    to_date = str(payload.get("to_date") or "").strip() or None
    prop_source = str(payload.get("prop_source") or "").strip() or None
    status = str(payload.get("status") or "").strip() or None

    rows = fetch_prop_history_rows(
        limit=limit,
        offset=offset,
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    total = count_prop_history_rows(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {k: _to_json_scalar(v) for k, v in row.items()}
        if normalized.get("id") is not None:
            normalized["id"] = str(normalized["id"])
        if normalized.get("user_id") is not None:
            normalized["user_id"] = str(normalized["user_id"])
        out_rows.append(normalized)
    return {
        "ok": True,
        "count": len(out_rows),
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "rows": out_rows,
    }


def get_model_training_prop_history(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Current DB-backed MLB prop history sourced from model_training_props."""
    limit = int(payload.get("limit") or 50)
    offset = int(payload.get("offset") or 0)
    from_date = str(payload.get("from_date") or "").strip() or None
    to_date = str(payload.get("to_date") or "").strip() or None
    prop_source = str(payload.get("prop_source") or "").strip() or "mlb_api"
    status = str(payload.get("status") or "").strip() or None

    rows = fetch_model_training_prop_history_rows(
        limit=limit,
        offset=offset,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    total = count_model_training_prop_history_rows(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {k: _to_json_scalar(v) for k, v in row.items()}
        if normalized.get("id") is not None:
            normalized["id"] = str(normalized["id"])
        out_rows.append(normalized)
    return {
        "ok": True,
        "count": len(out_rows),
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "source": "mlb.model_training_props",
        "rows": out_rows,
    }


def get_streak_dashboard(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Current player-level MLB streak context sourced from model_training_props."""
    from_date = str(payload.get("from_date") or "").strip() or None
    to_date = str(payload.get("to_date") or "").strip() or None
    prop_source = str(payload.get("prop_source") or "").strip() or "mlb_api"
    limit_per_side = int(payload.get("limit_per_side") or 5)

    started = time.perf_counter()
    rows = fetch_streak_dashboard_rows(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        limit_per_side=limit_per_side,
    )
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    logger.info(
        "mlb_streak_dashboard source=mlb.model_training_props from_date=%s to_date=%s rows=%s elapsed_ms=%s",
        from_date,
        to_date,
        len(rows),
        elapsed_ms,
    )
    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = {k: _to_json_scalar(v) for k, v in row.items()}
        if normalized.get("player_id") is not None:
            normalized["player_id"] = int(normalized["player_id"])
        out_rows.append(normalized)

    hot = [row for row in out_rows if row.get("streak_side") == "HOT"]
    cold = [row for row in out_rows if row.get("streak_side") == "COLD"]
    return {
        "ok": True,
        "count": len(out_rows),
        "source": "mlb.model_training_props",
        "elapsed_ms": elapsed_ms,
        "limit_per_side": max(1, min(limit_per_side, 20)),
        "hot": hot,
        "cold": cold,
        "rows": out_rows,
    }
