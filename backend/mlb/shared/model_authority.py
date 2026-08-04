"""Fail-closed authority gate for MLB predictive-model operations."""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTHORITY_PATH = REPO_ROOT / "backend/mlb/config/model_authority.json"
BLOCKED_STATUS = "MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL"


class MLBPredictiveModelBlocked(RuntimeError):
    pass


def authority() -> dict:
    payload = json.loads(AUTHORITY_PATH.read_text(encoding="utf-8"))
    if payload.get("authority_status") != "NO_QUALIFIED_MLB_MODEL":
        raise MLBPredictiveModelBlocked(f"{BLOCKED_STATUS}: invalid or unrecognized authority state")
    return payload


def predictive_model_is_qualified() -> bool:
    return authority().get("authority_status") != "NO_QUALIFIED_MLB_MODEL"


def assert_predictive_model_qualified(operation: str) -> None:
    payload = authority()
    if payload.get("authority_status") == "NO_QUALIFIED_MLB_MODEL":
        raise MLBPredictiveModelBlocked(f"{BLOCKED_STATUS}: operation={operation}")


def status_line() -> str:
    payload = authority()
    return f"{payload['authority_status']} {payload['blocked_runtime_status']}"
