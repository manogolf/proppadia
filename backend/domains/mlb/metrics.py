"""MLB metrics domain queries."""

from __future__ import annotations

from typing import Any, Dict, List

from backend.domains.mlb.repository.metrics_repository import (
    get_model_accuracy_rows,
    get_model_accuracy_weekly_rows,
    get_user_vs_model_accuracy_rows,
    get_user_vs_model_accuracy_weekly_rows,
)


def get_user_vs_model_accuracy() -> List[Dict[str, Any]]:
    return get_user_vs_model_accuracy_rows()


def get_model_accuracy_metrics() -> List[Dict[str, Any]]:
    return get_model_accuracy_rows()


def get_user_vs_model_accuracy_weekly() -> List[Dict[str, Any]]:
    return get_user_vs_model_accuracy_weekly_rows()


def get_model_accuracy_weekly() -> List[Dict[str, Any]]:
    return get_model_accuracy_weekly_rows()
