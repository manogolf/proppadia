"""MLB metrics application services."""

from __future__ import annotations

from typing import Any, Dict, List

from backend.domains.mlb.metrics import (
    get_model_accuracy_metrics,
    get_model_accuracy_weekly,
    get_user_vs_model_accuracy,
    get_user_vs_model_accuracy_weekly,
)


def fetch_model_metrics() -> List[Dict[str, Any]]:
    return get_model_accuracy_metrics()


def fetch_user_vs_model_metrics() -> List[Dict[str, Any]]:
    return get_user_vs_model_accuracy()


def fetch_user_vs_model_metrics_weekly() -> List[Dict[str, Any]]:
    return get_user_vs_model_accuracy_weekly()


def fetch_model_metrics_weekly() -> List[Dict[str, Any]]:
    return get_model_accuracy_weekly()

