"""Render deploy/status/metrics helpers for Ops endpoints."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests

RENDER_API_BASE = "https://api.render.com/v1"
REQUEST_TIMEOUT = 15


def _render_env() -> tuple[str, str]:
    api_key = str(os.getenv("RENDER_API_KEY") or "").strip()
    service_id = str(os.getenv("RENDER_SERVICE_ID") or "").strip()
    if not api_key:
        raise RuntimeError("RENDER_API_KEY is not configured")
    if not service_id:
        raise RuntimeError("RENDER_SERVICE_ID is not configured")
    return api_key, service_id


def _request(
    *,
    method: str,
    path: str,
    api_key: str,
    json_payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    if json_payload is not None:
        headers["Content-Type"] = "application/json"
    resp = requests.request(
        method=method.upper(),
        url=f"{RENDER_API_BASE}{path}",
        headers=headers,
        json=json_payload,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    if not resp.ok:
        text = resp.text[:500]
        raise RuntimeError(f"Render API {resp.status_code}: {text}")
    try:
        return resp.json()
    except Exception:
        return {}


def _normalize_deploy(row: Dict[str, Any]) -> Dict[str, Any]:
    commit = row.get("commit") if isinstance(row.get("commit"), dict) else {}
    return {
        "id": row.get("id"),
        "status": row.get("status"),
        "created_at": row.get("createdAt") or row.get("created_at"),
        "updated_at": row.get("updatedAt") or row.get("updated_at"),
        "finished_at": row.get("finishedAt") or row.get("finished_at"),
        "commit_id": commit.get("id") or row.get("commitId") or row.get("commit_id"),
        "commit_message": commit.get("message") or row.get("commitMessage") or row.get("commit_message"),
        "trigger": row.get("trigger"),
    }


def fetch_latest_deploy() -> Dict[str, Any]:
    api_key, service_id = _render_env()
    payload = _request(
        method="GET",
        path=f"/services/{service_id}/deploys",
        api_key=api_key,
        params={"limit": 1},
    )
    rows = payload if isinstance(payload, list) else []
    latest = rows[0] if rows else {}
    return {
        "ok": True,
        "service_id": service_id,
        "deploy": _normalize_deploy(latest) if latest else None,
    }


def trigger_redeploy(*, clear_cache: bool = False) -> Dict[str, Any]:
    api_key, service_id = _render_env()
    body: Dict[str, Any] = {}
    if clear_cache:
        body["clearCache"] = "clear"
    payload = _request(
        method="POST",
        path=f"/services/{service_id}/deploys",
        api_key=api_key,
        json_payload=body,
    )
    row = payload if isinstance(payload, dict) else {}
    return {
        "ok": True,
        "service_id": service_id,
        "deploy": _normalize_deploy(row),
    }


def _series_stats(rows: Any) -> Dict[str, Any]:
    series = rows if isinstance(rows, list) else []
    values = []
    unit = None
    for item in series:
        if not isinstance(item, dict):
            continue
        if unit is None and isinstance(item.get("unit"), str):
            unit = item.get("unit")
        for point in item.get("values") or []:
            try:
                value = float(point.get("value"))
            except Exception:
                continue
            values.append(
                {
                    "timestamp": point.get("timestamp"),
                    "value": value,
                }
            )

    values.sort(key=lambda x: str(x.get("timestamp") or ""))
    latest = values[-1] if values else None
    raw = [v["value"] for v in values]
    return {
        "points": len(values),
        "latest_value": latest.get("value") if latest else None,
        "latest_at": latest.get("timestamp") if latest else None,
        "min": min(raw) if raw else None,
        "max": max(raw) if raw else None,
        "avg": (sum(raw) / len(raw)) if raw else None,
        "unit": unit,
    }


def _fetch_metric(
    *,
    metric_path: str,
    api_key: str,
    service_id: str,
    start_time: str,
    end_time: str,
    resolution_seconds: int,
) -> Dict[str, Any]:
    rows = _request(
        method="GET",
        path=metric_path,
        api_key=api_key,
        params={
            "resource": service_id,
            "startTime": start_time,
            "endTime": end_time,
            "resolutionSeconds": resolution_seconds,
            "aggregationMethod": "AVG",
        },
    )
    stats = _series_stats(rows)
    return stats


def fetch_service_metrics(*, window_minutes: int = 360, resolution_seconds: int = 60) -> Dict[str, Any]:
    api_key, service_id = _render_env()
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=max(15, min(window_minutes, 24 * 60)))
    start_iso = start.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    cpu = _fetch_metric(
        metric_path="/metrics/cpu",
        api_key=api_key,
        service_id=service_id,
        start_time=start_iso,
        end_time=end_iso,
        resolution_seconds=max(30, min(int(resolution_seconds), 3600)),
    )
    memory = _fetch_metric(
        metric_path="/metrics/memory",
        api_key=api_key,
        service_id=service_id,
        start_time=start_iso,
        end_time=end_iso,
        resolution_seconds=max(30, min(int(resolution_seconds), 3600)),
    )
    return {
        "ok": True,
        "service_id": service_id,
        "window": {
            "start": start_iso,
            "end": end_iso,
            "minutes": max(15, min(window_minutes, 24 * 60)),
            "resolution_seconds": max(30, min(int(resolution_seconds), 3600)),
        },
        "cpu": cpu,
        "memory": memory,
    }
