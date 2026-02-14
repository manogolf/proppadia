"""Render deploy/status helpers for Ops endpoints."""

from __future__ import annotations

import os
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
