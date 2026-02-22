"""Shared API client adapters for script usage (in-process and remote HTTP)."""

from __future__ import annotations

import json
from typing import Any, Tuple


def safe_json(resp) -> Any:
    try:
        return resp.json()
    except Exception:
        return getattr(resp, "text", "")[:400]


def first_keys(payload: Any, *, max_keys: int = 8) -> str:
    if isinstance(payload, dict):
        keys = list(payload.keys())[:max_keys]
        mini = {k: payload.get(k) for k in keys}
        return json.dumps(mini, default=str)
    if isinstance(payload, list):
        return f"list(len={len(payload)})"
    return str(payload)


class ClientAdapter:
    def request(self, method: str, path: str, **kwargs):
        raise NotImplementedError

    def get_json(self, path: str) -> Tuple[int, Any]:
        resp = self.request("GET", path)
        return resp.status_code, safe_json(resp)


class InProcessClient(ClientAdapter):
    def __init__(self):
        from fastapi.testclient import TestClient
        from backend.app.api_server import app

        self._client = TestClient(app)

    def request(self, method: str, path: str, **kwargs):
        return self._client.request(method, path, **kwargs)


class HttpClient(ClientAdapter):
    def __init__(self, base_url: str, *, timeout: int = 20):
        import requests

        self._requests = requests
        self._base = base_url.rstrip("/")
        self._timeout = int(timeout)

    def request(self, method: str, path: str, **kwargs):
        return self._requests.request(method, f"{self._base}{path}", timeout=self._timeout, **kwargs)
