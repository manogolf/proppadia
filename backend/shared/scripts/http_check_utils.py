"""Shared HTTP and check-runner helpers for post-deploy scripts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from backend.shared.scripts.api_client_utils import HttpClient, first_keys, safe_json


@dataclass
class CheckResult:
    name: str
    method: str
    path: str
    status: int
    ok: bool
    detail: str


def run_check(
    client: HttpClient,
    *,
    name: str,
    method: str,
    path: str,
    expected_status: Sequence[int],
    validate=None,
    **kwargs,
) -> CheckResult:
    resp = client.request(method, path, **kwargs)
    body = safe_json(resp)
    ok = resp.status_code in set(expected_status)
    detail = first_keys(body)
    if ok and validate is not None:
        try:
            ok, extra = validate(body)
            if extra:
                detail = f"{detail} | {extra}"
        except Exception as e:
            ok = False
            detail = f"{detail} | validator error: {type(e).__name__}: {e}"
    return CheckResult(name, method, path, resp.status_code, ok, detail)
