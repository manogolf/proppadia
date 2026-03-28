"""Shared HTTP and check-runner helpers for post-deploy scripts."""

from __future__ import annotations

from dataclasses import dataclass
import time
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
    retry_attempts: int = 0,
    retry_backoff_seconds: float = 1.0,
    retry_statuses: Sequence[int] | None = None,
    validate=None,
    **kwargs,
) -> CheckResult:
    expected = set(expected_status)
    retryable = set(retry_statuses or [429, 502, 503, 504])
    retries = max(0, int(retry_attempts))
    backoff = max(0.0, float(retry_backoff_seconds))

    last_status = 0
    last_detail = "request failed"

    for attempt in range(retries + 1):
        try:
            resp = client.request(method, path, **kwargs)
        except Exception as e:
            last_status = 0
            last_detail = f"{type(e).__name__}: {e}"
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            return CheckResult(name, method, path, last_status, False, last_detail)

        body = safe_json(resp)
        ok = resp.status_code in expected
        detail = first_keys(body)
        if ok and validate is not None:
            try:
                ok, extra = validate(body)
                if extra:
                    detail = f"{detail} | {extra}"
            except Exception as e:
                ok = False
                detail = f"{detail} | validator error: {type(e).__name__}: {e}"

        if ok:
            return CheckResult(name, method, path, resp.status_code, True, detail)

        last_status = resp.status_code
        last_detail = detail
        if attempt < retries and resp.status_code in retryable:
            time.sleep(backoff * (attempt + 1))
            continue
        break

    return CheckResult(name, method, path, last_status, False, last_detail)
