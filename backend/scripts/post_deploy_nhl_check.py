#!/usr/bin/env python3
"""
Fast post-deploy NHL checks against a running backend URL.

This is intentionally lightweight and safe:
- no DB write operations
- validates core NHL API health and key read endpoints
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, List, Sequence


@dataclass
class CheckResult:
    name: str
    method: str
    path: str
    status: int
    ok: bool
    detail: str


class HttpClient:
    def __init__(self, base_url: str):
        import requests

        self._requests = requests
        self._base = base_url.rstrip("/")

    def request(self, method: str, path: str, **kwargs):
        return self._requests.request(method, f"{self._base}{path}", timeout=20, **kwargs)


def _safe_json(resp) -> Any:
    try:
        return resp.json()
    except Exception:
        txt = getattr(resp, "text", "")
        return txt[:400]


def _mini(payload: Any) -> str:
    if isinstance(payload, dict):
        keys = list(payload.keys())[:8]
        return json.dumps({k: payload.get(k) for k in keys}, default=str)
    if isinstance(payload, list):
        return f"list(len={len(payload)})"
    return str(payload)


def _run_check(
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
    body = _safe_json(resp)
    ok = resp.status_code in set(expected_status)
    detail = _mini(body)
    if ok and validate is not None:
        try:
            ok, extra = validate(body)
            if extra:
                detail = f"{detail} | {extra}"
        except Exception as e:
            ok = False
            detail = f"{detail} | validator error: {type(e).__name__}: {e}"
    return CheckResult(name, method, path, resp.status_code, ok, detail)


def _validate_health(body: Any):
    if not isinstance(body, dict):
        return False, "health body is not object"
    return body.get("ok") is True, "expects ok=true"


def _validate_nhl_ping(body: Any):
    if not isinstance(body, dict):
        return False, "ping body is not object"
    return body.get("ok") is True and body.get("sport") == "nhl", "expects ok=true,sport=nhl"


def _validate_ping_db(body: Any):
    if not isinstance(body, dict):
        return False, "ping-db body is not object"
    return body.get("ok") is True, "expects ok=true"


def _validate_ok_count_rows(body: Any):
    if not isinstance(body, dict):
        return False, "expects object"
    rows = body.get("rows")
    count = body.get("count")
    ok = body.get("ok") is True and isinstance(rows, list) and isinstance(count, int)
    return ok, "expects ok=true,count=int,rows=list"


def _validate_list_or_error(body: Any):
    if isinstance(body, list):
        return True, "expects list payload"
    if isinstance(body, dict) and body.get("ok") is False and isinstance(body.get("error"), str):
        return True, "allows structured db error payload"
    return False, "expects list payload (or {ok:false,error} object)"


def run(base_url: str, *, date: str, require_data: bool, allow_sparse: bool) -> int:
    client = HttpClient(base_url)
    checks: List[CheckResult] = []

    checks.append(
        _run_check(
            client, name="health", method="GET", path="/api/health", expected_status=[200], validate=_validate_health
        )
    )
    checks.append(
        _run_check(
            client,
            name="nhl_ping",
            method="GET",
            path="/api/nhl/ping",
            expected_status=[200],
            validate=_validate_nhl_ping,
        )
    )
    checks.append(
        _run_check(
            client,
            name="nhl_ping_db",
            method="GET",
            path="/api/nhl/ping-db",
            expected_status=[200],
            validate=_validate_ping_db,
        )
    )
    checks.append(
        _run_check(
            client,
            name="games_today",
            method="GET",
            path="/api/nhl/games/today",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=_validate_ok_count_rows,
        )
    )
    checks.append(
        _run_check(
            client,
            name="props_today",
            method="GET",
            path="/api/nhl/props/today",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=_validate_ok_count_rows,
        )
    )
    checks.append(
        _run_check(
            client,
            name="sog",
            method="GET",
            path="/api/nhl/sog",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=_validate_list_or_error,
        )
    )
    checks.append(
        _run_check(
            client,
            name="saves",
            method="GET",
            path="/api/nhl/saves",
            params={"date": date, "limit": 25},
            expected_status=[200],
            validate=_validate_list_or_error,
        )
    )

    passes = sum(1 for c in checks if c.ok)
    warns: List[str] = []
    if require_data:
        games = next((c for c in checks if c.name == "games_today"), None)
        props = next((c for c in checks if c.name == "props_today"), None)
        sog = next((c for c in checks if c.name == "sog"), None)
        saves = next((c for c in checks if c.name == "saves"), None)
        if games and '"count": 0' in games.detail:
            warns.append("games_today returned count=0")
        if props and '"count": 0' in props.detail:
            warns.append("props_today returned count=0")
        if sog and "list(len=0)" in sog.detail:
            warns.append("sog returned list(len=0)")
        if saves and "list(len=0)" in saves.detail:
            warns.append("saves returned list(len=0)")

    for c in checks:
        state = "PASS" if c.ok else "FAIL"
        print(f"{state} {c.name:24s} {c.method:4s} {c.path:30s} status={c.status} detail={c.detail}")
    for w in warns:
        print(f"WARN data-richness            {w}")
    print(f"\nSummary: {passes}/{len(checks)} passed")
    if passes != len(checks):
        return 1
    if require_data and warns:
        if allow_sparse:
            print("PASS strict-data gate         allow-sparse enabled; warnings tolerated")
            return 0
        print("FAIL strict-data gate         sparse probe data; run without --require-data or use --allow-sparse")
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Fast post-deploy NHL checks")
    ap.add_argument("--base-url", required=True, help="Running backend URL, e.g. https://baseball-streaks-sq44.onrender.com")
    ap.add_argument("--date", default="2025-11-20", help="Probe date for NHL data endpoints")
    ap.add_argument(
        "--require-data",
        action="store_true",
        help="Fail if games/props/sog/saves are sparse for the probe date",
    )
    ap.add_argument(
        "--allow-sparse",
        action="store_true",
        help="When used with --require-data, keep warnings but do not fail on sparse probe data",
    )
    args = ap.parse_args()
    return run(
        args.base_url,
        date=args.date,
        require_data=args.require_data,
        allow_sparse=args.allow_sparse,
    )


if __name__ == "__main__":
    raise SystemExit(main())
