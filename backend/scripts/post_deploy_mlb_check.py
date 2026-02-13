#!/usr/bin/env python3
"""
Fast post-deploy MLB checks against a running backend URL.

This is intentionally lightweight and safe:
- no DB write operations
- validates core MLB API health and contract-critical endpoints
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence


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


def _validate_ping(body: Any):
    if not isinstance(body, dict):
        return False, "ping body is not object"
    return body.get("ok") is True and body.get("sport") == "mlb", "expects ok=true,sport=mlb"


def _validate_predict(body: Any):
    if not isinstance(body, dict):
        return False, "predict body is not object"
    has_prob = isinstance(body.get("probability"), (int, float))
    has_token = isinstance(body.get("commit_token"), str) and "." in body.get("commit_token", "")
    return bool(has_prob and has_token), "expects probability + commit_token"


def build_predict_payload(player_id: int, game_date: str) -> Dict[str, Any]:
    return {
        "prop_type": "hits",
        "features": {
            "player_id": int(player_id),
            "player_name": "Post Deploy Check",
            "team_id": 119,
            "team": "LAD",
            "game_id": 123456,
            "game_date": game_date,
            "prop_type": "hits",
            "prop_value": 1.5,
            "over_under": "over",
            "is_home": True,
            "line_diff": 0.1,
            "hit_streak": 0,
            "win_streak": 0,
            "opponent_encoded": 147,
            "game_day_of_week": 1,
            "time_of_day_bucket": "evening",
            "rolling_result_avg_7": 0.0,
        },
    }


def run(base_url: str, *, date: str, player_id: int, search_q: str) -> int:
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
            name="mlb_ping",
            method="GET",
            path="/api/mlb/ping",
            expected_status=[200],
            validate=_validate_ping,
        )
    )
    checks.append(
        _run_check(
            client,
            name="players_lookup",
            method="GET",
            path="/api/players/lookup",
            params={"player_id": player_id},
            expected_status=[200],
        )
    )
    checks.append(
        _run_check(
            client,
            name="players_search",
            method="GET",
            path="/api/players/search",
            params={"q": search_q, "limit": 5},
            expected_status=[200],
        )
    )
    checks.append(
        _run_check(
            client,
            name="player_profile",
            method="GET",
            path=f"/api/player-profile/{player_id}",
            expected_status=[200],
        )
    )
    checks.append(
        _run_check(
            client,
            name="predict",
            method="POST",
            path="/api/predict",
            json=build_predict_payload(player_id, date),
            expected_status=[200],
            validate=_validate_predict,
        )
    )
    checks.append(
        _run_check(
            client,
            name="props_add_invalid_token",
            method="POST",
            path="/api/props/add",
            json={"prop_source": "post_deploy", "commit_token": "bad.token"},
            expected_status=[400],
        )
    )

    passes = sum(1 for c in checks if c.ok)
    for c in checks:
        state = "PASS" if c.ok else "FAIL"
        print(f"{state} {c.name:24s} {c.method:4s} {c.path:30s} status={c.status} detail={c.detail}")
    print(f"\nSummary: {passes}/{len(checks)} passed")
    return 0 if passes == len(checks) else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Fast post-deploy MLB checks")
    ap.add_argument("--base-url", required=True, help="Running backend URL, e.g. https://baseball-streaks-sq44.onrender.com")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--player-id", type=int, default=660271)
    ap.add_argument("--search-q", default="Judge")
    args = ap.parse_args()
    return run(args.base_url, date=args.date, player_id=args.player_id, search_q=args.search_q)


if __name__ == "__main__":
    raise SystemExit(main())
