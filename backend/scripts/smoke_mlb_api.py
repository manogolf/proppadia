#!/usr/bin/env python3
"""
MLB API smoke tests for Proppadia.

Modes:
- offline: route/wiring checks only (no external MLB API or DB dependency required)
- full: includes DB-backed metrics and MLB-schedule-backed endpoints

Examples:
  python backend/scripts/smoke_mlb_api.py --mode offline
  python backend/scripts/smoke_mlb_api.py --mode full --date 2025-08-15 --team-id 144 --player-id 660271
  python backend/scripts/smoke_mlb_api.py --mode full --base-url http://127.0.0.1:8001
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class CheckResult:
    name: str
    method: str
    path: str
    status: int
    ok: bool
    detail: str


class ClientAdapter:
    def request(self, method: str, path: str, **kwargs):
        raise NotImplementedError


class InProcessClient(ClientAdapter):
    def __init__(self):
        from fastapi.testclient import TestClient
        from backend.app.api_server import app

        self._client = TestClient(app)

    def request(self, method: str, path: str, **kwargs):
        return self._client.request(method, path, **kwargs)


class HttpClient(ClientAdapter):
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


def _first_keys(payload: Any) -> str:
    if isinstance(payload, dict):
        keys = list(payload.keys())[:8]
        mini = {k: payload.get(k) for k in keys}
        return json.dumps(mini, default=str)
    if isinstance(payload, list):
        return f"list(len={len(payload)})"
    return str(payload)


def _run_check(
    client: ClientAdapter,
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
    detail = _first_keys(body)
    if ok and validate is not None:
        try:
            ok, extra = validate(body)
            if extra:
                detail = f"{detail} | {extra}"
        except Exception as e:
            ok = False
            detail = f"{detail} | validator error: {type(e).__name__}: {e}"
    return CheckResult(name, method, path, resp.status_code, ok, detail)


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
            "player_name": "Smoke Test Player",
            "team_id": 144,
            "team": "ATL",
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


def build_prepare_payload(player_id: int, team_id: int, game_date: str) -> Dict[str, Any]:
    return {
        "player_id": int(player_id),
        "team_id": int(team_id),
        "game_date": game_date,
        "prop_type": "hits",
        "prop_value": 1.5,
        "over_under": "over",
    }


def run(mode: str, client: ClientAdapter, args) -> int:
    results: List[CheckResult] = []

    # Always-on route/wiring checks
    results.append(
        _run_check(client, name="health", method="GET", path="/api/health", expected_status=[200])
    )
    results.append(
        _run_check(client, name="mlb_ping", method="GET", path="/api/mlb/ping", expected_status=[200])
    )
    results.append(
        _run_check(
            client,
            name="players_resolve",
            method="GET",
            path="/api/players/resolve",
            params={"player_id": args.player_id},
            expected_status=[200],
        )
    )
    results.append(
        _run_check(
            client,
            name="players_lookup",
            method="GET",
            path="/api/players/lookup",
            params={"player_id": args.player_id},
            expected_status=[200],
        )
    )
    results.append(
        _run_check(
            client,
            name="players_search",
            method="GET",
            path="/api/players/search",
            params={"q": args.search_q, "limit": 5},
            expected_status=[200],
        )
    )
    results.append(
        _run_check(
            client,
            name="players_list_api",
            method="GET",
            path="/api/players",
            params={"limit": 20},
            expected_status=[200],
        )
    )
    results.append(
        _run_check(
            client,
            name="player_profile_api",
            method="GET",
            path=f"/api/player-profile/{args.player_id}",
            expected_status=[200],
        )
    )
    predict_payload = build_predict_payload(args.player_id, args.date)
    pred = _run_check(
        client,
        name="predict",
        method="POST",
        path="/api/predict",
        json=predict_payload,
        expected_status=[200],
        validate=_validate_predict,
    )
    results.append(pred)

    # In offline mode we only assert token validation behavior (no DB writes).
    results.append(
        _run_check(
            client,
            name="props_add_invalid_token",
            method="POST",
            path="/api/props/add",
            json={"prop_source": "user_added", "commit_token": "bad.token"},
            expected_status=[400],
        )
    )

    if mode == "full":
        results.append(
            _run_check(
                client,
                name="games_context",
                method="GET",
                path="/api/games/context",
                params={"team_id": args.team_id, "for_date": args.date},
                expected_status=[200],
            )
        )
        results.append(
            _run_check(
                client,
                name="prepare_prop",
                method="POST",
                path="/api/prepareProp",
                json=build_prepare_payload(args.player_id, args.team_id, args.date),
                expected_status=[200],
            )
        )
        results.append(
            _run_check(
                client,
                name="model_metrics",
                method="GET",
                path="/api/model-metrics",
                expected_status=[200],
            )
        )
        results.append(
            _run_check(
                client,
                name="user_vs_model",
                method="GET",
                path="/api/user-vs-model-accuracy",
                expected_status=[200],
            )
        )
        results.append(
            _run_check(
                client,
                name="user_vs_model_weekly",
                method="GET",
                path="/api/user-vs-model-accuracy-weekly",
                expected_status=[200],
            )
        )
        results.append(
            _run_check(
                client,
                name="model_weekly",
                method="GET",
                path="/api/model-accuracy-weekly",
                expected_status=[200],
            )
        )

    print(f"MLB smoke mode={mode}")
    failed = 0
    for r in results:
        mark = "PASS" if r.ok else "FAIL"
        if not r.ok:
            failed += 1
        print(
            f"{mark:4} {r.name:24} {r.method:4} {r.path:34} "
            f"status={r.status:<3} detail={r.detail[:180]}"
        )

    print(f"\nSummary: {len(results) - failed}/{len(results)} passed")
    return 1 if failed else 0


def parse_args():
    p = argparse.ArgumentParser(description="Smoke test MLB API endpoints.")
    p.add_argument("--mode", choices=["offline", "full"], default="offline")
    p.add_argument("--base-url", default="", help="Optional HTTP base URL, e.g. http://127.0.0.1:8001")
    p.add_argument("--date", default="2025-08-15", help="Historical date for full mode schedule checks")
    p.add_argument("--team-id", type=int, default=144, help="Team id used by games/context and prepareProp")
    p.add_argument("--player-id", type=int, default=660271, help="Player id used in lookups")
    p.add_argument("--search-q", default="Judge", help="Search query for /api/players/search")
    return p.parse_args()


def main():
    args = parse_args()
    if args.base_url:
        client: ClientAdapter = HttpClient(args.base_url)
    else:
        client = InProcessClient()
    code = run(args.mode, client, args)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
