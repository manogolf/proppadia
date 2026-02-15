#!/usr/bin/env python3
"""Probe MLB prediction readiness across a sample of players."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Sequence

from backend.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient, safe_json


def _obj(resp) -> Dict[str, Any]:
    body = safe_json(resp)
    return body if isinstance(body, dict) else {"_raw": str(body)}


def _load_players(client: ClientAdapter, limit: int) -> list[Dict[str, Any]]:
    resp = client.request("GET", "/api/players", params={"limit": int(limit)})
    body = safe_json(resp)
    if resp.status_code != 200 or not isinstance(body, list):
        return []
    out: list[Dict[str, Any]] = []
    for row in body:
        if not isinstance(row, dict):
            continue
        pid = row.get("player_id")
        tid = row.get("team_id")
        if pid is None or tid is None:
            continue
        try:
            out.append(
                {
                    "player_id": int(pid),
                    "team_id": int(tid),
                    "player_name": row.get("player_name"),
                }
            )
        except Exception:
            continue
    return out


def run(
    client: ClientAdapter,
    *,
    game_date: str,
    sample_size: int,
    require_min_success: int,
) -> int:
    players = _load_players(client, sample_size)
    attempts = 0
    prepare_success = 0
    predict_success = 0
    failures: list[Dict[str, Any]] = []

    for row in players[: max(1, int(sample_size))]:
        attempts += 1
        prep_req = {
            "player_id": int(row["player_id"]),
            "team_id": int(row["team_id"]),
            "game_date": game_date,
            "prop_type": "hits",
            "prop_value": 1.5,
            "over_under": "over",
        }
        prep_resp = client.request("POST", "/api/prepareProp", json=prep_req)
        prep_body = _obj(prep_resp)
        if prep_resp.status_code != 200 or not prep_body.get("ok"):
            failures.append(
                {
                    "player_id": row.get("player_id"),
                    "stage": "prepareProp",
                    "status": prep_resp.status_code,
                    "detail": prep_body,
                }
            )
            continue
        prepare_success += 1
        features = prep_body.get("features") or {}
        pred_req = {"prop_type": "hits", "features": features}
        pred_resp = client.request("POST", "/api/predict", json=pred_req)
        pred_body = _obj(pred_resp)
        token = pred_body.get("commit_token")
        if pred_resp.status_code != 200 or not token:
            failures.append(
                {
                    "player_id": row.get("player_id"),
                    "stage": "predict",
                    "status": pred_resp.status_code,
                    "detail": pred_body,
                }
            )
            continue
        predict_success += 1

    require = max(0, int(require_min_success))
    ok = predict_success >= require
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "game_date": game_date,
        "sample_requested": int(sample_size),
        "sample_loaded": len(players),
        "attempts": attempts,
        "prepare_success": prepare_success,
        "predict_success": predict_success,
        "require_min_success": require,
        "failure_count": len(failures),
        "failures": failures[:10],
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0 if ok else 1


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Probe MLB prediction readiness (prepare -> predict sample).")
    ap.add_argument("--base-url", default=None, help="Use running backend URL instead of in-process app.")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--sample-size", type=int, default=10)
    ap.add_argument("--require-min-success", type=int, default=1)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    client: ClientAdapter = HttpClient(args.base_url) if args.base_url else InProcessClient()
    return run(
        client,
        game_date=args.date,
        sample_size=args.sample_size,
        require_min_success=args.require_min_success,
    )


if __name__ == "__main__":
    raise SystemExit(main())
