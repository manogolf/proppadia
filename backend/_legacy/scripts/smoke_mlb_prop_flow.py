#!/usr/bin/env python3
"""
MLB golden-path smoke: prepare -> predict -> props/add.

This check is DB-write aware and is safe to rerun:
- first add may save a row
- immediate second add should return duplicate=true
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Optional

from backend.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient, safe_json
from backend.scripts.check_output_utils import print_check_rows, print_summary
from backend.scripts.http_check_utils import CheckResult


def _json_obj(resp) -> Dict[str, Any]:
    body = safe_json(resp)
    return body if isinstance(body, dict) else {"_raw": str(body)}


def _post(client: ClientAdapter, path: str, payload: Dict[str, Any]):
    return client.request("POST", path, json=payload)


def run(client: ClientAdapter, *, player_id: int, team_id: int, game_date: str, prop_source: str) -> int:
    steps: list[CheckResult] = []

    def _record(name: str, path: str, status: int, ok: bool, detail: str) -> int:
        steps.append(CheckResult(name=name, method="POST", path=path, status=status, ok=ok, detail=detail))
        return 1 if not ok else 0

    # 1) prepare
    prep_req = {
        "player_id": int(player_id),
        "team_id": int(team_id),
        "game_date": game_date,
        "prop_type": "hits",
        "prop_value": 1.5,
        "over_under": "over",
    }
    prep_resp = _post(client, "/api/prepareProp", prep_req)
    prep_body = _json_obj(prep_resp)
    if prep_resp.status_code != 200 or not isinstance(prep_body, dict) or not prep_body.get("ok"):
        _record(
            "prepareProp",
            "/api/prepareProp",
            prep_resp.status_code,
            False,
            f"status={prep_resp.status_code} body={json.dumps(prep_body, default=str)}",
        )
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1

    features: Dict[str, Any] = prep_body.get("features") or {}
    game_id = features.get("game_id")
    if game_id in (None, "", 0):
        _record(
            "prepareProp",
            "/api/prepareProp",
            prep_resp.status_code,
            False,
            "missing features.game_id (no writable golden-path add possible in this environment)",
        )
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1
    _record(
        "prepareProp",
        "/api/prepareProp",
        prep_resp.status_code,
        True,
        f"game_id={game_id} team={features.get('team')} player_id={features.get('player_id')}",
    )

    # 2) predict
    pred_req = {"prop_type": "hits", "features": features}
    pred_resp = _post(client, "/api/predict", pred_req)
    pred_body = _json_obj(pred_resp)
    token: Optional[str] = pred_body.get("commit_token")
    if pred_resp.status_code != 200 or not token or "." not in token:
        _record("predict", "/api/predict", pred_resp.status_code, False, f"status={pred_resp.status_code} body={json.dumps(pred_body, default=str)}")
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1
    _record("predict", "/api/predict", pred_resp.status_code, True, f"model={pred_body.get('model')} probability={pred_body.get('probability')}")

    # 3) add
    add_req = {"prop_source": prop_source, "commit_token": token}
    add_resp = _post(client, "/api/props/add", add_req)
    add_body = _json_obj(add_resp)
    if add_resp.status_code != 200 or not add_body.get("ok"):
        _record("props/add", "/api/props/add", add_resp.status_code, False, f"status={add_resp.status_code} body={json.dumps(add_body, default=str)}")
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1

    saved = bool(add_body.get("saved"))
    duplicate = bool(add_body.get("duplicate"))
    if not (saved or duplicate):
        _record("props/add", "/api/props/add", add_resp.status_code, False, f"unexpected save state body={json.dumps(add_body, default=str)}")
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1
    _record("props/add", "/api/props/add", add_resp.status_code, True, f"saved={saved} duplicate={duplicate}")

    # 4) duplicate behavior on immediate replay
    add2_resp = _post(client, "/api/props/add", add_req)
    add2_body = _json_obj(add2_resp)
    add2_dup = bool(add2_body.get("duplicate"))
    if add2_resp.status_code != 200 or not add2_dup:
        _record("props/add replay", "/api/props/add", add2_resp.status_code, False, f"status={add2_resp.status_code} body={json.dumps(add2_body, default=str)}")
        total, failed = print_check_rows(steps, name_width=20, path_width=24)
        print_summary(passed=total - failed, total=total)
        return 1
    _record("props/add replay", "/api/props/add", add2_resp.status_code, True, "duplicate=true")
    total, failed = print_check_rows(steps, name_width=20, path_width=24)
    print_summary(passed=total - failed, total=total)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="MLB golden-path smoke (prepare -> predict -> add)")
    ap.add_argument("--base-url", default=None, help="Use running backend URL instead of in-process app")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--team-id", type=int, default=119, help="Defaults to LAD for player_id 660271")
    ap.add_argument("--player-id", type=int, default=660271)
    ap.add_argument("--prop-source", default="smoke_test")
    args = ap.parse_args()

    client: ClientAdapter = HttpClient(args.base_url) if args.base_url else InProcessClient()
    return run(
        client,
        player_id=args.player_id,
        team_id=args.team_id,
        game_date=args.date,
        prop_source=args.prop_source,
    )


if __name__ == "__main__":
    raise SystemExit(main())
