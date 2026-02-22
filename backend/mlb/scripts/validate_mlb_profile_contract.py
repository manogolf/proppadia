#!/usr/bin/env python3
"""
Validate MLB player-profile API response contract.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from backend.shared.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient

def _is_intish(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    try:
        int(v)
        return True
    except Exception:
        return False


def _is_number(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    try:
        float(v)
        return True
    except Exception:
        return False


def _is_dateish(v: Any) -> bool:
    if isinstance(v, date):
        return True
    if v is None:
        return False
    try:
        date.fromisoformat(str(v))
        return True
    except Exception:
        return False


def _fail(msg: str) -> int:
    print(f"FAIL {msg}")
    return 1


def _validate_optional_str(row: Dict[str, Any], key: str) -> Optional[str]:
    val = row.get(key)
    if val is None:
        return None
    if not isinstance(val, str):
        return f"{key} should be string when present"
    return None


def validate_profile_contract(body: Dict[str, Any], expected_player_id: int) -> Optional[str]:
    required_top = {
        "player_info": dict,
        "streaks": list,
        "recent_props": list,
        "stat_derived": list,
        "training_summary": list,
        "season_stats": dict,
        "career_stats": dict,
    }
    for key, typ in required_top.items():
        if key not in body:
            return f"missing top-level key: {key}"
        if not isinstance(body[key], typ):
            return f"{key} should be {typ.__name__}"

    info = body["player_info"]
    if not isinstance(info, dict):
        return "player_info should be object"
    pid = info.get("player_id")
    if pid is not None:
        if not _is_intish(pid):
            return "player_info.player_id should be integer when present"
        if int(pid) != int(expected_player_id):
            return f"player_info.player_id mismatch: got {pid}, expected {expected_player_id}"
    err = _validate_optional_str(info, "player_name")
    if err:
        return f"player_info.{err}"
    err = _validate_optional_str(info, "team")
    if err:
        return f"player_info.{err}"
    if info.get("team_id") is not None and not _is_intish(info.get("team_id")):
        return "player_info.team_id should be integer when present"

    for i, row in enumerate(body["streaks"][:100]):
        if not isinstance(row, dict):
            return f"streaks[{i}] should be object"
        if row.get("prop_type") is not None and not isinstance(row.get("prop_type"), str):
            return f"streaks[{i}].prop_type should be string when present"
        if row.get("streak_type") is not None and not isinstance(row.get("streak_type"), str):
            return f"streaks[{i}].streak_type should be string when present"
        if row.get("streak_count") is not None and not _is_intish(row.get("streak_count")):
            return f"streaks[{i}].streak_count should be integer when present"

    for i, row in enumerate(body["recent_props"][:100]):
        if not isinstance(row, dict):
            return f"recent_props[{i}] should be object"
        if row.get("game_date") is not None and not _is_dateish(row.get("game_date")):
            return f"recent_props[{i}].game_date should be date-like when present"
        if row.get("prop_type") is not None and not isinstance(row.get("prop_type"), str):
            return f"recent_props[{i}].prop_type should be string when present"
        if row.get("over_under") is not None and str(row.get("over_under")).lower() not in {"over", "under"}:
            return f"recent_props[{i}].over_under should be over|under when present"
        if row.get("prop_value") is not None and not _is_number(row.get("prop_value")):
            return f"recent_props[{i}].prop_value should be numeric when present"
        if row.get("confidence_score") is not None and not _is_number(row.get("confidence_score")):
            return f"recent_props[{i}].confidence_score should be numeric when present"

    for i, row in enumerate(body["stat_derived"][:100]):
        if not isinstance(row, dict):
            return f"stat_derived[{i}] should be object"
        if row.get("game_date") is not None and not _is_dateish(row.get("game_date")):
            return f"stat_derived[{i}].game_date should be date-like when present"
        if row.get("prop_type") is not None and not isinstance(row.get("prop_type"), str):
            return f"stat_derived[{i}].prop_type should be string when present"

    for i, row in enumerate(body["training_summary"][:100]):
        if not isinstance(row, dict):
            return f"training_summary[{i}] should be object"
        if row.get("prop_type") is not None and not isinstance(row.get("prop_type"), str):
            return f"training_summary[{i}].prop_type should be string when present"
        if row.get("count") is not None and not _is_intish(row.get("count")):
            return f"training_summary[{i}].count should be integer when present"

    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate MLB /api/player-profile contract")
    ap.add_argument("--base-url", default="", help="Optional base URL, e.g. http://127.0.0.1:8001")
    ap.add_argument("--player-id", type=int, default=660271)
    args = ap.parse_args()

    client: ClientAdapter = HttpClient(args.base_url, timeout=25) if args.base_url else InProcessClient()
    path = f"/api/player-profile/{args.player_id}"
    status, body = client.get_json(path)
    if status != 200:
        return _fail(f"{path} returned {status}: {body}")
    if not isinstance(body, dict):
        return _fail(f"{path} expected object response, got {type(body).__name__}")

    err = validate_profile_contract(body, args.player_id)
    if err:
        return _fail(err)

    print("PASS player-profile contract validation")
    print(
        "Rows:"
        f" streaks={len(body.get('streaks') or [])}"
        f" recent_props={len(body.get('recent_props') or [])}"
        f" stat_derived={len(body.get('stat_derived') or [])}"
        f" training_summary={len(body.get('training_summary') or [])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
