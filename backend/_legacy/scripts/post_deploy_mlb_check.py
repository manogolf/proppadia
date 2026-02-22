#!/usr/bin/env python3
"""
Fast post-deploy MLB checks against a running backend URL.

This is intentionally lightweight and safe:
- no DB write operations
- validates core MLB API health and contract-critical endpoints
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, List

from backend.scripts.check_output_utils import print_check_rows, print_summary, print_warn_rows
from backend.scripts.check_validators import expect_ok, expect_ping_sport, expect_predict_probability_and_token
from backend.scripts.http_check_utils import CheckResult, HttpClient, run_check
from backend.scripts.sparse_warning_utils import find_sparse_warnings
from backend.scripts.strict_data_gate import enforce_strict_data_gate

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


def run(
    base_url: str,
    *,
    date: str,
    player_id: int,
    search_q: str,
    require_data: bool,
    allow_sparse: bool,
) -> int:
    client = HttpClient(base_url)
    checks: List[CheckResult] = []

    checks.append(
        run_check(
            client, name="health", method="GET", path="/api/health", expected_status=[200], validate=expect_ok
        )
    )
    checks.append(
        run_check(
            client,
            name="mlb_ping",
            method="GET",
            path="/api/mlb/ping",
            expected_status=[200],
            validate=expect_ping_sport("mlb"),
        )
    )
    checks.append(
        run_check(
            client,
            name="mlb_standings",
            method="GET",
            path="/api/mlb/standings",
            params={"season": 2025},
            expected_status=[200],
            validate=lambda body: (
                bool(
                    isinstance(body, dict)
                    and body.get("ok") is True
                    and isinstance(body.get("records"), list)
                    and body.get("source") in {"upstream", "cache", "stale_cache"}
                    and isinstance(body.get("cached_at"), str)
                    and isinstance(body.get("stale"), bool)
                ),
                "expects ok=true,records[],source,cached_at,stale",
            ),
        )
    )
    checks.append(
        run_check(
            client,
            name="market_supported",
            method="GET",
            path="/api/mlb/market-supported-props",
            expected_status=[200],
            validate=lambda body: (
                bool(isinstance(body, dict) and body.get("ok") is True and int(body.get("count") or 0) > 0),
                "expects ok=true,count>0",
            ),
        )
    )
    checks.append(
        run_check(
            client,
            name="market_cache_status",
            method="GET",
            path="/api/mlb/market-cache-status",
            expected_status=[200],
            validate=lambda body: (
                bool(
                    isinstance(body, dict)
                    and body.get("ok") is True
                    and isinstance(body.get("entries"), list)
                    and body.get("ttl_seconds") is not None
                ),
                "expects ok=true,entries[],ttl_seconds",
            ),
        )
    )
    checks.append(
        run_check(
            client,
            name="mlb_ping_db",
            method="GET",
            path="/api/mlb/ping-db",
            expected_status=[200],
            validate=expect_ok,
        )
    )
    checks.append(
        run_check(
            client,
            name="players_lookup",
            method="GET",
            path="/api/players/lookup",
            params={"player_id": player_id},
            expected_status=[200],
        )
    )
    checks.append(
        run_check(
            client,
            name="players_search",
            method="GET",
            path="/api/players/search",
            params={"q": search_q, "limit": 5},
            expected_status=[200],
        )
    )
    checks.append(
        run_check(
            client,
            name="players_list",
            method="GET",
            path="/api/players",
            params={"limit": 5},
            expected_status=[200],
        )
    )
    checks.append(
        run_check(
            client,
            name="mlb_players_list",
            method="GET",
            path="/api/mlb/players",
            params={"limit": 5},
            expected_status=[200],
            validate=lambda body: (isinstance(body, list), "expects list payload"),
        )
    )
    checks.append(
        run_check(
            client,
            name="player_profile",
            method="GET",
            path=f"/api/player-profile/{player_id}",
            expected_status=[200],
        )
    )
    checks.append(
        run_check(
            client,
            name="predict",
            method="POST",
            path="/api/predict",
            json=build_predict_payload(player_id, date),
            expected_status=[200],
            validate=expect_predict_probability_and_token,
        )
    )
    checks.append(
        run_check(
            client,
            name="props_add_invalid_token",
            method="POST",
            path="/api/props/add",
            json={"prop_source": "post_deploy", "commit_token": "bad.token"},
            expected_status=[400],
        )
    )

    passes = sum(1 for c in checks if c.ok)
    warns: List[str] = []
    if require_data:
        warns = find_sparse_warnings(
            checks,
            [
                ("players_lookup", "missing", '"found": true', "players_lookup returned found=false"),
                ("players_search", "contains", '"count": 0', "players_search returned count=0"),
                ("players_list", "contains", "list(len=0)", "players_list returned list(len=0)"),
                ("mlb_players_list", "contains", "list(len=0)", "mlb_players_list returned list(len=0)"),
                ("player_profile", "contains", '"player_name": null', "player_profile returned sparse player_info"),
            ],
        )

    print_check_rows(checks, name_width=24, path_width=30)
    print_warn_rows(warns, label="data-richness")
    print_summary(passed=passes, total=len(checks))
    if passes != len(checks):
        return 1
    return enforce_strict_data_gate(require_data=require_data, allow_sparse=allow_sparse, warns=warns)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fast post-deploy MLB checks")
    ap.add_argument("--base-url", required=True, help="Running backend URL, e.g. https://baseball-streaks-sq44.onrender.com")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--player-id", type=int, default=660271)
    ap.add_argument("--search-q", default="Judge")
    ap.add_argument(
        "--require-data",
        action="store_true",
        help="Fail if player lookup/search/profile are sparse for the probe player/query",
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
        player_id=args.player_id,
        search_q=args.search_q,
        require_data=args.require_data,
        allow_sparse=args.allow_sparse,
    )


if __name__ == "__main__":
    raise SystemExit(main())
