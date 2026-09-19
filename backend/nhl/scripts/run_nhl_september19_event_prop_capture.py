#!/usr/bin/env python3
"""Create-only event-specific NHL player-prop capture for September 19."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from backend.nhl.cross_market_shadow.core import team_code


SLATE_DATE = "2026-09-19"
RUN_TYPE = "SEPTEMBER_19_PRESEASON_CATCHUP"
SPORT = "icehockey_nhl"
REGIONS = ("us", "us2")
MARKETS = (
    "player_shots_on_goal",
    "player_shots_on_goal_alternate",
    "player_points",
    "player_total_saves",
)
FAMILIES = {
    "SOG": {"player_shots_on_goal", "player_shots_on_goal_alternate"},
    "POINTS": {"player_points"},
    "SAVES": {"player_total_saves"},
}
PRIOR_CONFIRMED_CREDITS = 4
TOTAL_AUTHORIZED_CREDITS = 60
PRIOR_CONFIRMED_USED_HEADER = 48929


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def create_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def create_json(path: Path, value: Any) -> None:
    create_bytes(path, (canonical(value) + "\n").encode())


def safe_headers(response: requests.Response) -> dict[str, str | None]:
    return {
        "x_requests_last": response.headers.get("x-requests-last"),
        "x_requests_used": response.headers.get("x-requests-used"),
        "x_requests_remaining": response.headers.get("x-requests-remaining"),
    }


def count_family(event: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    by_book: dict[str, dict[str, int]] = {}
    market_objects = outcomes = 0
    for book in event.get("bookmakers", []) or []:
        book_key = str(book.get("key") or "UNKNOWN")
        for market in book.get("markets", []) or []:
            key = str(market.get("key") or "")
            if key not in keys:
                continue
            size = len(market.get("outcomes", []) or [])
            market_objects += 1
            outcomes += size
            by_book.setdefault(book_key, {})[key] = size
    return {
        "book_count": len(by_book),
        "market_objects": market_objects,
        "outcome_rows": outcomes,
        "by_book_market_outcomes": by_book,
    }


def bind_events(events: list[dict[str, Any]], games: pd.DataFrame) -> list[dict[str, Any]]:
    schedule = games.copy()
    schedule["scheduled_start_time_utc"] = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True)
    bound = []
    for event in events:
        commence = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        home, away = team_code(event.get("home_team")), team_code(event.get("away_team"))
        candidates = schedule[
            schedule.home_team.eq(home)
            & schedule.away_team.eq(away)
            & schedule.scheduled_start_time_utc.sub(commence).abs().le(pd.Timedelta(minutes=15))
        ] if pd.notna(commence) else schedule.iloc[0:0]
        if len(candidates) == 1:
            game = candidates.iloc[0]
            bound.append({
                "canonical_game_id": int(game.game_id),
                "provider_event_id": str(event["id"]),
                "home_team": home,
                "away_team": away,
                "canonical_start_utc": game.scheduled_start_time_utc.isoformat(),
                "provider_start_utc": str(event.get("commence_time")),
            })
    if len(bound) != len({row["canonical_game_id"] for row in bound}):
        raise RuntimeError("DUPLICATE_PROVIDER_EVENT_BINDING")
    return sorted(bound, key=lambda row: row["canonical_game_id"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--game-spine", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    key = os.environ.get("ODDS_API_KEY", "").strip()
    if not key:
        raise SystemExit("ODDS_API_CREDENTIAL_MISSING_FAIL_CLOSED")
    root = args.output_root.resolve()
    if root.exists():
        raise SystemExit("CAPTURE_ROOT_ALREADY_EXISTS_DUPLICATE_BLOCKED")
    root.mkdir(parents=True, mode=0o700)
    games = pd.read_csv(args.game_spine)
    if len(games) != 7 or games.game_id.duplicated().any() or not games.slate_date.astype(str).eq(SLATE_DATE).all():
        raise SystemExit("CANONICAL_GAME_SPINE_FAILURE")
    create_bytes(root / "canonical_game_spine.csv", args.game_spine.read_bytes())

    events_started = utc()
    create_json(root / "claims/events.json", {
        "run_type": RUN_TYPE, "slate_date": SLATE_DATE,
        "claim_timestamp_utc": events_started, "endpoint": f"/v4/sports/{SPORT}/events",
        "quota_cost_contract": 0,
    })
    events_response = requests.get(
        f"https://api.the-odds-api.com/v4/sports/{SPORT}/events",
        params={"apiKey": key, "dateFormat": "iso"}, timeout=45,
    )
    events_completed = utc()
    events_body = events_response.content
    create_bytes(root / "raw/events_response.body", events_body)
    create_json(root / "raw/events_response.metadata.json", {
        "request_start_timestamp_utc": events_started,
        "response_timestamp_utc": events_completed,
        "http_status": events_response.status_code,
        "headers": safe_headers(events_response),
        "body_sha256": hashlib.sha256(events_body).hexdigest(),
        "credential_persisted": False,
    })
    events_response.raise_for_status()
    events = events_response.json()
    if not isinstance(events, list):
        raise RuntimeError("EVENT_LIST_SCHEMA_INVALID")
    bindings = bind_events(events, games)
    create_json(root / "event_bindings.json", {
        "binding_timestamp_utc": events_completed,
        "canonical_game_count": len(games),
        "provider_event_count": len(events),
        "bound_count": len(bindings),
        "bindings": bindings,
        "missing_canonical_game_ids": sorted(set(games.game_id.astype(int)) - {x["canonical_game_id"] for x in bindings}),
    })

    event_used = int(events_response.headers.get("x-requests-used", PRIOR_CONFIRMED_USED_HEADER))
    rejected_bulk_confirmed_credits = max(event_used - PRIOR_CONFIRMED_USED_HEADER, 0)
    running_credits = PRIOR_CONFIRMED_CREDITS + rejected_bulk_confirmed_credits
    results = []
    for binding in bindings:
        estimated = len(REGIONS) * len(MARKETS)
        if running_credits + estimated > TOTAL_AUTHORIZED_CREDITS:
            results.append({**binding, "status": "NOT_REQUESTED_CREDIT_CAP"})
            continue
        game_id = binding["canonical_game_id"]
        event_id = binding["provider_event_id"]
        claim = root / "claims/events" / f"{game_id}_{event_id}.json"
        started = utc()
        create_json(claim, {
            "run_type": RUN_TYPE, "slate_date": SLATE_DATE,
            "canonical_game_id": game_id, "provider_event_id": event_id,
            "claim_timestamp_utc": started, "regions": list(REGIONS), "markets": list(MARKETS),
            "estimated_credits": estimated, "automatic_retry_allowed": False,
        })
        response = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds",
            params={
                "apiKey": key, "regions": ",".join(REGIONS), "markets": ",".join(MARKETS),
                "oddsFormat": "american", "dateFormat": "iso",
            }, timeout=45,
        )
        completed = utc()
        body = response.content
        raw_body = root / "raw/events" / f"{game_id}_{event_id}.body"
        create_bytes(raw_body, body)
        headers = safe_headers(response)
        create_json(root / "raw/events" / f"{game_id}_{event_id}.metadata.json", {
            "request_start_timestamp_utc": started, "response_timestamp_utc": completed,
            "canonical_game_id": game_id, "provider_event_id": event_id,
            "http_status": response.status_code, "headers": headers,
            "body_sha256": hashlib.sha256(body).hexdigest(), "credential_persisted": False,
        })
        actual = int(headers["x_requests_last"] or 0)
        running_credits += actual
        if not response.ok:
            create_json(root / "failures" / f"{game_id}_{event_id}.json", {
                "canonical_game_id": game_id, "provider_event_id": event_id,
                "failure_timestamp_utc": completed, "http_status": response.status_code,
                "response_body_path": str(raw_body), "response_body_sha256": hashlib.sha256(body).hexdigest(),
                "headers": headers, "automatic_retry_allowed": False,
            })
            results.append({**binding, "status": "REJECTED_NO_RETRY", "http_status": response.status_code, "credits": actual})
            continue
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"EVENT_ODDS_SCHEMA_INVALID:{game_id}")
        family_counts = {name: count_family(payload, keys) for name, keys in FAMILIES.items()}
        for family, counts in family_counts.items():
            create_json(root / "receipts" / str(game_id) / f"{family.lower()}.json", {
                "schema_version": "nhl_event_prop_family_receipt_v1",
                "run_type": RUN_TYPE, "slate_date": SLATE_DATE,
                "canonical_game_id": game_id, "provider_event_id": event_id,
                "market_family": family, "markets": sorted(FAMILIES[family]),
                "request_start_timestamp_utc": started, "capture_timestamp_utc": completed,
                "raw_body_path": str(raw_body), "raw_body_sha256": hashlib.sha256(body).hexdigest(),
                "headers": headers, "counts": counts,
                "status": "CAPTURED" if counts["market_objects"] else "VALID_EMPTY_MARKET_UNAVAILABLE_NO_RETRY",
            })
        results.append({
            **binding, "status": "CAPTURED", "http_status": response.status_code,
            "request_start_timestamp_utc": started, "capture_timestamp_utc": completed,
            "credits": actual, "family_counts": family_counts,
        })

    summary = {
        "schema_version": "nhl_september19_event_prop_capture_v1",
        "run_type": RUN_TYPE, "slate_date": SLATE_DATE,
        "completed_timestamp_utc": utc(), "canonical_games": len(games),
        "provider_events": len(events), "bound_events": len(bindings),
        "prior_confirmed_credits": PRIOR_CONFIRMED_CREDITS,
        "rejected_bulk_confirmed_credits": rejected_bulk_confirmed_credits,
        "event_specific_confirmed_credits": sum(int(x.get("credits", 0)) for x in results),
        "total_recovery_confirmed_credits": running_credits,
        "authorized_credit_cap": TOTAL_AUTHORIZED_CREDITS,
        "results": results,
    }
    create_json(root / "capture_summary.json", summary)
    files = sorted(path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
    create_bytes(root / "SHA256SUMS", "".join(f"{sha256(path)}  {path.relative_to(root)}\n" for path in files).encode())
    print(root / "capture_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
