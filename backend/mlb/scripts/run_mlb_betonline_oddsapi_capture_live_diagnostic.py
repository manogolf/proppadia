"""Run one bounded live BetOnline OddsAPI MLB player-prop capture diagnostic.

This utility makes live OddsAPI requests only when explicitly invoked. It writes
raw request/response artifacts and validation summaries, but performs no DB
writes and does not alter production predictions, schedules, uploads, or models.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests

from backend.mlb.shared.betonline_market_registry import active_market_rows, market_batches


REPO_ROOT = Path(__file__).resolve().parents[3]
EVENTS_BASE = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events"
ET = ZoneInfo("America/New_York")
DEFAULT_OUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_oddsapi_capture_coverage_repair/2026-07-18/live_diagnostic"
)
STATUS_VALUES = {
    "BETONLINE_BOOK_PRESENT_MARKET_PRESENT",
    "BETONLINE_BOOK_PRESENT_MARKET_ABSENT",
    "BETONLINE_BOOK_ABSENT_FROM_RESPONSE",
    "REQUEST_FAILED",
    "PARSER_DROPPED_ROWS",
    "NO_ELIGIBLE_EVENTS",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "")).strip("_") or "blank"


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def parse_event_date_et(event: dict[str, Any]) -> str:
    raw = str(event.get("commence_time") or "")
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return dt.astimezone(ET).date().isoformat()


def is_betonline(book: dict[str, Any]) -> bool:
    text = f"{book.get('key') or ''} {book.get('title') or ''}".lower()
    return "betonline" in text or str(book.get("key") or "").lower() == "betonlineag"


def response_record(
    *,
    response: requests.Response | None,
    request_id: str,
    endpoint: str,
    event_id: str,
    markets: str,
    bookmaker: str,
    region: str,
    raw_path: Path,
    error: str = "",
) -> dict[str, Any]:
    headers = response.headers if response is not None else {}
    body = response.content if response is not None else str(error).encode()
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(body)
    return {
        "request_id": request_id,
        "request_timestamp_utc": now_utc(),
        "endpoint": endpoint,
        "event_id": event_id,
        "requested_markets": markets,
        "bookmaker": bookmaker,
        "region": region,
        "http_status": response.status_code if response is not None else "",
        "result_status": "PASS" if response is not None and response.ok else "FAIL",
        "quota_requests_used": headers.get("x-requests-used", ""),
        "quota_requests_remaining": headers.get("x-requests-remaining", ""),
        "quota_requests_last": headers.get("x-requests-last", ""),
        "raw_payload_path": rel(raw_path),
        "payload_sha256": sha256_bytes(body),
        "error": error,
    }


def fetch_json(
    *,
    session: requests.Session,
    url: str,
    params: dict[str, str],
    request_id: str,
    endpoint: str,
    event_id: str,
    markets: str,
    bookmaker: str,
    region: str,
    raw_dir: Path,
    request_rows: list[dict[str, Any]],
) -> Any | None:
    raw_path = raw_dir / f"{request_id}.json"
    try:
        response = session.get(url, params=params, timeout=30)
    except Exception as exc:
        request_rows.append(
            response_record(
                response=None,
                request_id=request_id,
                endpoint=endpoint,
                event_id=event_id,
                markets=markets,
                bookmaker=bookmaker,
                region=region,
                raw_path=raw_path,
                error=f"{type(exc).__name__}: {exc}",
            )
        )
        return None
    request_rows.append(
        response_record(
            response=response,
            request_id=request_id,
            endpoint=endpoint,
            event_id=event_id,
            markets=markets,
            bookmaker=bookmaker,
            region=region,
            raw_path=raw_path,
        )
    )
    try:
        return response.json()
    except Exception:
        return None


def analyze_payloads(
    *,
    event_payloads: list[dict[str, Any]],
    events: list[dict[str, Any]],
    request_failed_by_market: dict[str, bool],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    market_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    registry = active_market_rows()
    eligible_events = len(events)
    for market in registry:
        key = market["oddsapi_key"]
        book_present = False
        market_present = False
        games: set[str] = set()
        players: set[str] = set()
        lines: set[str] = set()
        sides: set[str] = set()
        raw_outcomes = 0
        parsed_rows = 0
        for payload in event_payloads:
            if not isinstance(payload, dict):
                continue
            game_id = str(payload.get("id") or "")
            for book in payload.get("bookmakers", []) or []:
                if not isinstance(book, dict) or not is_betonline(book):
                    continue
                book_present = True
                for item in book.get("markets", []) or []:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("key") or "") != key:
                        continue
                    market_present = True
                    games.add(game_id)
                    for outcome in item.get("outcomes", []) or []:
                        if not isinstance(outcome, dict):
                            continue
                        raw_outcomes += 1
                        player = str(outcome.get("description") or "").strip()
                        side = str(outcome.get("name") or "").strip().lower()
                        point = outcome.get("point")
                        price = outcome.get("price")
                        if player and side in {"over", "under"} and point is not None and price is not None:
                            parsed_rows += 1
                            players.add(player)
                            sides.add(side)
                            lines.add(str(point))
                        else:
                            rejected_rows.append(
                                {
                                    "raw_market_key": key,
                                    "game_id": game_id,
                                    "player_or_pitcher": player,
                                    "side": side,
                                    "line": "" if point is None else point,
                                    "price": "" if price is None else price,
                                    "reject_reason": "missing_player_side_line_or_price",
                                }
                            )
        if eligible_events == 0:
            status = "NO_ELIGIBLE_EVENTS"
        elif request_failed_by_market.get(key):
            status = "REQUEST_FAILED"
        elif not book_present:
            status = "BETONLINE_BOOK_ABSENT_FROM_RESPONSE"
        elif not market_present:
            status = "BETONLINE_BOOK_PRESENT_MARKET_ABSENT"
        elif raw_outcomes != parsed_rows:
            status = "PARSER_DROPPED_ROWS"
        else:
            status = "BETONLINE_BOOK_PRESENT_MARKET_PRESENT"
        market_rows.append(
            {
                "local_prop_type": market["local_prop_type"],
                "raw_market_key": key,
                "requested": "yes",
                "bookmaker": "betonlineag",
                "validation_status": status,
                "betonline_present": "yes" if book_present else "no",
                "raw_outcome_rows": raw_outcomes,
                "parsed_rows": parsed_rows,
                "games_covered": len(games),
                "players_or_pitchers_covered": len(players),
                "lines": "|".join(sorted(lines)),
                "side_coverage": "|".join(sorted(sides)),
                "rejected_rows": sum(1 for r in rejected_rows if r.get("raw_market_key") == key),
                "notes": "",
            }
        )
    return market_rows, rejected_rows


def run(date: str, bookmaker: str, output_dir: Path) -> dict[str, Any]:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY missing")
    run_tag = f"betonline_live_diag_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    out_dir = output_dir / run_tag
    raw_dir = out_dir / "raw"
    request_rows: list[dict[str, Any]] = []
    session = requests.Session()

    events_payload = fetch_json(
        session=session,
        url=EVENTS_BASE,
        params={"apiKey": api_key, "dateFormat": "iso"},
        request_id="000_events",
        endpoint="/v4/sports/baseball_mlb/events",
        event_id="",
        markets="",
        bookmaker="",
        region="",
        raw_dir=raw_dir,
        request_rows=request_rows,
    )
    all_events = [ev for ev in events_payload if isinstance(ev, dict)] if isinstance(events_payload, list) else []
    events = [ev for ev in all_events if parse_event_date_et(ev) == date]

    batches = market_batches(max_markets_per_call=6)
    event_payloads: list[dict[str, Any]] = []
    request_failed_by_market: dict[str, bool] = {}
    for ev_idx, ev in enumerate(events, start=1):
        event_id = str(ev.get("id") or "")
        if not event_id:
            continue
        merged: dict[str, Any] | None = None
        for batch_idx, batch in enumerate(batches, start=1):
            markets = batch["market_keys"]
            request_id = f"{ev_idx:03d}_{safe_name(event_id)}_batch_{batch_idx:02d}"
            payload = fetch_json(
                session=session,
                url=f"{EVENTS_BASE}/{event_id}/odds",
                params={
                    "apiKey": api_key,
                    "bookmakers": bookmaker,
                    "markets": markets,
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                    "includeBetLimits": "true",
                },
                request_id=request_id,
                endpoint="/v4/sports/baseball_mlb/events/{event_id}/odds",
                event_id=event_id,
                markets=markets,
                bookmaker=bookmaker,
                region="",
                raw_dir=raw_dir,
                request_rows=request_rows,
            )
            req = request_rows[-1]
            if req.get("result_status") != "PASS":
                for key in markets.split(","):
                    request_failed_by_market[key] = True
            if not isinstance(payload, dict):
                continue
            if merged is None:
                merged = dict(payload)
            else:
                books = merged.setdefault("bookmakers", [])
                if not isinstance(books, list):
                    books = []
                    merged["bookmakers"] = books
                for book in payload.get("bookmakers", []) or []:
                    if not isinstance(book, dict):
                        continue
                    existing = next((b for b in books if isinstance(b, dict) and b.get("key") == book.get("key")), None)
                    if existing is None:
                        books.append(book)
                    else:
                        existing.setdefault("markets", [])
                        existing["markets"].extend(book.get("markets", []) or [])
        if merged is not None:
            event_payloads.append(merged)

    market_rows, rejected_rows = analyze_payloads(
        event_payloads=event_payloads,
        events=events,
        request_failed_by_market=request_failed_by_market,
    )

    paths = {
        "request_manifest": out_dir / "betonline_live_request_manifest_2026-07-18.csv",
        "market_validation": out_dir / "betonline_live_market_validation_2026-07-18.csv",
        "rejected_rows": out_dir / "betonline_live_parser_rejected_rows_2026-07-18.csv",
        "event_summary": out_dir / "betonline_live_event_summary_2026-07-18.csv",
        "decisions": out_dir / "betonline_live_diagnostic_decisions_2026-07-18.csv",
        "summary": out_dir / "betonline_live_diagnostic_summary_2026-07-18.md",
        "machine": out_dir / "machine_readable_betonline_live_diagnostic_2026-07-18.json",
        "sha_manifest": out_dir / "sha256_manifest_2026-07-18.csv",
    }
    write_csv(
        paths["request_manifest"],
        request_rows,
        [
            "request_id",
            "request_timestamp_utc",
            "endpoint",
            "event_id",
            "requested_markets",
            "bookmaker",
            "region",
            "http_status",
            "result_status",
            "quota_requests_used",
            "quota_requests_remaining",
            "quota_requests_last",
            "raw_payload_path",
            "payload_sha256",
            "error",
        ],
    )
    write_csv(
        paths["market_validation"],
        market_rows,
        [
            "local_prop_type",
            "raw_market_key",
            "requested",
            "bookmaker",
            "validation_status",
            "betonline_present",
            "raw_outcome_rows",
            "parsed_rows",
            "games_covered",
            "players_or_pitchers_covered",
            "lines",
            "side_coverage",
            "rejected_rows",
            "notes",
        ],
    )
    write_csv(
        paths["rejected_rows"],
        rejected_rows,
        ["raw_market_key", "game_id", "player_or_pitcher", "side", "line", "price", "reject_reason"],
    )
    event_rows = [
        {
            "game_id": ev.get("id", ""),
            "commence_time": ev.get("commence_time", ""),
            "home_team": ev.get("home_team", ""),
            "away_team": ev.get("away_team", ""),
            "date_et": parse_event_date_et(ev),
        }
        for ev in events
    ]
    write_csv(paths["event_summary"], event_rows, ["game_id", "commence_time", "home_team", "away_team", "date_et"])

    statuses = {row["validation_status"] for row in market_rows}
    all_present = statuses == {"BETONLINE_BOOK_PRESENT_MARKET_PRESENT"}
    all_book_absent = statuses == {"BETONLINE_BOOK_ABSENT_FROM_RESPONSE"} if market_rows else False
    any_request_failed = any(row.get("result_status") != "PASS" for row in request_rows)
    if all_present:
        bookmaker_decision = "BETONLINE_RETURNED_FOR_ALL_REQUESTED_MARKETS"
        live_retention = "LIVE_RETENTION_CERTIFIED_FOR_NINE_MARKETS"
        schedule_activation = "PREPARE_EXISTING_WRAPPER_INTEGRATION_DO_NOT_ENABLE_YET"
    elif all_book_absent:
        bookmaker_decision = "BETONLINE_BOOKMAKER_ABSENT_FROM_ODDSAPI_RESPONSES"
        live_retention = "NOT_CERTIFIED_BOOKMAKER_ENDPOINT_ACCESS_GAP"
        schedule_activation = "DO_NOT_ACTIVATE_SCHEDULED_PATH"
    elif any_request_failed:
        bookmaker_decision = "REQUEST_FAILURE_PREVENTED_FULL_CERTIFICATION"
        live_retention = "NOT_CERTIFIED_REQUEST_FAILURES_PRESENT"
        schedule_activation = "DO_NOT_ACTIVATE_SCHEDULED_PATH"
    else:
        bookmaker_decision = "PARTIAL_BETONLINE_MARKET_RESPONSE"
        live_retention = "PARTIAL_LIVE_RETENTION_NOT_FULLY_CERTIFIED"
        schedule_activation = "DO_NOT_ACTIVATE_SCHEDULED_PATH"

    decisions = {
        "MLB_BETONLINE_CAPTURE_LIVE_REQUEST_DECISION": "ONE_BOUNDED_CURRENT_SLATE_DIAGNOSTIC_EXECUTED",
        "MLB_BETONLINE_CAPTURE_BOOKMAKER_RESPONSE_DECISION": bookmaker_decision,
        "MLB_BETONLINE_CAPTURE_NINE_MARKET_REQUEST_DECISION": "ALL_NINE_GOVERNED_MARKETS_REQUESTED",
        "MLB_BETONLINE_CAPTURE_RAW_RETENTION_DECISION": "EVERY_REQUEST_RESPONSE_PRESERVED_WITH_SHA256",
        "MLB_BETONLINE_CAPTURE_PARSER_LIVE_DECISION": "PARSER_PASS" if all_present else "PARSER_NOT_CERTIFIED_WITHOUT_FULL_MARKET_ROWS",
        "MLB_BETONLINE_CAPTURE_QUOTA_DECISION": "QUOTA_HEADERS_RETAINED_WHEN_RETURNED",
        "MLB_BETONLINE_CAPTURE_HITS_ALLOWED_MODEL_STATUS_DECISION": "TRUE_PRODUCTION_MODEL_AND_SEPARATE_RESEARCH_CHALLENGER",
        "MLB_BETONLINE_CAPTURE_LIVE_RETENTION_DECISION": live_retention,
        "MLB_BETONLINE_CAPTURE_SCHEDULED_ACTIVATION_DECISION": schedule_activation,
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    write_csv(paths["decisions"], [{"decision": k, "value": v, "notes": ""} for k, v in decisions.items()], ["decision", "value", "notes"])

    request_count = len(request_rows)
    quota_last_values = [str(r.get("quota_requests_last") or "") for r in request_rows if str(r.get("quota_requests_last") or "")]
    summary_lines = [
        "# BetOnline OddsAPI Live Diagnostic",
        "",
        f"Run tag: `{run_tag}`",
        f"Generated UTC: `{now_utc()}`",
        f"Slate date: `{date}`",
        f"Bookmaker: `{bookmaker}`",
        "",
        "## Scope",
        "",
        f"- Events checked: `{len(events)}`",
        f"- Request count: `{request_count}`",
        f"- Market batches per event: `{len(batches)}`",
        f"- Region parameter: omitted because bookmaker filter was used",
        f"- Quota last headers observed: `{','.join(quota_last_values)}`",
        "",
        "## Market Status Counts",
        "",
    ]
    for status in sorted(STATUS_VALUES):
        count = sum(1 for row in market_rows if row["validation_status"] == status)
        summary_lines.append(f"- `{status}`: `{count}`")
    summary_lines.extend(
        [
            "",
            "## Direct Answer",
            "",
            (
                "The approved live diagnostic returned BetOnline rows for all nine intended MLB player-prop markets."
                if all_present
                else "The approved live diagnostic did not certify all nine BetOnline markets; see market validation and decisions for the remaining blocker."
            ),
            "",
            "## No Production Change",
            "",
            "No DB writes, model changes, prediction changes, schedule activation, upload changes, or production behavior changes occurred.",
        ]
    )
    paths["summary"].write_text("\n".join(summary_lines) + "\n")
    machine = {
        "run_tag": run_tag,
        "generated_at_utc": now_utc(),
        "date": date,
        "bookmaker": bookmaker,
        "events_checked": len(events),
        "request_count": request_count,
        "market_rows": market_rows,
        "decisions": decisions,
        "production_status": "UNCHANGED",
        "db_writes": False,
        "schedule_changes": False,
        "model_changes": False,
        "raw_dir": rel(raw_dir),
    }
    write_json(paths["machine"], machine)
    sha_rows = []
    for path in sorted(out_dir.glob("**/*")):
        if path.is_file() and path.name != paths["sha_manifest"].name:
            sha_rows.append({"artifact": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(paths["sha_manifest"], sha_rows, ["artifact", "sha256", "bytes"])
    return {"out_dir": out_dir, "paths": paths, "machine": machine}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--bookmaker", default="betonlineag")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", default="live_diagnostic", choices=["live_diagnostic"])
    args = parser.parse_args()
    result = run(args.date, args.bookmaker, Path(args.output_dir))
    print(json.dumps({"out_dir": str(result["out_dir"]), **result["machine"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
