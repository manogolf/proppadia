#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from backend.app.services.mlb.market_odds_service import _bookmakers_query_csv


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SPORT_KEY = "baseball_mlb"
MARKET = "batter_hits_alternate"
DEFAULT_REGIONS = "us"
DEFAULT_SNAPSHOT_TIME_ET = "13:00"
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids/alternate_history")


def _load_dotenv_multi() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    root = Path(__file__).resolve().parents[3]
    for p in (root / ".env.local", root / ".env", root / "backend" / ".env", root / "mlb" / ".env"):
        if p.exists():
            load_dotenv(p, override=False)


_load_dotenv_multi()


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_snapshot_time(value: str) -> dt_time:
    return datetime.strptime(value, "%H:%M").time()


def _iter_dates(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _snapshot_iso_utc(day: date, snapshot_time_et: dt_time) -> str:
    local_dt = datetime.combine(day, snapshot_time_et, ET)
    return local_dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pick_data(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [x for x in payload["data"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _event_slate_date_et(event: dict[str, Any]) -> str | None:
    raw = event.get("commence_time") or event.get("commenceTime")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(ET).date().isoformat()


def _headers(resp: requests.Response) -> dict[str, str]:
    return {
        "x-requests-remaining": resp.headers.get("x-requests-remaining", ""),
        "x-requests-used": resp.headers.get("x-requests-used", ""),
        "x-requests-last": resp.headers.get("x-requests-last", ""),
    }


def _int_header(headers: dict[str, str], key: str) -> int | None:
    raw = str(headers.get(key, "")).strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _csv_tokens(value: str) -> list[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def _region_units(regions: str, bookmakers: str) -> int:
    books = _csv_tokens(bookmakers)
    if books:
        return max(1, math.ceil(len(books) / 10.0))
    regs = _csv_tokens(regions)
    return max(1, len(regs))


def _local_event_count(day: date) -> tuple[int | None, str]:
    root = Path("backend/mlb/exports/odds_history") / day.isoformat()
    candidates = [
        root / "events_for_slate.json",
        root / "events_raw.json",
        root / "manifest.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        if path.name == "manifest.json":
            for key in ("event_count", "events_count", "events_for_slate_count"):
                if key in payload:
                    try:
                        return int(payload[key]), str(path)
                    except Exception:
                        pass
            continue
        events = _pick_data(payload)
        if events:
            return len(events), str(path)
    return None, "default_estimate"


@dataclass
class ProbeCall:
    endpoint_type: str
    url: str
    status_code: int
    headers: dict[str, str]
    note: str = ""


class ProbeBudget:
    def __init__(self, max_calls: int, max_credits: int, min_remaining: int) -> None:
        self.max_calls = max_calls
        self.max_credits = max_credits
        self.min_remaining = min_remaining
        self.calls = 0
        self.credits = 0

    def before_call(self) -> None:
        if self.calls + 1 > self.max_calls:
            raise RuntimeError(f"MAX_ODDSAPI_CALLS exceeded before call {self.calls + 1}: cap={self.max_calls}")

    def after_call(self, headers: dict[str, str]) -> None:
        self.calls += 1
        last = _int_header(headers, "x-requests-last") or 0
        self.credits += last
        if self.credits > self.max_credits:
            raise RuntimeError(f"MAX_ODDSAPI_CREDITS exceeded: used={self.credits} cap={self.max_credits}")
        remaining = _int_header(headers, "x-requests-remaining")
        if remaining is not None and remaining < self.min_remaining:
            raise RuntimeError(
                f"OddsAPI remaining credits below threshold: remaining={remaining} min={self.min_remaining}"
            )


def _get_json(
    *,
    url: str,
    params: dict[str, Any],
    timeout: int,
    budget: ProbeBudget,
    calls: list[ProbeCall],
    endpoint_type: str,
) -> Any:
    budget.before_call()
    resp = requests.get(url, params=params, timeout=timeout)
    headers = _headers(resp)
    calls.append(
        ProbeCall(
            endpoint_type=endpoint_type,
            url=url,
            status_code=int(resp.status_code),
            headers=headers,
        )
    )
    budget.after_call(headers)
    resp.raise_for_status()
    return resp.json()


def _extract_market_counts(payload: dict[str, Any]) -> dict[str, Any]:
    rows = 0
    line_15 = 0
    books: set[str] = set()
    players: set[str] = set()
    lines: dict[str, int] = {}
    for book in payload.get("bookmakers") or []:
        if not isinstance(book, dict):
            continue
        book_key = str(book.get("key") or "").strip()
        if book_key:
            books.add(book_key)
        for market in book.get("markets") or []:
            if not isinstance(market, dict) or str(market.get("key") or "") != MARKET:
                continue
            for outcome in market.get("outcomes") or []:
                if not isinstance(outcome, dict):
                    continue
                rows += 1
                player = str(outcome.get("description") or "").strip()
                if player:
                    players.add(player)
                point = outcome.get("point")
                try:
                    point_num = float(point)
                    point_key = f"{point_num:g}"
                    if abs(point_num - 1.5) < 1e-9:
                        line_15 += 1
                except Exception:
                    point_key = str(point or "")
                if point_key:
                    lines[point_key] = lines.get(point_key, 0) + 1
    return {
        "book_count": len(books),
        "books_present": sorted(books),
        "player_line_rows": rows,
        "unique_players": len(players),
        "line_1_5_rows": line_15,
        "line_distribution": dict(sorted(lines.items(), key=lambda kv: kv[0])),
    }


def _run_probe(args: argparse.Namespace, snapshot_iso: str) -> dict[str, Any]:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY is not set")
    if args.require_confirm and str(args.confirm).strip().upper() != "YES":
        raise RuntimeError("REQUIRE_CONFIRM=1: pass --confirm YES to run the live probe")

    budget = ProbeBudget(
        max_calls=args.max_oddsapi_calls,
        max_credits=args.max_oddsapi_credits,
        min_remaining=args.min_remaining_credits,
    )
    calls: list[ProbeCall] = []
    base = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}"
    events_url = f"{base}/events"
    event_payload = _get_json(
        url=events_url,
        params={"apiKey": api_key, "date": snapshot_iso, "dateFormat": "iso"},
        timeout=args.timeout_sec,
        budget=budget,
        calls=calls,
        endpoint_type="historical_events",
    )
    events_all = _pick_data(event_payload)
    slate_date = args.date_from.isoformat()
    events = [ev for ev in events_all if _event_slate_date_et(ev) == slate_date]
    if not events:
        events = events_all[:1]
    if not events:
        return {
            "probe_ran": True,
            "endpoint_type": "historical_events",
            "historical_batter_hits_alternate_available": False,
            "events_returned": 0,
            "event_odds_called": False,
            "calls": [c.__dict__ for c in calls],
            "credits_used_observed": budget.credits,
        }

    event = events[0]
    event_id = str(event.get("id") or "").strip()
    odds_url = f"{base}/events/{event_id}/odds"
    params: dict[str, Any] = {
        "apiKey": api_key,
        "date": snapshot_iso,
        "regions": args.regions,
        "markets": MARKET,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    if args.bookmakers:
        params["bookmakers"] = args.bookmakers
    odds_payload = _get_json(
        url=odds_url,
        params=params,
        timeout=args.timeout_sec,
        budget=budget,
        calls=calls,
        endpoint_type="historical_event_odds",
    )
    data = odds_payload.get("data") if isinstance(odds_payload, dict) else odds_payload
    odds_obj = data if isinstance(data, dict) else {}
    counts = _extract_market_counts(odds_obj)
    return {
        "probe_ran": True,
        "snapshot_iso_utc": snapshot_iso,
        "sport_key": SPORT_KEY,
        "market": MARKET,
        "endpoint_type": "historical_event_odds",
        "event": {
            "id": event_id,
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
            "commence_time": event.get("commence_time"),
        },
        "events_returned": len(events_all),
        "event_odds_called": True,
        "historical_batter_hits_alternate_available": bool(counts["player_line_rows"] > 0),
        **counts,
        "calls": [c.__dict__ for c in calls],
        "api_calls_used": budget.calls,
        "credits_used_observed": budget.credits,
        "last_headers": calls[-1].headers if calls else {},
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _write_report(path: Path, summary: dict[str, Any], estimate_rows: list[dict[str, Any]]) -> None:
    probe = summary.get("probe", {})
    lines = [
        "# OddsAPI Alternate History Availability / Cost Audit",
        "",
        "Scope: cost and availability audit only. No historical backfill was run.",
        "",
        "## Configuration",
        "",
        f"- Sport key: `{SPORT_KEY}`",
        f"- Market: `{MARKET}`",
        f"- Date range estimated: `{summary['date_from']}` through `{summary['date_to']}`",
        f"- Snapshot time ET: `{summary['snapshot_time_et']}`",
        f"- Regions: `{summary['regions']}`",
        f"- Bookmakers override: `{summary['bookmakers'] or ''}`",
        f"- Region-equivalent units: `{summary['region_units']}`",
        f"- Dry run: `{summary['dry_run']}`",
        "",
        "## Docs / Cost Model",
        "",
        "- Historical sport events are available via `/v4/historical/sports/{sport}/events`.",
        "- Historical event odds are available via `/v4/historical/sports/{sport}/events/{eventId}/odds`.",
        "- Historical event odds cost is estimated as `10 x unique markets returned x region units`.",
        "- The events lookup is estimated as `1` credit per snapshot.",
        "- OddsAPI response headers are expected to include `x-requests-remaining`, `x-requests-used`, and `x-requests-last`.",
        "",
        "## Cost Estimate",
        "",
        "| window | dates | events/date | calls | expected credits | worst-case credits |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in estimate_rows:
        lines.append(
            f"| {row['window']} | {row['dates']} | {row['avg_events_per_date']} | {row['expected_calls']} | "
            f"{row['expected_credits']} | {row['worst_case_credits']} |"
        )
    lines.extend(["", "## Tiny Probe", ""])
    if not probe.get("probe_ran"):
        lines.append("Probe not run. Estimator mode made zero OddsAPI calls.")
    else:
        lines.extend(
            [
                f"- Historical batter_hits_alternate available in probe: `{probe.get('historical_batter_hits_alternate_available')}`",
                f"- API calls used: `{probe.get('api_calls_used')}`",
                f"- Observed credits used: `{probe.get('credits_used_observed')}`",
                f"- Events returned by historical events endpoint: `{probe.get('events_returned')}`",
                f"- Player-line rows: `{probe.get('player_line_rows')}`",
                f"- Line 1.5 rows: `{probe.get('line_1_5_rows')}`",
                f"- Books present: `{', '.join(probe.get('books_present') or [])}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Safety Recommendation",
            "",
            "Do not run a multi-day pull until the one-date probe is accepted and a max-credit cap is chosen.",
            "Recommended first real backfill shape: one date, one snapshot, event-level calls, `batter_hits_alternate` only, with explicit `MAX_ODDSAPI_CALLS` and `MAX_ODDSAPI_CREDITS` caps.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser(description="Estimate/probe OddsAPI historical batter_hits_alternate availability and cost.")
    ap.add_argument("--date-from", required=True, type=_parse_date)
    ap.add_argument("--date-to", required=True, type=_parse_date)
    ap.add_argument("--snapshot-time-et", default=DEFAULT_SNAPSHOT_TIME_ET)
    ap.add_argument("--regions", default=os.getenv("MLB_ODDS_REGIONS", DEFAULT_REGIONS) or DEFAULT_REGIONS)
    ap.add_argument("--bookmakers", default=_bookmakers_query_csv())
    ap.add_argument("--snapshots-per-date", type=int, default=1)
    ap.add_argument("--default-events-per-date", type=int, default=15)
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--timeout-sec", type=int, default=25)
    ap.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--run-probe", action="store_true")
    ap.add_argument("--require-confirm", type=int, default=int(os.getenv("REQUIRE_CONFIRM", "1")))
    ap.add_argument("--confirm", default=os.getenv("MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM", ""))
    ap.add_argument("--max-oddsapi-calls", type=int, default=int(os.getenv("MAX_ODDSAPI_CALLS", "0")))
    ap.add_argument("--max-oddsapi-credits", type=int, default=int(os.getenv("MAX_ODDSAPI_CREDITS", "0")))
    ap.add_argument("--min-remaining-credits", type=int, default=int(os.getenv("MIN_ODDSAPI_REMAINING_CREDITS", "0")))
    args = ap.parse_args()

    if args.date_to < args.date_from:
        raise SystemExit("--date-to must be on or after --date-from")
    snapshot_time = _parse_snapshot_time(args.snapshot_time_et)
    dates = _iter_dates(args.date_from, args.date_to)
    units = _region_units(args.regions, args.bookmakers)

    event_counts: list[int] = []
    sources: dict[str, str] = {}
    for day in dates:
        count, source = _local_event_count(day)
        event_counts.append(int(count if count is not None else args.default_events_per_date))
        sources[day.isoformat()] = source
    avg_events = round(sum(event_counts) / len(event_counts), 2) if event_counts else 0
    max_events = max(event_counts) if event_counts else args.default_events_per_date

    def estimate(label: str, num_dates: int, events_per_date: float, worst_events_per_date: int) -> dict[str, Any]:
        event_lookup_calls = num_dates * args.snapshots_per_date
        event_odds_calls = int(math.ceil(num_dates * args.snapshots_per_date * events_per_date))
        worst_event_odds_calls = num_dates * args.snapshots_per_date * worst_events_per_date
        event_odds_cost = 10 * units
        return {
            "window": label,
            "dates": num_dates,
            "snapshots_per_date": args.snapshots_per_date,
            "avg_events_per_date": events_per_date,
            "region_units": units,
            "event_lookup_calls": event_lookup_calls,
            "event_odds_calls": event_odds_calls,
            "expected_calls": event_lookup_calls + event_odds_calls,
            "expected_credits": event_lookup_calls + event_odds_calls * event_odds_cost,
            "worst_case_event_odds_calls": worst_event_odds_calls,
            "worst_case_credits": event_lookup_calls + worst_event_odds_calls * event_odds_cost,
        }

    estimate_rows = [
        estimate("requested_range", len(dates), avg_events, max_events),
        estimate("7_days", 7, avg_events or args.default_events_per_date, max(max_events, args.default_events_per_date)),
        estimate("14_days", 14, avg_events or args.default_events_per_date, max(max_events, args.default_events_per_date)),
        estimate("30_days", 30, avg_events or args.default_events_per_date, max(max_events, args.default_events_per_date)),
    ]

    probe: dict[str, Any] = {"probe_ran": False}
    if args.run_probe:
        if args.dry_run:
            raise SystemExit("--run-probe requires --no-dry-run")
        snapshot_iso = _snapshot_iso_utc(args.date_from, snapshot_time)
        probe = _run_probe(args, snapshot_iso)

    out_dir = Path(args.out_dir)
    summary = {
        "date_from": args.date_from.isoformat(),
        "date_to": args.date_to.isoformat(),
        "snapshot_time_et": args.snapshot_time_et,
        "regions": args.regions,
        "bookmakers": args.bookmakers,
        "region_units": units,
        "market": MARKET,
        "sport_key": SPORT_KEY,
        "dry_run": args.dry_run,
        "local_event_count_sources": sources,
        "cost_estimates": estimate_rows,
        "probe": probe,
    }
    _write_csv(out_dir / "oddsapi_alternate_history_cost_estimate.csv", estimate_rows)
    _write_json(out_dir / "oddsapi_alternate_history_probe_summary.json", summary)
    _write_report(out_dir / "oddsapi_alternate_history_availability_cost_audit.md", summary, estimate_rows)
    print(json.dumps({"out_dir": str(out_dir), "probe_ran": probe.get("probe_ran"), "expected_30_day_credits": estimate_rows[-1]["expected_credits"]}, indent=2))


if __name__ == "__main__":
    main()
