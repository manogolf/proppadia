#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import unicodedata
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
DEFAULT_OUT_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")


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


def _parse_time(value: str) -> dt_time:
    return datetime.strptime(value, "%H:%M").time()


def _iter_dates(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _snapshot_iso_utc(day: date, snapshot_time_et: dt_time) -> str:
    local_dt = datetime.combine(day, snapshot_time_et, ET)
    return local_dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _csv_tokens(value: str) -> list[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def _region_units(regions: str, bookmakers: str) -> int:
    books = _csv_tokens(bookmakers)
    if books:
        return max(1, math.ceil(len(books) / 10.0))
    regions_list = _csv_tokens(regions)
    return max(1, len(regions_list))


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


def _pick_data(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [x for x in payload["data"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _pick_event_odds_data(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _event_slate_date_et(event: dict[str, Any]) -> str | None:
    raw = event.get("commence_time") or event.get("commenceTime")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(ET).date().isoformat()


@dataclass
class ApiCall:
    endpoint_type: str
    status_code: int
    x_requests_last: int
    x_requests_remaining: str
    x_requests_used: str
    url: str
    event_id: str = ""


class Budget:
    def __init__(self, max_calls: int, max_credits: int, min_remaining: int, region_units: int) -> None:
        self.max_calls = max_calls
        self.max_credits = max_credits
        self.min_remaining = min_remaining
        self.region_units = region_units
        self.calls = 0
        self.credits = 0

    @property
    def event_odds_credit_ceiling(self) -> int:
        return 10 * self.region_units

    def before_call(self, expected_max_credit: int) -> None:
        if self.calls + 1 > self.max_calls:
            raise RuntimeError(f"MAX_ODDSAPI_CALLS would be exceeded: next={self.calls + 1} cap={self.max_calls}")
        if self.credits + expected_max_credit > self.max_credits:
            raise RuntimeError(
                f"MAX_ODDSAPI_CREDITS would be exceeded: current={self.credits} "
                f"next_expected={expected_max_credit} cap={self.max_credits}"
            )

    def after_call(self, headers: dict[str, str]) -> int:
        self.calls += 1
        last = _int_header(headers, "x-requests-last") or 0
        self.credits += last
        if self.credits > self.max_credits:
            raise RuntimeError(f"MAX_ODDSAPI_CREDITS exceeded after response: used={self.credits} cap={self.max_credits}")
        remaining = _int_header(headers, "x-requests-remaining")
        if remaining is not None and remaining < self.min_remaining:
            raise RuntimeError(f"Remaining credits below threshold: remaining={remaining} min={self.min_remaining}")
        return last


def _get_json(
    *,
    url: str,
    params: dict[str, Any],
    timeout: int,
    budget: Budget,
    expected_max_credit: int,
    calls: list[ApiCall],
    endpoint_type: str,
    event_id: str = "",
) -> Any:
    budget.before_call(expected_max_credit)
    resp = requests.get(url, params=params, timeout=timeout)
    headers = _headers(resp)
    last = budget.after_call(headers)
    calls.append(
        ApiCall(
            endpoint_type=endpoint_type,
            status_code=int(resp.status_code),
            x_requests_last=last,
            x_requests_remaining=headers.get("x-requests-remaining", ""),
            x_requests_used=headers.get("x-requests-used", ""),
            url=url,
            event_id=event_id,
        )
    )
    resp.raise_for_status()
    return resp.json()


def _load_player_map(day: date) -> dict[tuple[str, str], dict[str, Any]]:
    date_text = day.isoformat()
    paths = [
        Path(f"backend/mlb/exports/odds_history/{date_text}/mlb_slate_output.csv"),
        Path(f"backend/mlb/exports/odds_history/{date_text}/mlb_predictions_wide_calibrated.csv"),
        Path("backend/mlb/exports/odds_history/mlb_slate_output.csv"),
    ]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        import pandas as pd
    except Exception:
        return out
    for path in paths:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if "player_name" not in df.columns:
            continue
        for _, row in df.iterrows():
            if "date" in df.columns and str(row.get("date") or "")[:10] not in {"", date_text}:
                continue
            name = _norm_name(row.get("player_name"))
            if not name:
                continue
            team = str(row.get("team") or "").strip().upper()
            payload = {
                "player_id": row.get("player_id") if "player_id" in df.columns else None,
                "player_name": row.get("player_name"),
                "team": team or None,
                "opponent": row.get("opponent") if "opponent" in df.columns else None,
            }
            if team:
                out.setdefault((name, team), payload)
            out.setdefault((name, ""), payload)
    return out


def _team_for_player(player_norm: str, player_map: dict[tuple[str, str], dict[str, Any]]) -> tuple[str | None, dict[str, Any]]:
    mapped = player_map.get((player_norm, "")) or {}
    team = str(mapped.get("team") or "").strip().upper() or None
    return team, mapped


def _extract_rows(*, payloads: list[dict[str, Any]], snapshot_time: str, player_map: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ev in payloads:
        event_id = str(ev.get("id") or "").strip()
        home = str(ev.get("home_team") or "").strip()
        away = str(ev.get("away_team") or "").strip()
        game = f"{away} @ {home}" if home or away else ""
        commence = ev.get("commence_time")
        for book in ev.get("bookmakers") or []:
            if not isinstance(book, dict):
                continue
            book_key = str(book.get("key") or "").strip()
            for market in book.get("markets") or []:
                if not isinstance(market, dict) or str(market.get("key") or "").strip() != MARKET:
                    continue
                for outcome in market.get("outcomes") or []:
                    if not isinstance(outcome, dict):
                        continue
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    player = str(outcome.get("description") or "").strip()
                    if not player:
                        continue
                    player_norm = _norm_name(player)
                    team, mapped = _team_for_player(player_norm, player_map)
                    rows.append(
                        {
                            "event_id": event_id,
                            "game": game,
                            "home_team": home,
                            "away_team": away,
                            "commence_time": commence,
                            "bookmaker_key": book_key,
                            "bookmaker_title": book.get("title"),
                            "market_key": MARKET,
                            "player_name": player,
                            "normalized_player_name": player_norm,
                            "player_id": mapped.get("player_id"),
                            "team": team,
                            "opponent": mapped.get("opponent"),
                            "side": side,
                            "line": outcome.get("point"),
                            "price": outcome.get("price"),
                            "snapshot_timestamp": snapshot_time,
                        }
                    )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "event_id",
        "game",
        "home_team",
        "away_team",
        "commence_time",
        "bookmaker_key",
        "bookmaker_title",
        "market_key",
        "player_name",
        "normalized_player_name",
        "player_id",
        "team",
        "opponent",
        "side",
        "line",
        "price",
        "snapshot_timestamp",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _line_15_count(rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        try:
            if abs(float(row.get("line")) - 1.5) < 1e-9:
                count += 1
        except Exception:
            pass
    return count


def _book_list(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row.get("bookmaker_key") or "").strip() for row in rows if str(row.get("bookmaker_key") or "").strip()})


def _local_default_event_count(day: date) -> int:
    root = Path("backend/mlb/exports/odds_history") / day.isoformat()
    for name in ("events_for_slate.json", "events_raw.json"):
        path = root / name
        if not path.exists():
            continue
        try:
            events = _pick_data(json.loads(path.read_text()))
        except Exception:
            continue
        if events:
            return len(events)
    return 15


def _backfill_date(args: argparse.Namespace, day: date, snapshot_time: dt_time) -> dict[str, Any]:
    date_text = day.isoformat()
    snapshot_iso = _snapshot_iso_utc(day, snapshot_time)
    out_dir = Path(args.out_root) / date_text
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "live_alternate_book_level_rows.csv"
    summary_path = out_dir / "summary.json"
    raw_path = out_dir / "historical_event_odds_raw.json"
    events_path = out_dir / "historical_events_raw.json"

    units = _region_units(args.regions, args.bookmakers)
    expected_events = _local_default_event_count(day)
    dry_summary = {
        "date": date_text,
        "snapshot_iso_utc": snapshot_iso,
        "market": MARKET,
        "sport_key": SPORT_KEY,
        "dry_run": True,
        "estimated_events": expected_events,
        "estimated_calls": 1 + expected_events,
        "estimated_credits": 1 + expected_events * 10 * units,
        "max_oddsapi_calls": args.max_oddsapi_calls,
        "max_oddsapi_credits": args.max_oddsapi_credits,
        "output_csv": str(rows_path),
    }
    if args.dry_run:
        rows_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(dry_summary, indent=2, sort_keys=True))
        return dry_summary

    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY is not set")
    if args.require_confirm and str(args.confirm).strip().upper() != "YES":
        raise RuntimeError("REQUIRE_CONFIRM=1: pass confirmation to run a real pull")
    if args.max_oddsapi_calls <= 0:
        raise RuntimeError("MAX_ODDSAPI_CALLS is required and must be positive for real pulls")
    if args.max_oddsapi_credits <= 0:
        raise RuntimeError("MAX_ODDSAPI_CREDITS is required and must be positive for real pulls")

    budget = Budget(
        max_calls=args.max_oddsapi_calls,
        max_credits=args.max_oddsapi_credits,
        min_remaining=args.min_remaining_credits,
        region_units=units,
    )
    calls: list[ApiCall] = []
    base = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}"
    events_payload = _get_json(
        url=f"{base}/events",
        params={"apiKey": api_key, "date": snapshot_iso, "dateFormat": "iso"},
        timeout=args.timeout_sec,
        budget=budget,
        expected_max_credit=1,
        calls=calls,
        endpoint_type="historical_events",
    )
    events_all = _pick_data(events_payload)
    events = [ev for ev in events_all if _event_slate_date_et(ev) == date_text]
    events_path.write_text(json.dumps(events_payload, indent=2), encoding="utf-8")

    payloads: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for event in events:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            continue
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
        try:
            payload = _get_json(
                url=f"{base}/events/{event_id}/odds",
                params=params,
                timeout=args.timeout_sec,
                budget=budget,
                expected_max_credit=budget.event_odds_credit_ceiling,
                calls=calls,
                endpoint_type="historical_event_odds",
                event_id=event_id,
            )
            data = _pick_event_odds_data(payload)
            if data:
                payloads.append(data)
        except Exception as exc:
            errors.append({"event_id": event_id, "error": repr(exc)})
            if args.stop_on_error:
                raise

    raw_path.write_text(json.dumps(payloads, indent=2), encoding="utf-8")
    player_map = _load_player_map(day)
    rows = _extract_rows(payloads=payloads, snapshot_time=snapshot_iso, player_map=player_map)
    _write_csv(rows_path, rows)

    summary = {
        "date": date_text,
        "snapshot_iso_utc": snapshot_iso,
        "market": MARKET,
        "sport_key": SPORT_KEY,
        "dry_run": False,
        "events_returned": len(events_all),
        "events_for_date": len(events),
        "event_odds_payloads": len(payloads),
        "api_calls_used": budget.calls,
        "credits_used_observed": budget.credits,
        "rows": len(rows),
        "line_1_5_rows": _line_15_count(rows),
        "books": _book_list(rows),
        "errors": errors,
        "calls": [call.__dict__ for call in calls],
        "output_csv": str(rows_path),
        "summary_json": str(summary_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Safely backfill historical OddsAPI batter_hits_alternate source CSVs.")
    ap.add_argument("--date-from", required=True, type=_parse_date)
    ap.add_argument("--date-to", required=True, type=_parse_date)
    ap.add_argument("--snapshot-time-et", default=DEFAULT_SNAPSHOT_TIME_ET)
    ap.add_argument("--regions", default=os.getenv("MLB_ODDS_REGIONS", DEFAULT_REGIONS) or DEFAULT_REGIONS)
    ap.add_argument("--bookmakers", default=_bookmakers_query_csv())
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument("--timeout-sec", type=int, default=25)
    ap.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--require-confirm", type=int, default=int(os.getenv("REQUIRE_CONFIRM", "1")))
    ap.add_argument("--confirm", default=os.getenv("MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM", ""))
    ap.add_argument("--max-oddsapi-calls", type=int, default=int(os.getenv("MAX_ODDSAPI_CALLS", "0")))
    ap.add_argument("--max-oddsapi-credits", type=int, default=int(os.getenv("MAX_ODDSAPI_CREDITS", "0")))
    ap.add_argument("--min-remaining-credits", type=int, default=int(os.getenv("MIN_ODDSAPI_REMAINING_CREDITS", "0")))
    ap.add_argument("--stop-on-error", action="store_true")
    args = ap.parse_args()

    if args.date_to < args.date_from:
        raise SystemExit("--date-to must be on or after --date-from")

    snapshot_time = _parse_time(args.snapshot_time_et)
    summaries = [_backfill_date(args, day, snapshot_time) for day in _iter_dates(args.date_from, args.date_to)]
    summary_csv = Path(args.out_root) / "backfill_summary.csv"
    fields = [
        "date",
        "dry_run",
        "snapshot_iso_utc",
        "estimated_events",
        "estimated_calls",
        "estimated_credits",
        "events_returned",
        "events_for_date",
        "api_calls_used",
        "credits_used_observed",
        "rows",
        "line_1_5_rows",
        "output_csv",
    ]
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with summary_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in summaries:
            writer.writerow({field: row.get(field, "") for field in fields})
    print(json.dumps({"dates": len(summaries), "dry_run": args.dry_run, "summary_csv": str(summary_csv)}, indent=2))


if __name__ == "__main__":
    main()
