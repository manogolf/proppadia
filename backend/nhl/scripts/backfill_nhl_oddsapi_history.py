#!/usr/bin/env python3
"""
Backfill NHL player-prop odds snapshots from The Odds API historical endpoints.

Writes one folder per ET date:
  backend/nhl/exports/odds_history/YYYY-MM-DD/
    - manifest.json
    - events_raw.json
    - events_for_slate.json
    - odds_event_wrappers.json
    - odds_latest_compatible.json

Usage:
  python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --season 2025
  python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --from-date 2025-10-07 --to-date 2026-03-01
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SPORT_KEY = "icehockey_nhl"

DEFAULT_MARKETS = ",".join(
    [
        "player_shots_on_goal",
        "player_shots_on_goal_alternate",
        "player_total_saves",
        "player_points",
    ]
)
DEFAULT_REGIONS = "us,us2"
DEFAULT_ODDS_FORMAT = "american"
DEFAULT_SNAPSHOT_TIME_ET = "19:00"

# NHL season naming in this repo uses start-year as season id.
SEASON_START_BY_ID: dict[int, str] = {
    2023: "2023-10-10",
    2024: "2024-10-04",
    2025: "2025-10-07",
}

BACKEND_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUT_ROOT = BACKEND_DIR / "nhl" / "exports" / "odds_history"


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_snapshot_time_et(s: str) -> dt_time:
    try:
        return datetime.strptime(s, "%H:%M").time()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid --snapshot-time-et {s!r}; expected HH:MM") from e


def _iter_dates(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _safe_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def _to_utc_snapshot_iso(d: date, snapshot_time: dt_time) -> str:
    dt_et = datetime.combine(d, snapshot_time, ET)
    dt_utc = dt_et.astimezone(UTC)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_slate_date_et(event_obj: dict[str, Any]) -> str | None:
    raw = event_obj.get("commence_time") or event_obj.get("commenceTime")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(ET).date().isoformat()


def _pick_events_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


@dataclass
class FetchContext:
    api_key: str
    markets: str
    regions: str
    odds_format: str
    timeout: int


def _get_json(url: str, *, params: dict[str, Any], timeout: int) -> tuple[Any, dict[str, str], int]:
    resp = requests.get(url, params=params, timeout=timeout)
    status = int(resp.status_code)
    headers = {
        "x-requests-remaining": resp.headers.get("x-requests-remaining", ""),
        "x-requests-used": resp.headers.get("x-requests-used", ""),
    }
    resp.raise_for_status()
    return resp.json(), headers, status


def _fetch_events_snapshot(ctx: FetchContext, snapshot_iso_utc: str) -> tuple[Any, dict[str, str], int]:
    url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
    params = {
        "apiKey": ctx.api_key,
        "date": snapshot_iso_utc,
        "dateFormat": "iso",
    }
    return _get_json(url, params=params, timeout=ctx.timeout)


def _fetch_event_odds_snapshot(
    ctx: FetchContext, event_id: str, snapshot_iso_utc: str
) -> tuple[Any, dict[str, str], int]:
    url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "apiKey": ctx.api_key,
        "date": snapshot_iso_utc,
        "regions": ctx.regions,
        "markets": ctx.markets,
        "oddsFormat": ctx.odds_format,
        "dateFormat": "iso",
    }
    return _get_json(url, params=params, timeout=ctx.timeout)


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill NHL historical player-prop odds snapshots by ET date.")
    ap.add_argument("--season", type=int, default=None, help="Season id (start year), e.g. 2025.")
    ap.add_argument("--from-date", default=None, help="ET date YYYY-MM-DD (overrides --season start).")
    ap.add_argument("--to-date", default=None, help="ET date YYYY-MM-DD. Default: ET yesterday.")
    ap.add_argument("--snapshot-time-et", type=_parse_snapshot_time_et, default=DEFAULT_SNAPSHOT_TIME_ET)
    ap.add_argument("--markets", default=DEFAULT_MARKETS)
    ap.add_argument("--regions", default=DEFAULT_REGIONS)
    ap.add_argument("--odds-format", default=DEFAULT_ODDS_FORMAT, choices=["american", "decimal"])
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument("--timeout-sec", type=int, default=30)
    ap.add_argument("--sleep-ms", type=int, default=150)
    ap.add_argument("--max-days", type=int, default=None, help="Optional cap for incremental runs.")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key and not args.dry_run:
        raise SystemExit("Missing ODDS_API_KEY")

    if args.from_date:
        start_d = _parse_iso_date(args.from_date)
    elif args.season is not None:
        if args.season not in SEASON_START_BY_ID:
            raise SystemExit(
                f"Unknown --season {args.season}. Known starts: {sorted(SEASON_START_BY_ID)} "
                f"(use --from-date to override)."
            )
        start_d = _parse_iso_date(SEASON_START_BY_ID[args.season])
    else:
        raise SystemExit("Provide --season or --from-date")

    if args.to_date:
        end_d = _parse_iso_date(args.to_date)
    else:
        end_d = datetime.now(ET).date() - timedelta(days=1)

    if end_d < start_d:
        raise SystemExit(f"Invalid range: from {start_d} > to {end_d}")

    all_days = _iter_dates(start_d, end_d)

    out_root = Path(args.out_root)
    if not args.overwrite:
        pending_all = [d for d in all_days if not (out_root / d.isoformat() / "manifest.json").exists()]
    else:
        pending_all = list(all_days)

    if args.max_days is not None:
        run_days = pending_all[: max(0, int(args.max_days))]
    else:
        run_days = pending_all

    pre_skipped = len(all_days) - len(pending_all)
    ctx = FetchContext(
        api_key=api_key or "dry-run",
        markets=args.markets,
        regions=args.regions,
        odds_format=args.odds_format,
        timeout=int(args.timeout_sec),
    )

    print(
        f"[odds-backfill] start={start_d} end={end_d} days_total={len(all_days)} "
        f"days_pending={len(pending_all)} days_run={len(run_days)} "
        f"snapshot_time_et={args.snapshot_time_et.strftime('%H:%M')}"
    )
    print(f"[odds-backfill] out_root={out_root}")

    copied = 0
    skipped = pre_skipped
    failed = 0
    for d in run_days:
        day_str = d.isoformat()
        day_dir = out_root / day_str
        manifest_path = day_dir / "manifest.json"

        if manifest_path.exists() and not args.overwrite:
            print(f"[odds-backfill] skip {day_str}: manifest exists")
            skipped += 1
            continue

        snapshot_iso = _to_utc_snapshot_iso(d, args.snapshot_time_et)
        print(f"[odds-backfill] {day_str} snapshot_utc={snapshot_iso}")

        if args.dry_run:
            copied += 1
            continue

        try:
            events_raw, ev_headers, ev_status = _fetch_events_snapshot(ctx, snapshot_iso)
            events = _pick_events_list(events_raw)
            events_for_slate = [e for e in events if _event_slate_date_et(e) == day_str]

            wrappers: list[dict[str, Any]] = []
            odds_data: list[dict[str, Any]] = []
            last_headers: dict[str, str] = ev_headers

            for idx, ev in enumerate(events_for_slate, start=1):
                event_id = str(ev.get("id", "")).strip()
                if not event_id:
                    wrappers.append({})
                    odds_data.append({})
                    continue
                try:
                    odds_wrapped, odds_headers, _odds_status = _fetch_event_odds_snapshot(
                        ctx, event_id, snapshot_iso
                    )
                    last_headers = odds_headers
                    wrappers.append(odds_wrapped if isinstance(odds_wrapped, dict) else {"data": odds_wrapped})
                    if isinstance(odds_wrapped, dict):
                        odds_data.append(odds_wrapped.get("data") or {})
                    else:
                        odds_data.append({})
                except Exception as e:
                    print(f"[odds-backfill] {day_str} event {event_id} failed: {e}")
                    wrappers.append({"event_id": event_id, "error": str(e)})
                    odds_data.append({})

                if args.sleep_ms > 0 and idx < len(events_for_slate):
                    time.sleep(args.sleep_ms / 1000.0)

            manifest = {
                "ok": True,
                "season": args.season,
                "date_et": day_str,
                "snapshot_utc": snapshot_iso,
                "events_total_snapshot": len(events),
                "events_for_slate": len(events_for_slate),
                "odds_records": len(odds_data),
                "markets": args.markets,
                "regions": args.regions,
                "odds_format": args.odds_format,
                "api_status_events": ev_status,
                "api_usage": {
                    "x-requests-remaining": last_headers.get("x-requests-remaining"),
                    "x-requests-used": last_headers.get("x-requests-used"),
                },
                "paths": {
                    "events_raw": str(day_dir / "events_raw.json"),
                    "events_for_slate": str(day_dir / "events_for_slate.json"),
                    "odds_event_wrappers": str(day_dir / "odds_event_wrappers.json"),
                    "odds_latest_compatible": str(day_dir / "odds_latest_compatible.json"),
                },
            }

            _safe_json(day_dir / "events_raw.json", events_raw)
            _safe_json(day_dir / "events_for_slate.json", events_for_slate)
            _safe_json(day_dir / "odds_event_wrappers.json", wrappers)
            _safe_json(day_dir / "odds_latest_compatible.json", odds_data)
            _safe_json(manifest_path, manifest)

            print(
                f"[odds-backfill] wrote {day_str}: events_for_slate={len(events_for_slate)} "
                f"odds_records={len(odds_data)} remaining={manifest['api_usage']['x-requests-remaining']}"
            )
            copied += 1
        except Exception as e:
            print(f"[odds-backfill] FAILED {day_str}: {e}")
            failed += 1

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    print(
        f"[odds-backfill] done copied={copied} skipped={skipped} failed={failed} "
        f"out_root={out_root}"
    )


if __name__ == "__main__":
    main()
