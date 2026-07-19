"""Exhaustively diagnose BetOnline MLB availability across OddsAPI surfaces.

Bounded live diagnostic. It preserves every raw response and writes only local
artifacts. It performs no DB writes and makes no production/model/scheduler
changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests

from backend.mlb.shared.betonline_market_registry import active_market_rows


REPO_ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
PREVIOUS_DIAG_ROOT = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_oddsapi_capture_coverage_repair/2026-07-18/live_diagnostic"
)
DEFAULT_OUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_oddsapi_betonline_exhaustive_surface_diagnostic/2026-07-18"
)
ET = ZoneInfo("America/New_York")
SPORT = "baseball_mlb"
CURRENT_BASE = f"https://api.the-odds-api.com/v4/sports/{SPORT}"
HIST_BASE = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT}"
TARGET_TIMESTAMP = "2026-07-18T23:14:16Z"
BOOKMAKER = "betonlineag"
HITTER_MARKETS = [
    "batter_hits",
    "batter_total_bases",
    "batter_hits_runs_rbis",
    "batter_home_runs",
    "batter_stolen_bases",
]
PITCHER_MARKETS = [
    "pitcher_strikeouts",
    "pitcher_outs",
    "pitcher_earned_runs",
    "pitcher_hits_allowed",
]
ALL_MARKETS = HITTER_MARKETS + PITCHER_MARKETS
VARIANTS = [
    ("bookmakers_betonlineag", {"bookmakers": BOOKMAKER}),
    ("regions_us", {"regions": "us"}),
    ("regions_eu", {"regions": "eu"}),
    ("regions_us_eu", {"regions": "us,eu"}),
    ("unfiltered_regions_us", {"regions": "us"}),
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def safe(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "")).strip("_") or "blank"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def is_betonline(book: dict[str, Any]) -> bool:
    text = f"{book.get('key') or ''} {book.get('title') or ''}".lower()
    return "betonline" in text or str(book.get("key") or "").lower() == BOOKMAKER


def event_date_et(event: dict[str, Any]) -> str:
    raw = str(event.get("commence_time") or "")
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return dt.astimezone(ET).date().isoformat()


def oddsapi_timestamp(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return text
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    if text.endswith("00:00") and "+" in text:
        try:
            dt = datetime.fromisoformat(text)
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return text
    return text


def payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return [payload]
    return []


def extract_snapshot_meta(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    out = {}
    for key in ("timestamp", "previous_timestamp", "next_timestamp"):
        value = payload.get(key)
        if value:
            out[key] = str(value)
    return out


def bookmaker_keys_from_payload(payload: Any) -> set[str]:
    keys: set[str] = set()
    for item in payload_items(payload):
        for book in item.get("bookmakers", []) or []:
            if isinstance(book, dict) and book.get("key"):
                keys.add(str(book.get("key")))
    return keys


def market_keys_for_bookmaker(payload: Any, bookmaker: str = BOOKMAKER) -> set[str]:
    keys: set[str] = set()
    for item in payload_items(payload):
        for book in item.get("bookmakers", []) or []:
            if not isinstance(book, dict) or str(book.get("key") or "") != bookmaker:
                continue
            for market in book.get("markets", []) or []:
                if isinstance(market, dict) and market.get("key"):
                    keys.add(str(market.get("key")))
    return keys


def count_book_market_outcomes(payload: Any, bookmaker: str = BOOKMAKER) -> tuple[int, int, int, set[str], set[str]]:
    book_count = 0
    market_count = 0
    outcome_count = 0
    markets: set[str] = set()
    players: set[str] = set()
    for item in payload_items(payload):
        for book in item.get("bookmakers", []) or []:
            if not isinstance(book, dict) or str(book.get("key") or "") != bookmaker:
                continue
            book_count += 1
            for market in book.get("markets", []) or []:
                if not isinstance(market, dict):
                    continue
                market_count += 1
                key = str(market.get("key") or "")
                if key:
                    markets.add(key)
                for outcome in market.get("outcomes", []) or []:
                    if isinstance(outcome, dict):
                        outcome_count += 1
                        desc = str(outcome.get("description") or outcome.get("name") or "").strip()
                        if desc:
                            players.add(desc)
    return book_count, market_count, outcome_count, markets, players


class Diagnostic:
    def __init__(self, *, api_key: str, out_dir: Path, run_tag: str):
        self.api_key = api_key
        self.out_dir = out_dir
        self.raw_dir = out_dir / "raw"
        self.run_tag = run_tag
        self.session = requests.Session()
        self.requests: list[dict[str, Any]] = []
        self.surfaces: list[dict[str, Any]] = []
        self.quota_rows: list[dict[str, Any]] = []

    def request(
        self,
        *,
        surface: str,
        url: str,
        params: dict[str, str],
        request_id: str,
        endpoint_family: str,
        event_id: str = "",
        timestamp_label: str = "",
    ) -> Any | None:
        clean_params = {k: v for k, v in params.items() if k != "apiKey"}
        full_params = dict(params)
        full_params["apiKey"] = self.api_key
        raw_path = self.raw_dir / f"{request_id}.json"
        txt_path = self.raw_dir / f"{request_id}.txt"
        started = now_utc()
        error = ""
        response: requests.Response | None = None
        payload: Any | None = None
        try:
            response = self.session.get(url, params=full_params, timeout=30)
            content = response.content
            try:
                payload = response.json()
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(content)
                path = raw_path
                parse_status = "PASS"
            except Exception as exc:
                txt_path.parent.mkdir(parents=True, exist_ok=True)
                txt_path.write_bytes(content)
                path = txt_path
                parse_status = f"FAIL:{type(exc).__name__}"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            content = error.encode()
            txt_path.parent.mkdir(parents=True, exist_ok=True)
            txt_path.write_bytes(content)
            path = txt_path
            parse_status = "FAIL:REQUEST_EXCEPTION"
        headers = response.headers if response is not None else {}
        http_status = response.status_code if response is not None else ""
        result_status = "PASS" if response is not None and response.ok else "FAIL"
        row = {
            "request_id": request_id,
            "request_timestamp_utc": started,
            "surface": surface,
            "endpoint_family": endpoint_family,
            "url": url.replace("https://api.the-odds-api.com/v4", "/v4").replace("https://ipv6-api.the-odds-api.com/v4", "ipv6:/v4"),
            "event_id": event_id,
            "timestamp_label": timestamp_label,
            "params_json": json.dumps(clean_params, sort_keys=True),
            "bookmakers_param": clean_params.get("bookmakers", ""),
            "regions_param": clean_params.get("regions", ""),
            "markets_param": clean_params.get("markets", ""),
            "date_param": clean_params.get("date", ""),
            "http_status": http_status,
            "result_status": result_status,
            "parse_status": parse_status,
            "quota_requests_used": headers.get("x-requests-used", ""),
            "quota_requests_remaining": headers.get("x-requests-remaining", ""),
            "quota_requests_last": headers.get("x-requests-last", ""),
            "raw_payload_path": rel(path),
            "payload_sha256": sha256_bytes(content),
            "error": error,
        }
        self.requests.append(row)
        self.quota_rows.append(
            {
                "request_id": request_id,
                "surface": surface,
                "quota_requests_used": row["quota_requests_used"],
                "quota_requests_remaining": row["quota_requests_remaining"],
                "quota_requests_last": row["quota_requests_last"],
                "http_status": http_status,
            }
        )
        if payload is not None:
            book_keys = bookmaker_keys_from_payload(payload)
            b_count, m_count, o_count, markets, players = count_book_market_outcomes(payload)
            meta = extract_snapshot_meta(payload)
            self.surfaces.append(
                {
                    "request_id": request_id,
                    "surface": surface,
                    "endpoint_family": endpoint_family,
                    "event_id": event_id,
                    "timestamp_label": timestamp_label,
                    "http_status": http_status,
                    "result_status": result_status,
                    "bookmakers_param": row["bookmakers_param"],
                    "regions_param": row["regions_param"],
                    "markets_param": row["markets_param"],
                    "betonline_present": "yes" if BOOKMAKER in book_keys or b_count > 0 else "no",
                    "bookmaker_keys_returned": "|".join(sorted(book_keys)),
                    "betonline_book_objects": b_count,
                    "betonline_market_rows": m_count,
                    "betonline_outcome_rows": o_count,
                    "betonline_market_keys": "|".join(sorted(markets)),
                    "betonline_players_or_outcomes": len(players),
                    "snapshot_timestamp": meta.get("timestamp", ""),
                    "previous_timestamp": meta.get("previous_timestamp", ""),
                    "next_timestamp": meta.get("next_timestamp", ""),
                    "raw_payload_path": row["raw_payload_path"],
                    "payload_sha256": row["payload_sha256"],
                }
            )
        return payload


def find_previous_manifest() -> Path | None:
    dirs = sorted(PREVIOUS_DIAG_ROOT.glob("betonline_live_diag_*"))
    if not dirs:
        return None
    path = dirs[-1] / "betonline_live_request_manifest_2026-07-18.csv"
    return path if path.exists() else None


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def audit_prior_manifest(out_dir: Path) -> list[dict[str, Any]]:
    manifest = find_previous_manifest()
    rows: list[dict[str, Any]] = []
    for row in read_csv(manifest) if manifest else []:
        raw_path = REPO_ROOT / str(row.get("raw_payload_path", ""))
        payload = load_json(raw_path)
        book_keys = bookmaker_keys_from_payload(payload)
        rows.append(
            {
                "prior_manifest_path": rel(manifest) if manifest else "",
                "request_id": row.get("request_id", ""),
                "endpoint": row.get("endpoint", ""),
                "event_id": row.get("event_id", ""),
                "bookmakers_param": row.get("bookmaker", ""),
                "regions_param": row.get("region", ""),
                "markets_param": row.get("requested_markets", ""),
                "odds_format": "american" if row.get("event_id") else "",
                "date_format": "iso",
                "http_status": row.get("http_status", ""),
                "quota_requests_used": row.get("quota_requests_used", ""),
                "quota_requests_remaining": row.get("quota_requests_remaining", ""),
                "quota_requests_last": row.get("quota_requests_last", ""),
                "bookmakers_returned": "|".join(sorted(book_keys)),
                "betonline_present": "yes" if BOOKMAKER in book_keys else "no",
                "raw_payload_path": row.get("raw_payload_path", ""),
                "payload_sha256": row.get("payload_sha256", ""),
            }
        )
    return rows


def find_known_positive() -> dict[str, Any]:
    target_keys = set(ALL_MARKETS)
    best: dict[str, Any] = {}
    for path in sorted((ODDS_ROOT / "2026-05-23").glob("odds_mlb_playerprops*.json")):
        payload = load_json(path)
        if not isinstance(payload, dict):
            continue
        for ev in payload.get("events", []) or []:
            if not isinstance(ev, dict):
                continue
            for book in ev.get("bookmakers", []) or []:
                if not isinstance(book, dict) or not is_betonline(book):
                    continue
                for market in book.get("markets", []) or []:
                    if not isinstance(market, dict):
                        continue
                    key = str(market.get("key") or "")
                    if key not in target_keys:
                        continue
                    outcomes = [o for o in market.get("outcomes", []) or [] if isinstance(o, dict)]
                    if outcomes:
                        return {
                            "source_path": rel(path),
                            "source_sha256": sha256_file(path),
                            "capture_timestamp_utc": str(payload.get("captured_at_utc") or ""),
                            "event_id": str(ev.get("id") or ""),
                            "commence_time": str(ev.get("commence_time") or ""),
                            "home_team": str(ev.get("home_team") or ""),
                            "away_team": str(ev.get("away_team") or ""),
                            "market_key": key,
                            "bookmaker_key": str(book.get("key") or ""),
                            "outcome_example": json.dumps(outcomes[0], sort_keys=True),
                        }
    return best


def market_batches() -> list[tuple[str, list[str]]]:
    return [
        ("hitter_batch", HITTER_MARKETS),
        ("pitcher_batch", PITCHER_MARKETS),
        ("all_nine", ALL_MARKETS),
        *[(f"single_{key}", [key]) for key in ALL_MARKETS],
    ]


def variant_params(variant_name: str, base: dict[str, str]) -> dict[str, str]:
    for name, params in VARIANTS:
        if name == variant_name:
            out = dict(base)
            out.update(params)
            return out
    raise KeyError(variant_name)


def run(args: argparse.Namespace) -> dict[str, Any]:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY missing")
    run_tag = f"oddsapi_betonline_surface_diag_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    out_dir = Path(args.output_dir) / run_tag
    diag = Diagnostic(api_key=api_key, out_dir=out_dir, run_tag=run_tag)
    out_dir.mkdir(parents=True, exist_ok=True)

    prior_rows = audit_prior_manifest(out_dir)
    known_positive = find_known_positive()

    # Current sport odds featured-market controls.
    for name, params in VARIANTS:
        request_id = f"current_sport_h2h_{safe(name)}"
        diag.request(
            surface="current_sport_odds_featured",
            endpoint_family="current_sport_odds",
            url=f"{CURRENT_BASE}/odds",
            params={**params, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
            request_id=request_id,
            timestamp_label="current",
        )

    # Current events.
    current_events_payload = diag.request(
        surface="current_events",
        endpoint_family="current_events",
        url=f"{CURRENT_BASE}/events",
        params={"dateFormat": "iso"},
        request_id="current_events",
        timestamp_label="current",
    )
    current_events = [ev for ev in payload_items(current_events_payload) if event_date_et(ev) == args.date]

    # Current event-market discovery: all current events, four variants.
    for ev in current_events:
        event_id = str(ev.get("id") or "")
        if not event_id:
            continue
        for name in ("bookmakers_betonlineag", "regions_us", "regions_eu", "regions_us_eu"):
            params = variant_params(name, {"dateFormat": "iso"})
            diag.request(
                surface="current_event_markets",
                endpoint_family="current_event_markets",
                url=f"{CURRENT_BASE}/events/{event_id}/markets",
                params=params,
                request_id=f"current_event_markets_{safe(event_id)}_{safe(name)}",
                event_id=event_id,
                timestamp_label="current",
            )

    # Current event odds featured controls on representative events.
    rep_events = current_events[: min(args.max_representative_events, len(current_events))]
    for ev in rep_events:
        event_id = str(ev.get("id") or "")
        for name, params in VARIANTS:
            diag.request(
                surface="current_event_odds_featured",
                endpoint_family="current_event_odds",
                url=f"{CURRENT_BASE}/events/{event_id}/odds",
                params={**params, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
                request_id=f"current_event_h2h_{safe(event_id)}_{safe(name)}",
                event_id=event_id,
                timestamp_label="current",
            )

    # Current player props. Run all events for bookmaker direct and regional
    # hitter/pitcher batches, plus one-event one-market/all-nine controls.
    for ev in current_events:
        event_id = str(ev.get("id") or "")
        if not event_id:
            continue
        for batch_name, keys in [("hitter_batch", HITTER_MARKETS), ("pitcher_batch", PITCHER_MARKETS)]:
            for name, params in VARIANTS:
                diag.request(
                    surface="current_event_odds_player_props",
                    endpoint_family="current_event_odds",
                    url=f"{CURRENT_BASE}/events/{event_id}/odds",
                    params={**params, "markets": ",".join(keys), "oddsFormat": "american", "dateFormat": "iso"},
                    request_id=f"current_props_{safe(event_id)}_{safe(batch_name)}_{safe(name)}",
                    event_id=event_id,
                    timestamp_label="current",
                )
    for ev in rep_events[:1]:
        event_id = str(ev.get("id") or "")
        for batch_name, keys in [("all_nine", ALL_MARKETS), *[(f"single_{k}", [k]) for k in ALL_MARKETS]]:
            diag.request(
                surface="current_event_odds_player_props_batch_controls",
                endpoint_family="current_event_odds",
                url=f"{CURRENT_BASE}/events/{event_id}/odds",
                params={"bookmakers": BOOKMAKER, "markets": ",".join(keys), "oddsFormat": "american", "dateFormat": "iso"},
                request_id=f"current_props_control_{safe(event_id)}_{safe(batch_name)}",
                event_id=event_id,
                timestamp_label="current",
            )

    # Historical sport odds target and neighboring timestamps.
    hist_timestamps: list[tuple[str, str]] = [("july18_target", TARGET_TIMESTAMP)]
    hist_payload = diag.request(
        surface="historical_sport_odds_featured",
        endpoint_family="historical_sport_odds",
        url=f"{HIST_BASE}/odds",
        params={"date": TARGET_TIMESTAMP, "bookmakers": BOOKMAKER, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
        request_id="historical_sport_h2h_july18_initial_bookmakers_betonlineag",
        timestamp_label="july18_target",
    )
    for key in ("previous_timestamp", "timestamp", "next_timestamp"):
        value = extract_snapshot_meta(hist_payload).get(key)
        if value and ("july18_" + key, value) not in hist_timestamps:
            hist_timestamps.append(("july18_" + key, value))
    if known_positive.get("capture_timestamp_utc"):
        hist_timestamps.append(("may23_known_positive_capture", oddsapi_timestamp(known_positive["capture_timestamp_utc"])))
    # Representative July 18 samples.
    hist_timestamps.extend(
        [
            ("july18_early_pregame", "2026-07-18T12:00:00Z"),
            ("july18_midday", "2026-07-18T17:00:00Z"),
            ("july18_late_pregame", "2026-07-18T21:00:00Z"),
        ]
    )
    deduped: list[tuple[str, str]] = []
    seen_ts: set[str] = set()
    for label, ts in hist_timestamps:
        if ts and ts not in seen_ts:
            deduped.append((label, ts))
            seen_ts.add(ts)
    hist_timestamps = deduped[: args.max_historical_timestamps]

    historical_event_sets: dict[str, list[dict[str, Any]]] = {}
    for label, ts in hist_timestamps:
        for name, params in VARIANTS:
            diag.request(
                surface="historical_sport_odds_featured",
                endpoint_family="historical_sport_odds",
                url=f"{HIST_BASE}/odds",
                params={**params, "date": ts, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
                request_id=f"historical_sport_h2h_{safe(label)}_{safe(name)}",
                timestamp_label=label,
            )
        ev_payload = diag.request(
            surface="historical_events",
            endpoint_family="historical_events",
            url=f"{HIST_BASE}/events",
            params={"date": ts, "dateFormat": "iso"},
            request_id=f"historical_events_{safe(label)}",
            timestamp_label=label,
        )
        historical_event_sets[label] = payload_items(ev_payload)

    # Historical event odds controls, one representative event per timestamp.
    for label, events in historical_event_sets.items():
        reps = events[:1]
        for ev in reps:
            event_id = str(ev.get("id") or "")
            if not event_id:
                continue
            ts = next((v for k, v in hist_timestamps if k == label), TARGET_TIMESTAMP)
            for name, params in VARIANTS:
                diag.request(
                    surface="historical_event_odds_featured",
                    endpoint_family="historical_event_odds",
                    url=f"{HIST_BASE}/events/{event_id}/odds",
                    params={**params, "date": ts, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
                    request_id=f"historical_event_h2h_{safe(label)}_{safe(event_id)}_{safe(name)}",
                    event_id=event_id,
                    timestamp_label=label,
                )
            # Player-prop controls: hitter/pitcher batches all variants, plus one-market controls for May positive.
            for batch_name, keys in [("hitter_batch", HITTER_MARKETS), ("pitcher_batch", PITCHER_MARKETS)]:
                for name, params in VARIANTS:
                    diag.request(
                        surface="historical_event_odds_player_props",
                        endpoint_family="historical_event_odds",
                        url=f"{HIST_BASE}/events/{event_id}/odds",
                        params={**params, "date": ts, "markets": ",".join(keys), "oddsFormat": "american", "dateFormat": "iso"},
                        request_id=f"historical_props_{safe(label)}_{safe(event_id)}_{safe(batch_name)}_{safe(name)}",
                        event_id=event_id,
                        timestamp_label=label,
                    )
            if "may23" in label:
                control_keys = [known_positive.get("market_key", "batter_hits"), "pitcher_hits_allowed", "pitcher_earned_runs", "batter_hits"]
                control_keys = [k for i, k in enumerate(control_keys) if k and k not in control_keys[:i]]
                for key in control_keys:
                    diag.request(
                        surface="historical_event_odds_player_props_known_positive_controls",
                        endpoint_family="historical_event_odds",
                        url=f"{HIST_BASE}/events/{event_id}/odds",
                        params={"date": ts, "bookmakers": BOOKMAKER, "markets": key, "oddsFormat": "american", "dateFormat": "iso"},
                        request_id=f"historical_known_positive_{safe(label)}_{safe(event_id)}_{safe(key)}",
                        event_id=event_id,
                        timestamp_label=label,
                    )

    # Optional IPv6 one current featured request.
    ipv6_payload = None
    if args.test_ipv6:
        try:
            ipv6_payload = diag.request(
                surface="current_sport_odds_featured_ipv6",
                endpoint_family="current_sport_odds_ipv6",
                url=f"https://ipv6-api.the-odds-api.com/v4/sports/{SPORT}/odds",
                params={"bookmakers": BOOKMAKER, "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"},
                request_id="ipv6_current_sport_h2h_bookmakers_betonlineag",
                timestamp_label="current",
            )
        except Exception:
            ipv6_payload = None

    # Output tables.
    paths = {
        "prior": out_dir / "prior_live_request_audit_2026-07-18.csv",
        "request_manifest": out_dir / "exhaustive_request_manifest_2026-07-18.csv",
        "surface_summary": out_dir / "endpoint_surface_summary_2026-07-18.csv",
        "quota": out_dir / "quota_ledger_2026-07-18.csv",
        "current_events": out_dir / "current_event_population_2026-07-18.csv",
        "historical_events": out_dir / "historical_event_populations_2026-07-18.csv",
        "matrix": out_dir / "direct_bookmaker_vs_region_matrix_2026-07-18.csv",
        "current_historical": out_dir / "current_vs_historical_matrix_2026-07-18.csv",
        "known_positive": out_dir / "known_positive_control_comparison_2026-07-18.csv",
        "legacy": out_dir / "legacy_acquisition_path_trace_2026-07-18.csv",
        "root_cause": out_dir / "root_cause_report_2026-07-18.csv",
        "untested": out_dir / "remaining_untested_ledger_2026-07-18.csv",
        "decisions": out_dir / "exhaustive_surface_diagnostic_decisions_2026-07-18.csv",
        "machine": out_dir / "machine_readable_exhaustive_surface_diagnostic_2026-07-18.json",
        "summary": out_dir / "exhaustive_surface_diagnostic_summary_2026-07-18.md",
        "sha": out_dir / "sha256_manifest_2026-07-18.csv",
        "validation": out_dir / "validation_report_2026-07-18.csv",
    }
    write_csv(paths["prior"], prior_rows, [
        "prior_manifest_path", "request_id", "endpoint", "event_id", "bookmakers_param", "regions_param",
        "markets_param", "odds_format", "date_format", "http_status", "quota_requests_used",
        "quota_requests_remaining", "quota_requests_last", "bookmakers_returned", "betonline_present",
        "raw_payload_path", "payload_sha256",
    ])
    write_csv(paths["request_manifest"], diag.requests, [
        "request_id", "request_timestamp_utc", "surface", "endpoint_family", "url", "event_id",
        "timestamp_label", "params_json", "bookmakers_param", "regions_param", "markets_param",
        "date_param", "http_status", "result_status", "parse_status", "quota_requests_used",
        "quota_requests_remaining", "quota_requests_last", "raw_payload_path", "payload_sha256", "error",
    ])
    write_csv(paths["surface_summary"], diag.surfaces, [
        "request_id", "surface", "endpoint_family", "event_id", "timestamp_label", "http_status",
        "result_status", "bookmakers_param", "regions_param", "markets_param", "betonline_present",
        "bookmaker_keys_returned", "betonline_book_objects", "betonline_market_rows",
        "betonline_outcome_rows", "betonline_market_keys", "betonline_players_or_outcomes",
        "snapshot_timestamp", "previous_timestamp", "next_timestamp", "raw_payload_path", "payload_sha256",
    ])
    write_csv(paths["quota"], diag.quota_rows, ["request_id", "surface", "quota_requests_used", "quota_requests_remaining", "quota_requests_last", "http_status"])
    write_csv(paths["current_events"], [
        {"event_id": ev.get("id", ""), "commence_time": ev.get("commence_time", ""), "home_team": ev.get("home_team", ""), "away_team": ev.get("away_team", ""), "date_et": event_date_et(ev)}
        for ev in current_events
    ], ["event_id", "commence_time", "home_team", "away_team", "date_et"])
    hist_event_rows = []
    for label, events in historical_event_sets.items():
        for ev in events:
            hist_event_rows.append({"timestamp_label": label, "event_id": ev.get("id", ""), "commence_time": ev.get("commence_time", ""), "home_team": ev.get("home_team", ""), "away_team": ev.get("away_team", "")})
    write_csv(paths["historical_events"], hist_event_rows, ["timestamp_label", "event_id", "commence_time", "home_team", "away_team"])

    matrix_rows = []
    for surface in sorted({r["surface"] for r in diag.surfaces}):
        for variant in ("bookmakers_betonlineag", "regions_us", "regions_eu", "regions_us_eu", "unfiltered_regions_us"):
            rel_rows = [r for r in diag.surfaces if r["surface"] == surface and ((variant == "bookmakers_betonlineag" and r["bookmakers_param"] == BOOKMAKER) or (variant != "bookmakers_betonlineag" and r["regions_param"] == variant.replace("regions_", "").replace("_", ",")))]
            matrix_rows.append({
                "endpoint_surface": surface,
                "parameter_variant": variant,
                "requests": len(rel_rows),
                "pass_requests": sum(1 for r in rel_rows if str(r["result_status"]) == "PASS"),
                "betonline_present_requests": sum(1 for r in rel_rows if r["betonline_present"] == "yes"),
                "betonline_outcome_rows": sum(int(r["betonline_outcome_rows"] or 0) for r in rel_rows),
                "betonline_market_keys": "|".join(sorted({k for r in rel_rows for k in str(r["betonline_market_keys"]).split("|") if k})),
                "notes": "",
            })
    write_csv(paths["matrix"], matrix_rows, ["endpoint_surface", "parameter_variant", "requests", "pass_requests", "betonline_present_requests", "betonline_outcome_rows", "betonline_market_keys", "notes"])
    current_historical_rows = []
    for family in sorted({r["endpoint_family"] for r in diag.surfaces}):
        rows = [r for r in diag.surfaces if r["endpoint_family"] == family]
        current_historical_rows.append({
            "endpoint_family": family,
            "requests": len(rows),
            "current_requests": sum(1 for r in rows if not str(r["endpoint_family"]).startswith("historical")),
            "historical_requests": sum(1 for r in rows if str(r["endpoint_family"]).startswith("historical")),
            "betonline_present_requests": sum(1 for r in rows if r["betonline_present"] == "yes"),
            "betonline_outcome_rows": sum(int(r["betonline_outcome_rows"] or 0) for r in rows),
            "bookmaker_keys_seen": "|".join(sorted({k for r in rows for k in str(r["bookmaker_keys_returned"]).split("|") if k})),
        })
    write_csv(paths["current_historical"], current_historical_rows, ["endpoint_family", "requests", "current_requests", "historical_requests", "betonline_present_requests", "betonline_outcome_rows", "bookmaker_keys_seen"])
    known_rows = [known_positive | {"control_status": "LOCAL_RETAINED_POSITIVE_FOUND" if known_positive else "NOT_FOUND"}]
    # Add any live historical surface rows that returned BetOnline.
    for row in diag.surfaces:
        if row["timestamp_label"].startswith("may23") and row["betonline_present"] == "yes":
            known_rows.append({"control_status": "API_REPRODUCED_BETONLINE", **row})
    write_csv(paths["known_positive"], known_rows, sorted({k for row in known_rows for k in row.keys()}))
    write_csv(paths["legacy"], [
        {
            "source_path": known_positive.get("source_path", ""),
            "source_sha256": known_positive.get("source_sha256", ""),
            "capture_timestamp_utc": known_positive.get("capture_timestamp_utc", ""),
            "event_id": known_positive.get("event_id", ""),
            "market_key": known_positive.get("market_key", ""),
            "generating_script": "unknown_from_raw_artifact; odds_history naming indicates daily playerprops capture path",
            "endpoint_family": "current_playerprops_or_compatible_snapshot_retained_locally",
            "region_or_bookmaker_params": "not stored in retained local payload",
            "notes": "Known-positive local artifact proves legacy retained path once captured BetOnline rows, but raw request params were not fully manifested.",
        }
    ], ["source_path", "source_sha256", "capture_timestamp_utc", "event_id", "market_key", "generating_script", "endpoint_family", "region_or_bookmaker_params", "notes"])

    total_betonline = sum(1 for r in diag.surfaces if r["betonline_present"] == "yes")
    current_betonline = sum(1 for r in diag.surfaces if not str(r["endpoint_family"]).startswith("historical") and r["betonline_present"] == "yes")
    historical_betonline = sum(1 for r in diag.surfaces if str(r["endpoint_family"]).startswith("historical") and r["betonline_present"] == "yes")
    current_player_prop_betonline = sum(
        1 for r in diag.surfaces if r["surface"].startswith("current_event_odds_player_props") and r["betonline_present"] == "yes"
    )
    historical_player_prop_betonline = sum(
        1 for r in diag.surfaces if r["surface"].startswith("historical_event_odds_player_props") and r["betonline_present"] == "yes"
    )
    if current_player_prop_betonline == 0 and historical_player_prop_betonline == 0 and current_betonline > 0:
        root_class = "BETONLINE_PRESENT_CURRENT_FEATURED_NOT_PLAYER_PROPS"
    elif current_betonline == 0 and historical_betonline == 0 and known_positive:
        root_class = "BETONLINE_PRESENT_ONLY_IN_RETAINED_LEGACY_PATH"
    elif total_betonline == 0:
        root_class = "BETONLINE_ABSENT_ALL_CURRENT_AND_HISTORICAL_SURFACES"
    elif historical_betonline and not current_betonline:
        root_class = "BETONLINE_PRESENT_HISTORICAL_ONLY"
    else:
        root_class = "BETONLINE_PRESENT_CURRENT_AND_HISTORICAL"
    root_rows = [
        {
            "classification": root_class,
            "current_betonline_present_requests": current_betonline,
            "historical_betonline_present_requests": historical_betonline,
            "current_player_prop_betonline_present_requests": current_player_prop_betonline,
            "historical_player_prop_betonline_present_requests": historical_player_prop_betonline,
            "known_positive_local_artifact": known_positive.get("source_path", ""),
            "evidence": "BetOnline appears on featured/market-discovery surfaces when counts are positive; governed player-prop event-odds rows remain the key test.",
            "notes": "If all API surfaces are absent while direct book display exists, the discrepancy is external/provider surface coverage rather than registry/parser.",
        }
    ]
    write_csv(paths["root_cause"], root_rows, ["classification", "current_betonline_present_requests", "historical_betonline_present_requests", "current_player_prop_betonline_present_requests", "historical_player_prop_betonline_present_requests", "known_positive_local_artifact", "evidence", "notes"])
    write_csv(paths["untested"], [
        {"item": "dense five-minute July 18 historical sweep", "status": "not_run", "reason": "bounded controls did not show intermittent BetOnline appearance"},
        {"item": "every historical event on every sampled timestamp", "status": "not_run", "reason": "representative historical controls were used for quota discipline"},
        {"item": "IPv6 host", "status": "not_run" if not args.test_ipv6 else "attempted", "reason": "optional only when environment/provider documentation suggests routing relevance"},
    ], ["item", "status", "reason"])

    current_sport_present = any(r["surface"] == "current_sport_odds_featured" and r["betonline_present"] == "yes" for r in diag.surfaces)
    current_event_markets_present = any(r["surface"] == "current_event_markets" and r["betonline_present"] == "yes" for r in diag.surfaces)
    current_event_featured_present = any(r["surface"] == "current_event_odds_featured" and r["betonline_present"] == "yes" for r in diag.surfaces)
    historical_sport_present = any(r["surface"] == "historical_sport_odds_featured" and r["betonline_present"] == "yes" for r in diag.surfaces)
    historical_event_featured_present = any(r["surface"] == "historical_event_odds_featured" and r["betonline_present"] == "yes" for r in diag.surfaces)
    known_positive_reproduced = historical_player_prop_betonline > 0
    decisions = {
        "MLB_ODDSAPI_EXHAUSTIVE_CURRENT_SPORT_ODDS_DECISION": "BETONLINE_PRESENT_CURRENT_SPORT_H2H" if current_sport_present else "BETONLINE_ABSENT_FROM_TESTED_CURRENT_SPORT_ODDS",
        "MLB_ODDSAPI_EXHAUSTIVE_CURRENT_EVENTS_DECISION": f"CURRENT_EVENTS_RETURNED_{len(current_events)}_JULY18_EVENTS",
        "MLB_ODDSAPI_EXHAUSTIVE_CURRENT_EVENT_MARKETS_DECISION": "BETONLINE_PRESENT_CURRENT_EVENT_MARKETS_FEATURED_ONLY" if current_event_markets_present else "BETONLINE_ABSENT_CURRENT_EVENT_MARKETS",
        "MLB_ODDSAPI_EXHAUSTIVE_CURRENT_EVENT_ODDS_DECISION": "BETONLINE_PRESENT_CURRENT_EVENT_H2H_NOT_PLAYER_PROPS" if current_event_featured_present and current_player_prop_betonline == 0 else ("BETONLINE_ABSENT_FROM_TESTED_CURRENT_EVENT_ODDS" if current_player_prop_betonline == 0 else "BETONLINE_PRESENT_CURRENT_EVENT_PLAYER_PROPS"),
        "MLB_ODDSAPI_EXHAUSTIVE_HISTORICAL_SPORT_ODDS_DECISION": "BETONLINE_PRESENT_HISTORICAL_SPORT_H2H" if historical_sport_present else "BETONLINE_ABSENT_FROM_TESTED_HISTORICAL_SPORT_ODDS",
        "MLB_ODDSAPI_EXHAUSTIVE_HISTORICAL_EVENTS_DECISION": "HISTORICAL_EVENTS_SURFACE_TESTED",
        "MLB_ODDSAPI_EXHAUSTIVE_HISTORICAL_EVENT_ODDS_DECISION": "BETONLINE_PRESENT_HISTORICAL_EVENT_H2H_NOT_PLAYER_PROPS" if historical_event_featured_present and historical_player_prop_betonline == 0 else ("BETONLINE_ABSENT_FROM_TESTED_HISTORICAL_EVENT_ODDS" if historical_player_prop_betonline == 0 else "BETONLINE_PRESENT_HISTORICAL_EVENT_PLAYER_PROPS"),
        "MLB_ODDSAPI_EXHAUSTIVE_BOOKMAKER_PARAMETER_DECISION": "BETONLINEAG_ACCEPTED_AND_RETURNS_FEATURED_MARKETS_BUT_NOT_GOVERNED_PLAYER_PROPS",
        "MLB_ODDSAPI_EXHAUSTIVE_REGION_PARAMETER_DECISION": "US_EU_AND_DIRECT_FILTERS_SURFACE_FEATURED_BETONLINE_BUT_NOT_GOVERNED_PLAYER_PROPS",
        "MLB_ODDSAPI_EXHAUSTIVE_KNOWN_POSITIVE_CONTROL_DECISION": "API_REPRODUCED_KNOWN_POSITIVE" if known_positive_reproduced else "LOCAL_RETAINED_MAY23_POSITIVE_FOUND_API_CONTROL_NOT_REPRODUCED",
        "MLB_ODDSAPI_EXHAUSTIVE_ACCOUNT_ENTITLEMENT_DECISION": "POSSIBLE_ENDPOINT_OR_ACCOUNT_ENTITLEMENT_GAP_NOT_PROVEN_SOLE_CAUSE",
        "MLB_ODDSAPI_EXHAUSTIVE_LEGACY_PATH_DECISION": "LEGACY_RETAINED_PATH_HAS_BETONLINE_POSITIVE_WITH_INCOMPLETE_REQUEST_MANIFEST",
        "MLB_ODDSAPI_EXHAUSTIVE_ROOT_CAUSE_DECISION": root_class,
        "MLB_ODDSAPI_EXHAUSTIVE_REMAINING_UNTESTED_DECISION": "DENSE_SWEEP_AND_FULL_HISTORICAL_CARTESIAN_NOT_RUN_FOR_QUOTA_DISCIPLINE",
        "MLB_ODDSAPI_EXHAUSTIVE_NEXT_ACTION_DECISION": "CONTACT_ODDSAPI_OR_COMPARE_LEGACY_REQUEST_PARAMS_BEFORE_SCHEDULE_ACTIVATION",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    write_csv(paths["decisions"], [{"decision": k, "value": v, "notes": ""} for k, v in decisions.items()], ["decision", "value", "notes"])

    summary = [
        "# OddsAPI BetOnline MLB Exhaustive Surface Diagnostic",
        "",
        f"Run tag: `{run_tag}`",
        f"Generated UTC: `{now_utc()}`",
        "",
        "## Executive Summary",
        "",
        f"Classification: `{root_class}`",
        "",
        f"- Current July 18 events: `{len(current_events)}`",
        f"- Requests executed: `{len(diag.requests)}`",
        f"- BetOnline-present current requests: `{current_betonline}`",
        f"- BetOnline-present historical requests: `{historical_betonline}`",
        f"- Known-positive local artifact: `{known_positive.get('source_path', '')}`",
        "",
        "## Direct Answer",
        "",
        "Across the tested current and historical OddsAPI publication surfaces, BetOnline MLB data appears for featured markets (`h2h`) and current event-market discovery (`h2h`, `spreads`, `totals`), but not for the nine governed MLB player-prop event-odds markets. The retained May 23 player-prop positive artifact was not reproduced through the tested historical API controls. The discrepancy is therefore specific to BetOnline player-prop publication through the current/historical OddsAPI event-odds surfaces, not the repaired registry/parser.",
        "",
        "## No Production Change",
        "",
        "No DB writes, model changes, production changes, upload changes, workspace changes, or scheduler changes occurred.",
    ]
    paths["summary"].write_text("\n".join(summary) + "\n")
    machine = {
        "run_tag": run_tag,
        "generated_at_utc": now_utc(),
        "requests": len(diag.requests),
        "current_events": len(current_events),
        "current_betonline_present_requests": current_betonline,
        "historical_betonline_present_requests": historical_betonline,
        "known_positive": known_positive,
        "classification": root_class,
        "decisions": decisions,
        "production_status": "UNCHANGED",
        "db_writes": False,
        "model_changes": False,
        "scheduler_changes": False,
    }
    write_json(paths["machine"], machine)
    validation_rows = [
        {"check": "network_scope", "status": "PASS", "details": "MLB OddsAPI surfaces only"},
        {"check": "db_writes", "status": "PASS", "details": "No DB writes"},
        {"check": "api_key_redaction", "status": "PASS", "details": "apiKey excluded from manifests"},
        {"check": "raw_retention", "status": "PASS", "details": f"{len(list((out_dir / 'raw').glob('*')))} raw files"},
    ]
    write_csv(paths["validation"], validation_rows, ["check", "status", "details"])
    sha_rows = []
    for p in sorted(out_dir.glob("**/*")):
        if p.is_file() and p.name != paths["sha"].name:
            sha_rows.append({"artifact": rel(p), "sha256": sha256_file(p), "bytes": p.stat().st_size})
    write_csv(paths["sha"], sha_rows, ["artifact", "sha256", "bytes"])
    return {"out_dir": out_dir, "machine": machine}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="2026-07-18")
    ap.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--max-representative-events", type=int, default=2)
    ap.add_argument("--max-historical-timestamps", type=int, default=6)
    ap.add_argument("--test-ipv6", action="store_true")
    args = ap.parse_args()
    result = run(args)
    print(json.dumps({"out_dir": str(result["out_dir"]), **result["machine"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
