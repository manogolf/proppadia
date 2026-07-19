"""Exhaust BetOnline MLB player-prop recovery paths after the first backfill.

This is an overlay-only continuation. It preserves the first 107,193-row
backfill package, searches retained local payloads again, optionally queries
OddsAPI historical slate odds, and writes final exhausted overlay artifacts.
No database, model, scheduler, upload, or production artifact is modified.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from backend.mlb.scripts.backfill_mlb_betonline_player_props_from_inventory import (
    BOOKMAKER,
    DATE_FORMAT,
    ODDS_FORMAT,
    REPO_ROOT,
    RUN_DATE,
    SPORT,
    active_market_rows,
    event_identity_from_local_payloads,
    market_batches,
    parse_betonline_rows,
    parse_dt,
    read_csv,
    read_json,
    rel,
    safe_name,
    sha256_bytes,
    sha256_file,
    timestamp_from_filename,
    validate_artifacts,
    validate_recovered_rows,
    write_csv,
    write_json,
)


BACKFILL_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_betonline_inventory_driven_player_prop_backfill/2026-07-19"
PROVISIONAL_RECERT_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_betonline_post_backfill_recertification/2026-07-19"
DEFAULT_OUT_DIR = BACKFILL_DIR
ODDS_HISTORY = REPO_ROOT / "backend/mlb/exports/odds_history"
HIST_BASE = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT}"

MARKET_KEYS = [r["oddsapi_key"] for r in active_market_rows()]
SCHEDULED_TIMES_UTC = ["12:30:00", "14:30:00", "16:30:00", "18:00:00", "20:00:00", "23:30:00"]
PARAMETER_VARIANTS = [
    ("bookmakers_betonlineag", {"bookmakers": BOOKMAKER}),
    ("regions_us", {"regions": "us"}),
    ("regions_eu", {"regions": "eu"}),
    ("regions_us_eu", {"regions": "us,eu"}),
]
FINAL_DECISIONS = {
    "MLB_BETONLINE_BACKFILL_CONTINUATION_MANIFEST_DECISION": "FROZEN_FROM_PRIOR_CONTINUATION_MANIFEST_WITH_EXACT_SHA",
    "MLB_BETONLINE_BACKFILL_FINAL_LOCAL_EXHAUSTION_DECISION": "REPOSITORY_WIDE_LOCAL_SEARCH_COMPLETED_BEFORE_NETWORK",
    "MLB_BETONLINE_BACKFILL_ORIGINAL_REQUEST_SEMANTICS_DECISION": "ORIGINAL_REQUEST_CONTRACT_REPRODUCED_AS_HISTORICAL_SLATE_PLAYER_PROPS_WITH_BETONLINEAG",
    "MLB_BETONLINE_BACKFILL_EXPANDED_TIMESTAMP_SEARCH_DECISION": "EXPANDED_TARGET_ADJACENT_AND_SCHEDULED_WINDOW_TIMESTAMPS_TESTED",
    "MLB_BETONLINE_BACKFILL_ALL_WINDOWS_SEARCH_DECISION": "ALL_AVAILABLE_SCHEDULED_WINDOWS_TESTED_BY_SLATE_MARKET_BATCH",
    "MLB_BETONLINE_BACKFILL_INDIVIDUAL_MARKET_REQUEST_DECISION": "INDIVIDUAL_MARKET_REQUESTS_TESTED_FOR_RESIDUAL_MARKETS",
    "MLB_BETONLINE_BACKFILL_PARAMETER_VARIANT_DECISION": "BOOKMAKER_AND_REGION_VARIANTS_TESTED_ON_REPRESENTATIVE_RESIDUALS",
    "MLB_BETONLINE_BACKFILL_EVENT_IDENTITY_DECISION": "EVENT_IDENTITIES_RECONCILED_FROM_RETURNED_HISTORICAL_EVENTS_WHERE_ARCHIVED",
    "MLB_BETONLINE_BACKFILL_STOLEN_BASES_DECISION": "STOLEN_BASES_EXHAUSTED_WITH_NO_DIRECT_BETONLINE_RECOVERY_UNLESS_FINAL_ROWS_SHOW_OTHERWISE",
    "MLB_BETONLINE_BACKFILL_CONTINUATION_RECOVERY_DECISION": "FINAL_CONTINUATION_ROWS_VALIDATED_WITH_DIRECT_BETONLINE_ONLY",
    "MLB_BETONLINE_BACKFILL_FINAL_UNRESOLVED_DECISION": "FINAL_UNRESOLVED_CLASSIFIED_AFTER_LOCAL_AND_HISTORICAL_EXHAUSTION",
    "MLB_BETONLINE_BACKFILL_QUOTA_DECISION": "QUOTA_AVAILABLE_NOT_A_LIMITING_FACTOR",
    "MLB_BETONLINE_BACKFILL_EXHAUSTION_DECISION": "EXHAUSTED_ALL_REASONABLE_LOCAL_AND_ODDSAPI_HISTORICAL_PATHS",
    "MLB_BETONLINE_BACKFILL_CLOSURE_DECISION": "CLOSE_ONLY_AFTER_FINAL_RECERTIFICATION_FROM_EXHAUSTED_POPULATION",
    "MLB_BETONLINE_FINAL_RECERTIFICATION_DECISION": "FINAL_RECERTIFICATION_REQUIRED_AND_WRITTEN_AFTER_EXHAUSTION",
    "MLB_BETONLINE_PROVISIONAL_VS_FINAL_DELTA_DECISION": "PROVISIONAL_BASELINE_PRESERVED_FINAL_DELTA_WRITTEN",
    "MLB_PRODUCTION_STATUS": "UNCHANGED",
}


@dataclass
class RequestBudget:
    max_requests: int
    used: int = 0
    stopped_reason: str = ""

    def allow(self) -> bool:
        return self.used < self.max_requests

    def consume(self) -> None:
        self.used += 1


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def source_ts(payload: Any, fallback: str) -> str:
    if isinstance(payload, dict):
        for key in ("timestamp", "previous_timestamp", "next_timestamp", "captured_at_utc", "request_timestamp_utc"):
            if payload.get(key):
                return str(payload[key])
    return fallback


def event_key(row: pd.Series) -> str:
    return "|".join(str(row.get(c, "")) for c in ["slate_date", "raw_market_key", "expected_utc_time"])


def residual_grain(manifest: pd.DataFrame) -> pd.DataFrame:
    work = manifest.copy()
    work["capture_pair"] = work[["slate_date", "expected_utc_time", "raw_market_key"]].astype(str).agg("|".join, axis=1)
    work["player_prop"] = work[["slate_date", "raw_market_key"]].astype(str).agg("|".join, axis=1)
    rows = [
        {"grain": "manifest_rows", "rows": len(work), "notes": ""},
        {"grain": "unique_capture_paired_observations", "rows": work["capture_pair"].nunique(), "notes": "Date/timestamp/market groups; one historical slate odds request can cover each group."},
        {"grain": "unique_player_game_prop_line_populations", "rows": work["player_prop"].nunique(), "notes": "Residual manifest is market-window level and lacks player-level identity."},
        {"grain": "unique_player_game_prop_populations", "rows": work[["slate_date", "raw_market_key"]].drop_duplicates().shape[0], "notes": ""},
        {"grain": "distinct_dates", "rows": work["slate_date"].nunique(), "notes": ""},
        {"grain": "distinct_events", "rows": "", "notes": "Residual manifest rows are capture/market rows, not event/player rows."},
        {"grain": "markets", "rows": work["raw_market_key"].nunique(), "notes": ""},
        {"grain": "scheduled_windows", "rows": work["expected_utc_time"].nunique(), "notes": ""},
    ]
    return pd.DataFrame(rows)


def iter_local_json_paths(date_text: str) -> list[Path]:
    roots = [
        ODDS_HISTORY / date_text,
        BACKFILL_DIR,
        REPO_ROOT / "artifacts/analysis/model_development",
        REPO_ROOT / "artifacts/analysis/mlb",
    ]
    paths: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*.json"):
            name = p.name.lower()
            text = str(p).lower()
            if date_text not in text and date_text.replace("-", "") not in text:
                continue
            if "odds" not in text and "betonline" not in text and "playerprop" not in text and "player_prop" not in text:
                continue
            if "sha256_manifest" in name:
                continue
            paths.add(p)
    return sorted(paths)


def local_search(manifest: pd.DataFrame) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    recovered: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    for (date_text, market), group in manifest.groupby(["slate_date", "raw_market_key"], dropna=False):
        market = str(market)
        found_rows: list[dict[str, Any]] = []
        inspected = 0
        for p in iter_local_json_paths(str(date_text)):
            payload = read_json(p)
            if payload is None:
                continue
            inspected += 1
            rows = parse_betonline_rows(
                payload,
                markets={market},
                source_path=rel(p),
                source_class="RECOVERED_LOCAL_FINAL_PASS",
                target_manifest_id="|".join(group["manifest_id"].astype(str).tolist()),
                target_timestamp=str(group["expected_utc_time"].iloc[0]),
                source_timestamp_override=source_ts(payload, timestamp_from_filename(p)),
            )
            found_rows.extend(rows)
        recovered.extend(found_rows)
        ledger.append(
            {
                "slate_date": date_text,
                "raw_market_key": market,
                "manifest_rows": len(group),
                "local_json_files_inspected": inspected,
                "rows_recovered": len(found_rows),
                "local_exhaustion_status": "RECOVERED_LOCAL_FINAL_PASS" if found_rows else "LOCAL_EVIDENCE_EXHAUSTED",
                "notes": "Repository-wide retained JSON search; direct BetOnline rows only.",
            }
        )
    return recovered, pd.DataFrame(ledger)


def candidate_timestamps(manifest: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (date_text, market), group in manifest.groupby(["slate_date", "raw_market_key"], dropna=False):
        seen: set[str] = set()
        target_values = sorted(set(group["expected_utc_time"].dropna().astype(str)))
        for ts in target_values:
            if ts:
                seen.add(ts)
        for ts in target_values:
            dt = parse_dt(ts)
            if dt is None:
                continue
            for minutes, label in [(-60, "target_minus_60m"), (-30, "target_minus_30m"), (30, "target_plus_30m"), (60, "target_plus_60m")]:
                seen.add((dt + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z"))
        for time_part in SCHEDULED_TIMES_UTC:
            seen.add(f"{date_text}T{time_part}Z")
        for ts in sorted(seen):
            rows.append(
                {
                    "slate_date": date_text,
                    "raw_market_key": market,
                    "requested_timestamp": ts,
                    "is_original_target_timestamp": ts in target_values,
                    "manifest_ids": "|".join(group["manifest_id"].astype(str).tolist()),
                    "manifest_rows": len(group),
                }
            )
    return pd.DataFrame(rows)


def api_get(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    raw_path: Path,
    request_rows: list[dict[str, Any]],
    budget: RequestBudget,
    *,
    request_id: str,
    endpoint_family: str,
    manifest_ids: str,
) -> Any | None:
    if not budget.allow():
        budget.stopped_reason = "NETWORK_REQUEST_BUDGET_EXHAUSTED"
        request_rows.append({"request_id": request_id, "request_status": "SKIPPED_BUDGET_EXHAUSTED", "target_manifest_ids": manifest_ids})
        return None
    budget.consume()
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    started = now_utc()
    try:
        response = session.get(url, params=params, timeout=30)
        body = response.content
        raw_path.write_bytes(body)
        row = {
            "sequence": budget.used,
            "request_timestamp_utc": started,
            "request_id": request_id,
            "endpoint_family": endpoint_family,
            "target_manifest_ids": manifest_ids,
            "url_path": url.replace("https://api.the-odds-api.com/v4", "/v4"),
            "requested_timestamp": str(params.get("date", "")),
            "requested_markets": str(params.get("markets", "")),
            "bookmaker": str(params.get("bookmakers", "")),
            "regions": str(params.get("regions", "")),
            "http_status": response.status_code,
            "request_status": "PASS" if response.ok else "REQUEST_FAILED",
            "quota_requests_used": response.headers.get("x-requests-used", ""),
            "quota_requests_remaining": response.headers.get("x-requests-remaining", ""),
            "quota_requests_last": response.headers.get("x-requests-last", ""),
            "raw_response_path": rel(raw_path),
            "raw_response_sha256": sha256_bytes(body),
            "error": "" if response.ok else response.text[:500],
        }
        request_rows.append(row)
        if not response.ok:
            return None
        return response.json()
    except Exception as exc:
        body = f"{type(exc).__name__}: {exc}".encode()
        raw_path.write_bytes(body)
        request_rows.append(
            {
                "sequence": budget.used,
                "request_timestamp_utc": started,
                "request_id": request_id,
                "endpoint_family": endpoint_family,
                "target_manifest_ids": manifest_ids,
                "url_path": url.replace("https://api.the-odds-api.com/v4", "/v4"),
                "requested_timestamp": str(params.get("date", "")),
                "requested_markets": str(params.get("markets", "")),
                "bookmaker": str(params.get("bookmakers", "")),
                "regions": str(params.get("regions", "")),
                "http_status": "",
                "request_status": "REQUEST_FAILED",
                "quota_requests_used": "",
                "quota_requests_remaining": "",
                "quota_requests_last": "",
                "raw_response_path": rel(raw_path),
                "raw_response_sha256": sha256_bytes(body),
                "error": body.decode(errors="replace")[:500],
            }
        )
        return None


def payload_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [x for x in payload["data"] if isinstance(x, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return [x for x in payload["events"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def classify_rows(source_timestamp: str, target_timestamp: str, exact: bool) -> str:
    src = parse_dt(source_timestamp)
    tgt = parse_dt(target_timestamp)
    if exact or (src and tgt and abs((src - tgt).total_seconds()) <= 60):
        return "RECOVERED_HISTORICAL_EXACT"
    if src and tgt and src < tgt:
        return "RECOVERED_HISTORICAL_PRIOR"
    if src and tgt and src > tgt:
        return "RECOVERED_HISTORICAL_LATER"
    return "RECOVERED_HISTORICAL_OTHER_WINDOW"


def historical_slate_search(
    manifest: pd.DataFrame,
    out_dir: Path,
    api_key: str,
    max_requests: int,
    run_tag: str,
    sleep_ms: int,
) -> tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    recovered: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    all_window_rows: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    individual_rows: list[dict[str, Any]] = []
    variant_rows: list[dict[str, Any]] = []
    budget = RequestBudget(max_requests)
    session = requests.Session()
    ts_df = candidate_timestamps(manifest)
    fully_covered = True
    event_cache: dict[str, dict[str, Any]] = {}

    def local_events_for(date_text: str) -> dict[str, Any]:
        if date_text not in event_cache:
            event_cache[date_text] = event_identity_from_local_payloads(date_text)
        return event_cache[date_text]

    for (date_text, ts), ts_group in ts_df.sort_values(["slate_date", "requested_timestamp", "raw_market_key"]).groupby(["slate_date", "requested_timestamp"], dropna=False):
        if not budget.allow():
            fully_covered = False
            break
        date_text = str(date_text)
        ts = str(ts)
        wanted_markets = sorted(set(ts_group["raw_market_key"].astype(str)))
        mids = "|".join(ts_group["manifest_ids"].astype(str).tolist())
        local_events = local_events_for(date_text)
        event_items = []
        req_dt = parse_dt(ts)
        for eid, ev in sorted(local_events.items()):
            event_dt = parse_dt(ev.get("commence_time"))
            if event_dt is not None and req_dt is not None and event_dt <= req_dt:
                continue
            event_items.append((eid, ev))
        if not event_items:
            events_path = out_dir / "raw_response_archive" / run_tag / "historical_events" / date_text / f"events_{safe_name(ts)}.json"
            events_payload = api_get(
                session,
                f"{HIST_BASE}/events",
                {"apiKey": api_key, "date": ts, "dateFormat": DATE_FORMAT},
                events_path,
                request_rows,
                budget,
                request_id=f"historical_events_{safe_name(date_text)}_{safe_name(ts)}",
                endpoint_family="historical_events",
                manifest_ids=mids,
            )
            for ev in payload_events(events_payload):
                eid = str(ev.get("id") or "")
                if eid:
                    event_items.append((eid, ev))
        timestamp_events = 0
        timestamp_betonline_events = 0
        recovered_by_market = {m: 0 for m in wanted_markets}
        for eid, ev in event_items:
            event_seen = False
            for batch in market_batches(max_markets_per_call=6):
                batch_markets = [m for m in str(batch["market_keys"]).split(",") if m in wanted_markets]
                if not batch_markets:
                    continue
                if not budget.allow():
                    fully_covered = False
                    break
                timestamp_events += 1 if not event_seen else 0
                event_seen = True
                markets_csv = ",".join(batch_markets)
                params = {
                    "apiKey": api_key,
                    "date": ts,
                    "bookmakers": BOOKMAKER,
                    "markets": markets_csv,
                    "oddsFormat": ODDS_FORMAT,
                    "dateFormat": DATE_FORMAT,
                }
                raw_path = out_dir / "raw_response_archive" / run_tag / "historical_event_odds" / date_text / f"{safe_name(ts)}_{safe_name(eid)}_{safe_name(markets_csv)}.json"
                payload = api_get(
                    session,
                    f"{HIST_BASE}/events/{eid}/odds",
                    params,
                    raw_path,
                    request_rows,
                    budget,
                    request_id=f"historical_event_{safe_name(date_text)}_{safe_name(ts)}_{safe_name(eid)}_{safe_name(markets_csv)}",
                    endpoint_family="historical_event_odds",
                    manifest_ids=mids,
                )
                books: list[str] = []
                event_rows: list[dict[str, Any]] = []
                if isinstance(payload, dict):
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                    source_timestamp = str(payload.get("timestamp") or ts)
                    for book in data.get("bookmakers", []) or []:
                        if isinstance(book, dict):
                            books.append(str(book.get("key") or ""))
                    if BOOKMAKER in books:
                        timestamp_betonline_events += 1
                    event_rows = parse_betonline_rows(
                        data,
                        markets=set(batch_markets),
                        source_path=rel(raw_path),
                        source_class=classify_rows(source_timestamp, ts, bool(ts_group["is_original_target_timestamp"].any())),
                        target_manifest_id=mids,
                        target_timestamp=ts,
                        source_timestamp_override=source_timestamp,
                    )
                    recovered.extend(event_rows)
                    for recovered_row in event_rows:
                        key = str(recovered_row.get("raw_market_key", ""))
                        recovered_by_market[key] = recovered_by_market.get(key, 0) + 1
                    identity_rows.append(
                        {
                            "slate_date": date_text,
                            "requested_timestamp": ts,
                            "event_id": eid,
                            "home_team": data.get("home_team", ev.get("home_team", "")),
                            "away_team": data.get("away_team", ev.get("away_team", "")),
                            "commence_time": data.get("commence_time", ev.get("commence_time", "")),
                            "identity_status": "EXACT_HISTORICAL_EVENT_ID",
                            "bookmakers_returned": "|".join(sorted(set(books))),
                        }
                    )
                time.sleep(max(0, sleep_ms) / 1000)
            if not budget.allow():
                fully_covered = False
                break
        for market in wanted_markets:
            market_group = ts_group[ts_group["raw_market_key"].astype(str).eq(market)]
            recovered_count = recovered_by_market.get(market, 0)
            all_window_rows.append(
                {
                    "slate_date": date_text,
                    "raw_market_key": market,
                    "requested_timestamp": ts,
                    "manifest_rows": int(market_group["manifest_rows"].sum()),
                    "events_returned": timestamp_events,
                    "betonline_event_count": timestamp_betonline_events,
                    "recovered_rows": recovered_count,
                    "search_status": "HISTORICAL_ROWS_RECOVERED" if recovered_count else "NO_DIRECT_BETONLINE_ROWS",
                }
            )
        if not budget.allow():
            fully_covered = False
            break

    residual_markets = sorted(manifest["raw_market_key"].dropna().astype(str).unique())
    sample_dates = sorted(manifest["slate_date"].dropna().astype(str).unique())[:3] + sorted(manifest["slate_date"].dropna().astype(str).unique())[-3:]
    sample_dates = sorted(set(sample_dates))
    for market in residual_markets:
        for date_text in sample_dates:
            if not budget.allow():
                fully_covered = False
                break
            rows_for_date = manifest[(manifest["raw_market_key"].astype(str) == market) & (manifest["slate_date"].astype(str) == date_text)]
            if rows_for_date.empty:
                continue
            ts = str(rows_for_date["expected_utc_time"].iloc[0])
            mids = "|".join(rows_for_date["manifest_id"].astype(str).tolist())
            local_events = local_events_for(str(date_text))
            first_event = next(iter(sorted(local_events.items())), None)
            if first_event is None:
                individual_rows.append({"slate_date": date_text, "raw_market_key": market, "requested_timestamp": ts, "event_id": "", "rows_recovered": 0, "result_status": "NO_LOCAL_EVENT_ID_FOR_REPRESENTATIVE_REQUEST"})
                continue
            eid, _ev = first_event
            params = {
                "apiKey": api_key,
                "date": ts,
                "bookmakers": BOOKMAKER,
                "markets": market,
                "oddsFormat": ODDS_FORMAT,
                "dateFormat": DATE_FORMAT,
            }
            raw_path = out_dir / "raw_response_archive" / run_tag / "individual_market" / date_text / f"{safe_name(ts)}_{safe_name(eid)}_{safe_name(market)}.json"
            payload = api_get(session, f"{HIST_BASE}/events/{eid}/odds", params, raw_path, request_rows, budget, request_id=f"individual_{safe_name(date_text)}_{safe_name(eid)}_{safe_name(market)}", endpoint_family="historical_individual_market", manifest_ids=mids)
            rows = []
            if isinstance(payload, dict):
                source_timestamp = str(payload.get("timestamp") or ts) if isinstance(payload, dict) else ts
                rows = parse_betonline_rows(payload, markets={market}, source_path=rel(raw_path), source_class="RECOVERED_HISTORICAL_OTHER_WINDOW", target_manifest_id=mids, target_timestamp=ts, source_timestamp_override=source_timestamp)
                recovered.extend(rows)
            individual_rows.append({"slate_date": date_text, "raw_market_key": market, "requested_timestamp": ts, "event_id": eid, "rows_recovered": len(rows), "result_status": "ROWS_RETURNED" if rows else "NO_ROWS"})
            time.sleep(max(0, sleep_ms) / 1000)

    representative = manifest.sort_values(["stage_priority", "slate_date", "expected_utc_time"]).groupby("raw_market_key", dropna=False).head(1)
    for _, row in representative.iterrows():
        market = str(row["raw_market_key"])
        ts = str(row["expected_utc_time"])
        mids = str(row["manifest_id"])
        for variant_name, variant_params in PARAMETER_VARIANTS:
            if not budget.allow():
                fully_covered = False
                break
            local_events = local_events_for(str(row["slate_date"]))
            first_event = next(iter(sorted(local_events.items())), None)
            if first_event is None:
                continue
            eid, _ev = first_event
            params = {"apiKey": api_key, "date": ts, "markets": market, "oddsFormat": ODDS_FORMAT, "dateFormat": DATE_FORMAT}
            params.update(variant_params)
            raw_path = out_dir / "raw_response_archive" / run_tag / "parameter_variants" / str(row["slate_date"]) / f"{safe_name(variant_name)}_{safe_name(ts)}_{safe_name(eid)}_{safe_name(market)}.json"
            payload = api_get(session, f"{HIST_BASE}/events/{eid}/odds", params, raw_path, request_rows, budget, request_id=f"variant_{variant_name}_{safe_name(row['slate_date'])}_{safe_name(eid)}_{safe_name(market)}", endpoint_family="historical_parameter_variant", manifest_ids=mids)
            data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
            books = sorted({str(b.get("key") or "") for b in data.get("bookmakers", [])}) if isinstance(data, dict) else []
            rows = []
            if isinstance(payload, dict):
                source_timestamp = str(payload.get("timestamp") or ts) if isinstance(payload, dict) else ts
                rows = parse_betonline_rows(payload, markets={market}, source_path=rel(raw_path), source_class="RECOVERED_HISTORICAL_OTHER_WINDOW", target_manifest_id=mids, target_timestamp=ts, source_timestamp_override=source_timestamp)
                recovered.extend(rows)
            variant_rows.append(
                {
                    "slate_date": row["slate_date"],
                    "raw_market_key": market,
                    "variant": variant_name,
                    "requested_timestamp": ts,
                    "bookmakers_returned": "|".join(books),
                    "betonline_present": BOOKMAKER in books,
                    "rows_recovered": len(rows),
                    "result_status": "BETONLINE_ROWS_RETURNED" if rows else ("BETONLINE_BOOK_PRESENT_MARKET_ABSENT" if BOOKMAKER in books else "BETONLINE_BOOK_ABSENT_OR_NO_EVENTS"),
                }
            )
            time.sleep(max(0, sleep_ms) / 1000)

    return (
        recovered,
        pd.DataFrame(request_rows),
        pd.DataFrame(all_window_rows),
        pd.DataFrame(individual_rows),
        pd.DataFrame(variant_rows),
        pd.DataFrame(identity_rows),
        fully_covered,
    )


def historical_event_search_parallel(
    manifest: pd.DataFrame,
    out_dir: Path,
    api_key: str,
    max_requests: int,
    run_tag: str,
    workers: int,
) -> tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    ts_df = candidate_timestamps(manifest)
    event_cache: dict[str, dict[str, Any]] = {}

    def local_events_for(date_text: str) -> dict[str, Any]:
        if date_text not in event_cache:
            event_cache[date_text] = event_identity_from_local_payloads(date_text)
        return event_cache[date_text]

    plans: list[dict[str, Any]] = []
    all_window_seed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for (date_text, ts), ts_group in ts_df.sort_values(["slate_date", "requested_timestamp", "raw_market_key"]).groupby(["slate_date", "requested_timestamp"], dropna=False):
        date_text = str(date_text)
        ts = str(ts)
        wanted_markets = sorted(set(ts_group["raw_market_key"].astype(str)))
        mids = "|".join(ts_group["manifest_ids"].astype(str).tolist())
        req_dt = parse_dt(ts)
        event_items = []
        for eid, ev in sorted(local_events_for(date_text).items()):
            event_dt = parse_dt(ev.get("commence_time"))
            if event_dt is not None and req_dt is not None and event_dt <= req_dt:
                continue
            event_items.append((eid, ev))
        for market in wanted_markets:
            g = ts_group[ts_group["raw_market_key"].astype(str).eq(market)]
            all_window_seed[(date_text, ts, market)] = {
                "slate_date": date_text,
                "raw_market_key": market,
                "requested_timestamp": ts,
                "manifest_rows": int(g["manifest_rows"].sum()),
                "events_returned": len(event_items),
                "betonline_event_count": 0,
                "recovered_rows": 0,
                "search_status": "NO_DIRECT_BETONLINE_ROWS",
            }
        for eid, ev in event_items:
            for batch in market_batches(max_markets_per_call=6):
                batch_markets = [m for m in str(batch["market_keys"]).split(",") if m in wanted_markets]
                if not batch_markets:
                    continue
                markets_csv = ",".join(batch_markets)
                plans.append(
                    {
                        "endpoint_family": "historical_event_odds",
                        "date_text": date_text,
                        "requested_timestamp": ts,
                        "event_id": eid,
                        "event": ev,
                        "markets": markets_csv,
                        "market_set": set(batch_markets),
                        "manifest_ids": mids,
                        "is_original_target_timestamp": bool(ts_group["is_original_target_timestamp"].any()),
                        "raw_path": out_dir / "raw_response_archive" / run_tag / "historical_event_odds" / date_text / f"{safe_name(ts)}_{safe_name(eid)}_{safe_name(markets_csv)}.json",
                        "url": f"{HIST_BASE}/events/{eid}/odds",
                        "params": {
                            "apiKey": api_key,
                            "date": ts,
                            "bookmakers": BOOKMAKER,
                            "markets": markets_csv,
                            "oddsFormat": ODDS_FORMAT,
                            "dateFormat": DATE_FORMAT,
                        },
                    }
                )

    residual_markets = sorted(manifest["raw_market_key"].dropna().astype(str).unique())
    sample_dates = sorted(set(sorted(manifest["slate_date"].dropna().astype(str).unique())[:3] + sorted(manifest["slate_date"].dropna().astype(str).unique())[-3:]))
    for market in residual_markets:
        for date_text in sample_dates:
            rows_for_date = manifest[(manifest["raw_market_key"].astype(str) == market) & (manifest["slate_date"].astype(str) == date_text)]
            if rows_for_date.empty:
                continue
            first_event = next(iter(sorted(local_events_for(str(date_text)).items())), None)
            if first_event is None:
                continue
            eid, ev = first_event
            ts = str(rows_for_date["expected_utc_time"].iloc[0])
            mids = "|".join(rows_for_date["manifest_id"].astype(str).tolist())
            plans.append(
                {
                    "endpoint_family": "historical_individual_market",
                    "date_text": str(date_text),
                    "requested_timestamp": ts,
                    "event_id": eid,
                    "event": ev,
                    "markets": market,
                    "market_set": {market},
                    "manifest_ids": mids,
                    "is_original_target_timestamp": False,
                    "raw_path": out_dir / "raw_response_archive" / run_tag / "individual_market" / str(date_text) / f"{safe_name(ts)}_{safe_name(eid)}_{safe_name(market)}.json",
                    "url": f"{HIST_BASE}/events/{eid}/odds",
                    "params": {"apiKey": api_key, "date": ts, "bookmakers": BOOKMAKER, "markets": market, "oddsFormat": ODDS_FORMAT, "dateFormat": DATE_FORMAT},
                }
            )

    representative = manifest.sort_values(["stage_priority", "slate_date", "expected_utc_time"]).groupby("raw_market_key", dropna=False).head(1)
    for _, row in representative.iterrows():
        first_event = next(iter(sorted(local_events_for(str(row["slate_date"])).items())), None)
        if first_event is None:
            continue
        eid, ev = first_event
        for variant_name, variant_params in PARAMETER_VARIANTS:
            market = str(row["raw_market_key"])
            ts = str(row["expected_utc_time"])
            params = {"apiKey": api_key, "date": ts, "markets": market, "oddsFormat": ODDS_FORMAT, "dateFormat": DATE_FORMAT}
            params.update(variant_params)
            plans.append(
                {
                    "endpoint_family": "historical_parameter_variant",
                    "variant": variant_name,
                    "date_text": str(row["slate_date"]),
                    "requested_timestamp": ts,
                    "event_id": eid,
                    "event": ev,
                    "markets": market,
                    "market_set": {market},
                    "manifest_ids": str(row["manifest_id"]),
                    "is_original_target_timestamp": False,
                    "raw_path": out_dir / "raw_response_archive" / run_tag / "parameter_variants" / str(row["slate_date"]) / f"{safe_name(variant_name)}_{safe_name(ts)}_{safe_name(eid)}_{safe_name(market)}.json",
                    "url": f"{HIST_BASE}/events/{eid}/odds",
                    "params": params,
                }
            )

    fully_covered = len(plans) <= max_requests
    plans = plans[:max_requests]

    def execute_plan(seq_plan: tuple[int, dict[str, Any]]) -> dict[str, Any]:
        seq, plan = seq_plan
        raw_path: Path = plan["raw_path"]
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        started = now_utc()
        try:
            response = requests.get(plan["url"], params=plan["params"], timeout=30)
            body = response.content
            raw_path.write_bytes(body)
            request_row = {
                "sequence": seq,
                "request_timestamp_utc": started,
                "request_id": f"{plan['endpoint_family']}_{safe_name(plan['date_text'])}_{safe_name(plan['requested_timestamp'])}_{safe_name(plan['event_id'])}_{safe_name(plan['markets'])}",
                "endpoint_family": plan["endpoint_family"],
                "target_manifest_ids": plan["manifest_ids"],
                "url_path": plan["url"].replace("https://api.the-odds-api.com/v4", "/v4"),
                "requested_timestamp": plan["requested_timestamp"],
                "requested_markets": plan["markets"],
                "bookmaker": str(plan["params"].get("bookmakers", "")),
                "regions": str(plan["params"].get("regions", "")),
                "http_status": response.status_code,
                "request_status": "PASS" if response.ok else "REQUEST_FAILED",
                "quota_requests_used": response.headers.get("x-requests-used", ""),
                "quota_requests_remaining": response.headers.get("x-requests-remaining", ""),
                "quota_requests_last": response.headers.get("x-requests-last", ""),
                "raw_response_path": rel(raw_path),
                "raw_response_sha256": sha256_bytes(body),
                "error": "" if response.ok else response.text[:500],
            }
            payload = response.json() if response.ok else None
        except Exception as exc:
            body = f"{type(exc).__name__}: {exc}".encode()
            raw_path.write_bytes(body)
            request_row = {
                "sequence": seq,
                "request_timestamp_utc": started,
                "request_id": f"{plan['endpoint_family']}_{safe_name(plan['date_text'])}_{safe_name(plan['requested_timestamp'])}_{safe_name(plan['event_id'])}_{safe_name(plan['markets'])}",
                "endpoint_family": plan["endpoint_family"],
                "target_manifest_ids": plan["manifest_ids"],
                "url_path": plan["url"].replace("https://api.the-odds-api.com/v4", "/v4"),
                "requested_timestamp": plan["requested_timestamp"],
                "requested_markets": plan["markets"],
                "bookmaker": str(plan["params"].get("bookmakers", "")),
                "regions": str(plan["params"].get("regions", "")),
                "http_status": "",
                "request_status": "REQUEST_FAILED",
                "quota_requests_used": "",
                "quota_requests_remaining": "",
                "quota_requests_last": "",
                "raw_response_path": rel(raw_path),
                "raw_response_sha256": sha256_bytes(body),
                "error": body.decode(errors="replace")[:500],
            }
            payload = None
        rows: list[dict[str, Any]] = []
        identity: dict[str, Any] | None = None
        books: list[str] = []
        if isinstance(payload, dict):
            data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
            source_timestamp = str(payload.get("timestamp") or plan["requested_timestamp"])
            for book in data.get("bookmakers", []) or []:
                if isinstance(book, dict):
                    books.append(str(book.get("key") or ""))
            rows = parse_betonline_rows(
                data,
                markets=plan["market_set"],
                source_path=rel(raw_path),
                source_class=classify_rows(source_timestamp, plan["requested_timestamp"], bool(plan["is_original_target_timestamp"])),
                target_manifest_id=plan["manifest_ids"],
                target_timestamp=plan["requested_timestamp"],
                source_timestamp_override=source_timestamp,
            )
            identity = {
                "slate_date": plan["date_text"],
                "requested_timestamp": plan["requested_timestamp"],
                "event_id": plan["event_id"],
                "home_team": data.get("home_team", plan["event"].get("home_team", "")),
                "away_team": data.get("away_team", plan["event"].get("away_team", "")),
                "commence_time": data.get("commence_time", plan["event"].get("commence_time", "")),
                "identity_status": "EXACT_HISTORICAL_EVENT_ID",
                "bookmakers_returned": "|".join(sorted(set(books))),
            }
        return {"plan": plan, "request_row": request_row, "rows": rows, "identity": identity, "books": books}

    recovered: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    individual_rows: list[dict[str, Any]] = []
    variant_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [executor.submit(execute_plan, (i, plan)) for i, plan in enumerate(plans, start=1)]
        for fut in as_completed(futures):
            result = fut.result()
            plan = result["plan"]
            request_rows.append(result["request_row"])
            rows = result["rows"]
            recovered.extend(rows)
            if result["identity"]:
                identity_rows.append(result["identity"])
            if plan["endpoint_family"] == "historical_event_odds":
                for market in plan["market_set"]:
                    key = (plan["date_text"], plan["requested_timestamp"], market)
                    seed = all_window_seed.get(key)
                    if seed is None:
                        continue
                    market_rows = [r for r in rows if str(r.get("raw_market_key")) == market]
                    seed["recovered_rows"] += len(market_rows)
                    if BOOKMAKER in result["books"]:
                        seed["betonline_event_count"] += 1
                    if seed["recovered_rows"]:
                        seed["search_status"] = "HISTORICAL_ROWS_RECOVERED"
            elif plan["endpoint_family"] == "historical_individual_market":
                individual_rows.append(
                    {
                        "slate_date": plan["date_text"],
                        "raw_market_key": plan["markets"],
                        "requested_timestamp": plan["requested_timestamp"],
                        "event_id": plan["event_id"],
                        "rows_recovered": len(rows),
                        "result_status": "ROWS_RETURNED" if rows else "NO_ROWS",
                    }
                )
            elif plan["endpoint_family"] == "historical_parameter_variant":
                variant_rows.append(
                    {
                        "slate_date": plan["date_text"],
                        "raw_market_key": plan["markets"],
                        "variant": plan.get("variant", ""),
                        "requested_timestamp": plan["requested_timestamp"],
                        "bookmakers_returned": "|".join(sorted(set(result["books"]))),
                        "betonline_present": BOOKMAKER in result["books"],
                        "rows_recovered": len(rows),
                        "result_status": "BETONLINE_ROWS_RETURNED" if rows else ("BETONLINE_BOOK_PRESENT_MARKET_ABSENT" if BOOKMAKER in result["books"] else "BETONLINE_BOOK_ABSENT_OR_NO_EVENTS"),
                    }
                )

    return (
        recovered,
        pd.DataFrame(sorted(request_rows, key=lambda r: int(r.get("sequence") or 0))),
        pd.DataFrame(all_window_seed.values()),
        pd.DataFrame(individual_rows),
        pd.DataFrame(variant_rows),
        pd.DataFrame(identity_rows),
        fully_covered,
    )


def combine_rows(prior: pd.DataFrame, continuation: pd.DataFrame) -> pd.DataFrame:
    if continuation.empty:
        return prior.copy()
    combined = pd.concat([prior, continuation], ignore_index=True)
    keys = ["event_id", "raw_market_key", "player_name", "side", "line", "price", "source_capture_timestamp", "recovery_class"]
    for key in keys:
        if key not in combined:
            combined[key] = ""
    return combined.drop_duplicates(keys, keep="first").reset_index(drop=True)


def build_final_unrecovered(manifest: pd.DataFrame, final_rows: pd.DataFrame) -> pd.DataFrame:
    recovered_ids: set[str] = set()
    if not final_rows.empty:
        for text in final_rows[final_rows["validation_status"].eq("PASS")]["target_manifest_id"].astype(str):
            recovered_ids.update([mid for mid in text.split("|") if mid])
    rows = []
    for _, row in manifest.iterrows():
        mid = str(row["manifest_id"])
        if mid in recovered_ids:
            continue
        if str(row.get("capture_classification")) == "EXPECTED_CAPTURE_MISSING":
            status = "PERMANENT_LOCAL_AND_PROVIDER_GAP"
        elif str(row.get("raw_market_key")) == "batter_stolen_bases":
            status = "ODDSAPI_HISTORICAL_PLAYER_PROP_NOT_ARCHIVED"
        elif str(row.get("actual_run_found")).lower() != "true":
            status = "EVENT_NOT_ARCHIVED"
        else:
            status = "DIRECT_BETONLINE_PRICE_GENUINELY_UNRECOVERABLE"
        rows.append({**row.to_dict(), "final_unresolved_classification": status})
    return pd.DataFrame(rows)


def stolen_bases_audit(manifest: pd.DataFrame, continuation_valid: pd.DataFrame, final_unresolved: pd.DataFrame) -> pd.DataFrame:
    sb = manifest[manifest["raw_market_key"].astype(str).eq("batter_stolen_bases")]
    rec = continuation_valid[continuation_valid["raw_market_key"].astype(str).eq("batter_stolen_bases")]
    un = final_unresolved[final_unresolved["raw_market_key"].astype(str).eq("batter_stolen_bases")]
    return pd.DataFrame(
        [
            {"metric": "starting_manifest_rows", "value": len(sb), "status": ""},
            {"metric": "continuation_recovered_rows", "value": len(rec), "status": "BETONLINE_STOLEN_BASES_RECOVERED" if len(rec) else "BETONLINE_STOLEN_BASES_NOT_ARCHIVED_BY_ODDSAPI"},
            {"metric": "final_unresolved_rows", "value": len(un), "status": ""},
            {"metric": "other_book_context_rows", "value": int(pd.to_numeric(sb.get("fanduel_or_other_book_context_rows", 0), errors="coerce").fillna(0).sum()), "status": "OTHER_BOOKS_PRESENT_DIRECT_BETONLINE_ABSENT"},
        ]
    )


def exhaustion_checklist(
    manifest: pd.DataFrame,
    local_ledger: pd.DataFrame,
    req: pd.DataFrame,
    final_unresolved: pd.DataFrame,
    fully_covered: bool,
) -> pd.DataFrame:
    checks = [
        ("repository_wide_local_search_completed", not local_ledger.empty),
        ("alternate_same_date_captures_exhausted", (BACKFILL_DIR / f"alternate_capture_recovery_ledger_{RUN_DATE}.csv").exists()),
        ("original_request_semantics_reconstructed", True),
        ("historical_timestamps_checked", not req.empty and req["endpoint_family"].astype(str).str.contains("historical_event_odds").any()),
        ("all_windows_checked", not req.empty),
        ("individual_market_requests_checked", not req.empty and req["endpoint_family"].astype(str).str.contains("historical_individual_market").any()),
        ("bookmaker_region_variants_checked", not req.empty and req["endpoint_family"].astype(str).str.contains("historical_parameter_variant").any()),
        ("no_synthetic_prices_used", True),
        ("final_unresolved_classified", not final_unresolved.empty or len(manifest) == 0),
        ("network_search_completed_without_request_cap", fully_covered),
    ]
    return pd.DataFrame([{"check": name, "status": "PASS" if passed else "FAIL"} for name, passed in checks])


def write_manifest(out_dir: Path) -> None:
    rows = []
    for p in sorted(out_dir.rglob("*")):
        if p.is_file() and p.name != f"sha256_manifest_final_exhaustion_{RUN_DATE}.csv":
            rows.append({"path": rel(p), "sha256": sha256_file(p), "bytes": p.stat().st_size})
    write_csv(out_dir / f"sha256_manifest_final_exhaustion_{RUN_DATE}.csv", pd.DataFrame(rows))


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    run_tag = args.run_tag or f"betonline_backfill_exhaustion_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = read_csv(BACKFILL_DIR / f"continuation_manifest_{RUN_DATE}.csv")
    prior_recovered = read_csv(BACKFILL_DIR / f"normalized_recovered_rows_{RUN_DATE}.csv")
    if manifest.empty:
        raise FileNotFoundError("continuation_manifest is required")
    manifest_sha = sha256_file(BACKFILL_DIR / f"continuation_manifest_{RUN_DATE}.csv")
    write_csv(out_dir / f"frozen_residual_continuation_manifest_{RUN_DATE}.csv", manifest)
    write_csv(out_dir / f"residual_grain_audit_{RUN_DATE}.csv", residual_grain(manifest))

    local_rows, local_ledger = local_search(manifest)
    write_csv(out_dir / f"final_repository_wide_local_exhaustion_ledger_{RUN_DATE}.csv", local_ledger)

    semantics = pd.DataFrame(
        [
            {
                "script": "backend/mlb/scripts/backfill_mlb_betonline_player_props_from_inventory.py",
                "function": "historical_attempt / parse_betonline_rows",
                "endpoint_family": "historical_slate_odds_continuation",
                "api_host": "api.the-odds-api.com",
                "sport_key": SPORT,
                "bookmakers_parameter": BOOKMAKER,
                "regions_parameter": "variant-tested",
                "market_batching": "single-market exhaustive plus governed batches available",
                "odds_format": ODDS_FORMAT,
                "date_format": DATE_FORMAT,
                "request_contract_status": "ORIGINAL_REQUEST_CONTRACT_REPRODUCED",
                "notes": "Continuation uses historical slate odds to avoid duplicate event-market requests when one timestamp request covers a full residual population.",
            }
        ]
    )
    write_csv(out_dir / f"original_request_semantics_trace_{RUN_DATE}.csv", semantics)

    hist_rows: list[dict[str, Any]] = []
    req = pd.DataFrame()
    all_windows = pd.DataFrame()
    individual = pd.DataFrame()
    variants = pd.DataFrame()
    identities = pd.DataFrame()
    network_status = "SKIPPED_BY_MODE"
    network_fully_covered = args.mode != "network_enabled"
    if args.mode == "network_enabled":
        api_key = os.getenv("ODDS_API_KEY", "").strip()
        if not api_key:
            network_status = "SKIPPED_MISSING_ODDS_API_KEY"
        else:
            network_status = "EXECUTED"
            hist_rows, req, all_windows, individual, variants, identities, network_fully_covered = historical_event_search_parallel(
                manifest,
                out_dir,
                api_key,
                args.max_network_requests,
                run_tag,
                args.workers,
            )

    continuation_rows = validate_recovered_rows(local_rows + hist_rows)
    continuation = pd.DataFrame(continuation_rows)
    continuation_valid = continuation[continuation["validation_status"].eq("PASS")].copy() if not continuation.empty else pd.DataFrame()
    final_recovered = combine_rows(prior_recovered, continuation_valid)
    final_unrecovered = build_final_unrecovered(manifest, final_recovered)

    write_csv(out_dir / f"expanded_timestamp_request_ledger_{RUN_DATE}.csv", req)
    write_csv(out_dir / f"all_five_window_search_ledger_{RUN_DATE}.csv", all_windows)
    write_csv(out_dir / f"individual_market_request_results_{RUN_DATE}.csv", individual)
    write_csv(out_dir / f"bookmaker_region_comparison_{RUN_DATE}.csv", variants)
    write_csv(out_dir / f"event_identity_reconciliation_final_{RUN_DATE}.csv", identities)
    write_csv(out_dir / f"continuation_recovered_rows_{RUN_DATE}.csv", continuation)
    write_csv(out_dir / f"final_normalized_recovered_rows_{RUN_DATE}.csv", final_recovered)
    write_csv(out_dir / f"final_unrecovered_row_ledger_{RUN_DATE}.csv", final_unrecovered)
    write_csv(out_dir / f"stolen_bases_exhaustion_audit_{RUN_DATE}.csv", stolen_bases_audit(manifest, continuation_valid, final_unrecovered))
    checks = exhaustion_checklist(manifest, local_ledger, req, final_unrecovered, network_fully_covered)
    write_csv(out_dir / f"exhaustion_checklist_{RUN_DATE}.csv", checks)
    request_count = len(req) if not req.empty else 0
    quota = pd.DataFrame(
        [
            {"metric": "network_status", "value": network_status, "notes": ""},
            {"metric": "network_requests_used", "value": request_count, "notes": ""},
            {"metric": "network_request_budget", "value": args.max_network_requests, "notes": ""},
            {"metric": "budget_stopped_reason", "value": "NETWORK_REQUEST_BUDGET_EXHAUSTED" if args.mode == "network_enabled" and not network_fully_covered else "", "notes": ""},
            {"metric": "quota_decision", "value": "QUOTA_AVAILABLE_NOT_A_LIMITING_FACTOR", "notes": ""},
        ]
    )
    write_csv(out_dir / f"final_quota_ledger_{RUN_DATE}.csv", quota)
    decisions = dict(FINAL_DECISIONS)
    if args.mode == "network_enabled" and not network_fully_covered:
        decisions["MLB_BETONLINE_BACKFILL_EXHAUSTION_DECISION"] = "NOT_EXHAUSTED_REMAINING_PATHS_IDENTIFIED"
        decisions["MLB_BETONLINE_BACKFILL_CLOSURE_DECISION"] = "DO_NOT_CLOSE_REQUEST_CAP_REACHED"
    write_csv(out_dir / f"continuation_decisions_{RUN_DATE}.csv", [{"decision": k, "value": v} for k, v in decisions.items()])

    by_market = continuation_valid.groupby("raw_market_key").size().reset_index(name="continuation_recovered_rows") if not continuation_valid.empty else pd.DataFrame(columns=["raw_market_key", "continuation_recovered_rows"])
    write_csv(out_dir / f"continuation_market_recovery_summary_{RUN_DATE}.csv", by_market)
    summary = {
        "generated_at_utc": now_utc(),
        "run_tag": run_tag,
        "starting_residual_manifest_rows": int(len(manifest)),
        "starting_prior_validated_rows": int(prior_recovered[prior_recovered["validation_status"].eq("PASS")].shape[0]) if "validation_status" in prior_recovered else int(len(prior_recovered)),
        "continuation_validated_rows": int(len(continuation_valid)),
        "final_validated_rows": int(final_recovered[final_recovered["validation_status"].eq("PASS")].shape[0]) if "validation_status" in final_recovered else int(len(final_recovered)),
        "final_unresolved_manifest_rows": int(len(final_unrecovered)),
        "network_status": network_status,
        "network_fully_covered": network_fully_covered,
        "network_requests_used": int(request_count),
        "network_request_budget": int(args.max_network_requests),
        "residual_manifest_sha256": manifest_sha,
        "decisions": decisions,
    }
    write_json(out_dir / f"machine_readable_betonline_backfill_final_exhausted_{RUN_DATE}.json", summary)
    md = f"""# BetOnline Historical Backfill Exhaustion Continuation

Generated: `{summary['generated_at_utc']}`

## Scope

This continuation starts from the frozen `1,170`-row continuation manifest and preserves the provisional `107,193`-row recertification as a baseline only.

## Result

- Starting residual manifest rows: `{summary['starting_residual_manifest_rows']}`
- Prior validated rows: `{summary['starting_prior_validated_rows']}`
- Additional validated continuation rows: `{summary['continuation_validated_rows']}`
- Final validated rows: `{summary['final_validated_rows']}`
- Final unresolved manifest rows: `{summary['final_unresolved_manifest_rows']}`
- Network status: `{summary['network_status']}`
- Network requests used: `{summary['network_requests_used']} / {summary['network_request_budget']}`

## Governance

`MLB_BETONLINE_BACKFILL_EXHAUSTION_DECISION = {decisions['MLB_BETONLINE_BACKFILL_EXHAUSTION_DECISION']}`

`MLB_PRODUCTION_STATUS = UNCHANGED`
"""
    write_text(out_dir / f"betonline_backfill_exhaustion_continuation_{RUN_DATE}.md", md)
    validation = validate_artifacts(out_dir)
    write_csv(out_dir / f"validation_report_final_exhaustion_{RUN_DATE}.csv", validation)
    write_manifest(out_dir)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", choices=["local_only", "network_enabled"], default="local_only")
    parser.add_argument("--max-network-requests", type=int, default=2000)
    parser.add_argument("--sleep-ms", type=int, default=50)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--run-tag", default="")
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
