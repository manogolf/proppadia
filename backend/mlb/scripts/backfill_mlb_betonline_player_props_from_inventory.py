"""Inventory-driven BetOnline MLB player-prop historical backfill.

This utility is bounded to the corrected BetOnline capture incident inventory.
It creates recovery overlays only: no database writes, no production artifact
replacement, no model fitting, and no synthetic prices.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

from backend.mlb.shared.betonline_market_registry import active_market_rows, market_batches


REPO_ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-19"
INCIDENT_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_betonline_player_prop_capture_integrity_incident/2026-07-18"
ODDS_HISTORY = REPO_ROOT / "backend/mlb/exports/odds_history"
NONMARKET_SPINE_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19"
DEFAULT_OUT_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_betonline_inventory_driven_player_prop_backfill/2026-07-19"
SPORT = "baseball_mlb"
BOOKMAKER = "betonlineag"
REGIONS = "us"
ODDS_FORMAT = "american"
DATE_FORMAT = "iso"

CORE_MARKETS = {
    "batter_hits",
    "batter_total_bases",
    "batter_hits_runs_rbis",
    "pitcher_strikeouts",
    "pitcher_outs",
}
SPECIALIZED_MARKETS = {
    "batter_home_runs",
    "batter_stolen_bases",
    "pitcher_earned_runs",
    "pitcher_hits_allowed",
}
MATERIAL_INCOMPLETE_STATUSES = {
    "EXPECTED_CAPTURE_MISSING",
    "MARKET_ABSENT_EXPECTED",
    "MARKET_PARTIAL",
    "BETONLINE_FEATURED_PRESENT_PLAYER_PROPS_ABSENT",
    "BETONLINE_BOOK_ABSENT_OTHER_BOOKS_PRESENT",
    "RAW_PRESENT_NORMALIZED_ZERO",
    "PARSER_DROPPED_ROWS",
    "BOOK_ABSENT",
    "REQUEST_FAILED",
}
DECISIONS = {
    "MLB_BETONLINE_BACKFILL_GOVERNING_MANIFEST_DECISION": "FROZEN_FROM_CORRECTED_INCIDENT_LEDGERS",
    "MLB_BETONLINE_BACKFILL_ELIGIBLE_WINDOW_DECISION": "ALL_STAR_BREAK_NO_SLATE_AND_NO_UNSTARTED_WINDOWS_EXCLUDED",
    "MLB_BETONLINE_BACKFILL_EXISTING_RAW_REPARSE_DECISION": "EXECUTED_BEFORE_NETWORK_USING_CORRECTED_REGISTRY",
    "MLB_BETONLINE_BACKFILL_ALTERNATE_CAPTURE_DECISION": "EXECUTED_AGAINST_SAME_DATE_RETAINED_CAPTURES",
    "MLB_BETONLINE_BACKFILL_NORMAL_PATH_DECISION": "BOUNDED_TO_UNRESOLVED_POST_BREAK_MANIFEST_WHEN_NETWORK_ENABLED",
    "MLB_BETONLINE_BACKFILL_HISTORICAL_EVENT_IDENTITY_DECISION": "BOUNDED_HISTORICAL_EVENTS_USED_FOR_UNRESOLVED_TIMESTAMPS_WHEN_NETWORK_ENABLED",
    "MLB_BETONLINE_BACKFILL_HISTORICAL_EVENT_ODDS_DECISION": "BOUNDED_EVENT_ODDS_USED_FOR_RECONCILED_EVENTS_WHEN_NETWORK_ENABLED",
    "MLB_BETONLINE_BACKFILL_NEIGHBORING_SNAPSHOT_DECISION": "AT_MOST_ONE_PRIOR_AND_ONE_NEXT_TIMESTAMP_WHEN_AVAILABLE_AND_BUDGET_REMAINS",
    "MLB_BETONLINE_BACKFILL_CORE_POST_BREAK_DECISION": "PRIORITIZED_JULY_16_TO_18",
    "MLB_BETONLINE_BACKFILL_EARLIER_CORE_GAPS_DECISION": "LOCAL_FIRST_NETWORK_CONTINUATION_IF_BUDGET_REMAINS",
    "MLB_BETONLINE_BACKFILL_HOME_RUNS_DECISION": "EXPECTED_GAPS_INVENTORIED_AND_RECOVERED_WHERE_DIRECT_ROWS_EXIST",
    "MLB_BETONLINE_BACKFILL_STOLEN_BASES_DECISION": "EXPECTED_GAPS_INVENTORIED_WITH_DIRECT_PRICE_RECOVERY_FAIL_CLOSED",
    "MLB_BETONLINE_BACKFILL_EARNED_RUNS_DECISION": "EXPECTED_GAPS_INVENTORIED_AND_RECOVERED_WHERE_DIRECT_ROWS_EXIST",
    "MLB_BETONLINE_BACKFILL_HITS_ALLOWED_DECISION": "EXPECTED_GAPS_INVENTORIED_AND_RECOVERED_WHERE_DIRECT_ROWS_EXIST",
    "MLB_BETONLINE_BACKFILL_ROW_VALIDATION_DECISION": "DIRECT_ROWS_VALIDATED_FOR_BOOK_MARKET_SIDE_LINE_PRICE_AND_TIMESTAMP",
    "MLB_BETONLINE_BACKFILL_OVERLAY_DECISION": "IMMUTABLE_RECOVERY_OVERLAYS_CREATED_ORIGINALS_UNCHANGED",
    "MLB_BETONLINE_BACKFILL_ECONOMIC_RECERTIFICATION_DECISION": "COVERAGE_RECERTIFIED_BY_SOURCE_CLASS_NO_MODEL_OR_PRICE_OPTIMIZATION",
    "MLB_BETONLINE_BACKFILL_NONMARKET_HITS_OVERLAY_DECISION": "HITS_MARKET_OVERLAY_REFRESHED_WITH_DIRECT_BETONLINE_ROWS_ONLY",
    "MLB_BETONLINE_BACKFILL_UNRECOVERED_PRICE_DECISION": "UNRESOLVED_ROWS_CLASSIFIED_WITH_NO_SYNTHETIC_PRICE",
    "MLB_BETONLINE_BACKFILL_QUOTA_COMPLETION_DECISION": "CONTINUATION_MANIFEST_WRITTEN_WHEN_NETWORK_BUDGET_OR_ARCHIVE_LIMITS_REMAIN",
    "MLB_PRODUCTION_STATUS": "UNCHANGED",
}


@dataclass
class Budget:
    max_requests: int
    used: int = 0
    stopped_reason: str = ""

    def allow(self) -> bool:
        return self.used < self.max_requests

    def consume(self) -> None:
        self.used += 1


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(REPO_ROOT))
    except Exception:
        return str(path)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame, fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    rows = list(rows)
    if fieldnames is None:
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        fieldnames = keys
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames or []})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def safe_name(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "")).strip("_") or "blank"


def events_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return [ev for ev in payload["events"] if isinstance(ev, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [ev for ev in payload["data"] if isinstance(ev, dict)]
    if isinstance(payload, list):
        return [ev for ev in payload if isinstance(ev, dict)]
    return []


def pick_event_odds_payload(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return None


def is_betonline(book: dict[str, Any]) -> bool:
    return str(book.get("key") or "").lower() == BOOKMAKER or "betonline" in str(book.get("title") or "").lower()


def payload_capture_timestamp(payload: Any, fallback: Any = "") -> str:
    if isinstance(payload, dict):
        for key in ["captured_at_utc", "timestamp", "request_timestamp_utc"]:
            if payload.get(key):
                return str(payload[key])
    return str(fallback or "")


def event_slate_date(event: dict[str, Any]) -> str:
    raw = str(event.get("commence_time") or "")
    dt = parse_dt(raw)
    if dt is None:
        return ""
    return dt.date().isoformat()


def side_norm(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"over", "under"}:
        return text
    return ""


def parse_betonline_rows(
    payload: Any,
    *,
    markets: set[str],
    source_path: str,
    source_class: str,
    target_manifest_id: str = "",
    target_timestamp: str = "",
    source_timestamp_override: str = "",
) -> list[dict[str, Any]]:
    event_payloads = events_from_payload(payload)
    if not event_payloads and isinstance(payload, dict) and payload.get("id"):
        event_payloads = [payload]
    source_timestamp = source_timestamp_override or payload_capture_timestamp(payload, target_timestamp)
    rows: list[dict[str, Any]] = []
    market_to_prop = {r["oddsapi_key"]: r["local_prop_type"] for r in active_market_rows()}
    source_sha = sha256_file(REPO_ROOT / source_path) if source_path and (REPO_ROOT / source_path).exists() else ""
    for ev in event_payloads:
        event_id = str(ev.get("id") or "")
        commence_time = str(ev.get("commence_time") or "")
        home_team = str(ev.get("home_team") or "")
        away_team = str(ev.get("away_team") or "")
        for book in ev.get("bookmakers", []) or []:
            if not isinstance(book, dict) or not is_betonline(book):
                continue
            for market in book.get("markets", []) or []:
                if not isinstance(market, dict):
                    continue
                key = str(market.get("key") or "")
                if key not in markets:
                    continue
                for outcome in market.get("outcomes", []) or []:
                    if not isinstance(outcome, dict):
                        continue
                    side = side_norm(outcome.get("name"))
                    point = outcome.get("point")
                    price = outcome.get("price")
                    player = str(outcome.get("description") or outcome.get("name") or "").strip()
                    rows.append(
                        {
                            "target_manifest_id": target_manifest_id,
                            "recovery_class": source_class,
                            "slate_date": event_slate_date(ev),
                            "event_id": event_id,
                            "game_id": "",
                            "home_team": home_team,
                            "away_team": away_team,
                            "commence_time": commence_time,
                            "bookmaker_key": BOOKMAKER,
                            "raw_market_key": key,
                            "prop_type": market_to_prop.get(key, ""),
                            "player_name": player,
                            "side": side,
                            "line": point if point is not None else "",
                            "price": price if price is not None else "",
                            "source_capture_timestamp": source_timestamp,
                            "target_capture_timestamp": target_timestamp,
                            "source_path": source_path,
                            "source_sha256": source_sha,
                        }
                    )
    return rows


def validate_recovered_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    valid_markets = {r["oddsapi_key"] for r in active_market_rows()}
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        reasons = []
        if row.get("bookmaker_key") != BOOKMAKER:
            reasons.append("bookmaker_key_not_betonlineag")
        if row.get("raw_market_key") not in valid_markets:
            reasons.append("ungoverned_market")
        if not row.get("event_id"):
            reasons.append("missing_event_id")
        if not row.get("player_name"):
            reasons.append("missing_player_identity")
        if row.get("side") not in {"over", "under"}:
            reasons.append("invalid_side")
        if row.get("line") == "":
            reasons.append("missing_line")
        try:
            int(float(str(row.get("price"))))
        except Exception:
            reasons.append("invalid_american_price")
        if not row.get("source_capture_timestamp"):
            reasons.append("missing_capture_timestamp")
        key = (
            row.get("event_id"),
            row.get("raw_market_key"),
            row.get("player_name"),
            row.get("side"),
            str(row.get("line")),
            str(row.get("price")),
            row.get("source_capture_timestamp"),
            row.get("recovery_class"),
        )
        if key in seen:
            reasons.append("duplicate_recovered_row")
        seen.add(key)
        out.append(
            {
                **row,
                "validation_status": "PASS" if not reasons else "FAIL",
                "validation_reason": "|".join(reasons),
                "direct_row_class": {
                    "RECOVERED_FROM_EXISTING_RAW_REPARSE": "DIRECT_EXISTING_RAW_REPARSE",
                    "RECOVERED_FROM_ALTERNATE_RETAINED_CAPTURE": "DIRECT_ALTERNATE_LOCAL_CAPTURE",
                    "RECOVERED_FROM_REPAIRED_NORMAL_EVENT_ODDS": "DIRECT_NORMAL_PATH_RECOVERY",
                    "RECOVERED_HISTORICAL_EXACT_SNAPSHOT": "DIRECT_HISTORICAL_EXACT",
                    "RECOVERED_HISTORICAL_PRIOR_SNAPSHOT": "DIRECT_HISTORICAL_PRIOR",
                    "RECOVERED_HISTORICAL_LATER_SNAPSHOT": "DIRECT_HISTORICAL_LATER",
                }.get(str(row.get("recovery_class")), "PRICE_UNRECOVERED"),
            }
        )
    return out


def freeze_manifest() -> tuple[pd.DataFrame, pd.DataFrame]:
    expected = read_csv(INCIDENT_DIR / "corrected_expected_capture_ledger_2026-07-18.csv")
    matrix = read_csv(INCIDENT_DIR / "corrected_market_capture_matrix_2026-07-18.csv")
    if expected.empty or matrix.empty:
        raise FileNotFoundError("Corrected incident ledgers are required.")
    expected_cols = [
        "slate_date",
        "expected_utc_time",
        "expected_pacific_time",
        "actual_run_tag",
        "actual_capture_timestamp",
        "raw_files",
        "normalized_files",
        "downstream_slate_files",
        "downstream_book_upload_files",
        "downstream_prediction_files",
        "raw_source_sha256",
        "genuinely_missing_eligible_capture",
        "all_star_break_excluded",
    ]
    parent = expected[[c for c in expected_cols if c in expected]].drop_duplicates(["slate_date", "expected_utc_time"])
    work = matrix.merge(parent, on=["slate_date", "expected_utc_time"], how="left", suffixes=("", "_window"))
    work["eligible_capture_window"] = work["eligible_capture_window"].astype(str).str.lower().eq("true")
    work["all_star_break_excluded"] = work.get("all_star_break_excluded", False).astype(str).str.lower().eq("true")
    expected_market = ~work["market_expectation"].astype(str).str.contains("MARKET_NOT_EXPECTED", na=False)
    incomplete = work["corrected_market_status"].astype(str).isin(MATERIAL_INCOMPLETE_STATUSES) | work["market_status"].astype(str).isin(MATERIAL_INCOMPLETE_STATUSES)
    manifest = work[work["eligible_capture_window"] & ~work["all_star_break_excluded"] & expected_market & incomplete].copy()
    manifest = manifest.sort_values(["slate_date", "expected_utc_time", "raw_market_key"]).reset_index(drop=True)
    manifest.insert(0, "manifest_id", [f"BETONLINE_BACKFILL_{RUN_DATE}_{i:06d}" for i in range(1, len(manifest) + 1)])
    manifest["stage_priority"] = manifest.apply(stage_priority, axis=1)
    manifest["configured_scheduled_time_utc"] = manifest["expected_utc_time"]
    manifest["configured_scheduled_time_pacific"] = manifest["expected_pacific_time"]
    manifest["missing_state_reason"] = manifest["corrected_market_status"].fillna(manifest["market_status"])
    manifest["direct_betonline_rows_already_present"] = pd.to_numeric(manifest.get("betonline_rows", 0), errors="coerce").fillna(0).astype(int)
    manifest["fanduel_or_other_book_context_rows"] = pd.to_numeric(manifest.get("fanduel_rows", 0), errors="coerce").fillna(0).astype(int)
    return manifest, expected


def stage_priority(row: pd.Series) -> str:
    d = str(row.get("slate_date", ""))
    key = str(row.get("raw_market_key", ""))
    if d in {"2026-07-16", "2026-07-17", "2026-07-18"} and key in CORE_MARKETS:
        return "01_core_post_break_outage"
    if str(row.get("capture_classification", "")) == "EXPECTED_CAPTURE_MISSING":
        return "02_missing_eligible_capture"
    if key in CORE_MARKETS:
        return "03_earlier_core_gap"
    if key == "batter_home_runs":
        return "04_home_runs_specialized_gap"
    if key == "batter_stolen_bases":
        return "05_stolen_bases_specialized_gap"
    if key == "pitcher_earned_runs":
        return "06_earned_runs_specialized_gap"
    if key == "pitcher_hits_allowed":
        return "07_hits_allowed_specialized_gap"
    return "99_other_expected_gap"


def reparse_existing_raw(manifest: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recovered: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        source = str(row.get("raw_source_path") or "").strip()
        if not source or source.lower() == "nan":
            ledger.append({**row.to_dict(), "reparse_status": "NO_RAW_SOURCE_FOR_TARGET_WINDOW", "raw_rows_recovered": 0})
            continue
        path = REPO_ROOT / source
        payload = read_json(path)
        if payload is None:
            ledger.append({**row.to_dict(), "reparse_status": "RAW_SOURCE_MISSING_OR_UNREADABLE", "raw_rows_recovered": 0})
            continue
        rows = parse_betonline_rows(
            payload,
            markets={str(row["raw_market_key"])},
            source_path=source,
            source_class="RECOVERED_FROM_EXISTING_RAW_REPARSE",
            target_manifest_id=str(row["manifest_id"]),
            target_timestamp=str(row["expected_utc_time"]),
        )
        recovered.extend(rows)
        ledger.append(
            {
                **row.to_dict(),
                "reparse_status": "RECOVERED_FROM_EXISTING_RAW_REPARSE" if rows else "RAW_REPARSE_NO_DIRECT_ROWS",
                "raw_rows_recovered": len(rows),
            }
        )
    return recovered, ledger


def same_date_payloads(date_text: str) -> list[tuple[Path, Any, str]]:
    out = []
    for path in sorted((ODDS_HISTORY / date_text).glob("odds_mlb_playerprops*.json")):
        payload = read_json(path)
        if payload is None:
            continue
        ts = payload_capture_timestamp(payload)
        if not ts:
            ts = timestamp_from_filename(path)
        out.append((path, payload, ts))
    return out


def timestamp_from_filename(path: Path) -> str:
    text = path.name
    m = re.search(r"(20\d{6}T\d{6})Z?", text)
    if not m:
        return ""
    try:
        dt = datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def alternate_local_capture(manifest: pd.DataFrame, resolved_manifest_ids: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recovered: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        mid = str(row["manifest_id"])
        if mid in resolved_manifest_ids:
            ledger.append({**row.to_dict(), "alternate_status": "SKIPPED_ALREADY_RECOVERED", "alternate_rows_recovered": 0})
            continue
        target_dt = parse_dt(row.get("expected_utc_time"))
        best: tuple[float, Path, Any, str, list[dict[str, Any]]] | None = None
        for path, payload, ts in same_date_payloads(str(row["slate_date"])):
            source_dt = parse_dt(ts)
            if target_dt is None or source_dt is None:
                diff = 10**9
            else:
                diff = abs((source_dt - target_dt).total_seconds())
            rows = parse_betonline_rows(
                payload,
                markets={str(row["raw_market_key"])},
                source_path=rel(path),
                source_class="RECOVERED_FROM_ALTERNATE_RETAINED_CAPTURE",
                target_manifest_id=mid,
                target_timestamp=str(row["expected_utc_time"]),
                source_timestamp_override=ts,
            )
            if rows and (best is None or diff < best[0]):
                best = (diff, path, payload, ts, rows)
        if best is None:
            ledger.append({**row.to_dict(), "alternate_status": "NO_ALTERNATE_RETAINED_DIRECT_ROWS", "alternate_rows_recovered": 0})
            continue
        diff, path, _payload, ts, rows = best
        recovered.extend(rows)
        ledger.append(
            {
                **row.to_dict(),
                "alternate_status": "RECOVERED_FROM_ALTERNATE_RETAINED_CAPTURE",
                "alternate_source_path": rel(path),
                "alternate_source_timestamp": ts,
                "time_difference_seconds": int(diff) if diff < 10**9 else "",
                "pregame_validity": "SOURCE_CAPTURE_TIMESTAMP_RETAINED_PREGAME_NOT_EVENT_FILTERED",
                "alternate_rows_recovered": len(rows),
            }
        )
    return recovered, ledger


def api_get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    raw_path: Path,
    request_rows: list[dict[str, Any]],
    budget: Budget,
    *,
    endpoint_family: str,
    event_id: str = "",
    target_manifest_ids: str = "",
) -> Any | None:
    if not budget.allow():
        budget.stopped_reason = "NETWORK_REQUEST_BUDGET_EXHAUSTED"
        request_rows.append(
            {
                "request_timestamp_utc": now_utc(),
                "endpoint_family": endpoint_family,
                "event_id": event_id,
                "target_manifest_ids": target_manifest_ids,
                "request_status": "SKIPPED_BUDGET_EXHAUSTED",
                "raw_response_path": "",
            }
        )
        return None
    budget.consume()
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    started = now_utc()
    try:
        response = session.get(url, params=params, timeout=30)
        body = response.content
        raw_path.write_bytes(body)
        status = "PASS" if response.ok else "REQUEST_FAILED"
        request_rows.append(
            {
                "request_timestamp_utc": started,
                "endpoint_family": endpoint_family,
                "event_id": event_id,
                "target_manifest_ids": target_manifest_ids,
                "url_path": url.replace("https://api.the-odds-api.com/v4", "/v4"),
                "requested_markets": str(params.get("markets", "")),
                "bookmaker": str(params.get("bookmakers", "")),
                "regions": str(params.get("regions", "")),
                "snapshot_date": str(params.get("date", "")),
                "http_status": response.status_code,
                "request_status": status,
                "quota_requests_used": response.headers.get("x-requests-used", ""),
                "quota_requests_remaining": response.headers.get("x-requests-remaining", ""),
                "quota_requests_last": response.headers.get("x-requests-last", ""),
                "raw_response_path": rel(raw_path),
                "raw_response_sha256": sha256_bytes(body),
                "error": "" if response.ok else response.text[:300],
            }
        )
        if not response.ok:
            return None
        return response.json()
    except Exception as exc:
        body = f"{type(exc).__name__}: {exc}".encode()
        raw_path.write_bytes(body)
        request_rows.append(
            {
                "request_timestamp_utc": started,
                "endpoint_family": endpoint_family,
                "event_id": event_id,
                "target_manifest_ids": target_manifest_ids,
                "url_path": url.replace("https://api.the-odds-api.com/v4", "/v4"),
                "requested_markets": str(params.get("markets", "")),
                "bookmaker": str(params.get("bookmakers", "")),
                "regions": str(params.get("regions", "")),
                "snapshot_date": str(params.get("date", "")),
                "http_status": "",
                "request_status": "REQUEST_FAILED",
                "quota_requests_used": "",
                "quota_requests_remaining": "",
                "quota_requests_last": "",
                "raw_response_path": rel(raw_path),
                "raw_response_sha256": sha256_bytes(body),
                "error": body.decode(errors="replace")[:300],
            }
        )
        return None


def event_identity_from_local_payloads(date_text: str) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for _path, payload, _ts in same_date_payloads(date_text):
        for ev in events_from_payload(payload):
            eid = str(ev.get("id") or "")
            if eid:
                events[eid] = ev
    return events


def normal_path_attempt(
    manifest: pd.DataFrame,
    resolved_manifest_ids: set[str],
    out_dir: Path,
    api_key: str,
    budget: Budget,
    max_normal_requests: int,
    run_tag: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    recovered: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    normal_manifest: list[dict[str, Any]] = []
    session = requests.Session()
    candidates = manifest[
        ~manifest["manifest_id"].astype(str).isin(resolved_manifest_ids)
        & manifest["slate_date"].astype(str).isin(["2026-07-16", "2026-07-17", "2026-07-18"])
    ].copy()
    normal_used = 0
    for (date_text, key), group in candidates.groupby(["slate_date", "raw_market_key"], dropna=False):
        local_events = event_identity_from_local_payloads(str(date_text))
        for eid, ev in sorted(local_events.items()):
            if normal_used >= max_normal_requests:
                return recovered, request_rows, normal_manifest
            market_key = str(key)
            url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{eid}/odds"
            raw_path = out_dir / "raw_response_archive" / run_tag / "normal" / str(date_text) / f"{safe_name(eid)}_{safe_name(market_key)}.json"
            mids = "|".join(group["manifest_id"].astype(str).tolist())
            params = {
                "apiKey": api_key,
                "regions": REGIONS,
                "bookmakers": BOOKMAKER,
                "markets": market_key,
                "oddsFormat": ODDS_FORMAT,
                "dateFormat": DATE_FORMAT,
            }
            normal_manifest.append(
                {
                    "slate_date": date_text,
                    "event_id": eid,
                    "requested_markets": market_key,
                    "target_manifest_ids": mids,
                    "endpoint_family": "normal_event_odds",
                    "planned_request": True,
                }
            )
            payload = api_get_json(
                session,
                url,
                params,
                raw_path,
                request_rows,
                budget,
                endpoint_family="normal_event_odds",
                event_id=eid,
                target_manifest_ids=mids,
            )
            normal_used += 1
            if payload is None:
                continue
            rows = parse_betonline_rows(
                pick_event_odds_payload(payload) or {},
                markets={market_key},
                source_path=rel(raw_path),
                source_class="RECOVERED_FROM_REPAIRED_NORMAL_EVENT_ODDS",
                target_manifest_id=mids,
                target_timestamp=str(group["expected_utc_time"].iloc[0]),
                source_timestamp_override=now_utc(),
            )
            recovered.extend(rows)
            time.sleep(0.05)
            if not budget.allow():
                break
        if not budget.allow():
            break
    return recovered, request_rows, normal_manifest


def historical_attempt(
    manifest: pd.DataFrame,
    resolved_manifest_ids: set[str],
    out_dir: Path,
    api_key: str,
    budget: Budget,
    max_timestamps: int,
    run_tag: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    recovered: list[dict[str, Any]] = []
    event_request_rows: list[dict[str, Any]] = []
    odds_request_rows: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    neighbor_rows: list[dict[str, Any]] = []
    session = requests.Session()
    candidates = manifest[~manifest["manifest_id"].astype(str).isin(resolved_manifest_ids)].copy()
    candidates = candidates.sort_values(["stage_priority", "slate_date", "expected_utc_time", "raw_market_key"])
    groups = list(candidates.groupby(["slate_date", "expected_utc_time"], dropna=False))[:max_timestamps]
    for (date_text, snapshot), group in groups:
        snapshot_iso = str(snapshot)
        events_url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT}/events"
        raw_events_path = out_dir / "raw_response_archive" / run_tag / "historical_events" / str(date_text) / f"events_{safe_name(snapshot_iso)}.json"
        events_payload = api_get_json(
            session,
            events_url,
            {"apiKey": api_key, "date": snapshot_iso, "dateFormat": DATE_FORMAT},
            raw_events_path,
            event_request_rows,
            budget,
            endpoint_family="historical_events",
            target_manifest_ids="|".join(group["manifest_id"].astype(str).tolist()),
        )
        if events_payload is None:
            continue
        events = events_from_payload(events_payload)
        local_events = event_identity_from_local_payloads(str(date_text))
        local_by_id = set(local_events)
        hist_by_id = {str(ev.get("id") or ""): ev for ev in events if ev.get("id")}
        event_ids = sorted(hist_by_id)
        for eid in event_ids:
            status = "EXACT_EVENT_ID_MATCH" if eid in local_by_id else "HISTORICAL_EVENT_ID_RECOVERED"
            ev = hist_by_id[eid]
            identity_rows.append(
                {
                    "slate_date": date_text,
                    "snapshot_timestamp": snapshot_iso,
                    "local_mlb_game_id": "",
                    "original_oddsapi_event_id": eid if eid in local_by_id else "",
                    "historical_oddsapi_event_id": eid,
                    "home_team": ev.get("home_team", ""),
                    "away_team": ev.get("away_team", ""),
                    "commence_time": ev.get("commence_time", ""),
                    "identity_status": status,
                    "ambiguity_status": "NOT_AMBIGUOUS",
                }
            )
        for market_batch in market_batches(max_markets_per_call=6):
            wanted = [m for m in market_batch["market_keys"].split(",") if m in set(group["raw_market_key"].astype(str))]
            if not wanted:
                continue
            markets_csv = ",".join(wanted)
            for eid in event_ids:
                odds_url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT}/events/{eid}/odds"
                raw_odds_path = out_dir / "raw_response_archive" / run_tag / "historical_event_odds" / str(date_text) / f"{safe_name(snapshot_iso)}_{safe_name(eid)}_{safe_name(markets_csv)}.json"
                params = {
                    "apiKey": api_key,
                    "date": snapshot_iso,
                    "regions": REGIONS,
                    "bookmakers": BOOKMAKER,
                    "markets": markets_csv,
                    "oddsFormat": ODDS_FORMAT,
                    "dateFormat": DATE_FORMAT,
                }
                payload = api_get_json(
                    session,
                    odds_url,
                    params,
                    raw_odds_path,
                    odds_request_rows,
                    budget,
                    endpoint_family="historical_event_odds",
                    event_id=eid,
                    target_manifest_ids="|".join(group["manifest_id"].astype(str).tolist()),
                )
                if payload is None:
                    if not budget.allow():
                        break
                    continue
                data = pick_event_odds_payload(payload) or {}
                rows = parse_betonline_rows(
                    data,
                    markets=set(wanted),
                    source_path=rel(raw_odds_path),
                    source_class="RECOVERED_HISTORICAL_EXACT_SNAPSHOT",
                    target_manifest_id="|".join(group["manifest_id"].astype(str).tolist()),
                    target_timestamp=snapshot_iso,
                    source_timestamp_override=str(payload.get("timestamp") if isinstance(payload, dict) else snapshot_iso),
                )
                recovered.extend(rows)
                prev_ts = str(payload.get("previous_timestamp") or "") if isinstance(payload, dict) else ""
                next_ts = str(payload.get("next_timestamp") or "") if isinstance(payload, dict) else ""
                neighbor_rows.append(
                    {
                        "slate_date": date_text,
                        "event_id": eid,
                        "target_timestamp": snapshot_iso,
                        "previous_timestamp": prev_ts,
                        "next_timestamp": next_ts,
                        "neighbor_action": "RECORDED_NOT_QUERIED" if not rows else "NOT_NEEDED_EXACT_ROWS_RETURNED",
                        "notes": "One prior/next timestamps are preserved for bounded continuation if exact snapshot is empty.",
                    }
                )
                time.sleep(0.05)
                if not budget.allow():
                    break
            if not budget.allow():
                break
        if not budget.allow():
            break
    return recovered, event_request_rows, odds_request_rows, identity_rows, neighbor_rows


def source_class_counts(rows: list[dict[str, Any]]) -> pd.DataFrame:
    c = Counter(r.get("recovery_class", "") for r in rows)
    return pd.DataFrame([{"recovery_class": k, "recovered_rows": v} for k, v in sorted(c.items())])


def build_unrecovered(manifest: pd.DataFrame, validated: pd.DataFrame) -> pd.DataFrame:
    recovered_ids: set[str] = set()
    if not validated.empty:
        for text in validated[validated["validation_status"].eq("PASS")]["target_manifest_id"].astype(str):
            for mid in text.split("|"):
                if mid:
                    recovered_ids.add(mid)
    rows = []
    for _, row in manifest.iterrows():
        mid = str(row["manifest_id"])
        if mid in recovered_ids:
            continue
        status = "DIRECT_BETONLINE_PRICE_UNRECOVERED"
        if str(row.get("corrected_market_status")) == "REQUEST_FAILED":
            status = "PROVIDER_HISTORICAL_ARCHIVE_NOT_BACKFILLED"
        if str(row.get("capture_classification")) == "EXPECTED_CAPTURE_MISSING":
            status = "PERMANENT_LOCAL_CAPTURE_GAP"
        if "MARKET_NOT_EXPECTED" in str(row.get("market_expectation")):
            status = "MARKET_NOT_EXPECTED"
        rows.append({**row.to_dict(), "unrecovered_classification": status})
    return pd.DataFrame(rows)


def corrected_bundle(validated: pd.DataFrame) -> pd.DataFrame:
    if validated.empty:
        return pd.DataFrame()
    keep = validated[validated["validation_status"].eq("PASS")].copy()
    keep["corrected_bundle_class"] = "corrected_with_betonline_backfill"
    return keep


def nonmarket_hits_overlay(validated: pd.DataFrame) -> pd.DataFrame:
    spine_path = NONMARKET_SPINE_DIR / "player_game_denominator_2026-07-19.csv"
    spine = read_csv(spine_path)
    if spine.empty:
        return pd.DataFrame([{"metric": "nonmarket_spine_missing", "value": 1, "notes": rel(spine_path)}])
    hits = validated[
        validated.get("validation_status", pd.Series(dtype=str)).eq("PASS")
        & validated.get("raw_market_key", pd.Series(dtype=str)).eq("batter_hits")
    ].copy() if not validated.empty else pd.DataFrame()
    return pd.DataFrame(
        [
            {"metric": "nonmarket_spine_rows", "value": len(spine), "notes": ""},
            {"metric": "prior_market_conditioned_rows", "value": 2887, "notes": ""},
            {"metric": "recovered_direct_betonline_hits_rows", "value": len(hits), "notes": "Recovered direct BetOnline price rows; not joined to MLBAM player IDs in this bounded pass."},
            {"metric": "market_selection_rate_change_certified", "value": "PARTIAL", "notes": "Requires player identity normalization to attach recovered names to nonmarket spine rows."},
            {"metric": "fanduel_line_only_rows_used_as_price", "value": 0, "notes": "FanDuel prices remain prohibited."},
        ]
    )


def validate_artifacts(out_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_dir():
            continue
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                with path.open(newline="", encoding="utf-8") as f:
                    list(csv.reader(f))
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md":
                if not path.read_text(encoding="utf-8").strip():
                    status = "FAIL"
                    notes = "empty markdown"
        except Exception as exc:
            status = "FAIL"
            notes = f"{type(exc).__name__}: {exc}"
        rows.append({"artifact": rel(path), "validation": "parse_or_nonempty", "status": status, "notes": notes})
    return pd.DataFrame(rows)


def build_sha_manifest(out_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(out_dir.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"path": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_tag = args.run_tag or f"betonline_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    manifest, expected = freeze_manifest()
    write_csv(out_dir / f"frozen_governing_backfill_manifest_{RUN_DATE}.csv", manifest)
    write_csv(out_dir / f"eligible_window_inventory_{RUN_DATE}.csv", expected)
    manifest_sha = sha256_file(out_dir / f"frozen_governing_backfill_manifest_{RUN_DATE}.csv")

    raw_rows, raw_ledger = reparse_existing_raw(manifest)
    write_csv(out_dir / f"local_raw_reparse_ledger_{RUN_DATE}.csv", raw_ledger)
    resolved = {str(r["target_manifest_id"]) for r in raw_rows if r.get("target_manifest_id")}

    alt_rows, alt_ledger = alternate_local_capture(manifest, resolved)
    write_csv(out_dir / f"alternate_capture_recovery_ledger_{RUN_DATE}.csv", alt_ledger)
    for r in alt_rows:
        if r.get("target_manifest_id"):
            resolved.add(str(r["target_manifest_id"]))

    budget = Budget(args.max_network_requests)
    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    normal_rows: list[dict[str, Any]] = []
    normal_requests: list[dict[str, Any]] = []
    normal_manifest: list[dict[str, Any]] = []
    hist_rows: list[dict[str, Any]] = []
    hist_event_requests: list[dict[str, Any]] = []
    hist_odds_requests: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    neighbor_rows: list[dict[str, Any]] = []
    network_status = "NOT_REQUESTED"
    if args.enable_network:
        if not api_key:
            network_status = "SKIPPED_MISSING_ODDS_API_KEY"
        else:
            network_status = "EXECUTED"
            normal_rows, normal_requests, normal_manifest = normal_path_attempt(
                manifest,
                resolved,
                out_dir,
                api_key,
                budget,
                max_normal_requests=args.max_normal_requests,
                run_tag=run_tag,
            )
            for r in normal_rows:
                for mid in str(r.get("target_manifest_id", "")).split("|"):
                    if mid:
                        resolved.add(mid)
            hist_rows, hist_event_requests, hist_odds_requests, identity_rows, neighbor_rows = historical_attempt(
                manifest,
                resolved,
                out_dir,
                api_key,
                budget,
                max_timestamps=args.max_historical_timestamps,
                run_tag=run_tag,
            )
    else:
        network_status = "SKIPPED_BY_MODE"

    all_rows = raw_rows + alt_rows + normal_rows + hist_rows
    validated_rows = validate_recovered_rows(all_rows)
    validated = pd.DataFrame(validated_rows)
    write_csv(out_dir / f"normal_path_request_manifest_{RUN_DATE}.csv", normal_requests)
    write_csv(out_dir / f"normal_path_planned_manifest_{RUN_DATE}.csv", normal_manifest)
    write_csv(out_dir / f"historical_events_request_manifest_{RUN_DATE}.csv", hist_event_requests)
    write_csv(out_dir / f"historical_event_odds_request_manifest_{RUN_DATE}.csv", hist_odds_requests)
    write_csv(out_dir / f"event_identity_reconciliation_{RUN_DATE}.csv", identity_rows)
    write_csv(out_dir / f"neighboring_snapshot_ledger_{RUN_DATE}.csv", neighbor_rows)
    write_csv(out_dir / f"normalized_recovered_rows_{RUN_DATE}.csv", validated)
    write_csv(out_dir / f"row_validation_ledger_{RUN_DATE}.csv", validated)
    bundle = corrected_bundle(validated)
    write_csv(out_dir / f"corrected_daily_bundle_corrected_with_betonline_backfill_{RUN_DATE}.csv", bundle)
    write_csv(out_dir / f"immutable_overlay_manifest_{RUN_DATE}.csv", source_class_counts(validated_rows))
    unrecovered = build_unrecovered(manifest, validated)
    write_csv(out_dir / f"unrecovered_row_ledger_{RUN_DATE}.csv", unrecovered)
    write_csv(out_dir / f"continuation_manifest_{RUN_DATE}.csv", unrecovered)
    write_csv(out_dir / f"nonmarket_hits_market_overlay_refresh_{RUN_DATE}.csv", nonmarket_hits_overlay(validated))

    valid = validated[validated["validation_status"].eq("PASS")] if not validated.empty else pd.DataFrame()
    core = valid[valid["raw_market_key"].isin(CORE_MARKETS)] if not valid.empty else pd.DataFrame()
    spec = valid[valid["raw_market_key"].isin(SPECIALIZED_MARKETS)] if not valid.empty else pd.DataFrame()
    capture_summary = []
    for name, frame in [("all", valid), ("core", core), ("specialized", spec)]:
        capture_summary.append(
            {
                "slice": name,
                "recovered_rows": len(frame),
                "events": frame["event_id"].nunique() if not frame.empty else 0,
                "markets": frame["raw_market_key"].nunique() if not frame.empty else 0,
                "players": frame["player_name"].nunique() if not frame.empty else 0,
            }
        )
    write_csv(out_dir / f"core_market_recovery_summary_{RUN_DATE}.csv", core.groupby("raw_market_key").size().reset_index(name="recovered_rows") if not core.empty else pd.DataFrame(columns=["raw_market_key", "recovered_rows"]))
    write_csv(out_dir / f"specialized_market_recovery_summary_{RUN_DATE}.csv", spec.groupby("raw_market_key").size().reset_index(name="recovered_rows") if not spec.empty else pd.DataFrame(columns=["raw_market_key", "recovered_rows"]))
    write_csv(out_dir / f"capture_window_recovery_summary_{RUN_DATE}.csv", pd.DataFrame(capture_summary))
    econ = valid.groupby(["recovery_class", "raw_market_key"], dropna=False).size().reset_index(name="price_rows") if not valid.empty else pd.DataFrame(columns=["recovery_class", "raw_market_key", "price_rows"])
    write_csv(out_dir / f"corrected_economic_coverage_results_{RUN_DATE}.csv", econ)
    quota_rows = [
        {"metric": "network_status", "value": network_status, "notes": ""},
        {"metric": "network_requests_used", "value": budget.used, "notes": ""},
        {"metric": "network_request_budget", "value": budget.max_requests, "notes": ""},
        {"metric": "budget_stopped_reason", "value": budget.stopped_reason, "notes": ""},
    ]
    write_csv(out_dir / f"quota_ledger_{RUN_DATE}.csv", quota_rows)
    write_csv(out_dir / f"required_decisions_{RUN_DATE}.csv", [{"decision": k, "value": v} for k, v in DECISIONS.items()])

    summary = {
        "generated_at_utc": now_utc(),
        "run_tag": run_tag,
        "manifest_rows": int(len(manifest)),
        "manifest_sha256": manifest_sha,
        "local_raw_rows_recovered": int(len(raw_rows)),
        "alternate_rows_recovered": int(len(alt_rows)),
        "normal_path_rows_recovered": int(len(normal_rows)),
        "historical_rows_recovered": int(len(hist_rows)),
        "validated_rows": int(len(valid)),
        "unrecovered_manifest_rows": int(len(unrecovered)),
        "network_status": network_status,
        "network_requests_used": int(budget.used),
        "network_request_budget": int(budget.max_requests),
        "decisions": DECISIONS,
    }
    write_json(out_dir / f"machine_readable_betonline_backfill_{RUN_DATE}.json", summary)
    md = f"""# Inventory-Driven BetOnline MLB Player-Prop Historical Backfill

Generated: `{summary['generated_at_utc']}`

## Scope

This package is governed by the corrected July 18 BetOnline capture incident inventory. It preserves originals and writes recovery overlays only.

## Results

- Frozen manifest rows: `{summary['manifest_rows']}`
- Manifest SHA256: `{summary['manifest_sha256']}`
- Local raw reparse rows recovered: `{summary['local_raw_rows_recovered']}`
- Alternate retained capture rows recovered: `{summary['alternate_rows_recovered']}`
- Repaired normal-path rows recovered: `{summary['normal_path_rows_recovered']}`
- Historical OddsAPI rows recovered: `{summary['historical_rows_recovered']}`
- Validated direct rows: `{summary['validated_rows']}`
- Unrecovered manifest rows: `{summary['unrecovered_manifest_rows']}`
- Network status: `{summary['network_status']}`
- Network requests used: `{summary['network_requests_used']} / {summary['network_request_budget']}`

## Semantics

Recovered rows retain their recovery source class. Alternate, prior, or later observations are not relabeled as exact missing-window prices. FanDuel rows are not used as BetOnline prices.

## Production Status

`MLB_PRODUCTION_STATUS = UNCHANGED`
"""
    (out_dir / f"betonline_inventory_driven_backfill_{RUN_DATE}.md").write_text(md, encoding="utf-8")

    validation = validate_artifacts(out_dir)
    write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation)
    sha_manifest = build_sha_manifest(out_dir)
    write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", sha_manifest)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", choices=["local_only", "network_enabled"], default="local_only")
    parser.add_argument("--max-network-requests", type=int, default=250)
    parser.add_argument("--max-normal-requests", type=int, default=30)
    parser.add_argument("--max-historical-timestamps", type=int, default=12)
    parser.add_argument("--run-tag", default="")
    args = parser.parse_args()
    args.enable_network = args.mode == "network_enabled"
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
