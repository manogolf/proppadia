#!/usr/bin/env python3
"""Bounded, create-only September 19 preseason market evidence catch-up."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


RUN_TYPE = "SEPTEMBER_19_PRESEASON_CATCHUP"
SLATE_DATE = "2026-09-19"
REGIONS = ("us", "us2")
BUNDLES = {
    "mainline": ("h2h", "spreads"),
    "sog": ("player_shots_on_goal", "player_shots_on_goal_alternate"),
    "points": ("player_points",),
    "saves": ("player_total_saves",),
}
FAMILY_NAMES = {
    "mainline": ("h2h", "spreads"),
    "sog": ("sog",),
    "points": ("points",),
    "saves": ("goalie_saves",),
}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def create_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def create_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def count_payload(payload: list, markets: tuple[str, ...]) -> dict:
    events = books = market_objects = outcomes = 0
    for event in payload if isinstance(payload, list) else []:
        events += 1
        for book in event.get("bookmakers", []) or []:
            books += 1
            for market in book.get("markets", []) or []:
                if market.get("key") in markets:
                    market_objects += 1
                    outcomes += len(market.get("outcomes", []) or [])
    return {"events": events, "bookmaker_objects": books, "market_objects": market_objects, "outcomes": outcomes}


def fetch_bundle(bundle: str, markets: tuple[str, ...], root: Path, api_key: str) -> dict:
    claim = root / "claims" / f"{bundle}.json"
    started = now_utc()
    create_json(claim, {
        "schema_version": "nhl_preseason_catchup_paid_claim_v1",
        "run_type": RUN_TYPE,
        "slate_date": SLATE_DATE,
        "bundle": bundle,
        "markets": list(markets),
        "regions": list(REGIONS),
        "claim_timestamp_utc": started,
        "automatic_retry_allowed": False,
    })
    params = {
        "apiKey": api_key,
        "regions": ",".join(REGIONS),
        "markets": ",".join(markets),
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": "proppadia-nhl-preseason-catchup/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = response.read()
            headers = {name.lower(): value for name, value in response.headers.items()}
        payload = json.loads(body)
        completed = now_utc()
        raw = root / "raw" / f"{bundle}.json"
        envelope = {
            "schema_version": "nhl_preseason_catchup_raw_v1",
            "run_type": RUN_TYPE,
            "slate_date": SLATE_DATE,
            "claim_path": str(claim),
            "request_start_timestamp_utc": started,
            "capture_timestamp_utc": completed,
            "provider": "THE_ODDS_API",
            "request_metadata": {
                "endpoint": "icehockey_nhl/odds",
                "regions": list(REGIONS),
                "markets": list(markets),
                "odds_format": "american",
                "date_format": "iso",
            },
            "credit_headers": {
                "x_requests_last": headers.get("x-requests-last"),
                "x_requests_used": headers.get("x-requests-used"),
                "x_requests_remaining": headers.get("x-requests-remaining"),
            },
            "provider_response": payload,
        }
        create_json(raw, envelope)
        counts = count_payload(payload, markets)
        result = {
            "bundle": bundle,
            "claim": str(claim),
            "raw": str(raw),
            "raw_sha256": sha256(raw),
            "request_start_timestamp_utc": started,
            "capture_timestamp_utc": completed,
            "credit_headers": envelope["credit_headers"],
            "counts": counts,
            "status": "CAPTURED",
        }
        for family in FAMILY_NAMES[bundle]:
            receipt = root / "receipts" / f"{family}.json"
            create_json(receipt, {
                "schema_version": "nhl_preseason_catchup_family_receipt_v1",
                "run_type": RUN_TYPE,
                "slate_date": SLATE_DATE,
                "market_family": family,
                "bundle": bundle,
                "markets": list(markets),
                "regions": list(REGIONS),
                "request_start_timestamp_utc": started,
                "capture_timestamp_utc": completed,
                "raw_path": str(raw),
                "raw_sha256": result["raw_sha256"],
                "credit_headers": envelope["credit_headers"],
                "counts": counts,
                "status": "CAPTURED",
            })
        return result
    except BaseException as error:
        failure = root / "failures" / f"{bundle}.json"
        create_json(failure, {
            "schema_version": "nhl_preseason_catchup_failure_v1",
            "run_type": RUN_TYPE,
            "slate_date": SLATE_DATE,
            "bundle": bundle,
            "failure_timestamp_utc": now_utc(),
            "error_type": type(error).__name__,
            "error": str(error),
            "automatic_retry_allowed": False,
        })
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    root = args.output_root.resolve()
    if args.finalize_only:
        files = sorted(path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
        create_text(root / "SHA256SUMS", "".join(f"{sha256(path)}  {path.relative_to(root)}\n" for path in files))
        print(root / "SHA256SUMS")
        return 0
    key = os.environ.get("ODDS_API_KEY", "").strip()
    if not key:
        raise SystemExit("ODDS_API_CREDENTIAL_MISSING_FAIL_CLOSED")
    results = []
    for bundle, markets in BUNDLES.items():
        claim = root / "claims" / f"{bundle}.json"
        family_receipts = [root / "receipts" / f"{family}.json" for family in FAMILY_NAMES[bundle]]
        failure = root / "failures" / f"{bundle}.json"
        if claim.exists():
            if all(path.exists() for path in family_receipts):
                receipt = json.loads(family_receipts[0].read_text())
                results.append({"bundle": bundle, "status": "CAPTURED_PRIOR_INVOCATION", "credit_headers": receipt["credit_headers"], "counts": receipt["counts"]})
            else:
                results.append({"bundle": bundle, "status": "PRIOR_ATTEMPT_BLOCKS_AUTOMATIC_RETRY", "failure": str(failure) if failure.exists() else None, "credit_headers": {"x_requests_last": None}})
            continue
        try:
            results.append(fetch_bundle(bundle, markets, root, key))
        except BaseException as error:
            results.append({"bundle": bundle, "status": "FAILED_NO_AUTOMATIC_RETRY", "error_type": type(error).__name__, "error": str(error), "failure": str(failure), "credit_headers": {"x_requests_last": None}})
    summary = root / "capture_summary.json"
    create_json(summary, {
        "schema_version": "nhl_preseason_catchup_summary_v1",
        "run_type": RUN_TYPE,
        "slate_date": SLATE_DATE,
        "completed_timestamp_utc": now_utc(),
        "bundles": results,
        "request_count": len(results),
        "reported_credits": sum(int(x.get("credit_headers", {}).get("x_requests_last") or 0) for x in results),
    })
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
