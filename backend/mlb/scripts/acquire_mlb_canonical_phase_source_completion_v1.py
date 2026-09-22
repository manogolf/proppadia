#!/usr/bin/env python3
"""Acquire the single predeclared MLB canonical-phase source completion.

This utility is deliberately specific: it accepts only the frozen V1 proposal,
performs at most one HTTP request with redirects disabled, and never overwrites
an existing retained response.  It has no retry path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_SOURCE_COMPLETION_V1"
EXPECTED_ENDPOINT = "https://statsapi.mlb.com/api/v1/schedule"
EXPECTED_PARAMETERS = {
    "sportId": 1,
    "startDate": "2026-02-20",
    "endDate": "2026-03-25",
}
EXPECTED_STORAGE_PATH = (
    "backend/mlb/data/external/statsapi/raw/2026/"
    "schedule_2026-02-20_2026-03-25.json"
)
RELEVANT_RESPONSE_HEADERS = (
    "Content-Type",
    "Content-Length",
    "Date",
    "ETag",
    "Last-Modified",
    "Cache-Control",
    "Age",
    "Via",
    "X-Cache",
)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _atomic_write_new(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise RuntimeError(f"DESTINATION_ALREADY_EXISTS:{path}") from None
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_json_new(path: Path, value: Any) -> None:
    _atomic_write_new(
        path,
        (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )


def _validated_request(proposal_path: Path, repo_root: Path) -> tuple[str, Path, dict[str, Any]]:
    proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    if proposal.get("expected_request_count") != 1:
        raise RuntimeError("PROPOSAL_REQUEST_COUNT_NOT_ONE")
    if proposal.get("expected_paid_credit_count") != 0:
        raise RuntimeError("PROPOSAL_PAID_CREDIT_COUNT_NOT_ZERO")
    if proposal.get("endpoint") != EXPECTED_ENDPOINT:
        raise RuntimeError("PROPOSAL_ENDPOINT_MISMATCH")
    if proposal.get("parameters") != EXPECTED_PARAMETERS:
        raise RuntimeError("PROPOSAL_PARAMETERS_MISMATCH")
    if proposal.get("storage_path") != EXPECTED_STORAGE_PATH:
        raise RuntimeError("PROPOSAL_STORAGE_PATH_MISMATCH")
    destination = repo_root / EXPECTED_STORAGE_PATH
    if destination.exists():
        raise RuntimeError(f"DESTINATION_ALREADY_EXISTS:{destination}")
    query = urllib.parse.urlencode(
        [("sportId", "1"), ("startDate", "2026-02-20"), ("endDate", "2026-03-25")]
    )
    return f"{EXPECTED_ENDPOINT}?{query}", destination, proposal


def acquire(proposal_path: Path, receipt_path: Path, repo_root: Path, timeout: float) -> dict[str, Any]:
    request_url, destination, proposal = _validated_request(proposal_path, repo_root)
    requested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    request = urllib.request.Request(
        request_url,
        headers={"Accept": "application/json", "User-Agent": CONTRACT_NAME},
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirect())
    # Exactly one opener call. There is intentionally no retry or recovery path.
    with opener.open(request, timeout=timeout) as response:
        raw = response.read()
        status = int(response.status)
        response_url = str(response.geturl())
        headers = {
            name.lower(): response.headers.get(name)
            for name in RELEVANT_RESPONSE_HEADERS
            if response.headers.get(name) is not None
        }
    completed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if status != 200:
        raise RuntimeError(f"UNEXPECTED_HTTP_STATUS:{status}")
    if response_url != request_url:
        raise RuntimeError(f"UNEXPECTED_RESPONSE_URL:{response_url}")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"MALFORMED_JSON_RESPONSE:{exc}") from None
    if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
        raise RuntimeError("MALFORMED_SCHEDULE_RESPONSE")

    digest = hashlib.sha256(raw).hexdigest()
    acquisition_identity = hashlib.sha256(
        _canonical_json(
            {
                "provider": "MLB StatsAPI",
                "endpoint": EXPECTED_ENDPOINT,
                "parameters": EXPECTED_PARAMETERS,
                "storage_path": EXPECTED_STORAGE_PATH,
            }
        ).encode("utf-8")
    ).hexdigest()
    _atomic_write_new(destination, raw)
    receipt = {
        "contract_name": CONTRACT_NAME,
        "status": "SUCCESS",
        "provider": "MLB StatsAPI",
        "request_count": 1,
        "retry_count": 0,
        "paid_provider_request_count": 0,
        "paid_credit_count": 0,
        "request_url": request_url,
        "request_parameters": EXPECTED_PARAMETERS,
        "request_started_at_utc": requested_at,
        "request_completed_at_utc": completed_at,
        "http_status": status,
        "response_url": response_url,
        "response_headers": headers,
        "raw_response_path": EXPECTED_STORAGE_PATH,
        "raw_response_byte_count": len(raw),
        "raw_response_sha256": digest,
        "hashing_method": "SHA-256 over exact HTTP response bytes before JSON parsing",
        "acquisition_identity_sha256": acquisition_identity,
        "proposal_path": str(proposal_path.relative_to(repo_root)),
        "proposal_sha256": hashlib.sha256(proposal_path.read_bytes()).hexdigest(),
        "expected_target_game_pk_count": proposal["expected_target_game_pk_count"],
        "existing_response_reused": False,
        "atomic_write": True,
        "overwrite_performed": False,
    }
    _write_json_new(receipt_path, receipt)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proposal", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--repo-root", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        receipt = acquire(
            args.proposal.resolve(),
            args.receipt.resolve(),
            args.repo_root.resolve(),
            args.timeout_seconds,
        )
    except (RuntimeError, OSError, urllib.error.URLError) as exc:
        print(json.dumps({"status": "FAIL_CLOSED", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
