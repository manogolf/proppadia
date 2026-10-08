#!/usr/bin/env python3
"""Reuse a verified current-ET-day NHL 8rain catalog or fetch and retain one."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


BASE = "https://app.8rainstation.com/public/api/catalog/"
ROOT = Path("artifacts/operational/nhl/8rain_catalog")
PLAYER_CATALOG_LIMIT = 2000
ENDPOINTS = {
    "model_spec.json": "model-spec?league=nhl",
    "teams.json": "teams?league=nhl",
    "players.json": f"players?league=nhl&limit={PLAYER_CATALOG_LIMIT}",
}


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"CATALOG_JSON_ROOT_NOT_OBJECT:{path.name}")
    return value


def validate_catalog(path: Path) -> dict:
    """Validate bundle structure, NHL identity, metadata, byte counts and hashes."""
    path = Path(path).expanduser().resolve()
    metadata_path = path / "catalog_metadata.json"
    if not metadata_path.is_file():
        raise ValueError("CATALOG_METADATA_MISSING")
    metadata = _read_json(metadata_path)
    stamp = metadata.get("retrieved_at_utc")
    try:
        retrieved_at = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        if retrieved_at.tzinfo is None:
            raise ValueError
        retrieved_at = retrieved_at.astimezone(timezone.utc)
    except (TypeError, ValueError):
        raise ValueError("CATALOG_TIMESTAMP_INVALID") from None

    hashes = metadata.get("files")
    if not isinstance(hashes, dict):
        raise ValueError("CATALOG_FILE_HASHES_MISSING")
    for filename in ENDPOINTS:
        file_path = path / filename
        record = hashes.get(filename)
        if not file_path.is_file() or not isinstance(record, dict):
            raise ValueError(f"CATALOG_REQUIRED_FILE_MISSING_OR_UNHASHED:{filename}")
        body = file_path.read_bytes()
        if int(record.get("bytes", -1)) != len(body):
            raise ValueError(f"CATALOG_BYTE_COUNT_MISMATCH:{filename}")
        if record.get("sha256") != hashlib.sha256(body).hexdigest():
            raise ValueError(f"CATALOG_HASH_MISMATCH:{filename}")

    spec = _read_json(path / "model_spec.json")
    teams = _read_json(path / "teams.json")
    players = _read_json(path / "players.json")
    if str((spec.get("league") or {}).get("code", "")).lower() != "nhl":
        raise ValueError("8RAIN_CATALOG_LEAGUE_MISMATCH")
    if (not isinstance(spec.get("markets"), dict) or not spec["markets"]
            or not isinstance(spec.get("stats"), list) or not spec["stats"]
            or any(not isinstance(row, dict) for row in spec["stats"])):
        raise ValueError("8RAIN_CATALOG_SCHEMA_INVALID:model_spec.json")
    if not isinstance(teams.get("data"), list) or not teams["data"]:
        raise ValueError("8RAIN_CATALOG_SCHEMA_INVALID:teams.json")
    if not isinstance(players.get("data"), list) or not players["data"]:
        raise ValueError("8RAIN_CATALOG_SCHEMA_INVALID:players.json")
    if any(not isinstance(row, dict) for row in teams["data"]):
        raise ValueError("8RAIN_CATALOG_ENTITY_INVALID:teams.json")
    if any(not isinstance(row, dict) for row in players["data"]):
        raise ValueError("8RAIN_CATALOG_ENTITY_INVALID:players.json")
    player_url = str((metadata.get("endpoints") or {}).get("players.json", ""))
    if f"limit={PLAYER_CATALOG_LIMIT}" not in player_url:
        raise ValueError("8RAIN_PLAYER_CATALOG_LIMIT_UNSPECIFIED")
    if len(players["data"]) >= PLAYER_CATALOG_LIMIT:
        raise ValueError("8RAIN_PLAYER_CATALOG_LIMIT_REACHED_PAGINATION_REQUIRED")
    if not any(row.get("code") and row.get("name") for row in teams["data"]):
        raise ValueError("8RAIN_CATALOG_NO_MAPPABLE_TEAMS")
    if not any(row.get("code") and row.get("name") for row in players["data"]):
        raise ValueError("8RAIN_CATALOG_NO_MAPPABLE_PLAYERS")
    return {
        "path": str(path),
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "retrieved_at_et": retrieved_at.astimezone(ZoneInfo("America/New_York")).isoformat(),
        "catalog_sha256": hashlib.sha256(metadata_path.read_bytes()).hexdigest(),
        "files": hashes,
        "player_count": len(players["data"]),
        "player_catalog_completeness": "LIKELY_COMPLETE",
        "player_catalog_limit": PLAYER_CATALOG_LIMIT,
        "team_count": len(teams["data"]),
        "league_code": "nhl",
        "schema_version": metadata.get("schema_version"),
        "source": metadata.get("endpoints", {}),
        "slate_date": metadata.get("slate_date"),
    }


def current_day_catalog(root: Path, *, slate_date: str) -> tuple[Path, dict] | None:
    """Return newest valid catalog whose recorded fetch time falls on ET slate date."""
    valid: list[tuple[datetime, Path, dict]] = []
    for directory in Path(root).glob("retrieval=*"):
        if not directory.is_dir():
            continue
        try:
            info = validate_catalog(directory)
            fetched_at = datetime.fromisoformat(info["retrieved_at_utc"].replace("Z", "+00:00"))
            if (info["schema_version"] == "NHL_8RAIN_CATALOG_BUNDLE_V1"
                    and info["slate_date"] != slate_date):
                continue
            if info["retrieved_at_et"][:10] == slate_date:
                valid.append((fetched_at, directory.resolve(), info))
        except Exception:
            # Retained partial, malformed, or hash-invalid bundles are never reused.
            continue
    if not valid:
        return None
    valid.sort(key=lambda item: (item[0], item[1].name))
    _, path, info = valid[-1]
    return path, info


def _fetch_json(url: str) -> bytes:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"8RAIN_CATALOG_HTTP_STATUS:{response.status}:{url}")
        return response.read()


def fetch_catalog(root: Path, *, slate_date: str,
                  fetch_json: Callable[[str], bytes] = _fetch_json) -> tuple[Path, dict]:
    """Fetch all catalog endpoints, validate the complete bundle, and retain immutably."""
    root = Path(root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".catalog-staging-", dir=root) as staging_name:
        staging = Path(staging_name)
        fetched_at = datetime.now(timezone.utc)
        file_records = {}
        endpoint_urls = {}
        for filename, endpoint in ENDPOINTS.items():
            url = BASE + endpoint
            body = fetch_json(url)
            parsed = json.loads(body)
            if not isinstance(parsed, dict):
                raise ValueError(f"8RAIN_CATALOG_SCHEMA_INVALID:{filename}")
            if filename == "model_spec.json":
                if str((parsed.get("league") or {}).get("code", "")).lower() != "nhl":
                    raise ValueError("8RAIN_CATALOG_LEAGUE_MISMATCH")
            elif (not isinstance(parsed.get("data"), list) or not parsed["data"]
                  or any(not isinstance(row, dict) for row in parsed["data"])):
                raise ValueError(f"8RAIN_CATALOG_SCHEMA_INVALID:{filename}")
            (staging / filename).write_bytes(body)
            file_records[filename] = {
                "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
            }
            endpoint_urls[filename] = url
        metadata = {
            "schema_version": "NHL_8RAIN_CATALOG_BUNDLE_V1",
            "league_code": "nhl",
            "slate_date": slate_date,
            "retrieved_at_utc": fetched_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "endpoints": endpoint_urls,
            "player_catalog_completeness": "LIKELY_COMPLETE",
            "player_catalog_limit": PLAYER_CATALOG_LIMIT,
            "files": file_records,
        }
        (staging / "catalog_metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        validate_catalog(staging)
        stamp = fetched_at.strftime("%Y%m%dT%H%M%S%fZ")
        destination = root / f"retrieval={stamp}_{uuid.uuid4().hex[:8]}"
        staging.rename(destination)
    info = validate_catalog(destination)
    return destination.resolve(), info


def get_current_catalog(root: Path, *, slate_date: str,
                        fetch_json: Callable[[str], bytes] = _fetch_json,
                        force_fetch: bool = False) -> tuple[Path, dict]:
    if not force_fetch:
        retained = current_day_catalog(root, slate_date=slate_date)
        if retained is not None:
            path, info = retained
            return path, {**info, "catalog_use": "REUSED"}
    path, info = fetch_catalog(root, slate_date=slate_date, fetch_json=fetch_json)
    return path, {**info, "catalog_use": "FRESHLY_FETCHED"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slate-date", required=True, help="Current ET slate YYYY-MM-DD")
    parser.add_argument("--force-fetch", action="store_true", help="Fetch instead of reusing a same-day catalog")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    path, info = get_current_catalog(
        args.root, slate_date=args.slate_date, force_fetch=args.force_fetch)
    print(path)
    print(json.dumps(info, sort_keys=True))


if __name__ == "__main__":
    main()
