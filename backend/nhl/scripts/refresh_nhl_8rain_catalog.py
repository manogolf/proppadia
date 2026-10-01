#!/usr/bin/env python3
"""Fetch and preserve the public 8rain NHL catalog bundle (GET only)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


BASE = "https://app.8rainstation.com/public/api/catalog/"
ROOT = Path("artifacts/operational/nhl/8rain_catalog")


def main() -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = ROOT / f"retrieval={timestamp}"
    destination.mkdir(parents=True, exist_ok=False)
    files = {
        "model_spec.json": "model-spec?league=nhl",
        "teams.json": "teams?league=nhl",
        "players.json": "players?league=nhl",
    }
    manifest = {"retrieved_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "endpoints": {}, "files": {}}
    for filename, endpoint in files.items():
        url = BASE + endpoint
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=30) as response:
            data = response.read()
        parsed = json.loads(data)
        if filename == "model_spec.json" and parsed.get("league", {}).get("code") != "nhl":
            raise ValueError("8RAIN_CATALOG_LEAGUE_MISMATCH")
        if filename != "model_spec.json" and not isinstance(parsed.get("data"), list):
            raise ValueError(f"8RAIN_CATALOG_SCHEMA_INVALID:{filename}")
        (destination / filename).write_bytes(data)
        manifest["endpoints"][filename] = url
        manifest["files"][filename] = {
            "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
        }
    (destination / "catalog_metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(destination)
    print(json.dumps(manifest["files"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
