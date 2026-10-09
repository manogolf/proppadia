#!/usr/bin/env python3
"""Acquire official Gamecenter PBP only for unresolved 2025-26 target keys."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

IDS = (2025020621, 2025020642, 2025020667, 2025020982, 2025021041, 2025021165, 2025021216, 2025021255)
BASE = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch(game_id: int, attempts: int = 5) -> bytes:
    url = BASE.format(game_id=game_id)
    req = urllib.request.Request(url, headers={"User-Agent": "proppadia-nhl-points-outcome-recovery/1.0"})
    last = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read()
                if response.status != 200:
                    raise RuntimeError(f"HTTP_{response.status}")
                data = json.loads(body)
                if int(data.get("id", -1)) != game_id or int(str(data.get("season", "0"))[:4]) != 2025 or int(data.get("gameType", 0)) != 2:
                    raise RuntimeError(f"IDENTITY_MISMATCH:{game_id}")
                return body
        except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(2**attempt, 16))
    raise RuntimeError(f"OFFICIAL_NHL_PBP_FAILED:{game_id}:{last}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", type=Path, required=True)
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    ledger = []
    stamp = datetime.now(timezone.utc).isoformat()
    for gid in IDS:
        body = fetch(gid)
        path = args.output_dir / f"game={gid}.play-by-play.json"
        path.write_bytes(body)
        ledger.append({"game_id": gid, "endpoint": BASE.format(game_id=gid), "path": path.name,
                       "sha256": sha(body), "bytes": len(body), "method": "GET",
                       "acquired_at_utc": stamp, "retry_policy": "up to 5 attempts; exponential backoff"})
    (args.output_dir / "request_ledger.json").write_text(json.dumps(ledger, indent=2, sort_keys=True)+"\n")
    print(json.dumps({"requests": len(ledger), "credits": 0, "acquired_at_utc": stamp, "responses": ledger}, indent=2))


if __name__ == "__main__":
    main()
