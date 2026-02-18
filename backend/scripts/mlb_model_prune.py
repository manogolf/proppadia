#!/usr/bin/env python3
"""Prune old model snapshots in archive directory."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List


def main() -> int:
    ap = argparse.ArgumentParser(description="Prune model archive snapshots, keeping newest N.")
    ap.add_argument("--archive-dir", required=True)
    ap.add_argument("--keep", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    archive_dir = Path(args.archive_dir).resolve()
    keep = max(0, int(args.keep))
    if not archive_dir.exists():
        payload = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "status": "pass",
            "ok": True,
            "archive_dir": str(archive_dir),
            "snapshots": [],
            "deleted": [],
            "dry_run": bool(args.dry_run),
        }
        print(json.dumps(payload, indent=2))
        return 0

    snapshots: List[Path] = [p for p in archive_dir.iterdir() if p.is_dir()]
    snapshots.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    to_delete = snapshots[keep:]

    deleted = []
    for p in to_delete:
        if not args.dry_run:
            shutil.rmtree(p)
        deleted.append(p.name)

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass",
        "ok": True,
        "archive_dir": str(archive_dir),
        "snapshots": [p.name for p in snapshots],
        "deleted": deleted,
        "dry_run": bool(args.dry_run),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

