#!/usr/bin/env python3
"""Publish a snapshot into active latest with atomic-ish swap."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="Publish snapshot to model latest directory.")
    ap.add_argument("--archive-dir", required=True)
    ap.add_argument("--snapshot-id", required=True)
    ap.add_argument("--latest-dir", required=True)
    ap.add_argument("--backup-prefix", default="latest.prev")
    args = ap.parse_args()

    archive_dir = Path(args.archive_dir).resolve()
    snapshot_id = str(args.snapshot_id).strip()
    latest_dir = Path(args.latest_dir).resolve()
    snapshot_path = archive_dir / snapshot_id

    if not snapshot_path.exists() or not snapshot_path.is_dir():
        raise SystemExit(f"snapshot missing: {snapshot_path}")

    latest_parent = latest_dir.parent
    latest_parent.mkdir(parents=True, exist_ok=True)
    backup_dir = latest_parent / f"{args.backup_prefix}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    staging_dir = latest_parent / f".latest.staging.{snapshot_id}"

    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    shutil.copytree(snapshot_path, staging_dir)

    if latest_dir.exists():
        latest_dir.rename(backup_dir)
    staging_dir.rename(latest_dir)

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass",
        "ok": True,
        "snapshot_id": snapshot_id,
        "latest_dir": str(latest_dir),
        "backup_dir": str(backup_dir) if backup_dir.exists() else None,
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

