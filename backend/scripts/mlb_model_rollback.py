#!/usr/bin/env python3
"""Rollback model latest to a prior archive snapshot."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone


def main() -> int:
    ap = argparse.ArgumentParser(description="Rollback latest model dir to a specified snapshot.")
    ap.add_argument("--archive-dir", required=True)
    ap.add_argument("--snapshot-id", required=True)
    ap.add_argument("--latest-dir", required=True)
    args = ap.parse_args()

    cmd = [
        sys.executable,
        "backend/scripts/mlb_model_publish.py",
        "--archive-dir",
        args.archive_dir,
        "--snapshot-id",
        args.snapshot_id,
        "--latest-dir",
        args.latest_dir,
        "--backup-prefix",
        "latest.rollback.prev",
    ]
    proc = subprocess.run(cmd, check=False)
    rc = proc.returncode

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass" if rc == 0 else "fail",
        "ok": rc == 0,
        "snapshot_id": args.snapshot_id,
        "latest_dir": args.latest_dir,
    }
    print(json.dumps(payload, indent=2))
    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
