#!/usr/bin/env python3
"""Create a reproducible MLB model snapshot manifest + archive copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _collect_files(src: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for p in sorted(src.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(src).as_posix()
        rows.append(
            {
                "path": rel,
                "size_bytes": p.stat().st_size,
                "sha256": _sha256(p),
            }
        )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Snapshot MLB model directory into archive + manifest.")
    ap.add_argument("--source", required=True)
    ap.add_argument("--archive-dir", required=True)
    ap.add_argument("--snapshot-id", required=True)
    ap.add_argument("--manifest-output", required=True)
    ap.add_argument("--copy", action="store_true")
    args = ap.parse_args()

    source = Path(args.source).resolve()
    archive_dir = Path(args.archive_dir).resolve()
    snapshot_id = str(args.snapshot_id).strip()
    manifest_output = Path(args.manifest_output).resolve()
    snapshot_path = archive_dir / snapshot_id

    if not source.exists() or not source.is_dir():
        raise SystemExit(f"source missing or not a directory: {source}")
    archive_dir.mkdir(parents=True, exist_ok=True)
    manifest_output.parent.mkdir(parents=True, exist_ok=True)

    if args.copy:
        if snapshot_path.exists():
            raise SystemExit(f"snapshot already exists: {snapshot_path}")
        shutil.copytree(source, snapshot_path)

    source_for_manifest = snapshot_path if args.copy else source
    files = _collect_files(source_for_manifest)
    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "pass",
        "ok": True,
        "snapshot_id": snapshot_id,
        "source": str(source),
        "archive_path": str(snapshot_path) if args.copy else None,
        "file_count": len(files),
        "total_size_bytes": sum(int(r["size_bytes"]) for r in files),
        "files": files,
    }
    manifest_output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

