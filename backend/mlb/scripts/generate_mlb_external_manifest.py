#!/usr/bin/env python3
"""Generate a deterministic SHA256 manifest without modifying source files."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output.resolve()
    rows = []
    for path in sorted(p for p in root.rglob("*") if p.is_file() and p.resolve() != output):
        rows.append({"path": str(path.relative_to(root)), "size_bytes": path.stat().st_size,
                     "mtime_ns": path.stat().st_mtime_ns, "sha256": digest(path)})
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["path", "size_bytes", "mtime_ns", "sha256"]).to_csv(output, index=False)
    print(f"files={len(rows)} output={output}")


if __name__ == "__main__":
    main()
