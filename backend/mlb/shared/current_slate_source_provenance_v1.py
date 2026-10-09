"""Immutable source-input receipts for natural MLB current-slate consumers."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


def retain_bytes(path: Path, raw: bytes) -> dict[str, Any]:
    """Retain exact consumed bytes create-only and return their content identity."""
    digest = hashlib.sha256(raw).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError(f"CURRENT_SLATE_SOURCE_CONFLICT:{path}")
    else:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(raw)
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise RuntimeError(f"CURRENT_SLATE_SOURCE_HASH_MISMATCH:{path}")
    return {"path": str(path), "sha256": digest, "bytes": len(raw)}


def write_receipt(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    """Write a content-addressed immutable receipt; never overwrite conflicts."""
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
    digest = hashlib.sha256(body).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != body:
            raise RuntimeError(f"CURRENT_SLATE_RECEIPT_CONFLICT:{path}")
    else:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(body)
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return {"path": str(path), "sha256": digest}


def validate_boundaries(counts: dict[str, int]) -> None:
    """Reject negative or impossible counts; boundaries may only lose rows."""
    previous: int | None = None
    for name, count in counts.items():
        if int(count) < 0:
            raise ValueError(f"CURRENT_SLATE_NEGATIVE_COUNT:{name}")
        if previous is not None and int(count) > previous:
            raise ValueError(f"CURRENT_SLATE_HANDOFF_GAIN:{name}")
        previous = int(count)


def input_status(count: int) -> str:
    return "VALID_EMPTY" if int(count) == 0 else "VALID_NONEMPTY"


def required_input(path: Path) -> tuple[bytes, dict[str, Any]]:
    """Read a required source; absence is missing evidence, never valid-empty."""
    raw = path.read_bytes()
    return raw, {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                 "bytes": len(raw), "status": "VALID_EMPTY" if not raw else "VALID_NONEMPTY"}


def filtering_boundary(input_count: int, output_count: int) -> dict[str, int]:
    """Account for retained and rejected rows at a non-expanding handoff."""
    before, after = int(input_count), int(output_count)
    if before < 0 or after < 0 or after > before:
        raise ValueError("CURRENT_SLATE_INVALID_FILTER_BOUNDARY")
    return {"input_rows": before, "output_rows": after, "rejected_rows": before - after}
