#!/usr/bin/env python3
"""Clean transient failure entries from NHL SOG calibration history JSONL."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DEFAULT_ERROR_SUBSTRINGS = [
    "nodename nor servname provided, or not known",
    "name or service not known",
    "temporary failure in name resolution",
]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists() or not path.is_file():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _is_transient_dns_failure(row: dict[str, Any], substrings: list[str]) -> bool:
    checks = row.get("checks") or {}
    if not isinstance(checks, dict):
        return False
    exp = checks.get("experiment") or {}
    if not isinstance(exp, dict):
        return False
    err = str(exp.get("error") or "").strip().lower()
    if not err:
        return False
    return any(token in err for token in substrings)


def _is_incomplete_experiment_failure(row: dict[str, Any]) -> bool:
    failures = row.get("failures") or []
    if not isinstance(failures, list):
        return False
    if "experiment_failed" not in failures:
        return False
    # Typical signature of non-actionable transient run:
    # no experiment config/counts payload and line deltas all missing.
    summary = row.get("experiment_summary") or {}
    if not isinstance(summary, dict):
        return False
    if summary.get("config") is not None:
        return False
    if summary.get("counts") is not None:
        return False
    missing_delta_flags = [f for f in failures if str(f).startswith("missing_line_delta:")]
    return len(missing_delta_flags) > 0


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Clean transient DNS failures from NHL SOG calibration history JSONL.")
    ap.add_argument("--input", default="artifacts/nhl_sog_calibration_history.jsonl")
    ap.add_argument("--output", default="")
    ap.add_argument("--in-place", action="store_true")
    ap.add_argument("--backup", action="store_true", help="When rewriting in-place, write a backup first.")
    ap.add_argument(
        "--error-substring",
        action="append",
        default=[],
        help="Case-insensitive error substring to drop (repeatable).",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    in_path = Path(args.input)
    if args.in_place:
        out_path = in_path
    elif str(args.output).strip():
        out_path = Path(str(args.output).strip())
    else:
        raise SystemExit("--output is required unless --in-place is set.")

    rows = _load_jsonl(in_path)
    tokens = [str(x).lower() for x in (args.error_substring or []) if str(x).strip()]
    if not tokens:
        tokens = [x.lower() for x in DEFAULT_ERROR_SUBSTRINGS]

    kept: list[dict[str, Any]] = []
    dropped = 0
    dropped_dns = 0
    dropped_incomplete = 0
    for row in rows:
        if _is_transient_dns_failure(row, tokens):
            dropped += 1
            dropped_dns += 1
            continue
        if _is_incomplete_experiment_failure(row):
            dropped += 1
            dropped_incomplete += 1
            continue
        kept.append(row)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if out_path == in_path and bool(args.backup) and in_path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = in_path.with_name(f"{in_path.name}.bak.{stamp}")
        backup_path.write_text(in_path.read_text(encoding="utf-8"), encoding="utf-8")

    with out_path.open("w", encoding="utf-8") as fh:
        for row in kept:
            fh.write(json.dumps(row, default=str))
            fh.write("\n")

    summary = {
        "ok": True,
        "status": "pass",
        "input": str(in_path),
        "output": str(out_path),
        "backup": str(backup_path) if backup_path else None,
        "before_rows": len(rows),
        "after_rows": len(kept),
        "dropped_rows": dropped,
        "dropped_dns_rows": dropped_dns,
        "dropped_incomplete_rows": dropped_incomplete,
        "error_substrings": tokens,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
