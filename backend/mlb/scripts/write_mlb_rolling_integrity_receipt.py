"""Persist run-bound evidence for the natural rolling-integrity gate."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
RECEIPT_ROOT = ROOT / "artifacts/ops/mlb_rolling_integrity_receipts"


def build_receipt(
    *, run_id: str, slate_date: str, started_at_utc: str, ended_at_utc: str,
    exit_code: int, output_path: Path,
) -> dict[str, Any]:
    output_bytes = output_path.read_bytes()
    output_text = output_bytes.decode("utf-8", errors="replace")
    passed = exit_code == 0 and "PASS mlb rolling integrity" in output_text
    status = "PASS" if passed else "FAIL" if exit_code != 0 else "COMPLETED_NO_PASS_MARKER"
    receipt: dict[str, Any] = {
        "schema_version": "MLB_ROLLING_INTEGRITY_RUN_RECEIPT_V1",
        "run_id": run_id,
        "slate_date": slate_date,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "status": status,
        "exit_code": exit_code,
        "result_marker_present": "PASS mlb rolling integrity" in output_text,
        "output_path": str(output_path),
        "output_bytes": len(output_bytes),
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
    }
    canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    receipt["receipt_sha256"] = hashlib.sha256(canonical).hexdigest()
    return receipt


def write_receipt(**kwargs: Any) -> Path:
    receipt = build_receipt(**kwargs)
    path = RECEIPT_ROOT / receipt["slate_date"] / f"{receipt['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--started-at-utc", required=True)
    parser.add_argument("--ended-at-utc", required=True)
    parser.add_argument("--exit-code", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    path = write_receipt(
        run_id=args.run_id, slate_date=args.slate_date,
        started_at_utc=args.started_at_utc, ended_at_utc=args.ended_at_utc,
        exit_code=args.exit_code, output_path=args.output,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
