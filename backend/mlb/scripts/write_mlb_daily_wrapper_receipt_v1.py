"""Atomically retain the exact exit status of one natural MLB wrapper run."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = ROOT / "artifacts/ops/mlb_wrapper_run_receipts"


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("WRAPPER_RECEIPT_TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    return parsed.astimezone(timezone.utc)


def write_receipt(
    *, run_identity: str, started_at_utc: str, wrapper_rc: int,
    root: Path = DEFAULT_ROOT, finished_at_utc: str | None = None,
) -> dict[str, str | int]:
    if not re.fullmatch(r"local_daily_[A-Za-z0-9_.-]+", str(run_identity)):
        raise ValueError("WRAPPER_RECEIPT_RUN_ID_INVALID")
    rc = int(wrapper_rc)
    if rc < 0 or rc > 255:
        raise ValueError("WRAPPER_RECEIPT_EXIT_STATUS_INVALID")
    started = _timestamp(started_at_utc)
    finished = _timestamp(finished_at_utc or datetime.now(timezone.utc).isoformat())
    if finished < started:
        raise ValueError("WRAPPER_RECEIPT_FINISH_PRECEDES_START")
    payload: dict[str, str | int] = {
        "schema_version": "MLB_NATURAL_WRAPPER_RUN_RECEIPT_V1",
        "run_identity": run_identity,
        "started_at_utc": started.isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished.isoformat().replace("+00:00", "Z"),
        "wrapper_rc": rc,
        "status": "SUCCEEDED" if rc == 0 else "FAILED",
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload["receipt_sha256"] = hashlib.sha256(canonical).hexdigest()
    serialized = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    run_date = started.date().isoformat()
    destination = Path(root) / run_date / f"{run_identity}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{run_identity}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        # Hard-link publication is atomic and create-only: an existing receipt
        # can never be replaced by a later invocation using the same identity.
        os.link(temporary, destination)
        return {"path": str(destination), "sha256": hashlib.sha256(serialized).hexdigest(),
                "run_identity": run_identity, "wrapper_rc": rc}
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-identity", required=True)
    parser.add_argument("--started-at-utc", required=True)
    parser.add_argument("--wrapper-rc", required=True, type=int)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    print(json.dumps(write_receipt(
        run_identity=args.run_identity,
        started_at_utc=args.started_at_utc,
        wrapper_rc=args.wrapper_rc,
        root=args.root,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
