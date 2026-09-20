"""Privacy-safe process provenance for NHL observer status receipts."""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


LAUNCHD_LABEL = "com.proppadia.nhl.mainline-cross-market-shadow"


def _sha256(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        return digest.hexdigest()
    except OSError:
        return None


def _parent_identity() -> str | None:
    """Return only the executable basename, never arguments or environment."""
    try:
        result = subprocess.run(
            ["/bin/ps", "-p", str(os.getppid()), "-o", "comm="],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        value = result.stdout.strip()
        return Path(value).name if value else None
    except (OSError, subprocess.SubprocessError):
        return None


def observer_provenance(script_path: Path, now: datetime | None = None) -> dict[str, object]:
    now = now or datetime.now(timezone.utc)
    explicit = os.environ.get("PROPPADIA_INVOCATION_ORIGIN", "").strip()
    validation_id = os.environ.get("PROPPADIA_VALIDATION_ID", "").strip()
    service = os.environ.get("XPC_SERVICE_NAME", "").strip()
    if explicit.upper() in {"VALIDATION_TEST", "CODEX_VALIDATION_SMOKE_TEST"} or validation_id:
        classification = "VALIDATION_TEST"
        origin = explicit or "VALIDATION_TEST"
    elif service == LAUNCHD_LABEL:
        classification = "LAUNCHAGENT"
        origin = service
    else:
        classification = "MANUAL_OR_UNKNOWN"
        origin = explicit or "UNDECLARED"
    executable = Path(sys.executable).resolve()
    script = script_path.resolve()
    return {
        "invocation_origin": origin,
        "invocation_classification": classification,
        "run_or_validation_identifier": validation_id or None,
        "process_id": os.getpid(),
        "parent_process_id": os.getppid(),
        "parent_process_identity": _parent_identity(),
        "invocation_timestamp_utc": now.astimezone(timezone.utc).isoformat(),
        "executable_sha256": _sha256(executable),
        "script_sha256": _sha256(script),
    }
