"""Install the run-bound receipt call into the external natural-run wrapper."""
from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
from pathlib import Path

OLD_TRAP = (
    b"trap 'wrapper_rc=$?; release_launchagent_locks; "
    b"write_launchagent_summary \"$wrapper_rc\"; exit \"$wrapper_rc\"' EXIT"
)
NEW_TRAP = b'''write_natural_wrapper_receipt() {
  .venv/bin/python -m backend.mlb.scripts.write_mlb_daily_wrapper_receipt_v1 \\
    --run-identity "$MLB_RUN_TAG" \\
    --started-at-utc "$MLB_WRAPPER_RUN_STARTED_AT_UTC" \\
    --wrapper-rc "$1"
}
trap 'wrapper_rc=$?; release_launchagent_locks; if ! write_natural_wrapper_receipt "$wrapper_rc"; then echo "[$(date -u +%FT%TZ)] WARN per-run wrapper receipt failed; original_wrapper_rc=${wrapper_rc}" >&2; fi; write_launchagent_summary "$wrapper_rc"; exit "$wrapper_rc"' EXIT'''


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def install(path: Path) -> dict[str, str]:
    path = Path(path)
    original = path.read_bytes()
    backup = path.with_name(path.name + ".pre_wrapper_run_receipt_v1")
    if NEW_TRAP in original:
        return {"status": "ALREADY_INSTALLED", "path": str(path), "sha256": _sha(original)}
    if original.count(OLD_TRAP) != 1:
        raise RuntimeError("EXPECTED_WRAPPER_EXIT_TRAP_NOT_FOUND_EXACTLY_ONCE")
    if backup.exists():
        raise FileExistsError(f"wrapper rollback backup already exists: {backup}")
    updated = original.replace(OLD_TRAP, NEW_TRAP, 1)
    backup_fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, path.stat().st_mode & 0o777)
    with os.fdopen(backup_fd, "wb") as handle:
        handle.write(original)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(backup, path.stat().st_mode & 0o777)
    temp_fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temp_name)
    try:
        with os.fdopen(temp_fd, "wb") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "status": "INSTALLED",
        "path": str(path),
        "backup_path": str(backup),
        "previous_sha256": _sha(original),
        "installed_sha256": _sha(updated),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wrapper", type=Path)
    args = parser.parse_args()
    print(install(args.wrapper))


if __name__ == "__main__":
    main()
