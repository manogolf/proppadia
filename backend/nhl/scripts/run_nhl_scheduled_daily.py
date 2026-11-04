#!/usr/bin/env python3
"""Scheduled NHL daily run: odds capture, game-line refresh and the 8rain file (P2.33).

Runs the operator's 8rain sequence (docs/NHL SOG Command Deck.md, "Current
8rain Operator Sequence") unattended, in order:

1. ``backend.nhl.cli daily --with-odds``
2. the current-slate mainline cross-market capture (AUTO when today has no
   package yet, otherwise REFRESH)
3. ``run_nhl_8rain_export.py --latest-refresh``

Uploading the CSV to 8rain stays manual. launchd does not wake a sleeping Mac,
so after a wake the 05:30 morning prep and this job can start together; this
job first waits (up to 20 minutes) for a running morning prep to finish. A
run is skipped (exit 0) when another
scheduled run holds the lock or a manual ``daily`` is already running. A failed
daily run stops before the export, so the newest file in
``artifacts/operational/nhl/8rain_uploads/<date>/`` is always from a good run.
Each run writes step logs and ``summary.json`` under
``artifacts/operational/nhl/scheduled_daily/<date>/<run_id>/``.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / "bin" / "python"
OUT = ROOT / "artifacts/operational/nhl/scheduled_daily"
DAILY_RUNS = ROOT / "artifacts/operational/nhl/daily_runs"
CROSS_MARKET = ROOT / "artifacts/operational/nhl/cross_market_shadow"
ENV_FILE = ROOT / "backend/.env"
STEP_TIMEOUT_SECONDS = 45 * 60
MANUAL_DAILY_PATTERN = "backend.nhl.cli daily"
MORNING_PATTERN = "run_nhl_morning_orchestration.py"
MORNING_WAIT_SECONDS = 20 * 60


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def slate_today() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def manual_daily_running(own_pid: int) -> bool:
    """True when some other process is running the daily CLI."""
    found = subprocess.run(["pgrep", "-f", MANUAL_DAILY_PATTERN], capture_output=True, text=True)
    return any(int(pid) != own_pid for pid in found.stdout.split())


def wait_for_morning_prep(timeout: int = MORNING_WAIT_SECONDS, poll: int = 15) -> bool:
    """Wait while the morning prep is running; False if it is still running at the timeout."""
    deadline = time.monotonic() + timeout
    while subprocess.run(["pgrep", "-f", MORNING_PATTERN], capture_output=True).returncode == 0:
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll)
    return True


def has_cross_market_package(slate: str) -> bool:
    return any((CROSS_MARKET / f"season={slate[:4]}" / f"slate_date={slate}").glob("run_type=*/state=*"))


def latest_daily_receipt(after_utc: str) -> dict | None:
    """The newest daily run receipt written after this scheduled run started."""
    start = datetime.fromisoformat(after_utc.replace("Z", "+00:00")).timestamp()
    receipts = [p for p in DAILY_RUNS.glob("run_id=*/RUN_COMPLETE.json") if p.stat().st_mtime >= start]
    if not receipts:
        return None
    newest = max(receipts, key=lambda p: p.stat().st_mtime)
    return {"path": str(newest.relative_to(ROOT)), **json.loads(newest.read_text())}


def run_step(name: str, command: list[str], run_dir: Path, timeout: int) -> dict:
    started = utc_now()
    log = run_dir / f"{name}.log"
    try:
        with log.open("w") as handle:
            done = subprocess.run(command, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT, timeout=timeout)
        code = done.returncode
    except subprocess.TimeoutExpired:
        code = "TIMEOUT"
    return {"step": name, "command": [str(c) for c in command], "started_utc": started,
            "finished_utc": utc_now(), "exit_code": code, "log": str(log.relative_to(ROOT))}


def export_csv_path(log: Path) -> str | None:
    match = re.search(r"^CSV: (.+)$", log.read_text(), flags=re.MULTILINE)
    return match.group(1).strip() if match else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Resolve and print the steps without running them")
    args = parser.parse_args(argv)
    slate = slate_today()
    (OUT / "locks").mkdir(parents=True, exist_ok=True)
    lock = (OUT / "locks" / "scheduled_daily.lock").open("a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("NHL_SCHEDULED_DAILY=SKIPPED_SCHEDULED_RUN_ACTIVE")
        return 0
    if manual_daily_running(os.getpid()):
        print("NHL_SCHEDULED_DAILY=SKIPPED_MANUAL_DAILY_ACTIVE")
        return 0
    if not args.dry_run and not wait_for_morning_prep():
        print("NHL_SCHEDULED_DAILY=SKIPPED_MORNING_PREP_STILL_RUNNING")
        return 1
    phase = "REFRESH" if has_cross_market_package(slate) else "AUTO"
    steps = [
        ("01_daily_with_odds", [PYTHON, "-m", "backend.nhl.cli", "daily", "--with-odds"]),
        ("02_cross_market_capture", [PYTHON, ROOT / "backend/nhl/scripts/run_nhl_mainline_cross_market_capture_warn_only.py",
                                     "--slate-date", slate, "--phase", phase, "--env-file", ENV_FILE]),
        ("03_8rain_export", [PYTHON, ROOT / "backend/nhl/scripts/run_nhl_8rain_export.py", "--latest-refresh"]),
    ]
    if args.dry_run:
        print(json.dumps({"slate": slate, "steps": [[str(c) for c in cmd] for _, cmd in steps]}, indent=2))
        return 0
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = OUT / slate / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    summary = {"schema_version": "NHL_SCHEDULED_DAILY_V1", "slate_date": slate, "run_id": run_id,
               "started_utc": utc_now(), "steps": [], "status": "RUNNING", "upload_csv": None}
    write = lambda: (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write()
    for name, command in steps:
        result = run_step(name, command, run_dir, STEP_TIMEOUT_SECONDS)
        summary["steps"].append(result)
        if name == "01_daily_with_odds":
            receipt = latest_daily_receipt(summary["started_utc"])
            result["daily_classification"] = receipt.get("final_classification") if receipt else None
            result["daily_receipt"] = receipt.get("path") if receipt else None
            if result["exit_code"] != 0 or result["daily_classification"] != "READY":
                summary["status"] = "FAILED_DAILY"
                break
        elif name == "02_cross_market_capture" and result["exit_code"] != 0:
            # The export falls back to today's newest valid package; record it.
            summary["cross_market_warning"] = f"capture exit {result['exit_code']}"
        elif name == "03_8rain_export":
            if result["exit_code"] != 0:
                summary["status"] = "FAILED_EXPORT"
                break
            summary["upload_csv"] = export_csv_path(ROOT / result["log"])
        write()
    else:
        summary["status"] = "READY"
    summary["finished_utc"] = utc_now()
    write()
    print("NHL_SCHEDULED_DAILY=" + json.dumps({"status": summary["status"], "slate": slate,
                                               "upload_csv": summary["upload_csv"]}))
    return 0 if summary["status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
