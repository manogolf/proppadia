#!/usr/bin/env python3
"""Fail-closed containment for a failed legacy MLB stat-derived stage.

This module never runs the stat-derived stage. It records that stage's retained
failure, skips dependent/unproven work, and invokes only the two independently
governed hooks already present later in the daily wrapper.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence


CONTRACT_PATH = Path("backend/mlb/contracts/mlb_stat_derived_degraded_stage_containment_v1.json")
SKIP_STATUS = "SKIPPED_UPSTREAM_STAT_DERIVED_UNAVAILABLE"
CLASSIFICATION = "DEGRADED_STAT_DERIVED_FAILED_INDEPENDENT_STAGES_COMPLETED"
KNOWN_DEFECT = "POSTPONED_AS_FINAL_ADMISSION_PLUS_UNORDERED_DELETE_INSERT_CTE"
KNOWN_FAILURE_CLASSIFICATION = "UNIQUE_VIOLATION_PLAYER_DERIVED_STATS_PLAYER_GAME"
FINGERPRINT_CLASSIFIER_VERSION = "MLB_STAT_DERIVED_FAILURE_FINGERPRINT_V2"
MAX_EXCERPT_LINES = 12
MAX_EXCERPT_LINE_CHARS = 1000


@dataclass(frozen=True)
class Context:
    slate_date: str
    completed_slate_date: str
    run_identity: str
    wrapper_started_at_utc: str
    stat_derived_rc: int
    failure_stdout_log: Path
    failure_stderr_log: Path
    receipt_root: Path
    bvp_inline_rc: int = 0
    bvp_inline_result: str = ""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sanitize(line: str) -> str:
    value = re.sub(
        r"(?i)(postgres(?:ql)?://)[^\s\"'<>]+",
        r"\1[REDACTED]",
        line.strip(),
    )
    value = re.sub(
        r"(?i)\b(api[_-]?key|token|password)\s*[:=]\s*[\"']?[^\s\"',}]+",
        lambda match: f"{match.group(1)}=[REDACTED]",
        value,
    )
    value = re.sub(
        r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s\"',}]+",
        r"\1[REDACTED]",
        value,
    )
    return value[:MAX_EXCERPT_LINE_CHARS]


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError:
        return b""


def _canonical_output_sha256(stdout: bytes, stderr: bytes) -> str:
    """Hash the complete ordered stream pair without inventing interleaving."""
    digest = hashlib.sha256()
    digest.update(b"MLB_STAT_DERIVED_CANONICAL_STREAM_PAIR_V1\0")
    digest.update(b"stdout\0")
    digest.update(len(stdout).to_bytes(8, "big"))
    digest.update(stdout)
    digest.update(b"stderr\0")
    digest.update(len(stderr).to_bytes(8, "big"))
    digest.update(stderr)
    return digest.hexdigest()


def _bounded_excerpt(text: str) -> str:
    patterns = (
        "UniqueViolation",
        "duplicate key value violates unique constraint",
        "DETAIL:",
        "already exists",
        "Traceback",
        "OperationalError",
        "DatabaseError",
        "make[",
        "Error ",
        "Exception",
    )
    selected = []
    for raw in text.splitlines():
        if any(token.lower() in raw.lower() for token in patterns):
            selected.append(_sanitize(raw))
    if not selected:
        selected = [_sanitize(raw) for raw in text.splitlines() if raw.strip()]
    return "\n".join(selected[-MAX_EXCERPT_LINES:])


def _structured_failure(text: str, rc: int) -> dict:
    exception_match = re.search(r"\b(UniqueViolation)\b", text)
    constraint_match = re.search(
        r"unique constraint\s+[\"']?([A-Za-z0-9_.-]+)[\"']?",
        text,
        flags=re.IGNORECASE,
    )
    identity_match = re.search(
        r"Key\s*\(\s*player_id\s*,\s*game_id\s*\)\s*=\s*"
        r"\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*already exists\.?",
        text,
        flags=re.IGNORECASE,
    )
    detail_match = re.search(
        r"DETAIL:\s*(Key\s*\(\s*player_id\s*,\s*game_id\s*\)\s*=\s*"
        r"\(\s*\d+\s*,\s*\d+\s*\)\s*already exists\.?)",
        text,
        flags=re.IGNORECASE,
    )
    exception_class = exception_match.group(1) if exception_match else None
    constraint_name = constraint_match.group(1) if constraint_match else None
    player_id = int(identity_match.group(1)) if identity_match else None
    game_id = int(identity_match.group(2)) if identity_match else None
    database_detail = (
        re.sub(r"\s+", " ", detail_match.group(1)).strip()
        if detail_match
        else None
    )
    boundary_found = bool(
        exception_class == "UniqueViolation"
        and constraint_name == "player_derived_stats_player_id_game_id_key"
        and player_id is not None
        and game_id is not None
        and database_detail is not None
    )
    classification = (
        KNOWN_FAILURE_CLASSIFICATION
        if boundary_found
        else "UNCLASSIFIED_STAT_DERIVED_NONZERO"
    )
    return {
        "classifier_version": FINGERPRINT_CLASSIFIER_VERSION,
        "classification": classification,
        "exception_class": exception_class,
        "constraint_name": constraint_name,
        "player_id": player_id,
        "game_id": game_id,
        "gamePk": game_id,
        "database_detail": database_detail,
        "stage_exit_code": int(rc),
        "boundary_found": boundary_found,
    }


def failure_fingerprint(stdout_path: Path, stderr_path: Path, rc: int) -> dict:
    stdout = _read_bytes(stdout_path)
    stderr = _read_bytes(stderr_path)
    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")
    # Parse complete original streams before excerpt selection, sanitization, or
    # truncation. Concatenation is only for recognition; the canonical stream
    # hash records the ordered pair and does not invent cross-stream ordering.
    structured = _structured_failure(stdout_text + "\n" + stderr_text, rc)
    canonical_fields = {
        key: structured[key]
        for key in (
            "classifier_version",
            "classification",
            "exception_class",
            "constraint_name",
            "player_id",
            "game_id",
            "database_detail",
            "stage_exit_code",
        )
    }
    canonical_structured = json.dumps(
        canonical_fields,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    stdout_excerpt = _bounded_excerpt(stdout_text)
    stderr_excerpt = _bounded_excerpt(stderr_text)
    exact_error_text = "\n".join(
        value for value in (stdout_excerpt, stderr_excerpt) if value
    ) or f"mlb-stat-derived-refresh exited rc={rc}; no bounded error line recovered"
    return {
        **structured,
        "fingerprint_sha256": _sha256_bytes(canonical_structured),
        "stdout_sha256": _sha256_bytes(stdout),
        "stderr_sha256": _sha256_bytes(stderr),
        "combined_canonical_output_sha256": _canonical_output_sha256(stdout, stderr),
        "combined_canonicalization": "MLB_STAT_DERIVED_CANONICAL_STREAM_PAIR_V1",
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "stdout_byte_count": len(stdout),
        "stderr_byte_count": len(stderr),
        "stdout_excerpt": stdout_excerpt,
        "stderr_excerpt": stderr_excerpt,
        "exact_error_text": exact_error_text,
    }


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def _load_contract(path: Path = CONTRACT_PATH) -> dict:
    contract = json.loads(path.read_text(encoding="utf-8"))
    if contract.get("overall_classification") != CLASSIFICATION:
        raise ValueError("containment contract classification mismatch")
    return contract


def _requested_dates(slate_date: str) -> dict:
    end = date.fromisoformat(slate_date) - timedelta(days=1)
    start = date.fromisoformat(slate_date) - timedelta(days=2)
    return {
        "days_ago": 2,
        "effective_from_date": start.isoformat(),
        "effective_to_date": end.isoformat(),
        "skip_existing_dates": True,
        "derived_days": 7,
        "derived_min": 0,
        "require_regular_season": True,
    }


def _default_commands(ctx: Context) -> Mapping[str, Sequence[str]]:
    return {
        "full-game-totals-daily-hook": [
            "bin/mlb_full_game_totals_daily_hook.sh", ctx.slate_date, ctx.run_identity
        ],
        "totals-prospective-shadow-daily-hook": [
            "bin/mlb_totals_prospective_shadow_daily_hook.sh",
            ctx.slate_date,
            ctx.completed_slate_date,
            ctx.run_identity,
            ctx.wrapper_started_at_utc,
            "auto",
        ],
    }


def _new_receipt(ctx: Context, contract: dict) -> dict:
    stages = [{
        "sequence": 0,
        "name": "mlb-stat-derived-refresh",
        "classification": "FAILED_STAGE",
        "status": "FAILED",
        "exit_code": ctx.stat_derived_rc,
    }]
    for item in contract["stages_after_failure"]:
        stage = dict(item)
        if stage["name"] == "wrapper-summary-and-lock-release":
            stage["status"] = "DEFERRED_TO_WRAPPER_EXIT_TRAP"
        elif stage["classification"] == "PROVEN_INDEPENDENT":
            stage["status"] = "PENDING"
            stage["attempt_count"] = 0
        else:
            stage["status"] = SKIP_STATUS
            stage["reason"] = "stat-derived output unavailable; stale output is not admissible"
        stages.append(stage)
    return {
        "contract": contract["contract"],
        "run_identity": ctx.run_identity,
        "slate_date": ctx.slate_date,
        "completed_slate_date": ctx.completed_slate_date,
        "wrapper_started_at_utc": ctx.wrapper_started_at_utc,
        "failure_timestamp_utc": _utc_now(),
        "requested_dates": _requested_dates(ctx.slate_date),
        "failure": {
            "stage": "mlb-stat-derived-refresh",
            "exit_code": ctx.stat_derived_rc,
            "fingerprint": failure_fingerprint(
                ctx.failure_stdout_log,
                ctx.failure_stderr_log,
                ctx.stat_derived_rc,
            ),
        },
        "upstream": {
            "bvp_inline_rc": ctx.bvp_inline_rc,
            "bvp_inline_result": ctx.bvp_inline_result,
            "bvp_was_not_reinvoked": True,
        },
        "stages": stages,
        "governance": {
            "stale_stat_derived_output_admitted": False,
            "completion_checkpoint_advanced": False,
            "new_request_eligibility_added": False,
            "ordinary_hook_claims_and_credit_guards_preserved": True,
        },
        "overall": {
            "classification": CLASSIFICATION,
            "wrapper_exit_code": ctx.stat_derived_rc,
            "receipt_state": "IN_PROGRESS",
        },
    }


def run_containment(
    ctx: Context,
    *,
    commands: Mapping[str, Sequence[str]] | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    contract_path: Path = CONTRACT_PATH,
) -> tuple[Path, dict]:
    if ctx.stat_derived_rc == 0:
        raise ValueError("containment cannot run after a successful stat-derived stage")
    contract = _load_contract(contract_path)
    commands = commands or _default_commands(ctx)
    receipt_path = ctx.receipt_root / ctx.slate_date / f"{ctx.run_identity}.json"
    lock_path = receipt_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            if receipt.get("run_identity") != ctx.run_identity or receipt.get("failure", {}).get("exit_code") != ctx.stat_derived_rc:
                raise RuntimeError("existing containment receipt conflicts with invocation identity")
        else:
            receipt = _new_receipt(ctx, contract)
            _atomic_write(receipt_path, receipt)

        stage_by_name = {stage["name"]: stage for stage in receipt["stages"]}
        allowed = {item["name"] for item in contract["stages_after_failure"] if item["classification"] == "PROVEN_INDEPENDENT"}
        allowed.discard("wrapper-summary-and-lock-release")
        if set(commands) != allowed:
            raise RuntimeError(f"independent command set mismatch: expected={sorted(allowed)} actual={sorted(commands)}")

        for name in sorted(allowed, key=lambda n: stage_by_name[n]["sequence"]):
            stage = stage_by_name[name]
            if stage["status"] != "PENDING":
                continue
            stage["status"] = "RUNNING_CLAIMED"
            stage["attempt_count"] = 1
            stage["claimed_at_utc"] = _utc_now()
            stage["command"] = list(commands[name])
            _atomic_write(receipt_path, receipt)
            try:
                result = runner(list(commands[name]), check=False)
                rc = int(result.returncode)
                stage["exit_code"] = rc
                stage["status"] = "COMPLETED" if rc == 0 else "FAILED_INDEPENDENT_STAGE"
                stage["finished_at_utc"] = _utc_now()
                _atomic_write(receipt_path, receipt)
            except BaseException as exc:
                stage["status"] = "INTERRUPTED_OR_EXCEPTION_AFTER_DURABLE_CLAIM"
                stage["exception"] = f"{type(exc).__name__}: {exc}"
                stage["finished_at_utc"] = _utc_now()
                receipt["overall"]["receipt_state"] = "INTERRUPTED"
                _atomic_write(receipt_path, receipt)
                raise

        interrupted = any(
            stage.get("status") == "INTERRUPTED_OR_EXCEPTION_AFTER_DURABLE_CLAIM"
            for stage in receipt["stages"]
        )
        receipt["overall"]["receipt_state"] = (
            "INCOMPLETE_CLAIMED_STAGE_NOT_RETRIED" if interrupted else "COMPLETE"
        )
        receipt["overall"]["finished_at_utc"] = _utc_now()
        _atomic_write(receipt_path, receipt)
        return receipt_path, receipt


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--completed-slate-date", required=True)
    parser.add_argument("--run-identity", required=True)
    parser.add_argument("--wrapper-started-at-utc", required=True)
    parser.add_argument("--stat-derived-rc", required=True, type=int)
    parser.add_argument("--failure-stdout-log", type=Path, required=True)
    parser.add_argument("--failure-stderr-log", type=Path, required=True)
    parser.add_argument("--receipt-root", type=Path, default=Path("artifacts/ops/mlb_stat_derived_degraded_containment_v1"))
    parser.add_argument("--bvp-inline-rc", type=int, default=0)
    parser.add_argument("--bvp-inline-result", default="")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    ctx = Context(
        slate_date=args.slate_date,
        completed_slate_date=args.completed_slate_date,
        run_identity=args.run_identity,
        wrapper_started_at_utc=args.wrapper_started_at_utc,
        stat_derived_rc=args.stat_derived_rc,
        failure_stdout_log=args.failure_stdout_log,
        failure_stderr_log=args.failure_stderr_log,
        receipt_root=args.receipt_root,
        bvp_inline_rc=args.bvp_inline_rc,
        bvp_inline_result=args.bvp_inline_result,
    )
    receipt_path, receipt = run_containment(ctx)
    print(
        f"classification={CLASSIFICATION} stat_derived_rc={ctx.stat_derived_rc} "
        f"receipt={receipt_path} receipt_state={receipt['overall']['receipt_state']}"
    )
    return 0 if receipt["overall"]["receipt_state"] == "COMPLETE" else 76


if __name__ == "__main__":
    raise SystemExit(main())
