"""Detached prod12 cycle runner for ops-triggered automation."""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[4]
STATE_DIR = REPO_ROOT / "artifacts" / "ops"
STATE_PATH = STATE_DIR / "mlb_prod12_cycle_state.json"
LOG_PATH = STATE_DIR / "mlb_prod12_cycle.log"
LAUNCH_LOCK_PATH = STATE_DIR / ".mlb_prod12_cycle.launch.lock"

_ALLOWED_ENV_OVERRIDES = {
    "MLB_WEEKLY_BASE_URL",
    "MLB_DAILY_BASE_URL",
    "MLB_BASE_URL",
    "MLB_DATE",
    "MLB_CRON_RUN_MODE",
    "MLB_CRON_WEEKLY_DAY_UTC",
    "MLB_PROD12_DAILY_PROP_TYPES",
    "MLB_PROD12_PROP_TYPES",
    "MLB_WEEKLY_PROP_SEQUENCE_ENABLED",
    "MLB_WEEKLY_PROP_SEQUENCE",
    "MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR",
    "MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC",
    "MLB_REPLAY_SAMPLE",
    "MLB_REPLAY_MIN_SUCCESS",
    "MLB_PREDICT_SAMPLE",
    "MLB_PREDICT_MIN_SUCCESS",
    "MLB_REPLAY_RETRY_ATTEMPTS",
    "MLB_REPLAY_RETRY_BACKOFF_MS",
    "MLB_REPLAY_MAX_PREDICT_P95_MS",
    "MLB_WEEKLY_PHASE2_ENABLED",
    "MLB_CANDIDATE_MIN_TOTAL",
    "MLB_PROD12_MIN_LIFT_PCT",
    "MLB_PROD12_MAX_PROP_DROP_PCT",
}
_WATCHERS: dict[str, threading.Thread] = {}
_WATCHERS_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_state_dir()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _tail_log(path: Path, lines: int) -> list[str]:
    if lines <= 0 or not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            all_lines = fh.readlines()
        return [line.rstrip("\n") for line in all_lines[-lines:]]
    except Exception:
        return []


def _sanitize_env_overrides(env_overrides: Optional[dict[str, Any]]) -> dict[str, str]:
    clean: dict[str, str] = {}
    for key, value in (env_overrides or {}).items():
        if key not in _ALLOWED_ENV_OVERRIDES:
            continue
        if value is None:
            continue
        text = str(value).strip()
        if text == "":
            continue
        clean[key] = text
    return clean


def _build_command() -> list[str]:
    return ["bash", "-lc", "unset MODEL_DIR && bin/mlb_prod12_cron_cycle.sh"]


def _watch_process(proc: subprocess.Popen[Any], run_id: str) -> None:
    exit_code = proc.wait()
    finished_at = _now_iso()
    try:
        _ensure_state_dir()
        with LAUNCH_LOCK_PATH.open("a+", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            state = _read_json(STATE_PATH)
            if state.get("run_id") == run_id:
                state.update(
                    {
                        "status": "succeeded" if exit_code == 0 else "failed",
                        "running": False,
                        "finished_at": finished_at,
                        "exit_code": int(exit_code),
                    }
                )
                _write_json(STATE_PATH, state)
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
    finally:
        with _WATCHERS_LOCK:
            _WATCHERS.pop(run_id, None)


def start_prod12_cycle(
    *,
    triggered_by: str = "ops_api",
    env_overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    _ensure_state_dir()
    overrides = _sanitize_env_overrides(env_overrides)

    with LAUNCH_LOCK_PATH.open("a+", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        state = _read_json(STATE_PATH)
        prior_pid = int(state.get("pid") or 0)
        if state.get("status") == "running" and _pid_alive(prior_pid):
            result = dict(state)
            result["ok"] = False
            result["status"] = "already_running"
            result["running"] = True
            result["state_path"] = str(STATE_PATH)
            result["log_path"] = str(LOG_PATH)
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
            return result

        run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
        command = _build_command()
        env = os.environ.copy()
        env.pop("MODEL_DIR", None)
        env.setdefault("PYTHONPATH", str(REPO_ROOT))
        # Runtime bootstrap is intentionally disabled in backend-service mode.
        env.setdefault("MLB_CRON_RUNTIME_PIP_BOOTSTRAP", "0")
        env.update(overrides)

        with LOG_PATH.open("a", encoding="utf-8") as log_fh:
            log_fh.write(f"[{_now_iso()}] start run_id={run_id} triggered_by={triggered_by}\n")
            proc = subprocess.Popen(  # noqa: S603
                command,
                cwd=str(REPO_ROOT),
                env=env,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        state = {
            "ok": True,
            "status": "running",
            "running": True,
            "run_id": run_id,
            "pid": int(proc.pid),
            "started_at": _now_iso(),
            "finished_at": None,
            "exit_code": None,
            "triggered_by": triggered_by,
            "command": "bin/mlb_prod12_cron_cycle.sh",
            "env_overrides": overrides,
            "state_path": str(STATE_PATH),
            "log_path": str(LOG_PATH),
        }
        _write_json(STATE_PATH, state)
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    watcher = threading.Thread(target=_watch_process, args=(proc, run_id), daemon=True, name=f"prod12-{run_id}")
    with _WATCHERS_LOCK:
        _WATCHERS[run_id] = watcher
    watcher.start()

    return state


def get_prod12_cycle_status(*, tail_lines: int = 120) -> dict[str, Any]:
    _ensure_state_dir()
    state = _read_json(STATE_PATH)
    if not state:
        return {
            "ok": True,
            "status": "idle",
            "running": False,
            "state_path": str(STATE_PATH),
            "log_path": str(LOG_PATH),
            "log_tail": _tail_log(LOG_PATH, max(0, int(tail_lines))),
        }

    pid = int(state.get("pid") or 0)
    running = bool(state.get("status") == "running" and _pid_alive(pid))
    result = dict(state)
    result["ok"] = True
    result["running"] = running
    if state.get("status") == "running" and not running:
        result["status"] = "unknown_exit"
    result["state_path"] = str(STATE_PATH)
    result["log_path"] = str(LOG_PATH)
    result["log_tail"] = _tail_log(LOG_PATH, max(0, int(tail_lines)))
    return result
