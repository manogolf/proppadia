"""Weekly NHL model evaluation schedule: is a report due, and build it if allowed.

The daily run calls `check_and_run` after prior-day learning. It never blocks
the daily run: a failed build is reported, not raised. While the schedule is
disabled it only reports status, so the operator still sees an overdue report.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "backend/nhl/config/nhl_model_evaluation_schedule_v1.json"
WEEKDAYS = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY")


def load_config(path: Path = CONFIG) -> dict[str, Any]:
    config = json.loads(Path(path).read_text())
    if config.get("schema_version") != "NHL_MODEL_EVALUATION_SCHEDULE_V1":
        raise ValueError("NHL_MODEL_EVALUATION_SCHEDULE_SCHEMA_INVALID")
    if config["weekly"]["run_on_et_weekday"] not in WEEKDAYS:
        raise ValueError("NHL_MODEL_EVALUATION_WEEKDAY_INVALID")
    return config


def latest_report(config: dict[str, Any], root: Path = ROOT) -> str | None:
    dates = []
    for path in root.glob(config["weekly"]["report_glob"]):
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.md$", path.name)
        if m:
            dates.append(m.group(1))
    return max(dates) if dates else None


def status(today_et: str, config: dict[str, Any], root: Path = ROOT) -> dict[str, Any]:
    """Report the weekly evaluation state for the ET day of the daily run.

    A week's report covers slates through the Saturday before the run day, so
    on the run weekday the report for yesterday is due; it is overdue once a
    run weekday has passed without one.
    """
    today = date.fromisoformat(today_et)
    run_day = WEEKDAYS.index(config["weekly"]["run_on_et_weekday"])
    last_run_day = today - timedelta(days=(today.weekday() - run_day) % 7)
    expected = (last_run_day - timedelta(days=1)).isoformat()
    latest = latest_report(config, root)
    current = latest is not None and latest >= expected
    next_run = last_run_day + timedelta(days=7) if current else last_run_day
    return {"enabled": bool(config.get("enabled")), "latest_report_as_of": latest,
            "expected_report_as_of": expected, "current": current,
            "due_today": (not current) and today == last_run_day,
            "overdue": (not current) and today > last_run_day,
            "next_run_et": next_run.isoformat()}


def check_and_run(today_et: str, *, config_path: Path = CONFIG, root: Path = ROOT,
                  runner=subprocess.run) -> dict[str, Any]:
    """Build the weekly report when enabled and due; never raise into the daily run."""
    try:
        config = load_config(config_path)
        state = status(today_et, config, root)
    except Exception as error:  # schedule problems must not stop the daily run
        return {"status": "SCHEDULE_ERROR", "error": f"{type(error).__name__}:{error}"}
    if state["current"]:
        return {**state, "status": "CURRENT"}
    if not state["enabled"]:
        return {**state, "status": "OVERDUE_SCHEDULE_DISABLED" if state["overdue"] or state["due_today"] else "NOT_DUE"}
    env = {**os.environ, "TMPDIR": str(root / "tmp")}
    builder = root / config["weekly"]["builder"]
    result = runner([sys.executable, str(builder), "--as-of", state["expected_report_as_of"],
                     "--days", str(config["weekly"]["covers_days"])],
                    cwd=root, env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "").strip().splitlines()[-1:] or [""]
        return {**state, "status": "BUILD_FAILED_NONBLOCKING", "error": tail[0][:300]}
    return {**status(today_et, config, root), "status": "BUILT",
            "report": f"docs/nhl/weekly_model_evaluation_{state['expected_report_as_of']}.md"}


def console_line(result: dict[str, Any]) -> str:
    """One line for the daily console and receipt."""
    s = result.get("status")
    if s == "CURRENT":
        return f"Weekly model evaluation: current (as of {result['latest_report_as_of']}); next {result['next_run_et']}."
    if s == "BUILT":
        return f"Weekly model evaluation: built {result['report']}."
    if s == "NOT_DUE":
        return f"Weekly model evaluation: next due {result['next_run_et']} (latest {result['latest_report_as_of']})."
    if s == "OVERDUE_SCHEDULE_DISABLED":
        return (f"Weekly model evaluation: DUE for week ending {result['expected_report_as_of']} "
                "(automatic build is switched off; ask Claude to run it).")
    return f"Weekly model evaluation: {s} {result.get('error', '')}".strip()
