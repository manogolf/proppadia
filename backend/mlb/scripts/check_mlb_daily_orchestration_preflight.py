#!/usr/bin/env python3
"""Check current-slate MLB daily artifacts before rendering the Ops Brief."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


DATE_COLUMNS = ("date", "game_date", "slate_date", "report_date")


@dataclass
class ArtifactCheck:
    name: str
    path: str
    required: bool
    exists: bool
    row_count: int
    date_values: list[str]
    status: str
    detail: str
    mtime_utc: str


def _mtime_utc(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _check_json_health(name: str, path: Path, *, expected_date: str, required: bool) -> ArtifactCheck:
    exists = path.exists()
    mtime = _mtime_utc(path)
    if not exists:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=False,
            row_count=0,
            date_values=[],
            status="fail" if required else "warn",
            detail="missing_required_health_json" if required else "missing_optional_health_json",
            mtime_utc="",
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=0,
            date_values=[],
            status="fail" if required else "warn",
            detail=f"json_read_error:{type(exc).__name__}",
            mtime_utc=mtime,
        )
    date_value = str(data.get("date") or "")[:10]
    status = str(data.get("status") or "").lower()
    if date_value and date_value != expected_date:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=len(data.get("checks") or []),
            date_values=[date_value],
            status="fail" if required else "warn",
            detail=f"date_mismatch:expected={expected_date}",
            mtime_utc=mtime,
        )
    if status not in {"pass", "ok"}:
        failed = [
            str(item.get("field_group") or item.get("name") or item.get("invariant") or "")
            for item in (data.get("checks") or [])
            if str(item.get("status") or "").lower() == "fail"
        ]
        check_status = "warn" if status == "warn" else ("fail" if required else "warn")
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=len(data.get("checks") or []),
            date_values=[date_value] if date_value else [],
            status=check_status,
            detail=f"health_status:{status or 'unknown'}:" + ",".join(failed[:8]),
            mtime_utc=mtime,
        )
    return ArtifactCheck(
        name=name,
        path=str(path),
        required=required,
        exists=True,
        row_count=len(data.get("checks") or []),
        date_values=[date_value] if date_value else [],
        status="pass",
        detail="ok",
        mtime_utc=mtime,
    )


def _write_markdown(path: Path, *, date_value: str, overall_status: str, checks: Iterable[ArtifactCheck]) -> None:
    lines = [
        f"# MLB Daily Preflight - {date_value}",
        "",
        f"- Status: `{overall_status}`",
        "",
        "| artifact | required | status | rows | dates | mtime UTC | detail | path |",
        "|---|---:|---|---:|---|---|---|---|",
    ]
    for check in checks:
        dates = ", ".join(check.date_values) if check.date_values else ""
        lines.append(
            f"| {check.name} | `{check.required}` | `{check.status}` | `{check.row_count}` | "
            f"`{dates}` | `{check.mtime_utc}` | `{check.detail}` | `{check.path}` |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Check current-slate MLB daily artifacts before Ops Brief rendering.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--slate-output-csv", required=True)
    ap.add_argument("--lane-selector-csv", required=True)
    ap.add_argument("--quick-card-csv", required=True)
    ap.add_argument("--hits-o15-simple-csv", required=True)
    ap.add_argument("--hits-o15-watch-csv", required=True)
    ap.add_argument("--hits-o15-layered-csv", required=True)
    ap.add_argument("--hits-u15-favorite-csv", required=True)
    ap.add_argument("--hits-o15-alternate-csv", required=True)
    ap.add_argument("--expanded-o15-context-health-json", required=True)
    ap.add_argument("--project-invariants-json", required=True)
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/orchestration/mlb_daily_preflight_latest.json")
    ap.add_argument("--out-md", default="artifacts/analysis/mlb/orchestration/mlb_daily_preflight_latest.md")
    args = ap.parse_args(argv)

    date_value = str(args.date).strip()
    # Slate output, lane selector, Quick Card, and the Hits O1.5/U1.5 boards
    # are retired while no MLB prop model qualifies; their arguments remain
    # accepted for existing callers but are no longer checked.
    checks = [
        _check_json_health(
            "mlb_project_invariants",
            Path(args.project_invariants_json),
            expected_date=date_value,
            required=True,
        )
    ]

    fail_count = sum(1 for check in checks if check.status == "fail")
    warn_count = sum(1 for check in checks if check.status == "warn")
    overall_status = "fail" if fail_count else "warn" if warn_count else "pass"

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "date": date_value,
        "status": overall_status,
        "ok": overall_status == "pass",
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": [asdict(check) for check in checks],
    }
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(Path(args.out_md), date_value=date_value, overall_status=overall_status, checks=checks)

    print(
        f"[mlb-daily-preflight] status={overall_status} fail={fail_count} warn={warn_count} "
        f"out_json={out_json}"
    )
    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
