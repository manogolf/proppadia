#!/usr/bin/env python3
"""Check current-slate MLB daily artifacts before rendering the Ops Brief."""

from __future__ import annotations

import argparse
import csv
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


def _read_csv_summary(path: Path) -> tuple[int, list[str], str]:
    if not path.exists():
        return 0, [], ""
    row_count = 0
    dates: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                row_count += 1
                for col in DATE_COLUMNS:
                    raw = str(row.get(col) or "").strip()
                    if len(raw) >= 10:
                        dates.add(raw[:10])
        return row_count, sorted(dates), ""
    except Exception as exc:
        return 0, [], f"read_error:{type(exc).__name__}"


def _mtime_utc(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _check_artifact(name: str, path: Path, *, expected_date: str, required: bool) -> ArtifactCheck:
    row_count, date_values, err = _read_csv_summary(path)
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
            detail="missing_required_input" if required else "missing_optional_input",
            mtime_utc="",
        )
    if err:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=0,
            date_values=[],
            status="fail" if required else "warn",
            detail=err,
            mtime_utc=mtime,
        )
    if date_values and any(value != expected_date for value in date_values):
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=row_count,
            date_values=date_values,
            status="fail" if required else "warn",
            detail=f"date_mismatch:expected={expected_date}",
            mtime_utc=mtime,
        )
    if row_count == 0:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=row_count,
            date_values=date_values,
            status="warn",
            detail="zero_rows",
            mtime_utc=mtime,
        )
    if not date_values:
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=row_count,
            date_values=date_values,
            status="warn",
            detail="no_date_column_detected",
            mtime_utc=mtime,
        )
    return ArtifactCheck(
        name=name,
        path=str(path),
        required=required,
        exists=True,
        row_count=row_count,
        date_values=date_values,
        status="pass",
        detail="ok",
        mtime_utc=mtime,
    )


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
            str(item.get("field_group") or item.get("name") or "")
            for item in (data.get("checks") or [])
            if str(item.get("status") or "").lower() == "fail"
        ]
        return ArtifactCheck(
            name=name,
            path=str(path),
            required=required,
            exists=True,
            row_count=len(data.get("checks") or []),
            date_values=[date_value] if date_value else [],
            status="fail" if required else "warn",
            detail="health_failed:" + ",".join(failed[:8]),
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
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/orchestration/mlb_daily_preflight_latest.json")
    ap.add_argument("--out-md", default="artifacts/analysis/mlb/orchestration/mlb_daily_preflight_latest.md")
    args = ap.parse_args(argv)

    date_value = str(args.date).strip()
    specs = [
        ("slate_output", Path(args.slate_output_csv), True),
        ("lane_selector", Path(args.lane_selector_csv), True),
        ("quick_card", Path(args.quick_card_csv), True),
        ("hits_o15_simple_filter", Path(args.hits_o15_simple_csv), True),
        ("hits_o15_watch_candidates", Path(args.hits_o15_watch_csv), True),
        ("hits_o15_layered_candidates", Path(args.hits_o15_layered_csv), True),
        ("hits_u15_favorite_audit", Path(args.hits_u15_favorite_csv), True),
        ("hits_o15_alternate_discovery", Path(args.hits_o15_alternate_csv), False),
    ]
    checks = [
        _check_artifact(name, path, expected_date=date_value, required=required)
        for name, path, required in specs
    ]
    checks.append(
        _check_json_health(
            "expanded_o15_context_health",
            Path(args.expanded_o15_context_health_json),
            expected_date=date_value,
            required=True,
        )
    )
    board_checks = [check for check in checks if check.name.startswith("hits_")]
    if all(check.row_count == 0 for check in board_checks) and any(check.status != "pass" for check in board_checks):
        checks.append(
            ArtifactCheck(
                name="board_zero_sanity",
                path="",
                required=True,
                exists=True,
                row_count=0,
                date_values=[],
                status="fail",
                detail="all_board_sections_zero_with_missing_or_unreadable_inputs",
                mtime_utc="",
            )
        )

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
