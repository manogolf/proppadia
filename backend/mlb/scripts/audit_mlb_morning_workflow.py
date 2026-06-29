#!/usr/bin/env python3
"""Audit the MLB morning workflow as an end-to-end user path.

This is workflow visibility only. It does not change model, selector,
upload, threshold, or grading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


MLB_ROOT = Path("artifacts/analysis/mlb")
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")


@dataclass
class AuditRow:
    check_id: str
    parent_check_id: str
    severity: str
    category: str
    check: str
    status: str
    code: str
    summary: str
    source: str
    target: str
    detail: str
    suppressed: bool
    root_cause: str


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _extract_markdown_field(text: str, label: str) -> str:
    pattern = re.compile(rf"^- {re.escape(label)}:\s*`?([^`\n]+)`?", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1).strip() if match else ""


def _extract_markdown_section_items(text: str, heading: str) -> list[str]:
    items: list[str] = []
    in_section = False
    for line in text.splitlines():
        if line.startswith("### "):
            in_section = line.strip() == f"### {heading}"
            continue
        if in_section and line.startswith("- "):
            value = line[2:].strip()
            if value and value.lower() != "none.":
                items.append(value)
    return items


def _path_date(path: Path | str) -> str:
    matches = DATE_RE.findall(str(path))
    return matches[-1] if matches else ""


def _csv_dates(path: Path) -> set[str]:
    rows = _read_csv(path)
    dates: set[str] = set()
    for row in rows:
        for key in ("date", "board_date", "game_date", "canonical_date"):
            value = str(row.get(key) or "")[:10]
            if DATE_RE.fullmatch(value):
                dates.add(value)
                break
    return dates


def _write_csv(path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows = list(rows)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _row_count(path: Path) -> int | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    if path.suffix.lower() != ".csv":
        return None
    with path.open("r", newline="", encoding="utf-8") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def _resolve_link(source: Path, href: str) -> Path | None:
    href = href.strip()
    if not href or href.startswith("#") or "://" in href or href.startswith("mailto:"):
        return None
    href = href.split("#", 1)[0]
    if not href:
        return None
    target = Path(href)
    if not target.is_absolute():
        target = source.parent / target
    return target.resolve()


def _links(source: Path) -> list[tuple[str, Path]]:
    text = _read_text(source)
    out: list[tuple[str, Path]] = []
    for label, href in LINK_RE.findall(text):
        target = _resolve_link(source, href)
        if target is not None:
            out.append((label, target))
    return out


def _contains_link_to(source: Path, target: Path, label_contains: str | None = None) -> bool:
    wanted = target.resolve()
    for label, resolved in _links(source):
        if resolved != wanted:
            continue
        if label_contains and label_contains.lower() not in label.lower():
            continue
        return True
    return False


def _default_severity(status: str) -> str:
    if status == "FAIL":
        return "BLOCKER"
    if status == "WARN":
        return "MAJOR"
    return "INFO"


def _add(
    rows: list[AuditRow],
    category: str,
    check: str,
    status: str,
    source: Path | str = "",
    target: Path | str = "",
    detail: str = "",
    *,
    check_id: str = "",
    parent_check_id: str = "",
    severity: str = "",
    code: str = "",
    summary: str = "",
    suppressed: bool = False,
    root_cause: str = "",
) -> None:
    rows.append(
        AuditRow(
            check_id=check_id or f"{category}_{len(rows) + 1}",
            parent_check_id=parent_check_id,
            severity=severity or _default_severity(status),
            category=category,
            check=check,
            status=status,
            code=code or check.lower().replace(" ", "_"),
            summary=summary or check,
            source=str(source),
            target=str(target),
            detail=detail,
            suppressed=suppressed,
            root_cause=root_cause,
        )
    )


def _artifact_checks(rows: list[AuditRow], artifacts: dict[str, Path], date_text: str) -> None:
    for name, path in artifacts.items():
        exists = path.exists()
        non_empty = exists and path.stat().st_size > 0
        text = _read_text(path) if path.suffix.lower() in {".md", ".csv"} else ""
        date_ok = date_text in text or name in {"decision_performance", "morning_timing_log", "morning_timing_template"}
        if path.suffix.lower() == ".csv" and name.startswith("candidate_csv_"):
            rc = _row_count(path)
            non_empty = rc is not None and rc > 0
        status = "PASS" if exists and non_empty and date_ok else "FAIL"
        detail = "ok"
        if not exists:
            detail = "missing"
        elif not non_empty:
            detail = "empty"
        elif not date_ok:
            detail = f"expected date {date_text} not found"
        _add(rows, "artifact", f"{name} exists/fresh/non-empty", status, target=path, detail=detail)


def _date_consistency_checks(
    rows: list[AuditRow],
    *,
    date_text: str,
    home: Path,
    ops_brief: Path,
    workbench: Path,
    link_rows: list[dict[str, str]],
    out_root: Path,
) -> None:
    home_text = _read_text(home)
    ops_text = _read_text(ops_brief)
    workbench_text = _read_text(workbench)
    home_date = _extract_markdown_field(home_text, "Current Slate")
    ops_date = _extract_markdown_field(ops_text, "Current slate date")
    workbench_date = _extract_markdown_field(workbench_text, "Current slate date")
    workbench_generated_at = _extract_markdown_field(workbench_text, "Generated at")
    _add(
        rows,
        "date_consistency",
        "Home Screen current slate date matches audit date",
        "PASS" if home_date == date_text else "FAIL",
        home,
        "",
        f"home_date={home_date or 'missing'} expected={date_text}",
    )
    _add(
        rows,
        "date_consistency",
        "Ops Brief current slate date matches audit date",
        "PASS" if ops_date == date_text else "FAIL",
        ops_brief,
        "",
        f"ops_date={ops_date or 'missing'} expected={date_text}",
    )
    stale_workbench = not (workbench_date == home_date == date_text)
    if stale_workbench:
        _add(
            rows,
            "date_consistency",
            "Workbench current slate date matches Home Screen",
            "FAIL",
            workbench,
            home,
            f"workbench_date={workbench_date or 'missing'} home_date={home_date or 'missing'} expected={date_text}",
            check_id="WORKBENCH_STALE_DATE",
            severity="BLOCKER",
            code="WORKBENCH_STALE_DATE",
            summary="Workbench slate date does not match Home slate date.",
            root_cause="workbench_stale_date",
        )
    else:
        _add(
            rows,
            "date_consistency",
            "Workbench current slate date matches Home Screen",
            "PASS",
            workbench,
            home,
            f"workbench_date={workbench_date or 'missing'} home_date={home_date or 'missing'} expected={date_text}",
        )
    if stale_workbench and workbench_generated_at:
        generated_dates = DATE_RE.findall(workbench_generated_at)
        generated_date = generated_dates[0] if generated_dates else ""
        status = "FAIL" if generated_date == date_text else "PASS"
        _add(
            rows,
            "date_consistency",
            "Workbench generated_at fresh but slate date stale",
            status,
            workbench,
            "",
            f"workbench_generated_at={workbench_generated_at}; generated_date={generated_date or 'missing'}; workbench_date={workbench_date or 'missing'}; expected={date_text}",
        )
    for row in link_rows:
        launch_path = Path(row.get("launch_csv_path") or "")
        if not launch_path:
            continue
        label = row.get("today_workbench") or launch_path.name
        link_date = row.get("launch_csv_date") or _path_date(launch_path)
        filter_required = str(row.get("current_slate_filter_required") or "").lower() == "true"
        if filter_required:
            csv_dates = _csv_dates(launch_path)
            status = "PASS" if date_text in csv_dates else "FAIL"
            detail = f"{label}: global/current-slate-filtered CSV dates include expected={date_text}; dates_seen={','.join(sorted(csv_dates))[:240]}"
        else:
            status = "PASS" if link_date == date_text else "FAIL"
            detail = f"{label}: launch_csv_date={link_date or 'missing'} expected={date_text}"
        _add(rows, "date_consistency", "Workbench candidate CSV link uses current slate date", status, workbench, launch_path, detail)
        if launch_path.exists() and launch_path.suffix.lower() == ".csv":
            csv_dates = _csv_dates(launch_path)
            if filter_required:
                status = "PASS" if date_text in csv_dates else "FAIL"
                detail = f"{label}: expected date present in global CSV; dates_seen={','.join(sorted(csv_dates))[:240]}"
            else:
                status = "PASS" if csv_dates == {date_text} else "FAIL"
                detail = f"{label}: csv_dates={','.join(sorted(csv_dates)) or 'none'} expected_only={date_text}"
            _add(rows, "date_consistency", "Linked candidate CSV row dates match current slate", status, launch_path, "", detail)
        if link_date and link_date != date_text:
            expected_path = Path(str(launch_path).replace(link_date, date_text))
            if expected_path.exists():
                _add(
                    rows,
                    "date_consistency",
                    "Current-slate CSV exists but Workbench points elsewhere",
                    "FAIL",
                    workbench,
                    expected_path,
                    f"{label}: workbench points to {launch_path}; current-slate file exists",
                )


def _group_cascading_failures(rows: list[AuditRow]) -> list[AuditRow]:
    has_stale_workbench = any(row.check_id == "WORKBENCH_STALE_DATE" and row.status == "FAIL" for row in rows)
    if not has_stale_workbench:
        return rows
    grouped: list[AuditRow] = []
    for row in rows:
        if row.check_id == "WORKBENCH_STALE_DATE":
            grouped.append(row)
            continue
        suppress = False
        if (
            row.status == "FAIL"
            and row.category == "date_consistency"
            and row.check
            in {
                "Workbench candidate CSV link uses current slate date",
                "Linked candidate CSV row dates match current slate",
                "Current-slate CSV exists but Workbench points elsewhere",
                "Workbench generated_at fresh but slate date stale",
            }
        ):
            suppress = True
        if row.status == "FAIL" and row.category == "artifact" and row.check.startswith("candidate_csv_"):
            suppress = True
        if suppress:
            row.parent_check_id = "WORKBENCH_STALE_DATE"
            row.suppressed = True
            row.root_cause = "workbench_stale_date"
            row.severity = "INFO"
            row.summary = f"Supporting evidence for stale Workbench: {row.check}"
        grouped.append(row)
    return grouped


def _link_checks(rows: list[AuditRow], sources: list[Path]) -> None:
    for source in sources:
        if not source.exists():
            continue
        for label, target in _links(source):
            same_file = source.resolve() == target
            if same_file:
                _add(rows, "navigation", "no circular markdown link", "FAIL", source, target, f"label={label}")
            elif not target.exists():
                _add(rows, "navigation", "markdown link resolves", "FAIL", source, target, f"broken label={label}")
            else:
                _add(rows, "navigation", "markdown link resolves", "PASS", source, target, f"label={label}")


def _transition_checks(rows: list[AuditRow], *, home: Path, ops_brief: Path, workbench: Path, candidate_csvs: list[Path], timing_template: Path, decision_performance: Path) -> None:
    home_text = _read_text(home)
    workbench_text = _read_text(workbench)
    ops_text = _read_text(ops_brief)
    _add(
        rows,
        "requested_gap",
        "Home Screen has no Priority Preview",
        "PASS" if "Priority Preview" not in home_text else "FAIL",
        home,
        "",
        "Home top workflow must not duplicate Morning Workbench priorities",
    )
    _add(
        rows,
        "requested_gap",
        "Home Screen has Log Morning Timing link",
        "PASS" if _contains_link_to(home, timing_template, "Log Morning Timing") else "FAIL",
        home,
        timing_template,
        "visible timing link required",
    )
    _add(
        rows,
        "requested_gap",
        "Workbench has Record Today's Conclusions / Timing Notes link",
        "PASS" if _contains_link_to(workbench, timing_template, "Record Today's Conclusions / Timing Notes") else "FAIL",
        workbench,
        timing_template,
        "visible bottom timing/conclusion link required",
    )
    _add(
        rows,
        "requested_gap",
        "Real Ops Brief labels workflow state honestly",
        "PASS" if "full three-phase body rewrite remains prototype-only" in ops_text else "FAIL",
        ops_brief,
        "",
        "real generator has handoff, not full body resequence",
    )
    _add(
        rows,
        "transition",
        "Home Screen points to Ops Brief",
        "PASS" if _contains_link_to(home, ops_brief, "Start Morning Review") else "FAIL",
        home,
        ops_brief,
        "Start Morning Review should open Ops Brief",
    )
    _add(
        rows,
        "transition",
        "Home Screen does not start at Workbench",
        "PASS" if not _contains_link_to(home, workbench, "Start Morning Review") else "FAIL",
        home,
        workbench,
        "Home must not skip Ops Brief",
    )
    _add(
        rows,
        "transition",
        "Ops Brief points to Morning Workbench",
        "PASS" if _contains_link_to(ops_brief, workbench) else "FAIL",
        ops_brief,
        workbench,
        "Ops Brief should hand off to Workbench",
    )
    has_candidate_link = any(_contains_link_to(workbench, path) for path in candidate_csvs)
    _add(
        rows,
        "transition",
        "Workbench points to current Candidate CSV",
        "PASS" if has_candidate_link else "FAIL",
        workbench,
        ", ".join(str(p) for p in candidate_csvs),
        "at least one current candidate CSV link required",
    )
    _add(
        rows,
        "transition",
        "Workbench points to timing/conclusion notes",
        "PASS" if _contains_link_to(workbench, timing_template) else "FAIL",
        workbench,
        timing_template,
        "conclusion/timing handoff required",
    )
    _add(
        rows,
        "transition",
        "Decision Performance reachable from Home Screen",
        "PASS" if _contains_link_to(home, decision_performance) else "FAIL",
        home,
        decision_performance,
        "historical evidence link required",
    )


def _workflow_completeness(rows: list[AuditRow], *, gate_summary: Path, ops_brief: Path, workbench: Path, candidate_csvs: list[Path], timing_template: Path) -> None:
    gate_text = _read_text(gate_summary)
    ops_text = _read_text(ops_brief)
    workbench_text = _read_text(workbench)
    checks = [
        ("Can user begin?", "PASS" if "Safe to begin candidate review: `YES`" in gate_text else "WARN", gate_summary, "safe-to-begin value visible"),
        ("Can user verify trust?", "PASS" if "System Readiness" in ops_text or "Pipeline & Ops" in ops_text else "FAIL", ops_brief, "Ops Brief contains readiness/system section"),
        ("Can user review baseball context?", "PASS" if "Today's Baseball" in ops_text or "Hits Environment" in ops_text else "FAIL", ops_brief, "Ops Brief contains baseball context"),
        ("Can user reach candidate CSV?", "PASS" if any(path.exists() for path in candidate_csvs) and "Open Today's Candidate CSV" in workbench_text else "FAIL", workbench, "candidate CSV link visible"),
        ("Can user record conclusions?", "PASS" if "Today's Conclusions" in workbench_text else "FAIL", workbench, "conclusion section visible"),
        ("Can user record timing?", "PASS" if timing_template.exists() and _contains_link_to(workbench, timing_template) else "FAIL", timing_template, "timing template linked"),
    ]
    for check, status, artifact, detail in checks:
        _add(rows, "workflow_completeness", check, status, target=artifact, detail=detail)


def _graph_rows(date_text: str, artifacts: dict[str, Path], candidate_csvs: list[Path]) -> list[dict[str, str]]:
    graph = [
        ("Home Screen", "Ops Brief", "Start Morning Review", artifacts["home"], artifacts["ops_brief"]),
        ("Ops Brief", "Morning Workbench", "Begin Candidate Review", artifacts["ops_brief"], artifacts["morning_workbench"]),
        ("Morning Workbench", "Decision Performance", "historical evidence", artifacts["morning_workbench"], artifacts["decision_performance"]),
        ("Morning Workbench", "Morning Timing Template", "record notes/timing", artifacts["morning_workbench"], artifacts["morning_timing_template"]),
    ]
    for path in candidate_csvs:
        graph.append(("Morning Workbench", f"Candidate CSV {path.name}", "Open Today's Candidate CSV", artifacts["morning_workbench"], path))
    return [
        {
            "date": date_text,
            "from_node": left,
            "to_node": right,
            "handoff": label,
            "from_artifact": str(src),
            "to_artifact": str(dst),
            "target_exists": str(dst.exists()).lower(),
        }
        for left, right, label, src, dst in graph
    ]


def _counted_rows(rows: list[AuditRow]) -> list[AuditRow]:
    return [row for row in rows if not row.suppressed]


def _gate_warning_context(target: str) -> dict[str, object]:
    path = Path(target)
    if path.name != "morning_gate_summary.md":
        return {}
    text = _read_text(path)
    if not text:
        return {}
    return {
        "operational_gate": _extract_markdown_field(text, "Operational Gate"),
        "safe_to_begin_candidate_review": _extract_markdown_field(text, "Safe to begin candidate review"),
        "reason": _extract_markdown_field(text, "Reason"),
        "blockers": _extract_markdown_section_items(text, "BLOCKER"),
        "major": _extract_markdown_section_items(text, "MAJOR"),
    }


def _warning_reason(row: AuditRow, gate_context: dict[str, object]) -> str:
    gate = str(gate_context.get("operational_gate") or "")
    safe_to_begin = str(gate_context.get("safe_to_begin_candidate_review") or "")
    if gate or safe_to_begin:
        return (
            "Morning Gate says "
            f"Operational Gate={gate or 'unknown'} and "
            f"Safe to begin candidate review={safe_to_begin or 'unknown'}."
        )
    return row.detail or row.summary or row.check


def _warning_rows(rows: list[AuditRow]) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    for row in rows:
        if row.status != "WARN" or row.suppressed:
            continue
        gate_context = _gate_warning_context(row.target)
        reason = _warning_reason(row, gate_context)
        warnings.append(
            {
                "check_id": row.check_id,
                "severity": row.severity,
                "category": row.category,
                "check": row.check,
                "target": row.target,
                "detail": row.detail,
                "reason": reason,
                "gate_context": gate_context,
            }
        )
    return warnings


def _blocking_warnings_count(warnings: list[dict[str, object]]) -> int:
    count = 0
    for warning in warnings:
        gate_context = warning.get("gate_context") if isinstance(warning.get("gate_context"), dict) else {}
        gate = str(gate_context.get("operational_gate") or "").upper()
        safe_to_begin = str(gate_context.get("safe_to_begin_candidate_review") or "").upper()
        severity = str(warning.get("severity") or "").upper()
        if gate == "FAIL" or safe_to_begin == "NO" or severity in {"BLOCKER", "MAJOR"}:
            count += 1
    return count


def _write_md(path: Path, date_text: str, rows: list[AuditRow], graph: list[dict[str, str]], generated: str) -> None:
    counted = _counted_rows(rows)
    counts = {status: sum(1 for row in counted if row.status == status) for status in ("PASS", "WARN", "FAIL")}
    raw_failures = sum(1 for row in rows if row.status == "FAIL")
    suppressed_cascades = sum(1 for row in rows if row.status == "FAIL" and row.suppressed)
    total = len(rows) or 1
    score = round(((counts["PASS"] + 0.5 * counts["WARN"]) / total) * 100, 2)
    overall = "FAIL" if counts["FAIL"] else "WARN" if counts["WARN"] else "PASS"
    root_failures = [row for row in rows if row.status == "FAIL" and not row.suppressed]
    evidence_rows = [row for row in rows if row.suppressed]
    warnings = _warning_rows(rows)
    blocking_warnings = _blocking_warnings_count(warnings)
    lines = [
        f"# MLB Morning Workflow Audit - {date_text}",
        "",
        f"- Generated UTC: `{generated}`",
        f"- Workflow Health: `{score:.2f}%`",
        f"- Status: `{overall}`",
        f"- Navigation: `{'PASS' if not any(r.status == 'FAIL' and r.category == 'navigation' and not r.suppressed for r in rows) else 'FAIL'}`",
        f"- Broken links: `{sum(1 for r in rows if r.status == 'FAIL' and r.category == 'navigation' and not r.suppressed)}`",
        f"- Missing artifacts: `{sum(1 for r in rows if r.status == 'FAIL' and r.category == 'artifact' and not r.suppressed)}`",
        f"- Duplicate destinations: `{_duplicate_destination_count(graph)}`",
        f"- Workflow blockers: `{counts['FAIL']}`",
        f"- Root failures: `{len(root_failures)}`",
        f"- Suppressed cascading checks: `{suppressed_cascades}`",
        f"- Total raw failures: `{raw_failures}`",
        f"- Warnings: `{len(warnings)}`",
        f"- Blocking warnings: `{blocking_warnings}`",
        "- Scope: workflow audit only; no production behavior changed.",
        "",
        "## Warnings / Blocking Warnings",
        "",
        "| check id | severity | category | check | target | reason |",
        "|---|---|---|---|---|---|",
    ]
    if warnings:
        for warning in warnings:
            reason = str(warning.get("reason") or "").replace("|", "\\|")
            target = str(warning.get("target") or "")
            lines.append(
                f"| `{warning.get('check_id')}` | `{warning.get('severity')}` | "
                f"{warning.get('category')} | {warning.get('check')} | `{target}` | {reason} |"
            )
    else:
        lines.append("| `none` | `INFO` | none | No warnings. |  |  |")
    gate_warnings = [warning for warning in warnings if warning.get("gate_context")]
    if gate_warnings:
        lines.extend(["", "### Morning Gate Context", ""])
        for warning in gate_warnings:
            gate_context = warning.get("gate_context") if isinstance(warning.get("gate_context"), dict) else {}
            blockers = gate_context.get("blockers") if isinstance(gate_context.get("blockers"), list) else []
            major = gate_context.get("major") if isinstance(gate_context.get("major"), list) else []
            lines.append(f"- Check: `{warning.get('check_id')}`")
            lines.append(f"- Operational Gate: `{gate_context.get('operational_gate') or 'unknown'}`")
            lines.append(f"- Safe to begin candidate review: `{gate_context.get('safe_to_begin_candidate_review') or 'unknown'}`")
            if gate_context.get("reason"):
                lines.append(f"- Gate reason: {gate_context.get('reason')}")
            lines.append("- Blockers:")
            if blockers:
                for blocker in blockers:
                    lines.append(f"  - {blocker}")
            else:
                lines.append("  - None.")
            if major:
                lines.append("- Major warnings:")
                for item in major:
                    lines.append(f"  - {item}")
            lines.append("")
    lines.extend(
        [
        "## Root Failures",
        "",
        "| check id | severity | code | summary | detail |",
        "|---|---|---|---|---|",
        ]
    )
    if root_failures:
        for row in root_failures:
            detail = row.detail.replace("|", "\\|")
            lines.append(f"| `{row.check_id}` | `{row.severity}` | `{row.code}` | {row.summary} | {detail} |")
    else:
        lines.append("| `none` | `INFO` | `none` | No root failures. |  |")
    lines.extend(
        [
            "",
            "## Supporting Evidence",
            "",
            "| parent | check id | status | summary | detail |",
            "|---|---|---|---|---|",
        ]
    )
    if evidence_rows:
        for row in evidence_rows:
            detail = row.detail.replace("|", "\\|")
            lines.append(f"| `{row.parent_check_id}` | `{row.check_id}` | `{row.status}` | {row.summary} | {detail} |")
    else:
        lines.append("| `none` | `none` | `PASS` | No suppressed cascading checks. |  |")
    lines.extend(
        [
            "",
            "## Handoff Graph",
            "",
            "| from | to | handoff | target exists |",
            "|---|---|---|---|",
        ]
    )
    for edge in graph:
        lines.append(f"| {edge['from_node']} | {edge['to_node']} | {edge['handoff']} | `{edge['target_exists']}` |")
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| check id | parent | severity | category | check | status | suppressed | detail | source | target |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        detail = row.detail.replace("|", "\\|")
        lines.append(f"| `{row.check_id}` | `{row.parent_check_id}` | `{row.severity}` | {row.category} | {row.check} | `{row.status}` | `{str(row.suppressed).lower()}` | {detail} | `{row.source}` | `{row.target}` |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _duplicate_destination_count(graph: list[dict[str, str]]) -> int:
    counts: dict[str, int] = {}
    for edge in graph:
        target = edge["to_artifact"]
        counts[target] = counts.get(target, 0) + 1
    return sum(1 for count in counts.values() if count > 1)


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit MLB morning workflow handoffs.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-root", default=str(MLB_ROOT))
    args = ap.parse_args()
    date_text = str(args.date)[:10]
    out_root = Path(args.out_root)
    out_dir = out_root / "morning_workflow"
    home = out_root / "daily" / date_text / "INDEX.md"
    ops_brief = out_root / f"mlb_daily_ops_brief_{date_text}.md"
    if not ops_brief.exists():
        ops_brief = out_root / "mlb_daily_ops_brief_latest.md"
    artifacts = {
        "home": home,
        "ops_brief": ops_brief,
        "morning_workbench": out_root / "review_aids" / "performance" / "o15_morning_workbench.md",
        "decision_performance": out_root / "review_aids" / "performance" / "review_aid_decision_performance_report.md",
        "morning_timing_template": out_root / "morning_timing_template.md",
        "morning_timing_log": out_root / "morning_timing_log.csv",
        "morning_gate": out_root / "morning_gate_summary.md",
    }
    link_rows = _read_csv(out_root / "review_aids" / "performance" / "o15_morning_workbench_links.csv")
    candidate_csvs = [Path(row["launch_csv_path"]) for row in link_rows if row.get("launch_csv_path")]
    for idx, path in enumerate(candidate_csvs, start=1):
        artifacts[f"candidate_csv_{idx}"] = path

    rows: list[AuditRow] = []
    _artifact_checks(rows, artifacts, date_text)
    _date_consistency_checks(
        rows,
        date_text=date_text,
        home=home,
        ops_brief=ops_brief,
        workbench=artifacts["morning_workbench"],
        link_rows=link_rows,
        out_root=out_root,
    )
    _link_checks(rows, [home, ops_brief, artifacts["morning_workbench"]])
    _transition_checks(
        rows,
        home=home,
        ops_brief=ops_brief,
        workbench=artifacts["morning_workbench"],
        candidate_csvs=candidate_csvs,
        timing_template=artifacts["morning_timing_template"],
        decision_performance=artifacts["decision_performance"],
    )
    _workflow_completeness(
        rows,
        gate_summary=artifacts["morning_gate"],
        ops_brief=ops_brief,
        workbench=artifacts["morning_workbench"],
        candidate_csvs=candidate_csvs,
        timing_template=artifacts["morning_timing_template"],
    )
    rows = _group_cascading_failures(rows)
    graph = _graph_rows(date_text, artifacts, candidate_csvs)
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    counted = _counted_rows(rows)
    counts = {status: sum(1 for row in counted if row.status == status) for status in ("PASS", "WARN", "FAIL")}
    raw_failures = sum(1 for row in rows if row.status == "FAIL")
    suppressed_cascades = sum(1 for row in rows if row.status == "FAIL" and row.suppressed)
    total = len(rows) or 1
    score = round(((counts["PASS"] + 0.5 * counts["WARN"]) / total) * 100, 2)
    overall = "fail" if counts["FAIL"] else "warn" if counts["WARN"] else "pass"
    warnings = _warning_rows(rows)
    blocking_warnings = _blocking_warnings_count(warnings)
    md_path = out_dir / f"morning_workflow_audit_{date_text}.md"
    csv_path = out_dir / f"morning_workflow_audit_{date_text}.csv"
    graph_path = out_dir / f"workflow_navigation_graph_{date_text}.csv"
    latest_json = out_dir / "morning_workflow_audit_latest.json"
    summary_path = out_root / "morning_workflow_audit.md"
    summary_csv = out_root / "morning_workflow_audit.csv"
    graph_latest = out_root / "workflow_navigation_graph.csv"
    payload = {
        "generated_at_utc": generated,
        "date": date_text,
        "status": overall,
        "workflow_health_score": score,
        "pass_count": counts["PASS"],
        "warn_count": counts["WARN"],
        "warnings_count": len(warnings),
        "blocking_warnings_count": blocking_warnings,
        "warning_rows": warnings,
        "fail_count": counts["FAIL"],
        "root_failures": counts["FAIL"],
        "suppressed_cascades": suppressed_cascades,
        "total_raw_failures": raw_failures,
        "broken_links": sum(1 for row in rows if row.status == "FAIL" and row.category == "navigation"),
        "missing_artifacts": sum(1 for row in rows if row.status == "FAIL" and row.category == "artifact" and not row.suppressed),
        "duplicate_destinations": _duplicate_destination_count(graph),
        "workflow_blockers": counts["FAIL"],
        "md": str(md_path),
        "csv": str(csv_path),
        "graph_csv": str(graph_path),
    }
    _write_csv(csv_path, [asdict(row) for row in rows])
    _write_csv(summary_csv, [asdict(row) for row in rows])
    _write_csv(out_root / "morning_workflow_validation.csv", [asdict(row) for row in rows])
    _write_csv(graph_path, graph)
    _write_csv(graph_latest, graph)
    _write_md(md_path, date_text, rows, graph, generated)
    summary_path.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    (out_root / "morning_workflow_validation.md").write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_json.parent.mkdir(parents=True, exist_ok=True)
    latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"[mlb-morning-workflow-audit] date={date_text} status={overall} "
        f"score={score:.2f} fail={counts['FAIL']} warn={counts['WARN']} out_md={md_path}"
    )
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
