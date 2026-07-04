#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


MLB_ROOT = Path("artifacts/analysis/mlb")
PERF_DIR = MLB_ROOT / "review_aids" / "performance"
SEVERITIES = ("BLOCKER", "MAJOR", "MINOR", "INFO")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _severity_for_check(check: dict[str, Any]) -> str:
    name = str(check.get("name") or "").lower()
    status = str(check.get("status") or "").lower()
    detail = str(check.get("detail") or "").lower()
    required = bool(check.get("required"))
    if status in {"pass", "ok"}:
        return "INFO"
    if status == "fail" or (required and not check.get("exists")):
        if any(token in name for token in ("slate", "identity", "invariants", "preflight")):
            return "BLOCKER"
        if any(token in name for token in ("watch", "layered", "alternate", "favorite", "candidate", "workbench")):
            return "BLOCKER"
        return "MAJOR" if required else "MINOR"
    if "zero_rows" in detail:
        if name in {"quick_card", "qc"}:
            return "MINOR"
        if any(token in name for token in ("candidate", "watch", "layered", "alternate")):
            return "MAJOR"
    if any(token in name for token in ("bvp", "shadow", "research", "snapshot")):
        return "MAJOR"
    return "MINOR"


def _gate_rows(date_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    preflight_path = MLB_ROOT / "orchestration" / f"mlb_daily_preflight_{date_text}.json"
    preflight = _read_json(preflight_path)
    if not preflight:
        rows.append(
            {
                "severity": "BLOCKER",
                "source": "preflight",
                "issue": "missing preflight JSON",
                "detail": "system integrity cannot be verified",
                "artifact": preflight_path.as_posix(),
                "action": "STOP",
            }
        )
    for check in preflight.get("checks") or []:
        status = str(check.get("status") or "").lower()
        if status in {"pass", "ok"}:
            continue
        severity = _severity_for_check(check)
        action = "STOP" if severity == "BLOCKER" else ("ACKNOWLEDGE" if severity == "MAJOR" else "NOTE")
        rows.append(
            {
                "severity": severity,
                "source": "preflight",
                "issue": str(check.get("name") or "unknown"),
                "detail": str(check.get("detail") or status or ""),
                "artifact": str(check.get("path") or preflight_path.as_posix()),
                "action": action,
            }
        )

    invariants_path = MLB_ROOT / "invariants" / f"mlb_project_invariants_{date_text}.json"
    invariants = _read_json(invariants_path)
    invariant_status = str(invariants.get("status") or "").lower()
    if not invariants:
        rows.append(
            {
                "severity": "BLOCKER",
                "source": "project_invariants",
                "issue": "missing project invariants",
                "detail": "doctrine invariants cannot be verified",
                "artifact": invariants_path.as_posix(),
                "action": "STOP",
            }
        )
    elif invariant_status not in {"pass", "ok"}:
        severity = "BLOCKER" if invariant_status == "fail" else "MAJOR"
        rows.append(
            {
                "severity": severity,
                "source": "project_invariants",
                "issue": "project invariants not clean",
                "detail": f"status={invariant_status}; fail={invariants.get('fail_count', '')}; warn={invariants.get('warn_count', '')}",
                "artifact": invariants_path.as_posix(),
                "action": "STOP" if severity == "BLOCKER" else "ACKNOWLEDGE",
            }
        )
    return rows


def _gate_decision(rows: list[dict[str, str]]) -> tuple[str, str, str]:
    severities = {row.get("severity") for row in rows}
    if "BLOCKER" in severities:
        return "FAIL", "NO", "One or more BLOCKER issues mean data cannot be trusted."
    if "MAJOR" in severities or "MINOR" in severities:
        return "WARN", "YES", "No BLOCKER issues; acknowledge warnings before candidate review."
    return "PASS", "YES", "No blocking or acknowledgement-required warnings."


def _rel_link(from_path: Path, to_path: Path, label: str) -> str:
    if not to_path.exists():
        return "`missing`"
    try:
        rel = Path(__import__("os").path.relpath(to_path, start=from_path.parent))
    except Exception:
        rel = to_path
    return f"[{label}]({rel.as_posix()})"


def _ops_brief_path(date_text: str) -> Path:
    dated = MLB_ROOT / f"mlb_daily_ops_brief_{date_text}.md"
    return dated if dated.exists() else MLB_ROOT / "mlb_daily_ops_brief_latest.md"


def _navigation_rows(date_text: str) -> list[dict[str, str]]:
    ops_brief = _ops_brief_path(date_text)
    gate = MLB_ROOT / "morning_gate_summary.md"
    workbench = PERF_DIR / "o15_morning_workbench.md"
    review_perf = PERF_DIR / "review_aid_decision_performance_report.md"
    expanded = MLB_ROOT / "expanded_o15_universe" / "expanded_o15_universe_rows.csv"
    links = PERF_DIR / "o15_morning_workbench_links.csv"
    timing = MLB_ROOT / "morning_timing_template.md"
    return [
        {
            "step": "1",
            "workflow_stage": "Ops Brief",
            "purpose": "System readiness and baseball context",
            "artifact": ops_brief.as_posix(),
        },
        {
            "step": "1a",
            "workflow_stage": "Morning Gate",
            "purpose": "Safe-to-begin decision",
            "artifact": gate.as_posix(),
        },
        {
            "step": "2",
            "workflow_stage": "Morning Workbench",
            "purpose": "Attention allocation",
            "artifact": workbench.as_posix(),
        },
        {
            "step": "3",
            "workflow_stage": "Today's Candidate CSVs",
            "purpose": "Open and pivot candidate rows",
            "artifact": links.as_posix(),
        },
        {
            "step": "4",
            "workflow_stage": "Pivot Exploration",
            "purpose": "Manual comparison by tier, price, and context",
            "artifact": expanded.as_posix(),
        },
        {
            "step": "5",
            "workflow_stage": "Decision Notes",
            "purpose": "Record today's operational conclusions",
            "artifact": timing.as_posix(),
        },
        {
            "step": "6",
            "workflow_stage": "Production Uploads",
            "purpose": "Future handoff after decisions",
            "artifact": "future",
        },
        {
            "step": "reference",
            "workflow_stage": "Review Aid Decision Performance",
            "purpose": "Historical evidence, not morning pivot surface",
            "artifact": review_perf.as_posix(),
        },
    ]


def _severity_table(lines: list[str], gate_rows: list[dict[str, str]], severity: str) -> None:
    rows = [row for row in gate_rows if row.get("severity") == severity]
    lines.append(f"### {severity}")
    if not rows:
        lines.append("- None.")
        lines.append("")
        return
    for row in rows:
        lines.append(f"- {row.get('issue')}: {row.get('detail')} (`{row.get('action')}`)")
    lines.append("")


def _write_ops_brief_prototype(path: Path, date_text: str, status: str, safe_to_begin: str, gate_rows: list[dict[str, str]]) -> None:
    workbench = PERF_DIR / "o15_morning_workbench.md"
    warning_counts = {severity: len([row for row in gate_rows if row.get("severity") == severity]) for severity in SEVERITIES}
    lines = [
        "# Ops Brief Morning Workflow Prototype",
        "",
        "Prototype top section for the real Ops Brief. This file does not replace the Ops Brief.",
        "",
        "## Phase 1: System Readiness",
        "",
        "Purpose: can I trust today's platform?",
        "",
        f"- System Status: `{status}`",
        f"- Operational Gate: `{status}`",
        f"- Safe to Continue? `{safe_to_begin}`",
        f"- Critical warnings: `{warning_counts.get('BLOCKER', 0)}` blocker, `{warning_counts.get('MAJOR', 0)}` major, `{warning_counts.get('MINOR', 0)}` minor",
        "",
        "Read this phase before doing any candidate review. BLOCKER means stop; WARN means acknowledge and continue only if understood.",
        "",
        "System readiness checks represented here:",
        "",
        "- Morning Gate",
        "- Pipeline",
        "- Source Health",
        "- Identity",
        "- Freshness",
        "- Feature Lineage",
        "- Invariants",
        "- Critical warnings",
        "- Postgrade operational alerts",
        "",
    ]
    for severity in ("BLOCKER", "MAJOR", "MINOR"):
        _severity_table(lines, gate_rows, severity)
    lines.extend(
        [
            "## Phase 2: Today's Baseball",
            "",
            "Purpose: what kind of baseball day is today?",
            "",
            "Keep reading the real Ops Brief here. This phase contains the baseball context that should calibrate the morning before candidates are reviewed:",
            "",
            "- Snapshot baseball items",
            "- Hits Environment",
            "- Expected matchups",
            "- Highest and lowest expected hits allowed",
            "- Expected team hits",
            "- Forecast unavailable starters",
            "- Yesterday's biggest misses",
            "- Model vs Fade",
            "- Prop outlook",
            "- Ranking / QC overlap",
            "- Bottom-order watch",
            "- User O1.5 proxy watch retired from Ops Brief",
            "- Review board summaries",
            "- Total Bases shadow",
            "- Tier audit",
            "- Other baseball decision context already present in the Ops Brief",
            "",
            "## Phase 3: Begin Candidate Review",
            "",
            "Purpose: transition from observation to decision.",
            "",
            f"- Platform Verified: `{'yes' if status != 'FAIL' else 'no'}`",
            "- Baseball Context Reviewed: `manual acknowledgement`",
            f"- Ready to Begin Candidate Review? `{safe_to_begin}`",
        ]
    )
    if status == "FAIL":
        lines.append("- STOP. Return to Phase 1 before candidate review.")
    else:
        lines.append(f"- {_rel_link(path, workbench, 'Open Morning Workbench →')}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_gate_summary(path: Path, date_text: str, status: str, safe_to_begin: str, reason: str, gate_rows: list[dict[str, str]]) -> None:
    lines = [
        "# Morning Gate Summary",
        "",
        f"- Date: `{date_text}`",
        f"- Operational Gate: `{status}`",
        f"- Safe to begin candidate review: `{safe_to_begin}`",
        f"- Reason: {reason}",
        "",
    ]
    for severity in SEVERITIES:
        _severity_table(lines, gate_rows, severity)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_mos(path: Path, date_text: str, status: str, safe_to_begin: str, reason: str, gate_rows: list[dict[str, str]], nav_rows: list[dict[str, str]]) -> None:
    ops_brief = _ops_brief_path(date_text)
    workbench = PERF_DIR / "o15_morning_workbench.md"
    gate = MLB_ROOT / "morning_gate_summary.md"
    lines = [
        "# MLB Morning Operating System",
        "",
        f"- Date: `{date_text}`",
        f"- Generated at: `{datetime.utcnow().replace(microsecond=0).isoformat()}Z`",
        "- Prototype navigation only; no existing report replaced.",
        "",
        "## Operating Flow",
        "",
        "Automation -> Home Screen -> Ops Brief -> Morning Workbench -> Today's Candidate CSV -> Pivot -> Decision -> Production Upload",
        "",
        "## Start Here",
        "",
        f"1. {_rel_link(path, ops_brief, 'Open Ops Brief')} - system readiness and baseball context.",
        f"2. {_rel_link(path, workbench, 'Open Morning Workbench')} - attention allocation after Ops Brief review.",
        "",
        f"- Operational Gate: `{status}`",
        f"- Safe to begin candidate review: `{safe_to_begin}`",
        f"- Reason: {reason}",
        f"- {_rel_link(path, gate, 'Open Morning Gate Summary')}",
    ]
    if status == "WARN":
        lines.append("- Warnings requiring acknowledgement:")
        for row in gate_rows:
            if row.get("severity") in {"MAJOR", "MINOR"}:
                lines.append(f"  - `{row.get('severity')}` {row.get('issue')}: {row.get('detail')}")
        lines.append(f"- Proceed to Morning Workbench: {_rel_link(path, workbench, 'Open Morning Workbench →')}")
    elif status == "FAIL":
        lines.append("- STOP. Return to Ops Brief before candidate review.")
    lines.extend(
        [
            "",
            "## Navigation Map",
            "",
            "| step | workflow stage | purpose | artifact |",
            "|---|---|---|---|",
        ]
    )
    for row in nav_rows:
        artifact = Path(row["artifact"])
        link = _rel_link(path, artifact, "open") if row["artifact"] != "future" else "`future`"
        lines.append(f"| {row['step']} | {row['workflow_stage']} | {row['purpose']} | {link} |")
    lines.extend(
        [
            "",
            "## Doctrine",
            "",
            "- The Home Screen starts the morning.",
            "- Ops Brief answers: can I trust today's platform, and what kind of baseball day is today?",
            "- Morning Workbench answers: where should I spend my attention today?",
            "- The Candidate CSV is the workbench.",
            "- The Pivot is the exploration tool.",
            "- History informs attention.",
            "- History never authorizes today's decision.",
            "- Timing notes are manual observation, not enforcement.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build MLB morning operating system prototype.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-root", default=str(MLB_ROOT))
    args = ap.parse_args()
    date_text = str(args.date)[:10]
    out_root = Path(args.out_root)
    gate_rows = _gate_rows(date_text)
    status, safe_to_begin, reason = _gate_decision(gate_rows)
    nav_rows = _navigation_rows(date_text)
    mos = out_root / "morning_operating_system.md"
    ops = out_root / "mlb_daily_ops_brief_morning_workflow_prototype.md"
    nav = out_root / "morning_navigation_map.csv"
    gate = out_root / "morning_gate_summary.md"
    gate_csv = out_root / "morning_gate_warnings.csv"
    _write_gate_summary(gate, date_text, status, safe_to_begin, reason, gate_rows)
    _write_mos(mos, date_text, status, safe_to_begin, reason, gate_rows, nav_rows)
    _write_ops_brief_prototype(ops, date_text, status, safe_to_begin, gate_rows)
    _write_csv(nav, nav_rows)
    _write_csv(gate_csv, gate_rows)
    print(f"system_status={status}")
    print(f"safe_to_begin_candidate_review={safe_to_begin}")
    print(f"morning_gate_summary={gate.as_posix()}")
    print(f"morning_operating_system={mos.as_posix()}")
    print(f"ops_brief_prototype={ops.as_posix()}")
    print(f"navigation_map={nav.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
