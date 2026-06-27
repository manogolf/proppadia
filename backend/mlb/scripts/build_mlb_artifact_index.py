#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


ET = ZoneInfo("America/New_York")
ROOTS = [
    Path("artifacts/analysis/mlb/review_aids"),
    Path("artifacts/analysis/mlb/review_aids/performance"),
    Path("artifacts/analysis/mlb/orchestration"),
    Path("artifacts/analysis/mlb/reconcile"),
    Path("artifacts/analysis/mlb/feature_lineage"),
    Path("artifacts/analysis/mlb/model_quality"),
    Path("backend/mlb/exports"),
    Path("backend/mlb/data/processed"),
]
DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
TS_RE = re.compile(r"(20\d{6}T\d{6}Z?)")


def _today_et() -> str:
    return datetime.now(ET).date().isoformat()


def _safe_read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _row_count(path: Path) -> int | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    if path.suffix.lower() == ".csv":
        try:
            with path.open(newline="", encoding="utf-8") as f:
                return max(sum(1 for _ in f) - 1, 0)
        except Exception:
            return None
    if path.suffix.lower() == ".json":
        data = _safe_read_json(path)
        for key in ("row_count", "rows", "board_rows", "candidate_rows", "rows_scored"):
            value = data.get(key)
            if isinstance(value, int):
                return value
    return None


def _mtime(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return ""


def _repo_path(path: Path) -> str:
    return path.as_posix()


def _relative_link(from_file: Path, target: Path, label: str | None = None) -> str:
    label = label or target.name
    rel_text = os.path.relpath(target, start=from_file.parent)
    return f"[{label}]({rel_text})"


def _link(path: Path, label: str | None = None) -> str:
    label = label or path.name
    return f"[{label}]({path.as_posix()})"


def _detect_date(path: Path) -> str:
    match = DATE_RE.search(path.as_posix())
    return match.group(1) if match else ""


def _pattern_key(path: Path) -> str:
    text = path.as_posix()
    text = DATE_RE.sub("<DATE>", text)
    text = TS_RE.sub("<TS>", text)
    return text


def _read_text_corpus() -> str:
    chunks: list[str] = []
    for path in [Path("Makefile")]:
        if path.exists():
            chunks.append(path.read_text(encoding="utf-8", errors="ignore"))
    for root in [Path("backend/mlb/scripts"), Path("backend/domains/mlb")]:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            try:
                chunks.append(path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                pass
    return "\n".join(chunks)


def _category(path: Path) -> tuple[str, bool, bool, str, str, str]:
    s = path.as_posix()
    name = path.name
    if "review_aids/performance" in s:
        return (
            "Performance / living metrics",
            False,
            False,
            "artifacts/analysis/mlb/review_aids/performance",
            "mlb-review-aid-performance",
            "review-aid performance tracker output",
        )
    if any(x in name for x in ["hits_o15_simple_filter_", "hits_o15_watch_candidates_", "hits_o15_layered_candidates_", "hits_u15_favorite_audit_", "hits_o15_alternate_discovery_"]):
        target = {
            "hits_o15_simple_filter_": "mlb-hits-o15-simple-filter",
            "hits_o15_watch_candidates_": "mlb-hits-o15-watch-candidates",
            "hits_o15_layered_candidates_": "mlb-hits-o15-layered-candidates",
            "hits_u15_favorite_audit_": "mlb-hits-u15-favorite-audit",
            "hits_o15_alternate_discovery_": "mlb-hits-o15-alternate-discovery-full",
        }
        generated = next((v for k, v in target.items() if k in name), "")
        return ("Daily Ops / open every day", True, False, "artifacts/analysis/mlb/daily/<DATE>/INDEX.md", generated, "routine current-slate review board")
    if any(x in name for x in ["mlb_daily_ops_brief", "mlb_daily_preflight"]) or "orchestration" in s:
        return ("Orchestration / runbooks", True, False, "artifacts/analysis/mlb/orchestration", "mlb-daily-ops-brief or mlb-daily-preflight", "daily runbook/preflight/brief support")
    if "feature_lineage" in s:
        return ("Feature lineage / repair history", False, False, "artifacts/analysis/mlb/feature_lineage", "", "lineage repair, health, or backfill evidence")
    if "model_quality" in s:
        return ("Performance / living metrics" if "shadow" in s or "evaluation" in s else "Research / evidence audits", False, "shadow" not in s, "artifacts/analysis/mlb/model_quality", "", "model-quality research or shadow evaluation")
    if "execution_vs_model" in s or "reconcile" in s:
        return ("Performance / living metrics", False, False, "artifacts/analysis/mlb/execution_vs_model/<DATE>", "mlb-daily-reconcile", "completed-slate reconcile/performance output")
    if "backend/mlb/exports/model_v2/lanes/today" in s:
        return ("Daily Ops / open every day", True, False, "backend/mlb/exports/model_v2/lanes/today/<DATE>", "mlb-daily-upload-prep", "lane selector, ranking input, Quick Card, or diagnostics")
    if "backend/mlb/exports" in s or "backend/mlb/data/processed" in s:
        return ("Daily Ops / open every day", True, False, path.parent.as_posix(), "", "pipeline export or processed daily artifact; keep path stable")
    if any(x in name for x in ["audit", "funnel", "cluster", "failure", "offensive_heat", "casey", "coverage", "decomposition", "conversion"]):
        return ("Research / evidence audits", False, True, "artifacts/analysis/mlb/research/<topic>", "", "research/evidence file; phase 2 candidate if unreferenced")
    return ("Archive / superseded", False, False, "TBD in phase 2", "", "unclassified; keep in place until reviewed")


def _scan_manifest(target_date: str, repo_refs: str) -> list[dict[str, Any]]:
    files: list[Path] = []
    for root in ROOTS:
        if not root.exists():
            continue
        files.extend(p for p in root.rglob("*") if p.is_file())
    by_pattern: dict[str, Path] = {}
    for path in files:
        key = _pattern_key(path)
        if key not in by_pattern or path.stat().st_mtime > by_pattern[key].stat().st_mtime:
            by_pattern[key] = path
    rows: list[dict[str, Any]] = []
    for path in sorted(files):
        rel = path.as_posix()
        category, daily_ops, research, home, generated_by, notes = _category(path)
        date_detected = _detect_date(path)
        latest = by_pattern.get(_pattern_key(path)) == path
        referenced = rel in repo_refs or path.name in repo_refs
        move_safe = bool(research and not daily_ops and not referenced and not rel.startswith("backend/"))
        if referenced:
            notes = f"{notes}; referenced by Makefile/backend scripts"
        rows.append(
            {
                "path": rel,
                "filename": path.name,
                "category": category,
                "date_detected": date_detected,
                "latest_for_date": "yes" if latest and (not date_detected or date_detected == target_date) else "no",
                "daily_ops": "yes" if daily_ops else "no",
                "research": "yes" if research else "no",
                "generated_by_target": generated_by,
                "recommended_home": home,
                "move_safe": "yes" if move_safe else "no",
                "notes": notes,
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _board_rows(date: str) -> list[dict[str, Any]]:
    specs = [
        ("O1.5 Simple Filter", Path(f"artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_{date}.csv"), True),
        ("O1.5 Watch Candidates", Path(f"artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_{date}.csv"), True),
        ("O1.5 Layered Candidates", Path(f"artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date}.csv"), True),
        ("U1.5 Favorite Audit", Path(f"artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{date}.csv"), True),
        ("O1.5 Alternate Discovery", Path(f"artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_{date}.csv"), False),
    ]
    out = []
    for name, path, required in specs:
        exists = path.exists()
        out.append(
            {
                "name": name,
                "path": path,
                "required": required,
                "status": "available" if exists else ("MISSING_INPUT" if required else "OPTIONAL_MISSING"),
                "rows": _row_count(path) if exists else None,
                "mtime": _mtime(path) if exists else "",
            }
        )
    return out


def _link_check_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        path = Path(item["path"])
        required = bool(item.get("required", True))
        exists = path.exists()
        rows.append(
            {
                "label": item.get("label", path.name),
                "repo_path": path.as_posix(),
                "required": "yes" if required else "no",
                "exists": "yes" if exists else "no",
                "status": "pass" if exists else ("broken_required" if required else "optional_missing"),
                "notes": item.get("notes", ""),
            }
        )
    return rows


def _add_link_item(items: list[dict[str, Any]], label: str, path: Path, required: bool = True, notes: str = "") -> None:
    items.append({"label": label, "path": path.as_posix(), "required": required, "notes": notes})


def _dashboard_link(index_path: Path, target: Path, label: str, required: bool, link_items: list[dict[str, Any]], notes: str = "") -> str:
    _add_link_item(link_items, label, target, required, notes)
    return _relative_link(index_path, target, label) if target.exists() else "`missing`"


def _load_research_threads(out_root: Path) -> tuple[list[dict[str, Any]], str]:
    path = out_root / "current_research_threads.json"
    if not path.exists():
        return [], f"Current research config missing: `{path.as_posix()}`"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], f"Current research config unreadable: `{path.as_posix()}` ({type(exc).__name__})"
    if not isinstance(data, list):
        return [], f"Current research config has unexpected shape: `{path.as_posix()}`"
    return [row for row in data if isinstance(row, dict)], ""


def _load_latest_research_snapshot(out_root: Path) -> tuple[dict[str, str], str]:
    path = out_root / "research_snapshots" / "snapshot_manifest.csv"
    if not path.exists():
        return {}, f"Latest research snapshot missing: `{path.as_posix()}`"
    try:
        with path.open(newline="", encoding="utf-8") as f:
            rows = [dict(row) for row in csv.DictReader(f)]
    except Exception as exc:
        return {}, f"Research snapshot manifest unreadable: `{path.as_posix()}` ({type(exc).__name__})"
    if not rows:
        return {}, f"Research snapshot manifest is empty: `{path.as_posix()}`"
    rows.sort(key=lambda row: (row.get("snapshot_date") or "", row.get("week") or ""))
    return rows[-1], ""


def _write_daily_index(date: str, completed_date: str, out_root: Path) -> None:
    daily_dir = out_root / "daily" / date
    daily_dir.mkdir(parents=True, exist_ok=True)
    index_path = daily_dir / "INDEX.md"
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    boards = _board_rows(date)
    ops_latest = out_root / "mlb_daily_ops_brief_latest.md"
    ops_dated = out_root / f"mlb_daily_ops_brief_{date}.md"
    preflight = out_root / "orchestration" / f"mlb_daily_preflight_{date}.md"
    preflight_json = out_root / "orchestration" / f"mlb_daily_preflight_{date}.json"
    expanded_health_md = out_root / "expanded_o15_universe" / f"expanded_o15_context_health_{date}.md"
    expanded_health_json = out_root / "expanded_o15_universe" / f"expanded_o15_context_health_{date}.json"
    identity_health_md = out_root / "identity" / "mlb_identity_health.md"
    identity_health_json = out_root / "identity" / "mlb_identity_health_summary.json"
    reconcile = out_root / "execution_vs_model" / completed_date / "reconcile_rows.csv"
    review_perf = out_root / "review_aids" / "performance" / "review_aid_performance_report.md"
    review_perf_json = out_root / "review_aids" / "performance" / "review_aid_performance_summary.json"
    upload_dir = Path("backend/mlb/data/processed/mlb_uploads") / date
    lane_dir = Path("backend/mlb/exports/model_v2/lanes/today") / date
    link_items: list[dict[str, Any]] = []
    warnings: list[str] = []
    research_threads, research_warning = _load_research_threads(out_root)
    latest_snapshot, snapshot_warning = _load_latest_research_snapshot(out_root)
    if research_warning:
        warnings.append(research_warning)
    if snapshot_warning:
        warnings.append(snapshot_warning)
    preflight_data = _safe_read_json(preflight_json)
    if preflight_data:
        status = preflight_data.get("status") or preflight_data.get("overall_status")
        if status and status != "pass":
            warnings.append(f"Preflight status is `{status}`.")
    expanded_health = _safe_read_json(expanded_health_json)
    if expanded_health:
        status = str(expanded_health.get("status") or "unknown")
        if status != "pass":
            failed = [
                str(item.get("field_group") or "")
                for item in (expanded_health.get("checks") or [])
                if str(item.get("status") or "") == "fail"
            ]
            warnings.append(f"Expanded O1.5 context health is `{status}`: {', '.join(failed[:8])}.")
    else:
        warnings.append(f"Expanded O1.5 context health missing: `{expanded_health_json.as_posix()}`.")
    identity_health = _safe_read_json(identity_health_json)
    if identity_health:
        status = str(identity_health.get("status") or "unknown")
        if status != "pass":
            warnings.append(
                f"MLB canonical identity health is `{status}`; warning artifacts: `{identity_health.get('warning_artifacts', '')}`."
            )
    else:
        warnings.append(f"MLB canonical identity health missing: `{identity_health_json.as_posix()}`.")
    for board in boards:
        if board["status"] != "available":
            if board["required"]:
                warnings.append(f"{board['name']}: `{board['status']}`")
            else:
                warnings.append(
                    f"{board['name']}: `{board['status']}`. Run `make mlb-hits-o15-alternate-discovery-full DATE={date}` to refresh it."
                )
    open_first = [
        ("Ops Brief", ops_dated if ops_dated.exists() else ops_latest, True),
        ("Preflight", preflight, True),
        ("Review Aid Performance", review_perf, False),
        ("Daily Feature Lineage Health", out_root / "feature_lineage" / f"daily_feature_lineage_health_{date}.md", False),
    ]
    lines = [
        f"# MLB Daily Index - {date}",
        "",
        f"- Current slate date: `{date}`",
        f"- Completed slate date: `{completed_date}`",
        f"- Generated (UTC): `{generated}`",
        "",
        "## Open First",
        "",
        "| item | status | open | repo path |",
        "|---|---|---|---|",
    ]
    for label, path, required in open_first:
        status = "available" if path.exists() else ("MISSING_INPUT" if required else "optional_missing")
        lines.append(
            f"| {label} | `{status}` | {_dashboard_link(index_path, path, label, required, link_items)} | `{_repo_path(path)}` |"
        )
    lines.extend(
        [
            "",
            "## Current Research / Active Questions",
            "",
            "### Latest Snapshot",
            "",
        ]
    )
    if latest_snapshot:
        snapshot_dir = Path(str(latest_snapshot.get("snapshot_path") or ""))
        snapshot_readme = snapshot_dir / "README.md" if snapshot_dir else Path("")
        snapshot_status = "available" if snapshot_readme.exists() else "MISSING_INPUT"
        snapshot_week = str(latest_snapshot.get("week") or "")
        snapshot_date = str(latest_snapshot.get("snapshot_date") or "")
        lines.extend(
            [
                "| week | date | status | open | repo path |",
                "|---|---|---|---|---|",
                f"| `{snapshot_week}` | `{snapshot_date}` | `{snapshot_status}` | {_dashboard_link(index_path, snapshot_readme, 'Latest Research Snapshot', False, link_items, notes='weekly research snapshot')} | `{_repo_path(snapshot_readme)}` |",
                "",
            ]
        )
    else:
        lines.extend(
            [
                f"Latest research snapshot unavailable. Generate one with `make mlb-research-snapshot DATE={date}`.",
                "",
            ]
        )
    lines.extend(["### Active Threads", ""])
    if research_threads:
        lines.extend(
            [
                "| research thread | status | current conclusion | next action | link | command |",
                "|---|---|---|---|---|---|",
            ]
        )
        for thread in research_threads:
            name = str(thread.get("name") or "Unnamed thread")
            status = str(thread.get("status") or "active")
            conclusion = str(thread.get("current_conclusion") or "")
            next_action = str(thread.get("next_action") or "")
            command = str(thread.get("command") or "")
            artifact = Path(str(thread.get("artifact") or ""))
            if str(artifact):
                link = _dashboard_link(index_path, artifact, name, False, link_items, notes="current research thread")
                repo_path = f"`{_repo_path(artifact)}`"
                link_cell = f"{link}<br>{repo_path}"
            else:
                link_cell = "`missing artifact config`"
            command_cell = f"`{command}`" if command else ""
            lines.append(f"| {name} | `{status}` | {conclusion} | {next_action} | {link_cell} | {command_cell} |")
    else:
        lines.append("Current research config missing.")
    lines.extend(
        [
            "",
            "Retired/historical audits are intentionally not listed here. Use the MLB artifact map and manifest for archive navigation.",
        ]
    )
    lines.extend(
        [
            "",
            "## Routine Review Boards",
            "",
            "| board | status | rows | mtime UTC | open | repo path |",
            "|---|---|---:|---|---|---|",
        ]
    )
    for board in boards:
        rows = "" if board["rows"] is None else str(board["rows"])
        path = board["path"]
        open_link = _dashboard_link(index_path, path, str(board["name"]), bool(board["required"]), link_items)
        lines.append(
            f"| {board['name']} | `{board['status']}` | {rows} | {board['mtime']} | {open_link} | `{_repo_path(path)}` |"
        )
    alt = next((b for b in boards if b["name"] == "O1.5 Alternate Discovery"), None)
    if alt and alt["status"] != "available":
        lines.extend(
            [
                "",
                "> Alternate discovery is optional/research. To generate it for this slate:",
                "",
                f"```bash\nmake mlb-hits-o15-alternate-discovery-full DATE={date}\n```",
            ]
        )
    lines.extend(
        [
            "",
            "## Production / Upload",
            "",
            "| item | status | rows | open | repo path |",
            "|---|---|---:|---|---|",
        ]
    )
    production_specs = [
        ("Lane Selector", lane_dir / f"hits_lane_selector_{date}.csv", True),
        ("Ranking Upload Input", lane_dir / f"hits_lane_selector_{date}_ranking_upload_input.csv", True),
        ("Quick Card", lane_dir / f"quick_card_hits_{date}.csv", True),
        ("Lane Selector Report", lane_dir / f"hits_lane_selector_{date}_daily_report.md", False),
        ("Book Upload Base", upload_dir / "05_book_upload_base.csv", False),
        ("Upload Manifest", upload_dir / "MANIFEST.md", False),
    ]
    for label, path, required in production_specs:
        exists = path.exists()
        status = "available" if exists else ("MISSING_INPUT" if required else "optional_missing")
        rows = _row_count(path) if exists else None
        lines.append(
            f"| {label} | `{status}` | {'' if rows is None else rows} | {_dashboard_link(index_path, path, label, required, link_items)} | `{_repo_path(path)}` |"
        )
    lines.extend(
        [
            "",
            "## Completed-Slate Performance",
            "",
            "| item | status | rows | open | repo path |",
            "|---|---|---:|---|---|",
        ]
    )
    performance_specs = [
        ("Reconcile Rows", reconcile, False),
        ("Review Aid Performance", review_perf, False),
        ("Review Aid Performance JSON", review_perf_json, False),
        ("Full Slate Summary", out_root / "execution_vs_model" / completed_date / "full_slate_summary.md", False),
        ("Total Bases Shadow Evaluation", out_root / "model_quality" / "total_bases_shadow" / "evaluation" / "total_bases_shadow_evaluation_summary.json", False),
    ]
    for label, path, required in performance_specs:
        exists = path.exists()
        status = "available" if exists else ("source-not-ready" if not required else "MISSING_INPUT")
        rows = _row_count(path) if exists else None
        lines.append(
            f"| {label} | `{status}` | {'' if rows is None else rows} | {_dashboard_link(index_path, path, label, required, link_items)} | `{_repo_path(path)}` |"
        )
    lines.extend(
        [
            "",
            "## Optional / Research",
            "",
            "| item | status | rows | open | repo path |",
            "|---|---|---:|---|---|",
        ]
    )
    optional_specs = [
        ("Expanded O1.5 Context Health", expanded_health_md, True),
        ("Expanded O1.5 Context Health JSON", expanded_health_json, True),
        ("MLB Identity Health", identity_health_md, True),
        ("MLB Identity Health JSON", identity_health_json, True),
        ("Expanded O1.5 Variable Importance", out_root / "expanded_o15_universe" / "expanded_o15_variable_importance.md", False),
        ("Expanded O1.5 Universe Rows", out_root / "expanded_o15_universe" / "expanded_o15_universe_rows.csv", False),
        ("Alternate Source Rows", out_root / "review_aids" / "oddsapi_batter_hits_alternate_live_discovery" / date / "live_alternate_book_level_rows.csv", False),
        ("Alternate Source Report", out_root / "review_aids" / "oddsapi_batter_hits_alternate_live_discovery" / date / "live_alternate_discovery_report.md", False),
        ("Alternate Slate Coverage Audit", out_root / "review_aids" / f"alternate_discovery_slate_coverage_{date}.md", False),
    ]
    for label, path, required in optional_specs:
        exists = path.exists()
        status = "available" if exists else "OPTIONAL_MISSING"
        rows = _row_count(path) if exists else None
        lines.append(
            f"| {label} | `{status}` | {'' if rows is None else rows} | {_dashboard_link(index_path, path, label, required, link_items)} | `{_repo_path(path)}` |"
        )
    link_check = _link_check_rows(link_items)
    broken_required = [r for r in link_check if r["status"] == "broken_required"]
    optional_missing = [r for r in link_check if r["status"] == "optional_missing"]
    for row in broken_required:
        warnings.append(f"Broken required link: {row['label']} -> `{row['repo_path']}`")
    lines.extend(
        [
            "",
            "## Warnings / Missing Inputs",
            "",
        ]
    )
    if warnings:
        lines.extend(f"- {w}" for w in warnings)
    else:
        lines.append("- None from index/preflight inputs.")
    if optional_missing:
        lines.append(f"- Optional/research missing links: `{len(optional_missing)}`; see `index_link_check.csv`.")
    lines.extend(
        [
            "",
            "## Navigation / Maps",
            "",
            "| item | open | repo path |",
            "|---|---|---|",
        ]
    )
    link_check_path = daily_dir / "index_link_check.csv"
    if not link_check_path.exists():
        link_check_path.write_text("label,repo_path,required,exists,status,notes\n", encoding="utf-8")
    nav_specs = [
        ("MLB Artifact Map", out_root / "README.md"),
        ("Review Aids Map", out_root / "review_aids" / "README.md"),
        ("Review-Aid Performance Map", out_root / "review_aids" / "performance" / "README.md"),
        ("Orchestration Map", out_root / "orchestration" / "README.md"),
        ("Feature Lineage Map", out_root / "feature_lineage" / "README.md"),
        ("Model Quality Map", out_root / "model_quality" / "README.md"),
        ("Artifact Manifest", out_root / "artifact_manifest.csv"),
        ("Phase 2 Move Candidates", out_root / "artifact_cleanup_phase2_candidates.csv"),
        ("Cleanup Plan", out_root / "artifact_cleanup_plan.md"),
        ("Index Link Check", link_check_path),
    ]
    for label, path in nav_specs:
        lines.append(f"| {label} | {_dashboard_link(index_path, path, label, False, link_items)} | `{_repo_path(path)}` |")
    # Recompute after navigation links are registered.
    link_check = _link_check_rows(link_items)
    _write_csv(daily_dir / "index_link_check.csv", link_check)
    broken_required = [r for r in link_check if r["status"] == "broken_required"]
    if broken_required:
        lines.append("")
        lines.append("## Link Check")
        lines.append("")
        lines.append(f"- Broken required links: `{len(broken_required)}`")
    (daily_dir / "INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


README_CONTENT = {
    "README.md": """# MLB Artifact Map

Open the current daily landing page first:

- `artifacts/analysis/mlb/daily/<DATE>/INDEX.md`

Current research focus is data-driven from:

- `artifacts/analysis/mlb/current_research_threads.json`

Main areas:

- `review_aids/`: daily hits review boards plus research audits.
- `review_aids/performance/`: outcome-backed board/layer/tier performance.
- `orchestration/`: daily ordering, preflight, runbooks, wrapper/status docs.
- `feature_lineage/`: lineage health, repair/backfill history, restoration evidence.
- `model_quality/`: offline experiments, canonical spines, shadow-model evaluations.
- `execution_vs_model/<DATE>/`: corrected completed-slate reconcile outputs.

First-pass cleanup intentionally keeps historical paths stable. Use `artifact_manifest.csv` and `artifact_cleanup_phase2_candidates.csv` before moving files.
""",
    "review_aids/README.md": """# MLB Review Aids

Daily-use boards:

- `hits_o15_simple_filter_<DATE>.csv/md`
- `hits_o15_watch_candidates_<DATE>.csv/md`
- `hits_o15_layered_candidates_<DATE>.csv/md`
- `hits_u15_favorite_audit_<DATE>.csv/md`
- `hits_o15_alternate_discovery_<DATE>.csv/md` when the live alternate source is available.

Research/evidence audits also live here for now. They are not moved in phase 1 because older scripts and reports may reference exact paths.

For the daily view, open `../daily/<DATE>/INDEX.md`.
""",
    "review_aids/performance/README.md": """# MLB Review-Aid Performance

Living outcome-backed metrics for the daily review boards.

Open:

- `review_aid_performance_report.md`
- `review_aid_performance_summary.json`
- `review_aid_performance_by_board.csv`
- `review_aid_performance_by_layer.csv`
- `review_aid_performance_by_tier.csv`

Generated by `make mlb-review-aid-performance` after completed-slate reconcile.
""",
    "orchestration/README.md": """# MLB Orchestration

Daily automation/runbook artifacts:

- `mlb_daily_final_runbook.md`
- `mlb_daily_target_inventory.csv`
- `mlb_daily_dependency_graph.csv`
- `mlb_morning_workflow_automation_correction.md`
- `mlb_daily_preflight_<DATE>.md/json`

Routine boards should be generated by the daily automation before Ops Brief. Manual board runs are emergency/research only.
""",
    "feature_lineage/README.md": """# MLB Feature Lineage

Lineage health, repair plans, restoration audits, and backfill evidence.

Daily-use:

- `daily_feature_lineage_health_latest.json`
- `daily_feature_lineage_health_<DATE>.md/json`

Historical Patch 1A/1B/1C/1D files are repair evidence and should stay in place unless a phase 2 migration updates references.
""",
    "model_quality/README.md": """# MLB Model Quality

Offline experiments, canonical modeling spines, BvP/rolling/market studies, and Total Bases shadow evaluation.

Daily/living items:

- `total_bases_shadow/<DATE>/`
- `total_bases_shadow/evaluation/`

Most other folders are research evidence and should not be treated as daily operating surfaces.
""",
}


def _write_readmes(out_root: Path) -> None:
    for rel, body in README_CONTENT.items():
        path = out_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")


def _write_cleanup_plan(out_root: Path, date: str) -> None:
    body = f"""# MLB Artifact Cleanup Plan

## Scope

This is pass 1 of a two-pass cleanup. It improves usability without moving existing production or historical artifact paths.

## Pass 1: Daily Surface And Maps

Completed by `make mlb-daily-index DATE={date}`:

1. Create `artifacts/analysis/mlb/daily/{date}/INDEX.md`.
2. Create/update library README files.
3. Create `artifact_manifest.csv`.
4. Create `artifact_cleanup_phase2_candidates.csv`.
5. Create `artifacts/analysis/mlb/daily/{date}/index_link_check.csv`.

The daily index uses links relative to `INDEX.md` so VS Code markdown preview can open them reliably. Validate those links with:

```bash
make mlb-daily-index-check DATE={date}
```

No production artifact paths are removed or renamed.

## Artifact Categories

- Daily Ops / open every day: Ops Brief, preflight, routine hits boards, upload prep, lane selector, Quick Card.
- Performance / living metrics: review-aid performance, reconcile summaries, Total Bases shadow evaluation.
- Research / evidence audits: Tier A failure, offensive heat, clustering, PA/opportunity, BvP/market studies.
- Feature lineage / repair history: Patch 1A-1D, backfills, rehydration, health checks.
- Orchestration / runbooks: dependency graph, target inventory, final runbook.
- Archive / superseded: older one-off files not currently classified as living surfaces.

## Pass 2 Candidate Policy

Only consider moving files where `artifact_cleanup_phase2_candidates.csv` has `move_safe=yes`.

Before moving any candidate:

1. Check Makefile and backend scripts for references.
2. Preserve a redirect/index entry from the old folder.
3. Move in batches by category, not one file at a time.
4. Run `make -n mlb-daily-review-and-upload`, `make mlb-daily-index DATE=<DATE>`, and any affected report targets.

## Recommended Phase 2 Folders

- `artifacts/analysis/mlb/research/review_aids/`
- `artifacts/analysis/mlb/research/feature_lineage/`
- `artifacts/analysis/mlb/archive/superseded/`

Keep daily and generated paths stable unless the producing script is patched in the same change.
"""
    (out_root / "artifact_cleanup_plan.md").write_text(body, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=_today_et())
    ap.add_argument("--completed-slate-date", default="")
    ap.add_argument("--out-root", default="artifacts/analysis/mlb")
    ap.add_argument("--check-only", action="store_true")
    args = ap.parse_args()
    date = str(args.date)
    completed = str(args.completed_slate_date or (datetime.fromisoformat(date).date() - timedelta(days=1)).isoformat())
    out_root = Path(args.out_root)
    if args.check_only:
        check_path = out_root / "daily" / date / "index_link_check.csv"
        if not check_path.exists():
            print(json.dumps({"status": "fail", "reason": "missing_index_link_check", "path": str(check_path)}, indent=2))
            return 1
        with check_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        broken = [r for r in rows if r.get("status") == "broken_required"]
        optional_missing = [r for r in rows if r.get("status") == "optional_missing"]
        status = "fail" if broken else "pass"
        print(
            json.dumps(
                {
                    "status": status,
                    "date": date,
                    "checked_links": len(rows),
                    "broken_required": len(broken),
                    "optional_missing": len(optional_missing),
                    "path": str(check_path),
                },
                indent=2,
            )
        )
        return 1 if broken else 0
    repo_refs = _read_text_corpus()
    rows = _scan_manifest(date, repo_refs)
    _write_csv(out_root / "artifact_manifest.csv", rows)
    phase2 = [r for r in rows if r.get("move_safe") == "yes"]
    _write_csv(out_root / "artifact_cleanup_phase2_candidates.csv", phase2)
    _write_daily_index(date, completed, out_root)
    _write_readmes(out_root)
    _write_cleanup_plan(out_root, date)
    print(
        json.dumps(
            {
                "date": date,
                "completed_slate_date": completed,
                "manifest_rows": len(rows),
                "phase2_candidates": len(phase2),
                "daily_index": str(out_root / "daily" / date / "INDEX.md"),
                "manifest": str(out_root / "artifact_manifest.csv"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
