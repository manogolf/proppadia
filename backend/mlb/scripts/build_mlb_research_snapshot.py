#!/usr/bin/env python3
"""Build immutable weekly MLB research snapshots."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_OUT_ROOT = Path("artifacts/analysis/mlb")
DEFAULT_SNAPSHOT_ROOT = DEFAULT_OUT_ROOT / "research_snapshots"
EXPANDED_DIR = DEFAULT_OUT_ROOT / "expanded_o15_universe"


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _fmt_pct(value: Any) -> str:
    try:
        if value in ("", None):
            return "n/a"
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "n/a"


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        if value in ("", None):
            return "n/a"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _safe_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        return sum(1 for _ in reader)


def _first_row(rows: Iterable[Dict[str, str]], **criteria: str) -> Dict[str, str]:
    for row in rows:
        if all(str(row.get(k) or "") == str(v) for k, v in criteria.items()):
            return row
    return {}


def _top_rows(path: Path, limit: int = 8) -> List[Dict[str, str]]:
    return _read_csv_rows(path)[:limit]


def _artifact_link(snapshot_dir: Path, path: Path, label: str) -> str:
    if not path.exists():
        return f"`missing: {path.as_posix()}`"
    rel = Path("../" * len(snapshot_dir.relative_to(DEFAULT_OUT_ROOT).parts)) / path.relative_to(DEFAULT_OUT_ROOT)
    return f"[{label}]({rel.as_posix()})"


def _snapshot_paths(snapshot_root: Path, snapshot_date: date) -> tuple[int, Path]:
    iso = snapshot_date.isocalendar()
    week = int(iso.week)
    return week, snapshot_root / f"{iso.year}" / f"week_{week:02d}_{snapshot_date.isoformat()}"


def _load_research_threads(out_root: Path) -> List[Dict[str, Any]]:
    data = _safe_json(out_root / "current_research_threads.json")
    return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []


def _summarize_expanded() -> Dict[str, Any]:
    summary_rows = _read_csv_rows(EXPANDED_DIR / "expanded_o15_universe_summary.csv")
    expanded = _first_row(summary_rows, window="full_history", population="expanded_total")
    alternate = _first_row(summary_rows, window="full_history", population="alternate_total")
    main = _first_row(summary_rows, window="full_history", population="main_total")
    return {
        "expanded": expanded,
        "alternate": alternate,
        "main": main,
        "expanded_rows": int(float(expanded.get("rows") or 0)),
        "resolved_rows": int(float(expanded.get("resolved") or 0)),
        "wins": int(float(expanded.get("wins") or 0)),
        "losses": int(float(expanded.get("losses") or 0)),
        "pushes": int(float(expanded.get("pushes") or 0)),
        "roi": expanded.get("roi") or "",
    }


def _context_health() -> Dict[str, Any]:
    data = _safe_json(EXPANDED_DIR / "expanded_o15_context_health_latest.json")
    if not isinstance(data, dict):
        return {"status": "missing", "checks": []}
    return data


def _current_hypotheses() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for thread in _load_research_threads(DEFAULT_OUT_ROOT):
        rows.append(
            {
                "name": str(thread.get("name") or "Unnamed thread"),
                "status": str(thread.get("status") or "active"),
                "current_conclusion": str(thread.get("current_conclusion") or ""),
                "next_action": str(thread.get("next_action") or ""),
                "artifact": str(thread.get("artifact") or ""),
                "command": str(thread.get("command") or ""),
            }
        )
    return rows


def _retired_hypotheses() -> List[Dict[str, str]]:
    return [
        {
            "hypothesis": "Late games are broadly bad.",
            "retired_because": "Late was decomposed into market/team/price composition; late +201 to +250 remained positive.",
            "current_status": "Retired as a standalone causal read; keep as market-composition context.",
        },
        {
            "hypothesis": "Alternate discovery predicts future Tier A migration.",
            "retired_because": "Future-tier migration was too weak to explain alternate value.",
            "current_status": "Retired as primary explanation; use broad factor discovery instead.",
        },
        {
            "hypothesis": "Tier A is universally strong across all O1.5 sources.",
            "retired_because": "Tier A was strong in main/reconstructed contexts but poor in historical alternate; source population matters.",
            "current_status": "Retired as universal rule; retain source-specific interpretation.",
        },
        {
            "hypothesis": "Low-attention alone is enough.",
            "retired_because": "The stronger framing is low-attention plus support, with price realism and BvP context.",
            "current_status": "Reframed as Hidden Support / low-attention signpost.",
        },
    ]


def _timeline_rows(snapshot_date: date, week: int) -> List[Dict[str, str]]:
    return [
        {
            "week": f"{snapshot_date.isocalendar().year}-W{week:02d}",
            "major_discovery": "Expanded O1.5 Universe is now the canonical O1.5 research surface.",
            "major_correction": "Production/main board performance and expanded alternate-market performance are separate populations.",
            "retired_hypotheses": "Tier A universalism; late-games-as-causal; alternate-as-future-Tier-A.",
        },
        {
            "week": f"{snapshot_date.isocalendar().year}-W{week:02d}",
            "major_discovery": "Broad variable importance and feature centrality are favored over tiny funnel optimization.",
            "major_correction": "Context hydration identity fixes materially improved game/team/rest/cluster coverage.",
            "retired_hypotheses": "Tiny high-ROI funnels as decision drivers.",
        },
    ]


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def _append_manifest(path: Path, row: Dict[str, Any]) -> None:
    fieldnames = [
        "snapshot_date",
        "week",
        "research_version",
        "expanded_rows",
        "resolved_rows",
        "major_hypothesis",
        "confidence",
        "notes",
        "snapshot_path",
    ]
    existing = _read_csv_rows(path)
    if any(r.get("snapshot_date") == row["snapshot_date"] and r.get("week") == row["week"] for r in existing):
        raise SystemExit(f"Refusing to append duplicate snapshot manifest row for {row['snapshot_date']} {row['week']}")
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not existing:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def _write_snapshot(snapshot_dir: Path, snapshot_date: date, week: int) -> Dict[str, Any]:
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    week_start = snapshot_date - timedelta(days=6)
    expanded = _summarize_expanded()
    health = _context_health()
    threads = _current_hypotheses()
    variable_rows = _count_csv_rows(EXPANDED_DIR / "expanded_o15_variable_rankings.csv")
    pairwise_rows = _count_csv_rows(EXPANDED_DIR / "expanded_o15_pairwise_interactions.csv")
    archetype_rows = _count_csv_rows(EXPANDED_DIR / "expanded_o15_candidate_archetypes.csv")
    building_blocks = _top_rows(EXPANDED_DIR / "expanded_o15_feature_centrality_building_blocks.csv", 10)
    risk_factors = _top_rows(EXPANDED_DIR / "expanded_o15_feature_centrality_risk_factors.csv", 10)
    variable_rankings = _top_rows(EXPANDED_DIR / "expanded_o15_variable_rankings.csv", 12)
    archetypes = _top_rows(EXPANDED_DIR / "expanded_o15_candidate_archetypes.csv", 10)

    snapshot_dir.mkdir(parents=True, exist_ok=False)

    lines = [
        f"# MLB Weekly Research Snapshot - Week {week:02d}",
        "",
        f"- Snapshot date: `{snapshot_date.isoformat()}`",
        f"- Week ending: `{snapshot_date.isoformat()}`",
        f"- Date range represented: `{week_start.isoformat()}` to `{snapshot_date.isoformat()}`",
        f"- Generated (UTC): `{generated}`",
        f"- Research version: `expanded_o15_research_v1`",
        f"- Status: `immutable snapshot`",
        "",
        "## Purpose",
        "",
        "This snapshot captures what the MLB research program believed at this checkpoint. It is a historical record, not a live report. Future research may correct or retire these conclusions.",
        "",
        "## Research Metrics",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| Expanded O1.5 rows | `{expanded['expanded_rows']}` |",
        f"| Expanded O1.5 resolved rows | `{expanded['resolved_rows']}` |",
        f"| Expanded O1.5 record | `{expanded['wins']}-{expanded['losses']}-{expanded['pushes']}` |",
        f"| Expanded O1.5 ROI | `{_fmt_pct(expanded['roi'])}` |",
        f"| Variable importance rows | `{variable_rows}` |",
        f"| Pairwise interactions | `{pairwise_rows}` |",
        f"| Candidate archetypes | `{archetype_rows}` |",
        f"| Context health | `{health.get('status', 'missing')}` |",
        "",
        "## Current Research Summary",
        "",
        "| thread | status | confidence | current conclusion | next work |",
        "|---|---|---|---|---|",
    ]
    for thread in threads:
        confidence = "Medium"
        status = thread["status"].lower()
        if "monitor" in status:
            confidence = "Medium-Low"
        if "active" in status.upper():
            confidence = "Medium"
        lines.append(
            f"| {thread['name']} | `{thread['status']}` | `{confidence}` | {thread['current_conclusion']} | {thread['next_action']} |"
        )

    lines.extend(
        [
            "",
            "## Expanded O1.5 Universe",
            "",
            "The Expanded O1.5 Universe is the canonical research universe for hits Over 1.5. The production/main board is one source inside this research surface, not the boundary of research.",
            "",
            "| population | rows | resolved | record | ROI | avg odds |",
            "|---|---:|---:|---|---:|---:|",
        ]
    )
    for label, row in (("expanded_total", expanded["expanded"]), ("alternate_total", expanded["alternate"]), ("main_total", expanded["main"])):
        lines.append(
            f"| `{label}` | `{row.get('rows','')}` | `{row.get('resolved','')}` | `{row.get('wins','')}-{row.get('losses','')}-{row.get('pushes','')}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('avg_odds'))}` |"
        )

    lines.extend(["", "## Feature Centrality - Building Blocks", "", "| signal | appearances | avg ROI | median ROI | classification |", "|---|---:|---:|---:|---|"])
    for row in building_blocks[:10]:
        lines.append(
            f"| `{row.get('specific_value','')}` | `{row.get('positive_50_appearances','')}` | `{_fmt_pct(row.get('avg_roi'))}` | `{_fmt_pct(row.get('median_roi'))}` | `{row.get('classification','')}` |"
        )

    lines.extend(["", "## Recurring Risk Factors", "", "| signal | negative appearances | avg ROI | median ROI | classification |", "|---|---:|---:|---:|---|"])
    for row in risk_factors[:10]:
        lines.append(
            f"| `{row.get('specific_value','')}` | `{row.get('negative_50_appearances','')}` | `{_fmt_pct(row.get('avg_roi'))}` | `{_fmt_pct(row.get('median_roi'))}` | `{row.get('classification','')}` |"
        )

    lines.extend(["", "## Variable Importance Highlights", "", "| category | variable | bucket | resolved | ROI | BetOnline ROI |", "|---|---|---|---:|---:|---:|"])
    for row in variable_rankings[:10]:
        lines.append(
            f"| `{row.get('category','')}` | `{row.get('variable','')}` | `{row.get('bucket','')}` | `{row.get('resolved','')}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_pct(row.get('roi_betonline'))}` |"
        )

    lines.extend(["", "## Candidate Archetypes", "", "| rank | type | definition | resolved | ROI | BetOnline ROI | note |", "|---:|---|---|---:|---:|---:|---|"])
    for row in archetypes[:8]:
        lines.append(
            f"| `{row.get('rank','')}` | `{row.get('archetype_type','')}` | `{row.get('definition','')}` | `{row.get('resolved','')}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_pct(row.get('roi_betonline'))}` | {row.get('note','')} |"
        )

    lines.extend(["", "## Context Health", "", "| field group | status | coverage | threshold | note |", "|---|---|---:|---:|---|"])
    for check in health.get("checks", []) if isinstance(health.get("checks"), list) else []:
        lines.append(
            f"| `{check.get('field_group','')}` | `{check.get('status','')}` | `{_fmt_pct(check.get('coverage'))}` | `{_fmt_pct(check.get('threshold')) if check.get('threshold') != '' else 'n/a'}` | {check.get('note','')} |"
        )

    lines.extend(
        [
            "",
            "## Research Doctrine Status",
            "",
            "The research program is now operating under Project Doctrine: historical repair is not complete until daily generation, automation wiring, health checks, Ops Brief/Daily Index visibility, regression detection, documentation, and validation are all present.",
            "",
            "For Expanded O1.5 specifically, context hydration has daily health checks and Daily Index visibility. New research fields must answer: `What keeps this populated tomorrow?`",
            "",
            "## Links To Source Artifacts",
            "",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_universe_manifest.md', 'Expanded O1.5 Manifest')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_variable_importance.md', 'Variable Importance')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_feature_centrality_audit.md', 'Feature Centrality')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_market_classification_audit.md', 'Market Classification')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_hidden_matchup_support_audit.md', 'Hidden Matchup Support')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_low_attention_signpost_audit.md', 'Low-Attention Signpost')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_agreement_score_audit.md', 'Agreement Score')}",
            f"- {_artifact_link(snapshot_dir, EXPANDED_DIR / 'expanded_o15_bvp_integration_audit.md', 'BvP Integration')}",
            f"- {_artifact_link(snapshot_dir, DEFAULT_OUT_ROOT / 'current_research_threads.json', 'Current Research Threads Config')}",
            "",
        ]
    )
    (snapshot_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")

    timeline = _timeline_rows(snapshot_date, week)
    t_lines = [
        f"# MLB Research Timeline - Snapshot {snapshot_date.isoformat()}",
        "",
        "| week | major discovery | major correction | retired hypotheses |",
        "|---|---|---|---|",
    ]
    for row in timeline:
        t_lines.append(f"| `{row['week']}` | {row['major_discovery']} | {row['major_correction']} | {row['retired_hypotheses']} |")
    (snapshot_dir / "timeline.md").write_text("\n".join(t_lines) + "\n", encoding="utf-8")

    retired = _retired_hypotheses()
    r_lines = [
        f"# Retired Hypotheses - Snapshot {snapshot_date.isoformat()}",
        "",
        "| retired hypothesis | retired because | current status |",
        "|---|---|---|",
    ]
    for row in retired:
        r_lines.append(f"| {row['hypothesis']} | {row['retired_because']} | {row['current_status']} |")
    (snapshot_dir / "retired_hypotheses.md").write_text("\n".join(r_lines) + "\n", encoding="utf-8")

    return {
        "expanded_rows": expanded["expanded_rows"],
        "resolved_rows": expanded["resolved_rows"],
        "major_hypothesis": "Expanded O1.5 Hidden Support / broad factor discovery",
        "confidence": "Medium",
        "notes": f"context_health={health.get('status', 'missing')}; variables={variable_rows}; pairwise={pairwise_rows}; archetypes={archetype_rows}",
    }


def main() -> int:
    global DEFAULT_OUT_ROOT, EXPANDED_DIR
    ap = argparse.ArgumentParser(description="Build an immutable MLB weekly research snapshot.")
    ap.add_argument("--date", default=date.today().isoformat(), help="Snapshot/week-ending date.")
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument("--snapshot-root", default="")
    args = ap.parse_args()

    snapshot_date = _parse_date(args.date)
    out_root = Path(args.out_root)
    snapshot_root = Path(args.snapshot_root) if args.snapshot_root else out_root / "research_snapshots"
    DEFAULT_OUT_ROOT = out_root
    EXPANDED_DIR = out_root / "expanded_o15_universe"

    week, snapshot_dir = _snapshot_paths(snapshot_root, snapshot_date)
    if snapshot_dir.exists():
        raise SystemExit(f"Refusing to overwrite immutable research snapshot: {snapshot_dir}")

    metrics = _write_snapshot(snapshot_dir, snapshot_date, week)
    manifest = snapshot_root / "snapshot_manifest.csv"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    _append_manifest(
        manifest,
        {
            "snapshot_date": snapshot_date.isoformat(),
            "week": f"{snapshot_date.isocalendar().year}-W{week:02d}",
            "research_version": "expanded_o15_research_v1",
            "expanded_rows": metrics["expanded_rows"],
            "resolved_rows": metrics["resolved_rows"],
            "major_hypothesis": metrics["major_hypothesis"],
            "confidence": metrics["confidence"],
            "notes": metrics["notes"],
            "snapshot_path": snapshot_dir.as_posix(),
        },
    )
    print(
        f"[mlb-research-snapshot] snapshot_date={snapshot_date.isoformat()} week={week:02d} "
        f"expanded_rows={metrics['expanded_rows']} resolved_rows={metrics['resolved_rows']} out_dir={snapshot_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
