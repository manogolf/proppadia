#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_REVIEW_AIDS_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_PERFORMANCE_DIR = Path("artifacts/analysis/mlb/review_aids/performance")
DEFAULT_EXPANDED_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")

ENVIRONMENT_COMPONENT_FIELDS = [
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "offense_hits_pg_last7",
    "offense_hits_pg_last15",
    "offense_hits_pg_last30",
    "offense_hits_form_blended",
    "league_offense_hits_form_blended",
    "offense_factor_vs_league",
    "offense_factor_vs_league_clamped",
    "bullpen_hits_allowed_pg_last7",
    "bullpen_hits_allowed_pg_last15",
    "bullpen_hits_allowed_pg_last30",
    "bullpen_hits_allowed_form_blended",
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


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


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _non_null(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _date_from_filename(path: Path) -> str:
    import re

    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    return match.group(1) if match else ""


def _artifact_specs(review_aids_dir: Path, performance_dir: Path, expanded_dir: Path, date_text: str) -> list[tuple[str, Path, str]]:
    specs: list[tuple[str, Path, str]] = []
    for name in (
        "hits_o15_simple_filter",
        "hits_o15_watch_candidates",
        "hits_o15_layered_candidates",
        "hits_u15_favorite_audit",
        "hits_o15_alternate_discovery",
    ):
        specs.append((name, review_aids_dir / f"{name}_{date_text}.csv", "current_board"))
    specs.extend(
        [
            ("hits_o15_tier_backtest_rows", review_aids_dir / "hits_o15_tier_backtest_rows.csv", "tier_audit_rows"),
            ("hits_u15_tier_backtest_rows", review_aids_dir / "hits_u15_tier_backtest_rows.csv", "tier_audit_rows"),
            (
                "o15_manual_unified_board_universe_rows",
                performance_dir / "o15_manual_unified_board_universe_rows.csv",
                "reconcile_linked_research_rows",
            ),
            (
                "expanded_o15_universe_rows",
                expanded_dir / "expanded_o15_universe_rows.csv",
                "expanded_universe_rows",
            ),
        ]
    )
    return specs


def _coverage_rows(artifact: str, path: Path, artifact_type: str) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    headers = set(rows[0].keys()) if rows else set()
    out: list[dict[str, Any]] = []
    for field in ENVIRONMENT_COMPONENT_FIELDS:
        present = field in headers
        non_null = sum(1 for row in rows if _non_null(row.get(field))) if present else 0
        out.append(
            {
                "artifact": artifact,
                "artifact_type": artifact_type,
                "path": _rel(path),
                "exists": path.exists(),
                "rows": len(rows),
                "field": field,
                "field_present": present,
                "non_null_rows": non_null,
                "coverage_pct": (non_null / len(rows)) if rows else "",
                "lineage_status": "present" if present else "missing_column",
            }
        )
    return out


def _artifact_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("artifact")), []).append(row)
    out: list[dict[str, Any]] = []
    for artifact, group in grouped.items():
        missing = [row["field"] for row in group if row.get("field_present") is not True]
        rows_count = int(group[0].get("rows") or 0)
        core = [
            "pitcher_expected_hits_allowed_weighted",
            "offense_factor_vs_league_clamped",
            "bullpen_hits_allowed_form_blended",
            "starter_expected_hits_allowed",
            "team_expected_hits_allowed",
        ]
        core_present = all(any(row["field"] == field and row.get("field_present") is True for row in group) for field in core)
        out.append(
            {
                "artifact": artifact,
                "artifact_type": group[0].get("artifact_type"),
                "path": group[0].get("path"),
                "rows": rows_count,
                "fields_present": len([row for row in group if row.get("field_present") is True]),
                "fields_expected": len(ENVIRONMENT_COMPONENT_FIELDS),
                "missing_fields": ",".join(missing),
                "core_fields_present": core_present,
                "status": "PASS" if not missing else "WARN",
            }
        )
    return out


def _write_report(path: Path, coverage_rows: list[dict[str, Any]], summary_rows: list[dict[str, Any]], date_text: str) -> None:
    pass_count = sum(1 for row in summary_rows if row.get("status") == "PASS")
    warn_count = sum(1 for row in summary_rows if row.get("status") == "WARN")
    lines = [
        "# Hits 1.5 Environment v1.1 Lineage Health",
        "",
        f"- Date: `{date_text}`",
        f"- Generated at: `{_now()}`",
        "- Scope: research/context lineage retention only.",
        "- These fields are not production decision rules and do not change tiers, selectors, uploads, grading, thresholds, Ops Brief, or Morning Workbench behavior.",
        "",
        "## Summary",
        "",
        f"- Artifacts checked: `{len(summary_rows)}`",
        f"- PASS: `{pass_count}`",
        f"- WARN: `{warn_count}`",
        "",
        "## Artifact Status",
        "",
        "| artifact | type | rows | fields present | status | missing fields |",
        "|---|---|---:|---:|---|---|",
    ]
    for row in summary_rows:
        missing = str(row.get("missing_fields") or "")
        lines.append(
            f"| `{row.get('artifact')}` | `{row.get('artifact_type')}` | `{row.get('rows')}` | "
            f"`{row.get('fields_present')}/{row.get('fields_expected')}` | `{row.get('status')}` | "
            f"{'none' if not missing else '`' + missing + '`'} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Missing values are allowed when the source context is unavailable; missing columns are WARN.",
            "- `starter_expected_hits_allowed` remains the active tier input.",
            "- `team_expected_hits_allowed` and the component stack are retained for future Environment v2 testing.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Check passive lineage retention for MLB Hits 1.5 environment components.")
    ap.add_argument("--date", default=datetime.now().date().isoformat())
    ap.add_argument("--review-aids-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--performance-dir", type=Path, default=DEFAULT_PERFORMANCE_DIR)
    ap.add_argument("--expanded-dir", type=Path, default=DEFAULT_EXPANDED_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    args = ap.parse_args()

    date_text = str(args.date)[:10]
    coverage: list[dict[str, Any]] = []
    for artifact, path, artifact_type in _artifact_specs(args.review_aids_dir, args.performance_dir, args.expanded_dir, date_text):
        coverage.extend(_coverage_rows(artifact, path, artifact_type))
    summary = _artifact_summary(coverage)

    out_dir = args.out_dir
    coverage_csv = out_dir / f"offensive_environment_v1_1_lineage_coverage_{date_text}.csv"
    summary_csv = out_dir / f"offensive_environment_v1_1_lineage_summary_{date_text}.csv"
    report_md = out_dir / f"offensive_environment_v1_1_lineage_health_{date_text}.md"
    latest_json = out_dir / "offensive_environment_v1_1_lineage_health_latest.json"
    _write_csv(coverage_csv, coverage)
    _write_csv(summary_csv, summary)
    _write_report(report_md, coverage, summary, date_text)
    payload = {
        "status": "pass" if all(row.get("status") == "PASS" for row in summary) else "warn",
        "generated_at": _now(),
        "date": date_text,
        "artifacts_checked": len(summary),
        "warn_artifacts": [row.get("artifact") for row in summary if row.get("status") == "WARN"],
        "outputs": {
            "coverage_csv": _rel(coverage_csv),
            "summary_csv": _rel(summary_csv),
            "report_md": _rel(report_md),
        },
    }
    latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
