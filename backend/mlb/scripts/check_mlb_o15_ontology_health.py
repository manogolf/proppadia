#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.mlb.ontology import ONTOLOGY_FIELDS, ontology_health_warnings


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _date_default() -> str:
    return datetime.now().date().isoformat()


def _artifact_specs(date_text: str, out_root: Path) -> list[dict[str, Any]]:
    review_dir = out_root / "review_aids"
    return [
        {
            "artifact": "hits_o15_simple_filter",
            "path": review_dir / f"hits_o15_simple_filter_{date_text}.csv",
            "required": True,
        },
        {
            "artifact": "hits_o15_watch_candidates",
            "path": review_dir / f"hits_o15_watch_candidates_{date_text}.csv",
            "required": True,
        },
        {
            "artifact": "hits_o15_layered_candidates",
            "path": review_dir / f"hits_o15_layered_candidates_{date_text}.csv",
            "required": True,
        },
        {
            "artifact": "hits_o15_alternate_discovery",
            "path": review_dir / f"hits_o15_alternate_discovery_{date_text}.csv",
            "required": False,
        },
        {
            "artifact": "expanded_o15_universe_rows",
            "path": out_root / "expanded_o15_universe" / "expanded_o15_universe_rows.csv",
            "required": True,
            "date_filter": date_text,
        },
        {
            "artifact": "review_aid_performance_latest_slate",
            "path": review_dir / "performance" / "review_aid_performance_latest_slate.csv",
            "required": False,
            "o15_only": True,
        },
    ]


def _row_in_scope(row: dict[str, Any], spec: dict[str, Any], date_text: str) -> bool:
    if spec.get("date_filter"):
        row_date = str(row.get("date") or row.get("board_date") or row.get("game_date") or "")[:10]
        if row_date != str(spec["date_filter"]):
            return False
    if spec.get("o15_only"):
        side = str(row.get("side") or "").strip().lower()
        line = str(row.get("line") or "").strip()
        board = str(row.get("board") or "").strip()
        return side == "over" and (line in {"1.5", "1.50"} or line == "") and not board.startswith("u15")
    return True


def _status_for(rows: list[dict[str, Any]]) -> str:
    if any(row.get("status") == "FAIL" for row in rows):
        return "fail"
    if any(row.get("status") == "WARN" for row in rows):
        return "warn"
    return "pass"


def _summarize_artifact(spec: dict[str, Any], date_text: str) -> dict[str, Any]:
    path = Path(spec["path"])
    required = bool(spec.get("required"))
    raw_rows = _read_csv(path)
    rows = [row for row in raw_rows if _row_in_scope(row, spec, date_text)]
    missing_counts = {field: 0 for field in ONTOLOGY_FIELDS}
    warning_counts: Counter[str] = Counter()
    invalid_rows = 0
    for row in rows:
        warnings = ontology_health_warnings(row)
        if warnings:
            invalid_rows += 1
        warning_counts.update(warnings)
        for field in ONTOLOGY_FIELDS:
            if not str(row.get(field) or "").strip():
                missing_counts[field] += 1
    if not path.exists():
        status = "FAIL" if required else "WARN"
        detail = "missing required artifact" if required else "optional artifact missing"
    elif rows and invalid_rows:
        status = "FAIL"
        detail = f"{invalid_rows} row(s) have missing or invalid ontology metadata"
    elif not rows and required:
        status = "WARN"
        detail = "artifact exists but has zero in-scope rows"
    else:
        status = "PASS"
        detail = "ok"
    return {
        "artifact": str(spec["artifact"]),
        "path": _rel(path),
        "required": required,
        "status": status,
        "rows": len(rows),
        "raw_rows": len(raw_rows),
        "invalid_rows": invalid_rows,
        "rows_missing_universe": missing_counts["universe"],
        "rows_missing_population": missing_counts["population"],
        "rows_missing_classification": max(
            missing_counts["classification_type"],
            missing_counts["classification_value"],
        ),
        "rows_missing_provenance": missing_counts["provenance_layer"],
        "warning_counts": json.dumps(dict(sorted(warning_counts.items())), sort_keys=True),
        "detail": detail,
    }


def _write_md(path: Path, date_text: str, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    lines = [
        f"# O1.5 Ontology Health - {date_text}",
        "",
        f"- Status: `{summary['status']}`",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Rows checked: `{summary['rows_checked']}`",
        f"- Invalid rows: `{summary['invalid_rows']}`",
        "",
        "## Artifact Coverage",
        "",
        "| artifact | status | rows | missing universe | missing population | missing classification | missing provenance | detail |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['artifact']} | `{row['status']}` | `{row['rows']}` | `{row['rows_missing_universe']}` | `{row['rows_missing_population']}` | `{row['rows_missing_classification']}` | `{row['rows_missing_provenance']}` | {row['detail']} |"
        )
    lines.extend(
        [
            "",
            "## Required Fields",
            "",
            ", ".join(f"`{field}`" for field in ONTOLOGY_FIELDS),
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Check O1.5 ontology metadata coverage.")
    ap.add_argument("--date", default=_date_default())
    ap.add_argument("--out-root", default="artifacts/analysis/mlb")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/ontology")
    args = ap.parse_args()

    date_text = str(args.date)[:10]
    out_root = Path(args.out_root)
    out_dir = Path(args.out_dir)
    rows = [_summarize_artifact(spec, date_text) for spec in _artifact_specs(date_text, out_root)]
    summary = {
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "date": date_text,
        "status": _status_for(rows),
        "rows_checked": sum(int(row["rows"]) for row in rows),
        "invalid_rows": sum(int(row["invalid_rows"]) for row in rows),
        "artifact_count": len(rows),
        "outputs": {
            "md": _rel(out_dir / "ontology_health.md"),
            "csv": _rel(out_dir / "ontology_health.csv"),
            "json": _rel(out_dir / "ontology_health.json"),
        },
    }
    _write_csv(out_dir / "ontology_health.csv", rows)
    (out_dir / "ontology_health.json").write_text(
        json.dumps({**summary, "artifacts": rows}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_md(out_dir / "ontology_health.md", date_text, summary, rows)
    print(f"ontology_health_status={summary['status']}")
    print(f"rows_checked={summary['rows_checked']}")
    print(f"invalid_rows={summary['invalid_rows']}")
    print(f"ontology_health_md={_rel(out_dir / 'ontology_health.md')}")
    return 0 if summary["status"] in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
