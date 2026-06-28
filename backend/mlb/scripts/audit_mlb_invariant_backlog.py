#!/usr/bin/env python3
"""Maintain and summarize the MLB invariant intake backlog."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FIELDS = [
    "date_added",
    "source_issue",
    "doctrine_area",
    "proposed_invariant",
    "severity",
    "status",
    "target_check",
    "owner_notes",
    "related_artifacts",
]
VALID_STATUSES = {"proposed", "accepted", "implemented", "rejected"}


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def _status(row: dict[str, str]) -> str:
    status = str(row.get("status") or "proposed").strip().lower()
    return status if status in VALID_STATUSES else "proposed"


def _current_audit_link(out_dir: Path, date_value: str) -> str:
    path = out_dir / f"mlb_project_invariants_{date_value}.md"
    if path.exists():
        return path.as_posix()
    latest = out_dir / "mlb_project_invariants_latest.json"
    if latest.exists():
        try:
            data = json.loads(latest.read_text(encoding="utf-8"))
            return str(data.get("md") or "")
        except Exception:
            return ""
    return ""


def _write_md(path: Path, *, rows: list[dict[str, str]], date_value: str, current_audit: str) -> dict[str, Any]:
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    counts = Counter(_status(row) for row in rows)
    accepted_without_check = [
        row for row in rows
        if _status(row) == "accepted" and not str(row.get("target_check") or "").strip()
    ]
    not_implemented = [row for row in rows if _status(row) in {"proposed", "accepted"}]
    invalid_status_rows = [
        row for row in rows
        if str(row.get("status") or "").strip().lower() not in VALID_STATUSES
    ]
    status = "WARN" if accepted_without_check or invalid_status_rows else "PASS"

    lines = [
        "# MLB Invariant Backlog",
        "",
        f"- Generated UTC: `{generated}`",
        f"- Status: `{status}`",
        "- Purpose: every future bug, repair, or doctrine lesson should be captured as a proposed invariant before it is forgotten.",
        "",
        "## Summary",
        "",
        f"- Proposed: `{counts.get('proposed', 0)}`",
        f"- Accepted not implemented: `{len([row for row in rows if _status(row) == 'accepted'])}`",
        f"- Implemented: `{counts.get('implemented', 0)}`",
        f"- Rejected: `{counts.get('rejected', 0)}`",
        f"- Accepted without automated target check: `{len(accepted_without_check)}`",
        f"- Current invariant audit: `{current_audit or 'missing'}`",
        "",
        "## Intake Rule",
        "",
        "Every resolved bug should answer: should this become an invariant?",
        "",
        "If yes, add it here as `proposed` or implement it directly in `make mlb-project-invariants`.",
        "If no, document the reason in the fix notes or owner notes.",
        "",
        "## Not Implemented",
        "",
    ]
    if not_implemented:
        lines.extend(
            [
                "| date | status | severity | doctrine area | proposed invariant | target check | related artifacts |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for row in not_implemented:
            lines.append(
                f"| {row.get('date_added', '')} | `{_status(row)}` | `{row.get('severity', '')}` | "
                f"{row.get('doctrine_area', '')} | {row.get('proposed_invariant', '')} | "
                f"`{row.get('target_check', '')}` | {row.get('related_artifacts', '')} |"
            )
    else:
        lines.append("- No proposed or accepted invariants are waiting for implementation.")
    lines.extend(["", "## All Backlog Rows", ""])
    if rows:
        lines.extend(
            [
                "| date | status | severity | source issue | doctrine area | invariant | target check | owner notes |",
                "|---|---|---|---|---|---|---|---|",
            ]
        )
        for row in rows:
            lines.append(
                f"| {row.get('date_added', '')} | `{_status(row)}` | `{row.get('severity', '')}` | "
                f"{row.get('source_issue', '')} | {row.get('doctrine_area', '')} | "
                f"{row.get('proposed_invariant', '')} | `{row.get('target_check', '')}` | {row.get('owner_notes', '')} |"
            )
    else:
        lines.append("- Backlog initialized; no rows yet.")
    if accepted_without_check:
        lines.extend(["", "## Warnings", ""])
        for row in accepted_without_check:
            lines.append(f"- Accepted invariant lacks automated target check: {row.get('proposed_invariant', '')}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "generated_at_utc": generated,
        "date": date_value,
        "status": status.lower(),
        "ok": status == "PASS",
        "counts": dict(counts),
        "proposed": counts.get("proposed", 0),
        "accepted_not_implemented": counts.get("accepted", 0),
        "implemented": counts.get("implemented", 0),
        "rejected": counts.get("rejected", 0),
        "accepted_without_target_check": len(accepted_without_check),
        "invalid_status_rows": len(invalid_status_rows),
        "current_invariant_audit": current_audit,
        "csv": str(path.with_suffix(".csv")),
        "md": str(path),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize the MLB invariant intake backlog.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/invariants")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    csv_path = out_dir / "invariant_backlog.csv"
    md_path = out_dir / "invariant_backlog.md"
    json_path = out_dir / "invariant_backlog_summary.json"
    rows = _read_rows(csv_path)
    if not csv_path.exists():
        _write_csv(csv_path, rows)
    current_audit = _current_audit_link(out_dir, str(args.date))
    payload = _write_md(md_path, rows=rows, date_value=str(args.date), current_audit=current_audit)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"[mlb-invariant-backlog] status={payload['status']} proposed={payload['proposed']} "
        f"accepted_not_implemented={payload['accepted_not_implemented']} implemented={payload['implemented']} "
        f"out_md={md_path}"
    )
    return 1 if payload["accepted_without_target_check"] or payload["invalid_status_rows"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
