#!/usr/bin/env python3
"""Health check for Expanded O1.5 context hydration coverage.

Research health only. Reads expanded_o15_universe_rows.csv and reports whether
the context fields expected by broad-factor research remain populated.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.shared.time_utils_backend import TIME_OF_DAY_BUCKETS, get_time_of_day_bucket_et


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")

THRESHOLDS = {
    "game_id": 0.95,
    "game_time": 0.95,
    "time_of_day_bucket": 0.95,
    "game_day_of_week": 0.95,
    "is_home": 0.80,
    "team_offense_context": 0.80,
    "same_game_cluster_context": 0.80,
    "rest_context": 0.75,
}

FIELD_GROUPS = {
    "game_id": ["game_id"],
    "game_time": ["game_time"],
    "time_of_day_bucket": ["time_of_day_bucket"],
    "game_day_of_week": ["game_day_of_week"],
    "is_home": ["is_home"],
    "team_offense_context": [
        "team_d7_runs_per_game",
        "team_d7_hits_per_game",
        "team_d7_total_bases_per_game",
        "team_d15_runs_per_game",
        "team_d15_hits_per_game",
        "team_d15_total_bases_per_game",
    ],
    "same_game_cluster_context": [
        "same_game_teammate_tier_a_count",
        "same_game_team_o15_candidate_count",
        "lineup_heat_cluster",
    ],
    "rest_context": [
        "previous_team_game_time",
        "day_after_night",
        "short_turnaround",
        "rest_day_before_game",
    ],
    "bvp_payload_present": ["bvp_payload_present"],
    "park_venue": ["park", "venue"],
    "lineup_slot": ["lineup_slot", "batting_order"],
}

NON_GATED = {"bvp_payload_present", "park_venue", "lineup_slot"}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _present(value: Any) -> bool:
    text = str(value or "").strip()
    return text != "" and text.lower() not in {"nan", "none", "null"}


def _group_present(row: dict[str, Any], fields: list[str]) -> bool:
    return all(_present(row.get(field)) for field in fields)


def _load_gap_reasons(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = _read_csv(path)
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("first_likely_gap") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return [
        {"reason": reason, "rows": count}
        for reason, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def _canonical_time_bucket(value: Any) -> str:
    try:
        return str(get_time_of_day_bucket_et(value))
    except Exception:
        return ""


def _health_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(rows)
    out: list[dict[str, Any]] = []
    for group, fields in FIELD_GROUPS.items():
        count = sum(1 for row in rows if _group_present(row, fields))
        coverage = count / total if total else 0.0
        threshold = THRESHOLDS.get(group)
        gated = group not in NON_GATED
        if not gated:
            status = "info"
        elif threshold is None:
            status = "info"
        else:
            status = "pass" if coverage >= threshold else "fail"
        out.append(
            {
                "field_group": group,
                "fields": ",".join(fields),
                "rows": total,
                "populated_rows": count,
                "coverage": coverage,
                "threshold": threshold if threshold is not None else "",
                "gated": gated,
                "status": status,
                "note": (
                    "reported_not_fail_gated"
                    if group == "bvp_payload_present"
                    else "source_unavailable_not_fail_gated"
                    if group in {"park_venue", "lineup_slot"}
                    else ""
                ),
            }
        )
    return out


def _time_bucket_health_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(rows)
    valid = set(TIME_OF_DAY_BUCKETS)
    invalid = 0
    mismatch = 0
    with_time = 0
    labels: dict[str, int] = {label: 0 for label in TIME_OF_DAY_BUCKETS}
    for row in rows:
        stored = str(row.get("time_of_day_bucket") or "").strip().lower()
        if stored in labels:
            labels[stored] += 1
        elif stored:
            invalid += 1
        canonical = _canonical_time_bucket(row.get("game_time"))
        if canonical:
            with_time += 1
        if stored and canonical and stored != canonical:
            mismatch += 1
    return [
        {
            "field_group": "time_of_day_bucket_valid_labels",
            "fields": "time_of_day_bucket",
            "rows": total,
            "populated_rows": total - invalid,
            "coverage": (total - invalid) / total if total else 0.0,
            "threshold": 1.0,
            "gated": True,
            "status": "pass" if invalid == 0 else "fail",
            "note": f"invalid_labels={invalid}; valid={','.join(TIME_OF_DAY_BUCKETS)}",
        },
        {
            "field_group": "time_of_day_bucket_matches_game_time_et",
            "fields": "game_time,time_of_day_bucket",
            "rows": total,
            "populated_rows": with_time - mismatch,
            "coverage": (with_time - mismatch) / with_time if with_time else 0.0,
            "threshold": 1.0,
            "gated": True,
            "status": "pass" if mismatch == 0 else "fail",
            "note": f"mismatched_rows={mismatch}; canonical_timezone=America/New_York",
        },
    ]


def _write_md(path: Path, *, date: str, status: str, rows: list[dict[str, Any]], gap_reasons: list[dict[str, Any]]) -> None:
    lines = [
        f"# Expanded O1.5 Context Health - {date}",
        "",
        f"- Status: `{status}`",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        "- Scope: research health only; no production selector/upload/model/grading changes.",
        "",
        "## Coverage Gates",
        "",
        "| field group | coverage | threshold | status | note |",
        "|---|---:|---:|---|---|",
    ]
    for row in rows:
        threshold = row.get("threshold")
        threshold_text = "" if threshold == "" else f"{float(threshold) * 100:.2f}%"
        lines.append(
            f"| {row.get('field_group')} | {float(row.get('coverage') or 0.0) * 100:.2f}% | "
            f"{threshold_text} | `{row.get('status')}` | {row.get('note') or ''} |"
        )
    lines.extend(
        [
            "",
            "## Suggested Repair",
            "",
            "If a gated field regresses, rerun:",
            "",
            "```bash",
            "make mlb-expanded-o15-universe",
            "make mlb-expanded-o15-variable-importance-survey",
            "```",
            "",
            "## Top Missing Reasons",
            "",
        ]
    )
    if gap_reasons:
        for row in gap_reasons[:10]:
            lines.append(f"- `{row.get('reason')}`: `{row.get('rows')}` rows")
    else:
        lines.append("- Identity gap rows not available.")
    lines.extend(
        [
            "",
            "## Team Expected Hits Note",
            "",
            "`team_expected_hits_allowed` is currently a context signal for player-prop research, not a direct team-hits wagering lane. A direct team-hits prop lane would require separate market capture, reconcile/outcome tracking, and validation.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(date: str, rows_csv: Path, out_dir: Path) -> dict[str, Any]:
    rows = _read_csv(rows_csv)
    health = _health_rows(rows)
    health.extend(_time_bucket_health_rows(rows))
    fail_count = sum(1 for row in health if row.get("status") == "fail")
    status = "fail" if fail_count else "pass"
    out_csv = out_dir / f"expanded_o15_context_health_{date}.csv"
    out_md = out_dir / f"expanded_o15_context_health_{date}.md"
    out_json = out_dir / f"expanded_o15_context_health_{date}.json"
    latest_json = out_dir / "expanded_o15_context_health_latest.json"
    gap_reasons = _load_gap_reasons(out_dir / "expanded_o15_context_identity_gap_rows.csv")
    _write_csv(out_csv, health)
    _write_md(out_md, date=date, status=status, rows=health, gap_reasons=gap_reasons)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "date": date,
        "status": status,
        "ok": status == "pass",
        "fail_count": fail_count,
        "rows_csv": str(rows_csv),
        "health_csv": str(out_csv),
        "health_md": str(out_md),
        "checks": health,
        "top_missing_reasons": gap_reasons[:10],
    }
    out_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Check Expanded O1.5 context hydration health.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--rows-csv", type=Path, default=DEFAULT_ROWS_CSV)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()
    payload = run(str(args.date), args.rows_csv, args.out_dir)
    print(
        f"[expanded-o15-context-health] status={payload['status']} fail={payload['fail_count']} "
        f"out={payload['health_md']}"
    )
    raise SystemExit(1 if payload["status"] == "fail" else 0)


if __name__ == "__main__":
    main()
