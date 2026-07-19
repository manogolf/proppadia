"""Update living status for Hits 1.5 suppression observation collection.

Read-only aggregation over existing run-bound shadow capture artifacts. It does
not inspect outcomes, call network services, write databases, or change
production behavior.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
AUDIT_DATE = "2026-07-17"
OBS_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits15_prospective_suppression_shadow/2026-07-17"
RUNS_DIR = OBS_ROOT / "runs"
LIVE_CAPTURE_ROOT = ROOT / "artifacts/analysis/model_development/mlb_july17_live_hits15_directional_capture/2026-07-17"
MILESTONES = {
    "genuine_run_tags": 10,
    "distinct_slate_dates": 5,
    "affirmative_suppression_propositions": 50,
    "exact_u15_price_bound_propositions": 30,
}


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_manifest(run_dir: Path) -> dict[str, Any] | None:
    matches = sorted(run_dir.glob("first_genuine_live_run_manifest_*.json"))
    if not matches:
        return None
    return json.loads(matches[-1].read_text(encoding="utf-8"))


def live_surface_counts(run_tag: str) -> dict[str, int]:
    path = LIVE_CAPTURE_ROOT / run_tag / "live_run_manifest.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): int(v) for k, v in payload.get("current_surface_counts", {}).items()}


def run_policy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(str(row["slate_date"]), []).append(row)
    for date_rows in by_date.values():
        date_rows.sort(key=lambda r: str(r.get("decision_timestamp_utc", "")))
        captured = [r for r in date_rows if r["genuine_capture"]]
        if captured:
            captured[0]["run_policy_role"] = "earliest_qualifying_run"
            captured[-1]["run_policy_role"] = (
                "single_qualifying_run" if len(captured) == 1 else "final_pregame_run"
            )
            for item in captured[1:-1]:
                item["run_policy_role"] = "later_market_refresh_run"
        for item in date_rows:
            item.setdefault("run_policy_role", "non_qualifying_or_readiness_run")
    return rows


def main() -> int:
    run_rows: list[dict[str, Any]] = []
    for run_dir in sorted(RUNS_DIR.glob("*")):
        if not run_dir.is_dir():
            continue
        manifest = load_manifest(run_dir)
        if manifest is None:
            continue
        decisions = manifest.get("decisions", {})
        run_tag = manifest.get("run_tag", "")
        date_value = manifest.get("date", "")
        full = read_csv(run_dir / f"full_proposition_classification_ledger_{AUDIT_DATE}_{run_tag}.csv")
        overlap = read_csv(run_dir / f"current_surface_overlap_report_{AUDIT_DATE}_{run_tag}.csv")
        replay = read_csv(run_dir / f"deterministic_replay_comparison_{AUDIT_DATE}_{run_tag}.csv")
        temporal = read_csv(run_dir / f"temporal_integrity_report_{AUDIT_DATE}_{run_tag}.csv")
        surface_counts = {r.get("surface_state", ""): int(float(r.get("hits15_rows") or 0)) for r in overlap}
        live_counts = live_surface_counts(run_tag)
        if live_counts:
            surface_counts = live_counts
        genuine = str(manifest.get("first_capture_status", "")).startswith("CAPTURED")
        temporal_failures = sum(1 for r in temporal if r.get("temporal_integrity_status") != "PASS")
        replay_failures = sum(1 for r in replay if str(r.get("match", "")).lower() not in {"true", "1", "yes"})
        run_rows.append(
            {
                "slate_date": date_value,
                "run_tag": run_tag,
                "decision_timestamp_utc": manifest.get("decision_timestamp_utc", ""),
                "run_output_dir": rel(run_dir),
                "genuine_capture": genuine,
                "first_capture_status": manifest.get("first_capture_status", ""),
                "hits15_propositions": manifest.get("hits15_propositions", 0),
                "affirmative_suppression_count": manifest.get("affirmative_suppression_count", 0),
                "exact_u15_price_bound_count": manifest.get("exact_u15_price_bound_count", 0),
                "surface_both": surface_counts.get("both", 0),
                "surface_over_only": surface_counts.get("OVER-only", 0),
                "surface_under_only": surface_counts.get("UNDER-only", 0),
                "surface_neither": surface_counts.get("neither", 0),
                "temporal_integrity_failures": temporal_failures,
                "deterministic_replay_failures": replay_failures,
                "outcome_grading_status": decisions.get("MLB_HITS15_SUPPRESSION_OUTCOME_GRADING_STATUS", "NOT_AUTHORIZED"),
                "production_status": decisions.get("MLB_HITS15_SUPPRESSION_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
                "proposition_rows_with_hash": sum(1 for r in full if r.get("immutable_row_hash")),
            }
        )
    run_rows = run_policy(run_rows)
    captured = [r for r in run_rows if r["genuine_capture"]]
    distinct_dates = sorted({r["slate_date"] for r in captured})
    cumulative_affirmative = sum(int(r["affirmative_suppression_count"]) for r in captured)
    cumulative_price = sum(int(r["exact_u15_price_bound_count"]) for r in captured)
    temporal_failures = sum(int(r["temporal_integrity_failures"]) for r in captured)
    replay_failures = sum(int(r["deterministic_replay_failures"]) for r in captured)
    latest = captured[-1] if captured else (run_rows[-1] if run_rows else {})
    progress = {
        "genuine_run_tags": len(captured),
        "distinct_slate_dates": len(distinct_dates),
        "affirmative_suppression_propositions": cumulative_affirmative,
        "exact_u15_price_bound_propositions": cumulative_price,
    }
    milestone_rows = []
    for name, target in MILESTONES.items():
        value = progress[name]
        milestone_rows.append(
            {
                "milestone": name,
                "current": value,
                "target": target,
                "remaining": max(0, target - value),
                "met": value >= target,
            }
        )
    milestone_met = all(row["met"] for row in milestone_rows) and temporal_failures == 0 and replay_failures == 0
    status = {
        "generated_at_utc": now(),
        "latest_run_tag": latest.get("run_tag", ""),
        "latest_slate_date": latest.get("slate_date", ""),
        "total_genuine_runs": len(captured),
        "distinct_slate_dates": len(distinct_dates),
        "cumulative_affirmative_suppression_rows": cumulative_affirmative,
        "cumulative_exact_u15_price_bound_rows": cumulative_price,
        "temporal_integrity_failures": temporal_failures,
        "deterministic_replay_failures": replay_failures,
        "milestone_status": "MILESTONE_REACHED_AWAITING_GRADING_AUTHORIZATION_REQUEST"
        if milestone_met
        else "IN_PROGRESS_CAPTURE_CONTINUES",
        "grading_authorization_status": "NOT_AUTHORIZED",
        "production_status": "NOT_AUTHORIZED",
        "hitter_owned_multi_hit_status": "NO_EXISTING_REGIME_VALIDATED",
        "hitter_prospective_challenger_status": "NOT_AUTHORIZED",
    }
    write_csv(OBS_ROOT / f"living_observation_run_index_{AUDIT_DATE}.csv", run_rows, [
        "slate_date",
        "run_tag",
        "decision_timestamp_utc",
        "run_policy_role",
        "run_output_dir",
        "genuine_capture",
        "first_capture_status",
        "hits15_propositions",
        "affirmative_suppression_count",
        "exact_u15_price_bound_count",
        "surface_both",
        "surface_over_only",
        "surface_under_only",
        "surface_neither",
        "temporal_integrity_failures",
        "deterministic_replay_failures",
        "outcome_grading_status",
        "production_status",
        "proposition_rows_with_hash",
    ])
    write_csv(OBS_ROOT / f"living_observation_milestone_progress_{AUDIT_DATE}.csv", milestone_rows, [
        "milestone",
        "current",
        "target",
        "remaining",
        "met",
    ])
    write_json(OBS_ROOT / f"living_observation_status_{AUDIT_DATE}.json", status)
    md = f"""# MLB Hits 1.5 Prospective Suppression Observation Status

Generated: `{status['generated_at_utc']}`

- Latest run tag: `{status['latest_run_tag']}`
- Latest slate date: `{status['latest_slate_date']}`
- Genuine run tags: `{status['total_genuine_runs']}` / `{MILESTONES['genuine_run_tags']}`
- Distinct slate dates: `{status['distinct_slate_dates']}` / `{MILESTONES['distinct_slate_dates']}`
- Affirmative suppression propositions: `{status['cumulative_affirmative_suppression_rows']}` / `{MILESTONES['affirmative_suppression_propositions']}`
- Exact U1.5 price-bound propositions: `{status['cumulative_exact_u15_price_bound_rows']}` / `{MILESTONES['exact_u15_price_bound_propositions']}`
- Temporal-integrity failures: `{status['temporal_integrity_failures']}`
- Deterministic-replay failures: `{status['deterministic_replay_failures']}`
- Milestone status: `{status['milestone_status']}`
- Outcome grading: `{status['grading_authorization_status']}`
- Production status: `{status['production_status']}`

No outcomes are inspected or graded during capture. Hitter-owned O1.5 remains `{status['hitter_owned_multi_hit_status']}`.
"""
    write_md(OBS_ROOT / f"living_observation_status_{AUDIT_DATE}.md", md)
    decisions = [
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_CAPTURE_DECISION", "value": "CAPTURED" if latest.get("genuine_capture") else "NON_QUALIFYING_RECORDED"},
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_POPULATION_DECISION", "value": "AFFIRMATIVE_POPULATION_CAPTURED" if int(latest.get("affirmative_suppression_count") or 0) else "ZERO_AFFIRMATIVE_POPULATION"},
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_PRICE_BINDING_DECISION", "value": "EXACT_U15_PRICE_BOUND" if int(latest.get("exact_u15_price_bound_count") or 0) else "NO_EXACT_U15_PRICE_BOUND"},
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_SURFACE_ALIGNMENT_DECISION", "value": "CURRENT_SURFACE_PRESERVED_RESEARCH_ONLY"},
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_TEMPORAL_INTEGRITY_DECISION", "value": "PASS" if int(latest.get("temporal_integrity_failures") or 0) == 0 else "FAIL"},
        {"decision": "MLB_HITS15_SUPPRESSION_RUN_DETERMINISTIC_REPLAY_DECISION", "value": "PASS" if int(latest.get("deterministic_replay_failures") or 0) == 0 else "FAIL"},
        {"decision": "MLB_HITS15_SUPPRESSION_OBSERVATION_MILESTONE_STATUS", "value": status["milestone_status"]},
        {"decision": "MLB_HITS15_SUPPRESSION_OUTCOME_GRADING_STATUS", "value": "NOT_AUTHORIZED"},
        {"decision": "MLB_HITS15_SUPPRESSION_PRODUCTION_STATUS", "value": "NOT_AUTHORIZED"},
    ]
    write_csv(OBS_ROOT / f"living_observation_decision_report_{AUDIT_DATE}.csv", decisions, ["decision", "value"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
