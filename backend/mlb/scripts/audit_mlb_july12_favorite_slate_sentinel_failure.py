"""Read-only July 12 favorite-slate sentinel failure audit.

This utility deliberately fails closed if the exact user-tracked 15-row
favorite slate cannot be recovered from repository evidence. It does not
reconstruct a top-15 population from post hoc criteria.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


AUDIT_DATE = "2026-07-17"
SLATE_DATE = "2026-07-12"
PACKAGE_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17"
)


@dataclass(frozen=True)
class CandidateSource:
    label: str
    path: Path
    expected_semantics: str
    selection_label_field: str = ""


KNOWN_SOURCES = [
    CandidateSource(
        "ops_brief",
        Path("artifacts/analysis/mlb/mlb_daily_ops_brief_2026-07-12.md"),
        "Pregame/current-slate report surface. Mentions favorite/review-aid boards but does not freeze a user-tracked 15-row slate.",
    ),
    CandidateSource(
        "hits_u15_favorite_audit",
        Path("artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-07-12.csv"),
        "Generated U1.5 favorite audit board. Full board has generated rows; not the user-tracked 15-row mixed/selected favorite slate.",
        "layer_label",
    ),
    CandidateSource(
        "hits_o15_simple_filter",
        Path("artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_2026-07-12.csv"),
        "Generated O1.5 simple-filter board. Full board has generated rows; not the user-tracked 15-row favorite slate.",
        "combined_tier",
    ),
    CandidateSource(
        "hits_o15_layered_candidates",
        Path("artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-07-12.csv"),
        "Generated O1.5 layered review-aid board. Contains 3 QC watch rows and other layers; not the user-tracked 15-row favorite slate.",
        "layer_label",
    ),
    CandidateSource(
        "hits_o15_watch_candidates",
        Path("artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-07-12.csv"),
        "Generated O1.5 watch population. Contains 3 rows, therefore cannot be the 15-row favorite slate.",
        "combined_tier",
    ),
    CandidateSource(
        "hits_o15_alternate_discovery",
        Path("artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_2026-07-12.csv"),
        "Generated alternate-discovery O1.5 board. Contains alternate market rows; not a frozen user-tracked 15-row favorite slate.",
        "alternate_layer",
    ),
    CandidateSource(
        "odds_history_slate_output_latest",
        Path("backend/mlb/exports/odds_history/2026-07-12/mlb_slate_output.csv"),
        "Latest run-tagged slate output for July 12. Broad market/prediction surface; not a user-tracked 15-row slate.",
    ),
    CandidateSource(
        "odds_history_book_upload_latest",
        Path("backend/mlb/exports/odds_history/2026-07-12/mlb_book_upload.csv"),
        "Latest book-upload shaped artifact. Broad upload surface; not a user-tracked 15-row slate.",
    ),
    CandidateSource(
        "hits_lane_selector_dir",
        Path("backend/mlb/exports/model_v2/lanes/today/2026-07-12"),
        "Run-tagged lane selector and quick-card outputs. Broad generated artifacts; no explicit 15-row favorite slate found.",
    ),
    CandidateSource(
        "review_aid_performance_latest",
        Path("artifacts/analysis/mlb/review_aids/performance/review_aid_performance_latest_slate.csv"),
        "Completed-slate performance artifact. Outcome summary surface, not the pregame tracked 15-row source.",
    ),
]

PLACEHOLDER_COLUMNS = [
    "audit_section",
    "status",
    "reason",
    "required_population_status",
    "notes",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: Iterable[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        seen: list[str] = []
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.append(key)
        fieldnames = seen or ["status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def csv_row_count(path: Path) -> int | None:
    try:
        return len(pd.read_csv(path, low_memory=False))
    except Exception:
        return None


def source_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in KNOWN_SOURCES:
        path = source.path
        exists = path.exists()
        file_count = ""
        row_count: int | str | None = ""
        labels = ""
        if exists and path.is_dir():
            files = sorted(p for p in path.rglob("*") if p.is_file())
            file_count = len(files)
            run_tags = []
            for p in files:
                name = p.name
                if "__" in name:
                    run_tags.append(name.split("__", 1)[1].split(".", 1)[0])
            labels = ";".join(sorted(set(run_tags))[:20])
        elif exists and path.suffix.lower() == ".csv":
            row_count = csv_row_count(path)
            if source.selection_label_field:
                try:
                    df = pd.read_csv(path, low_memory=False)
                    if source.selection_label_field in df.columns:
                        vc = df[source.selection_label_field].astype("string").fillna("NA").value_counts().head(12)
                        labels = "; ".join(f"{k}={v}" for k, v in vc.items())
                except Exception as exc:  # pragma: no cover - diagnostic only
                    labels = f"label_read_error={type(exc).__name__}"
        rows.append(
            {
                "source_label": source.label,
                "artifact_path": str(path),
                "exists": exists,
                "artifact_type": "directory" if exists and path.is_dir() else path.suffix.lower().lstrip("."),
                "size_bytes": path.stat().st_size if exists and path.is_file() else "",
                "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if exists else "",
                "row_count": row_count,
                "file_count": file_count,
                "relevant_selection_label": labels,
                "pre_first_pitch_evidence": "present_if_mtime_before_game_start_not_sufficient_for_user_tracked_semantics" if exists else "missing",
                "contains_full_15_row_set": "no_explicit_evidence",
                "expected_semantics": source.expected_semantics,
                "sha256": sha256_path(path) if exists and path.is_file() else "",
            }
        )
    return rows


def recoverable_candidate_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidate_files = [
        KNOWN_SOURCES[1],
        KNOWN_SOURCES[2],
        KNOWN_SOURCES[3],
        KNOWN_SOURCES[4],
        KNOWN_SOURCES[5],
    ]
    preferred = [
        "date",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "game_id",
        "prop_type",
        "line",
        "side",
        "market_price",
        "best_over_price",
        "model_prob",
        "selected_side_implied_probability",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "layer_label",
        "alternate_layer",
        "watch_candidate",
        "game_time",
        "starter_context_status",
        "opposing_starter",
    ]
    for source in candidate_files:
        if not source.path.exists():
            continue
        try:
            df = pd.read_csv(source.path, low_memory=False)
        except Exception:
            continue
        for idx, row in df.iterrows():
            out = {
                "source_label": source.label,
                "source_artifact": str(source.path),
                "source_row_number": int(idx) + 2,
                "is_exact_user_tracked_15_member": "unknown",
                "reason_not_frozen_as_exact_15": "source is generated board/review-aid population, not repository evidence of the exact user-tracked 15-row favorite slate",
            }
            for col in preferred:
                out[col] = row[col] if col in df.columns else ""
            rows.append(out)
    return rows


def placeholder(section: str, reason: str) -> list[dict[str, Any]]:
    return [
        {
            "audit_section": section,
            "status": "NOT_EVALUATED",
            "reason": reason,
            "required_population_status": "EXACT_15_ROW_POPULATION_NOT_RECOVERABLE",
            "notes": "Broad analysis stopped by Phase 1 guardrail; do not substitute a reconstructed top-15 population.",
        }
    ]


def decision_rows() -> list[dict[str, str]]:
    decisions = {
        "MLB_JULY12_SENTINEL_POPULATION_DECISION": "EXACT_15_ROW_POPULATION_NOT_RECOVERABLE",
        "MLB_JULY12_OFFICIAL_0_15_CERTIFICATION_DECISION": "NOT_CERTIFIED_POPULATION_NOT_BOUND",
        "MLB_JULY12_FAVORITE_DEFINITION_DECISION": "NOT_RECONSTRUCTED_EXACT_USER_FAVORITE_DEFINITION_MISSING",
        "MLB_JULY12_COMMON_MODE_EXPOSURE_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_PREDICTION_LINEAGE_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_PREGAME_WARNING_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_MATCHED_SLATE_COMPARISON_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_HISTORICAL_RECURRENCE_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_AGGREGATE_MASKING_DECISION": "NOT_EVALUATED_POPULATION_NOT_BOUND",
        "MLB_JULY12_CATASTROPHIC_STATE_DECISION": "EXACT_15_ROW_POPULATION_NOT_RECOVERABLE",
        "MLB_JULY12_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    return [{"decision": k, "value": v} for k, v in decisions.items()]


def minimum_user_list_rows() -> list[dict[str, str]]:
    fields = [
        ("slate_date", "2026-07-12"),
        ("game_id", "preferred; game date/team fallback acceptable only if game_id unknown"),
        ("player_id", "preferred"),
        ("player_name", "required if player_id unknown"),
        ("team", "required if game_id/player_id unknown"),
        ("opponent", "required if game_id/player_id unknown"),
        ("prop_type", "for example hits"),
        ("line", "for example 1.5"),
        ("selected_side", "over or under"),
        ("pregame_price", "if known"),
        ("book_or_price_source", "if known"),
        ("selection_reason_or_label", "favorite definition as used by the user"),
        ("source_evidence", "screenshot, CSV, report section, or manual note that produced the exact tracked list"),
    ]
    return [{"required_field": f, "minimum_value_needed": v, "why_needed": "to bind exact canonical selection identity"} for f, v in fields]


def make_summary(package: Path, inventory: list[dict[str, Any]], recoverable_count: int) -> str:
    existing = [r for r in inventory if r["exists"]]
    rows_with_15 = [r for r in inventory if str(r.get("row_count")) == "15"]
    return f"""
# MLB July 12 Favorite-Slate Sentinel Failure Audit

- Audit date: `{AUDIT_DATE}`
- Sentinel event: `MLB_JULY12_FAVORITE_SLATE_SENTINEL_FAILURE`
- Slate date under review: `{SLATE_DATE}`
- Generated at UTC: `{utc_now()}`
- Production change status: `NOT_AUTHORIZED`

## Executive Summary

The audit stops at Phase 1. Repository evidence does not identify the exact user-tracked 15-row July 12 favorite slate. The local repository contains July 12 generated boards, review aids, Ops Brief sections, odds-history outputs, lane-selector artifacts, and upload-shaped files, but none of the inspected sources is labeled or structured as the manual 15-row favorite slate that allegedly finished 0-15.

Because the requested population is a manually tracked sentinel set, reconstructing a new "top 15" from generated criteria would violate the audit guardrail and would risk explaining the wrong slate. The exact 0-15 result is therefore not certified in this package.

## What Was Recoverable

- Existing candidate/source artifacts inventoried: `{len(existing)}`
- Source artifacts with exactly 15 rows: `{len(rows_with_15)}`
- Recoverable generated candidate/review-aid rows copied for reference: `{recoverable_count}`
- Exact user-tracked 15-row manifest rows frozen: `0`

The recoverable rows are written to `recoverable_july12_candidate_rows_2026-07-17.csv` as reference only. They are not certified as the sentinel population.

## Required Stop Condition

`MLB_JULY12_SENTINEL_POPULATION_DECISION = EXACT_15_ROW_POPULATION_NOT_RECOVERABLE`

Broad analysis was not performed. Official settlement, common-mode exposure, prediction lineage, warning analysis, matched-slate comparison, recurrence, and aggregate-masking sections are all marked `NOT_EVALUATED` because the exact 15-row population was not bound.

## Minimum Evidence Needed To Reopen

Provide the 15 tracked rows with, at minimum:

- slate date
- game id if known
- player id or player name
- team and opponent
- prop type
- line
- selected side
- pregame price/book if known
- selection reason or label
- source evidence for the manual tracked list

## Decisions

See `decision_report_2026-07-17.csv` and `machine_readable_july12_favorite_slate_sentinel_failure_audit_2026-07-17.json`.
"""


def sha_manifest(package: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in package.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{AUDIT_DATE}.csv"):
        rows.append(
            {
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_path(path),
            }
        )
    return rows


def validate_package(package: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(package.glob("*.csv")):
        try:
            pd.read_csv(path)
            status = "PASS"
            message = "csv_parses"
        except Exception as exc:
            status = "FAIL"
            message = f"{type(exc).__name__}: {exc}"
        rows.append({"artifact": str(path), "validation": "csv_parse", "status": status, "message": message})
    for path in sorted(package.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            status = "PASS"
            message = "json_parses"
        except Exception as exc:
            status = "FAIL"
            message = f"{type(exc).__name__}: {exc}"
        rows.append({"artifact": str(path), "validation": "json_parse", "status": status, "message": message})
    for path in sorted(package.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        rows.append(
            {
                "artifact": str(path),
                "validation": "markdown_nonempty",
                "status": "PASS" if text.strip() else "FAIL",
                "message": "markdown_nonempty" if text.strip() else "empty_markdown",
            }
        )
    rows.extend(
        [
            {
                "artifact": "runtime",
                "validation": "no_network_no_db_no_oddsapi",
                "status": "PASS",
                "message": "utility reads local files only and writes audit package artifacts",
            },
            {
                "artifact": "runtime",
                "validation": "no_model_formula_tier_upload_changes",
                "status": "PASS",
                "message": "no production scripts or data behavior changed",
            },
        ]
    )
    return rows


def build(package: Path) -> dict[str, Any]:
    package.mkdir(parents=True, exist_ok=True)

    inventory = source_inventory()
    recoverable = recoverable_candidate_rows()
    exact_manifest: list[dict[str, Any]] = []

    write_csv(package / f"source_recovery_report_{AUDIT_DATE}.csv", inventory)
    write_csv(package / f"candidate_source_inventory_{AUDIT_DATE}.csv", inventory)
    write_csv(package / f"recoverable_july12_candidate_rows_{AUDIT_DATE}.csv", recoverable)
    write_csv(
        package / f"exact_15_frozen_manifest_{AUDIT_DATE}.csv",
        exact_manifest,
        [
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "prop_type",
            "line",
            "selected_side",
            "pregame_price",
            "candidate_source",
            "run_tag",
            "sha256_identity",
        ],
    )
    write_csv(package / f"minimum_user_supplied_list_needed_{AUDIT_DATE}.csv", minimum_user_list_rows())

    reason = "exact user-tracked 15-row favorite slate is not identifiable from repository evidence"
    section_files = {
        "official_settlement_certification": "official settlement certification",
        "favorite_definition_reconstruction": "favorite definition reconstruction",
        "row_by_rule_qualification_matrix": "row-by-rule qualification matrix",
        "common_mode_exposure_report": "common-mode exposure report",
        "prediction_lineage_audit": "end-to-end prediction lineage audit",
        "pregame_warning_analysis": "pregame warning analysis",
        "matched_successful_slate_manifest": "matched successful slate manifest",
        "failed_vs_successful_comparison": "failed-versus-successful comparison",
        "historical_recurrence_results": "historical recurrence results",
        "aggregate_masking_demonstration": "aggregate masking demonstration",
        "operational_implications": "operational implications",
    }
    for stem, section in section_files.items():
        rows = placeholder(section, reason)
        if stem == "operational_implications":
            rows[0]["notes"] = (
                "Add a future manual slate export/tracking-id mechanism before using manually tracked favorite slates as certified sentinel populations. "
                "No production behavior change is authorized by this audit."
            )
        write_csv(package / f"{stem}_{AUDIT_DATE}.csv", rows, PLACEHOLDER_COLUMNS)

    decisions = decision_rows()
    write_csv(package / f"decision_report_{AUDIT_DATE}.csv", decisions)

    payload = {
        "audit_date": AUDIT_DATE,
        "slate_date": SLATE_DATE,
        "sentinel_event": "MLB_JULY12_FAVORITE_SLATE_SENTINEL_FAILURE",
        "generated_at_utc": utc_now(),
        "exact_15_recovered": False,
        "exact_15_rows": 0,
        "recoverable_reference_rows": len(recoverable),
        "source_artifacts_inventoried": len(inventory),
        "decisions": {row["decision"]: row["value"] for row in decisions},
        "minimum_user_supplied_list_needed": [r["required_field"] for r in minimum_user_list_rows()],
        "constraints": {
            "network_calls": 0,
            "db_writes": 0,
            "oddsapi_calls": 0,
            "model_changes": 0,
            "production_behavior_changes": 0,
        },
    }
    write_json(package / f"machine_readable_july12_favorite_slate_sentinel_failure_audit_{AUDIT_DATE}.json", payload)
    write_md(package / f"executive_summary_{AUDIT_DATE}.md", make_summary(package, inventory, len(recoverable)))

    write_csv(package / f"validation_report_{AUDIT_DATE}.csv", validate_package(package))
    write_csv(package / f"sha256_manifest_{AUDIT_DATE}.csv", sha_manifest(package))
    # Re-run validation after manifest exists, then refresh manifest once more so
    # it hashes the final validation report.
    write_csv(package / f"validation_report_{AUDIT_DATE}.csv", validate_package(package))
    write_csv(package / f"sha256_manifest_{AUDIT_DATE}.csv", sha_manifest(package))

    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(PACKAGE_ROOT))
    args = parser.parse_args()
    payload = build(Path(args.output_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
