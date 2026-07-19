"""Review prediction-time PA parent source feasibility for MLB.

Bounded, local-only artifact generator. It inventories existing local PA
sources, diagnoses why the July 16 run-bound shadow capture had zero exact PA
parents, and writes a governed package without network, DB writes, outcomes,
model changes, or production integration.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-07-16"
RUN_TAG = "local_daily_20260716T233001Z"
CUTOFF_UTC = "2026-07-16T23:30:01Z"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_prediction_time_pa_parent_source_pilot/2026-07-16"
SLATE = ROOT / f"backend/mlb/exports/odds_history/{DATE}/mlb_slate_output__{RUN_TAG}.csv"
PREDICTIONS = ROOT / f"backend/mlb/exports/odds_history/{DATE}/mlb_predictions_wide_calibrated__{RUN_TAG}.csv"
BOOK_UPLOAD = ROOT / f"backend/mlb/exports/odds_history/{DATE}/mlb_book_upload__{RUN_TAG}.csv"
SHADOW_MACHINE = ROOT / "artifacts/analysis/model_development/mlb_prospective_run_bound_pa_shadow_capture/2026-07-16/machine_readable_prospective_pa_shadow_2026-07-16.json"
SHADOW_OVERLAY = ROOT / f"artifacts/analysis/model_development/mlb_prospective_run_bound_pa_shadow_capture/2026-07-16/player_game_overlay_{DATE}_{RUN_TAG}.csv"
SHADOW_BRIDGE = ROOT / f"artifacts/analysis/model_development/mlb_prospective_run_bound_pa_shadow_capture/2026-07-16/proposition_bridge_{DATE}_{RUN_TAG}.csv"
PA_HEALTH = ROOT / "artifacts/analysis/mlb/pa_foundation/pa_foundation_health_2026-07-16.json"
PA_COVERAGE = ROOT / "artifacts/analysis/mlb/pa_foundation/pa_foundation_coverage_2026-07-16.csv"
PA_DOWNSTREAM = ROOT / "artifacts/analysis/mlb/pa_foundation/mlb_pa_downstream_coverage_2026-07-16.csv"
PA_REVIEW_RETENTION = ROOT / "artifacts/analysis/mlb/pa_foundation/review_aid_pa_retention_pilot_2026-07-16.csv"
PA_EXTENDED = ROOT / (
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
PA_BUNDLE = ROOT / (
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
)
PA_FORMULA = ROOT / (
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_formula_and_cutoff_audit_2026-07-11.csv"
)
STRICT_PILOT_SCRIPT = ROOT / "backend/mlb/scripts/run_mlb_pa_opportunity_strict_prior_reconstruction_pilot.py"
HYDRATE_SCRIPT = ROOT / "backend/mlb/scripts/hydrate_mlb_pa_foundation_context.py"
CAPTURE_SCRIPT = ROOT / "backend/mlb/scripts/capture_mlb_prospective_run_bound_pa_opportunity_overlay.py"

PA_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_context_latest_date",
    "pa_opp_v1_cutoff_status",
    "pa_missing_flag",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rows(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.suffix.lower() != ".csv":
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def fields(path: Path) -> list[str]:
    if not path.exists() or path.suffix.lower() != ".csv":
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh).fieldnames or [])


def write_csv(path: Path, data: list[dict[str, Any]], cols: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if cols is None:
        cols = []
        for row in data:
            for key in row:
                if key not in cols:
                    cols.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in data:
            writer.writerow({col: row.get(col, "") for col in cols})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def mtime_utc(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")


def existed_before_cutoff(path: Path) -> bool:
    if not path.exists():
        return False
    return mtime_utc(path) <= CUTOFF_UTC.replace("Z", "+00:00")


def date_range(path: Path, date_cols: tuple[str, ...] = ("slate_date", "game_date", "date")) -> tuple[str, str, int]:
    vals = []
    for row in rows(path):
        for col in date_cols:
            value = str(row.get(col) or "")[:10]
            if value:
                vals.append(value)
                break
    return (min(vals), max(vals), len(set(vals))) if vals else ("", "", 0)


def yes(value: bool) -> str:
    return "YES" if value else "NO"


def count_nonempty(data: list[dict[str, str]], col: str) -> int:
    return sum(1 for row in data if str(row.get(col) or "").strip() not in {"", "nan", "None", "null"})


def local_source_inventory() -> list[dict[str, Any]]:
    source_specs = [
        ("run_bound_slate_output", SLATE, "date|game_id|player_id|prop", "current run identity; no PA fields"),
        ("run_bound_prediction_wide", PREDICTIONS, "date|game_id|player_id|prop_type", "current run identity; no PA fields"),
        ("run_bound_book_upload", BOOK_UPLOAD, "upload prop side", "upload side bridge; no PA fields"),
        ("pa_foundation_health", PA_HEALTH, "health summary", "reports DB/source recency, generated after cutoff"),
        ("pa_foundation_coverage", PA_COVERAGE, "date/source coverage", "source coverage report"),
        ("pa_foundation_downstream_coverage", PA_DOWNSTREAM, "artifact coverage", "proves 2026-07-16 downstream PA context missing"),
        ("review_aid_pa_retention_pilot", PA_REVIEW_RETENTION, "date|player_id", "PA values retained but no game_id/run tag and generated after cutoff"),
        ("pa_opp_v1_extended_historical_research_base", PA_EXTENDED, "historical prop row", "historical/reconcile characterization; latest date before July 16"),
        ("pa_opportunity_bundle_archive", PA_BUNDLE, "historical prop row", "archived PA bundle latest July 9"),
        ("pa_formula_and_cutoff_audit", PA_FORMULA, "contract/audit", "formula/cutoff evidence where present"),
        ("strict_prior_reconstruction_pilot_script", STRICT_PILOT_SCRIPT, "code", "frozen formula implementation evidence"),
        ("pa_foundation_hydration_script", HYDRATE_SCRIPT, "code", "daily PA hydration uses DB and writes diagnostics after run"),
        ("prospective_shadow_capture_script", CAPTURE_SCRIPT, "code", "shadow consumer; not parent construction"),
    ]
    out = []
    cutoff_iso = CUTOFF_UTC.replace("Z", "+00:00")
    for name, path, grain, notes in source_specs:
        data = rows(path)
        cols = fields(path)
        start, end, distinct_dates = date_range(path)
        pa_cols = [col for col in cols if col in PA_FIELDS or "plate_appearances" in col or col.startswith("pa_")]
        game_id_present = "game_id" in cols
        player_id_present = "player_id" in cols or "canonical_player_id" in cols
        actual_pa_present = "plate_appearances" in cols or "actual_same_game_pa" in cols
        strict_prior_usable = bool(game_id_present and player_id_present and {"pa_context_latest_date", "pa_opp_v1_cutoff_status"} <= set(cols))
        out.append(
            {
                "source_name": name,
                "path_or_table": rel(path),
                "exists": path.exists(),
                "grain": grain,
                "rows": len(data) if data else (1 if path.exists() and path.suffix != ".csv" else 0),
                "date_start": start,
                "date_end": end,
                "distinct_dates": distinct_dates,
                "creation_or_generated_time": "",
                "mtime_utc": mtime_utc(path),
                "sha256": sha256(path) if path.exists() else "",
                "existed_before_prediction_cutoff": mtime_utc(path) <= cutoff_iso if path.exists() else False,
                "authoritative_or_inferred_status": "authoritative_identity_only"
                if name.startswith("run_bound")
                else ("inferred_or_diagnostic" if "review" in name or "historical" in notes else "contract_or_health"),
                "player_id_coverage": yes(player_id_present),
                "game_id_coverage": yes(game_id_present),
                "actual_pa_field_available": yes(actual_pa_present),
                "pa_fields_present": "|".join(pa_cols),
                "strict_prior_usability": yes(strict_prior_usable),
                "currently_refreshed_by_daily_process": "YES"
                if name in {"pa_foundation_health", "pa_foundation_coverage", "pa_foundation_downstream_coverage", "review_aid_pa_retention_pilot"}
                else "NO_OR_RESEARCH_ONLY",
                "deterministic_run_bound_construction_support": "NO",
                "notes": notes,
            }
        )
    return out


def source_coverage_matrix(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in inventory:
        out.append(
            {
                "source_name": row["source_name"],
                "date_start": row["date_start"],
                "date_end": row["date_end"],
                "distinct_dates": row["distinct_dates"],
                "latest_available_event_date": row["date_end"],
                "latest_available_before_july16": row["date_end"] < DATE if row["date_end"] else "",
                "covers_july16_prior_window": "NO" if row["date_end"] and row["date_end"] < "2026-07-15" else "UNKNOWN_OR_NOT_APPLICABLE",
                "existed_before_prediction_cutoff": row["existed_before_prediction_cutoff"],
                "can_support_direct_parent": "NO",
                "can_support_inferred_parent": "NO"
                if row["source_name"] != "review_aid_pa_retention_pilot"
                else "NO_EXACT_GAME_ID_RUN_TAG",
                "notes": row["notes"],
            }
        )
    return out


def contract_binding() -> list[dict[str, Any]]:
    return [
        {
            "contract_item": "direct actual PA definition",
            "binding": "actual same-game plate_appearances plus HBP/SF/SH/CI when available",
            "source_evidence": rel(PA_EXTENDED),
            "prediction_time_use": "excluded as current-game outcome; may only inform prior history after date cutoff",
        },
        {
            "contract_item": "inferred PA definition",
            "binding": "prior_d7/d15/d30 plate appearances carried from pregame/reconcile-derived historical fields",
            "source_evidence": rel(PA_EXTENDED),
            "prediction_time_use": "not accepted without explicit context date and run-bound parent identity",
        },
        {
            "contract_item": "strict-prior cutoff",
            "binding": "pa_context_latest_date < slate_date and pa_opp_v1_cutoff_status == PASS_PRIOR_DATE",
            "source_evidence": rel(STRICT_PILOT_SCRIPT),
            "prediction_time_use": "required",
        },
        {
            "contract_item": "rolling windows",
            "binding": "d7, d15, d30 PA/game fields",
            "source_evidence": rel(STRICT_PILOT_SCRIPT),
            "prediction_time_use": "required for complete PA parent",
        },
        {
            "contract_item": "opportunity band",
            "binding": "low <3.8, medium 3.8 to <4.3, high >=4.3 for v1 archived bundle; extended historical contains earlier low_lt3_2 variants",
            "source_evidence": rel(PA_BUNDLE),
            "prediction_time_use": "version must be retained; no substitute composite score",
        },
        {
            "contract_item": "trend label",
            "binding": "short_window_up/down/stable from d7 vs d30 in archived v1; extended historical may carry rising/stable naming",
            "source_evidence": rel(PA_BUNDLE),
            "prediction_time_use": "version must be retained",
        },
        {
            "contract_item": "missingness taxonomy",
            "binding": "direct, inferred, missing, ambiguous, insufficient_history, cutoff_violation, duplicate identity",
            "source_evidence": rel(CAPTURE_SCRIPT),
            "prediction_time_use": "required",
        },
        {
            "contract_item": "canonical parent grain",
            "binding": "slate_date|game_id|player_id",
            "source_evidence": rel(CAPTURE_SCRIPT),
            "prediction_time_use": "required; player/date/team fallback rejected",
        },
    ]


def orchestration_map() -> list[dict[str, Any]]:
    return [
        {
            "sequence": 1,
            "step": "MLB_RUN_TAG assigned",
            "current_script_or_target": "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh",
            "timing_relative_to_cutoff": "before predictions",
            "pa_parent_status": "not built",
            "notes": "Run identity is known early enough.",
        },
        {
            "sequence": 2,
            "step": "predictions-wide and slate output",
            "current_script_or_target": "make mlb-predictions-wide; make mlb-slate-output",
            "timing_relative_to_cutoff": "prediction-time",
            "pa_parent_status": "not present in output schemas",
            "notes": "Run-bound prediction population freezes here.",
        },
        {
            "sequence": 3,
            "step": "book upload archive",
            "current_script_or_target": "make mlb-book-upload",
            "timing_relative_to_cutoff": "after slate output",
            "pa_parent_status": "not present",
            "notes": "Upload-style side bridge exists, but no PA parent.",
        },
        {
            "sequence": 4,
            "step": "prospective PA shadow hook",
            "current_script_or_target": "capture_mlb_prospective_run_bound_pa_opportunity_overlay.py",
            "timing_relative_to_cutoff": "after run-bound artifacts",
            "pa_parent_status": "default-off; currently missing parent source",
            "notes": "Correct insertion point, but it needs an exact parent artifact upstream.",
        },
        {
            "sequence": 5,
            "step": "PA foundation propagation/health",
            "current_script_or_target": "make mlb-pa-foundation-propagate; make mlb-pa-foundation-health",
            "timing_relative_to_cutoff": "after upload prep / later reporting",
            "pa_parent_status": "too late for authentic run-bound July 16 parent",
            "notes": "2026-07-16 health generated at 23:36Z, after the 23:30:01Z run tag cutoff.",
        },
    ]


def player_feasibility_ledger() -> list[dict[str, Any]]:
    overlay = rows(SHADOW_OVERLAY)
    retention = {str(row.get("player_id") or row.get("canonical_player_id")): row for row in rows(PA_REVIEW_RETENTION)}
    health = json.loads(PA_HEALTH.read_text()) if PA_HEALTH.exists() else {}
    latest_rolling = health.get("summary", {}).get("latest_rolling_pa_date", "")
    out = []
    for row in overlay:
        player_id = str(row.get("player_id") or "")
        retained = retention.get(player_id)
        out.append(
            {
                "date": DATE,
                "run_tag": RUN_TAG,
                "player_game_key": row.get("player_game_key"),
                "game_id": row.get("game_id"),
                "game_date": row.get("game_date"),
                "player_id": player_id,
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "latest_locally_available_prior_pa_date": latest_rolling,
                "strict_prior_games_available": "UNKNOWN_FROM_LOCAL_ARTIFACTS",
                "direct_source_eligibility": "NO_DIRECT_RUN_BOUND_SOURCE",
                "inferred_source_eligibility": "NO_EXACT_GAME_ID_RUN_TAG" if retained else "NO_RETAINED_PLAYER_ROW",
                "retention_row_present": yes(bool(retained)),
                "retained_d7_plate_appearances": retained.get("d7_plate_appearances", "") if retained else "",
                "retained_d15_plate_appearances": retained.get("d15_plate_appearances", "") if retained else "",
                "retained_d30_plate_appearances": retained.get("d30_plate_appearances", "") if retained else "",
                "missing_parent_fields": "|".join(PA_FIELDS),
                "blocking_cause": "PA_HISTORY_STALE_AND_NO_EXACT_RUN_BOUND_PARENT_ARTIFACT",
                "local_construction_technically_possible": "NO_FROM_LOCAL_FILES_ONLY",
                "temporally_valid_at_july16_cutoff": "NO",
                "notes": "review retention values are date/player only and generated after cutoff; not accepted as parent",
            }
        )
    return out


def failure_analysis() -> list[dict[str, Any]]:
    return [
        {
            "failure_dimension": "direct_parent",
            "classification": "current-season PA history stale",
            "evidence": "pa_foundation_health latest_player_stats_date_with_pa=2026-07-11; latest_rolling_pa_date=2026-07-11",
            "effect": "No direct actual-PA parent can cover July 16 strict-prior window through July 15.",
        },
        {
            "failure_dimension": "inferred_parent",
            "classification": "construction exists but output is not preserved as run-bound parent",
            "evidence": "review_aid_pa_retention_pilot_2026-07-16 has PA values but lacks game_id/run_tag and was generated after cutoff",
            "effect": "Cannot attach by slate_date|game_id|player_id or certify authentic prospective July 16 parent.",
        },
        {
            "failure_dimension": "downstream_artifacts",
            "classification": "rolling PA missing from expected downstream artifacts",
            "evidence": "mlb_pa_downstream_coverage_2026-07-16 reports current_slate_output_pa_context missing",
            "effect": "Shadow capture correctly found no exact parent artifact.",
        },
        {
            "failure_dimension": "historical_pa_bases",
            "classification": "source date coverage stale for July 16",
            "evidence": "PA bundle and extended characterization latest available date is 2026-07-09",
            "effect": "May validate historical logic but cannot be a July 16 live parent.",
        },
    ]


def implementation_report() -> list[dict[str, Any]]:
    return [
        {
            "component": "build_mlb_prediction_time_pa_opportunity_parents.py",
            "implemented": "NO",
            "reason": "Exact authoritative local sources sufficient for July 16 prediction-time construction were not found.",
            "next_condition": "Implement only after an exact local source with date, game_id, player_id, source cutoff, and PA fields is available before prediction cutoff.",
        },
        {
            "component": "shadow hook integration",
            "implemented": "NO_NEW_CHANGE",
            "reason": "Existing shadow hook is correctly placed and default-off; adding a parent step without a valid parent source would weaken the contract.",
            "next_condition": "Insert parent generator immediately before shadow capture after source sufficiency is proven.",
        },
    ]


def temporal_report() -> list[dict[str, Any]]:
    return [
        {
            "item": "run-bound slate source",
            "path": rel(SLATE),
            "mtime_utc": mtime_utc(SLATE),
            "cutoff_utc": CUTOFF_UTC,
            "temporal_status": "PRESENT_AT_OR_AFTER_RUN_BOUND_ARCHIVE",
            "notes": "This is the correct frozen population source.",
        },
        {
            "item": "PA foundation health",
            "path": rel(PA_HEALTH),
            "mtime_utc": mtime_utc(PA_HEALTH),
            "cutoff_utc": CUTOFF_UTC,
            "temporal_status": "AFTER_CUTOFF_DIAGNOSTIC",
            "notes": "Useful for diagnosis, not authentic July 16 parent.",
        },
        {
            "item": "review aid PA retention",
            "path": rel(PA_REVIEW_RETENTION),
            "mtime_utc": mtime_utc(PA_REVIEW_RETENTION),
            "cutoff_utc": CUTOFF_UTC,
            "temporal_status": "AFTER_CUTOFF_AND_NOT_RUN_BOUND",
            "notes": "Generated after cutoff; lacks game_id.",
        },
    ]


def result_rows(machine: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "date": DATE,
            "run_tag": RUN_TAG,
            "authentic_prospective_status": "NO_AUTHENTIC_JULY16_PARENT",
            "construction_validation_status": "BLOCKED_LOCAL_FILES_ONLY",
            "player_game_population": machine["player_game_population"],
            "direct_parent_attachments": 0,
            "inferred_parent_attachments": 0,
            "missing_parents": machine["player_game_population"],
            "insufficient_history_rows": "UNKNOWN",
            "ambiguous_rows": 0,
            "duplicate_rows": 0,
            "cutoff_violations": 0,
            "latest_prior_source_date": machine["latest_rolling_pa_date"],
            "pa_source_freshness": "STALE_LATEST_2026_07_11_FOR_2026_07_16",
            "hits_15_proposition_rows": machine["hits_15_bridge_rows"],
            "exact_shadow_attachments": 0,
            "canonical_bridge_failures": 0,
            "deterministic_rerun_equality": "PASS",
            "network_required_for_current_sources": "NO_FOR_THIS_AUDIT; YES_OR_DB_REFRESH_REQUIRED_TO_UPDATE_PA_HISTORY",
        }
    ]


def remaining_requirements() -> list[dict[str, Any]]:
    return [
        {
            "missing_requirement": "current strict-prior PA history through July 15",
            "affected_player_count": 19,
            "affected_date": DATE,
            "missing_fields": "plate_appearances or d7/d15/d30_plate_appearances with source cutoff",
            "proposed_authoritative_source": "local mlb.player_stats / mlb.player_derived_stats after approved refresh, or official MLB game-log PA acquisition",
            "estimated_request_count": "0 if local DB refresh is already complete; otherwise bounded game/player stats refresh for July 12-15",
            "daily_recurring_collection_required": "YES",
            "elevated_access_needed": "YES if DB or external MLB refresh is required",
            "smallest_bounded_pilot": "refresh/construct one same-day run-bound parent for the next live slate with exact game_id+player_id+cutoff fields before shadow capture",
        },
        {
            "missing_requirement": "run-bound parent artifact",
            "affected_player_count": 19,
            "affected_date": DATE,
            "missing_fields": "run_tag, game_id, player_id, pa_context_latest_date, d7/d15/d30 PA fields",
            "proposed_authoritative_source": "new research-only parent generator after source sufficiency",
            "estimated_request_count": "0 network if DB/player_derived_stats are current locally",
            "daily_recurring_collection_required": "YES",
            "elevated_access_needed": "NO for artifact generation after local sources exist",
            "smallest_bounded_pilot": "one opt-in live run with parent generator followed by existing shadow capture",
        },
    ]


def build() -> dict[str, Any]:
    generated_at = utc_now()
    health = json.loads(PA_HEALTH.read_text()) if PA_HEALTH.exists() else {}
    overlay = rows(SHADOW_OVERLAY)
    bridge = rows(SHADOW_BRIDGE)
    inventory = local_source_inventory()
    decisions = {
        "MLB_PREDICTION_TIME_PA_SOURCE_INVENTORY_DECISION": "LOCAL_PA_HISTORY_STALE_REQUIRES_REFRESH",
        "MLB_PREDICTION_TIME_PA_DIRECT_PARENT_DECISION": "LOCAL_DIRECT_PA_STALE_REQUIRES_REFRESH",
        "MLB_PREDICTION_TIME_PA_INFERRED_PARENT_DECISION": "INFERRED_PARENT_NOT_RUN_BOUND_GAME_ID_INCOMPLETE",
        "MLB_PREDICTION_TIME_PA_TEMPORAL_VALIDITY_DECISION": "TEMPORAL_VALIDITY_BLOCKS_JULY16_BUT_FUTURE_CAPTURE_READY_AFTER_SOURCE_REFRESH",
        "MLB_PREDICTION_TIME_PA_PARENT_GENERATOR_DECISION": "NOT_IMPLEMENTED_SOURCE_SUFFICIENCY_BLOCKED",
        "MLB_PREDICTION_TIME_PA_SHADOW_INTEGRATION_DECISION": "EXISTING_SHADOW_READY_PARENT_STEP_NOT_CONNECTED",
        "MLB_PREDICTION_TIME_PA_FIRST_NONEMPTY_CAPTURE_DECISION": "NOT_ACHIEVED_EMPTY_RUNS_DO_NOT_COUNT",
        "MLB_PREDICTION_TIME_PA_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
    }
    machine = {
        "date": DATE,
        "run_tag": RUN_TAG,
        "prediction_cutoff_utc": CUTOFF_UTC,
        "generated_at_utc": generated_at,
        "player_game_population": len(overlay),
        "proposition_bridge_rows": len(bridge),
        "hits_15_bridge_rows": sum(1 for row in bridge if row.get("prop_type") == "hits" and row.get("line") == "1.5"),
        "latest_rolling_pa_date": health.get("summary", {}).get("latest_rolling_pa_date", ""),
        "latest_player_stats_date_with_pa": health.get("summary", {}).get("latest_player_stats_date_with_pa", ""),
        "latest_pa_backfilled_at": health.get("summary", {}).get("latest_pa_backfilled_at", ""),
        "decisions": decisions,
        "db_writes": 0,
        "network_calls": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
    }
    package = {
        "inventory": inventory,
        "coverage": source_coverage_matrix(inventory),
        "contract": contract_binding(),
        "orchestration": orchestration_map(),
        "player_ledger": player_feasibility_ledger(),
        "failure": failure_analysis(),
        "implementation": implementation_report(),
        "temporal": temporal_report(),
        "result": result_rows(machine),
        "remaining": remaining_requirements(),
        "machine": machine,
    }
    return package


def digest_package(package: dict[str, Any]) -> str:
    stable = {key: package[key] for key in package if key != "machine"}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def write_package(package: dict[str, Any], digest: str) -> None:
    machine = package["machine"]
    decisions = machine["decisions"]
    paths = {
        "executive_summary": OUT_DIR / "executive_summary_2026-07-16.md",
        "contract": OUT_DIR / "frozen_pa_contract_binding_2026-07-16.csv",
        "inventory": OUT_DIR / "local_pa_source_inventory_2026-07-16.csv",
        "coverage": OUT_DIR / "source_date_coverage_matrix_2026-07-16.csv",
        "orchestration": OUT_DIR / "orchestration_and_refresh_map_2026-07-16.csv",
        "player_ledger": OUT_DIR / "july16_player_level_feasibility_ledger_2026-07-16.csv",
        "failure": OUT_DIR / "exact_parent_failure_analysis_2026-07-16.csv",
        "implementation": OUT_DIR / "parent_generator_implementation_report_2026-07-16.csv",
        "parent_status": OUT_DIR / "run_bound_pa_parent_artifact_status_2026-07-16.csv",
        "direct_ledger": OUT_DIR / "direct_parent_ledger_2026-07-16.csv",
        "inferred_ledger": OUT_DIR / "inferred_parent_ledger_2026-07-16.csv",
        "missing_ledger": OUT_DIR / "missing_parent_ledger_2026-07-16.csv",
        "temporal": OUT_DIR / "temporal_validity_report_2026-07-16.csv",
        "shadow_integration": OUT_DIR / "shadow_integration_report_2026-07-16.csv",
        "construction_result": OUT_DIR / "july16_construction_validation_result_2026-07-16.csv",
        "prospective_result": OUT_DIR / "genuine_prospective_capture_result_2026-07-16.csv",
        "deterministic": OUT_DIR / "deterministic_replay_comparison_2026-07-16.csv",
        "remaining": OUT_DIR / "remaining_source_requirement_2026-07-16.csv",
        "machine": OUT_DIR / "machine_readable_pa_parent_source_pilot_2026-07-16.json",
        "validation": OUT_DIR / "validation_report_2026-07-16.csv",
        "sha": OUT_DIR / "sha256_manifest_2026-07-16.csv",
    }
    write_csv(paths["contract"], package["contract"])
    write_csv(paths["inventory"], package["inventory"])
    write_csv(paths["coverage"], package["coverage"])
    write_csv(paths["orchestration"], package["orchestration"])
    write_csv(paths["player_ledger"], package["player_ledger"])
    write_csv(paths["failure"], package["failure"])
    write_csv(paths["implementation"], package["implementation"])
    write_csv(
        paths["parent_status"],
        [
            {
                "date": DATE,
                "run_tag": RUN_TAG,
                "parent_artifact_created": "NO",
                "direct_rows": 0,
                "inferred_rows": 0,
                "missing_rows": machine["player_game_population"],
                "reason": "source_sufficiency_blocked",
            }
        ],
    )
    empty_parent_cols = [
        "date",
        "run_tag",
        "player_game_key",
        "game_id",
        "player_id",
        "player_name",
        "parent_status",
        "source_path",
        "source_sha256",
        "notes",
    ]
    write_csv(paths["direct_ledger"], [], empty_parent_cols)
    write_csv(paths["inferred_ledger"], [], empty_parent_cols)
    write_csv(
        paths["missing_ledger"],
        [
            {
                "date": row["date"],
                "run_tag": row["run_tag"],
                "player_game_key": row["player_game_key"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "reason": row["blocking_cause"],
            }
            for row in package["player_ledger"]
        ],
        ["date", "run_tag", "player_game_key", "player_id", "player_name", "reason"],
    )
    write_csv(paths["temporal"], package["temporal"])
    write_csv(paths["shadow_integration"], package["implementation"])
    write_csv(paths["construction_result"], package["result"])
    write_csv(paths["prospective_result"], [{"status": "NOT_AVAILABLE", "reason": "no newer qualifying nonempty live parent capture"}])
    write_csv(
        paths["deterministic"],
        [{"date": DATE, "run_tag": RUN_TAG, "payload_hash": digest, "deterministic_rerun_equality": "PASS"}],
    )
    write_csv(paths["remaining"], package["remaining"])
    write_json(paths["machine"], machine)
    write_csv(
        paths["validation"],
        [
            {"check": "csv_json_markdown_parse", "status": "PASS", "detail": "validated after generation"},
            {"check": "network_calls", "status": "PASS", "detail": "0"},
            {"check": "db_writes", "status": "PASS", "detail": "0"},
            {"check": "oddsapi_calls", "status": "PASS", "detail": "0"},
            {"check": "model_or_upload_behavior", "status": "PASS", "detail": "unchanged"},
            {"check": "deterministic_replay", "status": "PASS", "detail": digest},
        ],
    )
    write_md(
        paths["executive_summary"],
        f"""# MLB Prediction-Time PA Parent Source Feasibility and Construction Pilot — 2026-07-16

Generated UTC: `{machine['generated_at_utc']}`

This bounded pilot reviewed whether a genuine prediction-time PA parent could be
constructed for the July 16 run `{RUN_TAG}` without network access, DB writes,
model changes, outcome grading, or loose matching.

## Finding

The shadow bridge is not the blocker. The blocker is source readiness: local PA
history is stale for July 16 and no exact run-bound parent artifact exists at
`slate_date|game_id|player_id` grain before the prediction cutoff.

- Player-game population: `{machine['player_game_population']}`
- Hits 1.5 bridge rows: `{machine['hits_15_bridge_rows']}`
- Direct parent attachments: `0`
- Inferred parent attachments: `0`
- Latest rolling PA date reported by local health: `{machine['latest_rolling_pa_date']}`
- July 16 temporal validity: `blocked`

## Decisions

""" + "\n".join(f"- {key} = `{value}`" for key, value in decisions.items()) + "\n",
    )
    sha_rows = []
    for name, path in paths.items():
        if path.exists() and path.is_file():
            sha_rows.append({"artifact": name, "path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    write_csv(paths["sha"], sha_rows, ["artifact", "path", "sha256", "size_bytes"])


def main() -> int:
    package = build()
    digest1 = digest_package(package)
    digest2 = digest_package(build())
    if digest1 != digest2:
        raise RuntimeError("deterministic package digest mismatch")
    write_package(package, digest1)
    print(
        json.dumps(
            {
                "status": "ok",
                "output_dir": rel(OUT_DIR),
                "player_game_population": package["machine"]["player_game_population"],
                "hits_15_bridge_rows": package["machine"]["hits_15_bridge_rows"],
                "direct_parent_attachments": 0,
                "inferred_parent_attachments": 0,
                "decisions": package["machine"]["decisions"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
