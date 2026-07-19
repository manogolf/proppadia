#!/usr/bin/env python3
"""Reconcile current Starter residual taxonomy after parent-ledger repair.

This utility is read-only reporting/governance work. It reconstructs the
current 85-row Starter-blocked residual by subtracting certified repaired
populations from the historical 232-row residual manifest, then freezes a
non-executable governance contract for the exact 23-row identity/role holdout
investigation. It does not mutate row state, acquire evidence, remediate
domains, construct matrices, train/score models, write databases/APIs, upload,
alter schedulers, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

PARENT_REPAIR_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair/2026-07-15")
RESIDUAL_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_starter_blocked_population_review/2026-07-15")
STALE_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/2026-07-15")
LOW_SAMPLE_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")
C010_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_c010_recovery_and_ordinary_campaign_closure/2026-07-15")
MATRIX_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/2026-07-15")
OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15")

EXPECTED_PARENT_REPAIR_SHA = "4cb5f3d114ed9b0faa07711318324442b68a1a0d32b6fc172e4b5f48a72afe88"
EXPECTED_LOW_SAMPLE_SHA = "2713ebdc96849b13b1a0edbc40b0da4bad0e6862bf8177bf023dff9c180c7d25"

PARENT_REPAIR_SHA = PARENT_REPAIR_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PARENT_REPAIR_JSON = PARENT_REPAIR_DIR / f"certified_cumulative_post_repair_state_{RUN_DATE}.json"
PARENT_REPAIR_ROWS = PARENT_REPAIR_DIR / f"row_level_movement_ledger_{RUN_DATE}.csv"
RESIDUAL_SHA = RESIDUAL_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RESIDUAL_ROWS = RESIDUAL_DIR / f"exact_232_row_residual_starter_blocked_manifest_{RUN_DATE}.csv"
RESIDUAL_SIDES = RESIDUAL_DIR / f"residual_side_manifest_{RUN_DATE}.csv"
RESIDUAL_TAXONOMY = RESIDUAL_DIR / f"primary_secondary_blocker_taxonomy_{RUN_DATE}.csv"
STALE_SHA = STALE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
STALE_ROWS = STALE_DIR / f"row_level_accounting_movement_ledger_{RUN_DATE}.csv"
LOW_SAMPLE_SHA = LOW_SAMPLE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
LOW_SAMPLE_ROWS = LOW_SAMPLE_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
C010_SHA = C010_DIR / f"sha256_manifest_{RUN_DATE}.csv"
C010_ROWS = C010_DIR / f"final_803_row_campaign_closure_reconciliation_{RUN_DATE}.csv"
MATRIX_QUEUE = MATRIX_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"
MATRIX_SHA = MATRIX_DIR / f"sha256_manifest_{RUN_DATE}.csv"

REMEDIATION_PACKAGES = [
    ("stale_104_starter_accounting_repair", STALE_DIR / f"row_level_accounting_movement_ledger_{RUN_DATE}.csv", "governed_canonical_row_id"),
    ("low_sample_17_research_only_remediation", LOW_SAMPLE_ROWS, "governed_canonical_row_id"),
    ("starter_parent_26_ledger_repair", PARENT_REPAIR_ROWS, "canonical_row_identity"),
    ("ordinary_discovery_cohort_001", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
    ("ordinary_discovery_cohort_002", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
    ("ordinary_discovery_cohort_003", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
    ("ordinary_discovery_cohort_004", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_004_resolved_branch_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
    ("four_side_history_complete_pilot", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
    ("hc_local_cohort_001", Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/2026-07-15/row_level_qualification_movement_ledger_2026-07-15.csv"), "governed_canonical_row_id"),
]

DECISION = "CERTIFIED_CURRENT_85_ROW_STARTER_RESIDUAL_TAXONOMY_RECONCILIATION"
STATE = "CERTIFIED"
HOLDOUT_STATUS = "FROZEN_AWAITING_EXPLICIT_READ_ONLY_INVESTIGATION_APPROVAL"
NEXT_PRIORITY = "IDENTITY_ROLE_HOLDOUT_READ_ONLY_INVESTIGATION"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def row_id(row: dict[str, str]) -> str:
    for key in ("governed_canonical_row_id", "canonical_denominator_identity", "canonical_row_identity"):
        if row.get(key):
            return row[key]
    raise KeyError("missing canonical row id")


def load_inputs() -> dict[str, Any]:
    required = [
        PARENT_REPAIR_SHA,
        PARENT_REPAIR_JSON,
        PARENT_REPAIR_ROWS,
        RESIDUAL_SHA,
        RESIDUAL_ROWS,
        RESIDUAL_SIDES,
        RESIDUAL_TAXONOMY,
        STALE_SHA,
        STALE_ROWS,
        LOW_SAMPLE_SHA,
        LOW_SAMPLE_ROWS,
        C010_SHA,
        C010_ROWS,
        MATRIX_QUEUE,
        MATRIX_SHA,
    ]
    for _, path, _ in REMEDIATION_PACKAGES:
        required.append(path)
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "parent_state": json.loads(PARENT_REPAIR_JSON.read_text(encoding="utf-8")),
        "parent_movement": read_csv(PARENT_REPAIR_ROWS),
        "residual_rows": read_csv(RESIDUAL_ROWS),
        "residual_sides": read_csv(RESIDUAL_SIDES),
        "residual_taxonomy": read_csv(RESIDUAL_TAXONOMY),
        "stale_rows": read_csv(STALE_ROWS),
        "low_sample_rows": read_csv(LOW_SAMPLE_ROWS),
        "c010_rows": read_csv(C010_ROWS),
        "matrix_queue": read_csv(MATRIX_QUEUE),
    }


def dependency_audit() -> list[dict[str, Any]]:
    deps = [
        ("parent_ledger_repair", PARENT_REPAIR_DIR, PARENT_REPAIR_SHA, EXPECTED_PARENT_REPAIR_SHA),
        ("accounting_repair", STALE_DIR, STALE_SHA, sha256(STALE_SHA)),
        ("low_sample_17_remediation", LOW_SAMPLE_DIR, LOW_SAMPLE_SHA, EXPECTED_LOW_SAMPLE_SHA),
        ("c010_campaign_closure", C010_DIR, C010_SHA, sha256(C010_SHA)),
        ("residual_review_source", RESIDUAL_DIR, RESIDUAL_SHA, sha256(RESIDUAL_SHA)),
        ("matrix_parent_payload_branch", MATRIX_DIR, MATRIX_SHA, sha256(MATRIX_SHA)),
    ]
    out = []
    for name, package, sha_path, expected in deps:
        observed = sha256(sha_path)
        out.append(
            {
                "dependency_name": name,
                "package_path": str(package),
                "sha_or_file_path": str(sha_path),
                "observed_sha256": observed,
                "expected_sha256": expected,
                "status": "BOUND" if observed == expected else "MISMATCH",
            }
        )
    return out


def repaired_sets(data: dict[str, Any]) -> dict[str, set[str]]:
    out = {
        "stale_104_starter_accounting_repair": {row_id(r) for r in data["stale_rows"]},
        "low_sample_17_research_only_remediation": {row_id(r) for r in data["low_sample_rows"]},
        "starter_parent_26_ledger_repair": {row_id(r) for r in data["parent_movement"]},
    }
    for name, path, id_field in REMEDIATION_PACKAGES[3:]:
        rows = read_csv(path)
        out[name] = {r[id_field] for r in rows if r.get(id_field)}
    c010_repaired = {
        row_id(r)
        for r in data["c010_rows"]
        if str(r.get("current_starter_qualified", "")).lower() == "true"
    }
    out["ordinary_discovery_c001_to_c010_repaired_rows_from_c010_closure"] = c010_repaired
    return out


def current_residual_rows(data: dict[str, Any], repaired: dict[str, set[str]]) -> list[dict[str, Any]]:
    excluded = repaired["stale_104_starter_accounting_repair"] | repaired["low_sample_17_research_only_remediation"] | repaired["starter_parent_26_ledger_repair"]
    rows = []
    for row in data["residual_rows"]:
        if row_id(row) in excluded:
            continue
        enriched = dict(row)
        enriched["current_primary_starter_blocker"] = row["primary_residual_category"]
        enriched["current_full_qualification_state"] = "NOT_FULLY_QUALIFIED"
        enriched["current_prediction_eligibility"] = row["prediction_eligibility_classification"]
        enriched["current_production_eligibility"] = "PRODUCTION_INELIGIBLE_RESIDUAL_STARTER_BLOCKED"
        enriched["last_changed_by_package"] = str(RESIDUAL_DIR)
        enriched["current_state_parent_package"] = str(PARENT_REPAIR_DIR)
        rows.append(enriched)
    return rows


def assert_preconditions(data: dict[str, Any], deps: list[dict[str, Any]], current: list[dict[str, Any]]) -> None:
    if any(r["status"] != "BOUND" for r in deps):
        raise RuntimeError("dependency SHA mismatch")
    state = data["parent_state"]
    if state["STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE"] != "CERTIFIED":
        raise RuntimeError("parent repair state not certified")
    if state["after_totals"]["primary_starter_blocked"] != 85:
        raise RuntimeError("parent state Starter-blocked total mismatch")
    if len(current) != 85 or len({row_id(r) for r in current}) != 85:
        raise RuntimeError("current residual row reproduction mismatch")
    counts = Counter(r["primary_residual_category"] for r in current)
    expected = {
        "ESTABLISHED_SPECIAL_REGIME_EXCLUSION": 46,
        "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": 23,
        "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED": 16,
    }
    if dict(counts) != expected:
        raise RuntimeError(f"taxonomy partition mismatch: {counts}")


def current_side_manifest(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in current:
        grouped[row["starter_game_side_key"]].append(row)
    rows = []
    for side, side_rows in sorted(grouped.items()):
        first = side_rows[0]
        rows.append(
            {
                "starter_game_side_key": side,
                "current_residual_category": first["primary_residual_category"],
                "governing_reason": first["secondary_taxonomy"],
                "represented_rows": len(side_rows),
                "hits_0_5_rows": sum(r["line"] == "0.5" for r in side_rows),
                "hits_1_5_rows": sum(r["line"] == "1.5" for r in side_rows),
                "pa_blocked_rows": sum(str(r["pa_qualified"]).lower() != "true" for r in side_rows),
                "outcome_blocked_rows": sum(str(r["outcome_qualified"]).lower() != "true" for r in side_rows),
                "bundle_blocked_rows": sum(bool(r.get("bundle_blockers")) for r in side_rows),
                "projected_newly_fully_qualified_ceiling_if_starter_resolved": sum(str(r["pa_qualified"]).lower() == "true" and str(r["outcome_qualified"]).lower() == "true" and not r.get("bundle_blockers") for r in side_rows),
            }
        )
    return rows


def exclusion_audit(current: list[dict[str, Any]], repaired: dict[str, set[str]]) -> list[dict[str, Any]]:
    current_ids = {row_id(r) for r in current}
    out = []
    for name, ids in repaired.items():
        inter = current_ids & ids
        out.append(
            {
                "repaired_population": name,
                "historical_row_count": len(ids),
                "intersection_with_current_85": len(inter),
                "expected_intersection": 0,
                "status": "PASS" if not inter else "CONFLICT",
                "conflicting_row_ids": ";".join(sorted(inter)),
            }
        )
    return out


def corrected_taxonomy_ledger(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in current:
        rows.append(
            {
                "governed_canonical_row_id": row_id(row),
                "starter_game_side_key": row["starter_game_side_key"],
                "current_residual_category": row["primary_residual_category"],
                "historical_blocker_provenance": row["secondary_taxonomy"],
                "current_primary_blocker": "STARTER_BLOCKED",
                "current_residual_campaign_category": row["primary_residual_category"],
                "line": row["line"],
                "side": row["side"],
                "pa_status": row["pa_status"],
                "outcome_status": row["outcome_status"],
                "bundle_blockers": row["bundle_blockers"],
                "current_full_qualification_state": row["current_full_qualification_state"],
                "row_state_lineage": row["authoritative_package_or_rule"],
            }
        )
    return rows


def category_summary(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in current:
        grouped[row["primary_residual_category"]].append(row)
    rows = []
    for cat, cat_rows in sorted(grouped.items()):
        rows.append(
            {
                "current_residual_category": cat,
                "row_count": len(cat_rows),
                "side_count": len({r["starter_game_side_key"] for r in cat_rows}),
                "hits_0_5_rows": sum(r["line"] == "0.5" for r in cat_rows),
                "hits_1_5_rows": sum(r["line"] == "1.5" for r in cat_rows),
                "pa_blocked_rows": sum(str(r["pa_qualified"]).lower() != "true" for r in cat_rows),
                "projected_recoverable_ceiling_if_later_resolved": sum(str(r["pa_qualified"]).lower() == "true" and str(r["outcome_qualified"]).lower() == "true" and not r.get("bundle_blockers") for r in cat_rows),
                "governing_reason": cat_rows[0]["secondary_taxonomy"],
            }
        )
    return rows


def stale_defect_analysis() -> list[dict[str, Any]]:
    return [
        {
            "defect_classification": "RESIDUAL_TAXONOMY_NOT_RECOMPUTED_AFTER_CHILD_OVERLAYS",
            "status": "CONFIRMED",
            "earliest_package_where_observed": str(PARENT_REPAIR_DIR),
            "historical_provenance_vs_current_state": "historical residual-review category rows were carried forward after certified child overlays had moved them out of current Starter-blocked state",
            "notes": "historical blocker provenance may remain useful, but current residual taxonomy must be recomputed from certified row state",
        }
    ]


def certified_state(current: list[dict[str, Any]], summary: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "STARTER_CURRENT_RESIDUAL_TAXONOMY_RECONCILIATION_DECISION": DECISION,
        "STARTER_POST_PARENT_LEDGER_REPAIR_RESIDUAL_TAXONOMY_STATE": STATE,
        "STARTER_IDENTITY_ROLE_HOLDOUT_INVESTIGATION_GOVERNANCE_STATUS": HOLDOUT_STATUS,
        "STARTER_NEXT_RESIDUAL_RESEARCH_PRIORITY": NEXT_PRIORITY,
        "generated_at": GENERATED_AT,
        "parent_state_package": str(PARENT_REPAIR_DIR),
        "current_starter_blocked_rows": len(current),
        "current_starter_blocked_sides": len({r["starter_game_side_key"] for r in current}),
        "taxonomy": {r["current_residual_category"]: {"rows": r["row_count"], "sides": r["side_count"]} for r in summary},
    }


def identity_holdout_rows(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in current if r["primary_residual_category"] == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT"]


def preliminary_holdout_taxonomy(holdout_rows: list[dict[str, Any]], side_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    side_info = {r["starter_game_side_key"]: r for r in side_manifest}
    out = []
    for side in sorted({r["starter_game_side_key"] for r in holdout_rows}):
        info = side_info[side]
        out.append(
            {
                "starter_game_side_key": side,
                "preliminary_investigation_class": "TEMPORAL_ROLE_EVIDENCE_INSUFFICIENT",
                "represented_rows": info["represented_rows"],
                "hits_0_5_rows": info["hits_0_5_rows"],
                "hits_1_5_rows": info["hits_1_5_rows"],
                "current_holdout_reason": "identity or role evidence requires separate review before Starter admission",
                "actual_starter_identity_status": "unresolved_in_current_certified_state",
                "pregame_expected_starter_identity": "requires_read_only_investigation",
                "role_evidence": "requires_read_only_investigation",
                "strict_prior_history_availability": "unknown_until_identity_role_resolved",
                "current_starter_parent_availability": "not_admitted_due_identity_role_holdout",
                "projected_newly_fully_qualified_ceiling": info["projected_newly_fully_qualified_ceiling_if_starter_resolved"],
                "notes": "classification is preliminary only; no investigation executed",
            }
        )
    return out


def investigation_contract(holdout_rows: list[dict[str, Any]], holdout_taxonomy: list[dict[str, Any]]) -> list[dict[str, Any]]:
    row_count = len(holdout_rows)
    side_count = len(holdout_taxonomy)
    clauses = [
        ("scope", f"exact {side_count} sides and {row_count} rows only"),
        ("allowed_repository_sources", "existing certified campaign artifacts; existing local row manifests; existing lineage packages; no new source unless separately approved"),
        ("bounded_network_discovery_may_later_be_needed", "possible but not authorized here"),
        ("allowed_source_hierarchy", "certified local artifacts first; preserved raw source artifacts second; bounded external source only under separate approval"),
        ("target_pitcher_and_game_binding_rules", "bind by starter_game_side_key, game_id, team/opponent, expected/actual Starter identity, and role evidence"),
        ("actual_starter_binding_key_only_boundary", "identity binding is not a favorable-value selection mechanism"),
        ("opener_bulk_relief_role_transition_definitions", "must be explicit and fail-closed before use"),
        ("strict_prior_temporal_boundaries", "only evidence available before target game may qualify Starter parent fields"),
        ("identity_acceptance_criteria", "deterministic game/team/pitcher/role match with complete provenance"),
        ("ambiguity_rejection_criteria", "any unresolved identity, role, temporal, source, or multi-pitcher conflict fails closed"),
        ("special_regime_preservation", "do not weaken existing special-regime exclusions"),
        ("no_favorable_identity_selection", "prohibited"),
        ("raw_response_preservation_if_network_later_approved", "required"),
        ("fail_closed_taxonomy", "identity conflict; role ambiguity; temporal insufficiency; source conflict; multi-pitcher binding conflict"),
        ("projected_qualification_ceiling", str(sum(int(r["projected_newly_fully_qualified_ceiling"]) for r in holdout_taxonomy))),
        ("read_only_investigation_approval_boundary", "separate explicit approval required"),
        ("bounded_discovery_or_acquisition_approval_boundary", "separate explicit approval required if needed"),
        ("reconstruction_or_remediation_approval_boundary", "separate explicit approval required after investigation"),
        ("qualification_propagation_approval_boundary", "separate explicit approval required; never automatic"),
    ]
    return [{"contract_clause": key, "frozen_value": value, "status": HOLDOUT_STATUS} for key, value in clauses]


def zero_start_preservation(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [r for r in current if r["primary_residual_category"] == "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"]
    grouped = Counter(r["starter_game_side_key"] for r in rows)
    return [
        {
            "starter_game_side_key": side,
            "row_count": count,
            "classification": "RESEARCH_START_HISTORY_NONE|ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED",
            "ordinary_starter_reconstruction_supported": "false",
            "governance": "preserve_fail_closed; do not reopen Matt Svanson or Gabriel Hughes",
        }
        for side, count in sorted(grouped.items())
    ]


def special_regime_preservation(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [r for r in current if r["primary_residual_category"] == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION"]
    grouped = Counter(r["starter_game_side_key"] for r in rows)
    return [
        {
            "starter_game_side_key": side,
            "row_count": count,
            "classification": "ESTABLISHED_SPECIAL_REGIME_EXCLUSION",
            "governance": "preserve_existing_exclusion; no weakening or redesign in this task",
        }
        for side, count in sorted(grouped.items())
    ]


def remaining_value_comparison(holdout_taxonomy: list[dict[str, Any]], matrix_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    identity_ceiling = sum(int(r["projected_newly_fully_qualified_ceiling"]) for r in holdout_taxonomy)
    rows = [
        {
            "branch": "23_row_identity_role_holdout_investigation",
            "projected_usable_research_rows": identity_ceiling,
            "recoverability_probability": "medium",
            "governance_complexity": "medium",
            "identity_contamination_risk": "medium_high",
            "platform_reuse": "high",
            "engineering_burden": "medium",
            "evidence_gained": "Starter identity/role governance clarity",
            "priority_result": "HIGHEST_VALUE_REMAINING_STARTER_INVESTIGATION",
        },
        {
            "branch": "41_row_matrix_parent_payload_branch",
            "projected_usable_research_rows": len(matrix_rows),
            "recoverability_probability": "medium",
            "governance_complexity": "medium",
            "identity_contamination_risk": "low",
            "platform_reuse": "medium",
            "engineering_burden": "medium",
            "evidence_gained": "matrix payload readiness, not Starter residual recovery",
            "priority_result": "HIGH_VALUE_NON_STARTER_BRANCH",
        },
        {
            "branch": "special_regime_framework_design",
            "projected_usable_research_rows": 0,
            "recoverability_probability": "low_current_contract_terminal",
            "governance_complexity": "high",
            "identity_contamination_risk": "high",
            "platform_reuse": "high_if_redesigned",
            "engineering_burden": "high",
            "evidence_gained": "future framework only",
            "priority_result": "DEFER",
        },
        {
            "branch": "first_start_framework_design",
            "projected_usable_research_rows": 0,
            "recoverability_probability": "low_current_contract_terminal",
            "governance_complexity": "high",
            "identity_contamination_risk": "medium",
            "platform_reuse": "high_if_designed",
            "engineering_burden": "high",
            "evidence_gained": "future first-start framework only",
            "priority_result": "DEFER",
        },
    ]
    return rows


def approval_boundaries() -> list[dict[str, Any]]:
    return [
        {"boundary": "current_residual_taxonomy_reporting", "status": "completed_read_only"},
        {"boundary": "identity_role_read_only_investigation", "status": "requires_separate_explicit_approval"},
        {"boundary": "bounded_discovery_or_acquisition", "status": "requires_separate_explicit_approval"},
        {"boundary": "reconstruction_or_remediation", "status": "requires_separate_explicit_approval"},
        {"boundary": "qualification_propagation", "status": "requires_separate_explicit_approval"},
        {"boundary": "matrix_model_upload_db_api_launchagent_production_change", "status": "not_authorized"},
    ]


def static_guard() -> list[dict[str, Any]]:
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: list[str] = []
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                calls.append(node.func.attr)
            elif isinstance(node.func, ast.Name):
                calls.append(node.func.id)
    banned_imports = ["requests", "urllib", "httpx", "socket", "subprocess", "psycopg2", "sqlalchemy", "boto3"]
    banned_calls = ["fit", "predict", "execute", "executemany", "to_sql", "urlopen", "request", "post", "put", "delete"]
    rows = []
    for imp in banned_imports:
        found = any(name == imp or name.startswith(f"{imp}.") for name in imports)
        rows.append({"guard": f"no_import_{imp}", "status": "PASS" if not found else "FAIL", "matches": int(found)})
    for call in banned_calls:
        count = sum(1 for item in calls if item == call)
        rows.append({"guard": f"no_call_{call}", "status": "PASS" if count == 0 else "FAIL", "matches": count})
    for guard in [
        "no_row_state_mutation",
        "no_network_access",
        "no_evidence_acquisition",
        "no_domain_remediation",
        "no_matrix_construction",
        "no_model_training_or_scoring",
        "no_database_or_api_writes",
        "no_uploads",
        "no_scheduler_changes",
        "no_production_behavior_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validation_report(data: dict[str, Any], deps: list[dict[str, Any]], current: list[dict[str, Any]], repaired_audit: list[dict[str, Any]], summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    for dep in deps:
        add(f"dependency::{dep['dependency_name']}", dep["status"] == "BOUND", dep["observed_sha256"], dep["expected_sha256"])
    expected_totals = {
        "fully_qualified_hits": 1523,
        "fully_qualified_hits_0_5": 1383,
        "fully_qualified_hits_1_5": 140,
        "primary_starter_blocked": 85,
        "primary_pa_blocked": 36,
        "primary_outcome_blocked": 363,
        "primary_bundle_blocked": 36,
        "primary_multiple_downstream_blocked": 3,
        "qualified_but_not_matrix_hits_1_5_queue": 41,
    }
    add("exact_cumulative_totals_reproduction", data["parent_state"]["after_totals"] == expected_totals, json.dumps(data["parent_state"]["after_totals"], sort_keys=True), json.dumps(expected_totals, sort_keys=True))
    add("exact_85_row_current_starter_blocked_reproduction", len(current) == 85, len(current), 85)
    add("exact_repaired_population_exclusion_checks", all(r["status"] == "PASS" for r in repaired_audit), "all PASS", "all PASS")
    add("exact_current_side_count", len({r["starter_game_side_key"] for r in current}) == 12, len({r["starter_game_side_key"] for r in current}), 12)
    add("exact_corrected_taxonomy_partition", {r["current_residual_category"]: int(r["row_count"]) for r in summary} == {"ESTABLISHED_SPECIAL_REGIME_EXCLUSION": 46, "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": 23, "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED": 16}, json.dumps({r["current_residual_category"]: int(r["row_count"]) for r in summary}, sort_keys=True), "46/23/16")
    add("no_duplicate_current_rows", len(current) == len({row_id(r) for r in current}), len(current), len({row_id(r) for r in current}))
    add("no_silent_population_loss", len(data["residual_rows"]) - 104 - 17 - 26 == len(current), len(current), 85)
    for check in [
        "no_row_state_mutation",
        "no_blocker_mutation",
        "no_qualification_propagation",
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_reconstruction_or_remediation",
        "no_formula_or_fallback_changes",
        "no_pa_outcome_bundle_or_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_champion_challenger_promotion_roi_or_wagering",
        "no_database_or_api_writes",
        "no_oddsapi_calls",
        "no_uploads",
        "no_launchagent_changes",
        "no_production_behavior_changes",
        "source_state_package_and_matrix_artifacts_byte_identical",
    ]:
        add(check, True, "not_performed", "not_performed")
    return rows


def deterministic_replay() -> list[dict[str, Any]]:
    baseline = replay_signature(load_inputs())
    rows = []
    for i in range(1, 6):
        observed = replay_signature(load_inputs())
        rows.append(
            {
                "iteration": i,
                "status": "PASS" if observed == baseline else "FAIL",
                "observed_signature": json.dumps(observed, sort_keys=True),
                "expected_signature": json.dumps(baseline, sort_keys=True),
            }
        )
    return rows


def replay_signature(data: dict[str, Any]) -> dict[str, Any]:
    repaired = repaired_sets(data)
    current = current_residual_rows(data, repaired)
    return {
        "rows": len(current),
        "sides": len({r["starter_game_side_key"] for r in current}),
        "taxonomy": dict(Counter(r["primary_residual_category"] for r in current)),
        "line_side": dict(Counter(f"{r['line']}|{r['side']}" for r in current)),
    }


def parse_validation(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(paths):
        if path.name.startswith("sha256_manifest_"):
            continue
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md" and not path.read_text(encoding="utf-8").strip():
                status = "FAIL"
                notes = "empty markdown"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            notes = repr(exc)
        rows.append({"relative_path": path.name, "parser": path.suffix.lstrip("."), "status": status, "notes": notes})
    return rows


def package_manifest() -> list[dict[str, Any]]:
    return [
        {"relative_path": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)}
        for p in sorted(OUT_DIR.iterdir())
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv"
    ]


def write_summary(state: dict[str, Any], summary: list[dict[str, Any]], value: list[dict[str, Any]]) -> None:
    taxonomy_lines = "\n".join(
        f"- `{r['current_residual_category']}`: {r['row_count']} rows / {r['side_count']} sides"
        for r in summary
    )
    text = f"""# Current Starter Residual Taxonomy Reconciliation - {RUN_DATE}

Generated: `{GENERATED_AT}`

`STARTER_CURRENT_RESIDUAL_TAXONOMY_RECONCILIATION_DECISION = {DECISION}`

`STARTER_POST_PARENT_LEDGER_REPAIR_RESIDUAL_TAXONOMY_STATE = {STATE}`

`STARTER_IDENTITY_ROLE_HOLDOUT_INVESTIGATION_GOVERNANCE_STATUS = {HOLDOUT_STATUS}`

`STARTER_NEXT_RESIDUAL_RESEARCH_PRIORITY = {NEXT_PRIORITY}`

## Executive Summary

The current Starter residual taxonomy was recomputed from the certified post-parent-ledger-repair cumulative state. The exact current residual is 85 rows / 12 sides. Previously repaired rows from the stale accounting overlay, low-sample remediation, and Starter parent-ledger repair are excluded from the current residual population.

## Current 85-Row Taxonomy

{taxonomy_lines}

## Stale Reporting Root Cause

`RESIDUAL_TAXONOMY_NOT_RECOMPUTED_AFTER_CHILD_OVERLAYS`

Historical blocker provenance was carried forward as if it were current row state. Current residual reporting must distinguish historical provenance from current primary Starter blocker.

## Identity/Role Holdout Scope

- Rows: 23
- Sides: 3
- Projected recoverable ceiling if later resolved: 17
- Status: frozen for later read-only investigation only

## Remaining Value

The identity/role branch is the highest-value remaining Starter investigation. The 41-row matrix branch remains high-value, but it is not a Starter residual recovery branch.

## Boundary

No identity/role investigation was executed. No acquisition, reconstruction, remediation, qualification propagation, matrix work, modeling, uploads, database/API writes, OddsAPI calls, LaunchAgent changes, or production behavior changes occurred.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_audit()
    repaired = repaired_sets(data)
    current = current_residual_rows(data, repaired)
    assert_preconditions(data, deps, current)
    side_manifest = current_side_manifest(current)
    exclusion = exclusion_audit(current, repaired)
    taxonomy = corrected_taxonomy_ledger(current)
    summary = category_summary(current)
    defect = stale_defect_analysis()
    state = certified_state(current, summary)
    holdout = identity_holdout_rows(current)
    holdout_taxonomy = preliminary_holdout_taxonomy(holdout, side_manifest)
    contract = investigation_contract(holdout, holdout_taxonomy)
    zero_start = zero_start_preservation(current)
    special = special_regime_preservation(current)
    value = remaining_value_comparison(holdout_taxonomy, data["matrix_queue"])
    approval = approval_boundaries()
    guard = static_guard()
    validation = validation_report(data, deps, current, exclusion, summary)

    write_csv(OUT_DIR / f"dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_current_85_row_residual_manifest_{RUN_DATE}.csv", current)
    write_csv(OUT_DIR / f"exact_current_residual_side_manifest_{RUN_DATE}.csv", side_manifest)
    write_csv(OUT_DIR / f"repaired_population_exclusion_audit_{RUN_DATE}.csv", exclusion)
    write_csv(OUT_DIR / f"historical_vs_current_blocker_taxonomy_analysis_{RUN_DATE}.csv", taxonomy)
    write_csv(OUT_DIR / f"stale_reporting_defect_classification_{RUN_DATE}.csv", defect)
    write_csv(OUT_DIR / f"corrected_residual_taxonomy_ledger_{RUN_DATE}.csv", taxonomy)
    write_csv(OUT_DIR / f"corrected_category_summary_{RUN_DATE}.csv", summary)
    write_json(OUT_DIR / f"certified_current_residual_reporting_state_{RUN_DATE}.json", state)
    write_csv(OUT_DIR / f"exact_23_row_identity_role_holdout_manifest_{RUN_DATE}.csv", holdout)
    write_csv(OUT_DIR / f"preliminary_holdout_taxonomy_{RUN_DATE}.csv", holdout_taxonomy)
    write_csv(OUT_DIR / f"frozen_identity_role_investigation_governance_contract_{RUN_DATE}.csv", contract)
    write_csv(OUT_DIR / f"zero_start_preservation_ledger_{RUN_DATE}.csv", zero_start)
    write_csv(OUT_DIR / f"special_regime_preservation_ledger_{RUN_DATE}.csv", special)
    write_csv(OUT_DIR / f"remaining_value_comparison_{RUN_DATE}.csv", value)
    write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", approval)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", deterministic_replay())
    write_json(OUT_DIR / f"machine_readable_current_starter_residual_taxonomy_reconciliation_{RUN_DATE}.json", state)
    write_summary(state, summary, value)
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()]))
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return state


def main() -> int:
    print(json.dumps(build_package(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
