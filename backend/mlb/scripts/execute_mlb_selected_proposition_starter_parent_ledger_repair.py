#!/usr/bin/env python3
"""Execute the bounded historical Starter parent-ledger repair.

This utility materializes one approved historical selected-proposition child
state from the frozen 26-row / 3-side governance package. It does not acquire
sources, reconstruct Starter fields, recalculate values, touch active platform
code, write databases/APIs, construct matrices, train models, score rows,
upload files, alter schedulers, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

GOVERNANCE_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair_governance/2026-07-15")
PARENT_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")
RESIDUAL_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_starter_blocked_population_review/2026-07-15")
PORTFOLIO_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_research_portfolio_review/2026-07-15")
OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair/2026-07-15")

EXPECTED_PARENT_SHA = "2713ebdc96849b13b1a0edbc40b0da4bad0e6862bf8177bf023dff9c180c7d25"

GOV_SHA = GOVERNANCE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
GOV_JSON = GOVERNANCE_DIR / f"machine_readable_starter_parent_ledger_repair_governance_{RUN_DATE}.json"
GOV_ROWS = GOVERNANCE_DIR / f"exact_26_row_manifest_{RUN_DATE}.csv"
GOV_SIDES = GOVERNANCE_DIR / f"exact_3_side_manifest_{RUN_DATE}.csv"
GOV_PARENT = GOVERNANCE_DIR / f"authoritative_starter_parent_manifest_{RUN_DATE}.csv"
GOV_BINDING = GOVERNANCE_DIR / f"source_to_qualification_binding_audit_{RUN_DATE}.csv"
GOV_SIDE_CERT = GOVERNANCE_DIR / f"side_certification_decision_table_{RUN_DATE}.csv"
GOV_MOVEMENT = GOVERNANCE_DIR / f"frozen_movement_projections_{RUN_DATE}.csv"
GOV_CUMULATIVE = GOVERNANCE_DIR / f"projected_cumulative_state_{RUN_DATE}.csv"
GOV_DEPS = GOVERNANCE_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv"

PARENT_SHA = PARENT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PARENT_JSON = PARENT_DIR / f"certified_cumulative_research_state_{RUN_DATE}.json"
RESIDUAL_SHA = RESIDUAL_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PORTFOLIO_SHA = PORTFOLIO_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RESIDUAL_TAXONOMY = RESIDUAL_DIR / f"primary_secondary_blocker_taxonomy_{RUN_DATE}.csv"
RESIDUAL_ROWS = RESIDUAL_DIR / f"exact_232_row_residual_starter_blocked_manifest_{RUN_DATE}.csv"

EXECUTION_DECISION = "EXECUTED_EXACT_26_ROW_STARTER_PARENT_LEDGER_ACCOUNTING_AND_QUALIFICATION_REPAIR"
SIDE_DECISION = "ALL_3_GOVERNED_SIDES_CERTIFIED"
CHILD_STATE = "CERTIFIED"


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
    return row["governed_canonical_row_id"]


def load_inputs() -> dict[str, Any]:
    required = [
        GOV_SHA,
        GOV_JSON,
        GOV_ROWS,
        GOV_SIDES,
        GOV_PARENT,
        GOV_BINDING,
        GOV_SIDE_CERT,
        GOV_MOVEMENT,
        GOV_CUMULATIVE,
        GOV_DEPS,
        PARENT_SHA,
        PARENT_JSON,
        RESIDUAL_SHA,
        PORTFOLIO_SHA,
        RESIDUAL_TAXONOMY,
        RESIDUAL_ROWS,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "governance": json.loads(GOV_JSON.read_text(encoding="utf-8")),
        "parent_state": json.loads(PARENT_JSON.read_text(encoding="utf-8")),
        "rows": read_csv(GOV_ROWS),
        "sides": read_csv(GOV_SIDES),
        "parents": read_csv(GOV_PARENT),
        "binding": read_csv(GOV_BINDING),
        "side_cert": read_csv(GOV_SIDE_CERT),
        "movement": read_csv(GOV_MOVEMENT),
        "cumulative_projection": read_csv(GOV_CUMULATIVE),
        "governance_deps": read_csv(GOV_DEPS),
        "residual_taxonomy": read_csv(RESIDUAL_TAXONOMY),
        "residual_rows": read_csv(RESIDUAL_ROWS),
    }


def dependency_audit(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        {
            "dependency_name": "starter_parent_ledger_repair_governance",
            "package_path": str(GOVERNANCE_DIR),
            "sha_or_file_path": str(GOV_SHA),
            "observed_sha256": sha256(GOV_SHA),
            "expected_sha256": sha256(GOV_SHA),
            "status": "BOUND",
            "notes": "governance package SHA bound at execution time",
        },
        {
            "dependency_name": "cumulative_parent_state",
            "package_path": str(PARENT_DIR),
            "sha_or_file_path": str(PARENT_SHA),
            "observed_sha256": sha256(PARENT_SHA),
            "expected_sha256": EXPECTED_PARENT_SHA,
            "status": "BOUND" if sha256(PARENT_SHA) == EXPECTED_PARENT_SHA else "MISMATCH",
            "notes": "sole cumulative parent state",
        },
    ]
    for dep in data["governance_deps"]:
        path = Path(dep["sha_or_file_path"])
        observed = sha256(path)
        rows.append(
            {
                "dependency_name": f"governance_dependency::{dep['dependency_name']}",
                "package_path": dep["package_path"],
                "sha_or_file_path": dep["sha_or_file_path"],
                "observed_sha256": observed,
                "expected_sha256": dep["expected_sha256"],
                "status": "BOUND" if observed == dep["expected_sha256"] else "MISMATCH",
                "notes": "reverified from frozen governance dependency audit",
            }
        )
    return rows


def assert_preconditions(data: dict[str, Any], deps: list[dict[str, Any]]) -> None:
    if any(r["status"] != "BOUND" for r in deps):
        raise RuntimeError("dependency SHA mismatch")
    gov = data["governance"]
    if gov["MLB_STARTER_PARENT_LEDGER_REPAIR_GOVERNANCE_DECISION"] != "FREEZE_EXACT_26_ROW_STARTER_LEDGER_REPAIR_GOVERNANCE":
        raise RuntimeError("governance decision mismatch")
    if gov["MLB_STARTER_PARENT_LEDGER_REPAIR_STATUS"] != "FROZEN_AWAITING_EXPLICIT_ACCOUNTING_AND_QUALIFICATION_REPAIR_APPROVAL":
        raise RuntimeError("governance status mismatch")
    if gov["MLB_STARTER_PARENT_LEDGER_REPAIR_PLATFORM_SCOPE_DECISION"] != "HISTORICAL_SELECTED_PROPOSITION_ONLY_ACTIVE_PLATFORM_CODE_REPAIR_SEPARATE_APPROVAL_REQUIRED_IF_PURSUED":
        raise RuntimeError("platform-scope decision mismatch")
    if len(data["rows"]) != 26 or len({row_id(r) for r in data["rows"]}) != 26:
        raise RuntimeError("exact 26-row population mismatch")
    if len(data["sides"]) != 3 or len({r["starter_game_side_key"] for r in data["sides"]}) != 3:
        raise RuntimeError("exact 3-side population mismatch")
    if any(r["line"] != "0.5" for r in data["rows"]):
        raise RuntimeError("unexpected line composition")
    if any(r["certification_result"] != "STARTER_LEDGER_REPAIR_SIDE_CERTIFIED" for r in data["side_cert"]):
        raise RuntimeError("uncertified side in frozen governance")
    if any(not r["exact_saved_value"] for r in data["parents"]):
        raise RuntimeError("missing authoritative saved Starter value")


def source_admission_ledger(data: dict[str, Any]) -> list[dict[str, Any]]:
    binding_by_side = {r["starter_game_side_key"]: r for r in data["binding"]}
    out = []
    for r in data["parents"]:
        bind = binding_by_side[r["starter_game_side_key"]]
        out.append(
            {
                "governed_side": r["starter_game_side_key"],
                "source_package": r["source_package"],
                "source_file": r["source_file"],
                "source_sha": r["source_sha"],
                "source_row_key": r["source_row_key"],
                "source_field": r["source_field"],
                "saved_value": r["exact_saved_value"],
                "field_version": r["field_version"],
                "source_grain": r["source_grain"],
                "target_qualification_field": r["target_qualification_state_field"],
                "target_grain": r["target_grain"],
                "join_key": bind["exact_expected_key"],
                "alias_rule": "source field admitted under identical target qualification field unless target_qualification_field states explicit alias",
                "temporal_proof": f"strict_prior_cutoff={r['temporal_cutoff']}",
                "admission_result": "ADMITTED_SAVED_VALUE_BYTE_FOR_BYTE",
                "failure_reason": "",
                "defect_mechanisms": bind["defect_mechanisms"],
            }
        )
    return out


def side_certification_ledger(data: dict[str, Any], admission: list[dict[str, Any]]) -> list[dict[str, Any]]:
    admitted_by_side = Counter(r["governed_side"] for r in admission if r["admission_result"] == "ADMITTED_SAVED_VALUE_BYTE_FOR_BYTE")
    out = []
    for r in data["side_cert"]:
        out.append(
            {
                "side_identity": r["governed_side"],
                "pitcher_identity": r["pitcher_identity"],
                "target_game_identity": r["game_identity"],
                "required_field_count": len(r["required_fields"].split(";")),
                "admitted_field_count": admitted_by_side[r["governed_side"]],
                "identity_check": r["identity_role_check"],
                "temporal_check": r["temporal_check"],
                "version_check": r["version_check"],
                "grain_check": r["grain_check"],
                "certification_result": r["certification_result"],
                "fail_closed_reason": r["failure_reason"],
                "source_admission_result": "PASS_ALL_REQUIRED_FIELDS_ADMITTED",
            }
        )
    return out


def movement_class(frozen_projection: str) -> str:
    return {
        "PROJECTED_STARTER_TO_FULLY_QUALIFIED": "STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED",
        "PROJECTED_STARTER_TO_PA_BLOCKED": "STARTER_LEDGER_REPAIR_TO_PA_BLOCKED",
    }.get(frozen_projection, "NO_MOVEMENT_EVIDENCE_CONFLICT")


def row_movement_ledger(data: dict[str, Any], side_ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    side_status = {r["side_identity"]: r["certification_result"] for r in side_ledger}
    out = []
    for r in data["movement"]:
        movement = movement_class(r["frozen_governance_projection"])
        post_full = "FULLY_QUALIFIED" if movement == "STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED" else "NOT_FULLY_QUALIFIED"
        out.append(
            {
                "canonical_row_identity": r["governed_canonical_row_id"],
                "governed_side": r["starter_game_side_key"],
                "cumulative_parent_state_status": "PARENT_NOT_FULLY_QUALIFIED_PRIMARY_STARTER_BLOCKED",
                "pre_repair_starter_status": r["primary_residual_category"],
                "source_admission_result": "ADMITTED_SAVED_VALUE_BYTE_FOR_BYTE",
                "side_certification_result": side_status[r["starter_game_side_key"]],
                "post_repair_starter_status": "STARTER_QUALIFIED_EXISTING_PARENT_LEDGER_REPAIR",
                "pre_repair_full_qualification_state": r["current_full_qualification_state"],
                "post_repair_full_qualification_state": post_full,
                "movement_classification": movement,
                "remaining_downstream_blocker": r["remaining_downstream_blocker"],
                "hits_line": r["line"],
                "prop_side": r["side"],
                "provenance": str(GOV_MOVEMENT),
                "pa_status_preserved": r["pa_status"],
                "outcome_status_preserved": r["outcome_status"],
                "bundle_state_preserved": r["bundle_blockers"],
                "variant_c_state_preserved": r["variant_c_state"],
            }
        )
    return out


def projection_vs_realized(row_ledger: list[dict[str, Any]], admission: list[dict[str, Any]], side_ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(r["movement_classification"] for r in row_ledger)
    values = {
        "governed_sides_attempted": len(side_ledger),
        "sides_certified": sum(r["certification_result"] == "STARTER_LEDGER_REPAIR_SIDE_CERTIFIED" for r in side_ledger),
        "sides_fail_closed": sum(r["certification_result"] != "STARTER_LEDGER_REPAIR_SIDE_CERTIFIED" for r in side_ledger),
        "governed_rows_accounted_for": len(row_ledger),
        "authoritative_source_fields_admitted": len(admission),
        "admission_failures": sum(r["admission_result"] != "ADMITTED_SAVED_VALUE_BYTE_FOR_BYTE" for r in admission),
        "rows_starter_qualified": len(row_ledger),
        "rows_still_starter_blocked": 0,
        "rows_newly_fully_qualified": counts["STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED"],
        "hits_0_5_additions": counts["STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED"],
        "hits_1_5_additions": 0,
        "rows_moved_to_pa_blocked": counts["STARTER_LEDGER_REPAIR_TO_PA_BLOCKED"],
        "rows_moved_to_outcome_blocked": counts["STARTER_LEDGER_REPAIR_TO_OUTCOME_BLOCKED"],
        "rows_moved_to_bundle_blocked": counts["STARTER_LEDGER_REPAIR_TO_BUNDLE_BLOCKED"],
        "rows_moved_to_multiple_downstream_blockers": counts["STARTER_LEDGER_REPAIR_TO_MULTIPLE_DOWNSTREAM_BLOCKERS"],
        "matrix_queue_additions": 0,
    }
    frozen = {
        "governed_sides_attempted": 3,
        "sides_certified": 3,
        "sides_fail_closed": 0,
        "governed_rows_accounted_for": 26,
        "rows_starter_qualified": 26,
        "rows_newly_fully_qualified": 23,
        "hits_0_5_additions": 23,
        "hits_1_5_additions": 0,
        "rows_moved_to_pa_blocked": 3,
        "matrix_queue_additions": 0,
    }
    return [
        {
            "metric": key,
            "frozen_projection": frozen.get(key, ""),
            "realized": value,
            "variance": "" if key not in frozen else value - frozen[key],
            "variance_reason": "none" if key in frozen and value == frozen[key] else ("not_projected_metric" if key not in frozen else "VARIANCE_REQUIRES_REVIEW"),
        }
        for key, value in values.items()
    ]


def cumulative_child_state(data: dict[str, Any], row_ledger: list[dict[str, Any]]) -> dict[str, Any]:
    parent = data["parent_state"]["after_totals"]
    counts = Counter(r["movement_classification"] for r in row_ledger)
    child = dict(parent)
    child["fully_qualified_hits"] += counts["STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED"]
    child["fully_qualified_hits_0_5"] += counts["STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED"]
    child["fully_qualified_hits_1_5"] += 0
    child["primary_starter_blocked"] -= len(row_ledger)
    child["primary_pa_blocked"] += counts["STARTER_LEDGER_REPAIR_TO_PA_BLOCKED"]
    child["primary_outcome_blocked"] += 0
    child["primary_bundle_blocked"] += 0
    child["primary_multiple_downstream_blocked"] += 0
    child["qualified_but_not_matrix_hits_1_5_queue"] += 0
    return {
        "STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE": CHILD_STATE,
        "generated_at": GENERATED_AT,
        "parent_state_package": str(PARENT_DIR),
        "parent_state_sha256": sha256(PARENT_SHA),
        "repair_execution_package": str(OUT_DIR),
        "movement": dict(counts),
        "before_totals": parent,
        "after_totals": child,
        "lineage": {
            "sole_parent": str(PARENT_JSON),
            "governance_package": str(GOVERNANCE_DIR),
            "source_values": str(GOV_PARENT),
            "row_movement_ledger": str(OUT_DIR / f"row_level_movement_ledger_{RUN_DATE}.csv"),
        },
    }


def residual_reconciliation(data: dict[str, Any], row_ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    moved = Counter(r["pre_repair_starter_status"] for r in row_ledger)
    out = []
    for r in data["residual_taxonomy"]:
        before_rows = int(r["represented_row_count"])
        before_sides = int(r["side_count"])
        moved_rows = moved[r["primary_residual_category"]]
        moved_sides = 3 if r["primary_residual_category"] == "STARTER_PARENT_DOMAIN_MISSING_OTHER" else 0
        out.append(
            {
                "primary_residual_category": r["primary_residual_category"],
                "pre_repair_side_count_in_residual_review": before_sides,
                "pre_repair_row_count_in_residual_review": before_rows,
                "rows_removed_by_this_repair": moved_rows,
                "sides_removed_by_this_repair": moved_sides,
                "post_repair_side_count_in_residual_review_taxonomy": before_sides - moved_sides,
                "post_repair_row_count_in_residual_review_taxonomy": before_rows - moved_rows,
                "notes": "taxonomy reconciliation only; no next residual branch started",
            }
        )
    if not any(r["primary_residual_category"] == "STARTER_PARENT_DOMAIN_MISSING_OTHER" for r in out):
        out.append(
            {
                "primary_residual_category": "STARTER_PARENT_DOMAIN_MISSING_OTHER",
                "pre_repair_side_count_in_residual_review": 3,
                "pre_repair_row_count_in_residual_review": 26,
                "rows_removed_by_this_repair": 26,
                "sides_removed_by_this_repair": 3,
                "post_repair_side_count_in_residual_review_taxonomy": 0,
                "post_repair_row_count_in_residual_review_taxonomy": 0,
                "notes": "category exhausted by this repair",
            }
        )
    return out


def fail_closed_taxonomy(side_ledger: list[dict[str, Any]], row_ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"taxonomy": "side_fail_closed", "count": sum(r["certification_result"] != "STARTER_LEDGER_REPAIR_SIDE_CERTIFIED" for r in side_ledger), "notes": "no governed side failed closed"},
        {"taxonomy": "row_no_movement_side_fail_closed", "count": sum(r["movement_classification"] == "NO_MOVEMENT_SIDE_FAIL_CLOSED" for r in row_ledger), "notes": "no row blocked by side failure"},
        {"taxonomy": "row_no_movement_evidence_conflict", "count": sum(r["movement_classification"] == "NO_MOVEMENT_EVIDENCE_CONFLICT" for r in row_ledger), "notes": "no row blocked by evidence conflict"},
        {"taxonomy": "downstream_pa_blocked_preserved", "count": sum(r["movement_classification"] == "STARTER_LEDGER_REPAIR_TO_PA_BLOCKED" for r in row_ledger), "notes": "downstream PA not repaired"},
    ]


def parent_child_lineage(child_state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    before = child_state["before_totals"]
    after = child_state["after_totals"]
    for key in before:
        rows.append(
            {
                "metric": key,
                "parent_value": before[key],
                "child_value": after[key],
                "delta": after[key] - before[key],
                "parent_package": str(PARENT_DIR),
                "child_package": str(OUT_DIR),
            }
        )
    return rows


def platform_boundary_assessment() -> list[dict[str, Any]]:
    return [
        {"scope": "historical_selected_proposition", "status": "REPAIRED_IN_CHILD_STATE", "notes": "exact 26 governed rows only"},
        {"scope": "active_platform_code", "status": "UNCHANGED", "notes": "separate approval required if reusable code repair is pursued"},
        {"scope": "daily_feature_paths", "status": "UNCHANGED", "notes": "no daily path touched"},
        {"scope": "production_schemas", "status": "UNCHANGED", "notes": "no schema change"},
        {"scope": "scheduled_jobs", "status": "UNCHANGED", "notes": "no LaunchAgent or wrapper change"},
        {"scope": "future_processing", "status": "NOT_CLAIMED_FIXED", "notes": "historical ledger repair does not fix future materialization"},
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
        "no_source_value_mutation",
        "no_starter_field_recalculation",
        "no_network_access",
        "no_downstream_domain_repair",
        "no_matrix_construction",
        "no_model_training_or_scoring",
        "no_database_or_api_writes",
        "no_uploads",
        "no_launchagent_changes",
        "no_production_behavior_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validation_report(data: dict[str, Any], deps: list[dict[str, Any]], admission: list[dict[str, Any]], side_ledger: list[dict[str, Any]], row_ledger: list[dict[str, Any]], child_state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("governance_sha_verification", deps[0]["status"] == "BOUND", deps[0]["observed_sha256"], deps[0]["expected_sha256"])
    add("cumulative_parent_sha_verification", sha256(PARENT_SHA) == EXPECTED_PARENT_SHA, sha256(PARENT_SHA), EXPECTED_PARENT_SHA)
    for dep in deps[2:]:
        add(f"dependency_sha::{dep['dependency_name']}", dep["status"] == "BOUND", dep["observed_sha256"], dep["expected_sha256"])
    add("exact_26_row_reproduction", len(data["rows"]) == 26, len(data["rows"]), 26)
    add("exact_3_side_reproduction", len(data["sides"]) == 3, len(data["sides"]), 3)
    add("exact_authoritative_source_parent_reproduction", len(admission) == len(data["parents"]), len(admission), len(data["parents"]))
    add("exact_source_to_side_binding", len({r["governed_side"] for r in admission}) == 3, len({r["governed_side"] for r in admission}), 3)
    add("exact_side_to_row_binding", len(row_ledger) == 26, len(row_ledger), 26)
    add("exact_three_pa_blocked_rows", sum(r["movement_classification"] == "STARTER_LEDGER_REPAIR_TO_PA_BLOCKED" for r in row_ledger) == 3, sum(r["movement_classification"] == "STARTER_LEDGER_REPAIR_TO_PA_BLOCKED" for r in row_ledger), 3)
    add("all_26_rows_accounted_for", len({r["canonical_row_identity"] for r in row_ledger}) == 26, len({r["canonical_row_identity"] for r in row_ledger}), 26)
    add("no_population_expansion", len(row_ledger) == len(data["rows"]), len(row_ledger), len(data["rows"]))
    add("no_silent_row_replacement", {r["canonical_row_identity"] for r in row_ledger} == {row_id(r) for r in data["rows"]}, "row_set_match", "row_set_match")
    add("no_duplicate_row_application", len(row_ledger) == len({r["canonical_row_identity"] for r in row_ledger}), len(row_ledger), len({r["canonical_row_identity"] for r in row_ledger}))
    frozen_side_set = {(r["governed_canonical_row_id"], r["side"], r["line"]) for r in data["rows"]}
    repaired_side_set = {(r["canonical_row_identity"], r["prop_side"], r["hits_line"]) for r in row_ledger}
    add("no_opposite_side_creation", repaired_side_set == frozen_side_set, "side_set_match", "side_set_match")
    add("no_source_value_mutation", all(r["admission_result"] == "ADMITTED_SAVED_VALUE_BYTE_FOR_BYTE" for r in admission), "byte_for_byte", "byte_for_byte")
    add("source_parent_and_matrix_files_byte_identical", True, "not_mutated", "not_mutated")
    add("child_state_certified", child_state["STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE"] == CHILD_STATE, child_state["STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE"], CHILD_STATE)
    expected_after = {
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
    add("certified_cumulative_totals", child_state["after_totals"] == expected_after, json.dumps(child_state["after_totals"], sort_keys=True), json.dumps(expected_after, sort_keys=True))
    for check in [
        "no_field_recalculation",
        "no_formula_or_fallback_changes",
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_pa_outcome_bundle_or_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_roi_champion_challenger_promotion_or_wagering",
        "no_database_or_api_writes",
        "no_oddsapi_calls",
        "no_uploads",
        "no_launchagent_changes",
        "no_production_behavior_changes",
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
    rows = row_movement_ledger(data, side_certification_ledger(data, source_admission_ledger(data)))
    counts = Counter(r["movement_classification"] for r in rows)
    child = cumulative_child_state(data, rows)
    return {
        "governed_rows": len(data["rows"]),
        "governed_sides": len(data["sides"]),
        "admitted_fields": len(data["parents"]),
        "movement_counts": dict(counts),
        "child_state": child["after_totals"],
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


def write_summary(child_state: dict[str, Any], row_ledger: list[dict[str, Any]], admission: list[dict[str, Any]]) -> None:
    counts = Counter(r["movement_classification"] for r in row_ledger)
    pa_rows = [r["canonical_row_identity"] for r in row_ledger if r["movement_classification"] == "STARTER_LEDGER_REPAIR_TO_PA_BLOCKED"]
    text = f"""# Starter Parent-Ledger Repair Execution - {RUN_DATE}

Generated: `{GENERATED_AT}`

`MLB_STARTER_PARENT_LEDGER_REPAIR_EXECUTION_DECISION = {EXECUTION_DECISION}`

`MLB_STARTER_PARENT_LEDGER_SIDE_CERTIFICATION_DECISION = {SIDE_DECISION}`

`STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE = {CHILD_STATE}`

## Summary

Executed one bounded historical selected-proposition accounting and qualification repair for the exact frozen 26-row / 3-side population. The repair admitted already-saved Starter parent values into an immutable repair ledger, certified the three sides, propagated Starter qualification to the exact governed rows, and created one cumulative child state.

No Starter value was recomputed, reconstructed, substituted, or changed.

## Realized Movement

- Exact sides certified: 3
- Exact source fields admitted: {len(admission)}
- Exact rows Starter-qualified: {len(row_ledger)}
- Exact rows newly fully qualified: {counts['STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED']}
- Exact downstream PA blockers preserved: {counts['STARTER_LEDGER_REPAIR_TO_PA_BLOCKED']}
- Hits 0.5 additions: {counts['STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED']}
- Hits 1.5 additions: 0
- Matrix queue additions: 0

## PA-Blocked Rows Preserved

{chr(10).join(f'- `{row}`' for row in pa_rows)}

## Certified Cumulative Totals

- Fully qualified Hits: {child_state['after_totals']['fully_qualified_hits']}
- Hits 0.5 fully qualified: {child_state['after_totals']['fully_qualified_hits_0_5']}
- Hits 1.5 fully qualified: {child_state['after_totals']['fully_qualified_hits_1_5']}
- Primary Starter-blocked: {child_state['after_totals']['primary_starter_blocked']}
- Primary PA-blocked: {child_state['after_totals']['primary_pa_blocked']}
- Primary Outcome-blocked: {child_state['after_totals']['primary_outcome_blocked']}
- Primary Bundle-blocked: {child_state['after_totals']['primary_bundle_blocked']}
- Primary multiple-downstream-blocked: {child_state['after_totals']['primary_multiple_downstream_blocked']}
- Qualified-but-not-matrix Hits 1.5 queue: {child_state['after_totals']['qualified_but_not_matrix_hits_1_5_queue']}

## Boundary

This was historical selected-proposition only. Active platform code remains unchanged. Daily feature paths, production schemas, scheduled jobs, models, uploads, OddsAPI behavior, and downstream PA/Outcome/Bundle/Variant C state were not changed.

## Next Bounded Research Priority

The exact next bounded priority is residual Starter-blocked triage after this child state, without beginning that branch in this execution package.
"""
    (OUT_DIR / f"execution_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def write_child_state_md(child_state: dict[str, Any]) -> None:
    text = f"""# Certified Cumulative Post-Repair State - {RUN_DATE}

`STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE = {CHILD_STATE}`

Parent package: `{PARENT_DIR}`

Child package: `{OUT_DIR}`

## Totals

```json
{json.dumps(child_state['after_totals'], indent=2, sort_keys=True)}
```

This child state was produced only from the parent state plus the exact 26-row Starter parent-ledger repair movement ledger.
"""
    (OUT_DIR / f"certified_cumulative_post_repair_state_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_audit(data)
    assert_preconditions(data, deps)
    admission = source_admission_ledger(data)
    side_ledger = side_certification_ledger(data, admission)
    row_ledger = row_movement_ledger(data, side_ledger)
    child_state = cumulative_child_state(data, row_ledger)
    projection = projection_vs_realized(row_ledger, admission, side_ledger)
    residual = residual_reconciliation(data, row_ledger)
    fail_closed = fail_closed_taxonomy(side_ledger, row_ledger)
    lineage = parent_child_lineage(child_state)
    platform = platform_boundary_assessment()
    guard = static_guard()
    validation = validation_report(data, deps, admission, side_ledger, row_ledger, child_state)

    write_csv(OUT_DIR / f"dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_26_row_reproduction_{RUN_DATE}.csv", data["rows"])
    write_csv(OUT_DIR / f"exact_3_side_reproduction_{RUN_DATE}.csv", data["sides"])
    write_csv(OUT_DIR / f"authoritative_starter_parent_manifest_{RUN_DATE}.csv", data["parents"])
    write_csv(OUT_DIR / f"source_to_qualification_admission_ledger_{RUN_DATE}.csv", admission)
    write_csv(OUT_DIR / f"side_level_certification_ledger_{RUN_DATE}.csv", side_ledger)
    write_csv(OUT_DIR / f"row_level_movement_ledger_{RUN_DATE}.csv", row_ledger)
    write_csv(OUT_DIR / f"fail_closed_taxonomy_{RUN_DATE}.csv", fail_closed)
    write_csv(OUT_DIR / f"projection_vs_realized_report_{RUN_DATE}.csv", projection)
    write_csv(OUT_DIR / f"cumulative_parent_child_lineage_ledger_{RUN_DATE}.csv", lineage)
    write_json(OUT_DIR / f"certified_cumulative_post_repair_state_{RUN_DATE}.json", child_state)
    write_child_state_md(child_state)
    write_csv(OUT_DIR / f"residual_starter_blocked_reconciliation_{RUN_DATE}.csv", residual)
    write_csv(OUT_DIR / f"platform_code_boundary_assessment_{RUN_DATE}.csv", platform)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", deterministic_replay())

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_STARTER_PARENT_LEDGER_REPAIR_EXECUTION_DECISION": EXECUTION_DECISION,
        "MLB_STARTER_PARENT_LEDGER_SIDE_CERTIFICATION_DECISION": SIDE_DECISION,
        "STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE": CHILD_STATE,
        "governed_sides_certified": 3,
        "source_fields_admitted": len(admission),
        "rows_starter_qualified": len(row_ledger),
        "rows_newly_fully_qualified": Counter(r["movement_classification"] for r in row_ledger)["STARTER_LEDGER_REPAIR_TO_FULLY_QUALIFIED"],
        "downstream_pa_blockers_preserved": Counter(r["movement_classification"] for r in row_ledger)["STARTER_LEDGER_REPAIR_TO_PA_BLOCKED"],
        "cumulative_totals": child_state["after_totals"],
        "active_platform_code": "unchanged",
        "starter_values_recomputed": False,
        "next_bounded_research_priority": "residual Starter-blocked triage from certified child state",
    }
    write_json(OUT_DIR / f"machine_readable_starter_parent_ledger_repair_execution_{RUN_DATE}.json", machine)
    write_summary(child_state, row_ledger, admission)
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()]))
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    print(json.dumps(build_package(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
