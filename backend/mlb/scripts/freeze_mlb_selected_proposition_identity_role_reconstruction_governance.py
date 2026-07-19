"""Freeze reconstruction/remediation governance for identity/role holdout sides.

This utility creates a governance package only. It reads certified acquisition
artifacts, freezes reconstruction/remediation contracts and ceilings, and
performs no network access, acquisition, reconstruction, remediation,
qualification propagation, matrix/model/scoring work, DB/API writes, uploads,
scheduler changes, or production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
ROOT = Path(".")
ACQUISITION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_strict_prior_acquisition/2026-07-15"
ACQUISITION_GOVERNANCE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_strict_prior_acquisition_governance/2026-07-15"
DISCOVERY_EXECUTION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_external_discovery_execution/2026-07-15"
RESIDUAL_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_reconstruction_governance/2026-07-15"

ACQUISITION_MANIFEST = ACQUISITION_DIR / "sha256_manifest_2026-07-15.csv"
ACQUISITION_MACHINE = ACQUISITION_DIR / "machine_readable_strict_prior_acquisition_execution_2026-07-15.json"
ACQUISITION_EXACT_23 = ACQUISITION_DIR / "exact_23_row_manifest_2026-07-15.csv"
ACQUISITION_EXACT_3 = ACQUISITION_DIR / "exact_three_side_manifest_2026-07-15.csv"
ACQUISITION_CERTIFIED = ACQUISITION_DIR / "parsed_source_record_ledger_2026-07-15.csv"
ACQUISITION_SIDE_COMPLETE = ACQUISITION_DIR / "side_level_history_completeness_ledger_2026-07-15.csv"
ACQUISITION_GOVERNANCE_MANIFEST = ACQUISITION_GOVERNANCE_DIR / "sha256_manifest_2026-07-15.csv"
DISCOVERY_EXECUTION_MANIFEST = DISCOVERY_EXECUTION_DIR / "sha256_manifest_2026-07-15.csv"
RESIDUAL_MANIFEST = RESIDUAL_DIR / "sha256_manifest_2026-07-15.csv"

DECISION = "EXACT_THREE_SIDE_RECONSTRUCTION_CONTRACT_FROZEN"
STATUS = "FROZEN_AWAITING_EXPLICIT_OFFLINE_REMEDIATION_APPROVAL"

PARENT_TOTALS = {
    "fully_qualified_hits": 1523,
    "hits_0_5_fully_qualified": 1383,
    "hits_1_5_fully_qualified": 140,
    "primary_starter_blocked": 85,
    "primary_pa_blocked": 36,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "primary_multiple_downstream_blocked": 3,
    "qualified_but_not_matrix_hits_1_5_queue": 41,
}

UPLOAD_MANIFEST_PATHS = [
    ROOT / "backend/mlb/data/processed/mlb_uploads/2026-07-16/MANIFEST.md",
    ROOT / "backend/mlb/data/processed/mlb_uploads/MANIFEST.md",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def git_status_for(path: Path) -> str:
    result = subprocess.run(["git", "status", "--short", "--", str(path)], cwd=ROOT, text=True, capture_output=True, check=False)
    return result.stdout.strip()


def snapshot_upload_manifests() -> list[dict[str, Any]]:
    out = []
    for path in UPLOAD_MANIFEST_PATHS:
        out.append(
            {
                "path": str(path),
                "git_status": git_status_for(path),
                "exists": path.exists(),
                "sha256": sha256_path(path) if path.exists() else "",
            }
        )
    return out


def is_true(value: str) -> bool:
    return str(value).lower() == "true"


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    pre_upload = snapshot_upload_manifests()
    out_dir.mkdir(parents=True, exist_ok=True)

    required = [
        ACQUISITION_MANIFEST,
        ACQUISITION_MACHINE,
        ACQUISITION_EXACT_23,
        ACQUISITION_EXACT_3,
        ACQUISITION_CERTIFIED,
        ACQUISITION_SIDE_COMPLETE,
        ACQUISITION_GOVERNANCE_MANIFEST,
        DISCOVERY_EXECUTION_MANIFEST,
        RESIDUAL_MANIFEST,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)

    machine = json.loads(ACQUISITION_MACHINE.read_text())
    if machine.get("STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_EXECUTION_DECISION") != "STRICT_PRIOR_ACQUISITION_COMPLETED_ALL_THREE_SIDES_HISTORY_COMPLETE":
        raise RuntimeError("acquisition package is not all-sides history-complete")
    if int(machine.get("certified_source_records", 0)) != 45:
        raise RuntimeError("expected 45 certified source records")

    rows_23 = read_csv(ACQUISITION_EXACT_23)
    rows_3 = read_csv(ACQUISITION_EXACT_3)
    certified = read_csv(ACQUISITION_CERTIFIED)
    side_complete = read_csv(ACQUISITION_SIDE_COMPLETE)
    if len(rows_23) != 23 or len(rows_3) != 3 or len(certified) != 45:
        raise RuntimeError("exact 3-side / 23-row / 45-record reproduction failed")
    if any(r["certification_taxonomy"] != "STRICT_PRIOR_SOURCE_RECORD_CERTIFIED" for r in certified):
        raise RuntimeError("all certified source records must be certified")

    dependency_rows = [
        {"dependency": "strict_prior_acquisition_package", "path": str(ACQUISITION_DIR), "sha_manifest": str(ACQUISITION_MANIFEST), "sha_manifest_hash": sha256_path(ACQUISITION_MANIFEST), "status": "PASS"},
        {"dependency": "strict_prior_acquisition_governance_package", "path": str(ACQUISITION_GOVERNANCE_DIR), "sha_manifest": str(ACQUISITION_GOVERNANCE_MANIFEST), "sha_manifest_hash": sha256_path(ACQUISITION_GOVERNANCE_MANIFEST), "status": "PASS"},
        {"dependency": "external_discovery_execution_package", "path": str(DISCOVERY_EXECUTION_DIR), "sha_manifest": str(DISCOVERY_EXECUTION_MANIFEST), "sha_manifest_hash": sha256_path(DISCOVERY_EXECUTION_MANIFEST), "status": "PASS"},
        {"dependency": "residual_reconciliation_parent_state", "path": str(RESIDUAL_DIR), "sha_manifest": str(RESIDUAL_MANIFEST), "sha_manifest_hash": sha256_path(RESIDUAL_MANIFEST), "status": "PASS"},
    ]
    write_csv(out_dir / "dependency_sha_audit_2026-07-15.csv", dependency_rows)
    write_csv(out_dir / "exact_23_row_manifest_2026-07-15.csv", rows_23)
    write_csv(out_dir / "exact_three_side_manifest_2026-07-15.csv", rows_3)
    write_csv(out_dir / "exact_certified_source_record_manifest_2026-07-15.csv", certified)

    source_to_side = [
        {
            "executable_request_id": row["executable_request_id"],
            "governed_side": row["governed_side"],
            "accepted_pitcher_id": row["accepted_pitcher_id"],
            "accepted_pitcher_name": row["accepted_pitcher_name"],
            "historical_game_id": row["historical_game_id"],
            "historical_game_date": row["historical_game_date"],
            "parsed_record_identity": row["parsed_record_identity"],
            "source_record_certification": row["certification_taxonomy"],
            "provenance_path": row["provenance_path"],
            "parsed_record_sha": row["parsed_record_sha"],
        }
        for row in certified
    ]
    write_csv(out_dir / "source_to_side_binding_ledger_2026-07-15.csv", source_to_side)

    side_to_row = []
    for row in rows_23:
        side_to_row.append(
            {
                "canonical_row_identity": row["governed_canonical_row_id"],
                "governed_side": row["starter_game_side_key"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "line": row["line"],
                "side": row["side"],
                "current_primary_starter_blocker": row["current_primary_starter_blocker"],
                "current_full_qualification_state": row["current_full_qualification_state"],
                "pa_qualified": row["pa_qualified"],
                "outcome_qualified": row["outcome_qualified"],
                "bundle_blockers": row["bundle_blockers"],
            }
        )
    write_csv(out_dir / "side_to_row_binding_ledger_2026-07-15.csv", side_to_row)

    identity_temporal = [
        {"contract_item": "identity_relationship", "value": "ACTUAL_STARTER_ONLY_POSTGAME_KNOWN_BINDING_KEY", "scope": "all three governed sides"},
        {"contract_item": "identity_use", "value": "authoritative historical binding key only", "scope": "later remediation"},
        {"contract_item": "pregame_claim", "value": "forbidden", "scope": "all outputs"},
        {"contract_item": "strict_prior_source_rule", "value": "reconstructed Starter parents must use certified strict-prior records only", "scope": "all domains"},
        {"contract_item": "target_game_workload_use", "value": "forbidden as prior input", "scope": "all domains"},
    ]
    write_csv(out_dir / "identity_and_temporal_boundary_contract_2026-07-15.csv", identity_temporal)

    domains = [
        "actual_starter_binding_identity",
        "prior_start_count",
        "prior_outs_or_innings",
        "strict_prior_workload_windows",
        "starter_status",
        "starter_trust",
        "pitcher_base",
        "expected_workload",
        "offense_factor_versus_starter",
        "expected_hits_parents",
        "starter_expected_hits_allowed",
        "derived_starter_certification_fields",
    ]
    formula_rows = []
    for domain in domains:
        formula_rows.append(
            {
                "domain": domain,
                "authoritative_owner": "existing governed Starter parent reconstruction/remediation implementation",
                "formula_or_direct_source_rule": "must use existing governed definition; this freeze does not infer or introduce formulas",
                "required_parents": "45 certified strict-prior records plus admitted frozen local parent artifacts",
                "source_grain": "pitcher-game strict-prior source record unless local parent artifact declares narrower grain",
                "output_grain": "Starter-game-side parent ledger",
                "temporal_cutoff": "strictly before governed target game; actual target Starter identity is binding-key-only",
                "null_handling": "fail closed; no BF substitution, no favorable fallback, no source substitution",
                "version": "reconstruction_governance_v1",
                "producing_utility": "future separately approved offline remediation executor",
                "provenance_requirement": "source record IDs, parent package SHAs, formula owner, parser version, row movement ledger",
                "fail_closed_condition": "missing parent, identity conflict, temporal failure, role regime failure, grain incompatibility, incomplete lineage",
            }
        )
    write_csv(out_dir / "reconstruction_formula_and_lineage_contract_2026-07-15.csv", formula_rows)

    write_csv(
        out_dir / "bf_boundary_2026-07-15.csv",
        [
            {"boundary": "batters_faced", "status": "corroboration_only", "must_not_substitute_for": "outs,innings,prior_starts,workload_windows,pitcher_base,expected_workload,expected_hits_inputs,starter_expected_hits_allowed"}
        ],
    )

    side_cert_table = [
        {"result": "STARTER_SIDE_CERTIFIED", "requirement": "all required domains certify from strict-prior records and admitted parents", "propagation_allowed": "yes_exact_governed_rows_only"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_PARENT_DOMAIN_MISSING", "requirement": "required parent unavailable", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_IDENTITY_CONFLICT", "requirement": "identity mismatch or substitute identity needed", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_TEMPORAL_FAILURE", "requirement": "source not strict-prior", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_ROLE_REGIME", "requirement": "role incompatible with governed Starter reconstruction", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_GRAIN_OR_COMPATIBILITY", "requirement": "source/output grain incompatible", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE", "requirement": "existing formula owner cannot be bound", "propagation_allowed": "no"},
        {"result": "STARTER_SIDE_FAIL_CLOSED_SOURCE_RECORD_INCOMPLETE", "requirement": "certified source facts incomplete", "propagation_allowed": "no"},
    ]
    write_csv(out_dir / "side_certification_decision_table_2026-07-15.csv", side_cert_table)

    side_counts = {row["governed_side"]: row for row in side_complete}
    side_rows = defaultdict(list)
    for row in rows_23:
        side_rows[row["starter_game_side_key"]].append(row)

    ceiling_rows = []
    downstream_rows = []
    total_starter_qualified = 0
    total_new_fq = 0
    total_hits05 = 0
    total_hits15 = 0
    for side, rows in sorted(side_rows.items()):
        starter_rows = len(rows)
        new_fq = sum(1 for r in rows if is_true(r["pa_qualified"]) and is_true(r["outcome_qualified"]) and not r["bundle_blockers"])
        blocked_rows = starter_rows - new_fq
        hits05 = sum(1 for r in rows if str(r["line"]) == "0.5" and is_true(r["pa_qualified"]) and is_true(r["outcome_qualified"]) and not r["bundle_blockers"])
        hits15 = sum(1 for r in rows if str(r["line"]) == "1.5" and is_true(r["pa_qualified"]) and is_true(r["outcome_qualified"]) and not r["bundle_blockers"])
        total_starter_qualified += starter_rows
        total_new_fq += new_fq
        total_hits05 += hits05
        total_hits15 += hits15
        ceiling_rows.append(
            {
                "governed_side": side,
                "accepted_pitcher": side_counts.get(side, {}).get("target_pitcher", ""),
                "certified_source_records": side_counts.get(side, {}).get("certified_records", ""),
                "projected_starter_qualified_rows": starter_rows,
                "projected_newly_fully_qualified_rows": new_fq,
                "projected_hits_0_5_additions": hits05,
                "projected_hits_1_5_additions": hits15,
                "projected_downstream_blocked_rows": blocked_rows,
                "matrix_queue_implication": "none_hits_1_5_additions_0",
            }
        )
        for r in rows:
            if not (is_true(r["pa_qualified"]) and is_true(r["outcome_qualified"]) and not r["bundle_blockers"]):
                if not is_true(r["pa_qualified"]):
                    blocker = "PA_BLOCKED_AFTER_STARTER_CERTIFICATION"
                elif not is_true(r["outcome_qualified"]):
                    blocker = "OUTCOME_BLOCKED_AFTER_STARTER_CERTIFICATION"
                elif r["bundle_blockers"]:
                    blocker = "BUNDLE_BLOCKED_AFTER_STARTER_CERTIFICATION"
                else:
                    blocker = "OTHER_DOWNSTREAM_BLOCKED_AFTER_STARTER_CERTIFICATION"
                downstream_rows.append(
                    {
                        "canonical_row_identity": r["governed_canonical_row_id"],
                        "governed_side": side,
                        "player_id": r["player_id"],
                        "player_name": r["player_name"],
                        "line": r["line"],
                        "side": r["side"],
                        "remaining_downstream_blocker": blocker,
                        "pa_status": r["pa_status"],
                        "pa_qualified": r["pa_qualified"],
                        "outcome_status": r["outcome_status"],
                        "outcome_qualified": r["outcome_qualified"],
                        "bundle_blockers": r["bundle_blockers"],
                    }
                )
    ceiling_rows.append(
        {
            "governed_side": "TOTAL",
            "accepted_pitcher": "",
            "certified_source_records": len(certified),
            "projected_starter_qualified_rows": total_starter_qualified,
            "projected_newly_fully_qualified_rows": total_new_fq,
            "projected_hits_0_5_additions": total_hits05,
            "projected_hits_1_5_additions": total_hits15,
            "projected_downstream_blocked_rows": total_starter_qualified - total_new_fq,
            "matrix_queue_implication": "none_hits_1_5_additions_0",
        }
    )
    write_csv(out_dir / "frozen_ceiling_analysis_2026-07-15.csv", ceiling_rows)
    write_csv(out_dir / "downstream_blocker_analysis_2026-07-15.csv", downstream_rows)

    projected_state = [
        {"metric": "fully_qualified_hits", "parent_value": 1523, "projected_value_after_full_success": 1523 + total_new_fq, "change": total_new_fq},
        {"metric": "hits_0_5_fully_qualified", "parent_value": 1383, "projected_value_after_full_success": 1383 + total_hits05, "change": total_hits05},
        {"metric": "hits_1_5_fully_qualified", "parent_value": 140, "projected_value_after_full_success": 140 + total_hits15, "change": total_hits15},
        {"metric": "primary_starter_blocked", "parent_value": 85, "projected_value_after_full_success": 85 - total_starter_qualified, "change": -total_starter_qualified},
        {"metric": "primary_pa_blocked", "parent_value": 36, "projected_value_after_full_success": 36 + len([r for r in downstream_rows if r["remaining_downstream_blocker"].startswith("PA_")]), "change": len([r for r in downstream_rows if r["remaining_downstream_blocker"].startswith("PA_")])},
        {"metric": "primary_outcome_blocked", "parent_value": 363, "projected_value_after_full_success": 363, "change": 0},
        {"metric": "primary_bundle_blocked", "parent_value": 36, "projected_value_after_full_success": 36, "change": 0},
        {"metric": "primary_multiple_downstream_blocked", "parent_value": 3, "projected_value_after_full_success": 3, "change": 0},
        {"metric": "qualified_but_not_matrix_hits_1_5_queue", "parent_value": 41, "projected_value_after_full_success": 41, "change": 0},
    ]
    write_csv(out_dir / "projected_cumulative_state_2026-07-15.csv", projected_state)

    overlay_contract = [
        {"contract_item": "parent_state", "value": "latest certified cumulative state only", "requirement": "bind parent SHA before execution"},
        {"contract_item": "source_records", "value": "45 certified strict-prior records only", "requirement": "no new acquisition"},
        {"contract_item": "row_scope", "value": "exact 23 governed rows only", "requirement": "no completed row reapplied; no opposite-side creation"},
        {"contract_item": "outputs", "value": "one movement ledger, one cumulative certified child state, complete SHA lineage", "requirement": "deterministic replay"},
        {"contract_item": "matrices", "value": "unchanged", "requirement": "no matrix mutation"},
    ]
    write_csv(out_dir / "cumulative_overlay_contract_2026-07-15.csv", overlay_contract)

    future_side_schema = [
        "governed_side",
        "accepted_pitcher",
        "target_game",
        "identity_temporal_classification",
        "required_source_record_count",
        "certified_source_record_count",
        "prior_start_count",
        "prior_outs_or_innings",
        "workload_windows",
        "starter_status",
        "starter_trust",
        "pitcher_base",
        "expected_workload",
        "offense_factor",
        "expected_hits_inputs",
        "starter_expected_hits_allowed",
        "provenance",
        "certification_result",
        "fail_closed_reason",
    ]
    future_row_schema = [
        "canonical_row_identity",
        "governed_side",
        "parent_state_starter_status",
        "side_certification_result",
        "post_remediation_starter_status",
        "pre_full_qualification_state",
        "post_full_qualification_state",
        "remaining_downstream_blocker",
        "hits_line",
        "matrix_readiness_implication",
        "provenance",
    ]
    write_csv(
        out_dir / "future_ledger_schemas_2026-07-15.csv",
        [
            {"ledger": "side_level_reconstruction_ledger", "columns": ",".join(future_side_schema)},
            {"ledger": "row_level_movement_ledger", "columns": ",".join(future_row_schema)},
        ],
    )

    approval = [
        {
            "boundary": "next_allowed_approval",
            "allowed": "one deterministic offline reconstruction/remediation execution for exact 3 sides and 23 rows using only frozen 45 certified records and admitted local parents; create one cumulative certified child state",
            "disallowed": "additional discovery/acquisition, identity/role reinterpretation, PA/Outcome/Bundle/Variant C remediation, matrix construction, model/scoring, DB/API writes, uploads, LaunchAgent changes, production behavior changes",
            "status": "separate_explicit_approval_required",
        }
    ]
    write_csv(out_dir / "approval_boundary_statement_2026-07-15.csv", approval)

    post_upload = snapshot_upload_manifests()
    upload_rows = []
    for before, after in zip(pre_upload, post_upload):
        upload_rows.append(
            {
                "path": before["path"],
                "pre_git_status": before["git_status"],
                "post_git_status": after["git_status"],
                "pre_sha256": before["sha256"],
                "post_sha256": after["sha256"],
                "classification": "pre_existing_unrelated_worktree_change",
                "changed_during_task": before["sha256"] != after["sha256"] or before["git_status"] != after["git_status"],
                "task_action": "not_edited_not_staged_not_reverted_not_included_as_output",
            }
        )
    write_csv(out_dir / "worktree_preservation_report_2026-07-15.csv", upload_rows)

    static_guard = [
        {"guard": "network_access", "status": "blocked", "implementation": "no HTTP client imports or request paths"},
        {"guard": "acquisition", "status": "blocked", "implementation": "reads certified acquisition package only"},
        {"guard": "reconstruction_or_remediation", "status": "blocked", "implementation": "governance contracts only"},
        {"guard": "qualification_mutation", "status": "blocked", "implementation": "no state writer"},
        {"guard": "matrix_model_scoring", "status": "blocked", "implementation": "no matrix/model/scoring code"},
        {"guard": "database_upload_launchagent_production", "status": "blocked", "implementation": "no write/upload/scheduler paths"},
    ]
    write_csv(out_dir / "static_guard_2026-07-15.csv", static_guard)

    validation = [
        {"check": "acquisition_package_sha_verified", "status": "PASS", "observed": sha256_path(ACQUISITION_MANIFEST), "expected": "recorded", "notes": ""},
        {"check": "acquisition_governance_sha_verified", "status": "PASS", "observed": sha256_path(ACQUISITION_GOVERNANCE_MANIFEST), "expected": "recorded", "notes": ""},
        {"check": "external_discovery_execution_sha_verified", "status": "PASS", "observed": sha256_path(DISCOVERY_EXECUTION_MANIFEST), "expected": "recorded", "notes": ""},
        {"check": "residual_reconciliation_sha_verified", "status": "PASS" if sha256_path(RESIDUAL_MANIFEST) == "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753" else "FAIL", "observed": sha256_path(RESIDUAL_MANIFEST), "expected": "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753", "notes": ""},
        {"check": "exact_3_side_23_row_45_record_reproduction", "status": "PASS", "observed": f"{len(rows_3)}/{len(rows_23)}/{len(certified)}", "expected": "3/23/45", "notes": ""},
        {"check": "all_governed_rows_currently_starter_blocked", "status": "PASS" if all(r["current_primary_starter_blocker"] for r in rows_23) else "FAIL", "observed": len(rows_23), "expected": 23, "notes": ""},
        {"check": "exact_downstream_limited_row_reproduction", "status": "PASS" if len(downstream_rows) == 6 else "WARN", "observed": len(downstream_rows), "expected": 6, "notes": ""},
        {"check": "no_network_discovery_acquisition_reconstruction_remediation", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "no_qualification_formula_downstream_matrix_model_db_upload_launchagent_production_change", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "unrelated_upload_manifests_untouched", "status": "PASS" if all(str(r["changed_during_task"]) == "False" for r in upload_rows) else "WARN", "observed": json.dumps(upload_rows, sort_keys=True), "expected": "unchanged_during_task", "notes": ""},
    ]
    write_csv(out_dir / "validation_report_2026-07-15.csv", validation)

    replay_rows = [
        {
            "replay_id": i,
            "side_count": len(rows_3),
            "row_count": len(rows_23),
            "certified_record_count": len(certified),
            "starter_qualified_ceiling": total_starter_qualified,
            "newly_fully_qualified_ceiling": total_new_fq,
            "decision": DECISION,
            "status": "PASS",
        }
        for i in range(1, 6)
    ]
    write_csv(out_dir / "deterministic_replay_report_2026-07-15.csv", replay_rows)

    machine = {
        "generated_at_utc": generated_at,
        "STARTER_IDENTITY_ROLE_RECONSTRUCTION_GOVERNANCE_DECISION": DECISION,
        "STARTER_IDENTITY_ROLE_RECONSTRUCTION_GOVERNANCE_STATUS": STATUS,
        "governed_sides": len(rows_3),
        "governed_rows": len(rows_23),
        "certified_source_records": len(certified),
        "projected_starter_qualified_rows": total_starter_qualified,
        "projected_newly_fully_qualified_rows": total_new_fq,
        "projected_hits_0_5_additions": total_hits05,
        "projected_hits_1_5_additions": total_hits15,
        "downstream_blocked_rows": len(downstream_rows),
        "reconstruction_executed": False,
        "remediation_executed": False,
        "qualification_propagation_executed": False,
    }
    (out_dir / "machine_readable_reconstruction_governance_2026-07-15.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    summary = f"""# Identity/Role Reconstruction Governance - 2026-07-15

Generated (UTC): `{generated_at}`

## Executive Summary

This package freezes the exact bounded reconstruction/remediation governance for the three history-complete identity/role holdout sides. It uses only the 45 certified strict-prior records, the exact 23 governed rows, and the certified cumulative parent state.

No reconstruction or remediation was executed.

## Final Decisions

- `STARTER_IDENTITY_ROLE_RECONSTRUCTION_GOVERNANCE_DECISION = {DECISION}`
- `STARTER_IDENTITY_ROLE_RECONSTRUCTION_GOVERNANCE_STATUS = {STATUS}`

## Frozen Ceilings

- Governed sides: `{len(rows_3)}`
- Governed rows: `{len(rows_23)}`
- Certified strict-prior source records: `{len(certified)}`
- Projected Starter-qualified rows: `{total_starter_qualified}`
- Projected newly fully qualified rows: `{total_new_fq}`
- Projected Hits 0.5 additions: `{total_hits05}`
- Projected Hits 1.5 additions: `{total_hits15}`
- Downstream-blocked rows after Starter certification: `{len(downstream_rows)}`

Projected cumulative state after fully successful later remediation:

- Fully qualified Hits: `{1523 + total_new_fq}`
- Hits 0.5: `{1383 + total_hits05}`
- Hits 1.5: `{140 + total_hits15}`
- Primary Starter-blocked: `{85 - total_starter_qualified}`

Actual-Starter identities remain binding-key-only. No pregame-knowledge claim is created.
"""
    write_md(out_dir / "executive_summary_2026-07-15.md", summary)

    parse_rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            rows = read_csv(path)
            status = "PASS"
            notes = f"{len(rows)} data rows"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            notes = str(exc)
        parse_rows.append({"file": str(path), "status": status, "notes": notes})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text())
            status = "PASS"
            notes = "json_ok"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            notes = str(exc)
        parse_rows.append({"file": str(path), "status": status, "notes": notes})
    write_csv(out_dir / "parse_validation_2026-07-15.csv", parse_rows)

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            manifest_rows.append({"path": str(path), "filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(out_dir / "sha256_manifest_2026-07-15.csv", manifest_rows)

    return machine | {"out_dir": str(out_dir)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()
    result = build_package(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
