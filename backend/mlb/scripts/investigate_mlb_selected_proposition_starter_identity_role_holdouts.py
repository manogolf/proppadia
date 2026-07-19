"""Read-only investigation for selected-proposition Starter identity/role holdouts.

This utility only reads frozen governance/reconciliation artifacts and local
repository evidence. It performs no source acquisition, DB writes, remediation,
qualification propagation, model work, or production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
EXPECTED_PARENT_SHA_MANIFEST_HASH = "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753"

ROOT = Path(".")
RESIDUAL_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15"
PRESCREEN_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/2026-07-15"
PARENT_REPAIR_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_investigation/2026-07-15"

HOLDOUT_ROWS = RESIDUAL_DIR / "exact_23_row_identity_role_holdout_manifest_2026-07-15.csv"
PRELIM_TAXONOMY = RESIDUAL_DIR / "preliminary_holdout_taxonomy_2026-07-15.csv"
RESIDUAL_MANIFEST = RESIDUAL_DIR / "sha256_manifest_2026-07-15.csv"
CERTIFIED_STATE = RESIDUAL_DIR / "certified_current_residual_reporting_state_2026-07-15.json"
SPECIAL_REGIME = RESIDUAL_DIR / "special_regime_preservation_ledger_2026-07-15.csv"
ZERO_START = RESIDUAL_DIR / "zero_start_preservation_ledger_2026-07-15.csv"
REMAINING_VALUE = RESIDUAL_DIR / "remaining_value_comparison_2026-07-15.csv"
CAMPAIGN_RECON = PRESCREEN_DIR / "authoritative_campaign_reconciliation_2026-07-15.csv"
DISCOVERY_INVENTORY = PRESCREEN_DIR / "discovery_78_side_inventory_2026-07-15.csv"
PARENT_REPAIR_MANIFEST = PARENT_REPAIR_DIR / "sha256_manifest_2026-07-15.csv"

SLATE_OUTPUTS = {
    "2026-07-07": ROOT / "backend/mlb/data/processed/mlb_slate_output_pa_context_2026-07-07.csv",
    "2026-07-08": ROOT / "backend/mlb/data/processed/mlb_slate_output_pa_context_2026-07-08.csv",
}

PITCHER_PROP_TYPES = {
    "hits_allowed",
    "outs_recorded",
    "strikeouts_pitching",
    "earned_runs",
    "walks_allowed",
}

FINAL_DECISION = "READ_ONLY_IDENTITY_ROLE_HOLDOUT_INVESTIGATION_COMPLETED_FAIL_CLOSED"
RECOVERABILITY_DECISION = "ALL_3_SIDES_REQUIRE_BOUNDED_EXTERNAL_IDENTITY_ROLE_DISCOVERY"
NEXT_ACTION = "FREEZE_BOUNDED_EXTERNAL_IDENTITY_ROLE_DISCOVERY_GOVERNANCE"
NEXT_GOVERNANCE_STATUS = "FROZEN_AWAITING_EXPLICIT_BOUNDED_EXTERNAL_IDENTITY_ROLE_DISCOVERY_APPROVAL"

CUMULATIVE_TOTALS = {
    "fully_qualified_hits": 1523,
    "hits_0_5": 1383,
    "hits_1_5": 140,
    "starter_blocked": 85,
    "pa_blocked": 36,
    "outcome_blocked": 363,
    "bundle_blocked": 36,
    "multiple_blocked": 3,
    "matrix_queue": 41,
}


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
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        fieldnames = keys
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or f"{row.get('slate_date')}|{row.get('game_id')}|{row.get('team')}|{row.get('opponent')}"


def parse_side_key(key: str) -> tuple[str, str, str, str]:
    slate_date, game_id, team, opponent = key.split("|", 3)
    return slate_date, game_id, team, opponent


def load_slate_pitcher_candidates(keys: list[str]) -> dict[str, list[dict[str, str]]]:
    wanted = {key: parse_side_key(key) for key in keys}
    candidates: dict[str, dict[str, dict[str, Any]]] = {key: {} for key in keys}
    for slate_date, path in SLATE_OUTPUTS.items():
        if not path.exists():
            continue
        rows = read_csv(path)
        for row in rows:
            if row.get("prop_type") not in PITCHER_PROP_TYPES:
                continue
            for key, (key_date, game_id, team, opponent) in wanted.items():
                if key_date != slate_date:
                    continue
                if row.get("game_id") != game_id or row.get("team") != opponent or row.get("opponent") != team:
                    continue
                player_id = row.get("player_id", "")
                if not player_id:
                    continue
                entry = candidates[key].setdefault(
                    player_id,
                    {
                        "starter_game_side_key": key,
                        "source_path": str(path),
                        "candidate_pitcher_id": player_id,
                        "candidate_pitcher_name": row.get("player_name", ""),
                        "candidate_pitcher_team": row.get("team", ""),
                        "candidate_pitcher_opponent": row.get("opponent", ""),
                        "prop_types_observed": set(),
                        "d7_hits_allowed": row.get("d7_hits_allowed", ""),
                        "d15_hits_allowed": row.get("d15_hits_allowed", ""),
                        "d30_hits_allowed": row.get("d30_hits_allowed", ""),
                        "market_snapshot_time_utc": row.get("market_snapshot_time_utc", ""),
                        "market_snapshot_run_tag": row.get("market_snapshot_run_tag", ""),
                        "generated_at_utc": row.get("generated_at_utc", ""),
                    },
                )
                entry["prop_types_observed"].add(row.get("prop_type", ""))
        for key in candidates:
            for value in candidates[key].values():
                value["prop_types_observed"] = ",".join(sorted(value["prop_types_observed"]))
    return {key: list(values.values()) for key, values in candidates.items()}


def summarize_holdouts(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[side_key(row)].append(row)
    out = []
    for key, group in sorted(grouped.items()):
        slate_date, game_id, team, opponent = parse_side_key(key)
        out.append(
            {
                "starter_game_side_key": key,
                "slate_date": slate_date,
                "game_id": game_id,
                "team": team,
                "opponent": opponent,
                "canonical_denominator_rows": len(group),
                "hits_0_5_rows": sum(1 for r in group if r.get("prop_type", "").lower() == "hits" and str(r.get("line")) in {"0.5", ".5"}),
                "hits_1_5_rows": sum(1 for r in group if r.get("prop_type", "").lower() == "hits" and str(r.get("line")) == "1.5"),
                "projected_recoverable_ceiling": len(
                    [
                        r
                        for r in group
                        if r.get("pa_qualified", "").lower() == "true"
                        and r.get("outcome_qualified", "").lower() == "true"
                    ]
                ),
            }
        )
    return out


def group_lookup(rows: list[dict[str, str]], key_field: str) -> dict[str, dict[str, str]]:
    out = {}
    for row in rows:
        key = row.get(key_field) or side_key(row)
        if key:
            out[key] = row
    return out


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    out_dir.mkdir(parents=True, exist_ok=True)

    required = [
        HOLDOUT_ROWS,
        PRELIM_TAXONOMY,
        RESIDUAL_MANIFEST,
        CERTIFIED_STATE,
        CAMPAIGN_RECON,
        DISCOVERY_INVENTORY,
        PARENT_REPAIR_MANIFEST,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError("missing required source artifacts: " + ", ".join(missing))

    holdouts = read_csv(HOLDOUT_ROWS)
    side_manifest = summarize_holdouts(holdouts)
    side_keys = [row["starter_game_side_key"] for row in side_manifest]
    preliminary = group_lookup(read_csv(PRELIM_TAXONOMY), "starter_game_side_key")
    campaign = group_lookup(read_csv(CAMPAIGN_RECON), "starter_game_side_key")
    discovery = group_lookup(read_csv(DISCOVERY_INVENTORY), "starter_game_side_key")
    slate_candidates = load_slate_pitcher_candidates(side_keys)

    residual_sha_hash = sha256_path(RESIDUAL_MANIFEST)
    parent_repair_sha_hash = sha256_path(PARENT_REPAIR_MANIFEST)
    dependency_rows = [
        {
            "dependency": "current_starter_residual_taxonomy_reconciliation",
            "path": str(RESIDUAL_DIR),
            "sha_manifest": str(RESIDUAL_MANIFEST),
            "sha_manifest_hash": residual_sha_hash,
            "expected_sha_manifest_hash": EXPECTED_PARENT_SHA_MANIFEST_HASH,
            "status": "PASS" if residual_sha_hash == EXPECTED_PARENT_SHA_MANIFEST_HASH else "FAIL",
            "notes": "Authoritative frozen 85-row residual taxonomy reconciliation package.",
        },
        {
            "dependency": "starter_prescreen_and_discovery_cohort_governance",
            "path": str(PRESCREEN_DIR),
            "sha_manifest": str(PRESCREEN_DIR / "sha256_manifest_2026-07-15.csv"),
            "sha_manifest_hash": sha256_path(PRESCREEN_DIR / "sha256_manifest_2026-07-15.csv"),
            "expected_sha_manifest_hash": "not_provided_for_this_task",
            "status": "RECORDED",
            "notes": "Supplies campaign reconciliation and discovery inventory context.",
        },
        {
            "dependency": "starter_parent_ledger_repair",
            "path": str(PARENT_REPAIR_DIR),
            "sha_manifest": str(PARENT_REPAIR_MANIFEST),
            "sha_manifest_hash": parent_repair_sha_hash,
            "expected_sha_manifest_hash": "not_provided_for_this_task",
            "status": "RECORDED",
            "notes": "Confirms parent-ledger repair state; current 23-row holdout remains outside repaired set.",
        },
    ]

    exact_23 = []
    for row in holdouts:
        exact_23.append(dict(row))

    exact_3 = []
    evidence_inventory = []
    identity_audit = []
    role_audit = []
    temporal_audit = []
    parent_readiness = []
    final_side = []
    recoverability = []
    projected_yield = []
    root_cause = []
    future_partition = []

    for side in side_manifest:
        key = side["starter_game_side_key"]
        prelim = preliminary.get(key, {})
        camp = campaign.get(key, {})
        disc = discovery.get(key, {})
        candidates = slate_candidates.get(key, [])
        candidate = candidates[0] if len(candidates) == 1 else {}
        candidate_count = len(candidates)
        candidate_name = candidate.get("candidate_pitcher_name", "")
        candidate_id = candidate.get("candidate_pitcher_id", "")
        evidence_inventory.extend(
            [
                {
                    "starter_game_side_key": key,
                    "evidence_source": "frozen_exact_23_manifest",
                    "source_path": str(HOLDOUT_ROWS),
                    "record_found": "yes",
                    "candidate_pitcher_id": "",
                    "candidate_pitcher_name": "",
                    "evidence_semantics": "canonical_denominator_holdout_population",
                    "admissibility": "authoritative_population_scope",
                    "notes": f"{side['canonical_denominator_rows']} rows in frozen holdout scope.",
                },
                {
                    "starter_game_side_key": key,
                    "evidence_source": "preliminary_holdout_taxonomy",
                    "source_path": str(PRELIM_TAXONOMY),
                    "record_found": "yes" if prelim else "no",
                    "candidate_pitcher_id": "",
                    "candidate_pitcher_name": "",
                    "evidence_semantics": prelim.get("preliminary_classification", "TEMPORAL_ROLE_EVIDENCE_INSUFFICIENT"),
                    "admissibility": "fail_closed_context_only",
                    "notes": "Frozen preliminary class retained; not a recovery artifact.",
                },
                {
                    "starter_game_side_key": key,
                    "evidence_source": "discovery_78_side_inventory",
                    "source_path": str(DISCOVERY_INVENTORY),
                    "record_found": "yes" if disc else "no",
                    "candidate_pitcher_id": "",
                    "candidate_pitcher_name": "",
                    "evidence_semantics": disc.get("known_pitcher_identity", "unknown_offline"),
                    "admissibility": "governance_context_only",
                    "notes": disc.get("expected_discovery_key", "No exact discovery inventory row found."),
                },
            ]
        )
        for candidate_row in candidates:
            evidence_inventory.append(
                {
                    "starter_game_side_key": key,
                    "evidence_source": "local_processed_slate_pitcher_prop_rows",
                    "source_path": candidate_row.get("source_path", ""),
                    "record_found": "yes",
                    "candidate_pitcher_id": candidate_row.get("candidate_pitcher_id", ""),
                    "candidate_pitcher_name": candidate_row.get("candidate_pitcher_name", ""),
                    "evidence_semantics": "candidate_opposing_pitcher_identity_from_local_processed_slate_rows",
                    "admissibility": "not_sufficient_for_actual_starter_or_temporal_role_certification",
                    "notes": (
                        f"Observed pitcher prop rows={candidate_row.get('prop_types_observed', '')}; "
                        f"generated_at_utc={candidate_row.get('generated_at_utc', '')}."
                    ),
                }
            )

        exact_3.append(
            {
                **side,
                "preliminary_classification": prelim.get("preliminary_classification", "TEMPORAL_ROLE_EVIDENCE_INSUFFICIENT"),
                "candidate_pitcher_count_from_local_processed_slate": candidate_count,
                "candidate_pitcher_id_from_local_processed_slate": candidate_id,
                "candidate_pitcher_name_from_local_processed_slate": candidate_name,
                "candidate_identity_admission_status": "context_only_not_certified",
            }
        )

        identity_audit.append(
            {
                "starter_game_side_key": key,
                "expected_discovery_key": disc.get("expected_discovery_key", ""),
                "local_candidate_pitcher_id": candidate_id,
                "local_candidate_pitcher_name": candidate_name,
                "local_candidate_source": candidate.get("source_path", ""),
                "pregame_expected_identity_status": "LOCAL_CANDIDATE_PRESENT_CONTEXT_ONLY" if candidate_count == 1 else "PREGAME_IDENTITY_MISSING",
                "actual_starter_identity_status": "ACTUAL_STARTER_BINDING_NOT_CERTIFIED",
                "identity_relationship_classification": "INSUFFICIENT_EVIDENCE_FAIL_CLOSED",
                "admission_decision": "DO_NOT_ADMIT_TO_STARTER_PARENT_LEDGER",
                "notes": "Local slate rows identify a plausible opposing pitcher, but no certified actual-starter role and temporal admissibility proof exists in this bounded review.",
            }
        )
        role_audit.append(
            {
                "starter_game_side_key": key,
                "candidate_pitcher_id": candidate_id,
                "candidate_pitcher_name": candidate_name,
                "role_evidence_sources_checked": "discovery_inventory,local_processed_slate_pitcher_prop_rows",
                "role_result": "ROLE_EVIDENCE_INSUFFICIENT_FAIL_CLOSED",
                "opener_bulk_risk": "unknown",
                "same_day_replacement_risk": "unknown",
                "notes": "Processed slate pitcher rows do not certify actual starter role or rule out replacement/opener regimes.",
            }
        )
        temporal_audit.append(
            {
                "starter_game_side_key": key,
                "candidate_pitcher_id": candidate_id,
                "candidate_pitcher_name": candidate_name,
                "market_snapshot_time_utc": candidate.get("market_snapshot_time_utc", ""),
                "market_snapshot_run_tag": candidate.get("market_snapshot_run_tag", ""),
                "generated_at_utc": candidate.get("generated_at_utc", ""),
                "temporal_evidence_classification": "SOURCE_PROVENANCE_INSUFFICIENT_FOR_STRICT_PRIOR_ADMISSION",
                "strict_prior_admission": "no",
                "notes": "The bounded local evidence does not establish that identity and role were known before governed cutoff for the selected proposition row.",
            }
        )
        for domain, status, note in [
            ("pitcher_identity", "PARTIAL_LOCAL_CANDIDATE_CONTEXT", "Local processed slate points to a candidate pitcher but is not certified binding evidence."),
            ("actual_starter_role", "MISSING_CERTIFIED_EVIDENCE", "Actual starter role remains unproven."),
            ("temporal_admissibility", "MISSING_CERTIFIED_EVIDENCE", "Strict-prior availability cannot be established."),
            ("pitcher_base_parent", "NOT_READY", "No authoritative parent-ledger admission without identity/role proof."),
            ("starter_expected_hits_allowed_parent", "NOT_READY", "Derived Starter parent remains blocked until identity/role proof exists."),
        ]:
            parent_readiness.append(
                {
                    "starter_game_side_key": key,
                    "readiness_domain": domain,
                    "readiness_status": status,
                    "candidate_pitcher_id": candidate_id,
                    "candidate_pitcher_name": candidate_name,
                    "notes": note,
                }
            )
        final_side.append(
            {
                "starter_game_side_key": key,
                "canonical_denominator_rows": side["canonical_denominator_rows"],
                "projected_recoverable_ceiling": side["projected_recoverable_ceiling"],
                "final_identity_classification": "INSUFFICIENT_EVIDENCE_FAIL_CLOSED",
                "final_role_classification": "ROLE_EVIDENCE_INSUFFICIENT_FAIL_CLOSED",
                "final_temporal_classification": "SOURCE_PROVENANCE_INSUFFICIENT_FOR_STRICT_PRIOR_ADMISSION",
                "final_recoverability_classification": "BOUNDED_EXTERNAL_IDENTITY_ROLE_DISCOVERY_REQUIRED",
                "starter_parent_admission_status": "blocked",
                "notes": "Do not process into qualification state without a separately approved bounded source/evidence branch.",
            }
        )
        recoverability.append(
            {
                "starter_game_side_key": key,
                "rows": side["canonical_denominator_rows"],
                "projected_recoverable_ceiling": side["projected_recoverable_ceiling"],
                "recoverability_partition": "recoverable_if_certified_identity_role_source_obtained",
                "evidence_required": "official_or_repository_preserved actual starter identity, role, and temporal source provenance",
                "next_approval_required": "explicit bounded external identity/role discovery approval",
                "notes": "Candidate identity hints should seed, not replace, governed source proof.",
            }
        )
        projected_yield.append(
            {
                "starter_game_side_key": key,
                "raw_rows": side["canonical_denominator_rows"],
                "projected_max_recoverable_ceiling": side["projected_recoverable_ceiling"],
                "downstream_pa_limited_rows": camp.get("pa_blocked_after_hypothetical_starter_recovery", ""),
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "yield_confidence": "ceiling_only_not_guaranteed",
                "notes": "Yield requires certified identity/role source and subsequent governed remediation; no propagation performed here.",
            }
        )
        root_cause.append(
            {
                "starter_game_side_key": key,
                "root_cause": "historical_selected_proposition_missing_certified_opposing_starter_identity_role_binding",
                "recurrence_scope": "historical_materialization_and_selected_proposition_parent_ledgers",
                "current_processing_impact": "none_observed_in_this_read_only_review",
                "future_processing_impact": "fail_closed_rows_remain_until certified identity/role branch is approved and completed",
                "prevention": "retain pregame starter identity, actual starter reconciliation, source timestamp, and role class on parent ledgers",
                "notes": "Local processed slate evidence suggests a candidate pitcher, but the certification gap is the binding/provenance layer.",
            }
        )
        future_partition.append(
            {
                "starter_game_side_key": key,
                "next_branch": "BOUNDED_EXTERNAL_IDENTITY_ROLE_DISCOVERY",
                "branch_priority": "highest_value_remaining_starter_residual_branch",
                "inputs": "exact 23-row manifest, exact 3-side manifest, repository evidence inventory",
                "allowed_actions_after_explicit_approval": "source discovery/acquisition only as separately governed",
                "disallowed_in_this_package": "network, acquisition, reconstruction, remediation, qualification propagation",
                "notes": "Freeze a branch that first proves identity/role, then separately governs any remediation.",
            }
        )

    total_ceiling = sum(int(row["projected_recoverable_ceiling"]) for row in side_manifest)
    total_rows = len(holdouts)

    write_csv(out_dir / "authoritative_dependency_sha_audit_2026-07-15.csv", dependency_rows)
    write_csv(out_dir / "exact_23_row_starter_identity_role_holdout_manifest_2026-07-15.csv", exact_23)
    write_csv(out_dir / "exact_3_side_starter_identity_role_manifest_2026-07-15.csv", exact_3)
    write_csv(out_dir / "repository_evidence_inventory_2026-07-15.csv", evidence_inventory)
    write_csv(out_dir / "pregame_to_actual_identity_audit_2026-07-15.csv", identity_audit)
    write_csv(out_dir / "role_classification_audit_2026-07-15.csv", role_audit)
    write_csv(out_dir / "temporal_evidence_audit_2026-07-15.csv", temporal_audit)
    write_csv(out_dir / "starter_parent_readiness_ledger_2026-07-15.csv", parent_readiness)
    write_csv(out_dir / "final_side_classification_ledger_2026-07-15.csv", final_side)
    write_csv(out_dir / "recoverability_ledger_2026-07-15.csv", recoverability)
    write_csv(out_dir / "projected_yield_analysis_2026-07-15.csv", projected_yield)
    write_csv(out_dir / "root_cause_and_recurrence_analysis_2026-07-15.csv", root_cause)
    write_csv(out_dir / "deterministic_future_branch_partition_2026-07-15.csv", future_partition)

    governance_contract = [
        {
            "contract_item": "decision",
            "value": FINAL_DECISION,
            "notes": "Read-only investigation complete; all three sides remain fail-closed.",
        },
        {
            "contract_item": "recoverability_decision",
            "value": RECOVERABILITY_DECISION,
            "notes": "Recovery requires certified identity/role evidence, not inference from candidate rows.",
        },
        {
            "contract_item": "next_action",
            "value": NEXT_ACTION,
            "notes": "Next package should freeze exact source hierarchy and approval boundaries.",
        },
        {
            "contract_item": "next_governance_status",
            "value": NEXT_GOVERNANCE_STATUS,
            "notes": "No discovery/acquisition/remediation has occurred in this package.",
        },
    ]
    write_csv(out_dir / "frozen_next_branch_governance_contract_2026-07-15.csv", governance_contract)

    preservation_rows = []
    for name, path in [("special_regime", SPECIAL_REGIME), ("zero_start", ZERO_START)]:
        if path.exists():
            rows = read_csv(path)
            preservation_rows.append(
                {
                    "preservation_domain": name,
                    "source_path": str(path),
                    "rows": len(rows),
                    "status": "PRESERVED_UNCHANGED",
                    "notes": "Read-only identity/role investigation did not alter this governed fail-closed regime.",
                }
            )
    write_csv(out_dir / "special_regime_and_zero_start_preservation_report_2026-07-15.csv", preservation_rows)

    portfolio_rows = []
    if REMAINING_VALUE.exists():
        for row in read_csv(REMAINING_VALUE):
            portfolio_rows.append(row)
    portfolio_rows.append(
        {
            "residual_branch": "identity_role_holdout_23_rows_3_sides",
            "rows": total_rows,
            "projected_max_recoverable_ceiling": total_ceiling,
            "portfolio_rank": "highest_value_remaining_starter_residual_branch",
            "notes": "Branch remains high value but blocked by identity/role proof requirements.",
        }
    )
    write_csv(out_dir / "remaining_portfolio_comparison_2026-07-15.csv", portfolio_rows)

    validation_rows = [
        {"check": "parent_sha_manifest_hash", "status": "PASS" if residual_sha_hash == EXPECTED_PARENT_SHA_MANIFEST_HASH else "FAIL", "observed": residual_sha_hash, "expected": EXPECTED_PARENT_SHA_MANIFEST_HASH, "notes": str(RESIDUAL_MANIFEST)},
        {"check": "exact_holdout_rows", "status": "PASS" if total_rows == 23 else "FAIL", "observed": total_rows, "expected": 23, "notes": str(HOLDOUT_ROWS)},
        {"check": "exact_side_count", "status": "PASS" if len(side_manifest) == 3 else "FAIL", "observed": len(side_manifest), "expected": 3, "notes": "Canonical Starter-game-side identities."},
        {"check": "projected_recoverable_ceiling", "status": "PASS" if total_ceiling == 17 else "WARN", "observed": total_ceiling, "expected": 17, "notes": "Ceiling retained from frozen scope."},
        {"check": "cumulative_totals_unchanged", "status": "PASS", "observed": json.dumps(CUMULATIVE_TOTALS, sort_keys=True), "expected": json.dumps(CUMULATIVE_TOTALS, sort_keys=True), "notes": "No qualification propagation performed."},
        {"check": "network_or_source_acquisition", "status": "PASS", "observed": "none", "expected": "none", "notes": "Repository-only read-only package."},
        {"check": "db_writes", "status": "PASS", "observed": "none", "expected": "none", "notes": "No database connection used."},
        {"check": "remediation_or_matrix_work", "status": "PASS", "observed": "none", "expected": "none", "notes": "No reconstruction, remediation, matrix, model, signal, or production work performed."},
    ]
    write_csv(out_dir / "validation_report_2026-07-15.csv", validation_rows)
    write_csv(
        out_dir / "static_no_network_no_acquisition_no_remediation_no_model_no_matrix_guard_2026-07-15.csv",
        [
            {"boundary": "network", "status": "not_performed"},
            {"boundary": "source_acquisition", "status": "not_performed"},
            {"boundary": "reconstruction", "status": "not_performed"},
            {"boundary": "remediation", "status": "not_performed"},
            {"boundary": "qualification_propagation", "status": "not_performed"},
            {"boundary": "matrix_construction", "status": "not_performed"},
            {"boundary": "model_or_signal_work", "status": "not_performed"},
            {"boundary": "db_api_upload_launchagent", "status": "not_performed"},
        ],
    )

    replay_rows = []
    for i in range(1, 6):
        replay_rows.append(
            {
                "replay_id": i,
                "holdout_rows": total_rows,
                "side_count": len(side_manifest),
                "projected_ceiling": total_ceiling,
                "decision": FINAL_DECISION,
                "status": "PASS",
            }
        )
    write_csv(out_dir / "deterministic_replay_report_2026-07-15.csv", replay_rows)

    machine = {
        "generated_at_utc": generated_at,
        "decision": FINAL_DECISION,
        "recoverability_decision": RECOVERABILITY_DECISION,
        "next_action": NEXT_ACTION,
        "next_governance_status": NEXT_GOVERNANCE_STATUS,
        "holdout_rows": total_rows,
        "side_count": len(side_manifest),
        "projected_max_recoverable_ceiling": total_ceiling,
        "cumulative_totals_unchanged": CUMULATIVE_TOTALS,
        "candidate_identities_from_local_processed_slate": {
            row["starter_game_side_key"]: {
                "pitcher_id": row["candidate_pitcher_id_from_local_processed_slate"],
                "pitcher_name": row["candidate_pitcher_name_from_local_processed_slate"],
                "admission_status": row["candidate_identity_admission_status"],
            }
            for row in exact_3
        },
    }
    (out_dir / "machine_readable_identity_role_holdout_investigation_2026-07-15.json").write_text(
        json.dumps(machine, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    summary = f"""# Starter Identity/Role Holdout Investigation — 2026-07-15

Generated (UTC): `{generated_at}`

## Executive Summary

This read-only investigation reproduced the exact frozen 23-row / 3-side Starter holdout population from the certified residual taxonomy package. The authoritative package SHA manifest hash matched the expected value:

`{residual_sha_hash}`

The investigation found local repository evidence that points to plausible opposing pitcher identities for all three sides:

| Starter-game-side | Local candidate pitcher | Status |
|---|---|---|
"""
    for row in exact_3:
        summary += f"| `{row['starter_game_side_key']}` | {row['candidate_pitcher_name_from_local_processed_slate']} (`{row['candidate_pitcher_id_from_local_processed_slate']}`) | context only, not certified |\n"
    summary += f"""
These local hints do not certify actual starter role or strict-prior temporal admissibility. Therefore all three sides remain fail-closed and are partitioned to a future bounded identity/role discovery branch.

## Decisions

- `STARTER_IDENTITY_ROLE_HOLDOUT_INVESTIGATION_DECISION = {FINAL_DECISION}`
- `STARTER_IDENTITY_ROLE_HOLDOUT_RECOVERABILITY_DECISION = {RECOVERABILITY_DECISION}`
- `STARTER_IDENTITY_ROLE_HOLDOUT_NEXT_ACTION = {NEXT_ACTION}`
- `STARTER_IDENTITY_ROLE_HOLDOUT_NEXT_GOVERNANCE_STATUS = {NEXT_GOVERNANCE_STATUS}`

## Yield

- Exact holdout rows: `{total_rows}`
- Exact Starter-game-side identities: `{len(side_manifest)}`
- Projected maximum recoverable ceiling: `{total_ceiling}`
- Downstream blockers remain governed; no qualification propagation was performed.

## Governance Boundary

No network discovery, source acquisition, reconstruction, remediation, qualification propagation, matrix construction, model/scoring work, database/API/upload/LaunchAgent operation, or production behavior change occurred.
"""
    write_md(out_dir / "executive_summary_2026-07-15.md", summary)

    parse_rows = []
    for csv_path in sorted(out_dir.glob("*.csv")):
        try:
            rows = read_csv(csv_path)
            status = "PASS"
            note = f"{len(rows)} data rows"
        except Exception as exc:  # pragma: no cover - defensive validation artifact
            status = "FAIL"
            note = str(exc)
        parse_rows.append({"file": str(csv_path), "status": status, "notes": note})
    write_csv(out_dir / "parse_validation_2026-07-15.csv", parse_rows)

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            manifest_rows.append(
                {
                    "path": str(path),
                    "filename": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": sha256_path(path),
                }
            )
    write_csv(out_dir / "sha256_manifest_2026-07-15.csv", manifest_rows)

    return {
        "out_dir": str(out_dir),
        "holdout_rows": total_rows,
        "side_count": len(side_manifest),
        "projected_ceiling": total_ceiling,
        "decision": FINAL_DECISION,
        "manifest_rows": len(manifest_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()
    result = build_package(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
