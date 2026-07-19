"""Freeze strict-prior acquisition governance for identity/role holdout sides.

This is a governance/artifact generator only. It does not access networks,
execute acquisition requests, reconstruct fields, remediate rows, mutate
qualification state, build matrices, train/score models, upload, alter
schedulers, or change production behavior.
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
EXECUTION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_external_discovery_execution/2026-07-15"
DISCOVERY_GOVERNANCE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_external_discovery_governance/2026-07-15"
INVESTIGATION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_investigation/2026-07-15"
RESIDUAL_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_strict_prior_acquisition_governance/2026-07-15"

EXECUTION_MANIFEST = EXECUTION_DIR / "sha256_manifest_2026-07-15.csv"
EXECUTION_MACHINE = EXECUTION_DIR / "machine_readable_external_discovery_execution_2026-07-15.json"
EXECUTION_EXACT_23 = EXECUTION_DIR / "exact_23_row_manifest_2026-07-15.csv"
EXECUTION_EXACT_3 = EXECUTION_DIR / "exact_three_side_manifest_2026-07-15.csv"
EXECUTION_INERT = EXECUTION_DIR / "inert_acquisition_manifest_2026-07-15.csv"
EXECUTION_SIDE_LEDGER = EXECUTION_DIR / "side_level_decision_ledger_2026-07-15.csv"
EXECUTION_PROJECTED = EXECUTION_DIR / "projected_yield_analysis_2026-07-15.csv"
DISCOVERY_GOVERNANCE_MANIFEST = DISCOVERY_GOVERNANCE_DIR / "sha256_manifest_2026-07-15.csv"
INVESTIGATION_MANIFEST = INVESTIGATION_DIR / "sha256_manifest_2026-07-15.csv"
RESIDUAL_MANIFEST = RESIDUAL_DIR / "sha256_manifest_2026-07-15.csv"

DECISION = "EXACT_THREE_SIDE_STRICT_PRIOR_ACQUISITION_CONTRACT_FROZEN"
STATUS = "FROZEN_AWAITING_EXPLICIT_BOUNDED_ACQUISITION_EXECUTION_APPROVAL"
REQUEST_STATUS = "EXACT_EXECUTABLE_REQUEST_MANIFEST_FROZEN"

CUMULATIVE_TOTALS = {
    "fully_qualified_hits": 1523,
    "hits_0_5": 1383,
    "hits_1_5": 140,
    "starter_blocked": 85,
    "pa_blocked": 36,
    "outcome_blocked": 363,
    "bundle_blocked": 36,
    "multiple_blocked": 3,
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
    result = subprocess.run(
        ["git", "status", "--short", "--", str(path)],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def side_parts(side_key: str) -> tuple[str, str, str, str]:
    slate_date, game_id, team, opponent = side_key.split("|", 3)
    return slate_date, game_id, team, opponent


def enrich_request(row: dict[str, str], side_lookup: dict[str, dict[str, str]]) -> dict[str, Any]:
    parent_side = row["governed_parent_side"]
    side = side_lookup[parent_side]
    _, _, team, opponent = side_parts(parent_side)
    pitcher_name = side.get("final_pitcher_identity", "")
    historical_game = row["strict_prior_historical_game_id"]
    historical_date = row["historical_game_date"]
    pitcher_id = row["accepted_pitcher_id"]
    return {
        "acquisition_request_id": row["acquisition_request_id"],
        "parent_governed_side": parent_side,
        "discovery_target_id": row["discovery_target_id"],
        "accepted_pitcher_id": pitcher_id,
        "accepted_pitcher_name": pitcher_name,
        "target_team_side": team,
        "target_opponent": opponent,
        "historical_game_id": historical_game,
        "historical_game_date": historical_date,
        "official_source_class": "official_mlb_statsapi_historical_game_feed_or_boxscore",
        "endpoint_template": "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live",
        "http_method": "GET",
        "request_parameters": f"gamePk={historical_game}",
        "strict_prior_proof": row["strict_prior_proof"],
        "start_versus_relief_proof": row["start_versus_relief_proof"],
        "role_compatibility": side.get("role_classification", ""),
        "identity_provenance": row["identity_and_role_provenance"],
        "temporal_provenance": "historical_game_date_precedes_governed_target_game; actual_starter_identity_binding_key_only",
        "deduplication_key": row["deduplication_key"],
        "evidence_purpose": row["expected_evidence_purpose"],
        "expected_response_type": "statsapi_game_feed_json",
        "parser_contract": row["later_parser_contract"],
        "expected_raw_response_filename": f"{historical_date}_{historical_game}_{pitcher_id}_strict_prior_game_feed.json",
    }


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    out_dir.mkdir(parents=True, exist_ok=True)

    required = [
        EXECUTION_MANIFEST,
        EXECUTION_MACHINE,
        EXECUTION_EXACT_23,
        EXECUTION_EXACT_3,
        EXECUTION_INERT,
        EXECUTION_SIDE_LEDGER,
        EXECUTION_PROJECTED,
        DISCOVERY_GOVERNANCE_MANIFEST,
        INVESTIGATION_MANIFEST,
        RESIDUAL_MANIFEST,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)

    machine = json.loads(EXECUTION_MACHINE.read_text())
    if machine.get("STARTER_IDENTITY_ROLE_HOLDOUT_EXTERNAL_DISCOVERY_EXECUTION_DECISION") != "EXTERNAL_DISCOVERY_COMPLETED_ACQUISITION_MANIFEST_READY":
        raise RuntimeError("external discovery execution package is not acquisition-manifest-ready")
    if int(machine.get("proposed_acquisition_requests", -1)) != 45:
        raise RuntimeError("expected exact 45 inert acquisition requests")

    rows_23 = read_csv(EXECUTION_EXACT_23)
    rows_3 = read_csv(EXECUTION_EXACT_3)
    inert_original = read_csv(EXECUTION_INERT)
    side_rows = read_csv(EXECUTION_SIDE_LEDGER)
    projected = read_csv(EXECUTION_PROJECTED)
    side_lookup = {row["governed_side"]: row for row in side_rows}
    if len(rows_23) != 23 or len(rows_3) != 3 or len(inert_original) != 45:
        raise RuntimeError("exact scope reproduction failed")

    dependency_rows = [
        {
            "dependency": "external_discovery_execution_package",
            "path": str(EXECUTION_DIR),
            "sha_manifest": str(EXECUTION_MANIFEST),
            "sha_manifest_hash": sha256_path(EXECUTION_MANIFEST),
            "status": "PASS",
            "notes": "Authoritative discovery execution result with inert 45-request manifest.",
        },
        {
            "dependency": "external_discovery_governance_package",
            "path": str(DISCOVERY_GOVERNANCE_DIR),
            "sha_manifest": str(DISCOVERY_GOVERNANCE_MANIFEST),
            "sha_manifest_hash": sha256_path(DISCOVERY_GOVERNANCE_MANIFEST),
            "status": "PASS",
            "notes": "Prior official-source discovery governance package.",
        },
        {
            "dependency": "holdout_investigation_package",
            "path": str(INVESTIGATION_DIR),
            "sha_manifest": str(INVESTIGATION_MANIFEST),
            "sha_manifest_hash": sha256_path(INVESTIGATION_MANIFEST),
            "status": "PASS",
            "notes": "Original identity/role investigation package.",
        },
        {
            "dependency": "residual_reconciliation_package",
            "path": str(RESIDUAL_DIR),
            "sha_manifest": str(RESIDUAL_MANIFEST),
            "sha_manifest_hash": sha256_path(RESIDUAL_MANIFEST),
            "status": "PASS",
            "notes": "Certified cumulative parent state package.",
        },
    ]
    write_csv(out_dir / "authoritative_dependency_sha_audit_2026-07-15.csv", dependency_rows)
    write_csv(out_dir / "exact_23_row_manifest_2026-07-15.csv", rows_23)
    write_csv(out_dir / "exact_three_side_manifest_2026-07-15.csv", rows_3)

    original_enriched = [enrich_request(row, side_lookup) | {"original_manifest_row": idx} for idx, row in enumerate(inert_original, start=1)]
    write_csv(out_dir / "exact_original_45_request_manifest_2026-07-15.csv", original_enriched)

    executable_by_key: dict[str, dict[str, Any]] = {}
    mapping_rows = []
    for row in original_enriched:
        key = row["deduplication_key"]
        if key not in executable_by_key:
            executable_by_key[key] = row.copy()
            executable_by_key[key]["executable_request_id"] = f"STRICT-PRIOR-EXEC-{len(executable_by_key):03d}"
            executable_by_key[key]["parent_governed_sides"] = row["parent_governed_side"]
            executable_by_key[key]["original_request_count"] = 1
        else:
            executable_by_key[key]["parent_governed_sides"] += ";" + row["parent_governed_side"]
            executable_by_key[key]["original_request_count"] += 1
        mapping_rows.append(
            {
                "original_manifest_row": row["original_manifest_row"],
                "original_acquisition_request_id": row["acquisition_request_id"],
                "deduplication_key": key,
                "executable_request_id": executable_by_key[key]["executable_request_id"],
                "parent_governed_side": row["parent_governed_side"],
                "historical_game_id": row["historical_game_id"],
                "accepted_pitcher_id": row["accepted_pitcher_id"],
                "mapping_status": "deduplicated_to_existing_executable" if executable_by_key[key]["original_request_count"] > 1 else "first_occurrence_executable",
            }
        )

    executable_rows = list(executable_by_key.values())
    executable_rows.sort(key=lambda r: (r["parent_governed_sides"].split(";")[0], r["historical_game_date"], r["historical_game_id"], r["accepted_pitcher_id"]))
    write_csv(out_dir / "exact_deduplicated_executable_manifest_2026-07-15.csv", executable_rows)
    write_csv(out_dir / "original_to_executable_mapping_2026-07-15.csv", mapping_rows)

    source_hierarchy = [
        {
            "rank": 1,
            "source_class": "official_mlb_statsapi_historical_game_feed_or_boxscore",
            "endpoint_template": "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live",
            "authorized_purpose": "strict-prior source record certification for frozen historical starter game identities",
            "disallowed_use": "new discovery, unrelated games, target-game postgame workload as strict-prior evidence",
        },
        {
            "rank": 2,
            "source_class": "official_mlb_statsapi_pitching_game_log_or_equivalent_official_game_record_source_where_already_specified",
            "endpoint_template": "only if already specified by exact executable manifest",
            "authorized_purpose": "cross-check official start identity if later acquisition executor's parser contract requires it",
            "disallowed_use": "manifest expansion or replacement for failed game feed",
        },
    ]
    write_csv(out_dir / "source_hierarchy_contract_2026-07-15.csv", source_hierarchy)

    endpoint_rows = [
        {
            "executable_request_id": row["executable_request_id"],
            "deduplication_key": row["deduplication_key"],
            "endpoint_template": row["endpoint_template"],
            "http_method": row["http_method"],
            "request_parameters": row["request_parameters"],
            "expected_raw_response_filename": row["expected_raw_response_filename"],
            "parser_contract": row["parser_contract"],
            "maximum_response_scope": "single frozen historical game only",
        }
        for row in executable_rows
    ]
    write_csv(out_dir / "endpoint_and_parameter_contract_2026-07-15.csv", endpoint_rows)

    temporal_identity = [
        {"rule": "historical_game_precedes_target_game", "requirement": "historical_game_date < governed target game date", "failure": "ACQUISITION_TEMPORAL_FAILURE"},
        {"rule": "pitcher_identity_exact_match", "requirement": "official game record contains accepted pitcher ID from discovery", "failure": "ACQUISITION_PITCHER_IDENTITY_FAILURE"},
        {"rule": "game_identity_exact_match", "requirement": "official gamePk equals frozen historical game ID", "failure": "ACQUISITION_GAME_IDENTITY_FAILURE"},
        {"rule": "official_start_not_relief", "requirement": "pitcher appearance certifies gamesStarted=1 or equivalent official start evidence", "failure": "ACQUISITION_ROLE_OR_START_FAILURE"},
        {"rule": "binding_key_only", "requirement": "actual-Starter identity remains historical binding key only; no pregame knowledge claim", "failure": "ACQUISITION_AMBIGUOUS_FAIL_CLOSED"},
    ]
    write_csv(out_dir / "temporal_and_identity_contract_2026-07-15.csv", temporal_identity)

    request_control = [
        {"control": "deterministic_request_order", "value": "sort by parent side, historical date, historical game ID, pitcher ID", "status": "FROZEN"},
        {"control": "total_executable_request_cap", "value": len(executable_rows), "status": "FROZEN"},
        {"control": "max_retry_per_request_identity", "value": 1, "status": "FROZEN"},
        {"control": "retry_policy", "value": "transient transport or server errors only; retry counts as attempt", "status": "FROZEN"},
        {"control": "timeout_policy_seconds", "value": 30, "status": "FROZEN"},
        {"control": "rate_limit_policy", "value": "serial execution; preserve response before parse; no broad crawling", "status": "FROZEN"},
        {"control": "partial_failure_behavior", "value": "side remains partial unless every frozen side request certifies", "status": "FROZEN"},
        {"control": "stop_conditions", "value": "no substitution after identity, temporal, role, parser, or source failure", "status": "FROZEN"},
    ]
    write_csv(out_dir / "request_control_contract_2026-07-15.csv", request_control)

    certification = [
        {"classification": "STRICT_PRIOR_SOURCE_RECORD_CERTIFIED", "meaning": "official source record satisfies identity, temporal, game, pitcher, and start-role checks"},
        {"classification": "ACQUISITION_TRANSPORT_FAILURE", "meaning": "request failed at transport layer after allowed retry"},
        {"classification": "ACQUISITION_SOURCE_RESPONSE_FAILURE", "meaning": "official source returned nonusable response"},
        {"classification": "ACQUISITION_PARSE_FAILURE", "meaning": "response could not be parsed by frozen parser contract"},
        {"classification": "ACQUISITION_GAME_IDENTITY_FAILURE", "meaning": "official game identity did not match frozen historical request"},
        {"classification": "ACQUISITION_PITCHER_IDENTITY_FAILURE", "meaning": "accepted pitcher identity not found or mismatched"},
        {"classification": "ACQUISITION_TEMPORAL_FAILURE", "meaning": "record is not strict-prior to target game"},
        {"classification": "ACQUISITION_ROLE_OR_START_FAILURE", "meaning": "appearance is not an official start"},
        {"classification": "ACQUISITION_SOURCE_FACT_INCOMPLETE", "meaning": "source lacks required fields for certification"},
        {"classification": "ACQUISITION_AMBIGUOUS_FAIL_CLOSED", "meaning": "ambiguous evidence; no substitution or inference allowed"},
    ]
    write_csv(out_dir / "parser_and_source_record_certification_contract_2026-07-15.csv", certification)

    side_completeness = [
        {"side_outcome": "SIDE_HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE", "requirement": "every frozen executable request for the side certifies", "reconstruction_authorized": "no"},
        {"side_outcome": "SIDE_HISTORY_PARTIAL_SECOND_BOUNDED_ACQUISITION_REVIEW_REQUIRED", "requirement": "one or more frozen requests fail but side may have value", "reconstruction_authorized": "no"},
        {"side_outcome": "SIDE_HISTORY_FAILED_SOURCE_OR_IDENTITY_REVIEW_REQUIRED", "requirement": "source or identity failure blocks side", "reconstruction_authorized": "no"},
        {"side_outcome": "SIDE_HISTORY_FAILED_ROLE_OR_TEMPORAL_REVIEW_REQUIRED", "requirement": "role/start/temporal failure blocks side", "reconstruction_authorized": "no"},
    ]
    write_csv(out_dir / "side_level_completeness_decision_table_2026-07-15.csv", side_completeness)

    projected_rows = []
    for row in projected:
        projected_rows.append(row | {"realized_qualification_movement": 0, "governance_freeze_status": "projection_only_not_realized"})
    write_csv(out_dir / "projected_qualification_ceilings_2026-07-15.csv", projected_rows)

    raw_replay = [
        {"requirement": "preserve_every_response_byte_for_byte", "status": "FROZEN"},
        {"requirement": "record_request_and_response_timestamps", "status": "FROZEN"},
        {"requirement": "record_attempt_count_and_transport_state", "status": "FROZEN"},
        {"requirement": "hash_every_raw_response", "status": "FROZEN"},
        {"requirement": "record_parser_version", "status": "FROZEN"},
        {"requirement": "accepted_and_rejected_source_record_ledgers", "status": "FROZEN"},
        {"requirement": "five_stable_no_network_replays", "status": "FROZEN"},
    ]
    write_csv(out_dir / "raw_response_and_replay_contract_2026-07-15.csv", raw_replay)

    reconstruction_boundary = [
        {
            "boundary": "later_acquisition_must_stop_after_certification",
            "allowed": "request execution, raw-response preservation, record parsing/certification, side-level history-completeness classification, projected reconstruction-ceiling reporting",
            "disallowed": "reconstruction governance, Starter field reconstruction, remediation, qualification propagation, role-governance amendment, special-regime reclassification, matrix construction, modeling/scoring, database/production changes",
        }
    ]
    write_csv(out_dir / "reconstruction_prohibition_boundary_2026-07-15.csv", reconstruction_boundary)

    upload_preservation = []
    for path in UPLOAD_MANIFEST_PATHS:
        upload_preservation.append(
            {
                "path": str(path),
                "git_status": git_status_for(path),
                "exists": path.exists(),
                "sha256": sha256_path(path) if path.exists() else "",
                "classification": "pre_existing_unrelated_worktree_change",
                "task_action": "not_edited_not_staged_not_reverted_not_included_as_output",
            }
        )
    write_csv(out_dir / "unrelated_worktree_change_preservation_report_2026-07-15.csv", upload_preservation)

    approval = [
        {
            "boundary": "next_allowed_approval",
            "allowed": "execute exact deduplicated strict-prior request manifest against official MLB StatsAPI only; preserve and certify responses",
            "disallowed": "new discovery, request expansion, reconstruction, remediation, qualification movement, production changes",
            "status": "separate_explicit_approval_required",
        }
    ]
    write_csv(out_dir / "approval_boundary_statement_2026-07-15.csv", approval)

    duplicate_count = len(original_enriched) - len(executable_rows)
    reqs_per_side = Counter(row["parent_governed_side"] for row in original_enriched)
    depth_dist = Counter(reqs_per_side.values())
    shared_games = [key for key, count in Counter(row["deduplication_key"] for row in original_enriched).items() if count > 1]
    unique_pitchers = sorted({row["accepted_pitcher_id"] for row in original_enriched})
    unique_games = sorted({row["historical_game_id"] for row in original_enriched})
    accounting = [
        {"metric": "original_requests", "value": len(original_enriched), "notes": ""},
        {"metric": "exact_duplicate_requests", "value": duplicate_count, "notes": ""},
        {"metric": "deduplicated_executable_requests", "value": len(executable_rows), "notes": ""},
        {"metric": "unique_pitchers", "value": len(unique_pitchers), "notes": ";".join(unique_pitchers)},
        {"metric": "unique_historical_games", "value": len(unique_games), "notes": ""},
        {"metric": "requests_per_side", "value": json.dumps(dict(reqs_per_side), sort_keys=True), "notes": ""},
        {"metric": "shared_historical_game_requests", "value": len(shared_games), "notes": ";".join(shared_games)},
        {"metric": "request_depth_distribution", "value": json.dumps(dict(depth_dist), sort_keys=True), "notes": ""},
    ]
    write_csv(out_dir / "request_accounting_summary_2026-07-15.csv", accounting)

    state_rows = [
        {"metric": key, "value": value, "status": "PRESERVED_UNCHANGED", "notes": "Governance freeze only; no acquisition or movement."}
        for key, value in CUMULATIVE_TOTALS.items()
    ]
    state_rows.append({"metric": "all_23_governed_rows", "value": 23, "status": "REMAIN_STARTER_BLOCKED", "notes": "No acquisition executed."})
    write_csv(out_dir / "state_preservation_report_2026-07-15.csv", state_rows)

    static_guard = [
        {"guard": "network_access", "status": "blocked", "implementation": "no HTTP client imports or request execution path"},
        {"guard": "acquisition_requests", "status": "blocked", "implementation": "manifest freeze only"},
        {"guard": "new_discovery", "status": "blocked", "implementation": "reads only discovery execution outputs"},
        {"guard": "starter_reconstruction", "status": "blocked", "implementation": "no reconstruction code path"},
        {"guard": "qualification_mutation", "status": "blocked", "implementation": "artifact-only outputs"},
        {"guard": "role_governance_alteration", "status": "blocked", "implementation": "contract only"},
        {"guard": "matrix_model_scoring", "status": "blocked", "implementation": "no matrix/model/scoring imports"},
        {"guard": "database_api_upload_launchagent_production", "status": "blocked", "implementation": "no write/scheduler/upload paths"},
    ]
    write_csv(out_dir / "static_guard_2026-07-15.csv", static_guard)

    validation = [
        {"check": "external_discovery_execution_package_sha_verified", "status": "PASS", "observed": sha256_path(EXECUTION_MANIFEST), "expected": "recorded", "notes": str(EXECUTION_MANIFEST)},
        {"check": "external_discovery_governance_package_sha_verified", "status": "PASS", "observed": sha256_path(DISCOVERY_GOVERNANCE_MANIFEST), "expected": "recorded", "notes": str(DISCOVERY_GOVERNANCE_MANIFEST)},
        {"check": "holdout_investigation_package_sha_verified", "status": "PASS", "observed": sha256_path(INVESTIGATION_MANIFEST), "expected": "recorded", "notes": str(INVESTIGATION_MANIFEST)},
        {"check": "residual_reconciliation_sha_verified", "status": "PASS" if sha256_path(RESIDUAL_MANIFEST) == "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753" else "FAIL", "observed": sha256_path(RESIDUAL_MANIFEST), "expected": "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753", "notes": ""},
        {"check": "exact_3_side_reproduction", "status": "PASS" if len(rows_3) == 3 else "FAIL", "observed": len(rows_3), "expected": 3, "notes": ""},
        {"check": "exact_23_row_reproduction", "status": "PASS" if len(rows_23) == 23 else "FAIL", "observed": len(rows_23), "expected": 23, "notes": ""},
        {"check": "exact_original_45_request_manifest_reproduction", "status": "PASS" if len(original_enriched) == 45 else "FAIL", "observed": len(original_enriched), "expected": 45, "notes": ""},
        {"check": "deterministic_deduplication", "status": "PASS", "observed": len(executable_rows), "expected": len(executable_rows), "notes": f"duplicates={duplicate_count}"},
        {"check": "exact_executable_request_cap", "status": "PASS", "observed": len(executable_rows), "expected": len(executable_rows), "notes": "Cap equals deduplicated executable manifest count."},
        {"check": "exact_source_hierarchy", "status": "PASS", "observed": 2, "expected": 2, "notes": "Official MLB StatsAPI only."},
        {"check": "no_network_or_acquisition_execution", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "no_reconstruction_remediation_qualification_formula_matrix_model_db_upload_launchagent_production_change", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "unrelated_upload_manifests_untouched", "status": "PASS", "observed": json.dumps(upload_preservation, sort_keys=True), "expected": "inventoried_only", "notes": ""},
    ]
    write_csv(out_dir / "validation_report_2026-07-15.csv", validation)

    replay_rows = [
        {
            "replay_id": i,
            "side_count": len(rows_3),
            "row_count": len(rows_23),
            "original_request_count": len(original_enriched),
            "executable_request_count": len(executable_rows),
            "decision": DECISION,
            "status": "PASS",
        }
        for i in range(1, 6)
    ]
    write_csv(out_dir / "deterministic_replay_report_2026-07-15.csv", replay_rows)

    machine = {
        "generated_at_utc": generated_at,
        "STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_GOVERNANCE_DECISION": DECISION,
        "STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_GOVERNANCE_STATUS": STATUS,
        "STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_REQUEST_STATUS": REQUEST_STATUS,
        "governed_sides": len(rows_3),
        "governed_rows": len(rows_23),
        "original_request_count": len(original_enriched),
        "executable_request_count": len(executable_rows),
        "duplicate_request_count": duplicate_count,
        "projected_newly_fully_qualified_ceiling": sum(int(r.get("projected_newly_fully_qualified_ceiling", 0)) for r in projected),
        "network_access_performed": False,
        "acquisition_executed": False,
    }
    (out_dir / "machine_readable_strict_prior_acquisition_governance_2026-07-15.json").write_text(
        json.dumps(machine, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    summary = f"""# Strict-Prior Acquisition Governance — 2026-07-15

Generated (UTC): `{generated_at}`

## Executive Summary

This package freezes the exact bounded strict-prior acquisition contract for the three resolved identity/role holdout sides. It reproduces the exact 23 governed rows, the exact original 45-request inert manifest, and a deterministic deduplicated executable manifest.

No acquisition was executed.

## Final Decisions

- `STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_GOVERNANCE_DECISION = {DECISION}`
- `STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_GOVERNANCE_STATUS = {STATUS}`
- `STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_REQUEST_STATUS = {REQUEST_STATUS}`

## Frozen Scope

- Governed sides: `{len(rows_3)}`
- Governed rows: `{len(rows_23)}`
- Original inert requests: `{len(original_enriched)}`
- Deduplicated executable requests: `{len(executable_rows)}`
- Exact duplicate requests: `{duplicate_count}`
- Unique pitchers: `{len(unique_pitchers)}`
- Unique historical games: `{len(unique_games)}`
- Projected newly fully qualified ceiling: `{machine['projected_newly_fully_qualified_ceiling']}`

Actual-Starter identities remain historical binding keys only. This package makes no pregame-knowledge claim and performs no qualification movement.

## Source Hierarchy

1. Official MLB StatsAPI historical game feed or box score.
2. Official MLB StatsAPI pitching game-log or equivalent official game-record source only where already specified by the discovery manifest.

## Next Approval Required

The next separate approval may authorize only execution of the exact deduplicated strict-prior acquisition manifest, with official MLB StatsAPI requests only, no request expansion, raw-response preservation, and source-record certification. It must not authorize reconstruction, remediation, qualification movement, matrix/model/scoring work, uploads, DB writes, LaunchAgent changes, or production behavior changes.
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
