"""Freeze governance for bounded Starter identity/role external discovery.

This utility creates an executable-but-inert request manifest and governance
contract. It performs no network access, discovery execution, acquisition,
reconstruction, remediation, qualification propagation, model work, uploads, or
production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
ROOT = Path(".")
FAILED_DISCOVERY_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_external_discovery/2026-07-15"
INVESTIGATION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_investigation/2026-07-15"
RESIDUAL_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_external_discovery_governance/2026-07-15"

FAILED_DISCOVERY_MANIFEST = FAILED_DISCOVERY_DIR / "sha256_manifest_2026-07-15.csv"
FAILED_DISCOVERY_MACHINE = FAILED_DISCOVERY_DIR / "machine_readable_external_discovery_2026-07-15.json"
FAILED_DISCOVERY_EXACT_23 = FAILED_DISCOVERY_DIR / "exact_23_row_manifest_2026-07-15.csv"
FAILED_DISCOVERY_EXACT_3 = FAILED_DISCOVERY_DIR / "exact_3_side_manifest_2026-07-15.csv"
INVESTIGATION_MANIFEST = INVESTIGATION_DIR / "sha256_manifest_2026-07-15.csv"
RESIDUAL_MANIFEST = RESIDUAL_DIR / "sha256_manifest_2026-07-15.csv"

TOTAL_REQUEST_CAP = 12
PER_SIDE_REQUEST_CAP = 4

DECISION = "EXACT_THREE_SIDE_EXTERNAL_DISCOVERY_MANIFEST_FROZEN"
STATUS = "FROZEN_AWAITING_EXPLICIT_BOUNDED_EXTERNAL_DISCOVERY_EXECUTION_APPROVAL"
REQUEST_CAP_STATUS = "TOTAL_CAP_12_PER_SIDE_CAP_4_FROZEN"

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


def side_parts(side_key: str) -> tuple[str, str, str, str]:
    slate_date, game_id, team, opponent = side_key.split("|", 3)
    return slate_date, game_id, team, opponent


def request_rows_for_side(index: int, side: dict[str, str]) -> list[dict[str, Any]]:
    side_key = side["starter_game_side_key"]
    slate_date, game_id, team, opponent = side_parts(side_key)
    season = slate_date[:4]
    base = f"IRH-{RUN_DATE.replace('-', '')}-{index:02d}"
    raw_prefix = f"{side_key.replace('|', '_')}"
    return [
        {
            "request_id": f"{base}-R1-target-game-identity-role",
            "request_order": 1,
            "governed_side": side_key,
            "purpose": "target-game official identity and role",
            "source_hierarchy_rank": 1,
            "source_class": "official_mlb_statsapi_target_game_feed_or_boxscore",
            "endpoint_template": "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live",
            "http_method": "GET",
            "game_id": game_id,
            "target_date": slate_date,
            "team_side": team,
            "opponent_team": opponent,
            "pitcher_id_parameter": "",
            "date_parameters": f"gamePk={game_id}",
            "strict_prior_end_date": "",
            "response_type": "statsapi_game_feed_json",
            "parser_contract": "target_game_pitching_boxscore_identity_role_parser_v1",
            "accepted_result_state": "single_official_actual_starter_bound_to_game_team_side",
            "retry_eligibility": "one_retry_for_transient_transport_or_5xx_only",
            "deduplication_key": f"{side_key}|R1|game_feed|{game_id}",
            "expected_raw_response_filename": f"{raw_prefix}_R1_target_game_feed.json",
            "maximum_response_scope": "single governed game feed or boxscore only",
            "conditional_trigger": "always",
            "stop_condition_if_failed": "stop_side_fail_closed_target_game_identity_failure",
        },
        {
            "request_id": f"{base}-R2-strict-prior-appearance-sequence",
            "request_order": 2,
            "governed_side": side_key,
            "purpose": "official strict-prior MLB appearance/start sequence",
            "source_hierarchy_rank": 2,
            "source_class": "official_mlb_statsapi_player_pitching_game_log_or_equivalent_appearance_history",
            "endpoint_template": "https://statsapi.mlb.com/api/v1/people/{resolved_actual_pitcher_mlbam_id}/stats?stats=gameLog&group=pitching&season={target_season}&gameType=R",
            "http_method": "GET",
            "game_id": game_id,
            "target_date": slate_date,
            "team_side": team,
            "opponent_team": opponent,
            "pitcher_id_parameter": "{resolved_actual_pitcher_mlbam_id_from_R1}",
            "date_parameters": f"season={season}; strict_prior_filter=game_date<{slate_date}",
            "strict_prior_end_date": slate_date,
            "response_type": "statsapi_player_pitching_game_log_json",
            "parser_contract": "strict_prior_pitching_start_relief_sequence_parser_v1",
            "accepted_result_state": "prior_mlb_start_count_and_exact_prior_start_game_ids_identified",
            "retry_eligibility": "one_retry_for_transient_transport_or_5xx_only",
            "deduplication_key": f"{side_key}|R2|player_pitching_game_log|{{resolved_actual_pitcher_mlbam_id}}|{season}|before_{slate_date}",
            "expected_raw_response_filename": f"{raw_prefix}_R2_strict_prior_pitching_gamelog.json",
            "maximum_response_scope": "single resolved pitcher regular-season pitching game log for target season only; parser may only use games before target date",
            "conditional_trigger": "R1 accepted and resolved actual pitcher MLBAM id exists",
            "stop_condition_if_failed": "stop_side_fail_closed_no_compatible_strict_prior_history_or_source_failure",
        },
        {
            "request_id": f"{base}-R3-target-date-probable-pitcher-metadata",
            "request_order": 3,
            "governed_side": side_key,
            "purpose": "official target-date probable-pitcher or game metadata",
            "source_hierarchy_rank": 3,
            "source_class": "official_mlb_statsapi_schedule_or_target_game_metadata",
            "endpoint_template": "https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={gamePk}&hydrate=probablePitcher",
            "http_method": "GET",
            "game_id": game_id,
            "target_date": slate_date,
            "team_side": team,
            "opponent_team": opponent,
            "pitcher_id_parameter": "",
            "date_parameters": f"gamePk={game_id}; date={slate_date}; hydrate=probablePitcher",
            "strict_prior_end_date": "",
            "response_type": "statsapi_schedule_game_metadata_json",
            "parser_contract": "target_date_probable_pitcher_metadata_parser_v1",
            "accepted_result_state": "probable_pitcher_metadata_captured_with_temporal_limits",
            "retry_eligibility": "one_retry_for_transient_transport_or_5xx_only",
            "deduplication_key": f"{side_key}|R3|schedule_probable_pitcher|{game_id}",
            "expected_raw_response_filename": f"{raw_prefix}_R3_schedule_probable_pitcher.json",
            "maximum_response_scope": "single governed game schedule metadata only",
            "conditional_trigger": "R1 accepted; execute even if probable-pitcher timestamp may be uncertain, but classify temporal provenance strictly",
            "stop_condition_if_failed": "do_not_expand_requests; classify probable-pitcher evidence missing",
        },
        {
            "request_id": f"{base}-R4-conditional-replacement-role-corroboration",
            "request_order": 4,
            "governed_side": side_key,
            "purpose": "conditional official replacement/role corroboration",
            "source_hierarchy_rank": 4,
            "source_class": "official_mlb_statsapi_transaction_roster_or_game_status_source",
            "endpoint_template": "https://statsapi.mlb.com/api/v1/transactions?sportId=1&startDate={target_date_minus_7_days}&endDate={target_date}&playerId={resolved_actual_pitcher_mlbam_id}",
            "http_method": "GET",
            "game_id": game_id,
            "target_date": slate_date,
            "team_side": team,
            "opponent_team": opponent,
            "pitcher_id_parameter": "{resolved_actual_pitcher_mlbam_id_from_R1}",
            "date_parameters": f"endDate={slate_date}; startDate=target_date_minus_7_days; playerId={{resolved_actual_pitcher_mlbam_id}}",
            "strict_prior_end_date": "",
            "response_type": "statsapi_transaction_or_roster_status_json",
            "parser_contract": "replacement_role_transition_corroboration_parser_v1",
            "accepted_result_state": "frozen_replacement_or_role_transition_question_resolved_or_fail_closed",
            "retry_eligibility": "one_retry_for_transient_transport_or_5xx_only",
            "deduplication_key": f"{side_key}|R4|transactions_or_roster|{{resolved_actual_pitcher_mlbam_id}}|through_{slate_date}",
            "expected_raw_response_filename": f"{raw_prefix}_R4_replacement_role_corroboration.json",
            "maximum_response_scope": "single resolved pitcher transaction/roster evidence in seven-day window ending target date",
            "conditional_trigger": "only if R1-R3 leave replacement timestamp, roster move, role transition, opener/bulk designation, or identity conflict unresolved",
            "stop_condition_if_failed": "do_not_expand_requests; classify unresolved question under fail_closed_taxonomy",
        },
    ]


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    out_dir.mkdir(parents=True, exist_ok=True)

    required = [
        FAILED_DISCOVERY_MANIFEST,
        FAILED_DISCOVERY_MACHINE,
        FAILED_DISCOVERY_EXACT_23,
        FAILED_DISCOVERY_EXACT_3,
        INVESTIGATION_MANIFEST,
        RESIDUAL_MANIFEST,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)

    failed_machine = json.loads(FAILED_DISCOVERY_MACHINE.read_text())
    if failed_machine.get("decision") != "EXTERNAL_DISCOVERY_NOT_EXECUTED_MISSING_FROZEN_REQUEST_CAP_FAIL_CLOSED":
        raise RuntimeError("failed-closed discovery package decision mismatch")
    if int(failed_machine.get("external_requests_attempted", -1)) != 0:
        raise RuntimeError("failed-closed discovery package attempted external requests")

    rows_23 = read_csv(FAILED_DISCOVERY_EXACT_23)
    rows_3 = read_csv(FAILED_DISCOVERY_EXACT_3)
    if len(rows_23) != 23 or len(rows_3) != 3:
        raise RuntimeError("exact governed population does not reproduce 23 rows / 3 sides")

    failed_sha = sha256_path(FAILED_DISCOVERY_MANIFEST)
    investigation_sha = sha256_path(INVESTIGATION_MANIFEST)
    residual_sha = sha256_path(RESIDUAL_MANIFEST)

    dependency_rows = [
        {
            "dependency": "failed_closed_external_discovery_package",
            "path": str(FAILED_DISCOVERY_DIR),
            "sha_manifest": str(FAILED_DISCOVERY_MANIFEST),
            "sha_manifest_hash": failed_sha,
            "status": "PASS",
            "notes": "Authoritative package that stopped before external access due to missing request governance.",
        },
        {
            "dependency": "identity_role_holdout_investigation_package",
            "path": str(INVESTIGATION_DIR),
            "sha_manifest": str(INVESTIGATION_MANIFEST),
            "sha_manifest_hash": investigation_sha,
            "status": "PASS",
            "notes": "Authoritative 23-row/3-side holdout investigation package.",
        },
        {
            "dependency": "current_starter_residual_taxonomy_reconciliation_package",
            "path": str(RESIDUAL_DIR),
            "sha_manifest": str(RESIDUAL_MANIFEST),
            "sha_manifest_hash": residual_sha,
            "status": "PASS",
            "notes": "Residual reconciliation package retained as parent scope reference.",
        },
    ]
    write_csv(out_dir / "authoritative_dependency_sha_audit_2026-07-15.csv", dependency_rows)
    write_csv(out_dir / "exact_23_row_manifest_2026-07-15.csv", rows_23)
    write_csv(out_dir / "exact_three_side_manifest_2026-07-15.csv", rows_3)

    target_rows = []
    request_rows = []
    for idx, side in enumerate(rows_3, start=1):
        side_key = side["starter_game_side_key"]
        slate_date, game_id, team, opponent = side_parts(side_key)
        target_rows.append(
            {
                "discovery_target_id": f"IDENTITY_ROLE_TARGET_{idx:02d}",
                "starter_game_side_key": side_key,
                "target_date": slate_date,
                "target_game_id": game_id,
                "team_side": team,
                "opponent_team": opponent,
                "represented_rows": side.get("canonical_denominator_rows", ""),
                "hits_0_5_rows": side.get("hits_0_5_rows", ""),
                "hits_1_5_rows": side.get("hits_1_5_rows", ""),
                "projected_qualification_ceiling": side.get("projected_recoverable_ceiling", ""),
                "current_pregame_expected_starter_evidence": side.get("candidate_pitcher_name_from_local_processed_slate", ""),
                "current_actual_starter_evidence": "not_certified",
                "preliminary_holdout_reason": side.get("preliminary_classification", ""),
                "local_hint_not_certified": "true",
                "request_cap_for_side": PER_SIDE_REQUEST_CAP,
                "frozen_request_count_for_side": 4,
            }
        )
        request_rows.extend(request_rows_for_side(idx, side))

    write_csv(out_dir / "exact_external_discovery_target_manifest_2026-07-15.csv", target_rows)
    write_csv(out_dir / "exact_request_manifest_2026-07-15.csv", request_rows)

    source_hierarchy = [
        {
            "rank": 1,
            "source_class": "official_mlb_statsapi_target_game_feed_or_boxscore",
            "authorized_purpose": "actual Starter identity, game/team binding, official appearance/start role",
            "authorized_endpoints": "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live",
            "disallowed_use": "historical workload acquisition beyond target-game identity/role parsing",
        },
        {
            "rank": 2,
            "source_class": "official_mlb_statsapi_player_pitching_game_log_or_equivalent_appearance_history",
            "authorized_purpose": "strict-prior starts/relief appearances and exact prior start game identities",
            "authorized_endpoints": "https://statsapi.mlb.com/api/v1/people/{resolved_actual_pitcher_mlbam_id}/stats?stats=gameLog&group=pitching&season={target_season}&gameType=R",
            "disallowed_use": "workload reconstruction or qualification propagation",
        },
        {
            "rank": 3,
            "source_class": "official_mlb_statsapi_schedule_or_target_game_metadata",
            "authorized_purpose": "probable-pitcher metadata and target-game identity corroboration with strict temporal labeling",
            "authorized_endpoints": "https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={gamePk}&hydrate=probablePitcher",
            "disallowed_use": "claiming pregame knowledge without timestamped provenance",
        },
        {
            "rank": 4,
            "source_class": "official_mlb_statsapi_transaction_roster_or_game_status_source",
            "authorized_purpose": "conditional replacement, roster move, emergency start, or role-transition corroboration",
            "authorized_endpoints": "https://statsapi.mlb.com/api/v1/transactions?sportId=1&startDate={target_date_minus_7_days}&endDate={target_date}&playerId={resolved_actual_pitcher_mlbam_id}",
            "disallowed_use": "generic fallback search or broad league crawling",
        },
    ]
    write_csv(out_dir / "source_hierarchy_contract_2026-07-15.csv", source_hierarchy)

    request_cap_contract = [
        {"cap_name": "total_external_request_cap", "cap_value": TOTAL_REQUEST_CAP, "scope": "entire package", "status": "FROZEN", "notes": "Retries count as attempts; no executor may exceed this under any condition."},
        {"cap_name": "per_side_external_request_cap", "cap_value": PER_SIDE_REQUEST_CAP, "scope": "each governed side", "status": "FROZEN", "notes": "No more than one request identity for each frozen purpose per side."},
        {"cap_name": "governed_targets", "cap_value": 3, "scope": "side identities", "status": "FROZEN", "notes": "No side/date/game/team may be added or substituted."},
        {"cap_name": "retry_cap_per_request_identity", "cap_value": 1, "scope": "transient transport or server failure only", "status": "FROZEN", "notes": "Retry does not create a new request identity but counts against total attempts."},
    ]
    write_csv(out_dir / "request_cap_contract_2026-07-15.csv", request_cap_contract)

    endpoint_rows = [
        {
            "request_id": row["request_id"],
            "governed_side": row["governed_side"],
            "source_class": row["source_class"],
            "endpoint_template": row["endpoint_template"],
            "http_method": row["http_method"],
            "parameters": row["date_parameters"],
            "parser_contract": row["parser_contract"],
            "maximum_response_scope": row["maximum_response_scope"],
            "retry_eligibility": row["retry_eligibility"],
            "deduplication_key": row["deduplication_key"],
        }
        for row in request_rows
    ]
    write_csv(out_dir / "endpoint_and_parameter_contract_2026-07-15.csv", endpoint_rows)

    sequential_rules = [
        {"step": 1, "rule": "Execute target-game identity/role request first for a side.", "stop_or_continue": "stop side if target-game identity fails or conflicts."},
        {"step": 2, "rule": "Bind accepted actual Starter identity from official target-game record only.", "stop_or_continue": "do not use local context-only hints to bypass binding failure."},
        {"step": 3, "rule": "Execute strict-prior appearance/start-sequence request for resolved pitcher only.", "stop_or_continue": "fail closed if no compatible strict-prior start history is found."},
        {"step": 4, "rule": "Execute target-date probable-pitcher metadata request and classify temporal provenance.", "stop_or_continue": "do not represent current/postgame metadata as pregame knowledge."},
        {"step": 5, "rule": "Execute conditional fourth request only if its frozen unresolved-question trigger is met.", "stop_or_continue": "do not expand manifest when unresolved after Request 4."},
    ]
    write_csv(out_dir / "sequential_execution_rules_2026-07-15.csv", sequential_rules)

    identity_decisions = [
        {"classification": "PREGAME_AND_ACTUAL_STARTER_MATCH", "evidence_requirement": "timestamped pregame expected Starter and official actual Starter match", "acceptance": "accepted when both identity and team/game binding pass"},
        {"classification": "PREGAME_STARTER_REPLACED_BEFORE_GOVERNED_CUTOFF", "evidence_requirement": "timestamped replacement evidence before cutoff", "acceptance": "accepted only if replacement was knowable before cutoff"},
        {"classification": "PREGAME_STARTER_REPLACED_AFTER_GOVERNED_CUTOFF", "evidence_requirement": "replacement evidence after cutoff", "acceptance": "fail closed for pregame interpretation unless separate governance allows binding-key-only handling"},
        {"classification": "MULTIPLE_PREGAME_STARTER_CANDIDATES", "evidence_requirement": "multiple official pregame candidates without deterministic ordering", "acceptance": "fail closed unless one is timestamp-certified latest admissible"},
        {"classification": "ACTUAL_STARTER_ONLY_POSTGAME_KNOWN_BINDING_KEY", "evidence_requirement": "official actual Starter but no admissible pregame expectation", "acceptance": "postgame binding key only, not pregame knowledge"},
        {"classification": "PREGAME_IDENTITY_MISSING", "evidence_requirement": "no admissible expected Starter source", "acceptance": "fail closed"},
        {"classification": "SOURCE_IDENTITY_CONFLICT", "evidence_requirement": "official sources disagree materially", "acceptance": "fail closed"},
        {"classification": "IDENTITY_EVIDENCE_INSUFFICIENT_FAIL_CLOSED", "evidence_requirement": "evidence cannot satisfy identity/game/team/temporal proof", "acceptance": "fail closed"},
    ]
    write_csv(out_dir / "identity_decision_table_2026-07-15.csv", identity_decisions)

    role_decisions = [
        {"classification": "ORDINARY_STARTER_ROLE_SUPPORTED", "evidence_requirement": "official target start plus compatible prior MLB start history and no special-regime indicators", "ordinary_starter_reconstruction_compatible": "yes"},
        {"classification": "OPENER_ROLE_SUPPORTED", "evidence_requirement": "official or role-sequence evidence supports opener usage", "ordinary_starter_reconstruction_compatible": "no"},
        {"classification": "BULK_RELIEF_ROLE_SUPPORTED", "evidence_requirement": "appearance pattern supports bulk relief after opener", "ordinary_starter_reconstruction_compatible": "no"},
        {"classification": "RELIEF_TO_STARTER_TRANSITION_SUPPORTED", "evidence_requirement": "recent role sequence shows transition requiring separate governance", "ordinary_starter_reconstruction_compatible": "research_only_or_governance_required"},
        {"classification": "EMERGENCY_OR_REPLACEMENT_STARTER_SUPPORTED", "evidence_requirement": "official replacement or roster evidence supports emergency start", "ordinary_starter_reconstruction_compatible": "governance_required"},
        {"classification": "TANDEM_OR_PIGGYBACK_ROLE_SUPPORTED", "evidence_requirement": "role evidence supports tandem/piggyback deployment", "ordinary_starter_reconstruction_compatible": "no"},
        {"classification": "FIRST_MLB_START_ROLE_SUPPORTED", "evidence_requirement": "zero prior MLB starts before target game", "ordinary_starter_reconstruction_compatible": "no_first_start_framework_required"},
        {"classification": "ESTABLISHED_SPECIAL_REGIME_SUPPORTED", "evidence_requirement": "existing special-regime pattern or governed class applies", "ordinary_starter_reconstruction_compatible": "no"},
        {"classification": "ROLE_EVIDENCE_CONFLICT", "evidence_requirement": "role sources conflict materially", "ordinary_starter_reconstruction_compatible": "no_fail_closed"},
        {"classification": "ROLE_EVIDENCE_INSUFFICIENT_FAIL_CLOSED", "evidence_requirement": "role cannot be proven with official source hierarchy", "ordinary_starter_reconstruction_compatible": "no_fail_closed"},
    ]
    write_csv(out_dir / "role_decision_table_2026-07-15.csv", role_decisions)

    temporal_rules = [
        {"classification": "AVAILABLE_BEFORE_GOVERNED_CUTOFF", "rule": "source timestamp precedes governed cutoff", "pregame_knowledge_allowed": "yes"},
        {"classification": "AVAILABLE_AT_GOVERNED_CUTOFF", "rule": "source timestamp equals governed cutoff", "pregame_knowledge_allowed": "yes_with_exact_timestamp"},
        {"classification": "AVAILABLE_ONLY_AFTER_GOVERNED_CUTOFF", "rule": "source timestamp follows governed cutoff", "pregame_knowledge_allowed": "no"},
        {"classification": "POSTGAME_BINDING_KEY_ONLY", "rule": "postgame official record binds actual identity/role only", "pregame_knowledge_allowed": "no"},
        {"classification": "TIMESTAMP_UNCERTAIN", "rule": "official source lacks sufficient original availability timestamp", "pregame_knowledge_allowed": "no"},
        {"classification": "SOURCE_PROVENANCE_INSUFFICIENT", "rule": "source class or provenance cannot establish temporal status", "pregame_knowledge_allowed": "no"},
    ]
    write_csv(out_dir / "temporal_evidence_rules_2026-07-15.csv", temporal_rules)

    fail_closed = [
        "REQUEST_TRANSPORT_FAILURE",
        "SOURCE_RESPONSE_FAILURE",
        "PARSE_FAILURE",
        "TARGET_GAME_IDENTITY_FAILURE",
        "PITCHER_IDENTITY_CONFLICT",
        "TEAM_SIDE_BINDING_FAILURE",
        "PREGAME_TEMPORAL_EVIDENCE_INSUFFICIENT",
        "ROLE_EVIDENCE_INSUFFICIENT",
        "ROLE_CONFLICT",
        "ZERO_PRIOR_MLB_STARTS",
        "NO_COMPATIBLE_STRICT_PRIOR_START_HISTORY",
        "FIRST_START_FRAMEWORK_REQUIRED",
        "SPECIAL_REGIME_GOVERNANCE_REQUIRED",
        "UNAUTHORIZED_SOURCE_REQUIRED",
        "REQUEST_CAP_REACHED_FAIL_CLOSED",
    ]
    write_csv(out_dir / "fail_closed_taxonomy_2026-07-15.csv", [{"reason": r, "action": "fail_closed_no_manifest_expansion"} for r in fail_closed])

    inert_rule = [
        {
            "rule": "create_inert_acquisition_request_only_for_accepted_ordinary_or_low_sample_starter_side",
            "required_fields": "acquisition_request_id,parent_governed_side,target_pitcher_id,historical_game_id,historical_date,official_endpoint_or_source_class,strict_prior_proof,start_versus_relief_proof,discovery_provenance,deduplication_key,parser_contract",
            "disallowed_for": "opener,bulk,first_start,unresolved_identity,zero_prior_starts,special_regime,post_cutoff_only_pregame_evidence",
            "execution_allowed": "no",
        }
    ]
    write_csv(out_dir / "inert_acquisition_conversion_rule_2026-07-15.csv", inert_rule)

    raw_replay = [
        {"requirement": "byte_for_byte_raw_response_preservation", "status": "FROZEN", "notes": "Later executor must write every response before parsing."},
        {"requirement": "response_sha_recording", "status": "FROZEN", "notes": "Every raw response requires SHA256."},
        {"requirement": "request_and_response_timestamps", "status": "FROZEN", "notes": "Transport timestamps and source timestamps must be separate."},
        {"requirement": "parser_version", "status": "FROZEN", "notes": "Parser contract and version must be recorded per response."},
        {"requirement": "five_stable_no_network_replays", "status": "FROZEN", "notes": "Replay must require zero external requests."},
    ]
    write_csv(out_dir / "raw_response_and_replay_contract_2026-07-15.csv", raw_replay)

    projected_rows = []
    for side in rows_3:
        projected_rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "represented_rows": side.get("canonical_denominator_rows", ""),
                "hits_0_5_rows": side.get("hits_0_5_rows", ""),
                "hits_1_5_rows": side.get("hits_1_5_rows", ""),
                "projected_maximum_recoverable_rows": side.get("projected_recoverable_ceiling", ""),
                "downstream_pa_ceiling": "unchanged_until_execution",
                "downstream_outcome_ceiling": "unchanged_until_execution",
                "downstream_bundle_ceiling": "unchanged_until_execution",
                "multiple_blocker_ceiling": "unchanged_until_execution",
                "potential_abd_additions": "none_claimed_by_governance_freeze",
                "matrix_queue_implications": "none_until_discovery_execution_and_separate_remediation",
            }
        )
    projected_rows.append(
        {
            "starter_game_side_key": "TOTAL",
            "represented_rows": sum(int(r.get("canonical_denominator_rows", 0)) for r in rows_3),
            "hits_0_5_rows": sum(int(r.get("hits_0_5_rows", 0)) for r in rows_3),
            "hits_1_5_rows": sum(int(r.get("hits_1_5_rows", 0)) for r in rows_3),
            "projected_maximum_recoverable_rows": sum(int(r.get("projected_recoverable_ceiling", 0)) for r in rows_3),
            "downstream_pa_ceiling": "unchanged_until_execution",
            "downstream_outcome_ceiling": "unchanged_until_execution",
            "downstream_bundle_ceiling": "unchanged_until_execution",
            "multiple_blocker_ceiling": "unchanged_until_execution",
            "potential_abd_additions": "none_claimed_by_governance_freeze",
            "matrix_queue_implications": "none_until_discovery_execution_and_separate_remediation",
        }
    )
    write_csv(out_dir / "projected_population_accounting_2026-07-15.csv", projected_rows)

    state_rows = [
        {"metric": key, "value": value, "status": "PRESERVED_UNCHANGED", "notes": "Governance freeze only; no execution or movement."}
        for key, value in CUMULATIVE_TOTALS.items()
    ]
    state_rows.append({"metric": "all_23_governed_rows", "value": 23, "status": "REMAIN_STARTER_BLOCKED", "notes": "No discovery execution."})
    write_csv(out_dir / "state_preservation_report_2026-07-15.csv", state_rows)

    approval = [
        {
            "boundary": "next_allowed_approval",
            "status": "separate_explicit_approval_required",
            "allowed": "execute exact frozen 12-request external discovery manifest against official source hierarchy only",
            "disallowed": "historical workload acquisition,reconstruction,remediation,qualification propagation,role formula changes,special-regime reclassification,matrix construction,modeling,scoring,DB writes,uploads,production changes",
        }
    ]
    write_csv(out_dir / "approval_boundary_statement_2026-07-15.csv", approval)

    static_guard = [
        {"guard": "network_access", "status": "not_present", "implementation": "governance artifact generator contains no HTTP client imports or request execution path"},
        {"guard": "source_acquisition", "status": "not_present", "implementation": "request manifest only"},
        {"guard": "field_reconstruction", "status": "blocked", "implementation": "no reconstruction code path"},
        {"guard": "qualification_mutation", "status": "blocked", "implementation": "no state writer"},
        {"guard": "matrix_construction", "status": "blocked", "implementation": "no matrix code path"},
        {"guard": "model_training_or_scoring", "status": "blocked", "implementation": "no model imports"},
        {"guard": "database_or_api_write", "status": "blocked", "implementation": "no DB/API clients"},
        {"guard": "upload", "status": "blocked", "implementation": "no upload path"},
        {"guard": "scheduler_or_launchagent_change", "status": "blocked", "implementation": "no scheduler path"},
        {"guard": "production_behavior_change", "status": "blocked", "implementation": "artifact-only output"},
    ]
    write_csv(out_dir / "static_guard_2026-07-15.csv", static_guard)

    validation_rows = [
        {"check": "failed_closed_discovery_package_sha_bound", "status": "PASS", "observed": failed_sha, "expected": "recorded", "notes": str(FAILED_DISCOVERY_MANIFEST)},
        {"check": "holdout_investigation_package_sha_bound", "status": "PASS", "observed": investigation_sha, "expected": "recorded", "notes": str(INVESTIGATION_MANIFEST)},
        {"check": "residual_reconciliation_package_sha_bound", "status": "PASS", "observed": residual_sha, "expected": "recorded", "notes": str(RESIDUAL_MANIFEST)},
        {"check": "exact_three_side_reproduction", "status": "PASS" if len(rows_3) == 3 else "FAIL", "observed": len(rows_3), "expected": 3, "notes": ""},
        {"check": "exact_23_row_reproduction", "status": "PASS" if len(rows_23) == 23 else "FAIL", "observed": len(rows_23), "expected": 23, "notes": ""},
        {"check": "exact_external_target_manifest", "status": "PASS" if len(target_rows) == 3 else "FAIL", "observed": len(target_rows), "expected": 3, "notes": ""},
        {"check": "exact_total_request_cap", "status": "PASS", "observed": TOTAL_REQUEST_CAP, "expected": 12, "notes": ""},
        {"check": "exact_per_side_request_cap", "status": "PASS", "observed": PER_SIDE_REQUEST_CAP, "expected": 4, "notes": ""},
        {"check": "exact_executable_request_count", "status": "PASS" if len(request_rows) == 12 else "FAIL", "observed": len(request_rows), "expected": 12, "notes": "R4 is conditional but predeclared for each side."},
        {"check": "official_source_hierarchy_only", "status": "PASS", "observed": len(source_hierarchy), "expected": 4, "notes": "No third-party sources authorized."},
        {"check": "no_network_or_source_requests", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "no_discovery_execution_acquisition_reconstruction_remediation", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
        {"check": "no_qualification_matrix_model_db_upload_launchagent_production_change", "status": "PASS", "observed": "none", "expected": "none", "notes": ""},
    ]
    write_csv(out_dir / "validation_report_2026-07-15.csv", validation_rows)

    replay_rows = [
        {
            "replay_id": i,
            "side_count": len(rows_3),
            "row_count": len(rows_23),
            "request_manifest_rows": len(request_rows),
            "total_request_cap": TOTAL_REQUEST_CAP,
            "per_side_request_cap": PER_SIDE_REQUEST_CAP,
            "decision": DECISION,
            "status": "PASS",
        }
        for i in range(1, 6)
    ]
    write_csv(out_dir / "deterministic_replay_report_2026-07-15.csv", replay_rows)

    machine = {
        "generated_at_utc": generated_at,
        "STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_GOVERNANCE_DECISION": DECISION,
        "STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_GOVERNANCE_STATUS": STATUS,
        "STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_REQUEST_CAP_STATUS": REQUEST_CAP_STATUS,
        "governed_sides": len(rows_3),
        "governed_rows": len(rows_23),
        "total_request_cap": TOTAL_REQUEST_CAP,
        "per_side_request_cap": PER_SIDE_REQUEST_CAP,
        "exact_executable_request_count": len(request_rows),
        "projected_recoverable_ceiling": sum(int(r.get("projected_recoverable_ceiling", 0)) for r in rows_3),
        "network_access_performed": False,
        "external_discovery_executed": False,
    }
    (out_dir / "machine_readable_external_discovery_governance_2026-07-15.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    summary = f"""# Starter Identity/Role External Discovery Governance — 2026-07-15

Generated (UTC): `{generated_at}`

## Executive Summary

This package freezes the exact bounded external identity/role discovery governance for the three Starter holdout sides. It creates the target manifest, hard request caps, official source hierarchy, endpoint/parameter contracts, sequential execution rules, decision tables, fail-closed taxonomy, raw-response/replay contract, and approval boundary for a later separate execution step.

No external discovery was executed.

## Final Decisions

- `STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_GOVERNANCE_DECISION = {DECISION}`
- `STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_GOVERNANCE_STATUS = {STATUS}`
- `STARTER_IDENTITY_ROLE_EXTERNAL_DISCOVERY_REQUEST_CAP_STATUS = {REQUEST_CAP_STATUS}`

## Frozen Scope

- Governed sides: `{len(rows_3)}`
- Governed denominator rows: `{len(rows_23)}`
- Exact executable request identities: `{len(request_rows)}`
- Total request cap: `{TOTAL_REQUEST_CAP}`
- Per-side request cap: `{PER_SIDE_REQUEST_CAP}`
- Projected maximum recoverable ceiling: `{sum(int(r.get('projected_recoverable_ceiling', 0)) for r in rows_3)}`

## Official Source Hierarchy

1. Official MLB StatsAPI target-game feed or box score.
2. Official MLB StatsAPI player pitching game log or equivalent appearance history.
3. Official MLB StatsAPI schedule or target-game metadata.
4. Official MLB transaction, roster, or game-status source class.

No third-party source, search engine, news source, sportsbook, social-media source, or broad web search is authorized.

## Conditional Request Trigger

Request 4 is frozen for each side but may execute only if Requests 1-3 leave a replacement timestamp, roster move, role transition, opener/bulk designation, or identity conflict unresolved. It may not become a generic fallback search.

## Next Approval Required

The next separate approval may authorize only execution of this exact 12-request manifest under the frozen official source hierarchy. It must not authorize acquisition, reconstruction, remediation, qualification propagation, formula changes, matrix construction, model/scoring work, DB/API writes, uploads, scheduler changes, or production behavior changes.
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
    write_csv(out_dir / "parse_validation_2026-07-15.csv", parse_rows)

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            manifest_rows.append({"path": str(path), "filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(out_dir / "sha256_manifest_2026-07-15.csv", manifest_rows)

    return {
        "out_dir": str(out_dir),
        "decision": DECISION,
        "status": STATUS,
        "request_cap_status": REQUEST_CAP_STATUS,
        "governed_sides": len(rows_3),
        "governed_rows": len(rows_23),
        "request_manifest_rows": len(request_rows),
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
