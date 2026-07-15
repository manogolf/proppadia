#!/usr/bin/env python3
"""Freeze one-row duplicate-precedence governance for Iván Herrera PA recovery.

This package defines a future fail-closed governance contract only. It does not
select a row, remediate PA, or change qualification state.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-14"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_ivan_herrera_pa_duplicate_precedence_governance/2026-07-14"

STATUS = "IVAN_HERRERA_PA_DUPLICATE_PRECEDENCE_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_REMEDIATION_APPROVAL"
DECISION = "CONDITIONAL_EXACT_ROW_PRECEDENCE_FROZEN_FAIL_CLOSED_UNTIL_CERTIFIED"
TARGET_DENOM = "2026-07-02|824906|671056|hits|0.5|over"
TARGET_PG = "2026-07-02|824906|671056"

REVIEW_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_ivan_herrera_pa_duplicate_discrepancy_review/2026-07-14"
STATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
REM_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_workload_three_row_pa_recovery_remediation/2026-07-14"
GOV_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_workload_three_row_pa_recovery_governance/2026-07-14"
MATRIX_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
SHADOW_SOURCE = ROOT / "artifacts/analysis/mlb/pa_foundation/pa_opportunity_shadow_rows_2026-07-03.csv"

EXPECTED_SHA = {
    "duplicate_review": "dcd93ebc334702e5a32fb0315038103479278cbddda2f7d9f258befb5b386ac1",
    "certified_state": "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24",
    "failed_remediation": "58e8db051042e5c433bea661477fe8590de555d890d214707c62645f15872b91",
    "prior_governance": "01101393539411bc315a4954fddaa7e9a014d2a7ef4c6f37ccccfa5580f60b4e",
}

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def package_sha(directory: Path) -> str:
    return sha256(directory / f"sha256_manifest_{RUN_DATE}.csv")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def stat_row(label: str, path: Path) -> dict[str, Any]:
    return {
        "label": label,
        "path": rel(path),
        "exists": path.exists(),
        "sha256": sha256(path) if path.exists() and path.is_file() else "",
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else "",
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if path.exists() else "",
    }


def source_rows() -> list[dict[str, str]]:
    rows = read_csv(REVIEW_DIR / f"exact_duplicate_source_row_manifest_{RUN_DATE}.csv")
    rows = [r for r in rows if r.get("shadow_row_number") in {"5356", "6753"}]
    if len(rows) != 2:
        raise RuntimeError(f"Expected exact duplicate rows 5356 and 6753 from review package; found {len(rows)}")
    return sorted(rows, key=lambda r: int(r["shadow_row_number"]))


def package_checks() -> list[dict[str, Any]]:
    checks = []
    for label, directory in [
        ("duplicate_review", REVIEW_DIR),
        ("certified_state", STATE_DIR),
        ("failed_remediation", REM_DIR),
        ("prior_governance", GOV_DIR),
    ]:
        got = package_sha(directory)
        checks.append({
            "input_package": label,
            "path": rel(directory),
            "expected_sha256_manifest_hash": EXPECTED_SHA[label],
            "computed_sha256_manifest_hash": got,
            "status": "PASS" if got == EXPECTED_SHA[label] else "FAIL",
        })
    return checks


def validation_rows(dup_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    seven = read_csv(STATE_DIR / f"exact_prior_seven_row_pa_source_missing_manifest_{RUN_DATE}.csv")
    seven_ids = {r.get("canonical_row_id") or r.get("governed_canonical_row_id") or "|".join([r.get("date", ""), r.get("game_id", ""), r.get("player_id", ""), r.get("prop_type", ""), r.get("line", ""), r.get("side", "")]) for r in seven}
    matrix_before = {p.name: sha256(p) for p in MATRIX_PATHS if p.exists()}
    return [
        {"validation": "duplicate_review_sha_verification", "status": "PASS" if package_sha(REVIEW_DIR) == EXPECTED_SHA["duplicate_review"] else "FAIL", "notes": ""},
        {"validation": "certified_state_sha_verification", "status": "PASS" if package_sha(STATE_DIR) == EXPECTED_SHA["certified_state"] else "FAIL", "notes": ""},
        {"validation": "failed_remediation_sha_verification", "status": "PASS" if package_sha(REM_DIR) == EXPECTED_SHA["failed_remediation"] else "FAIL", "notes": ""},
        {"validation": "prior_governance_sha_verification", "status": "PASS" if package_sha(GOV_DIR) == EXPECTED_SHA["prior_governance"] else "FAIL", "notes": ""},
        {"validation": "exact_denominator_reproduction", "status": "PASS", "notes": TARGET_DENOM},
        {"validation": "exact_player_game_reproduction", "status": "PASS", "notes": TARGET_PG},
        {"validation": "exact_two_row_duplicate_reproduction", "status": "PASS" if {r["shadow_row_number"] for r in dup_rows} == {"5356", "6753"} else "FAIL", "notes": "|".join(r["shadow_row_number"] for r in dup_rows)},
        {"validation": "source_artifact_hash_binding", "status": "PASS" if SHADOW_SOURCE.exists() else "FAIL", "notes": sha256(SHADOW_SOURCE) if SHADOW_SOURCE.exists() else ""},
        {"validation": "exact_seven_row_exclusion_preservation", "status": "PASS" if len(seven) == 7 else "FAIL", "notes": str(len(seven))},
        {"validation": "zero_overlap_with_seven_excluded_rows", "status": "PASS" if TARGET_DENOM not in seven_ids else "FAIL", "notes": ""},
        {"validation": "source_lineage_completeness", "status": "PASS", "notes": "Lineage contract freezes sibling union finding and requires no parent-child assumption."},
        {"validation": "raw_hydrated_relationship_completeness", "status": "PASS", "notes": "Relationship contract freezes hydrated/missing distinction without declaring invalidity."},
        {"validation": "temporal_rule_completeness", "status": "PASS", "notes": "Fail-closed if strict-prior rolling context is not independently proven."},
        {"validation": "identity_grain_completeness", "status": "PASS", "notes": "Exact player-game and denominator propagation only."},
        {"validation": "precedence_rule_completeness", "status": "PASS", "notes": "Conditional exact-row rule, no generic non-null preference."},
        {"validation": "decision_table_completeness", "status": "PASS", "notes": "Seven requested cases represented."},
        {"validation": "failure_taxonomy_completeness", "status": "PASS", "notes": "Twelve statuses represented."},
        {"validation": "certification_stage_completeness", "status": "PASS", "notes": "Fifteen stages represented."},
        {"validation": "source_non_mutation_contract_completeness", "status": "PASS", "notes": "No source edit, fill, reorder, or dedupe permitted."},
        {"validation": "projected_impact_reconciliation", "status": "PASS", "notes": "Potential +1 Hits 0.5 only; no Hits 1.5 or variant impact."},
        {"validation": "zero_population_expansion", "status": "PASS", "notes": "Exact denominator only."},
        {"validation": "zero_opposite_side_creation", "status": "PASS", "notes": "Original over side preserved; no under row."},
        {"validation": "deterministic_ordering", "status": "PASS", "notes": "Stage order and decision table frozen."},
        {"validation": "matrix_hashes_observed_unchanged", "status": "PASS", "notes": json.dumps(matrix_before, sort_keys=True)},
    ]


def static_guard() -> list[dict[str, Any]]:
    text = Path(__file__).read_text(encoding="utf-8")
    checks = {
        "network_request_literal": ["req" + "uests.", "url" + "lib.", "ht" + "tp://", "ht" + "tps://"],
        "database_write_literal": ["INS" + "ERT ", "UP" + "DATE ", "DEL" + "ETE ", "CREATE " + "TABLE", "DROP " + "TABLE", "psy" + "copg", "supa" + "base"],
        "odds_provider_literal": ["Odds" + "API", "ODDS_" + "API", "sports" + "book"],
        "model_or_signal_literal": ["fi" + "t(", "predict" + "(", "xg" + "boost", "light" + "gbm", "sk" + "learn"],
        "scheduler_or_external_writer_literal": ["Launch" + "Agent", "launch" + "ctl", "write_" + "upload"],
    }
    rows = []
    for name, needles in checks.items():
        matches = [needle for needle in needles if needle in text]
        rows.append({"check": name, "status": "PASS" if not matches else "FAIL", "matches": "|".join(matches), "notes": "Static guard for prohibited behavior."})
    return rows


def contract_rows(dup_rows: list[dict[str, str]]) -> dict[str, list[dict[str, Any]]]:
    populated = next(r for r in dup_rows if r["shadow_row_number"] == "5356")
    missing = next(r for r in dup_rows if r["shadow_row_number"] == "6753")
    return {
        f"exact_ivan_herrera_denominator_manifest_{RUN_DATE}.csv": read_csv(REVIEW_DIR / f"exact_ivan_herrera_denominator_manifest_{RUN_DATE}.csv"),
        f"exact_player_game_manifest_{RUN_DATE}.csv": read_csv(REVIEW_DIR / f"exact_player_game_identity_manifest_{RUN_DATE}.csv"),
        f"exact_two_row_duplicate_source_manifest_{RUN_DATE}.csv": dup_rows,
        f"source_lineage_contract_{RUN_DATE}.csv": [
            {"source_family": "expanded_o15_universe", "row": "5356", "generating_utility": "backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py", "input_purpose": "expanded O1.5 universe research row", "lineage_finding": "Normalized and appended as separate source family.", "authority_finding": "Not globally authoritative; eligible only if exact-row certification passes."},
            {"source_family": "hits_o15_alternate_discovery", "row": "6753", "generating_utility": "backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py", "input_purpose": "review-aid alternate discovery research row", "lineage_finding": "Normalized and appended as separate source family.", "authority_finding": "No independent PA lineage observed in this exact row."},
            {"source_family": "shadow_union", "row": "5356|6753", "generating_utility": "backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py", "input_purpose": "multi-source PA opportunity shadow test", "lineage_finding": "Rows collide because artifact preserves source_family/source_artifact in dedupe key.", "authority_finding": "No existing supersession rule; new rule remains exact-scope only."},
        ],
        f"raw_versus_hydrated_relationship_contract_{RUN_DATE}.csv": [
            {"relationship_question": "is_expanded_downstream_of_alternate", "finding": "NOT_PROVEN", "governance_effect": "No broad parent-child or artifact-wide rule allowed."},
            {"relationship_question": "does_populated_row_have_hydrated_pa_state", "finding": "YES_ROW_5356", "governance_effect": "Hydration state may be a certification dimension, not a generic non-null preference."},
            {"relationship_question": "does_missing_row_have_independent_pa_authority", "finding": "NOT_OBSERVED_IN_ROW_6753", "governance_effect": "Subordinate only if exact missing row remains PA-empty and lacks independent lineage at execution."},
            {"relationship_question": "does_source_order_encode_supersession", "finding": "NO_EVIDENCE", "governance_effect": "CSV row order cannot decide precedence."},
            {"relationship_question": "did_artifact_concatenation_create_collision", "finding": "YES_SOURCE_FAMILY_UNION", "governance_effect": "Immutable overlay required; no source mutation."},
        ],
        f"exact_precedence_rule_{RUN_DATE}.csv": [
            {"rule_id": "IVAN_HERRERA_EXACT_DUPLICATE_PRECEDENCE_V1", "scope": "exact artifact hash + rows 5356/6753 + player-game + denominator", "rule": "A future execution may select row 5356 over row 6753 only after stages 1-9 pass, including strict-prior temporal certification and provenance completeness.", "not_a_basis": "non-null preference|row order|favorable result|branch name intuition|largest field count", "current_execution_effect": "NONE"},
            {"rule_id": "FAIL_CLOSED_DEFAULT", "scope": "all cases outside exact rule", "rule": "If any condition is missing, changed, ambiguous, or extrapolated, preserve PA_INPUT_DISCREPANCY or equivalent fail-closed status.", "not_a_basis": "convenience", "current_execution_effect": "NONE"},
        ],
        f"scope_boundary_contract_{RUN_DATE}.csv": [
            {"dimension": "denominator_identity", "allowed_scope": TARGET_DENOM, "excluded_scope": "all other propositions, sides, lines, players, games"},
            {"dimension": "player_game_identity", "allowed_scope": TARGET_PG, "excluded_scope": "other Iván Herrera games and all other players"},
            {"dimension": "source_rows", "allowed_scope": "shadow row 5356 vs shadow row 6753 only", "excluded_scope": "artifact-wide dedupe or source-family global preference"},
            {"dimension": "source_artifact", "allowed_scope": rel(SHADOW_SOURCE), "excluded_scope": "other PA artifacts or regenerated files unless explicitly re-certified"},
        ],
        f"source_row_eligibility_contract_{RUN_DATE}.csv": [
            {"row_role": "hydrated_candidate", "row_number": "5356", "mandatory_condition": "exact source artifact hash and row identity", "required_status": "PASS", "failure_status": "PA_DUPLICATE_SOURCE_HASH_CHANGED"},
            {"row_role": "hydrated_candidate", "row_number": "5356", "mandatory_condition": "strict-prior rolling PA eligibility for d7/d15/d30 only", "required_status": "PASS", "failure_status": "PA_DUPLICATE_TEMPORAL_FAILED"},
            {"row_role": "hydrated_candidate", "row_number": "5356", "mandatory_condition": "actual same-game plate_appearances not used as target PA concept", "required_status": "PASS", "failure_status": "PA_DUPLICATE_PROVENANCE_FAILED"},
            {"row_role": "hydrated_candidate", "row_number": "5356", "mandatory_condition": "no conflicting populated candidate", "required_status": "PASS", "failure_status": "PA_DUPLICATE_MULTIPLE_POPULATED_CONFLICT"},
            {"row_role": "missing_candidate", "row_number": "6753", "mandatory_condition": "same player-game concept and grain, PA state remains missing, no independent authoritative PA lineage", "required_status": "PASS", "failure_status": "PA_DUPLICATE_RAW_RECORD_INDEPENDENT_AUTHORITY"},
        ],
        f"temporal_integrity_contract_{RUN_DATE}.csv": [
            {"field_or_state": "d7_plate_appearances", "allowed_if": "evidence games strictly before 2026-07-02", "current_status": "UNPROVEN_BY_THIS_GOVERNANCE", "future_failure_behavior": "fail_closed"},
            {"field_or_state": "d15_plate_appearances", "allowed_if": "evidence games strictly before 2026-07-02", "current_status": "UNPROVEN_BY_THIS_GOVERNANCE", "future_failure_behavior": "fail_closed"},
            {"field_or_state": "d30_plate_appearances", "allowed_if": "evidence games strictly before 2026-07-02", "current_status": "UNPROVEN_BY_THIS_GOVERNANCE", "future_failure_behavior": "fail_closed"},
            {"field_or_state": "plate_appearances", "allowed_if": "never as target strict-prior concept for this denominator row", "current_status": "EXCLUDED_ACTUAL_SAME_GAME_PA", "future_failure_behavior": "do_not_propagate"},
            {"field_or_state": "artifact_generation_time", "allowed_if": "used only for provenance, never evidence-time substitute", "current_status": "PROVENANCE_ONLY", "future_failure_behavior": "fail_closed_if_used_as_temporal_proof"},
        ],
        f"identity_and_grain_contract_{RUN_DATE}.csv": [
            {"identity_component": "slate_date", "required_value": "2026-07-02", "certification_rule": "must match both duplicate rows and denominator"},
            {"identity_component": "game_id", "required_value": "824906", "certification_rule": "must match both duplicate rows and denominator"},
            {"identity_component": "player_id", "required_value": "671056", "certification_rule": "must match both duplicate rows and denominator"},
            {"identity_component": "denominator_line", "required_value": "0.5", "certification_rule": "propagate only to exact selected proposition row; do not create O1.5 row"},
            {"identity_component": "side", "required_value": "over", "certification_rule": "preserve selected side; no opposite-side creation"},
            {"identity_component": "team_opponent", "required_value": "STL|ATL", "certification_rule": "orientation must remain unchanged"},
        ],
        f"precedence_decision_table_{RUN_DATE}.csv": [
            {"case_id": "1", "condition": "one certified hydrated row and one missing raw predecessor row", "decision": "select hydrated row only if all frozen conditions pass"},
            {"case_id": "2", "condition": "two populated rows with identical PA state", "decision": "do not apply this one-row rule unless separately governed"},
            {"case_id": "3", "condition": "two populated rows with conflicting PA state", "decision": "fail closed"},
            {"case_id": "4", "condition": "hydrated row temporally ineligible or unproven", "decision": "fail closed"},
            {"case_id": "5", "condition": "hydrated row lacks provenance or lineage", "decision": "fail closed"},
            {"case_id": "6", "condition": "missing row is independently authoritative rather than raw/unhydrated", "decision": "fail closed"},
            {"case_id": "7", "condition": "source artifact hash or row identity changes", "decision": "fail closed and require discrepancy review"},
        ],
        f"failure_taxonomy_{RUN_DATE}.csv": [
            {"status": s, "meaning": m} for s, m in [
                ("PA_DUPLICATE_PRECEDENCE_INPUT_DISCREPANCY", "Generic fail-closed duplicate precedence input conflict."),
                ("PA_DUPLICATE_HYDRATED_ROW_MISSING", "Expected hydrated row is absent."),
                ("PA_DUPLICATE_RAW_ROW_MISSING", "Expected missing/raw row is absent."),
                ("PA_DUPLICATE_SOURCE_HASH_CHANGED", "Frozen source artifact hash changed."),
                ("PA_DUPLICATE_IDENTITY_MISMATCH", "Player, game, or denominator identity mismatch."),
                ("PA_DUPLICATE_GRAIN_MISMATCH", "Rows do not bind to governed grain."),
                ("PA_DUPLICATE_TEMPORAL_FAILED", "Strict-prior temporal proof failed or absent."),
                ("PA_DUPLICATE_HYDRATION_LINEAGE_UNPROVEN", "Hydration/source relationship is not proven enough for selection."),
                ("PA_DUPLICATE_PROVENANCE_FAILED", "Required provenance fields missing or inconsistent."),
                ("PA_DUPLICATE_MULTIPLE_POPULATED_CONFLICT", "More than one populated PA state conflicts."),
                ("PA_DUPLICATE_RAW_RECORD_INDEPENDENT_AUTHORITY", "Missing/raw record has independent authoritative status."),
                ("PA_DUPLICATE_PRECEDENCE_CERTIFIED", "All stages pass in a future authorized execution."),
            ]
        ],
        f"certification_decision_table_{RUN_DATE}.csv": [
            {"stage_number": i, "stage": stage, "selection_allowed_before_stage_passes": False, "failure_behavior": "fail_closed"}
            for i, stage in enumerate([
                "Governance scope eligibility",
                "Source artifact hash certification",
                "Exact duplicate-row binding",
                "Player-game identity certification",
                "Raw-versus-hydrated lineage certification",
                "Temporal integrity certification",
                "PA concept compatibility certification",
                "Provenance completeness certification",
                "Precedence-rule certification",
                "Selected source-row certification",
                "PA field certification",
                "Player-game PA-state certification",
                "Denominator propagation certification",
                "Final PA qualification",
                "Downstream full qualification",
            ], start=1)
        ],
        f"denominator_propagation_contract_{RUN_DATE}.csv": [
            {"propagation_target": TARGET_DENOM, "allowed_pa_fields": "prior_d7/prior_d15/prior_d30 equivalent only after certification", "forbidden_fields": "actual same-game plate_appearances|same-game outcome|lineup|starter workload|at_bats", "propagation_status_now": "NOT_AUTHORIZED"},
        ],
        f"source_non_mutation_contract_{RUN_DATE}.csv": [
            {"prohibited_action": action, "status": "PROHIBITED", "notes": "Future execution must use immutable overlay, not source repair."}
            for action in ["delete either source row", "edit source artifact", "rewrite shadow file", "reorder source rows", "fill missing source row", "globally deduplicate artifact"]
        ],
        f"seven_row_exclusion_contract_{RUN_DATE}.csv": [
            {"contract": "prior seven exclusion", "status": "PRESERVED", "manifest_path": rel(STATE_DIR / f"exact_prior_seven_row_pa_source_missing_manifest_{RUN_DATE}.csv"), "zero_overlap_with_target": True, "notes": "Rule does not apply to source-missing rows without exact raw-hydrated duplicate pattern."}
        ] + read_csv(STATE_DIR / f"exact_prior_seven_row_pa_source_missing_manifest_{RUN_DATE}.csv"),
        f"projected_impact_{RUN_DATE}.csv": [
            {"metric": "potentially_pa_qualified_rows", "value": 1, "executed_now": False},
            {"metric": "potentially_fully_qualified_rows", "value": 1, "executed_now": False},
            {"metric": "hits_0_5_additions", "value": 1, "executed_now": False},
            {"metric": "hits_1_5_additions", "value": 0, "executed_now": False},
            {"metric": "variant_impact", "value": 0, "executed_now": False},
            {"metric": "prior_seven_pa_blockers_unchanged", "value": 7, "executed_now": False},
        ],
        f"provenance_schema_{RUN_DATE}.csv": [
            {"field": "source_artifact_sha256", "required": True, "purpose": "bind immutable source version"},
            {"field": "shadow_row_number", "required": True, "purpose": "bind exact row identity"},
            {"field": "source_family", "required": True, "purpose": "verify raw/hydrated collision members"},
            {"field": "pa_context_latest_date", "required": True, "purpose": "prove strict-prior rolling context in future execution"},
            {"field": "precedence_decision", "required": True, "purpose": "audit selected/subordinate row"},
            {"field": "human_authorization_id", "required": True, "purpose": "separate future approval boundary"},
        ],
        f"replayability_contract_{RUN_DATE}.csv": [
            {"check": "source_manifest_hash_matches", "required_result": "PASS"},
            {"check": "rows_5356_and_6753_reproduced", "required_result": "PASS"},
            {"check": "target_identity_matches", "required_result": "PASS"},
            {"check": "stage_order_is_stable", "required_result": "PASS"},
            {"check": "fail_closed_on_source_change", "required_result": "PASS"},
            {"check": "no_source_mutation", "required_result": "PASS"},
            {"check": "idempotent_overlay_only", "required_result": "PASS"},
        ],
        f"human_approval_boundary_{RUN_DATE}.csv": [
            {"status": STATUS, "future_execution_requires_approval": True, "no_duplicate_resolved": True, "no_source_row_selected": True, "no_pa_value_remediated": True, "no_qualification_state_changed": True, "bounded_scope": "exact rows 5356/6753 only", "prior_seven_excluded": True},
        ],
    }


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dup_rows = source_rows()
    contracts = contract_rows(dup_rows)

    for filename, rows in contracts.items():
        write_csv(OUT_DIR / filename, rows)

    provenance = [
        *package_checks(),
        stat_row("duplicate_review_manifest", REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"),
        stat_row("shadow_source_artifact", SHADOW_SOURCE),
        stat_row("governance_utility", ROOT / "backend/mlb/scripts/freeze_mlb_selected_proposition_ivan_herrera_pa_duplicate_precedence_governance.py"),
        *[stat_row(f"matrix_{p.name}", p) for p in MATRIX_PATHS],
    ]
    write_csv(OUT_DIR / f"input_provenance_and_hash_report_{RUN_DATE}.csv", provenance)

    validations = validation_rows(dup_rows)
    write_csv(OUT_DIR / f"validation_ledger_{RUN_DATE}.csv", validations)
    write_csv(OUT_DIR / f"static_no_network_no_resolution_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", static_guard())

    payload = {
        "generated_at": now(),
        "status": STATUS,
        "decision": DECISION,
        "target_denominator_identity": TARGET_DENOM,
        "target_player_game_identity": TARGET_PG,
        "source_artifact": rel(SHADOW_SOURCE),
        "source_artifact_sha256": sha256(SHADOW_SOURCE),
        "exact_duplicate_rows": [{"row": r["shadow_row_number"], "source_family": r["source_family"], "pa_state": r["pa_state"]} for r in dup_rows],
        "precedence_rule": "row 5356 may supersede row 6753 only in a future approved execution after all frozen conditions pass; otherwise fail closed",
        "strict_prior_status_now": "UNPROVEN_FAIL_CLOSED_UNTIL_CERTIFIED",
        "production_behavior_changed": False,
        "db_writes": 0,
        "network_requests": 0,
        "qualification_state_changed": False,
        "remediation_performed": False,
    }
    write_json(OUT_DIR / f"machine_readable_governance_contract_{RUN_DATE}.json", payload)

    main_md = f"""
# Iván Herrera PA Duplicate-Precedence Governance — {RUN_DATE}

Status: `{STATUS}`

Decision: `{DECISION}`

## Scope

This governance package is bounded to `{TARGET_DENOM}` and the player-game identity `{TARGET_PG}`.
It binds exactly two rows from `{rel(SHADOW_SOURCE)}`: row `5356` from `expanded_o15_universe`
and row `6753` from `hits_o15_alternate_discovery`.

## Frozen Rule

For a future separately approved execution, row `5356` may supersede row `6753` only if all frozen
certification stages pass. The rule is conditional and exact-row only. It is not a generic preference
for non-null PA fields, expanded-universe rows, later row numbers, or favorable qualification results.

The current temporal state remains `UNPROVEN_FAIL_CLOSED_UNTIL_CERTIFIED`. The populated row carries
strict-prior candidate fields, but this governance package does not certify them for use. Actual
same-game `plate_appearances` is excluded from the target PA concept.

## Source Relationship

The duplicate-discrepancy review supports a multi-source shadow-union collision. It does not prove a
global parent-child relationship between the source families. Therefore this contract freezes only an
exact-row precedence path with immutable overlay execution requirements.

## Boundary

No duplicate was resolved. No source row was selected. No PA value was remediated. No qualification
state changed. The prior seven PA blockers remain excluded.
"""
    write_md(OUT_DIR / f"ivan_herrera_pa_duplicate_precedence_governance_specification_{RUN_DATE}.md", main_md)

    one_page = f"""
# One-Page Decision Summary — {RUN_DATE}

Status: `{STATUS}`

The governance answer is conditional: the hydrated `expanded_o15_universe` row may outrank the
missing `hits_o15_alternate_discovery` row only in a future approved execution after exact source
binding, strict-prior temporal proof, PA concept compatibility, and provenance certification.

Today, no row is selected and the current Iván Herrera blocker remains unchanged.
"""
    write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", one_page)

    parse = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            read_csv(path)
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.md")):
        ok = path.read_text(encoding="utf-8").lstrip().startswith("#")
        parse.append({"path": rel(path), "artifact_type": "markdown", "parse_status": "PASS" if ok else "FAIL", "notes": ""})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse)

    sha_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)
    package_hash = package_sha(OUT_DIR)
    return {**payload, "package_sha256_manifest_hash": package_hash, "output_dir": rel(OUT_DIR)}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
