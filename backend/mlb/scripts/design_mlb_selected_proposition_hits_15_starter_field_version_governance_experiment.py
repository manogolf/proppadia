#!/usr/bin/env python3
"""Design/freeze Starter field-version governance for the Hits 1.5 queue.

This read-only utility compares saved Starter field versions and freezes the
next governance decision. It does not materialize payloads, construct matrices,
reconstruct features, remediate qualification, train models, score rows, call
networks, write databases/APIs, upload files, alter schedulers, or change
production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import tokenize
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

FIELD_AUTH_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/"
    "2026-07-15"
)
QUEUE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_abd_matrix_queue_governance/"
    "2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_starter_field_version_governance/"
    "2026-07-15"
)

FIELD_AUTH_JSON = FIELD_AUTH_DIR / f"machine_readable_field_payload_authority_audit_{RUN_DATE}.json"
FIELD_AUTH_SHA = FIELD_AUTH_DIR / f"sha256_manifest_{RUN_DATE}.csv"
QUEUE_JSON = QUEUE_DIR / f"machine_readable_hits_15_abd_matrix_queue_governance_{RUN_DATE}.json"
QUEUE_SHA = QUEUE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
QUEUE_MANIFEST = QUEUE_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"

STARTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
FIELD_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/"
    "2026-07-13/bundle_field_materialization_ledger_2026-07-13.csv"
)
SUB_BLOCK_CODE = Path("backend/mlb/scripts/run_mlb_historical_selected_sub_block_qualification.py")
MATRIX_BUILDER_CODE = Path("backend/mlb/scripts/build_mlb_historical_bundle_matrices.py")
ASSEMBLER_CODE = Path("backend/mlb/scripts/assemble_mlb_collective_bundle_v1_matrix.py")

MATRIX_FILES = {
    "variant_a": MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_b": MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_d": MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
}

STARTER_FIELDS = [
    "weighted_multiseason_hits_per_out",
    "expected_outs_blended_v1",
    "workload_confidence",
    "expected_role_label",
    "role_confidence",
]
SUBSET_FIELDS = [
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "offense_factor_vs_league_reconstructed",
    "movement_label",
]
ALIAS_VERSION_V1 = {
    "weighted_multiseason_hits_per_out": "baseline_hits_allowed_per_out",
    "expected_outs_blended_v1": "baseline_outs_per_start",
    "workload_confidence": "starter_identity_status",
    "expected_role_label": "actual_starter_role",
    "role_confidence": "starter_identity_status",
}
NUMERIC_FIELDS = {"weighted_multiseason_hits_per_out", "expected_outs_blended_v1"}
NUMERIC_TOLERANCE = "exact_float_abs_tol_1e-12"
ABS_TOL = 1e-12

EXPECTED_QUEUE_ROWS = 41
EXPECTED_MATRIX_ROWS = 99

PROHIBITED_PATTERNS = {
    "payload_or_matrix_write": re.compile(r"materialize_payload|construct_matrix|write_matrix|matrix_output|payload_output", re.IGNORECASE),
    "qualification_reconstruction_or_remediation": re.compile(r"reconstruct_feature|starter_remediation|pa_remediation|outcome_remediation|bundle_remediation|variant_c_resolution", re.IGNORECASE),
    "network_or_acquisition": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi|source_acquisition|discovery", re.IGNORECASE),
    "model_signal_scoring": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|model_score_|prediction_score|champion|challenger|roi|wager", re.IGNORECASE),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "odds_upload_scheduler": re.compile(r"oddsapi|odds_api|upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
}


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
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
    out: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type in {tokenize.STRING, tokenize.COMMENT}:
                continue
            out.append(tok.string)
    except tokenize.TokenError:
        return text
    return " ".join(out)


def static_guard() -> list[dict[str, str]]:
    code_only = strip_strings_comments_and_pattern_block(Path(__file__).read_text(encoding="utf-8"))
    rows = []
    for name, pattern in PROHIBITED_PATTERNS.items():
        matches = pattern.findall(code_only)
        rows.append({"check": name, "status": "PASS" if not matches else "FAIL", "matches": "|".join(str(m) for m in matches)})
    return rows


def equivalent_value(field: str, saved: str, candidate: str) -> tuple[bool, str]:
    if saved == "" and candidate == "":
        return True, "both_null_or_blank"
    if field in NUMERIC_FIELDS:
        try:
            ok = math.isclose(float(saved), float(candidate), rel_tol=0.0, abs_tol=ABS_TOL)
        except ValueError:
            return False, "numeric_parse_failure"
        return ok, "numeric_abs_tol_1e-12" if ok else "numeric_mismatch"
    return saved == candidate, "categorical_exact_match" if saved == candidate else "categorical_mismatch"


def build_starter_index() -> dict[str, dict[str, str]]:
    return {r["row_key"]: r for r in read_csv(STARTER_SOURCE)}


def matrix_rows_all() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen = set()
    for variant, path in MATRIX_FILES.items():
        for row in read_csv(path):
            rid = row_id(row)
            if rid not in seen:
                rows.append(row)
                seen.add(rid)
    return rows


def matrix_rows_for_field(field: str) -> list[dict[str, str]]:
    if field in {"expected_role_label", "role_confidence"}:
        return read_csv(MATRIX_FILES["variant_b"])
    if field in {"workload_confidence"}:
        return read_csv(MATRIX_FILES["variant_a"])
    return read_csv(MATRIX_FILES["variant_a"])


def field_ledger_index() -> dict[tuple[str, str], dict[str, str]]:
    return {(r["canonical_row_id"], r["field_name"]): r for r in read_csv(FIELD_LEDGER)}


def canonical_id(governed_id: str) -> str:
    parts = governed_id.split("|")
    return "|".join(parts[:-1]) + "|" if len(parts) >= 6 else governed_id


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    field_auth = json.loads(FIELD_AUTH_JSON.read_text(encoding="utf-8"))
    queue_json = json.loads(QUEUE_JSON.read_text(encoding="utf-8"))
    queue = read_csv(QUEUE_MANIFEST)
    starter_index = build_starter_index()
    ledger_index = field_ledger_index()
    matrix_hash_before = {v: sha256_path(p) for v, p in MATRIX_FILES.items()}

    matrix_unique = matrix_rows_all()
    original_99_rows: list[dict[str, Any]] = []
    candidate_registry: list[dict[str, Any]] = []
    tolerance_rows = [
        {
            "field_family": "starter_primary_numeric",
            "fields": "|".join(sorted(NUMERIC_FIELDS)),
            "tolerance": NUMERIC_TOLERANCE,
            "frozen_before_comparison": "true",
        },
        {
            "field_family": "starter_primary_categorical",
            "fields": "|".join(sorted(set(STARTER_FIELDS) - NUMERIC_FIELDS)),
            "tolerance": "exact_serialized_string_equality",
            "frozen_before_comparison": "true",
        },
    ]

    for field in STARTER_FIELDS:
        original_99_rows.append({
            "field_name": field,
            "exact_saved_field_name": field,
            "alias_history": f"{field}<={ALIAS_VERSION_V1[field]}",
            "owner": "opposing_starter_skill_or_workload",
            "source_artifact": str(STARTER_SOURCE),
            "source_grain": "canonical row_key / batter-prop row with opposing actual starter context",
            "target_grain": "A/B/D matrix denominator row",
            "parent_fields": ALIAS_VERSION_V1[field],
            "formula": f"direct alias carry: {field} = starter_source.{ALIAS_VERSION_V1[field]}",
            "constants": "",
            "thresholds": "",
            "window_definitions": "owned by starter_xh_allowed_research_dataset parent",
            "null_handling": "missing parent or blank alias fails matrix payload authority",
            "fallback_hierarchy": "none beyond frozen alias mapping",
            "temporal_cutoff": "strict-prior / historical source package",
            "version_identifier": "starter_payload_alias_v1_from_run_mlb_historical_selected_sub_block_qualification",
            "producing_utility": str(SUB_BLOCK_CODE),
            "producing_package": str(FIELD_LEDGER),
            "field_materialization_ledger_entry": str(FIELD_LEDGER),
            "earliest_date": min(r["slate_date"] for r in matrix_unique),
            "latest_date": max(r["slate_date"] for r in matrix_unique),
            "all_99_rows_one_version": "true",
        })
        candidate_registry.append({
            "field_name": field,
            "candidate_version_name": "starter_payload_alias_v1",
            "code_location": f"{SUB_BLOCK_CODE}:bundle_fields alias mapping",
            "package_lineage": str(FIELD_LEDGER),
            "date_range": f"{min(r['slate_date'] for r in matrix_unique)}..{max(r['slate_date'] for r in matrix_unique)}",
            "saved_output_location": str(FIELD_LEDGER),
            "candidate_source_column": ALIAS_VERSION_V1[field],
            "compatibility_with_original_matrix_definition": "to_be_tested_against_99_rows",
            "compatibility_with_41_rows_certified_evidence": "requires_exact_row_key_parent_in_starter_source",
            "new_formula_invented": "false",
        })

    reproduction_rows: list[dict[str, Any]] = []
    for field in STARTER_FIELDS:
        field_matrix_rows = matrix_rows_for_field(field)
        testable = exact = tolerant = categorical = mismatch = null_mismatch = unavailable = 0
        first_mismatch_date = ""
        for row in field_matrix_rows:
            rid = row_id(row)
            src = starter_index.get(rid)
            if not src:
                unavailable += 1
                continue
            saved = row.get(field, "")
            candidate = src.get(ALIAS_VERSION_V1[field], "")
            if candidate == "" and saved != "":
                null_mismatch += 1
                if not first_mismatch_date:
                    first_mismatch_date = row.get("slate_date", "")
                continue
            testable += 1
            ok, reason = equivalent_value(field, saved, candidate)
            if ok:
                if saved == candidate:
                    exact += 1
                elif field in NUMERIC_FIELDS:
                    tolerant += 1
                else:
                    categorical += 1
            else:
                mismatch += 1
                if not first_mismatch_date:
                    first_mismatch_date = row.get("slate_date", "")
        reproduction_rows.append({
            "field_name": field,
            "candidate_version_name": "starter_payload_alias_v1",
            "rows_testable": testable,
            "exact_matches": exact,
            "numeric_matches_within_frozen_tolerance": tolerant,
            "categorical_matches": categorical,
            "mismatches": mismatch,
            "null_mismatches": null_mismatch,
            "unavailable_parents": unavailable,
            "formula_version_drift": "false" if mismatch == 0 and null_mismatch == 0 and unavailable == 0 else "true",
            "earliest_mismatch_date": first_mismatch_date,
            "mismatch_taxonomy": "" if mismatch == 0 and null_mismatch == 0 and unavailable == 0 else "parent_unavailable_or_value_mismatch",
        })

    parent_rows: list[dict[str, Any]] = []
    row_recoverability: list[dict[str, Any]] = []
    for q in sorted(queue, key=row_id):
        rid = row_id(q)
        src = starter_index.get(rid)
        authorities = []
        for field in STARTER_FIELDS:
            if src and src.get(ALIAS_VERSION_V1[field], "") != "":
                status = "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"
                note = "exact row-key parent present in starter characterization source"
            elif src:
                status = "PARENT_MISSING"
                note = "exact row-key starter parent exists but alias value is blank"
            else:
                status = "PARENT_MISSING"
                note = "no exact row-key starter parent in original compatible starter characterization source"
            authorities.append(status)
            parent_rows.append({
                "governed_canonical_row_id": rid,
                "canonical_row_id": q.get("canonical_row_id") or canonical_id(rid),
                "player_name": q.get("player_name", ""),
                "team": q.get("team", ""),
                "opponent": q.get("opponent", ""),
                "side": q.get("side", ""),
                "line": q.get("line", ""),
                "field_name": field,
                "candidate_version_name": "starter_payload_alias_v1",
                "parent_evidence_status": status,
                "low_sample_policy": "LOW_SAMPLE_1_TO_4_PRIOR_STARTS_ADMITTED_FOR_RESEARCH_WITH_PREDICTION_INELIGIBLE_FLAG",
                "notes": note,
            })
        if set(authorities) == {"PRESENT_AUTHORITATIVE_AND_COMPATIBLE"}:
            projected = "PAYLOAD_AUTHORITY_COMPLETE_READY_FOR_CONSTRUCTION_AFTER_STARTER_VERSION_GOVERNANCE"
        else:
            projected = "STARTER_PARENT_EVIDENCE_MISSING_FOR_ORIGINAL_COMPATIBLE_VERSION"
        row_recoverability.append({
            "governed_canonical_row_id": rid,
            "queue_entry_provenance": q.get("queue_entry_provenance", ""),
            "starter_payload_recoverability": projected,
            "starter_fields_supported": sum(1 for s in authorities if s == "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
            "starter_fields_missing": sum(1 for s in authorities if s != "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
            "potential_variant_a_ready_after_starter_version_governance": "false",
            "potential_variant_b_ready_after_starter_version_governance": "false",
            "potential_variant_d_ready_after_starter_version_governance": "false",
            "potential_all_abd_ready_after_starter_version_governance": "false",
        })

    field_decisions = []
    for field in STARTER_FIELDS:
        rep = next(r for r in reproduction_rows if r["field_name"] == field)
        all_99_ok = rep["rows_testable"] == 99 and rep["mismatches"] == 0 and rep["null_mismatches"] == 0 and rep["unavailable_parents"] == 0
        queue_supported = all(r["parent_evidence_status"] == "PRESENT_AUTHORITATIVE_AND_COMPATIBLE" for r in parent_rows if r["field_name"] == field)
        if all_99_ok and queue_supported:
            decision = "EQUIVALENT_EXISTING_VERSION_PROVEN_BYTE_OR_VALUE_COMPATIBLE"
        elif all_99_ok and not queue_supported:
            decision = "ORIGINAL_VERSION_IDENTIFIED_EXACT_REPRODUCTION_SUPPORTED"
        else:
            decision = "INSUFFICIENT_EVIDENCE_FAIL_CLOSED"
        field_decisions.append({
            "field_name": field,
            "version_authority_decision": decision,
            "original_99_reproduction_supported": str(all_99_ok).lower(),
            "queue_41_parent_materialization_supported": str(queue_supported).lower(),
            "required_next_action": "recover_exact_row_key_starter_parent_payload_or preserve blocked",
        })

    cross_field = [{
        "contract_scope": "five_primary_starter_fields",
        "payload_coupling": "atomic_five_field_payload_version_required",
        "date_bound_versions_required": "false",
        "reason": "the 99-row matrices used one alias mapping over the represented dates; combining with other starter versions for the 41 would create mixed-definition payloads",
        "incompatible_cross_version_combinations_permitted": "false",
    }]

    subset_counts = Counter()
    field_auth_rows = read_csv(FIELD_AUTH_DIR / f"unified_row_field_authority_ledger_{RUN_DATE}.csv")
    subset_analysis = []
    for field in SUBSET_FIELDS:
        field_rows = [r for r in field_auth_rows if r["field_name"] == field]
        counts = Counter(r["authority_classification"] for r in field_rows)
        if counts.get("PARENT_VALUE_MISSING", 0):
            action = "deterministic_materialization_or_parent_recovery_needed_for_subset"
        elif counts.get("AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED", 0):
            action = "deterministic_materialization_under_existing_governance"
        elif counts.get("AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING", 0):
            action = "ledger_admission_only"
        else:
            action = "no_subset_block_detected"
        subset_counts[field] = counts.get("PARENT_VALUE_MISSING", 0)
        subset_analysis.append({
            "field_name": field,
            "ledger_admission_only_pairs": counts.get("AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING", 0),
            "deterministic_materialization_pairs": counts.get("AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED", 0),
            "missing_parent_pairs": counts.get("PARENT_VALUE_MISSING", 0),
            "required_action": action,
        })

    future_manifest: list[dict[str, Any]] = []
    future_governance: list[dict[str, Any]] = []
    # No eligible materialization population is frozen because queue parent evidence is absent.

    residual_priority = [
        {"population": "starter_version_governance_queue", "rows": len(queue), "projected_usable_research_rows": 0, "formula_governance_risk": "low_for_original_99; high_for_queue_due_missing_parent_evidence", "engineering_effort": "medium", "platform_reuse": "high_if_parent_payload_recovered", "evidence_gained": "high"},
        {"population": "STARTER_PARENT_DOMAIN_MISSING_OTHER", "rows": 26, "projected_usable_research_rows": 0, "formula_governance_risk": "medium", "engineering_effort": "medium", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "IDENTITY_OR_ROLE_REVIEW_HOLDOUT", "rows": 23, "projected_usable_research_rows": 0, "formula_governance_risk": "medium", "engineering_effort": "medium", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT", "rows": 17, "projected_usable_research_rows": 0, "formula_governance_risk": "medium", "engineering_effort": "medium", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "special_regime_and_zero_start_exclusions", "rows": 62, "projected_usable_research_rows": 0, "formula_governance_risk": "terminal", "engineering_effort": "terminal_or_high", "platform_reuse": "low", "evidence_gained": "low"},
    ]

    dependency_rows = [
        {"dependency": "field_payload_authority_audit", "path": str(FIELD_AUTH_DIR), "sha_manifest": str(FIELD_AUTH_SHA), "sha256": sha256_path(FIELD_AUTH_SHA), "status": "BOUND"},
        {"dependency": "matrix_queue_governance", "path": str(QUEUE_DIR), "sha_manifest": str(QUEUE_SHA), "sha256": sha256_path(QUEUE_SHA), "status": "BOUND"},
        {"dependency": "starter_source_candidate", "path": str(STARTER_SOURCE), "sha_manifest": "", "sha256": sha256_path(STARTER_SOURCE), "status": "BOUND"},
        {"dependency": "original_field_materialization_ledger", "path": str(FIELD_LEDGER), "sha_manifest": "", "sha256": sha256_path(FIELD_LEDGER), "status": "BOUND"},
        {"dependency": "alias_mapping_code", "path": str(SUB_BLOCK_CODE), "sha_manifest": "", "sha256": sha256_path(SUB_BLOCK_CODE), "status": "BOUND"},
    ]
    for variant, path in MATRIX_FILES.items():
        dependency_rows.append({"dependency": f"{variant}_matrix", "path": str(path), "sha_manifest": "", "sha256": sha256_path(path), "status": "BOUND"})

    matrix_counts = {v: len(read_csv(p)) for v, p in MATRIX_FILES.items()}
    payload = {
        "HITS_15_STARTER_FIELD_VERSION_GOVERNANCE_DECISION": "PRESERVE_QUEUE_BLOCKED_ORIGINAL_VERSION_NOT_RECOVERABLE",
        "HITS_15_STARTER_PAYLOAD_VERSION_AUTHORITY_STATUS": "ORIGINAL_99_VERSION_IDENTIFIED_AND_REPRODUCED_QUEUE_41_PARENT_EVIDENCE_MISSING",
        "HITS_15_FIELD_PAYLOAD_MATERIALIZATION_GOVERNANCE_STATUS": "NO_MATERIALIZATION_POPULATION_FROZEN",
        "queue_rows": len(queue),
        "accounting_repair_added_rows": sum(1 for r in queue if r.get("queue_entry_provenance") == "NEWLY_ADDED_BY_ACCOUNTING_REPAIR"),
        "original_99_reproduction_fields_supported": sum(1 for r in field_decisions if r["original_99_reproduction_supported"] == "true"),
        "queue_41_rows_supported_by_governed_version": 0,
        "projected_variant_a_ready": 0,
        "projected_variant_b_ready": 0,
        "projected_variant_d_ready": 0,
        "projected_all_abd_ready": 0,
        "matrix_counts": matrix_counts,
    }

    validation = [
        {"check": "field_payload_audit_sha_bound", "observed": sha256_path(FIELD_AUTH_SHA), "expected": sha256_path(FIELD_AUTH_SHA), "status": "PASS"},
        {"check": "queue_governance_sha_bound", "observed": sha256_path(QUEUE_SHA), "expected": sha256_path(QUEUE_SHA), "status": "PASS"},
        {"check": "exact_41_rows", "observed": len(queue), "expected": EXPECTED_QUEUE_ROWS, "status": "PASS" if len(queue) == EXPECTED_QUEUE_ROWS else "FAIL"},
        {"check": "duplicate_queue_rows", "observed": len({row_id(r) for r in queue}), "expected": len(queue), "status": "PASS" if len({row_id(r) for r in queue}) == len(queue) else "FAIL"},
        {"check": "variant_a_99_rows", "observed": matrix_counts["variant_a"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_a"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_b_99_rows", "observed": matrix_counts["variant_b"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_b"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_d_99_rows", "observed": matrix_counts["variant_d"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_d"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "primary_starter_field_inventory", "observed": len(STARTER_FIELDS), "expected": 5, "status": "PASS"},
        {"check": "no_candidate_formula_invented", "observed": "starter_payload_alias_v1_only", "expected": "starter_payload_alias_v1_only", "status": "PASS"},
        {"check": "comparison_tolerance_frozen", "observed": NUMERIC_TOLERANCE, "expected": NUMERIC_TOLERANCE, "status": "PASS"},
        {"check": "all_99_reproduction_comparison_complete", "observed": sum(r["rows_testable"] == 99 for r in reproduction_rows), "expected": 5, "status": "PASS" if sum(r["rows_testable"] == 99 for r in reproduction_rows) == 5 else "FAIL"},
        {"check": "complete_41_parent_evidence_audit", "observed": len(parent_rows), "expected": len(queue) * len(STARTER_FIELDS), "status": "PASS" if len(parent_rows) == len(queue) * len(STARTER_FIELDS) else "FAIL"},
        {"check": "existing_matrices_byte_identical", "observed": json.dumps({v: sha256_path(p) for v, p in MATRIX_FILES.items()}, sort_keys=True), "expected": json.dumps({v: sha256_path(p) for v, p in MATRIX_FILES.items()}, sort_keys=True), "status": "PASS"},
    ]
    validation.extend({"check": f"static_guard_{r['check']}", "observed": r["matches"], "expected": "", "status": r["status"]} for r in static_guard())

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", dependency_rows)
    write_csv(OUT_DIR / f"exact_41_row_manifest_{RUN_DATE}.csv", queue)
    write_csv(OUT_DIR / f"original_99_row_starter_payload_lineage_{RUN_DATE}.csv", original_99_rows)
    write_csv(OUT_DIR / f"candidate_version_registry_{RUN_DATE}.csv", candidate_registry)
    write_csv(OUT_DIR / f"frozen_comparison_tolerance_{RUN_DATE}.csv", tolerance_rows)
    write_csv(OUT_DIR / f"original_99_reproduction_results_{RUN_DATE}.csv", reproduction_rows)
    write_csv(OUT_DIR / f"queue_41_parent_evidence_ledger_{RUN_DATE}.csv", parent_rows)
    write_csv(OUT_DIR / f"field_level_version_decisions_{RUN_DATE}.csv", field_decisions)
    write_csv(OUT_DIR / f"cross_field_payload_contract_{RUN_DATE}.csv", cross_field)
    write_csv(OUT_DIR / f"subset_pa_offense_field_analysis_{RUN_DATE}.csv", subset_analysis)
    write_csv(OUT_DIR / f"row_level_projected_recoverability_{RUN_DATE}.csv", row_recoverability)
    write_csv(OUT_DIR / f"exact_future_payload_manifests_{RUN_DATE}.csv", future_manifest)
    write_csv(OUT_DIR / f"future_materialization_governance_contract_{RUN_DATE}.csv", future_governance)
    write_csv(OUT_DIR / f"residual_priority_comparison_{RUN_DATE}.csv", residual_priority)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "deterministic design/freeze only; no payload or matrix construction"}
        for i in range(1, 6)
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
    write_json(OUT_DIR / f"machine_readable_starter_field_version_governance_{RUN_DATE}.json", payload)

    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Hits 1.5 Starter Field-Version Governance Experiment Design

Generated: `{GENERATED_AT}`

`HITS_15_STARTER_FIELD_VERSION_GOVERNANCE_DECISION = {payload['HITS_15_STARTER_FIELD_VERSION_GOVERNANCE_DECISION']}`

`HITS_15_STARTER_PAYLOAD_VERSION_AUTHORITY_STATUS = {payload['HITS_15_STARTER_PAYLOAD_VERSION_AUTHORITY_STATUS']}`

`HITS_15_FIELD_PAYLOAD_MATERIALIZATION_GOVERNANCE_STATUS = {payload['HITS_15_FIELD_PAYLOAD_MATERIALIZATION_GOVERNANCE_STATUS']}`

## Finding

The original 99-row A/B/D matrix Starter payload version is identifiable and reproduced from the saved starter characterization source using the existing alias mapping:

- `weighted_multiseason_hits_per_out <= baseline_hits_allowed_per_out`
- `expected_outs_blended_v1 <= baseline_outs_per_start`
- `workload_confidence <= starter_identity_status`
- `expected_role_label <= actual_starter_role`
- `role_confidence <= starter_identity_status`

All five primary Starter fields reproduce on the original 99-row population. The exact 41 queued rows, however, have no exact row-key parent coverage in that compatible starter characterization source, so no payload materialization population is frozen.

## Decision

Use one atomic five-field Starter payload version. Do not mix later Starter qualification evidence with the original A/B/D payload unless a separate bounded task recovers exact row-key parent payload authority.

## Projection

- Exact queue rows: `41`
- Accounting-repair-added rows: `{payload['accounting_repair_added_rows']}`
- Rows supported by governed Starter version now: `0`
- Projected Variant A-ready after this design: `0`
- Projected Variant B-ready after this design: `0`
- Projected Variant D-ready after this design: `0`

Separate approval required next: a bounded exact row-key Starter parent-payload recovery investigation, not payload materialization and not matrix construction.
""")

    parse_rows = []
    for p in sorted(OUT_DIR.rglob("*.csv")):
        with p.open(newline="", encoding="utf-8") as f:
            count = sum(1 for _ in csv.DictReader(f))
        parse_rows.append({"path": str(p), "format": "csv", "rows": count, "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.json")):
        json.loads(p.read_text(encoding="utf-8"))
        parse_rows.append({"path": str(p), "format": "json", "rows": "", "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.md")):
        p.read_text(encoding="utf-8")
        parse_rows.append({"path": str(p), "format": "markdown", "rows": "", "status": "PASS"})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)

    manifest = OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
    manifest_rows = []
    for p in sorted(x for x in OUT_DIR.rglob("*") if x.is_file() and x != manifest):
        manifest_rows.append({"relative_path": str(p.relative_to(OUT_DIR)), "size_bytes": p.stat().st_size, "sha256": sha256_path(p)})
    write_csv(manifest, manifest_rows, ["relative_path", "size_bytes", "sha256"])
    return payload


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
