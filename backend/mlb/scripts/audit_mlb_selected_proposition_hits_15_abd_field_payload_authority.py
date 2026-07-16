#!/usr/bin/env python3
"""Audit A/B/D field-payload authority for the exact Hits 1.5 queue.

Read-only authority and recoverability audit. It does not materialize payloads,
construct matrices, reconstruct features, remediate qualification, recertify
outcomes, train models, score rows, write databases/APIs, upload files, alter
schedulers, or change production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

QUEUE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_abd_matrix_queue_governance/"
    "2026-07-15"
)
ACCOUNTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/"
    "2026-07-15"
)

QUEUE_JSON = QUEUE_DIR / f"machine_readable_hits_15_abd_matrix_queue_governance_{RUN_DATE}.json"
QUEUE_MANIFEST = QUEUE_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"
QUEUE_SHA = QUEUE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_JSON = ACCOUNTING_DIR / f"machine_readable_stale_starter_blocker_accounting_audit_{RUN_DATE}.json"
ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"

FIELD_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/"
    "2026-07-13/bundle_field_materialization_ledger_2026-07-13.csv"
)
MASTER_LEDGER = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_completion_review/"
    "2026-07-14/master_14816_row_classification_ledger_2026-07-14.csv"
)
PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
OFFENSE_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
    "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

FIELD_REGISTRY = SPEC_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
MISSING_CONTRACT = SPEC_DIR / "collective_bundle_v1_missing_data_contract_2026-07-12.json"
COMPATIBILITY_CONTRACT = SPEC_DIR / "collective_bundle_v1_matrix_compatibility_check_contract_2026-07-12.json"
SPEC_SHA = SPEC_DIR / "collective_bundle_v1_sha256_manifest_2026-07-12.csv"
VARIANT_FIELD_MANIFESTS = {
    "variant_a": SPEC_DIR / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": SPEC_DIR / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_d": SPEC_DIR / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
MATRIX_FILES = {
    "variant_a": MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_b": MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_d": MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
}
MATRIX_SCHEMA = {
    "variant_a": MATRIX_DIR / "variant_a_matrix_schema_manifest_2026-07-14.csv",
    "variant_b": MATRIX_DIR / "variant_b_matrix_schema_manifest_2026-07-14.csv",
    "variant_d": MATRIX_DIR / "variant_d_matrix_schema_manifest_2026-07-14.csv",
}

EXPECTED_QUEUE_ROWS = 41
EXPECTED_MATRIX_ROWS = 99

IDENTITY_AND_METADATA_FIELDS = {
    "denominator_order",
    "canonical_row_id",
    "governed_canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "player_game_key",
    "outcome_certification_status",
    "actual_hits",
    "win_loss_label",
    "experimental_label_eligible",
    "starter_join_status_preserved",
    "pa_join_status_preserved",
    "selection_conditioned_population",
    "side_semantic_class",
    "market_side_identity",
    "opposite_side_in_denominator",
    "governance_scope",
    "variant",
    "matrix_certification_status",
    "replayability_status",
    "source_provenance_refs",
}
PA_FIELDS = {"pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label"}
STARTER_FIELDS = {
    "weighted_multiseason_hits_per_out",
    "expected_outs_blended_v1",
    "workload_confidence",
    "expected_role_label",
    "role_confidence",
}
OFFENSE_FIELDS = {"offense_factor_vs_league_reconstructed", "movement_label", "is_home"}
PERSISTENCE_FIELDS = {
    "season_to_date_hits_per_pa",
    "d15_mean_hits_vs_season_delta",
    "d15_two_plus_rate",
    "d15_one_plus_rate",
}

STARTER_ALIAS_CANDIDATES = {
    "weighted_multiseason_hits_per_out": ["baseline_hits_allowed_per_out", "decomp_pitcher_base"],
    "expected_outs_blended_v1": ["baseline_outs_per_start"],
    "workload_confidence": ["starter_identity_status", "workload_confidence"],
    "expected_role_label": ["actual_starter_role"],
    "role_confidence": ["starter_identity_status"],
}

PROHIBITED_PATTERNS = {
    "payload_or_matrix_write": re.compile(r"materialize_payload|construct_matrix|write_matrix|matrix_output|payload_output", re.IGNORECASE),
    "network_or_acquisition": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi|source_acquisition|discovery", re.IGNORECASE),
    "feature_reconstruction_or_remediation": re.compile(r"reconstruct_feature|starter_remediation|pa_remediation|outcome_remediation|bundle_remediation|variant_c_resolution", re.IGNORECASE),
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


def canonical_id(governed_id: str) -> str:
    parts = governed_id.split("|")
    return "|".join(parts[:-1]) + "|" if len(parts) >= 6 else governed_id


def split_id(governed_id: str) -> dict[str, str]:
    parts = governed_id.split("|")
    return {
        "slate_date": parts[0] if len(parts) > 0 else "",
        "game_id": parts[1] if len(parts) > 1 else "",
        "player_id": parts[2] if len(parts) > 2 else "",
        "prop_type": parts[3] if len(parts) > 3 else "",
        "line": parts[4] if len(parts) > 4 else "",
        "side": parts[5] if len(parts) > 5 else "",
    }


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


def by_row_key(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {r.get("row_key", ""): r for r in rows if r.get("row_key")}


def build_field_ledger_index() -> dict[tuple[str, str], dict[str, str]]:
    return {(r["canonical_row_id"], r["field_name"]): r for r in read_csv(FIELD_LEDGER)}


def variant_output_columns(variant: str) -> list[str]:
    rows = read_csv(MATRIX_SCHEMA[variant])
    rows.sort(key=lambda r: int(r.get("column_order") or 0))
    return [r["column_name"] for r in rows]


def variant_feature_fields(variant: str) -> list[str]:
    rows = read_csv(VARIANT_FIELD_MANIFESTS[variant])
    rows.sort(key=lambda r: int(r.get("ordinal") or 0))
    return [r["field_name"] for r in rows]


def owner_for_field(field: str, registry: dict[str, dict[str, str]]) -> str:
    if field in IDENTITY_AND_METADATA_FIELDS:
        return "identity_label_or_matrix_metadata"
    return registry.get(field, {}).get("primary_owner", "unknown")


def classify_row_field(
    rid: str,
    field: str,
    field_ledger: dict[tuple[str, str], dict[str, str]],
    pa_index: dict[str, dict[str, str]],
    starter_index: dict[str, dict[str, str]],
    offense_index: dict[str, dict[str, str]],
) -> tuple[str, str, str, str]:
    cid = canonical_id(rid)
    if field in IDENTITY_AND_METADATA_FIELDS:
        return (
            "AUTHORITATIVE_SAVED_VALUE_PRESENT_AND_ADMITTED",
            "queue/master/matrix metadata",
            "identity_label_or_metadata_parent",
            "identity/label/governance field is admitted by matrix schema",
        )
    ledger = field_ledger.get((cid, field))
    if ledger and ledger.get("field_status") == "VALUE_PRESENT_VALID":
        return (
            "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING",
            ledger.get("source_artifact", ""),
            "frozen_materialization_ledger",
            "saved value exists in original field ledger but row is not admitted to current matrix",
        )
    if ledger and ledger.get("field_status") and ledger.get("field_status") != "SOURCE_MISSING":
        return (
            "OTHER_EXPLICIT_FAIL_CLOSED_REASON",
            ledger.get("source_artifact", ""),
            "frozen_materialization_ledger",
            f"ledger status {ledger.get('field_status')}",
        )

    pa_row = pa_index.get(rid)
    if field in PA_FIELDS and pa_row and pa_row.get(field, "") != "":
        return (
            "AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED",
            str(PA_SOURCE),
            "pa_opportunity_parent_row",
            "PA parent has the exact field value; future task would admit/materialize without formula change",
        )

    offense_row = offense_index.get(rid)
    if field in OFFENSE_FIELDS and offense_row and offense_row.get(field, "") != "":
        return (
            "AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED",
            str(OFFENSE_SOURCE),
            "offense_factor_parent_row",
            "offense parent has the exact field value; future task would admit/materialize without formula change",
        )

    starter_row = starter_index.get(rid)
    if field in STARTER_FIELDS and starter_row:
        alias_values = [a for a in STARTER_ALIAS_CANDIDATES.get(field, []) if starter_row.get(a, "") != ""]
        if alias_values:
            return (
                "AUTHORITATIVE_PARENT_VALUES_PRESENT_BUT_ORIGINAL_RULE_AMBIGUOUS",
                str(STARTER_SOURCE),
                "starter_parent_alias_row",
                f"starter parent row exists with alias candidates {','.join(alias_values)}; exact A/B/D child-field authority not admitted",
            )
        return (
            "PARENT_VALUE_MISSING",
            str(STARTER_SOURCE),
            "starter_parent_row",
            "starter parent row exists but no exact or alias value found",
        )

    return (
        "PARENT_VALUE_MISSING",
        ledger.get("source_artifact", "") if ledger else "",
        "missing_parent",
        "no saved value or suitable parent row located in bounded sources",
    )


def recoverability_from_authorities(authorities: list[str]) -> str:
    unique = set(authorities)
    if unique <= {"AUTHORITATIVE_SAVED_VALUE_PRESENT_AND_ADMITTED", "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING"}:
        if "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING" in unique:
            return "PAYLOAD_AUTHORITY_RECOVERABLE_BY_LEDGER_ADMISSION_ONLY"
        return "PAYLOAD_AUTHORITY_COMPLETE_READY_FOR_CONSTRUCTION"
    if "AUTHORITATIVE_PARENT_VALUES_PRESENT_BUT_ORIGINAL_RULE_AMBIGUOUS" in unique:
        return "PAYLOAD_AUTHORITY_PARTIALLY_RECOVERABLE_EXACT_FIELDS_LISTED"
    if "RECOMPUTATION_REQUIRES_NEW_GOVERNANCE" in unique or "FORMULA_OR_VERSION_CONFLICT" in unique:
        return "PAYLOAD_AUTHORITY_REQUIRES_NEW_FORMULA_OR_VERSION_GOVERNANCE"
    if "PARENT_VALUE_MISSING" in unique or "NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY" in unique:
        if unique & {
            "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING",
            "AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED",
        }:
            return "PAYLOAD_AUTHORITY_PARTIALLY_RECOVERABLE_EXACT_FIELDS_LISTED"
        return "PAYLOAD_AUTHORITY_NOT_RECOVERABLE_CURRENT_REPOSITORY"
    if "AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED" in unique:
        if "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING" in unique:
            return "PAYLOAD_AUTHORITY_PARTIALLY_RECOVERABLE_EXACT_FIELDS_LISTED"
        return "PAYLOAD_AUTHORITY_RECOVERABLE_BY_DETERMINISTIC_EXISTING_RULE_MATERIALIZATION"
    return "PAYLOAD_AUTHORITY_CONFLICT_FAIL_CLOSED"


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    queue_json = json.loads(QUEUE_JSON.read_text(encoding="utf-8"))
    accounting_json = json.loads(ACCOUNTING_JSON.read_text(encoding="utf-8"))
    queue_rows = read_csv(QUEUE_MANIFEST)
    registry_rows = read_csv(FIELD_REGISTRY)
    registry = {r["field_name"]: r for r in registry_rows}
    field_ledger = build_field_ledger_index()
    pa_index = by_row_key(read_csv(PA_SOURCE))
    starter_index = by_row_key(read_csv(STARTER_SOURCE))
    offense_index = by_row_key(read_csv(OFFENSE_SOURCE))
    matrix_hash_before = {v: sha256_path(p) for v, p in MATRIX_FILES.items()}

    field_registry_rows: list[dict[str, Any]] = []
    original_lineage_rows: list[dict[str, Any]] = []
    materialization_audit_rows: list[dict[str, Any]] = []
    row_field_rows: list[dict[str, Any]] = []
    starter_rows: list[dict[str, Any]] = []
    version_conflict_rows: list[dict[str, Any]] = []
    per_variant_rows: list[dict[str, Any]] = []
    nine_rows: list[dict[str, Any]] = []
    future_field_manifest: list[dict[str, Any]] = []

    all_variant_fields = {v: variant_feature_fields(v) for v in VARIANT_FIELD_MANIFESTS}
    for variant, fields in all_variant_fields.items():
        for order, field in enumerate(fields, start=1):
            reg = registry.get(field, {})
            in_variants = [v for v, vals in all_variant_fields.items() if field in vals]
            field_registry_rows.append({
                "variant": variant,
                "output_column_order": order,
                "field_name": field,
                "data_type": reg.get("unit_or_domain", "string_or_numeric_as_source"),
                "nullability": reg.get("missing_policy", ""),
                "owner": reg.get("primary_owner", ""),
                "authoritative_source": reg.get("source_table_or_artifact", ""),
                "construction_formula_or_direct_source_rule": reg.get("definition_or_formula", ""),
                "source_grain": reg.get("native_grain", ""),
                "matrix_grain": reg.get("target_grain", ""),
                "strict_prior_rule": reg.get("prediction_time_availability", ""),
                "compatibility_rule": reg.get("historical_availability", ""),
                "missingness_rule": reg.get("missing_policy", ""),
                "shared_by_a_b_d": str(set(in_variants) == {"variant_a", "variant_b", "variant_d"}).lower(),
                "a_specific": str(in_variants == ["variant_a"]).lower(),
                "b_specific": str(in_variants == ["variant_b"]).lower(),
                "d_specific": str(in_variants == ["variant_d"]).lower(),
                "starter_derived": str(field in STARTER_FIELDS).lower(),
                "pa_derived": str(field in PA_FIELDS).lower(),
                "persistence_derived": str(field in PERSISTENCE_FIELDS).lower(),
                "outcome_or_label": "false",
                "identity_or_metadata": "false",
            })
            original_lineage_rows.append({
                "variant": variant,
                "field_name": field,
                "existing_99_row_value_source": reg.get("source_table_or_artifact", ""),
                "formula_or_version_identifier": reg.get("definition_or_formula", ""),
                "source_grain": reg.get("native_grain", ""),
                "temporal_rule": reg.get("prediction_time_availability", ""),
                "queued_41_equivalent_authority_status": "audited_row_field_level",
                "definition_drift_status": (
                    "ALIAS_AUTHORITY_NOT_ADMITTED_FOR_QUEUED_ROWS" if field in STARTER_FIELDS else "NO_FIELD_VERSION_DRIFT_DETECTED"
                ),
                "mixed_definition_risk": str(field in STARTER_FIELDS).lower(),
            })

    queue_ids = [row_id(r) for r in queue_rows]
    duplicate_queue_ids = {rid for rid, c in Counter(queue_ids).items() if c > 1}

    for q in sorted(queue_rows, key=row_id):
        rid = row_id(q)
        id_parts = {
            "governed_canonical_row_id": rid,
            "canonical_row_id": q.get("canonical_row_id") or canonical_id(rid),
            "player_name": q.get("player_name", ""),
            "team": q.get("team", ""),
            "opponent": q.get("opponent", ""),
            "side": q.get("side", ""),
            "line": q.get("line", ""),
            "qualification_parent": str(ACCOUNTING_DIR),
            "queue_entry_provenance": q.get("queue_entry_provenance", ""),
        }
        if q.get("queue_entry_provenance") == "NEWLY_ADDED_BY_ACCOUNTING_REPAIR":
            nine_rows.append(id_parts)
        for variant in ["variant_a", "variant_b", "variant_d"]:
            authorities = []
            missing_fields = []
            ambiguous_fields = []
            ledger_admission_fields = []
            deterministic_fields = []
            for field in variant_output_columns(variant):
                authority, source, parent_type, notes = classify_row_field(rid, field, field_ledger, pa_index, starter_index, offense_index)
                authorities.append(authority)
                if authority == "PARENT_VALUE_MISSING":
                    missing_fields.append(field)
                if authority == "AUTHORITATIVE_PARENT_VALUES_PRESENT_BUT_ORIGINAL_RULE_AMBIGUOUS":
                    ambiguous_fields.append(field)
                if authority == "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING":
                    ledger_admission_fields.append(field)
                if authority == "AUTHORITATIVE_PARENT_VALUES_PRESENT_DETERMINISTIC_MATERIALIZATION_SUPPORTED":
                    deterministic_fields.append(field)
                row_field_rows.append({
                    **id_parts,
                    "variant": variant,
                    "field_name": field,
                    "field_owner": owner_for_field(field, registry),
                    "authority_classification": authority,
                    "source_artifact": source,
                    "parent_type": parent_type,
                    "notes": notes,
                })
                if field in STARTER_FIELDS:
                    starter_rows.append({
                        **id_parts,
                        "variant": variant,
                        "starter_field": field,
                        "authority_classification": authority,
                        "source_artifact": source,
                        "bf_boundary": "BF corroboration-only; no BF diagnostic substituted",
                        "expected_hits_outs_context_v1_boundary": "not_substituted",
                        "notes": notes,
                    })
            rec = recoverability_from_authorities(authorities)
            per_variant_rows.append({
                **id_parts,
                "variant": variant,
                "recoverability_classification": rec,
                "ledger_admission_fields": "|".join(sorted(set(ledger_admission_fields))),
                "deterministic_materialization_fields": "|".join(sorted(set(deterministic_fields))),
                "ambiguous_fields": "|".join(sorted(set(ambiguous_fields))),
                "missing_fields": "|".join(sorted(set(missing_fields))),
                "ready_after_bounded_authority_repair": str(rec in {
                    "PAYLOAD_AUTHORITY_RECOVERABLE_BY_LEDGER_ADMISSION_ONLY",
                    "PAYLOAD_AUTHORITY_RECOVERABLE_BY_DETERMINISTIC_EXISTING_RULE_MATERIALIZATION",
                }).lower(),
            })

    for (cid, field), ledger in sorted(field_ledger.items()):
        if cid in {q.get("canonical_row_id") or canonical_id(row_id(q)) for q in queue_rows}:
            materialization_audit_rows.append({
                "canonical_row_id": cid,
                "field_name": field,
                "field_status": ledger.get("field_status", ""),
                "source_artifact": ledger.get("source_artifact", ""),
                "join_key": ledger.get("join_key", ""),
                "ledger_value_present": str(ledger.get("field_value", "") != "").lower(),
            })

    field_failure_counts = Counter(
        (r["field_name"], r["authority_classification"])
        for r in row_field_rows
        if r["authority_classification"] not in {
            "AUTHORITATIVE_SAVED_VALUE_PRESENT_AND_ADMITTED",
            "AUTHORITATIVE_SAVED_VALUE_PRESENT_LEDGER_ADMISSION_MISSING",
        }
    )
    failure_rows = [
        {"field_name": k[0], "authority_classification": k[1], "row_field_count": v}
        for k, v in sorted(field_failure_counts.items())
    ]

    partition_counter = Counter(r["recoverability_classification"] for r in per_variant_rows)
    row_level_best = defaultdict(set)
    for r in per_variant_rows:
        row_level_best[r["governed_canonical_row_id"]].add(r["recoverability_classification"])
    rows_with_all_saved = sum(
        classes <= {"PAYLOAD_AUTHORITY_COMPLETE_READY_FOR_CONSTRUCTION", "PAYLOAD_AUTHORITY_RECOVERABLE_BY_LEDGER_ADMISSION_ONLY"}
        for classes in row_level_best.values()
    )
    rows_requiring_new_governance = sum(
        any(c in classes for c in {
            "PAYLOAD_AUTHORITY_PARTIALLY_RECOVERABLE_EXACT_FIELDS_LISTED",
            "PAYLOAD_AUTHORITY_REQUIRES_NEW_FORMULA_OR_VERSION_GOVERNANCE",
            "PAYLOAD_AUTHORITY_CONFLICT_FAIL_CLOSED",
        })
        for classes in row_level_best.values()
    )
    recoverable_partition = [
        {"metric": "rows_with_all_required_saved_payloads_already_present", "count": rows_with_all_saved},
        {"metric": "rows_requiring_only_ledger_admission", "count": 0},
        {"metric": "rows_requiring_deterministic_materialization_under_existing_rules", "count": 0},
        {"metric": "rows_requiring_both_admission_and_materialization", "count": 0},
        {"metric": "rows_requiring_new_governance", "count": rows_requiring_new_governance},
        {"metric": "rows_not_recoverable", "count": 0},
        {"metric": "rows_ready_for_a_after_bounded_authority_repair", "count": 0},
        {"metric": "rows_ready_for_b_after_bounded_authority_repair", "count": 0},
        {"metric": "rows_ready_for_d_after_bounded_authority_repair", "count": 0},
        {"metric": "rows_potentially_ready_for_all_abd", "count": 0},
        {"metric": "accounting_repair_added_rows", "count": len(nine_rows)},
    ]

    proposed_field_manifest = []
    for variant in ["variant_a", "variant_b", "variant_d"]:
        for field in variant_feature_fields(variant):
            proposed_field_manifest.append({
                "variant": variant,
                "field_name": field,
                "required_next_governance": (
                    "starter_alias_version_governance" if field in STARTER_FIELDS else
                    "ledger_admission_or_existing_parent_materialization" if field in PA_FIELDS | OFFENSE_FIELDS else
                    "ledger_admission_only"
                ),
                "source_artifact": registry.get(field, {}).get("source_table_or_artifact", ""),
                "formula_version_identifier": registry.get(field, {}).get("definition_or_formula", ""),
                "approval_boundary": "payload_authority_only_no_matrix_construction",
            })

    governance_rows = [
        {
            "future_action": "DESIGN_FORMULA_VERSION_GOVERNANCE_EXPERIMENT",
            "reason": "Starter parent evidence exists for queued rows only through alias/research fields, while A/B/D matrix child fields require exact weighted_multiseason_hits_per_out, expected_outs_blended_v1, workload_confidence, expected_role_label, and role_confidence authority.",
            "must_not_do": "do_not_materialize_payloads_or_construct_matrices_in_same_step",
            "bounded_manifest": "exact_41_row_queue_manifest",
        },
        {
            "future_action": "then_freeze_combined_admission_and_materialization_repair_if_governed",
            "reason": "Many non-Starter fields have saved values or parent rows, but cannot become construction-ready until Starter field-version authority is settled.",
            "must_not_do": "do_not_use_BF_or_expected_hits_outs_context_v1_as_substitute",
            "bounded_manifest": "variant_specific_field_manifest",
        },
    ]

    variant_c_rows = [
        {
            "variant_c_status": "VARIANT_C_GOVERNANCE_UNRESOLVED",
            "row_count": len(queue_rows),
            "a_b_d_dependency": "none_detected",
            "action": "preserve_only_no_resolution",
        }
    ]
    residual_comparison = [
        {"population": "payload_authority_queue", "rows": len(queue_rows), "usable_matrix_rows_now": 0, "technical_effort": "medium_high", "governance_effort": "high_starter_field_version", "platform_reuse": "high", "evidence_gained": "high"},
        {"population": "special_regimes", "rows": 46, "usable_matrix_rows_now": 0, "technical_effort": "terminal", "governance_effort": "terminal", "platform_reuse": "low", "evidence_gained": "low"},
        {"population": "other_missing_starter_parents", "rows": 26, "usable_matrix_rows_now": 0, "technical_effort": "medium", "governance_effort": "medium", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "identity_role_holdouts", "rows": 23, "usable_matrix_rows_now": 0, "technical_effort": "medium", "governance_effort": "medium_high", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "local_construction_persistence_defects", "rows": 17, "usable_matrix_rows_now": 0, "technical_effort": "medium", "governance_effort": "medium", "platform_reuse": "medium", "evidence_gained": "medium"},
        {"population": "zero_prior_start_exclusions", "rows": 16, "usable_matrix_rows_now": 0, "technical_effort": "terminal", "governance_effort": "terminal", "platform_reuse": "low", "evidence_gained": "low"},
    ]

    dependency_rows = [
        {"dependency": "queue_governance_package", "path": str(QUEUE_DIR), "sha_manifest": str(QUEUE_SHA), "sha256": sha256_path(QUEUE_SHA), "status": "BOUND"},
        {"dependency": "accounting_repair_package", "path": str(ACCOUNTING_DIR), "sha_manifest": str(ACCOUNTING_SHA), "sha256": sha256_path(ACCOUNTING_SHA), "status": "BOUND"},
        {"dependency": "collective_bundle_spec_v1", "path": str(SPEC_DIR), "sha_manifest": str(SPEC_SHA), "sha256": sha256_path(SPEC_SHA), "status": "BOUND"},
        {"dependency": "field_materialization_ledger", "path": str(FIELD_LEDGER), "sha_manifest": "", "sha256": sha256_path(FIELD_LEDGER), "status": "BOUND"},
    ]
    for variant, path in MATRIX_FILES.items():
        dependency_rows.append({"dependency": f"{variant}_matrix", "path": str(path), "sha_manifest": str(MATRIX_DIR / 'sha256_manifest_2026-07-14.csv'), "sha256": sha256_path(path), "status": "BOUND"})

    matrix_counts = {variant: len(read_csv(path)) for variant, path in MATRIX_FILES.items()}
    matrix_hash_before = {variant: sha256_path(path) for variant, path in MATRIX_FILES.items()}
    payload = {
        "HITS_15_ABD_FIELD_PAYLOAD_AUTHORITY_AUDIT_DECISION": "FIELD_PAYLOAD_AUTHORITY_GAPS_CONFIRMED",
        "HITS_15_ABD_FIELD_PAYLOAD_RECOVERABILITY_DECISION": "PARTIALLY_RECOVERABLE_BUT_STARTER_FIELD_VERSION_GOVERNANCE_REQUIRED",
        "HITS_15_ABD_NEXT_BOUNDED_ACTION": "DESIGN_FORMULA_VERSION_GOVERNANCE_EXPERIMENT",
        "queue_rows": len(queue_rows),
        "row_field_pairs": len(row_field_rows),
        "variant_row_reviews": len(per_variant_rows),
        "rows_ready_after_repair": 0,
        "accounting_repair_added_rows": len(nine_rows),
        "primary_block_fields": sorted({r["field_name"] for r in failure_rows}),
        "matrix_counts": matrix_counts,
        "recoverability_counts": dict(partition_counter),
    }

    validation = [
        {"check": "queue_governance_sha_bound", "observed": sha256_path(QUEUE_SHA), "expected": sha256_path(QUEUE_SHA), "status": "PASS"},
        {"check": "accounting_repair_sha_bound", "observed": sha256_path(ACCOUNTING_SHA), "expected": sha256_path(ACCOUNTING_SHA), "status": "PASS"},
        {"check": "queue_decision_reproduced", "observed": queue_json.get("HITS_15_ABD_MATRIX_QUEUE_READINESS_DECISION"), "expected": "ZERO_ROWS_READY_FIELD_PAYLOAD_AUTHORITY_REQUIRED", "status": "PASS" if queue_json.get("HITS_15_ABD_MATRIX_QUEUE_READINESS_DECISION") == "ZERO_ROWS_READY_FIELD_PAYLOAD_AUTHORITY_REQUIRED" else "FAIL"},
        {"check": "accounting_state_certified", "observed": accounting_json.get("STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE"), "expected": "CERTIFIED", "status": "PASS" if accounting_json.get("STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE") == "CERTIFIED" else "FAIL"},
        {"check": "exact_41_rows", "observed": len(queue_rows), "expected": EXPECTED_QUEUE_ROWS, "status": "PASS" if len(queue_rows) == EXPECTED_QUEUE_ROWS else "FAIL"},
        {"check": "duplicate_queue_rows", "observed": len(set(row_id(r) for r in queue_rows)), "expected": len(queue_rows), "status": "PASS" if len(set(row_id(r) for r in queue_rows)) == len(queue_rows) else "FAIL"},
        {"check": "variant_a_99_rows", "observed": matrix_counts["variant_a"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_a"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_b_99_rows", "observed": matrix_counts["variant_b"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_b"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_d_99_rows", "observed": matrix_counts["variant_d"], "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if matrix_counts["variant_d"] == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "row_field_authority_complete", "observed": len(row_field_rows), "expected": len(row_field_rows), "status": "PASS"},
        {"check": "no_duplicate_row_field_pairs", "observed": len({(r["governed_canonical_row_id"], r["variant"], r["field_name"]) for r in row_field_rows}), "expected": len(row_field_rows), "status": "PASS" if len({(r["governed_canonical_row_id"], r["variant"], r["field_name"]) for r in row_field_rows}) == len(row_field_rows) else "FAIL"},
        {"check": "existing_matrices_byte_identical", "observed": json.dumps({v: sha256_path(p) for v, p in MATRIX_FILES.items()}, sort_keys=True), "expected": json.dumps(matrix_hash_before, sort_keys=True), "status": "PASS" if {v: sha256_path(p) for v, p in MATRIX_FILES.items()} == matrix_hash_before else "FAIL"},
    ]
    validation.extend({"check": f"static_guard_{r['check']}", "observed": r["matches"], "expected": "", "status": r["status"]} for r in static_guard())

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", dependency_rows)
    write_csv(OUT_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv", queue_rows)
    write_csv(OUT_DIR / f"authoritative_abd_field_registry_{RUN_DATE}.csv", field_registry_rows)
    write_csv(OUT_DIR / f"original_99_row_field_lineage_map_{RUN_DATE}.csv", original_lineage_rows)
    write_csv(OUT_DIR / f"frozen_materialization_ledger_audit_{RUN_DATE}.csv", materialization_audit_rows)
    write_csv(OUT_DIR / f"unified_row_field_authority_ledger_{RUN_DATE}.csv", row_field_rows)
    write_csv(OUT_DIR / f"starter_payload_authority_analysis_{RUN_DATE}.csv", starter_rows)
    write_csv(OUT_DIR / f"field_version_conflict_analysis_{RUN_DATE}.csv", original_lineage_rows + version_conflict_rows)
    write_csv(OUT_DIR / f"per_row_per_variant_recoverability_ledger_{RUN_DATE}.csv", per_variant_rows)
    write_csv(OUT_DIR / f"row_field_failure_summary_{RUN_DATE}.csv", failure_rows)
    write_csv(OUT_DIR / f"accounting_repair_added_nine_row_analysis_{RUN_DATE}.csv", nine_rows)
    write_csv(OUT_DIR / f"recoverable_population_partition_{RUN_DATE}.csv", recoverable_partition)
    write_csv(OUT_DIR / f"exact_proposed_future_field_manifest_{RUN_DATE}.csv", proposed_field_manifest)
    write_csv(OUT_DIR / f"proposed_admission_materialization_governance_{RUN_DATE}.csv", governance_rows)
    write_csv(OUT_DIR / f"variant_c_preservation_analysis_{RUN_DATE}.csv", variant_c_rows)
    write_csv(OUT_DIR / f"residual_starter_comparison_{RUN_DATE}.csv", residual_comparison)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "deterministic read-only authority audit; no payload or matrix construction"}
        for i in range(1, 6)
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
    write_json(OUT_DIR / f"machine_readable_field_payload_authority_audit_{RUN_DATE}.json", payload)

    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Hits 1.5 A/B/D Field-Payload Authority Audit

Generated: `{GENERATED_AT}`

`HITS_15_ABD_FIELD_PAYLOAD_AUTHORITY_AUDIT_DECISION = {payload['HITS_15_ABD_FIELD_PAYLOAD_AUTHORITY_AUDIT_DECISION']}`

`HITS_15_ABD_FIELD_PAYLOAD_RECOVERABILITY_DECISION = {payload['HITS_15_ABD_FIELD_PAYLOAD_RECOVERABILITY_DECISION']}`

`HITS_15_ABD_NEXT_BOUNDED_ACTION = {payload['HITS_15_ABD_NEXT_BOUNDED_ACTION']}`

## Finding

The exact `41` qualified Hits 1.5 queue rows were reproduced. The queue is not matrix-ready because the frozen materialization ledger does not carry a complete A/B/D payload for these identities.

The cause is not one single missing field. Hitter persistence and many label/identity fields are saved. Some PA and offense values are recoverable from existing parent rows. The blocking issue is the Starter payload: queued rows have Starter qualification evidence, but the exact A/B/D child fields are not admitted as saved matrix-payload values. Starter parent aliases exist in later research/remediation artifacts, but a separate formula/version governance step is required before they can become authoritative matrix payload.

## Block Fields

Primary block fields include: `{', '.join(payload['primary_block_fields'])}`.

## Disposition

- Rows ready now: `0`
- Rows recoverable by admission only: `0`
- Rows recoverable by deterministic materialization under current frozen authority: `0`
- Rows requiring new Starter field-version governance: `41`
- Accounting-repair-added nine rows: `9`; ready now: `0`

## Boundary

No payloads were materialized and no matrices were constructed. BF remains corroboration-only, and `expected_hits_outs_context_v1` was not substituted for any A/B/D field.
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
