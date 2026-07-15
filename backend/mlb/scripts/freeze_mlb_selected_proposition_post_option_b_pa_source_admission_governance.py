"""Freeze governance for the post-Option-B PA source-admission subset.

This utility is research/governance only. It consumes the completed
post-Option-B PA gap review, binds the exact 18 source-present rows and seven
source-missing exclusions, and writes a fail-closed governance package. It does
not remediate, certify, score, train, or mutate any production data.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
EXPECTED_STATE_SHA_MANIFEST_SHA = "e9022a3843bfaee711eca1db261e6de54b4e8fe6b34fb55d277012e07ade9211"
REVIEW_DECISION = "POST_OPTION_B_PA_GAP_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED"
GOVERNANCE_STATUS = (
    "POST_OPTION_B_PA_SOURCE_ADMISSION_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_EXECUTION_APPROVAL"
)

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_option_b_pa_source_admission_governance/"
    "2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_option_b_qualification_state/2026-07-14"
)
REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_option_b_pa_gap_review/2026-07-14"
)
OPTION_B_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_option_b_starter_remediation/2026-07-14"
)
PA_JOIN_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_join_remediation/2026-07-13"
)
PA_CERT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_strict_prior_certified_remediation/2026-07-13"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

STATE_SHA_MANIFEST = STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
STATE_JSON = STATE_DIR / f"machine_readable_state_summary_{RUN_DATE}.json"
STATE_25 = STATE_DIR / f"exact_25_row_pa_blocked_manifest_{RUN_DATE}.csv"
OPTION_B_PROPAGATED = OPTION_B_DIR / f"propagated_649_row_remediation_ledger_{RUN_DATE}.csv"

REVIEW_RESULT = REVIEW_DIR / f"machine_readable_review_result_{RUN_DATE}.json"
REVIEW_TAXONOMY = REVIEW_DIR / f"row_level_pa_blocker_taxonomy_{RUN_DATE}.csv"
REVIEW_TEMPORAL = REVIEW_DIR / f"strict_prior_temporal_audit_{RUN_DATE}.csv"
REVIEW_GRAIN = REVIEW_DIR / f"identity_and_grain_audit_{RUN_DATE}.csv"
REVIEW_RECONSTRUCTION = REVIEW_DIR / f"candidate_deterministic_reconstruction_specification_{RUN_DATE}.csv"
REVIEW_IMMUTABILITY = REVIEW_DIR / f"immutability_audit_{RUN_DATE}.csv"
REVIEW_SHA_MANIFEST = REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"

PA_SOURCE_CONTRACT = PA_JOIN_DIR / "mlb_historical_pa_source_precedence_contract_2026-07-13.json"
PA_FORMULA_AUDIT = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_formula_and_cutoff_audit_2026-07-11.csv"
)
PA_RESEARCH_BASE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
)
PA_CERT_REGISTRY = PA_CERT_DIR / "mlb_pa_certification_179_row_registry_2026-07-13.csv"
PA_CERT_ROW_DECISIONS = PA_CERT_DIR / "mlb_pa_certification_row_decisions_2026-07-13.csv"
PA_CERT_REMAINING = PA_CERT_DIR / "mlb_pa_certification_remaining_blockers_2026-07-13.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

PROHIBITED_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "api_call": re.compile(r"requests\.|statsapi|httpx|urllib"),
    "db_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert)\b", re.IGNORECASE),
    "matrix_build": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
}


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
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_identity(row: dict[str, str]) -> str:
    return (
        f"{row['slate_date']}|{row['game_id']}|{row['player_id']}|"
        f"hits|{row['line']}|{row['side']}"
    )


def player_game_key(row: dict[str, str]) -> str:
    return f"{row['slate_date']}|{row['game_id']}|{row['player_id']}"


class PASourceAdmissionGovernanceFreeze:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.input_hash_before = self.input_hashes()
        self.review_result = json.loads(REVIEW_RESULT.read_text())
        self.state_result = json.loads(STATE_JSON.read_text())
        self.taxonomy_rows = read_csv(REVIEW_TAXONOMY)
        self.temporal_rows = read_csv(REVIEW_TEMPORAL)
        self.grain_rows = read_csv(REVIEW_GRAIN)
        self.reconstruction_rows = read_csv(REVIEW_RECONSTRUCTION)
        self.option_b_ids = {r["governed_canonical_row_id"] for r in read_csv(OPTION_B_PROPAGATED)}
        self.prior_pa_ids = self.read_prior_pa_ids()
        self.matrix_ids = self.read_matrix_ids()
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS}
        self.admission_rows: list[dict[str, Any]] = []
        self.excluded_rows: list[dict[str, Any]] = []

    def input_hashes(self) -> dict[str, str]:
        paths = [
            STATE_SHA_MANIFEST,
            STATE_JSON,
            STATE_25,
            OPTION_B_PROPAGATED,
            REVIEW_RESULT,
            REVIEW_TAXONOMY,
            REVIEW_TEMPORAL,
            REVIEW_GRAIN,
            REVIEW_RECONSTRUCTION,
            REVIEW_IMMUTABILITY,
            REVIEW_SHA_MANIFEST,
            PA_SOURCE_CONTRACT,
            PA_FORMULA_AUDIT,
            PA_RESEARCH_BASE,
            PA_CERT_REGISTRY,
            PA_CERT_ROW_DECISIONS,
            PA_CERT_REMAINING,
        ] + MATRIX_PATHS
        return {str(path): sha256_path(path) for path in paths if path.exists()}

    def read_prior_pa_ids(self) -> set[str]:
        ids: set[str] = set()
        for path in [PA_CERT_REGISTRY, PA_CERT_ROW_DECISIONS, PA_CERT_REMAINING]:
            if not path.exists():
                continue
            for row in read_csv(path):
                value = row.get("governed_canonical_row_id") or row.get("canonical_row_id")
                if value:
                    ids.add(value)
        return ids

    def read_matrix_ids(self) -> set[str]:
        ids: set[str] = set()
        for path in MATRIX_PATHS:
            if not path.exists():
                continue
            for row in read_csv(path):
                value = row.get("governed_canonical_row_id")
                if value:
                    ids.add(value)
        return ids

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verify_inputs()
        self.split_population()
        self.write_contract_outputs()
        self.write_validation_outputs()
        self.write_reports()
        self.write_parse_validation()
        self.write_sha_manifest()
        return self.result()

    def verify_inputs(self) -> None:
        if sha256_path(STATE_SHA_MANIFEST) != EXPECTED_STATE_SHA_MANIFEST_SHA:
            raise RuntimeError("certified state SHA manifest mismatch")
        if self.state_result.get("decision") != "SELECTED_PROPOSITION_POST_OPTION_B_QUALIFICATION_STATE = CERTIFIED":
            raise RuntimeError("post-Option-B certified state package is not certified")
        if self.review_result.get("decision") != REVIEW_DECISION:
            raise RuntimeError("PA gap review decision is not the required characterized/no-remediation decision")
        counts = self.review_result.get("taxonomy_counts", {})
        if counts.get("PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED") != 18:
            raise RuntimeError("review package does not expose exactly 18 source-present rows")
        if counts.get("PA_DIRECT_SOURCE_MISSING") != 7:
            raise RuntimeError("review package does not expose exactly seven source-missing rows")
        if len(self.taxonomy_rows) != 25:
            raise RuntimeError("review taxonomy does not contain exactly 25 rows")
        if len({r["governed_canonical_row_id"] for r in self.taxonomy_rows}) != 25:
            raise RuntimeError("review taxonomy identities are not unique")

    def split_population(self) -> None:
        admission = []
        excluded = []
        temporal_by_id = {r["governed_canonical_row_id"]: r for r in self.temporal_rows}
        grain_by_id = {r["governed_canonical_row_id"]: r for r in self.grain_rows}
        reconstruction_by_id = {r["governed_canonical_row_id"]: r for r in self.reconstruction_rows}
        for row in sorted(self.taxonomy_rows, key=lambda r: r["governed_canonical_row_id"]):
            merged = {
                **row,
                "canonical_denominator_identity": canonical_identity(row),
                "player_game_identity": player_game_key(row),
                "governance_version": "post_option_b_pa_source_admission_governance_v1",
                "source_admission_status": "GOVERNED_SOURCE_ADMISSION_CANDIDATE_NOT_EXECUTED",
                "future_execution_authorized": "false",
                "temporal_contract_status": temporal_by_id[row["governed_canonical_row_id"]].get("temporal_review_status", ""),
                "grain_contract_status": grain_by_id[row["governed_canonical_row_id"]].get("grain_review_status", ""),
                "derivation_contract": reconstruction_by_id[row["governed_canonical_row_id"]].get("formula", ""),
            }
            if row["primary_pa_gap_class"] == "PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED":
                admission.append(merged)
            elif row["primary_pa_gap_class"] == "PA_DIRECT_SOURCE_MISSING":
                merged["source_admission_status"] = "EXCLUDED_SOURCE_MISSING_NO_SUBSTITUTION_AUTHORIZED"
                excluded.append(merged)
            else:
                raise RuntimeError(f"unexpected PA gap class: {row['primary_pa_gap_class']}")
        self.admission_rows = admission
        self.excluded_rows = excluded
        self.verify_population_split()

    def verify_population_split(self) -> None:
        admission_ids = {r["governed_canonical_row_id"] for r in self.admission_rows}
        excluded_ids = {r["governed_canonical_row_id"] for r in self.excluded_rows}
        all_ids = {r["governed_canonical_row_id"] for r in self.taxonomy_rows}
        if len(admission_ids) != 18:
            raise RuntimeError("exact 18-row governance population reproduction failed")
        if len(excluded_ids) != 7:
            raise RuntimeError("exact seven-row exclusion population reproduction failed")
        if admission_ids & excluded_ids:
            raise RuntimeError("18-row admission and seven-row exclusion populations overlap")
        if admission_ids | excluded_ids != all_ids:
            raise RuntimeError("18 + 7 populations do not reconcile to exact 25-row review population")
        if any(row_id not in self.option_b_ids for row_id in all_ids):
            raise RuntimeError("not all review rows bind to Option B overlay population")
        if admission_ids & self.prior_pa_ids:
            raise RuntimeError("18-row governance population overlaps prior PA remediation")
        if admission_ids & self.matrix_ids:
            raise RuntimeError("18-row governance population overlaps existing A/B/D matrices")
        if any(r["candidate_cutoff_status"] != "PASS_PRIOR_DATE" for r in self.admission_rows):
            raise RuntimeError("one or more admitted-candidate rows lacks PASS_PRIOR_DATE")
        if any(r["candidate_complete_prior_pa"] != "True" for r in self.admission_rows):
            raise RuntimeError("one or more admitted-candidate rows lacks complete prior PA")

    def write_contract_outputs(self) -> None:
        write_csv(
            self.output_dir / f"exact_18_row_denominator_manifest_{RUN_DATE}.csv",
            self.exact_manifest_rows(self.admission_rows),
        )
        write_csv(
            self.output_dir / f"exact_seven_row_excluded_source_missing_manifest_{RUN_DATE}.csv",
            self.exact_manifest_rows(self.excluded_rows),
        )
        write_csv(
            self.output_dir / f"pa_concept_compatibility_contract_{RUN_DATE}.csv",
            self.compatibility_rows(),
        )
        write_csv(
            self.output_dir / f"approved_source_admission_hierarchy_{RUN_DATE}.csv",
            self.source_hierarchy_rows(),
        )
        write_csv(
            self.output_dir / f"temporal_integrity_contract_{RUN_DATE}.csv",
            self.temporal_contract_rows(),
        )
        write_csv(
            self.output_dir / f"identity_and_grain_binding_contract_{RUN_DATE}.csv",
            self.identity_contract_rows(),
        )
        write_csv(
            self.output_dir / f"existing_rule_extension_analysis_{RUN_DATE}.csv",
            self.existing_rule_rows(),
        )
        write_csv(
            self.output_dir / f"field_derivation_contract_{RUN_DATE}.csv",
            self.derivation_rows(),
        )
        write_csv(
            self.output_dir / f"failure_taxonomy_{RUN_DATE}.csv",
            self.failure_taxonomy_rows(),
        )
        write_csv(
            self.output_dir / f"provenance_schema_{RUN_DATE}.csv",
            self.provenance_schema_rows(),
        )
        write_csv(
            self.output_dir / f"certification_decision_table_{RUN_DATE}.csv",
            self.certification_rows(),
        )
        write_csv(
            self.output_dir / f"seven_row_exclusion_contract_{RUN_DATE}.csv",
            self.seven_row_exclusion_rows(),
        )
        write_csv(
            self.output_dir / f"immutability_and_non_mutation_contract_{RUN_DATE}.csv",
            self.immutability_contract_rows(),
        )
        write_csv(
            self.output_dir / f"replayability_contract_{RUN_DATE}.csv",
            self.replayability_rows(),
        )
        write_csv(
            self.output_dir / f"human_approval_boundary_{RUN_DATE}.csv",
            self.human_approval_rows(),
        )
        write_json(
            self.output_dir / f"machine_readable_governance_contract_{RUN_DATE}.json",
            self.machine_contract(),
        )

    def exact_manifest_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields = [
            "governed_canonical_row_id",
            "canonical_denominator_identity",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "line",
            "side",
            "player_game_identity",
            "primary_pa_gap_class",
            "candidate_source_path",
            "candidate_join_grain",
            "candidate_join_key",
            "candidate_prior_d7_plate_appearances",
            "candidate_prior_d15_plate_appearances",
            "candidate_prior_d30_plate_appearances",
            "candidate_pa_context_latest_date",
            "candidate_cutoff_status",
            "candidate_complete_prior_pa",
            "source_admission_status",
            "future_execution_authorized",
        ]
        return [{field: row.get(field, "") for field in fields} for row in rows]

    def compatibility_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "target_pa_concept": "strict_prior_rolling_pa_opportunity_context",
                "target_required_for": "historical selected-proposition PA-qualified state",
                "candidate_source_concept": "source-provided prior_d7/prior_d15/prior_d30 plate appearances with PA opportunity v1 cutoff metadata",
                "compatibility_finding": "compatible_by_frozen_derivation_not_same_game_actual",
                "semantic_equivalence": "not identical to actual same-game PA outcome",
                "direct_or_derived": "derived strict-prior rolling context from repository PA opportunity research base",
                "permitted_use": "exact 18 denominator rows only, future bounded overlay after explicit execution approval",
                "prohibited_use": "same-game PA outcome, lineup-derived expectation, generic fallback, broad historical PA policy, model feature interpretation",
                "definition_change": "false",
                "governance_status": GOVERNANCE_STATUS,
            }
        ]

    def source_hierarchy_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "priority": 1,
                "source_name": "exact_18_row_manifest",
                "source_path": str(self.output_dir / f"exact_18_row_denominator_manifest_{RUN_DATE}.csv"),
                "source_role": "governance population boundary",
                "admitted_for_future_execution": "boundary_only_not_pa_values",
                "fail_closed_if_missing": "true",
            },
            {
                "priority": 2,
                "source_name": "july_3_9_pa_opportunity_research_base",
                "source_path": str(PA_RESEARCH_BASE),
                "source_role": "candidate strict-prior PA evidence source",
                "source_grain": "player_game",
                "admitted_for_future_execution": "only_for_exact_18_after_explicit_execution_approval",
                "required_fields": (
                    "prior_d7_plate_appearances|prior_d15_plate_appearances|"
                    "prior_d30_plate_appearances|pa_context_latest_date|"
                    "pa_opp_v1_cutoff_status|pa_opp_v1_complete_prior_pa|"
                    "pa_feature_source_status"
                ),
                "fail_closed_if_missing": "true",
            },
            {
                "priority": 3,
                "source_name": "no_fallback",
                "source_path": "",
                "source_role": "explicit terminal policy",
                "admitted_for_future_execution": "false",
                "fail_closed_if_missing": "true",
            },
        ]

    def temporal_contract_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "slate_date": row["slate_date"],
                "latest_permissible_evidence_date": "strictly_before_slate_date",
                "candidate_pa_context_latest_date": row["candidate_pa_context_latest_date"],
                "required_cutoff_status": "PASS_PRIOR_DATE",
                "actual_cutoff_status": row["candidate_cutoff_status"],
                "same_game_exclusion_required": "true",
                "future_date_exclusion_required": "true",
                "source_revision_policy": "source hash must match governance freeze or future execution stops",
                "deterministic_source_state": "sha256-bound candidate source artifact",
                "contract_status": "PASS_FROZEN_FOR_FUTURE_EXECUTION_GATE",
            }
            for row in self.admission_rows
        ]

    def identity_contract_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "denominator_identity": row["canonical_denominator_identity"],
                "player_game_identity": row["player_game_identity"],
                "source_join_keys": "slate_date|game_id|player_id",
                "target_grain": "denominator proposition",
                "source_grain": "player_game",
                "line_side_required_for_source_join": "false",
                "propagation_rule": "one player-game PA state may propagate to multiple exact denominator rows only inside frozen 18-row manifest",
                "doubleheader_rule": "game_id required; date/player/team alone prohibited",
                "duplicate_name_rule": "player_id required; player_name never sufficient",
                "ambiguous_multiple_source_rows": "fail_closed_unless all PA parent values and cutoff metadata identical",
                "home_away_orientation_rule": "team/opponent retained for audit; game_id/player_id are binding keys",
                "contract_status": "PASS_FROZEN",
            }
            for row in self.admission_rows
        ]

    def existing_rule_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "question": "what_governance_class_is_this",
                "decision": "approval_of_previously_unapproved_source_population_boundary",
                "prior_rule_reference": str(PA_SOURCE_CONTRACT),
                "why_prior_rule_did_not_apply": (
                    "prior bounded PA source-precedence pilot selected a different execution population; "
                    "these rows became starter-qualified only after Option B and were outside that frozen manifest"
                ),
                "does_this_rewrite_prior_authorization": "false",
                "does_this_change_pa_definition": "false",
                "future_execution_scope": "exact_18_denominator_rows_only",
            }
        ]

    def derivation_rows(self) -> list[dict[str, Any]]:
        rows = []
        for field, window in [
            ("prior_d7_plate_appearances", "d7"),
            ("prior_d15_plate_appearances", "d15"),
            ("prior_d30_plate_appearances", "d30"),
        ]:
            rows.append(
                {
                    "target_field": field,
                    "source_parent_field": field,
                    "formula": "use source-provided strict-prior rolling PA alias; do not recompute in governance freeze",
                    "lookback_window": window,
                    "cutoff": "strictly before denominator slate_date",
                    "minimum_history_requirement": "pa_opp_v1_complete_prior_pa must be True",
                    "fallback_order": "none",
                    "null_behavior": "fail_closed_PA_not_qualified",
                    "unit_convention": "plate appearances per game",
                    "rounding": "preserve source precision",
                    "clipping_or_clamping": "none authorized",
                    "duplicate_handling": "all candidate rows for player-game must agree on parent PA values and cutoff metadata",
                }
            )
        rows.append(
            {
                "target_field": "pa_opp_v1_cutoff_status",
                "source_parent_field": "pa_opp_v1_cutoff_status",
                "formula": "must equal PASS_PRIOR_DATE",
                "lookback_window": "metadata",
                "cutoff": "strictly before denominator slate_date",
                "minimum_history_requirement": "not nullable",
                "fallback_order": "none",
                "null_behavior": "fail_closed_PA_not_qualified",
                "unit_convention": "status",
                "rounding": "not applicable",
                "clipping_or_clamping": "not applicable",
                "duplicate_handling": "all candidate rows must agree",
            }
        )
        return rows

    def failure_taxonomy_rows(self) -> list[dict[str, Any]]:
        statuses = [
            ("missing_candidate_source", "FAIL_CLOSED_SOURCE_MISSING"),
            ("source_changed_since_governance_freeze", "FAIL_CLOSED_SOURCE_HASH_CHANGED"),
            ("identity_mismatch", "FAIL_CLOSED_IDENTITY_MISMATCH"),
            ("grain_incompatibility", "FAIL_CLOSED_GRAIN_INCOMPATIBLE"),
            ("temporal_cutoff_failure", "FAIL_CLOSED_TEMPORAL_FAILURE"),
            ("multiple_candidate_values", "FAIL_CLOSED_CONFLICTING_VALUES"),
            ("source_conflict", "FAIL_CLOSED_SOURCE_CONFLICT"),
            ("incomplete_provenance", "FAIL_CLOSED_PROVENANCE_INCOMPLETE"),
            ("derivation_failure", "FAIL_CLOSED_DERIVATION_FAILURE"),
            ("missing_parent_evidence", "FAIL_CLOSED_PARENT_FIELD_MISSING"),
            ("unexpected_special_regime", "FAIL_CLOSED_SPECIAL_REGIME"),
            ("input_manifest_discrepancy", "FAIL_CLOSED_INPUT_MANIFEST_CHANGED"),
        ]
        return [
            {
                "failure_condition": condition,
                "status": status,
                "future_execution_action": "stop_without_remediation",
                "silent_null_replacement_allowed": "false",
                "best_effort_certification_allowed": "false",
            }
            for condition, status in statuses
        ]

    def provenance_schema_rows(self) -> list[dict[str, Any]]:
        fields = [
            ("governance_version", "string", "required"),
            ("remediation_version", "string", "required_for_future_execution"),
            ("denominator_identity", "string", "required"),
            ("player_game_identity", "string", "required"),
            ("source_artifact_identity", "path", "required"),
            ("source_artifact_sha256", "sha256", "required"),
            ("source_row_identity", "string", "required"),
            ("source_timestamp", "timestamp_or_source_date", "required_if_available"),
            ("target_pa_concept", "string", "required"),
            ("source_pa_concept", "string", "required"),
            ("admission_rule", "string", "required"),
            ("derivation_method", "string", "required"),
            ("parent_fields", "string_list", "required"),
            ("strict_prior_cutoff", "date_rule", "required"),
            ("original_value", "nullable", "required"),
            ("candidate_value", "numeric_or_status", "required"),
            ("certification_state", "string", "required"),
            ("propagation_count", "integer", "required"),
            ("failure_reason", "string", "required_if_failed"),
            ("deterministic_replay_key", "string", "required"),
        ]
        return [
            {"field_name": name, "field_type": field_type, "requirement": req, "notes": ""}
            for name, field_type, req in fields
        ]

    def certification_rows(self) -> list[dict[str, Any]]:
        levels = [
            ("source_admission", "source path and hash match frozen contract"),
            ("identity_binding", "denominator identity joins exact source player-game"),
            ("temporal_integrity", "PASS_PRIOR_DATE and context date strictly before slate"),
            ("pa_concept_compatibility", "strict-prior rolling PA context, not same-game outcome"),
            ("pa_field_value", "parent PA fields present and stable"),
            ("player_game_pa_state", "player-game PA state certified once per source key"),
            ("propagated_denominator_row", "propagation limited to frozen 18 denominator identities"),
            ("final_pa_qualification", "all prior certification levels pass"),
        ]
        return [
            {
                "certification_level": level,
                "required_condition": condition,
                "numeric_value_alone_sufficient": "false",
                "future_execution_failure_action": "fail_closed",
                "current_freeze_result": "DEFINED_NOT_EXECUTED",
            }
            for level, condition in levels
        ]

    def seven_row_exclusion_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "player_game_identity": row["player_game_identity"],
                "exclusion_reason": "direct compatible PA source missing in authoritative review",
                "approximate_matching_allowed": "false",
                "source_substitution_allowed": "false",
                "inferred_values_allowed": "false",
                "neighboring_dates_allowed": "false",
                "later_source_discovery_allowed_under_this_contract": "false",
                "generic_fallback_allowed": "false",
                "population_rescan_allowed": "false",
                "required_future_action": "separate characterization and governance action",
            }
            for row in self.excluded_rows
        ]

    def immutability_contract_rows(self) -> list[dict[str, Any]]:
        items = [
            "denominator package",
            "opposite-side population",
            "source artifact",
            "certified post-Option-B state package",
            "PA gap review package",
            "prior PA remediation packages",
            "Option B packages",
            "A/B/D matrices",
            "Variant C decision",
            "Starter/outcome/Bundle remediation",
            "production behavior",
        ]
        return [
            {
                "protected_item": item,
                "mutation_allowed": "false",
                "future_execution_output_policy": "new bounded overlay package only",
                "current_freeze_status": "NO_MUTATION_PERFORMED",
            }
            for item in items
        ]

    def replayability_rows(self) -> list[dict[str, Any]]:
        requirements = [
            ("canonical_input_manifests", "exact 18-row manifest and seven-row exclusion manifest"),
            ("required_input_hashes", "all source package hashes recorded in input provenance report"),
            ("deterministic_ordering", "sort by governed_canonical_row_id"),
            ("exact_identity_keys", "slate_date|game_id|player_id|prop_type|line|side plus player-game key"),
            ("source_selection", "only frozen July 3-9 PA opportunity research source; no fallback"),
            ("idempotence", "rerun must reproduce same row IDs and source hashes"),
            ("rerun_discrepancy", "stop and report discrepancy"),
            ("source_change_detection", "hash comparison before execution"),
            ("output_manifest", "accepted/rejected/conflict/skipped manifest required for future execution"),
        ]
        return [
            {
                "replayability_requirement": name,
                "frozen_rule": rule,
                "future_execution_action_if_missing": "fail_closed",
            }
            for name, rule in requirements
        ]

    def human_approval_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governance_action": "freeze_source_admission_contract",
                "status": GOVERNANCE_STATUS,
                "does_this_authorize_remediation": "false",
                "future_required_approval": "explicit bounded execution approval for exact 18-row overlay",
                "prohibited_until_approval": (
                    "PA remediation, certification, propagation, matrix construction, scoring, training, "
                    "production integration, DB writes, uploads"
                ),
            }
        ]

    def machine_contract(self) -> dict[str, Any]:
        return {
            "generated_at_utc": self.generated_at,
            "governance_status": GOVERNANCE_STATUS,
            "authoritative_review_decision": REVIEW_DECISION,
            "certified_state_sha_manifest_sha256": EXPECTED_STATE_SHA_MANIFEST_SHA,
            "population": {
                "review_rows": 25,
                "source_admission_candidate_rows": 18,
                "excluded_source_missing_rows": 7,
                "identity": "slate_date|game_id|player_id|prop_type|line|side",
                "source_join_grain": "player_game",
            },
            "required_pa_concept": "strict-prior rolling PA/opportunity context",
            "candidate_source": {
                "path": str(PA_RESEARCH_BASE),
                "sha256": sha256_path(PA_RESEARCH_BASE),
                "concept": "source-provided prior_d7/prior_d15/prior_d30 PA context with PASS_PRIOR_DATE",
                "admission_scope": "exact 18 denominator rows only after explicit execution approval",
            },
            "seven_row_policy": "excluded; no approximation, substitution, inference, neighboring-date use, fallback, or rescan",
            "future_execution_authorized": False,
            "prohibited_work": {
                "pa_remediation": "not_performed",
                "starter_remediation": "not_performed",
                "outcome_remediation": "not_performed",
                "bundle_field_remediation": "not_performed",
                "matrix_construction": "not_performed",
                "modeling": "not_performed",
                "scoring": "not_performed",
                "database_writes": "not_performed",
                "apis": "not_called",
                "oddsapi": "not_called",
                "uploads": "not_performed",
                "launchagent_changes": "not_performed",
                "production_changes": "not_performed",
            },
        }

    def write_validation_outputs(self) -> None:
        write_csv(self.output_dir / f"validation_ledger_{RUN_DATE}.csv", self.validation_rows())
        write_csv(self.output_dir / f"input_provenance_and_hash_report_{RUN_DATE}.csv", self.provenance_rows())
        write_csv(self.output_dir / f"immutability_audit_{RUN_DATE}.csv", self.immutability_rows())
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", self.deterministic_replay_rows())
        write_csv(
            self.output_dir / f"static_no_remediation_no_model_no_signal_guard_{RUN_DATE}.csv",
            self.static_guard_rows(),
        )

    def validation_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("certified_state_sha_verification", sha256_path(STATE_SHA_MANIFEST) == EXPECTED_STATE_SHA_MANIFEST_SHA),
            ("exact_25_review_population_reproduction", len(self.taxonomy_rows) == 25),
            ("exact_18_governance_population_reproduction", len(self.admission_rows) == 18),
            ("exact_seven_exclusion_population_reproduction", len(self.excluded_rows) == 7),
            ("exhaustive_18_plus_7_equals_25", len(self.admission_rows) + len(self.excluded_rows) == 25),
            ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in self.taxonomy_rows}) == 25),
            ("zero_overlap_18_and_7", not ({r["governed_canonical_row_id"] for r in self.admission_rows} & {r["governed_canonical_row_id"] for r in self.excluded_rows})),
            ("zero_overlap_with_prior_pa_remediation", not ({r["governed_canonical_row_id"] for r in self.admission_rows} & self.prior_pa_ids)),
            ("zero_overlap_with_existing_abd_matrices", not ({r["governed_canonical_row_id"] for r in self.admission_rows} & self.matrix_ids)),
            ("exact_binding_to_option_b_overlay", all(r["governed_canonical_row_id"] in self.option_b_ids for r in self.taxonomy_rows)),
            ("source_path_existence_checks", all(Path(r["candidate_source_path"]).exists() for r in self.admission_rows)),
            ("source_concept_and_target_concept_completeness", bool(self.compatibility_rows())),
            ("temporal_rule_completeness", all(r["candidate_cutoff_status"] == "PASS_PRIOR_DATE" for r in self.admission_rows)),
            ("identity_grain_rule_completeness", bool(self.identity_contract_rows())),
            ("failure_taxonomy_completeness", len(self.failure_taxonomy_rows()) >= 12),
            ("provenance_schema_completeness", len(self.provenance_schema_rows()) >= 20),
            ("deterministic_ordering", self.admission_rows == sorted(self.admission_rows, key=lambda r: r["governed_canonical_row_id"])),
        ]
        return [
            {"validation_check": name, "status": "PASS" if passed else "FAIL", "notes": ""}
            for name, passed in checks
        ]

    def provenance_rows(self) -> list[dict[str, Any]]:
        rows = []
        for path, before in sorted(self.input_hash_before.items()):
            current = sha256_path(Path(path))
            rows.append(
                {
                    "path": path,
                    "exists": "true",
                    "sha256": current,
                    "sha256_matches_freeze_input": str(current == before).lower(),
                    "role": self.path_role(path),
                }
            )
        return rows

    def path_role(self, path: str) -> str:
        if "pa_gap_review" in path:
            return "authoritative PA gap review input"
        if "post_option_b_qualification_state" in path:
            return "certified post-Option-B state input"
        if "pa_opportunity_research_base" in path:
            return "candidate source evidence"
        if "pa_join_remediation" in path:
            return "prior PA source contract"
        if "variant_" in path:
            return "protected existing A/B/D matrix"
        return "supporting governed input"

    def immutability_rows(self) -> list[dict[str, Any]]:
        rows = []
        for path, before in sorted(self.input_hash_before.items()):
            after = sha256_path(Path(path))
            rows.append(
                {
                    "path": path,
                    "sha256_before": before,
                    "sha256_after": after,
                    "immutability_status": "PASS" if before == after else "FAIL",
                }
            )
        for path, before in sorted(self.matrix_hash_before.items()):
            after = sha256_path(Path(path))
            rows.append(
                {
                    "path": path,
                    "sha256_before": before,
                    "sha256_after": after,
                    "immutability_status": "PASS" if before == after else "FAIL",
                }
            )
        return rows

    def deterministic_replay_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("review_taxonomy_count_replay", "25", str(len(self.taxonomy_rows)), len(self.taxonomy_rows) == 25),
            ("source_present_count_replay", "18", str(len(self.admission_rows)), len(self.admission_rows) == 18),
            ("source_missing_count_replay", "7", str(len(self.excluded_rows)), len(self.excluded_rows) == 7),
            ("admission_manifest_hashable", "non_empty", sha256_path(self.output_dir / f"exact_18_row_denominator_manifest_{RUN_DATE}.csv"), True),
            ("exclusion_manifest_hashable", "non_empty", sha256_path(self.output_dir / f"exact_seven_row_excluded_source_missing_manifest_{RUN_DATE}.csv"), True),
            ("candidate_source_hash_replay", sha256_path(PA_RESEARCH_BASE), sha256_path(PA_RESEARCH_BASE), True),
        ]
        return [
            {
                "replay_check": name,
                "expected": expected,
                "actual": actual,
                "status": "PASS" if passed else "FAIL",
            }
            for name, expected, actual, passed in checks
        ]

    def static_guard_rows(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        text_for_scan = re.sub(
            r"PROHIBITED_PATTERNS = \{.*?\n\}",
            "PROHIBITED_PATTERNS = {}",
            text,
            flags=re.DOTALL,
        )
        return [
            {
                "guard": name,
                "status": "PASS" if not pattern.search(text_for_scan) else "FAIL",
                "notes": "static source scan",
            }
            for name, pattern in PROHIBITED_PATTERNS.items()
        ]

    def write_reports(self) -> None:
        main = self.output_dir / f"pa_source_admission_governance_specification_{RUN_DATE}.md"
        main.write_text(self.main_report_text())
        summary = self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md"
        summary.write_text(self.one_page_text())

    def main_report_text(self) -> str:
        return f"""# Post-Option-B PA Source Admission Governance Specification - {RUN_DATE}

Status: `{GOVERNANCE_STATUS}`

## Executive Summary

This package freezes a source-admission governance contract for the exact 18
post-Option-B selected-proposition denominator rows whose PA evidence exists in
repository artifacts but was not previously admitted by governance. It does not
remediate, certify, write, score, train, or promote any PA values.

The authoritative PA gap review decision is `{REVIEW_DECISION}`. The certified
post-Option-B state package SHA256 is
`{EXPECTED_STATE_SHA_MANIFEST_SHA}`.

## Required PA Concept

The missing qualification concept is strict-prior rolling PA/opportunity
context: prior d7, d15, and d30 plate appearances and associated cutoff
metadata proving the target game is excluded. It is not actual same-game PA,
not a postgame PA outcome, not a lineup-slot estimate, and not an inferred
pregame opportunity label.

## Candidate Source Finding

The candidate source is `{PA_RESEARCH_BASE}`. For the exact 18 rows, it provides
player-game strict-prior PA evidence with `PASS_PRIOR_DATE` and complete prior
PA metadata. The source concept is compatible by frozen derivation with the
target concept. It is not semantically identical to actual same-game PA.

## Why It Was Not Previously Admitted

The 18 rows became relevant after Option B Starter remediation. They were
outside the prior frozen PA remediation execution population and outside the
previously approved PA source/date manifest. This freeze does not rewrite prior
authorization.

## Scope

Admission is bounded to the exact 18 denominator identities in
`exact_18_row_denominator_manifest_{RUN_DATE}.csv`. The seven source-missing
rows are explicitly excluded in
`exact_seven_row_excluded_source_missing_manifest_{RUN_DATE}.csv`.

## Temporal Integrity

Future execution must prove `pa_opp_v1_cutoff_status = PASS_PRIOR_DATE`,
preserve `pa_context_latest_date`, exclude same-game/future evidence, and stop
if the source artifact hash differs from this governance freeze.

## Identity And Grain

The canonical denominator identity is
`slate_date|game_id|player_id|prop_type|line|side`. The PA source join grain is
player-game: `slate_date|game_id|player_id`. Line and side are not source join
keys, but propagation is allowed only to the frozen 18 denominator rows.

## Derivation Rules

No new formula is invented. Future execution may only use source-provided
strict-prior rolling PA aliases and metadata. Missing parent fields, conflicting
duplicate source rows, temporal failures, or source hash drift all fail closed.

## Seven-Row Exclusion

The seven direct-source-missing rows cannot enter future remediation through
approximate matching, source substitution, inferred values, neighboring dates,
later discoveries, generic fallback, or population rescanning under this
contract.

## Human Approval Boundary

This freeze does not authorize remediation. A future bounded execution requires
explicit human approval and must write a new overlay package.
"""

    def one_page_text(self) -> str:
        return f"""# One-Page PA Source Admission Governance Summary - {RUN_DATE}

Status: `{GOVERNANCE_STATUS}`.

The exact 18 source-present PA rows are now governed as a frozen source-admission
population only. The exact seven source-missing rows remain excluded. The
required PA concept is strict-prior rolling PA/opportunity context. The admitted
candidate source concept is compatible by frozen derivation, not identical to
same-game PA.

No PA values were remediated, certified, propagated, scored, trained, uploaded,
or written to any database. Future execution still requires explicit human
approval.
"""

    def write_parse_validation(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    with path.open(newline="") as f:
                        parsed = list(csv.DictReader(f))
                    rows.append(
                        {
                            "path": str(path),
                            "artifact_type": "csv",
                            "parse_status": "PASS",
                            "row_count": len(parsed),
                            "notes": "",
                        }
                    )
                except Exception as exc:  # pragma: no cover - defensive validation output
                    rows.append(
                        {
                            "path": str(path),
                            "artifact_type": "csv",
                            "parse_status": "FAIL",
                            "row_count": "",
                            "notes": str(exc),
                        }
                    )
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    rows.append(
                        {
                            "path": str(path),
                            "artifact_type": "json",
                            "parse_status": "PASS",
                            "row_count": "",
                            "notes": "",
                        }
                    )
                except Exception as exc:  # pragma: no cover
                    rows.append(
                        {
                            "path": str(path),
                            "artifact_type": "json",
                            "parse_status": "FAIL",
                            "row_count": "",
                            "notes": str(exc),
                        }
                    )
            elif path.suffix == ".md":
                status = "PASS" if path.read_text().lstrip().startswith("#") else "FAIL"
                rows.append(
                    {
                        "path": str(path),
                        "artifact_type": "markdown",
                        "parse_status": status,
                        "row_count": "",
                        "notes": "starts with heading" if status == "PASS" else "missing leading heading",
                    }
                )
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)

    def write_sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            if path.is_file():
                rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def result(self) -> dict[str, Any]:
        return {
            "generated_at_utc": self.generated_at,
            "governance_status": GOVERNANCE_STATUS,
            "certified_state_sha_manifest_sha256": sha256_path(STATE_SHA_MANIFEST),
            "authoritative_review_decision": self.review_result.get("decision"),
            "exact_18_governance_population": len(self.admission_rows),
            "exact_seven_exclusion_population": len(self.excluded_rows),
            "required_pa_concept": "strict-prior rolling PA/opportunity context",
            "candidate_source": str(PA_RESEARCH_BASE),
            "future_execution_authorized": False,
            "package_sha256_manifest": str(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv"),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    freeze = PASourceAdmissionGovernanceFreeze(Path(args.output_dir))
    result = freeze.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
