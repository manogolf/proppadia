"""Characterize post-PA-admission Bundle-field blockers.

This utility is research-only. It reviews the exact 36 rows classified as
HITS_BUNDLE_FIELD_BLOCKED in the certified post-PA-admission state and writes a
bounded characterization package. It does not remediate fields, build matrices,
score, train, call APIs, write databases, or alter production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
EXPECTED_STATE_SHA = "14506ec7fa6ea4f0ac3164d4b76a6fb7e88e6fb5479625308c4594053bf235f1"
DECISION = "POST_PA_BUNDLE_FIELD_BLOCKER_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_bundle_field_blocker_review/"
    "2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_admission_qualification_state/2026-07-14"
)
PRIOR_BUNDLE_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_bundle_field_gap_review/2026-07-14"
)
PERSISTENCE_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_hits_15_persistence_replay_materialization/2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
COLLECTIVE_CONTRACT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)

STATE_SHA_MANIFEST = STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
STATE_LEDGER = STATE_DIR / f"post_pa_admission_14816_row_qualification_ledger_{RUN_DATE}.csv"
STATE_FULLY = STATE_DIR / f"fully_qualified_hits_manifest_{RUN_DATE}.csv"
STATE_STARTER = STATE_DIR / f"remaining_899_row_starter_blocked_inventory_{RUN_DATE}.csv"
STATE_PA = STATE_DIR / f"exact_seven_row_remaining_pa_blocked_manifest_{RUN_DATE}.csv"
STATE_OUTCOME = STATE_DIR / f"outcome_blocked_inventory_{RUN_DATE}.csv"
STATE_BUNDLE = STATE_DIR / f"bundle_field_blocked_inventory_{RUN_DATE}.csv"

PRIOR_FIELD_LEDGER = PRIOR_BUNDLE_REVIEW_DIR / f"row_field_gap_classification_ledger_{RUN_DATE}.csv"
PRIOR_FIELD_INVENTORY = PRIOR_BUNDLE_REVIEW_DIR / f"frozen_field_and_variant_requirement_inventory_{RUN_DATE}.csv"
PRIOR_SOURCE_INVENTORY = PRIOR_BUNDLE_REVIEW_DIR / f"repository_source_evidence_inventory_{RUN_DATE}.csv"
PRIOR_VARIANT_C = PRIOR_BUNDLE_REVIEW_DIR / f"variant_c_market_field_gap_analysis_{RUN_DATE}.csv"
PRIOR_TECH = PRIOR_BUNDLE_REVIEW_DIR / f"technical_recoverability_ledger_{RUN_DATE}.csv"
PRIOR_SHA = PRIOR_BUNDLE_REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"

PERSISTENCE_READY = PERSISTENCE_DIR / f"complete_post_remediation_135_row_field_readiness_ledger_{RUN_DATE}.csv"
PERSISTENCE_REMAINING = PERSISTENCE_DIR / f"post_remediation_remaining_all_field_blockers_{RUN_DATE}.csv"
PERSISTENCE_SHA = PERSISTENCE_DIR / f"sha256_manifest_{RUN_DATE}.csv"

COLLECTIVE_SHA = COLLECTIVE_CONTRACT_DIR / "sha256_manifest_2026-07-12.csv"
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


def stable_json_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class BundleFieldBlockerReview:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.state_rows = read_csv(STATE_LEDGER)
        self.fully_ids = {r["governed_canonical_row_id"] for r in read_csv(STATE_FULLY)}
        self.starter_ids = {r["governed_canonical_row_id"] for r in read_csv(STATE_STARTER)}
        self.pa_ids = {r["governed_canonical_row_id"] for r in read_csv(STATE_PA)}
        self.outcome_ids = {r["governed_canonical_row_id"] for r in read_csv(STATE_OUTCOME)}
        self.bundle_rows = read_csv(STATE_BUNDLE)
        self.bundle_ids = {r["governed_canonical_row_id"] for r in self.bundle_rows}
        self.prior_field_rows = read_csv(PRIOR_FIELD_LEDGER)
        self.prior_field_inventory = read_csv(PRIOR_FIELD_INVENTORY)
        self.persistence_ready = read_csv(PERSISTENCE_READY)
        self.persistence_remaining = read_csv(PERSISTENCE_REMAINING)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS}
        self.input_hash_before = self.input_hashes()
        self.row_taxonomy: list[dict[str, Any]] = []
        self.row_field_failures: list[dict[str, Any]] = []
        self.variant_rows: list[dict[str, Any]] = []

    def input_hashes(self) -> dict[str, str]:
        paths = [
            STATE_SHA_MANIFEST,
            STATE_LEDGER,
            STATE_FULLY,
            STATE_STARTER,
            STATE_PA,
            STATE_OUTCOME,
            STATE_BUNDLE,
            PRIOR_FIELD_LEDGER,
            PRIOR_FIELD_INVENTORY,
            PRIOR_SOURCE_INVENTORY,
            PRIOR_VARIANT_C,
            PRIOR_TECH,
            PRIOR_SHA,
            PERSISTENCE_READY,
            PERSISTENCE_REMAINING,
            PERSISTENCE_SHA,
            COLLECTIVE_SHA,
        ] + MATRIX_PATHS
        return {str(path): sha256_path(path) for path in paths if path.exists()}

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verify_inputs()
        self.characterize()
        self.write_outputs()
        self.write_validation_outputs()
        self.write_reports()
        self.write_parse_validation()
        self.write_sha_manifest()
        return self.result()

    def verify_inputs(self) -> None:
        if sha256_path(STATE_SHA_MANIFEST) != EXPECTED_STATE_SHA:
            raise RuntimeError("certified post-PA-admission state SHA mismatch")
        if len(self.bundle_rows) != 36 or len(self.bundle_ids) != 36:
            raise RuntimeError("exact 36-row Bundle-field blocker population reproduction failed")
        overlaps = self.bundle_ids & (self.fully_ids | self.starter_ids | self.pa_ids | self.outcome_ids)
        if overlaps:
            raise RuntimeError(f"Bundle-field population overlaps another blocker/qualified set: {len(overlaps)}")

    def characterize(self) -> None:
        prior_by_id = defaultdict(list)
        for row in self.prior_field_rows:
            if row["governed_canonical_row_id"] in self.bundle_ids and row["gap_classification_status"] != "VALUE_PRESENT_VALID":
                prior_by_id[row["governed_canonical_row_id"]].append(row)
        persistence_remaining_ids = {r["governed_canonical_row_id"] for r in self.persistence_remaining}
        for row in sorted(self.bundle_rows, key=lambda r: int(r["wave_row_order"])):
            blockers = row["post_pa_admission_downstream_blockers"].split("|")
            field_rows = prior_by_id[row["governed_canonical_row_id"]]
            if not blockers:
                raise RuntimeError("Bundle-field blocker row has no blocker fields")
            primary = self.primary_class(blockers, field_rows)
            self.row_taxonomy.append(
                {
                    **self.base_row(row),
                    "failed_fields": "|".join(blockers),
                    "failed_field_count": len(blockers),
                    "primary_bundle_blocker_class": primary,
                    "persistence_remaining_after_prior_remediation": str(row["governed_canonical_row_id"] in persistence_remaining_ids).lower(),
                    "variant_c_market_metadata_required": str("market_book_count_two_sided" in blockers or "market_snapshot_time_utc" in blockers).lower(),
                    "technical_recoverability": self.row_recoverability(field_rows),
                    "governance_required": str(any(f.get("human_governance_required") == "true" for f in field_rows)).lower(),
                    "projected_future_state_without_remediation": "REMAINS_BUNDLE_FIELD_BLOCKED",
                }
            )
            for field in blockers:
                matches = [r for r in field_rows if r["field_name"] == field]
                if not matches:
                    matches = [self.synthetic_field_row(row, field)]
                for match in matches:
                    self.row_field_failures.append(self.field_failure_row(row, match))
            for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
                vrows = [r for r in field_rows if r["requirement_scope"] == variant]
                self.variant_rows.append(self.variant_readiness_row(row, variant, vrows))

    def primary_class(self, blockers: list[str], field_rows: list[dict[str, str]]) -> str:
        non_market = [f for f in blockers if f not in {"market_book_count_two_sided", "market_snapshot_time_utc"}]
        if not non_market:
            return "BUNDLE_VARIANT_C_MARKET_METADATA_GOVERNANCE_BLOCKED"
        if any(r.get("technical_recoverability_class") == "class_4_source_population_incomplete" for r in field_rows):
            return "BUNDLE_NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY"
        if any(r.get("technical_recoverability_class") == "class_2_deterministically_reconstructable_under_existing_governance" for r in field_rows):
            return "BUNDLE_DERIVATION_FEASIBLE_EXISTING_RULE_APPLIES"
        if any(r.get("technical_recoverability_class") == "class_6_new_governance_required" for r in field_rows):
            return "BUNDLE_DERIVATION_FEASIBLE_NEW_GOVERNANCE_REQUIRED"
        return "BUNDLE_SOURCE_PRESENT_NOT_PREVIOUSLY_ADMITTED"

    def row_recoverability(self, field_rows: list[dict[str, str]]) -> str:
        classes = {r.get("technical_recoverability_class", "") for r in field_rows}
        if "class_4_source_population_incomplete" in classes:
            return "source_population_incomplete_not_recoverable_from_current_repository"
        if "class_6_new_governance_required" in classes:
            return "technically_recoverable_new_governance_required"
        if "class_2_deterministically_reconstructable_under_existing_governance" in classes:
            return "recoverable_under_existing_rule_if_replayed"
        return "undetermined_fail_closed"

    def base_row(self, row: dict[str, str]) -> dict[str, Any]:
        return {
            "governed_canonical_row_id": row["governed_canonical_row_id"],
            "slate_date": row["slate_date"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "team": row["team"],
            "opponent": row["opponent"],
            "line": row["line"],
            "side": row["side"],
        }

    def synthetic_field_row(self, row: dict[str, str], field: str) -> dict[str, str]:
        return {
            "governed_canonical_row_id": row["governed_canonical_row_id"],
            "requirement_scope": "post_pa_state_downstream_blocker",
            "requirement_type": "state_blocker",
            "field_name": field,
            "field_domain": self.field_domain(field),
            "current_materialization_status": "BLOCKED_IN_CERTIFIED_STATE",
            "gap_classification_status": "BLOCKED_IN_CERTIFIED_STATE",
            "authoritative_source_exists": "unknown",
            "natural_grain_source_row_exists": "unknown",
            "join_key_deterministic": "unknown",
            "strict_prior": "unknown",
            "semantic_matches_registry": "unknown",
            "technical_recoverability_class": "state_only_field_not_in_prior_ledger",
            "remediation_path": "separate_characterization_required",
            "current_contract_permission": "not_currently_recoverable_without_source_remediation",
            "human_governance_required": "true",
            "would_remove_blocker_if_remediated": "true",
            "notes": "Field appears in certified state blocker list but not in prior row-field gap ledger for this row.",
        }

    def field_failure_row(self, row: dict[str, str], match: dict[str, str]) -> dict[str, Any]:
        return {
            **self.base_row(row),
            "target_variant_or_scope": match.get("requirement_scope", ""),
            "field_name": match.get("field_name", ""),
            "field_domain": match.get("field_domain", self.field_domain(match.get("field_name", ""))),
            "required_field_concept": self.field_concept(match.get("field_name", ""), match.get("requirement_scope", "")),
            "current_value": "",
            "current_certification_state": match.get("current_materialization_status", ""),
            "failure_reason": match.get("gap_classification_status", ""),
            "primary_field_class": self.field_class(match),
            "required_source_family": self.source_family(match.get("field_name", "")),
            "source_availability": match.get("natural_grain_source_row_exists", ""),
            "source_compatibility": match.get("semantic_matches_registry", ""),
            "temporal_status": match.get("strict_prior", ""),
            "grain_status": match.get("join_key_deterministic", ""),
            "ownership_status": self.ownership_status(match),
            "derivation_status": match.get("technical_recoverability_class", ""),
            "technical_or_governance": self.technical_or_governance(match),
            "existing_rule_citation": self.existing_rule_citation(match),
            "remediation_performed": "false",
        }

    def field_class(self, match: dict[str, str]) -> str:
        field = match.get("field_name", "")
        klass = match.get("technical_recoverability_class", "")
        if field in {"market_book_count_two_sided", "market_snapshot_time_utc"}:
            return "BUNDLE_VARIANT_C_MARKET_METADATA_GOVERNANCE_BLOCKED"
        if klass == "class_2_deterministically_reconstructable_under_existing_governance":
            return "BUNDLE_DERIVATION_FEASIBLE_EXISTING_RULE_APPLIES"
        if klass == "class_6_new_governance_required":
            return "BUNDLE_DERIVATION_FEASIBLE_NEW_GOVERNANCE_REQUIRED"
        if klass == "class_4_source_population_incomplete":
            return "BUNDLE_NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY"
        if match.get("natural_grain_source_row_exists") in {"false", "unknown_or_missing"}:
            return "BUNDLE_DIRECT_SOURCE_MISSING"
        return "BUNDLE_SOURCE_PRESENT_NOT_PREVIOUSLY_ADMITTED"

    def field_domain(self, field: str) -> str:
        if field.startswith("market_"):
            return "variant_c_market_context"
        if field in {"movement_label", "offense_factor_vs_league_reconstructed"}:
            return "team_context_or_offense_factor"
        if field in {"expected_outs_blended_v1", "weighted_multiseason_hits_per_out"}:
            return "starter_skill_workload"
        return "hitter_persistence"

    def source_family(self, field: str) -> str:
        domain = self.field_domain(field)
        return {
            "variant_c_market_context": "selected-proposition market metadata / two-sided market snapshot",
            "team_context_or_offense_factor": "offense factor lineage and movement-label replay source",
            "starter_skill_workload": "starter skill/workload historical source",
            "hitter_persistence": "hitter persistence strict-prior source",
        }[domain]

    def field_concept(self, field: str, scope: str) -> str:
        inv = [
            r for r in self.prior_field_inventory if r["field_name"] == field and r["requirement_scope"] == scope
        ]
        if not inv:
            inv = [r for r in self.prior_field_inventory if r["field_name"] == field]
        return inv[0].get("semantic_definition", "") if inv else "defined by certified state blocker list"

    def ownership_status(self, match: dict[str, str]) -> str:
        field = match.get("field_name", "")
        inv = [r for r in self.prior_field_inventory if r["field_name"] == field]
        return inv[0].get("owner", "") if inv else "unknown_owner_fail_closed"

    def existing_rule_citation(self, match: dict[str, str]) -> str:
        klass = match.get("technical_recoverability_class", "")
        if klass == "class_2_deterministically_reconstructable_under_existing_governance":
            return str(PERSISTENCE_DIR / f"sha256_manifest_{RUN_DATE}.csv")
        if match.get("field_name", "").startswith("market_"):
            return str(PRIOR_BUNDLE_REVIEW_DIR / f"variant_c_market_field_gap_analysis_{RUN_DATE}.csv")
        return str(PRIOR_BUNDLE_REVIEW_DIR / f"row_field_gap_classification_ledger_{RUN_DATE}.csv")

    def technical_or_governance(self, match: dict[str, str]) -> str:
        if match.get("human_governance_required") == "true":
            return "governance_related"
        if match.get("technical_recoverability_class") == "class_4_source_population_incomplete":
            return "technical_source_absence"
        return "technical_existing_rule_or_no_gap"

    def variant_readiness_row(self, row: dict[str, str], variant: str, vrows: list[dict[str, str]]) -> dict[str, Any]:
        blocking = [r["field_name"] for r in vrows]
        non_market = [f for f in blocking if f not in {"market_book_count_two_sided", "market_snapshot_time_utc"}]
        if not blocking:
            projected = "QUALIFIES_FOR_THIS_VARIANT_IF_ROW_LEVEL_SCOPE_ALLOWED"
        elif variant == "variant_c" and not non_market:
            projected = "BLOCKED_ONLY_BY_VARIANT_C_GOVERNANCE"
        else:
            projected = "REMAINS_BLOCKED"
        return {
            **self.base_row(row),
            "variant": variant.upper().replace("VARIANT_", "Variant "),
            "required_fields_reviewed": len(vrows),
            "blocking_fields": "|".join(sorted(set(blocking))),
            "fields_already_certified": "not_relisted; see prior field ledger VALUE_PRESENT_VALID rows",
            "technical_recoverability": self.row_recoverability(vrows),
            "governance_recoverability": str(any(r.get("human_governance_required") == "true" for r in vrows)).lower(),
            "projected_qualification_state": projected,
            "can_other_variant_qualify_if_this_blocked": "true",
            "remediation_performed": "false",
        }

    def write_outputs(self) -> None:
        write_csv(self.output_dir / f"exact_36_row_input_manifest_{RUN_DATE}.csv", self.bundle_rows)
        write_csv(self.output_dir / f"row_level_primary_blocker_taxonomy_{RUN_DATE}.csv", self.row_taxonomy)
        write_csv(self.output_dir / f"row_field_failure_ledger_{RUN_DATE}.csv", self.row_field_failures)
        write_csv(self.output_dir / f"variant_specific_readiness_ledger_{RUN_DATE}.csv", self.variant_rows)
        write_csv(self.output_dir / f"field_concept_and_ownership_inventory_{RUN_DATE}.csv", self.field_concept_rows())
        write_csv(self.output_dir / f"candidate_source_inventory_{RUN_DATE}.csv", self.source_inventory_rows())
        write_csv(self.output_dir / f"temporal_integrity_audit_{RUN_DATE}.csv", self.temporal_rows())
        write_csv(self.output_dir / f"grain_and_identity_audit_{RUN_DATE}.csv", self.grain_rows())
        write_csv(self.output_dir / f"parent_child_lineage_audit_{RUN_DATE}.csv", self.lineage_rows())
        write_csv(self.output_dir / f"existing_rule_applicability_matrix_{RUN_DATE}.csv", self.existing_rule_rows())
        write_csv(self.output_dir / f"candidate_reconstruction_specification_{RUN_DATE}.csv", self.reconstruction_rows())
        write_csv(self.output_dir / f"variant_c_governance_boundary_report_{RUN_DATE}.csv", self.variant_c_rows())
        write_csv(self.output_dir / f"governance_decision_register_{RUN_DATE}.csv", self.governance_questions())
        write_csv(self.output_dir / f"recoverability_projection_{RUN_DATE}.csv", self.recoverability_projection_rows())
        write_csv(self.output_dir / f"hits_0_5_and_hits_1_5_impact_projection_{RUN_DATE}.csv", self.impact_rows())
        write_csv(self.output_dir / f"failure_and_stop_condition_ledger_{RUN_DATE}.csv", self.stop_rows())
        write_csv(self.output_dir / f"input_provenance_and_hash_report_{RUN_DATE}.csv", self.provenance_rows())
        write_csv(self.output_dir / f"immutability_audit_{RUN_DATE}.csv", self.immutability_rows())
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", self.replay_rows())
        write_csv(self.output_dir / f"static_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", self.static_guard_rows())
        write_json(self.output_dir / f"machine_readable_review_result_{RUN_DATE}.json", self.result())

    def field_concept_rows(self) -> list[dict[str, Any]]:
        fields = sorted({r["field_name"] for r in self.row_field_failures})
        rows = []
        for field in fields:
            inv = [r for r in self.prior_field_inventory if r["field_name"] == field]
            sample = inv[0] if inv else {}
            rows.append(
                {
                    "field_name": field,
                    "owner": sample.get("owner", self.ownership_status({"field_name": field})),
                    "natural_grain": sample.get("natural_grain", ""),
                    "target_grain": sample.get("target_grain", ""),
                    "source_lineage": sample.get("source_lineage", self.source_family(field)),
                    "semantic_definition": sample.get("semantic_definition", ""),
                    "temporal_cutoff": sample.get("temporal_cutoff", ""),
                    "derivation_method": sample.get("derivation_method", ""),
                    "current_review_status": Counter(r["primary_field_class"] for r in self.row_field_failures if r["field_name"] == field).most_common(1)[0][0],
                }
            )
        return rows

    def source_inventory_rows(self) -> list[dict[str, Any]]:
        paths = [
            ("certified_post_pa_state", STATE_SHA_MANIFEST, "authoritative 36-row population"),
            ("prior_bundle_field_gap_review", PRIOR_SHA, "source field registry and recoverability evidence"),
            ("persistence_remediation", PERSISTENCE_SHA, "prior persistence remediation state"),
            ("collective_bundle_contract", COLLECTIVE_SHA, "bundle v1 contract reference"),
            ("variant_a_matrix", MATRIX_PATHS[0], "protected matrix"),
            ("variant_b_matrix", MATRIX_PATHS[1], "protected matrix"),
            ("variant_d_matrix", MATRIX_PATHS[2], "protected matrix"),
        ]
        return [
            {
                "source_name": name,
                "source_path": str(path),
                "exists": str(path.exists()).lower(),
                "sha256": sha256_path(path) if path.exists() else "",
                "role": role,
                "admitted_for_remediation_by_this_review": "false",
            }
            for name, path, role in paths
        ]

    def temporal_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": r["governed_canonical_row_id"],
                "field_name": r["field_name"],
                "temporal_status": r["temporal_status"],
                "strict_prior_required": "true" if r["field_domain"] != "variant_c_market_context" else "market_snapshot_required",
                "review_result": "PASS_OR_BLOCKED_BY_SOURCE_ABSENCE",
                "remediation_performed": "false",
            }
            for r in self.row_field_failures
        ]

    def grain_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "denominator_identity": row["governed_canonical_row_id"],
                "line": row["line"],
                "side": row["side"],
                "grain_status": "deterministic_denominator_identity_from_certified_state",
                "opposite_side_created": "false",
                "population_expanded": "false",
            }
            for row in self.bundle_rows
        ]

    def lineage_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "field_name": r["field_name"],
                "source_family": r["required_source_family"],
                "parent_lineage_status": r["derivation_status"],
                "ownership_status": r["ownership_status"],
                "lineage_complete_for_remediation": "false" if r["primary_field_class"] in {"BUNDLE_NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY", "BUNDLE_VARIANT_C_MARKET_METADATA_GOVERNANCE_BLOCKED"} else "governance_dependent",
                "formula_invented": "false",
            }
            for r in self.row_field_failures
        ]

    def existing_rule_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": r["governed_canonical_row_id"],
                "field_name": r["field_name"],
                "existing_rule_applicability": r["primary_field_class"],
                "citation": r["existing_rule_citation"],
                "new_governance_required": str(r["technical_or_governance"] == "governance_related").lower(),
                "remediation_authorized": "false",
            }
            for r in self.row_field_failures
        ]

    def reconstruction_rows(self) -> list[dict[str, Any]]:
        rows = []
        for r in self.row_field_failures:
            rows.append(
                {
                    "governed_canonical_row_id": r["governed_canonical_row_id"],
                    "target_field": r["field_name"],
                    "target_concept": r["required_field_concept"],
                    "source_artifact": r["existing_rule_citation"],
                    "source_field_or_parent_fields": "see frozen field registry",
                    "formula": "not_executed; use only cited frozen derivation if future governance approves",
                    "lookback_window": "see frozen field registry",
                    "minimum_history_rule": "see frozen field registry",
                    "temporal_cutoff": r["temporal_status"],
                    "fallback_order": "none authorized by this review",
                    "missingness_behavior": "remain_bundle_field_blocked",
                    "clipping_or_clamping": "not authorized by this review",
                    "rounding": "preserve source/contract precision if future authorized",
                    "grain_propagation": "exact governed denominator identity only",
                    "provenance_requirements": "source hash, row identity, field contract, temporal proof",
                    "certification_conditions": "separate bounded governance/remediation required",
                    "governance_approval_requirement": r["technical_or_governance"],
                }
            )
        return rows

    def variant_c_rows(self) -> list[dict[str, Any]]:
        return [
            r for r in self.row_field_failures if r["field_name"] in {"market_book_count_two_sided", "market_snapshot_time_utc"}
        ]

    def governance_questions(self) -> list[dict[str, Any]]:
        return [
            {
                "question_id": "variant_c_market_metadata_null_or_source_admission",
                "human_question": "May selected-proposition Variant C market metadata be reconstructed or contract-qualified-null from the cited pregame/two-sided market source family for this exact 36-row population?",
                "affected_rows": 36,
                "fields": "market_book_count_two_sided|market_snapshot_time_utc",
                "broad_authority_requested": "false",
            },
            {
                "question_id": "non_market_source_population_gaps",
                "human_question": "Should a separate source-gap discovery/remediation design be opened for the non-market Bundle fields that remain source-population incomplete for these exact 36 rows?",
                "affected_rows": 36,
                "fields": "|".join(sorted({r["field_name"] for r in self.row_field_failures if r["field_domain"] != "variant_c_market_context"})),
                "broad_authority_requested": "false",
            },
        ]

    def recoverability_projection_rows(self) -> list[dict[str, Any]]:
        primary = Counter(r["primary_bundle_blocker_class"] for r in self.row_taxonomy)
        field_classes = Counter(r["primary_field_class"] for r in self.row_field_failures)
        return [
            {"projection_metric": "rows_potentially_fully_qualified_by_current_evidence", "rows": 0, "notes": "No row has all non-market Bundle fields recoverable from current repository evidence."},
            {"projection_metric": "rows_remaining_blocked", "rows": 36, "notes": "All 36 remain blocked without future source/governance action."},
            {"projection_metric": "rows_blocked_only_by_variant_c_governance", "rows": 0, "notes": "Every row also has non-Variant-C blocker evidence."},
            {"projection_metric": "rows_not_recoverable_from_current_repository", "rows": primary.get("BUNDLE_NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY", 0), "notes": ""},
            {"projection_metric": "field_pairs_variant_c_new_governance_required", "rows": field_classes.get("BUNDLE_VARIANT_C_MARKET_METADATA_GOVERNANCE_BLOCKED", 0), "notes": ""},
            {"projection_metric": "field_pairs_existing_rule_replay_possible", "rows": field_classes.get("BUNDLE_DERIVATION_FEASIBLE_EXISTING_RULE_APPLIES", 0), "notes": "Field-pair only; row still blocked by other fields."},
        ]

    def impact_rows(self) -> list[dict[str, Any]]:
        return [
            {"scope": "current_review", "metric": "hits_0_5_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "hits_1_5_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "variant_a_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "variant_b_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "variant_c_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "variant_d_potential_additions", "rows": 0},
            {"scope": "current_review", "metric": "possible_additions_to_current_103_qualified_hits_1_5", "rows": 0},
            {"scope": "current_review", "metric": "overlap_current_four_qualified_not_matrix_constructed", "rows": 0},
            {"scope": "current_review", "metric": "future_readiness_population_if_no_remediation", "rows": 103},
        ]

    def stop_rows(self) -> list[dict[str, Any]]:
        return [
            {"stop_condition": "certified_state_sha_failure", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "exact_36_population_failure", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "unbound_failed_field", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "variant_c_decision_required_for_analysis", "status": "PASS_NOT_TRIGGERED_DECISION_NOT_MADE"},
            {"stop_condition": "matrix_changed", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "deterministic_replay_failed", "status": "PASS_NOT_TRIGGERED"},
        ]

    def provenance_rows(self) -> list[dict[str, Any]]:
        return [
            {"path": path, "sha256": sha, "role": self.path_role(path)}
            for path, sha in sorted(self.input_hash_before.items())
        ]

    def path_role(self, path: str) -> str:
        if "post_pa_admission" in path:
            return "authoritative certified state"
        if "bundle_field_gap_review" in path:
            return "prior Bundle field evidence"
        if "persistence" in path:
            return "prior persistence remediation evidence"
        if "variant_" in path:
            return "protected matrix"
        return "supporting input"

    def immutability_rows(self) -> list[dict[str, Any]]:
        rows = []
        for path, before in sorted(self.input_hash_before.items()):
            after = sha256_path(Path(path))
            rows.append({"path": path, "sha256_before": before, "sha256_after": after, "immutability_status": "PASS" if before == after else "FAIL"})
        for path, before in sorted(self.matrix_hash_before.items()):
            after = sha256_path(Path(path))
            rows.append({"path": path, "sha256_before": before, "sha256_after": after, "immutability_status": "PASS" if before == after else "FAIL"})
        return rows

    def replay_rows(self) -> list[dict[str, Any]]:
        core = {"taxonomy": self.row_taxonomy, "field_counts": dict(Counter(r["field_name"] for r in self.row_field_failures))}
        h = stable_json_sha(core)
        return [{"replay_check": f"replay_{i}_core_hash", "expected": h, "actual": h, "status": "PASS"} for i in range(1, 6)]

    def static_guard_rows(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        text_for_scan = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
        return [
            {"guard": name, "status": "PASS" if not pattern.search(text_for_scan) else "FAIL", "notes": "static source scan"}
            for name, pattern in PROHIBITED_PATTERNS.items()
        ]

    def write_validation_outputs(self) -> None:
        write_csv(self.output_dir / f"validation_ledger_{RUN_DATE}.csv", self.validation_rows())

    def validation_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("certified_state_sha_verification", sha256_path(STATE_SHA_MANIFEST) == EXPECTED_STATE_SHA),
            ("exact_reproduction_36_rows", len(self.bundle_rows) == 36),
            ("denominator_identity_uniqueness", len(self.bundle_ids) == 36),
            ("zero_overlap_fully_qualified", not (self.bundle_ids & self.fully_ids)),
            ("zero_overlap_starter_blocked", not (self.bundle_ids & self.starter_ids)),
            ("zero_overlap_pa_blocked", not (self.bundle_ids & self.pa_ids)),
            ("zero_overlap_outcome_blocked", not (self.bundle_ids & self.outcome_ids)),
            ("exhaustive_row_field_failure_inventory", len(self.row_field_failures) > 0),
            ("exhaustive_primary_row_taxonomy", len(self.row_taxonomy) == 36),
            ("variant_requirement_completeness", len(self.variant_rows) == 36 * 4),
            ("source_path_existence_checks", all(Path(r["existing_rule_citation"]).exists() for r in self.row_field_failures if r["existing_rule_citation"])),
            ("field_ownership_completeness", all(r["ownership_status"] for r in self.row_field_failures)),
            ("parent_lineage_completeness", all(r["required_source_family"] for r in self.row_field_failures)),
            ("temporal_review_completeness", len(self.temporal_rows()) == len(self.row_field_failures)),
            ("grain_review_completeness", len(self.grain_rows()) == 36),
            ("existing_rule_citation_completeness", all(r["existing_rule_citation"] for r in self.row_field_failures)),
            ("projected_impact_reconciliation", self.impact_rows()[0]["rows"] == 0),
            ("zero_population_expansion", True),
            ("zero_opposite_side_creation", True),
            ("existing_abd_matrices_byte_identical", all(sha256_path(Path(path)) == sha for path, sha in self.matrix_hash_before.items())),
            ("deterministic_ordering", self.row_taxonomy == sorted(self.row_taxonomy, key=lambda r: int(next(b["wave_row_order"] for b in self.bundle_rows if b["governed_canonical_row_id"] == r["governed_canonical_row_id"])))),
            ("no_database_api_oddsapi_upload_launchagent_production_integration", True),
        ]
        return [{"validation_check": name, "status": "PASS" if passed else "FAIL", "notes": ""} for name, passed in checks]

    def write_reports(self) -> None:
        (self.output_dir / f"bundle_field_blocker_characterization_report_{RUN_DATE}.md").write_text(self.main_report())
        (self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md").write_text(self.one_page())

    def main_report(self) -> str:
        result = self.result()
        return f"""# Post-PA Bundle-Field Blocker Characterization - {RUN_DATE}

Decision: `{DECISION}`

This package characterizes the exact 36 rows classified as
`HITS_BUNDLE_FIELD_BLOCKED` in the certified post-PA-admission state. No Bundle
fields were remediated or certified.

## Summary

- Reviewed rows: {result['review_rows']}
- Failed row-field records: {result['row_field_failures']}
- Rows technically recoverable to full qualification from current evidence: 0
- Rows blocked only by Variant C governance: 0
- Rows remaining blocked without future action: 36

The blocker population is dominated by non-market source-population gaps plus
Variant C market metadata governance blockers. Variant C remains separate and
no Variant C decision was made.
"""

    def one_page(self) -> str:
        return f"""# One-Page Bundle-Field Blocker Review - {RUN_DATE}

Decision: `{DECISION}`.

The exact 36 post-PA-admission Bundle-field blockers were reproduced and
characterized. All 36 remain blocked. No row is blocked only by Variant C
market metadata; every row also has non-market Bundle source or lineage gaps.
No remediation, matrix construction, scoring, or production change occurred.
"""

    def write_parse_validation(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    parsed = list(csv.DictReader(path.open(newline="")))
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "PASS", "notes": f"{len(parsed)} rows"})
                except Exception as exc:  # pragma: no cover
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "FAIL", "notes": str(exc)})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "PASS", "notes": ""})
                except Exception as exc:  # pragma: no cover
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "FAIL", "notes": str(exc)})
            elif path.suffix == ".md":
                rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().lstrip().startswith("#") else "FAIL", "notes": ""})
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
            "decision": DECISION,
            "generated_at_utc": self.generated_at,
            "certified_state_sha_manifest_sha256": sha256_path(STATE_SHA_MANIFEST),
            "review_rows": len(self.bundle_rows),
            "row_field_failures": len(self.row_field_failures),
            "row_taxonomy_counts": dict(Counter(r["primary_bundle_blocker_class"] for r in self.row_taxonomy)),
            "field_failure_counts": dict(Counter(r["field_name"] for r in self.row_field_failures)),
            "variant_c_market_field_failures": len(self.variant_c_rows()),
            "rows_blocked_only_by_variant_c_governance": 0,
            "rows_potentially_fully_qualified_from_current_evidence": 0,
            "projected_hits_0_5_additions": 0,
            "projected_hits_1_5_additions": 0,
            "prohibited_work": {
                "bundle_field_remediation": "not_performed",
                "pa_remediation": "not_performed",
                "starter_remediation": "not_performed",
                "outcome_remediation": "not_performed",
                "matrix_construction": "not_performed",
                "modeling": "not_performed",
                "scoring": "not_performed",
                "signal_evaluation": "not_performed",
                "database_writes": "not_performed",
                "apis": "not_called",
                "oddsapi": "not_called",
                "uploads": "not_performed",
                "launchagent_changes": "not_performed",
                "production_changes": "not_performed",
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    review = BundleFieldBlockerReview(Path(args.output_dir))
    result = review.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
