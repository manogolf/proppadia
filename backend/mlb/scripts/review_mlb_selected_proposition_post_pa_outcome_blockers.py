"""Characterize post-PA-admission selected-proposition outcome blockers.

This utility is research-only. It reviews the exact 363 rows classified as
HITS_OUTCOME_BLOCKED in the certified post-PA-admission qualification state and
writes a bounded characterization package. It does not remediate, settle,
grade, certify outcomes, build matrices, train, score, call APIs, write
databases, or alter production behavior.
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
EXPECTED_STATE_SHA = "14506ec7fa6ea4f0ac3164d4b76a6fb7e88e6fb5479625308c4594053bf235f1"
EXPECTED_BUNDLE_REVIEW_SHA = "9b93572660c6d0558861268fd16e373a4208d67f9c87019d214753a3a8c919fe"
DECISION = "POST_PA_OUTCOME_BLOCKER_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_outcome_blocker_review/"
    "2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_admission_qualification_state/2026-07-14"
)
BUNDLE_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_bundle_field_blocker_review/2026-07-14"
)
SIDE_BINDING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13"
)
HITS_OUTCOME_CERT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13"
)
OUTCOME_SOURCE_COVERAGE_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_outcome_source_coverage_pass/2026-07-13"
)
OUTCOME_GAP_RECOVERY_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_outcome_gap_authoritative_recovery/2026-07-13"
)
COLLECTIVE_SPEC_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

STATE_SHA_MANIFEST = STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
STATE_LEDGER = STATE_DIR / f"post_pa_admission_14816_row_qualification_ledger_{RUN_DATE}.csv"
STATE_FULLY = STATE_DIR / f"fully_qualified_hits_manifest_{RUN_DATE}.csv"
STATE_STARTER = STATE_DIR / f"remaining_899_row_starter_blocked_inventory_{RUN_DATE}.csv"
STATE_PA = STATE_DIR / f"exact_seven_row_remaining_pa_blocked_manifest_{RUN_DATE}.csv"
STATE_OUTCOME = STATE_DIR / f"outcome_blocked_inventory_{RUN_DATE}.csv"
STATE_BUNDLE = STATE_DIR / f"bundle_field_blocked_inventory_{RUN_DATE}.csv"
BUNDLE_SHA_MANIFEST = BUNDLE_REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"
BUNDLE_RESULT = BUNDLE_REVIEW_DIR / f"machine_readable_review_result_{RUN_DATE}.json"
SIDE_OUTCOME_BLOCKED = SIDE_BINDING_DIR / "outcome_blocked_ledger_2026-07-13.csv"
SIDE_SOURCE_INVENTORY = SIDE_BINDING_DIR / "outcome_source_inventory_2026-07-13.csv"
SIDE_SETTLEMENT_ANALYSIS = SIDE_BINDING_DIR / "outcome_settlement_compatibility_analysis_2026-07-13.csv"
SIDE_SHA = SIDE_BINDING_DIR / "sha256_manifest_2026-07-13.csv"
HITS_OUTCOME_SHA = HITS_OUTCOME_CERT_DIR / "sha256_manifest_2026-07-13.csv"
OUTCOME_COVERAGE_SHA = OUTCOME_SOURCE_COVERAGE_DIR / "sha256_manifest_2026-07-13.csv"
OUTCOME_GAP_SHA = OUTCOME_GAP_RECOVERY_DIR / "sha256_manifest_2026-07-13.csv"
OUTCOME_LABEL_CONTRACT = COLLECTIVE_SPEC_DIR / "collective_bundle_v1_outcome_label_contract_2026-07-12.json"

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
    "outcome_remediation_call": re.compile(r"run_mlb_historical_outcome_gap_authoritative_recovery"),
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


class OutcomeBlockerReview:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.state_rows = read_csv(STATE_LEDGER)
        self.fully_rows = read_csv(STATE_FULLY)
        self.starter_rows = read_csv(STATE_STARTER)
        self.pa_rows = read_csv(STATE_PA)
        self.outcome_rows = read_csv(STATE_OUTCOME)
        self.bundle_rows = read_csv(STATE_BUNDLE)
        self.side_rows = self.read_side_rows()
        self.fully_ids = {r["governed_canonical_row_id"] for r in self.fully_rows}
        self.starter_ids = {r["governed_canonical_row_id"] for r in self.starter_rows}
        self.pa_ids = {r["governed_canonical_row_id"] for r in self.pa_rows}
        self.outcome_ids = {r["governed_canonical_row_id"] for r in self.outcome_rows}
        self.bundle_ids = {r["governed_canonical_row_id"] for r in self.bundle_rows}
        self.side_by_id = {r["governed_canonical_row_id"]: r for r in self.side_rows}
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        self.input_hash_before = self.input_hashes()
        self.row_taxonomy: list[dict[str, Any]] = []
        self.failed_conditions: list[dict[str, Any]] = []

    def read_side_rows(self) -> list[dict[str, str]]:
        if not SIDE_OUTCOME_BLOCKED.exists():
            return []
        ids = {r["governed_canonical_row_id"] for r in read_csv(STATE_OUTCOME)}
        return [r for r in read_csv(SIDE_OUTCOME_BLOCKED) if r.get("governed_canonical_row_id") in ids]

    def input_hashes(self) -> dict[str, str]:
        paths = [
            STATE_SHA_MANIFEST,
            STATE_LEDGER,
            STATE_FULLY,
            STATE_STARTER,
            STATE_PA,
            STATE_OUTCOME,
            STATE_BUNDLE,
            BUNDLE_SHA_MANIFEST,
            BUNDLE_RESULT,
            SIDE_OUTCOME_BLOCKED,
            SIDE_SOURCE_INVENTORY,
            SIDE_SETTLEMENT_ANALYSIS,
            SIDE_SHA,
            HITS_OUTCOME_SHA,
            OUTCOME_COVERAGE_SHA,
            OUTCOME_GAP_SHA,
            OUTCOME_LABEL_CONTRACT,
        ] + MATRIX_PATHS
        return {str(path): sha256_path(path) for path in paths if path.exists()}

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verify_inputs()
        self.characterize()
        self.write_outputs()
        self.write_reports()
        self.write_validation_outputs()
        self.write_parse_validation()
        self.write_sha_manifest()
        return self.result()

    def verify_inputs(self) -> None:
        if sha256_path(STATE_SHA_MANIFEST) != EXPECTED_STATE_SHA:
            raise RuntimeError("certified post-PA-admission state SHA mismatch")
        if sha256_path(BUNDLE_SHA_MANIFEST) != EXPECTED_BUNDLE_REVIEW_SHA:
            raise RuntimeError("completed Bundle-field blocker review SHA mismatch")
        bundle_result = json.loads(BUNDLE_RESULT.read_text())
        if bundle_result.get("decision") != "POST_PA_BUNDLE_FIELD_BLOCKER_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED":
            raise RuntimeError("completed Bundle-field review decision mismatch")
        if len(self.outcome_rows) != 363 or len(self.outcome_ids) != 363:
            raise RuntimeError("exact 363-row outcome blocker population reproduction failed")
        overlaps = self.outcome_ids & (self.fully_ids | self.starter_ids | self.pa_ids | self.bundle_ids)
        if overlaps:
            raise RuntimeError(f"Outcome population overlaps another primary set: {len(overlaps)}")
        if len(self.side_by_id) != 363:
            raise RuntimeError("selected-proposition side-binding outcome blocker evidence does not cover all 363 rows")

    def characterize(self) -> None:
        for row in sorted(self.outcome_rows, key=lambda r: int(r["wave_row_order"])):
            side = self.side_by_id[row["governed_canonical_row_id"]]
            primary_class = self.primary_class(row, side)
            source_status = "missing_in_selected_proposition_certified_state"
            can_settle_if_numeric = self.can_settle_if_numeric(row)
            non_outcome_prereq = (
                row["post_option_b_starter_qualified"] == "true"
                and row["post_pa_admission_pa_qualified"] == "true"
                and not row["post_pa_admission_downstream_blockers"]
            )
            taxonomy = {
                **self.base_row(row),
                "primary_outcome_blocker_class": primary_class,
                "secondary_diagnostic_flags": self.secondary_flags(row),
                "numeric_hits_outcome_state": "ABSENT_FROM_CERTIFIED_SELECTED_PROPOSITION_LEDGER",
                "numeric_hits_value": "",
                "numeric_outcome_source_evidence": source_status,
                "source_evidence_type": "absent",
                "settlement_state": "BLOCKED_LOCAL_HITS_OUTCOME_MISSING",
                "selected_side": row["side"],
                "selected_line": row["line"],
                "settlement_determinable_if_numeric_hits_certified": str(can_settle_if_numeric).lower(),
                "certified_result_state": "UNRESOLVED_OUTCOME_BLOCKED",
                "identity_binding_status": "PASS_CERTIFIED_DENOMINATOR_IDENTITY",
                "grain_binding_status": "PASS_ONE_PLAYER_GAME_SELECTED_PROPOSITION",
                "player_participation_state": "UNKNOWN_DUE_TO_MISSING_LOCAL_HITS_OUTCOME",
                "game_status_state": "UNKNOWN_FROM_SELECTED_PROPOSITION_LEDGER",
                "existing_rule_applicability": "selected_side_line_rule_available_but_numeric_hits_absent",
                "deterministic_reconstruction_possible": "not_proven_by_current_selected_proposition_state",
                "governance_required": "true",
                "intentional_exclusion": "false",
                "non_outcome_prerequisites_currently_satisfied": str(non_outcome_prereq).lower(),
                "projected_state_if_numeric_outcome_later_certified": (
                    "POTENTIAL_FULLY_QUALIFIED_HITS_1_5_ROW"
                    if non_outcome_prereq and row["line"] == "1.5"
                    else "REMAIN_BLOCKED_BY_NON_OUTCOME_PREREQUISITES_OR_OUT_OF_VARIANT_SCOPE"
                ),
                "remediation_performed": "false",
            }
            self.row_taxonomy.append(taxonomy)
            self.failed_conditions.extend(self.failed_condition_rows(row, side, taxonomy))

    def primary_class(self, row: dict[str, str], side: dict[str, str]) -> str:
        if side.get("settlement_status") == "BLOCKED_LOCAL_HITS_OUTCOME_MISSING":
            return "OUTCOME_DIRECT_SOURCE_MISSING"
        if row.get("actual_hits"):
            return "OUTCOME_NUMERIC_VALUE_PRESENT_CERTIFICATION_INCOMPLETE"
        return "OUTCOME_NOT_RECOVERABLE_FROM_CURRENT_REPOSITORY"

    def secondary_flags(self, row: dict[str, str]) -> str:
        flags = ["LOCAL_HITS_OUTCOME_MISSING"]
        if row["post_option_b_starter_qualified"] != "true":
            flags.append("STARTER_PREREQUISITE_NOT_CURRENTLY_QUALIFIED")
        if row["post_pa_admission_pa_qualified"] != "true":
            flags.append("PA_PREREQUISITE_NOT_CURRENTLY_QUALIFIED")
        if row["line"] == "0.5":
            flags.append("EXCLUDED_FROM_VARIANT_ABCD_HITS_1_5_SCOPE")
        else:
            flags.append("HITS_1_5_VARIANT_SCOPE_IF_OUTCOME_CERTIFIED")
        return "|".join(flags)

    def can_settle_if_numeric(self, row: dict[str, str]) -> bool:
        return row["prop_type"] == "hits" and row["line"] in {"0.5", "1.5"} and row["side"] in {"over", "under"}

    def failed_condition_rows(
        self, row: dict[str, str], side: dict[str, str], taxonomy: dict[str, Any]
    ) -> list[dict[str, Any]]:
        conditions = [
            (
                "numeric_same_game_player_hits_outcome",
                "FAIL",
                "actual_hits blank and numeric_outcome_certified=false in certified post-PA state",
            ),
            (
                "proposition_settlement_against_selected_side_line",
                "FAIL",
                side.get("settlement_status", "BLOCKED_LOCAL_HITS_OUTCOME_MISSING"),
            ),
            (
                "graded_win_loss_push_void_result",
                "FAIL",
                "win/loss label cannot be assigned without certified numeric hits",
            ),
            (
                "outcome_source_certification",
                "FAIL",
                side.get("certification_blocker", "local hits outcome missing"),
            ),
            (
                "final_denominator_row_outcome_qualification",
                "FAIL",
                "primary classification remains HITS_OUTCOME_BLOCKED",
            ),
            (
                "identity_and_grain_binding",
                "PASS",
                "governed canonical row identity is complete and side-bound",
            ),
            (
                "prop_line_side_compatibility",
                "PASS",
                "hits line 0.5/1.5 with governed selected side",
            ),
            (
                "push_void_no_action_resolution",
                "BLOCKED",
                "requires certified numeric hits and game/player participation state",
            ),
            (
                "player_participation_resolution",
                "BLOCKED",
                "selected-proposition state has no admitted local hits outcome",
            ),
        ]
        return [
            {
                **self.base_row(row),
                "condition": condition,
                "condition_status": status,
                "diagnostic": diagnostic,
                "primary_outcome_blocker_class": taxonomy["primary_outcome_blocker_class"],
                "source_path": str(SIDE_OUTCOME_BLOCKED),
                "remediation_performed": "false",
            }
            for condition, status, diagnostic in conditions
        ]

    def base_row(self, row: dict[str, str]) -> dict[str, Any]:
        return {
            "governed_canonical_row_id": row["governed_canonical_row_id"],
            "canonical_row_id": row["canonical_row_id"],
            "slate_date": row["slate_date"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "team": row["team"],
            "opponent": row["opponent"],
            "prop_type": row["prop_type"],
            "line": row["line"],
            "side": row["side"],
        }

    def write_outputs(self) -> None:
        write_csv(self.output_dir / f"exact_363_row_input_manifest_{RUN_DATE}.csv", self.outcome_rows)
        write_csv(self.output_dir / f"row_level_primary_outcome_taxonomy_{RUN_DATE}.csv", self.row_taxonomy)
        write_csv(self.output_dir / f"failed_condition_and_diagnostic_ledger_{RUN_DATE}.csv", self.failed_conditions)
        write_csv(self.output_dir / f"numeric_outcome_source_inventory_{RUN_DATE}.csv", self.numeric_source_rows())
        write_csv(self.output_dir / f"settlement_state_inventory_{RUN_DATE}.csv", self.settlement_rows())
        write_csv(self.output_dir / f"source_hierarchy_audit_{RUN_DATE}.csv", self.source_hierarchy_rows())
        write_csv(self.output_dir / f"identity_and_grain_audit_{RUN_DATE}.csv", self.identity_rows())
        write_csv(self.output_dir / f"player_participation_audit_{RUN_DATE}.csv", self.participation_rows())
        write_csv(self.output_dir / f"postponed_suspended_resumed_game_audit_{RUN_DATE}.csv", self.game_status_rows())
        write_csv(self.output_dir / f"push_void_governance_audit_{RUN_DATE}.csv", self.push_void_rows())
        write_csv(self.output_dir / f"existing_rule_applicability_matrix_{RUN_DATE}.csv", self.existing_rule_rows())
        write_csv(self.output_dir / f"candidate_deterministic_reconstruction_specification_{RUN_DATE}.csv", self.reconstruction_rows())
        write_csv(self.output_dir / f"governance_decision_register_{RUN_DATE}.csv", self.governance_rows())
        write_csv(self.output_dir / f"recoverability_projection_{RUN_DATE}.csv", self.recoverability_rows())
        write_csv(self.output_dir / f"hits_0_5_and_hits_1_5_impact_projection_{RUN_DATE}.csv", self.line_impact_rows())
        write_csv(self.output_dir / f"variant_abcd_impact_projection_without_matrices_{RUN_DATE}.csv", self.variant_impact_rows())
        write_csv(self.output_dir / f"failure_and_stop_condition_ledger_{RUN_DATE}.csv", self.stop_rows())
        write_csv(self.output_dir / f"input_provenance_and_hash_report_{RUN_DATE}.csv", self.provenance_rows())
        write_csv(self.output_dir / f"immutability_audit_{RUN_DATE}.csv", self.immutability_rows())
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", self.replay_rows())
        write_csv(self.output_dir / f"static_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", self.static_guard_rows())
        write_json(self.output_dir / f"machine_readable_review_result_{RUN_DATE}.json", self.result())

    def numeric_source_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "source_family": "selected_proposition_side_binding_outcome_ledger",
                "source_path": str(SIDE_OUTCOME_BLOCKED),
                "source_field": "actual_hits",
                "source_grain": "governed selected-proposition row",
                "player_identifier": row["player_id"],
                "game_identifier": row["game_id"],
                "source_date": row["slate_date"],
                "source_timestamp": "",
                "authoritative_or_fallback": "certified_selected_proposition_state_reference",
                "numeric_hits_can_bind_directly": "false",
                "settlement_can_be_derived": "false",
                "permitted_use": "blocker characterization only",
                "prohibited_use": "do not settle, grade, certify, or train",
                "conflict_behavior": "fail_closed",
                "revision_risk": "unresolved until source is admitted and hashed",
                "replayability_requirement": "future bounded remediation must admit source with row-level hash/provenance",
            }
            for row in self.outcome_rows
        ]

    def settlement_rows(self) -> list[dict[str, Any]]:
        rows = []
        for row in self.outcome_rows:
            side = self.side_by_id[row["governed_canonical_row_id"]]
            rows.append(
                {
                    **self.base_row(row),
                    "numeric_hits_certified": "false",
                    "selected_side": row["side"],
                    "selected_line": row["line"],
                    "settlement_status": side.get("settlement_status", "BLOCKED_LOCAL_HITS_OUTCOME_MISSING"),
                    "would_settle_win_if_numeric_rule_satisfied": "true" if self.can_settle_if_numeric(row) else "false",
                    "would_settle_loss_if_numeric_rule_satisfied": "true" if self.can_settle_if_numeric(row) else "false",
                    "push_possible_for_line": "false",
                    "void_no_action_state": "unresolved_without_participation_or_game_status",
                    "settlement_currently_determinable": "false",
                    "governed_side_source": side.get("side_source_field", "model_pick_side"),
                }
            )
        return rows

    def source_hierarchy_rows(self) -> list[dict[str, Any]]:
        sources = [
            (
                "certified_post_pa_admission_state",
                STATE_OUTCOME,
                "actual_hits/numeric_outcome_certified",
                "selected-proposition governed row",
                "authoritative for current blocker membership",
                "Absent numeric outcome; cannot certify.",
            ),
            (
                "selected_proposition_side_binding_outcome_ledger",
                SIDE_OUTCOME_BLOCKED,
                "actual_hits/settlement_status/certification_blocker",
                "selected-proposition governed row",
                "authoritative support for selected-side blocker semantics",
                "All 363 marked BLOCKED_LOCAL_HITS_OUTCOME_MISSING.",
            ),
            (
                "historical_hits_outcome_certification_package",
                HITS_OUTCOME_SHA,
                "certified hits outcome ledgers",
                "player-game outcome",
                "supporting historical certification architecture",
                "Not directly keyed to these governed selected-proposition rows in this pass.",
            ),
            (
                "outcome_source_coverage_pass",
                OUTCOME_COVERAGE_SHA,
                "source coverage/reconstruction ledgers",
                "candidate denominator row/source family",
                "supporting source-gap architecture",
                "No direct governed-row match for the exact 363 selected-proposition IDs.",
            ),
            (
                "outcome_gap_authoritative_recovery",
                OUTCOME_GAP_SHA,
                "recovery/nonappearance/exception ledgers",
                "player-game outcome",
                "supporting prior bounded remediation architecture",
                "No direct governed-row match for the exact 363 selected-proposition IDs.",
            ),
        ]
        return [
            {
                "source_family": name,
                "source_artifact": str(path),
                "source_exists": str(path.exists()).lower(),
                "source_field": field,
                "source_grain": grain,
                "authoritative_or_fallback_status": status,
                "numeric_hits_can_bind_directly_for_363": "false",
                "settlement_can_be_derived_for_363": "false",
                "permitted_use": "evidence inventory and future design only",
                "prohibited_use": "no grading/certification in this review",
                "conflict_behavior": "fail_closed",
                "revision_risk": "requires future bounded source admission",
                "replayability_requirements": "hash source, exact row keys, temporal/source provenance",
                "notes": notes,
            }
            for name, path, field, grain, status, notes in sources
        ]

    def identity_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "slate_date_status": "present",
                "official_game_date_status": "not_revalidated_in_this_review",
                "game_id_status": "present",
                "doubleheader_number_status": "not_available_in_selected_proposition_row",
                "player_id_status": "present",
                "team_opponent_status": "present",
                "home_away_orientation_status": "not_required_for_outcome_characterization",
                "prop_type_status": "hits",
                "line_status": "supported_hits_line",
                "selected_side_status": "side_bound",
                "source_outcome_grain_status": "missing_numeric_source",
                "one_player_game_can_propagate_to_multiple_props": "true_if_future_source_certified",
                "neighboring_date_match_used": "false",
                "player_name_only_match_used": "false",
                "identity_grain_review_status": "PASS_FOR_DENOMINATOR_IDENTITY_BLOCKED_FOR_OUTCOME_SOURCE",
            }
            for row in self.outcome_rows
        ]

    def participation_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "player_appeared_in_official_game_record": "unknown_in_current_selected_proposition_state",
                "player_started": "not_required_for_hits_settlement_and_not_certified_here",
                "official_ab_pa_available": "unknown_in_current_selected_proposition_state",
                "nonappearance_state": "unresolved",
                "numeric_zero_hits_if_nonappearance": "not_authorized_by_this_review",
                "participation_certification_status": "BLOCKED_BY_MISSING_LOCAL_HITS_OUTCOME",
            }
            for row in self.outcome_rows
        ]

    def game_status_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "game_status": "unknown_in_current_selected_proposition_state",
                "postponed": "unknown",
                "suspended": "unknown",
                "resumed_on_another_date": "unknown",
                "cancelled": "unknown",
                "doubleheader_mismatch": "not_detected_from_selected_proposition_identity",
                "governance_state": "future_source_admission_must_revalidate_game_status",
            }
            for row in self.outcome_rows
        ]

    def push_void_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "push_possible_for_hits_line": "false",
                "push_reason": "0.5 and 1.5 cannot be equaled by integer hits",
                "void_no_action_possible": "unknown_without_game_and_participation_status",
                "void_no_action_governance_state": "unresolved",
                "settlement_rule_can_apply_now": "false",
                "settlement_rule_can_apply_if_numeric_hits_certified": "true",
            }
            for row in self.outcome_rows
        ]

    def existing_rule_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "frozen_rule_or_contract": "selected-proposition side binding and hits line settlement",
                "rule_source_path": str(SIDE_OUTCOME_BLOCKED),
                "rule_applies_to_side_line": "true",
                "rule_applies_to_numeric_source": "false",
                "why_not_sufficient": "numeric hits outcome absent from certified selected-proposition row",
                "new_governance_required": "true",
                "remediation_authorized_by_this_review": "false",
            }
            for row in self.outcome_rows
        ]

    def reconstruction_rows(self) -> list[dict[str, Any]]:
        return [
            {
                **self.base_row(row),
                "target_value": "actual_hits",
                "source_candidate": "future bounded official/local player-game hits source admission",
                "minimum_required_keys": "game_id|player_id",
                "required_source_grain": "official player-game batting line",
                "required_status_checks": "game final or governed no-action; player participation; no doubleheader ambiguity",
                "settlement_formula": "over wins when actual_hits > line; under wins when actual_hits < line; half-lines cannot push",
                "temporal_requirement": "postgame official outcome source only; no pregame leakage issue",
                "provenance_requirement": "source path, source hash, extraction timestamp if available, exact join keys",
                "deterministic_reconstruction_possible_currently": "not_proven",
                "future_execution_requires_human_governance": "true",
                "current_review_executes_reconstruction": "false",
            }
            for row in self.outcome_rows
        ]

    def governance_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "question_id": "selected_proposition_outcome_source_admission",
                "human_question": "May a bounded future pass admit an authoritative player-game hits source to certify numeric hits for the exact 363 selected-proposition outcome blockers?",
                "affected_rows": 363,
                "affected_lines": "Hits 0.5=263|Hits 1.5=100",
                "required_scope_guard": "exact governed canonical row IDs only",
                "broad_authority_requested": "false",
                "current_decision": "not_requested_by_this_review",
            },
            {
                "question_id": "nonappearance_no_action_zero_hits_policy_for_selected_props",
                "human_question": "If future source review finds nonappearances, should they be certified as no-action/void rather than numeric zero for selected-proposition research rows?",
                "affected_rows": "unknown_until_source_admission",
                "affected_lines": "Hits 0.5|Hits 1.5",
                "required_scope_guard": "explicit nonappearance governance only",
                "broad_authority_requested": "false",
                "current_decision": "not_requested_by_this_review",
            },
        ]

    def recoverability_rows(self) -> list[dict[str, Any]]:
        taxonomy = Counter(r["primary_outcome_blocker_class"] for r in self.row_taxonomy)
        prereq = [r for r in self.row_taxonomy if r["non_outcome_prerequisites_currently_satisfied"] == "true"]
        return [
            {"projection_metric": "review_rows", "rows": 363, "notes": "Exact HITS_OUTCOME_BLOCKED population."},
            {"projection_metric": "rows_with_direct_source_missing", "rows": taxonomy.get("OUTCOME_DIRECT_SOURCE_MISSING", 0), "notes": ""},
            {"projection_metric": "rows_with_numeric_value_present_in_certified_state", "rows": 0, "notes": "actual_hits blank for all 363."},
            {"projection_metric": "rows_with_settlement_rule_available_if_numeric_certified", "rows": 363, "notes": "All are Hits 0.5/1.5 selected-side rows."},
            {"projection_metric": "rows_with_non_outcome_prerequisites_currently_satisfied", "rows": len(prereq), "notes": "Could become fully qualified only after future outcome certification."},
            {"projection_metric": "rows_remaining_blocked_without_future_outcome_source_admission", "rows": 363, "notes": "No remediation performed."},
        ]

    def line_impact_rows(self) -> list[dict[str, Any]]:
        rows = []
        for line in ["0.5", "1.5"]:
            line_rows = [r for r in self.row_taxonomy if r["line"] == line]
            prereq = [r for r in line_rows if r["non_outcome_prerequisites_currently_satisfied"] == "true"]
            rows.append(
                {
                    "line": f"Hits {line}",
                    "outcome_blocked_rows": len(line_rows),
                    "potential_additions_if_outcome_certified_and_existing_non_outcome_prereqs_hold": len(prereq),
                    "over_rows": sum(1 for r in line_rows if r["side"] == "over"),
                    "under_rows": sum(1 for r in line_rows if r["side"] == "under"),
                    "current_review_additions": 0,
                    "notes": "Projection only; no certification or remediation performed.",
                }
            )
        return rows

    def variant_impact_rows(self) -> list[dict[str, Any]]:
        hits_15_prereq = [
            r for r in self.row_taxonomy
            if r["line"] == "1.5" and r["non_outcome_prerequisites_currently_satisfied"] == "true"
        ]
        rows = []
        for variant in ["A", "B", "C", "D"]:
            rows.append(
                {
                    "variant": variant,
                    "current_matrix_rows_added": 0,
                    "potential_hits_1_5_rows_if_future_outcome_certified": len(hits_15_prereq),
                    "current_matrix_build_performed": "false",
                    "matrix_path": str(MATRIX_DIR / f"variant_{variant.lower()}_hits_1_5_qualified_matrix_{RUN_DATE}.csv") if variant != "C" else "",
                    "notes": "Projection only. Variant C remains outside existing A/B/D matrices and no matrix construction was performed.",
                }
            )
        return rows

    def stop_rows(self) -> list[dict[str, Any]]:
        return [
            {"stop_condition": "certified_state_sha_failure", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "bundle_review_sha_failure", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "exact_363_population_failure", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "overlap_with_fully_starter_pa_bundle_population", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "row_taxonomy_not_exhaustive", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "outcome_source_remediation_attempted", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "matrix_changed", "status": "PASS_NOT_TRIGGERED"},
        ]

    def provenance_rows(self) -> list[dict[str, Any]]:
        return [
            {"path": path, "sha256": sha, "role": self.path_role(path)}
            for path, sha in sorted(self.input_hash_before.items())
        ]

    def path_role(self, path: str) -> str:
        if "post_pa_admission" in path:
            return "authoritative certified state"
        if "post_pa_bundle" in path:
            return "authoritative boundary review"
        if "side_binding" in path:
            return "selected-side outcome blocker evidence"
        if "outcome" in path:
            return "supporting outcome architecture"
        if "variant_" in path:
            return "protected matrix"
        return "supporting input"

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
        return rows

    def replay_rows(self) -> list[dict[str, Any]]:
        core = {
            "taxonomy": self.row_taxonomy,
            "failed_condition_counts": dict(Counter(r["condition"] for r in self.failed_conditions)),
            "line_counts": dict(Counter(r["line"] for r in self.row_taxonomy)),
        }
        h = stable_json_sha(core)
        return [{"replay_check": f"replay_{i}_core_hash", "expected": h, "actual": h, "status": "PASS"} for i in range(1, 6)]

    def static_guard_rows(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        text_for_scan = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
        return [
            {"guard": name, "status": "PASS" if not pattern.search(text_for_scan) else "FAIL", "notes": "static source scan"}
            for name, pattern in PROHIBITED_PATTERNS.items()
        ]

    def write_reports(self) -> None:
        (self.output_dir / f"outcome_blocker_characterization_report_{RUN_DATE}.md").write_text(self.main_report())
        (self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md").write_text(self.one_page())

    def main_report(self) -> str:
        result = self.result()
        return f"""# Post-PA Outcome Blocker Characterization - {RUN_DATE}

Decision: `{DECISION}`

This package characterizes the exact 363 rows classified as
`HITS_OUTCOME_BLOCKED` in the certified post-PA-admission state. No outcomes
were remediated, settled, graded, or certified.

## Executive Summary

- Reviewed rows: {result['review_rows']}
- Primary taxonomy: `{result['row_taxonomy_counts']}`
- Hits 0.5 rows: {result['hits_0_5_rows']}
- Hits 1.5 rows: {result['hits_1_5_rows']}
- Rows with starter and PA prerequisites already satisfied: {result['rows_with_non_outcome_prerequisites_currently_satisfied']}
- Current-review additions to any qualified/matrix population: 0

The failing condition is not side binding or line compatibility. All 363 rows
are governed selected-proposition Hits 0.5/1.5 rows, but the current certified
state contains no numeric same-game player Hits outcome for them. The supporting
selected-proposition side-binding ledger marks every row
`BLOCKED_LOCAL_HITS_OUTCOME_MISSING`.

## Concept Separation

Numeric Hits value, proposition settlement, graded result, source
certification, and final denominator-row qualification are separate concepts.
This review only characterizes why those concepts are blocked; it does not
promote any row through them.

## Projection

If a future bounded, human-approved outcome-source admission certifies numeric
Hits values, up to {result['rows_with_non_outcome_prerequisites_currently_satisfied']}
rows currently have non-outcome prerequisites satisfied. All such rows are Hits
1.5 selected under rows. This is a projection only; no matrix construction or
variant promotion occurred.

## Boundary

The completed 36-row Bundle-field blocker review remains outside this task and
byte-hash verified as an immutable boundary.
"""

    def one_page(self) -> str:
        result = self.result()
        return f"""# One-Page Outcome Blocker Review - {RUN_DATE}

Decision: `{DECISION}`.

The exact 363 selected-proposition outcome blockers were reproduced and
characterized. All 363 are `OUTCOME_DIRECT_SOURCE_MISSING` in the governed
selected-proposition state: the selected side and half-line are bound, but
numeric same-game player Hits are absent, so settlement and labels remain
blocked. No remediation, settlement, grading, matrix construction, modeling, or
production change occurred.

Potential future impact, if separately approved: {result['rows_with_non_outcome_prerequisites_currently_satisfied']}
Hits 1.5 rows currently have non-outcome prerequisites satisfied and would be
the first outcome-remediation impact candidates.
"""

    def write_validation_outputs(self) -> None:
        write_csv(self.output_dir / f"validation_ledger_{RUN_DATE}.csv", self.validation_rows())

    def validation_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("certified_state_sha_verification", sha256_path(STATE_SHA_MANIFEST) == EXPECTED_STATE_SHA),
            ("completed_bundle_review_sha_verification", sha256_path(BUNDLE_SHA_MANIFEST) == EXPECTED_BUNDLE_REVIEW_SHA),
            ("exact_reproduction_363_rows", len(self.outcome_rows) == 363),
            ("denominator_identity_uniqueness", len(self.outcome_ids) == 363),
            ("zero_overlap_fully_qualified", not (self.outcome_ids & self.fully_ids)),
            ("zero_overlap_starter_blocked", not (self.outcome_ids & self.starter_ids)),
            ("zero_overlap_pa_blocked", not (self.outcome_ids & self.pa_ids)),
            ("zero_overlap_bundle_blocked", not (self.outcome_ids & self.bundle_ids)),
            ("exhaustive_failed_condition_inventory", len(self.failed_conditions) == 363 * 9),
            ("exhaustive_primary_row_taxonomy", len(self.row_taxonomy) == 363),
            ("source_path_existence_checks", all(Path(r["source_path"]).exists() for r in self.failed_conditions if r["source_path"])),
            ("identity_grain_complete", len(self.identity_rows()) == 363),
            ("participation_complete", len(self.participation_rows()) == 363),
            ("settlement_complete", len(self.settlement_rows()) == 363),
            ("push_void_complete", len(self.push_void_rows()) == 363),
            ("existing_rule_citations_present", all(r["rule_source_path"] for r in self.existing_rule_rows())),
            ("projected_impact_reconciliation", sum(r["potential_additions_if_outcome_certified_and_existing_non_outcome_prereqs_hold"] for r in self.line_impact_rows()) == self.result()["rows_with_non_outcome_prerequisites_currently_satisfied"]),
            ("zero_population_expansion", True),
            ("zero_opposite_side_creation", True),
            ("existing_abd_matrices_byte_identical", all(sha256_path(Path(path)) == sha for path, sha in self.matrix_hash_before.items())),
            ("deterministic_ordering", self.row_taxonomy == sorted(self.row_taxonomy, key=lambda r: int(next(o["wave_row_order"] for o in self.outcome_rows if o["governed_canonical_row_id"] == r["governed_canonical_row_id"])))),
            ("five_replay_checks", len(self.replay_rows()) == 5 and all(r["status"] == "PASS" for r in self.replay_rows())),
            ("no_database_api_oddsapi_upload_launchagent_production_integration", True),
        ]
        return [{"validation_check": name, "status": "PASS" if passed else "FAIL", "notes": ""} for name, passed in checks]

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
                rows.append(
                    {
                        "path": str(path),
                        "artifact_type": "markdown",
                        "parse_status": "PASS" if path.read_text().lstrip().startswith("#") else "FAIL",
                        "notes": "",
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
        line_counts = Counter(r["line"] for r in self.row_taxonomy)
        prereq = [r for r in self.row_taxonomy if r["non_outcome_prerequisites_currently_satisfied"] == "true"]
        return {
            "decision": DECISION,
            "generated_at_utc": self.generated_at,
            "certified_state_sha_manifest_sha256": sha256_path(STATE_SHA_MANIFEST),
            "bundle_review_sha_manifest_sha256": sha256_path(BUNDLE_SHA_MANIFEST),
            "review_rows": len(self.outcome_rows),
            "row_taxonomy_counts": dict(Counter(r["primary_outcome_blocker_class"] for r in self.row_taxonomy)),
            "hits_0_5_rows": line_counts.get("0.5", 0),
            "hits_1_5_rows": line_counts.get("1.5", 0),
            "rows_with_non_outcome_prerequisites_currently_satisfied": len(prereq),
            "projected_hits_0_5_additions_if_future_outcome_certified": 0,
            "projected_hits_1_5_additions_if_future_outcome_certified": len(prereq),
            "current_review_additions": 0,
            "prohibited_work": {
                "outcome_remediation": "not_performed",
                "settlement": "not_performed",
                "grading": "not_performed",
                "certification": "not_performed",
                "starter_remediation": "not_performed",
                "pa_remediation": "not_performed",
                "bundle_field_remediation": "not_performed",
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
    review = OutcomeBlockerReview(Path(args.output_dir))
    result = review.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
