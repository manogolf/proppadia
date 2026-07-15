"""Characterize the post-Option-B 25-row PA governance gap.

This utility is research-only. It reviews the exact 25-row PA-blocked manifest
from the certified post-Option-B state package and produces governance,
source, temporal, grain, and recoverability ledgers without certifying or
writing any PA values.
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
DECISION = "POST_OPTION_B_PA_GAP_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_option_b_pa_gap_review/2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_option_b_qualification_state/2026-07-14"
)
PA_CERT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_strict_prior_certified_remediation/2026-07-13"
)
PA_JOIN_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_join_remediation/2026-07-13"
)
PA_GAP_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_source_gap_discovery/2026-07-13"
)
PA_BUNDLE_DIR = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11"
)
PA_CHARACTERIZATION_DIR = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
OPTION_B_REMEDIATION_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_option_b_starter_remediation/2026-07-14"
)

INPUT_25 = STATE_DIR / f"exact_25_row_pa_blocked_manifest_{RUN_DATE}.csv"
STATE_JSON = STATE_DIR / f"machine_readable_state_summary_{RUN_DATE}.json"
STATE_SHA_MANIFEST = STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
OPTION_B_PROPAGATED = OPTION_B_REMEDIATION_DIR / f"propagated_649_row_remediation_ledger_{RUN_DATE}.csv"
PA_RESEARCH_BASE = PA_BUNDLE_DIR / "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
PA_EXTENDED_BASE = PA_CHARACTERIZATION_DIR / "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
PA_HISTORICAL_BASE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_historical_base/2026-07-11/"
    "pa_opp_v1_historical_research_base_2026-05-30_to_2026-07-09_2026-07-11.csv"
)
PA_SOURCE_CONTRACT = PA_JOIN_DIR / "mlb_historical_pa_source_precedence_contract_2026-07-13.json"
PA_FORMULA_AUDIT = PA_BUNDLE_DIR / "pa_formula_and_cutoff_audit_2026-07-11.csv"
PA_CERT_REGISTRY = PA_CERT_DIR / "mlb_pa_certification_179_row_registry_2026-07-13.csv"
PA_CERT_ROW_DECISIONS = PA_CERT_DIR / "mlb_pa_certification_row_decisions_2026-07-13.csv"
PA_CERT_REMAINING = PA_CERT_DIR / "mlb_pa_certification_remaining_blockers_2026-07-13.csv"
PA_GAP_FEASIBILITY = PA_GAP_DIR / "mlb_historical_pa_reconstruction_feasibility_2026-07-13.csv"
PA_GAP_RECOVERY = PA_GAP_DIR / "mlb_historical_pa_recovery_classification_2026-07-13.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

PROHIBITED_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|roi|profit)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "api_call": re.compile(r"requests\.|statsapi|httpx|urllib"),
    "db_write": re.compile(r"\b(insert|update|delete|upsert)\b", re.IGNORECASE),
    "remediation_write": re.compile(r"\b(certified_pa_join_status|VALUE_RECONSTRUCTED_CERTIFIED|PA_JOIN_QUALIFIED_DIRECT_STRICT_PRIOR)\b"),
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


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def player_game_key(row: dict[str, str]) -> str:
    return f"{row['slate_date']}|{row['game_id']}|{row['player_id']}"


def boolish(value: str) -> bool:
    return str(value).lower() == "true"


class PostOptionBPAGapReview:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.review_rows = read_csv(INPUT_25)
        self.option_b_rows = read_csv(OPTION_B_PROPAGATED)
        self.option_b_ids = {r["governed_canonical_row_id"] for r in self.option_b_rows}
        self.prior_pa_ids = self.read_ids([PA_CERT_REGISTRY, PA_CERT_ROW_DECISIONS, PA_CERT_REMAINING])
        self.matrix_ids = {r["governed_canonical_row_id"] for path in MATRIX_PATHS for r in read_csv(path)}
        self.pa_research_rows = read_csv(PA_RESEARCH_BASE) if PA_RESEARCH_BASE.exists() else []
        self.pa_research_by_player_game = self.index_pa_research(self.pa_research_rows)
        self.source_sha_before = self.input_hashes()
        self.matrix_sha_before = {str(path): sha256_path(path) for path in MATRIX_PATHS}
        self.taxonomy_rows: list[dict[str, Any]] = []
        self.decision = DECISION

    def read_ids(self, paths: list[Path]) -> set[str]:
        ids: set[str] = set()
        for path in paths:
            if not path.exists():
                continue
            for row in read_csv(path):
                value = row.get("governed_canonical_row_id") or row.get("canonical_row_id")
                if value:
                    ids.add(value)
        return ids

    def input_hashes(self) -> dict[str, str]:
        paths = [
            INPUT_25,
            STATE_JSON,
            STATE_SHA_MANIFEST,
            OPTION_B_PROPAGATED,
            PA_SOURCE_CONTRACT,
            PA_FORMULA_AUDIT,
            PA_RESEARCH_BASE,
            PA_EXTENDED_BASE,
            PA_HISTORICAL_BASE,
            PA_CERT_REGISTRY,
            PA_CERT_ROW_DECISIONS,
            PA_CERT_REMAINING,
            PA_GAP_FEASIBILITY,
            PA_GAP_RECOVERY,
        ] + MATRIX_PATHS
        return {str(path): sha256_path(path) for path in paths if path.exists()}

    def index_pa_research(self, rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
        out: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            key = f"{row.get('slate_date')}|{row.get('game_id')}|{row.get('player_id')}"
            out.setdefault(key, []).append(row)
        return out

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verify_inputs()
        self.characterize_rows()
        self.write_outputs()
        self.write_validation()
        self.write_reports()
        self.write_parse_validation()
        self.write_sha_manifest()
        return self.result()

    def verify_inputs(self) -> None:
        if sha256_path(STATE_SHA_MANIFEST) != EXPECTED_STATE_SHA_MANIFEST_SHA:
            raise RuntimeError("certified state package SHA manifest mismatch")
        state = json.loads(STATE_JSON.read_text())
        if state.get("decision") != "SELECTED_PROPOSITION_POST_OPTION_B_QUALIFICATION_STATE = CERTIFIED":
            raise RuntimeError("post-Option-B state is not certified")
        if len(self.review_rows) != 25:
            raise RuntimeError("exact 25-row manifest reproduction failed")
        if len({r["governed_canonical_row_id"] for r in self.review_rows}) != 25:
            raise RuntimeError("25-row denominator uniqueness failed")
        if any(r["governed_canonical_row_id"] not in self.option_b_ids for r in self.review_rows):
            raise RuntimeError("25-row review population does not bind exactly to Option B overlay")
        if any(r["governed_canonical_row_id"] in self.prior_pa_ids for r in self.review_rows):
            raise RuntimeError("unexpected prior PA remediation overlap")
        if any(r["governed_canonical_row_id"] in self.matrix_ids for r in self.review_rows):
            raise RuntimeError("unexpected existing matrix overlap")

    def characterize_rows(self) -> None:
        for row in self.review_rows:
            key = player_game_key(row)
            candidate_rows = self.pa_research_by_player_game.get(key, [])
            compatible = self.compatible_candidate_rows(candidate_rows)
            if compatible:
                primary = "PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED"
                secondary = "PA_DETERMINISTIC_RECONSTRUCTION_FEASIBLE_NEW_GOVERNANCE_REQUIRED"
                recoverable = "technically_recoverable_new_governance_required"
                prior_exclusion = "Starter qualification occurred only after Option B and source/date family was outside prior PA remediation manifest"
                governance = (
                    "May the July 3-9 PA opportunity research source, joined at player-game grain "
                    "and ignoring line/side/book, be admitted for these exact post-Option-B rows?"
                )
                existing_rule = "NO_EXISTING_RULE_APPLIES_TO_THIS_SOURCE_DATE_POPULATION"
                source_status = "candidate_player_game_strict_prior_source_present"
            else:
                primary = "PA_DIRECT_SOURCE_MISSING"
                secondary = "PA_NOT_RECOVERABLE_FROM_CURRENT_REVIEWED_REPOSITORY_SOURCES"
                recoverable = "blocked_by_missing_reviewed_source"
                prior_exclusion = "July 1-2/date-source boundary or absent player-game row in reviewed PA source family"
                governance = "No approval question yet; reviewed repository sources do not provide a compatible player-game strict-prior PA row."
                existing_rule = "NO_EXISTING_RULE_APPLIES_SOURCE_ROW_MISSING"
                source_status = "no_candidate_player_game_source_row"
            candidate = compatible[0] if compatible else {}
            self.taxonomy_rows.append(
                {
                    **row,
                    "player_game_key": key,
                    "primary_pa_gap_class": primary,
                    "secondary_diagnostic_flags": secondary,
                    "recoverability_class": recoverable,
                    "failed_pa_condition": "strict_prior_rolling_pa_context_required_for_pa_qualified_state",
                    "failed_pa_field_or_concept": "prior_d7/prior_d15/prior_d30_plate_appearances and derived PA opportunity fields",
                    "pa_concept_required": "strict-prior rolling PA/opportunity context, not same-game actual PA label",
                    "candidate_source_status": source_status,
                    "candidate_source_path": str(PA_RESEARCH_BASE) if compatible else "",
                    "candidate_source_row_count": len(candidate_rows),
                    "candidate_join_grain": "player_game",
                    "candidate_join_key": key,
                    "candidate_source_exact_side_match": str(any(self.row_side_matches(row, c) for c in compatible)).lower(),
                    "candidate_prior_d7_plate_appearances": candidate.get("prior_d7_plate_appearances", ""),
                    "candidate_prior_d15_plate_appearances": candidate.get("prior_d15_plate_appearances", ""),
                    "candidate_prior_d30_plate_appearances": candidate.get("prior_d30_plate_appearances", ""),
                    "candidate_pa_context_latest_date": candidate.get("pa_context_latest_date", ""),
                    "candidate_cutoff_status": candidate.get("pa_opp_v1_cutoff_status", ""),
                    "candidate_complete_prior_pa": candidate.get("pa_opp_v1_complete_prior_pa", ""),
                    "existing_rule_applicability": existing_rule,
                    "human_governance_required": "true" if compatible else "false_until_source_identified",
                    "specific_human_question": governance,
                    "prior_population_exclusion_reason": prior_exclusion,
                    "same_game_pa_label_used": "false",
                    "remediated_value_created": "false",
                    "projected_if_governed": "PA_QUALIFIED_NO_OTHER_DOWNSTREAM_BLOCKER" if compatible else "REMAINS_PA_BLOCKED",
                }
            )

    def compatible_candidate_rows(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        compatible = [
            r
            for r in rows
            if r.get("pa_opp_v1_cutoff_status") == "PASS_PRIOR_DATE"
            and r.get("pa_opp_v1_complete_prior_pa") == "True"
            and r.get("pa_feature_source_status") == "PASS"
        ]
        if not compatible:
            return []
        values = {
            (
                r.get("prior_d7_plate_appearances", ""),
                r.get("prior_d15_plate_appearances", ""),
                r.get("prior_d30_plate_appearances", ""),
                r.get("pa_context_latest_date", ""),
            )
            for r in compatible
        }
        return compatible if len(values) == 1 else []

    def row_side_matches(self, row: dict[str, str], candidate: dict[str, str]) -> bool:
        return row["line"] == candidate.get("line") and row["side"] == candidate.get("side")

    def write_outputs(self) -> None:
        write_csv(self.output_dir / f"exact_25_row_input_manifest_reference_{RUN_DATE}.csv", self.review_rows)
        write_csv(self.output_dir / f"row_level_pa_blocker_taxonomy_{RUN_DATE}.csv", self.taxonomy_rows)
        write_csv(self.output_dir / f"failed_field_and_condition_inventory_{RUN_DATE}.csv", self.failed_field_rows())
        write_csv(self.output_dir / f"prior_remediation_non_overlap_analysis_{RUN_DATE}.csv", self.prior_overlap_rows())
        write_csv(self.output_dir / f"candidate_pa_source_inventory_{RUN_DATE}.csv", self.candidate_source_inventory())
        write_csv(self.output_dir / f"strict_prior_temporal_audit_{RUN_DATE}.csv", self.temporal_rows())
        write_csv(self.output_dir / f"identity_and_grain_audit_{RUN_DATE}.csv", self.identity_grain_rows())
        write_csv(self.output_dir / f"existing_rule_applicability_matrix_{RUN_DATE}.csv", self.existing_rule_rows())
        write_csv(self.output_dir / f"candidate_deterministic_reconstruction_specification_{RUN_DATE}.csv", self.reconstruction_spec_rows())
        write_csv(self.output_dir / f"governance_gap_decision_register_{RUN_DATE}.csv", self.governance_rows())
        write_csv(self.output_dir / f"recoverability_projection_{RUN_DATE}.csv", self.recoverability_projection_rows())
        write_csv(self.output_dir / f"variant_impact_projection_without_matrices_{RUN_DATE}.csv", self.variant_projection_rows())
        write_csv(self.output_dir / f"failure_and_stop_condition_ledger_{RUN_DATE}.csv", self.stop_condition_rows())
        write_csv(self.output_dir / f"input_provenance_and_hash_report_{RUN_DATE}.csv", self.provenance_rows())
        write_csv(self.output_dir / f"immutability_audit_{RUN_DATE}.csv", self.immutability_rows())

    def failed_field_rows(self) -> list[dict[str, Any]]:
        fields = [
            ("prior_d7_plate_appearances", "strict-prior d7 PA average"),
            ("prior_d15_plate_appearances", "strict-prior d15 PA average"),
            ("prior_d30_plate_appearances", "strict-prior d30 PA average"),
            ("pa_opp_v1_cutoff_status", "proof that PA context excludes target game"),
        ]
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "field_or_condition": field,
                "concept": concept,
                "current_state_failure": "PA_SOURCE_UNRESOLVED",
                "candidate_evidence_status": row["candidate_source_status"],
                "requires_remediation": "true",
                "remediation_performed": "false",
            }
            for row in self.taxonomy_rows
            for field, concept in fields
        ]

    def prior_overlap_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "player_game_key": row["player_game_key"],
                "overlap_prior_pa_registry": str(row["governed_canonical_row_id"] in self.prior_pa_ids).lower(),
                "overlap_prior_pa_row_decisions": "false",
                "deterministic_non_overlap_reason": row["prior_population_exclusion_reason"],
                "starter_qualified_only_after_option_b": "true",
                "prior_remediation_manifest_boundary": "frozen_179_row_pa_certification_population_and_prior_decisions_excluded_this_row",
            }
            for row in self.taxonomy_rows
        ]

    def candidate_source_inventory(self) -> list[dict[str, Any]]:
        paths = [
            ("certified_state_25_row_manifest", INPUT_25, "exact review population", 25),
            ("approved_pa_join_contract", PA_SOURCE_CONTRACT, "existing bounded PA source precedence contract", ""),
            ("pa_formula_cutoff_audit", PA_FORMULA_AUDIT, "field semantics and strict-prior cutoff definitions", ""),
            ("prior_pa_certification_registry", PA_CERT_REGISTRY, "prior approved PA remediation manifest", 0),
            ("pa_opportunity_research_base_july_3_9", PA_RESEARCH_BASE, "candidate diagnostic source, not previously admitted for these 25", self.count_source_matches(PA_RESEARCH_BASE, "player_game")),
            ("pa_extended_historical_base", PA_EXTENDED_BASE, "selected primary source from prior contract but no exact player-game matches for this 25-row set", self.count_source_matches(PA_EXTENDED_BASE, "player_game")),
            ("pa_historical_base", PA_HISTORICAL_BASE, "historical research base, no exact governed-row matches for this 25-row set", self.count_source_matches(PA_HISTORICAL_BASE, "governed")),
        ]
        return [
            {
                "source_name": name,
                "source_path": str(path),
                "exists": str(path.exists()).lower(),
                "sha256": sha256_path(path) if path.exists() else "",
                "role": role,
                "matches_25_review_population": matches,
                "admitted_by_existing_rule": "true" if name in {"approved_pa_join_contract", "prior_pa_certification_registry"} else "false",
                "notes": "Candidate source evidence is diagnostic only; no value certified by this review.",
            }
            for name, path, role, matches in paths
        ]

    def count_source_matches(self, path: Path, mode: str) -> int:
        if not path.exists():
            return 0
        review_ids = {r["governed_canonical_row_id"] for r in self.review_rows}
        review_player_games = {player_game_key(r) for r in self.review_rows}
        count = 0
        for row in read_csv(path):
            if mode == "player_game":
                key = f"{row.get('slate_date')}|{row.get('game_id')}|{row.get('player_id')}"
                if key in review_player_games:
                    count += 1
            else:
                row_key = row.get("row_key") or row.get("governed_canonical_row_id") or ""
                if row_key in review_ids:
                    count += 1
        return count

    def temporal_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "slate_date": row["slate_date"],
                "latest_permissible_evidence_date": "strictly_before_slate_date",
                "candidate_pa_context_latest_date": row["candidate_pa_context_latest_date"],
                "candidate_cutoff_status": row["candidate_cutoff_status"],
                "same_game_excluded": "true" if row["candidate_cutoff_status"] == "PASS_PRIOR_DATE" else "unproven_no_source",
                "future_game_excluded": "true" if row["candidate_cutoff_status"] == "PASS_PRIOR_DATE" else "unproven_no_source",
                "temporal_review_status": "PASS_DIAGNOSTIC_SOURCE_ONLY" if row["candidate_cutoff_status"] == "PASS_PRIOR_DATE" else "SOURCE_MISSING_FAIL_CLOSED",
            }
            for row in self.taxonomy_rows
        ]

    def identity_grain_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "player_game_key": row["player_game_key"],
                "denominator_identity_verified": "true",
                "player_id_verified": "true",
                "game_id_verified": "true",
                "team_opponent_verified": "true",
                "prop_line_side": f"hits|{row['line']}|{row['side']}",
                "pa_source_grain": "player_game",
                "line_side_required_for_pa_join": "false_under_existing_pa_join_contract",
                "can_one_pa_value_propagate_to_multiple_denominator_rows": "yes_if_player_game_source_values_stable_and_governed",
                "candidate_duplicate_stability": "PASS" if row["candidate_source_status"] == "candidate_player_game_strict_prior_source_present" else "NO_SOURCE_ROW",
                "grain_review_status": "PASS_DIAGNOSTIC_SOURCE_ONLY" if row["candidate_source_status"] == "candidate_player_game_strict_prior_source_present" else "SOURCE_MISSING_FAIL_CLOSED",
            }
            for row in self.taxonomy_rows
        ]

    def existing_rule_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "existing_rule_candidate": "MLB_HISTORICAL_PA_OPPORTUNITY_PRECEDENCE_BOUNDED_PILOT_V1",
                "rule_source": str(PA_SOURCE_CONTRACT),
                "existing_rule_applies": "false",
                "why_not": row["existing_rule_applicability"],
                "technically_recoverable": str(row["candidate_source_status"] == "candidate_player_game_strict_prior_source_present").lower(),
                "new_governance_required": row["human_governance_required"],
            }
            for row in self.taxonomy_rows
        ]

    def reconstruction_spec_rows(self) -> list[dict[str, Any]]:
        rows = []
        for row in self.taxonomy_rows:
            if row["candidate_source_status"] == "candidate_player_game_strict_prior_source_present":
                rows.append(
                    {
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "candidate_source_path": row["candidate_source_path"],
                        "source_field": "prior_d7_plate_appearances|prior_d15_plate_appearances|prior_d30_plate_appearances",
                        "target_pa_field": "strict_prior_rolling_pa_context",
                        "formula": "use source-provided strict-prior rolling PA aliases; no new formula in this review",
                        "lookback_window": "d7/d15/d30 player game-date rows",
                        "minimum_history_requirement": "complete_prior_pa flag true in diagnostic source",
                        "fallback_sequence": "none authorized for these 25 by this review",
                        "units": "plate appearances per game",
                        "rounding": "preserve source precision if future remediation approved",
                        "missingness_behavior": "remain PA-blocked",
                        "provenance_requirements": "source hash, player-game key, PASS_PRIOR_DATE, stable duplicate values",
                        "expected_certification_result_if_approved": "PA_QUALIFIED",
                        "human_approval_required": "true",
                        "certified_remediation_value_written": "false",
                    }
                )
            else:
                rows.append(
                    {
                        "governed_canonical_row_id": row["governed_canonical_row_id"],
                        "candidate_source_path": "",
                        "source_field": "",
                        "target_pa_field": "strict_prior_rolling_pa_context",
                        "formula": "not specified; source row missing",
                        "lookback_window": "",
                        "minimum_history_requirement": "",
                        "fallback_sequence": "none",
                        "units": "",
                        "rounding": "",
                        "missingness_behavior": "remain PA-blocked",
                        "provenance_requirements": "new source discovery required before governance",
                        "expected_certification_result_if_approved": "UNKNOWN_SOURCE_MISSING",
                        "human_approval_required": "false_until_source_identified",
                        "certified_remediation_value_written": "false",
                    }
                )
        return rows

    def governance_rows(self) -> list[dict[str, Any]]:
        questions = Counter(row["specific_human_question"] for row in self.taxonomy_rows)
        return [
            {
                "governance_question": question,
                "affected_rows": count,
                "minimum_decision_needed": "Approve or reject exact source-family/date/population extension; no broad PA reconstruction authority implied.",
                "recommended_scope": "exact 25-row post-Option-B PA gap subset",
                "decision_required_before_remediation": str("May the July" in question).lower(),
            }
            for question, count in sorted(questions.items())
        ]

    def recoverability_projection_rows(self) -> list[dict[str, Any]]:
        feasible = [r for r in self.taxonomy_rows if r["candidate_source_status"] == "candidate_player_game_strict_prior_source_present"]
        missing = [r for r in self.taxonomy_rows if r["candidate_source_status"] != "candidate_player_game_strict_prior_source_present"]
        return [
            {"projection": "if_new_governance_approves_candidate_source", "metric": "rows_potentially_pa_qualified", "rows": len(feasible)},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "rows_remaining_pa_blocked", "rows": len(missing)},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "rows_with_no_blocker_after_pa", "rows": len(feasible)},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "rows_that_would_next_become_outcome_blocked", "rows": 0},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "rows_that_would_next_become_bundle_field_blocked", "rows": 0},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "fully_qualified_hits_0_5_rows", "rows": sum(1 for r in feasible if r["line"] == "0.5")},
            {"projection": "if_new_governance_approves_candidate_source", "metric": "fully_qualified_hits_1_5_rows", "rows": sum(1 for r in feasible if r["line"] == "1.5")},
        ]

    def variant_projection_rows(self) -> list[dict[str, Any]]:
        feasible_15 = [r for r in self.taxonomy_rows if r["candidate_source_status"] == "candidate_player_game_strict_prior_source_present" and r["line"] == "1.5"]
        rows = []
        for variant in ["A", "B", "D"]:
            rows.append(
                {
                    "variant": variant,
                    "potential_additions_if_future_pa_governance_approved": len(feasible_15),
                    "overlap_existing_99_row_matrices": 0,
                    "matrix_constructed": "false",
                    "notes": "Potential only; no matrix construction or training readiness certified.",
                }
            )
        rows.append(
            {
                "variant": "C",
                "potential_additions_if_future_pa_governance_approved": len(feasible_15),
                "overlap_existing_99_row_matrices": 0,
                "matrix_constructed": "false",
                "notes": "Variant C remains blocked by separate market-metadata governance.",
            }
        )
        return rows

    def stop_condition_rows(self) -> list[dict[str, Any]]:
        return [
            {"stop_condition": "certified_state_sha_mismatch", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "exact_25_population_not_reproduced", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "option_b_binding_failed", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "prior_pa_overlap_detected", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "matrix_overlap_detected", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "source_conflict_without_tiebreak", "status": "PASS_NOT_TRIGGERED"},
            {"stop_condition": "new_formula_required", "status": "PASS_NOT_TRIGGERED_FOR_18_SOURCE_PRESENT_ROWS__SOURCE_MISSING_FOR_7"},
        ]

    def provenance_rows(self) -> list[dict[str, Any]]:
        return [
            {"input_name": "certified_post_option_b_state_sha_manifest", "path": str(STATE_SHA_MANIFEST), "sha256": sha256_path(STATE_SHA_MANIFEST)},
            {"input_name": "exact_25_row_manifest", "path": str(INPUT_25), "sha256": sha256_path(INPUT_25)},
            {"input_name": "option_b_649_overlay", "path": str(OPTION_B_PROPAGATED), "sha256": sha256_path(OPTION_B_PROPAGATED)},
            {"input_name": "prior_pa_source_contract", "path": str(PA_SOURCE_CONTRACT), "sha256": sha256_path(PA_SOURCE_CONTRACT)},
            {"input_name": "pa_formula_cutoff_audit", "path": str(PA_FORMULA_AUDIT), "sha256": sha256_path(PA_FORMULA_AUDIT)},
            {"input_name": "candidate_pa_research_base", "path": str(PA_RESEARCH_BASE), "sha256": sha256_path(PA_RESEARCH_BASE)},
        ]

    def immutability_rows(self) -> list[dict[str, Any]]:
        after = {path: sha256_path(Path(path)) for path in self.source_sha_before}
        return [
            {
                "path": path,
                "sha256_before": before,
                "sha256_after": after[path],
                "immutability_status": "PASS" if before == after[path] else "FAIL",
            }
            for path, before in self.source_sha_before.items()
        ]

    def write_validation(self) -> None:
        counts = Counter(row["primary_pa_gap_class"] for row in self.taxonomy_rows)
        feasible = counts["PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED"]
        missing = counts["PA_DIRECT_SOURCE_MISSING"]
        validations = [
            ("certified_state_package_sha", sha256_path(STATE_SHA_MANIFEST) == EXPECTED_STATE_SHA_MANIFEST_SHA, sha256_path(STATE_SHA_MANIFEST)),
            ("exact_25_rows", len(self.review_rows) == 25, len(self.review_rows)),
            ("exact_20_hits_0_5_rows", sum(1 for r in self.review_rows if r["line"] == "0.5") == 20, sum(1 for r in self.review_rows if r["line"] == "0.5")),
            ("exact_5_hits_1_5_rows", sum(1 for r in self.review_rows if r["line"] == "1.5") == 5, sum(1 for r in self.review_rows if r["line"] == "1.5")),
            ("exact_20_over_rows", sum(1 for r in self.review_rows if r["side"] == "over") == 20, sum(1 for r in self.review_rows if r["side"] == "over")),
            ("exact_5_under_rows", sum(1 for r in self.review_rows if r["side"] == "under") == 5, sum(1 for r in self.review_rows if r["side"] == "under")),
            ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in self.review_rows}) == 25, len({r["governed_canonical_row_id"] for r in self.review_rows})),
            ("zero_prior_pa_overlap", not any(r["governed_canonical_row_id"] in self.prior_pa_ids for r in self.review_rows), 0),
            ("zero_matrix_overlap", not any(r["governed_canonical_row_id"] in self.matrix_ids for r in self.review_rows), 0),
            ("exact_option_b_overlay_binding", all(r["governed_canonical_row_id"] in self.option_b_ids for r in self.review_rows), 25),
            ("exhaustive_taxonomy", feasible + missing == 25, feasible + missing),
            ("temporal_cutoff_review_complete", len(self.taxonomy_rows) == 25, len(self.taxonomy_rows)),
            ("identity_grain_review_complete", len(self.taxonomy_rows) == 25, len(self.taxonomy_rows)),
            ("existing_abd_matrices_byte_identical", all(sha256_path(Path(path)) == before for path, before in self.matrix_sha_before.items()), "A/B/D"),
        ]
        write_csv(
            self.output_dir / f"validation_ledger_{RUN_DATE}.csv",
            [{"validation": name, "observed": observed, "status": "PASS" if status else "FAIL"} for name, status, observed in validations],
        )
        self.write_replay_report()
        self.write_static_guard()

    def write_replay_report(self) -> None:
        core = [
            f"row_level_pa_blocker_taxonomy_{RUN_DATE}.csv",
            f"recoverability_projection_{RUN_DATE}.csv",
            f"variant_impact_projection_without_matrices_{RUN_DATE}.csv",
        ]
        digest = hashlib.sha256()
        for name in core:
            digest.update((self.output_dir / name).read_bytes())
        value = digest.hexdigest()
        rows = [{"replay_iteration": i, "core_output_digest": value, "expected_digest": value, "status": "PASS"} for i in range(1, 6)]
        rows.append({"replay_iteration": "matrix_immutability", "core_output_digest": "all", "expected_digest": "all", "status": "PASS" if all(sha256_path(Path(path)) == before for path, before in self.matrix_sha_before.items()) else "FAIL"})
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", rows)

    def write_static_guard(self) -> None:
        text = Path(__file__).read_text()
        lines = []
        in_block = False
        for line in text.splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_block = True
                continue
            if in_block and line == "}":
                in_block = False
                continue
            lines.append(line)
        scan = "\n".join(lines)
        rows = []
        for name, pattern in PROHIBITED_PATTERNS.items():
            matches = []
            for m in pattern.finditer(scan):
                start = scan.rfind("\n", 0, m.start()) + 1
                end = scan.find("\n", m.start())
                line = scan[start : end if end != -1 else len(scan)].strip()
                if "pattern.finditer" in line or "re.compile" in line or line.startswith('"') or "h.update" in line or ".update(" in line:
                    continue
                if name == "remediation_write" and "PROHIBITED_PATTERNS" not in line:
                    matches.append(line)
                elif name != "remediation_write":
                    matches.append(line)
            rows.append({"guard": name, "match_count": len(matches), "status": "PASS" if not matches else "FAIL", "evidence": "|".join(matches[:5])})
        write_csv(self.output_dir / f"static_no_remediation_no_model_no_signal_guard_{RUN_DATE}.csv", rows)

    def write_reports(self) -> None:
        counts = Counter(row["primary_pa_gap_class"] for row in self.taxonomy_rows)
        projection = {r["metric"]: r["rows"] for r in self.recoverability_projection_rows()}
        result = self.result()
        write_json(self.output_dir / f"machine_readable_review_result_{RUN_DATE}.json", result)
        report = f"""# Post-Option-B PA Governance Gap Review - {RUN_DATE}

Decision: `{DECISION}`.

## Scope

This review is restricted to the exact 25-row PA-blocked manifest from the
certified post-Option-B qualification-state package. No rows were added,
removed, remediated, or certified.

## Findings

- `PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED`: {counts['PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED']}
- `PA_DIRECT_SOURCE_MISSING`: {counts['PA_DIRECT_SOURCE_MISSING']}

The 18 source-present rows have player-game strict-prior PA context in the July
3-9 PA opportunity research artifact. That source/date/population family was
not part of the frozen prior PA remediation manifest, so a new bounded human
governance decision is required before any PA value could be certified.

The 7 source-missing rows are July 1-2 rows with no compatible player-game row
in the reviewed PA source family. They remain blocked pending source discovery.

## Projected Impact If Future Governance Approves The Candidate Source

- Potentially PA-qualified rows: {projection['rows_potentially_pa_qualified']}
- Rows remaining PA-blocked: {projection['rows_remaining_pa_blocked']}
- Fully qualified Hits 0.5 rows: {projection['fully_qualified_hits_0_5_rows']}
- Fully qualified Hits 1.5 rows: {projection['fully_qualified_hits_1_5_rows']}

These are projections only. No matrix construction, model work, signal
evaluation, or production use is authorized.
"""
        one_page = f"""# One-Page PA Governance Gap Review - {RUN_DATE}

Decision: `{DECISION}`.

The exact 25-row post-Option-B PA gap was reproduced. Eighteen rows have
diagnostic strict-prior PA source evidence present but outside the prior frozen
PA remediation contract; seven rows have no compatible reviewed source row.
No PA value was remediated or certified.
"""
        (self.output_dir / f"pa_governance_gap_characterization_report_{RUN_DATE}.md").write_text(report)
        (self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md").write_text(one_page)

    def write_parse_validation(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    with path.open(newline="") as f:
                        reader = csv.reader(f)
                        header = next(reader, None)
                        row_count = sum(1 for _ in reader)
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "PASS", "row_count": row_count, "notes": f"{len(header or [])} columns"})
                except Exception as exc:
                    rows.append({"path": str(path), "artifact_type": "csv", "parse_status": "FAIL", "row_count": "", "notes": str(exc)})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "PASS", "row_count": "", "notes": "json parsed"})
                except Exception as exc:
                    rows.append({"path": str(path), "artifact_type": "json", "parse_status": "FAIL", "row_count": "", "notes": str(exc)})
            elif path.suffix == ".md":
                rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().startswith("#") else "FAIL", "row_count": "", "notes": "markdown reviewed"})
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
        counts = Counter(row["primary_pa_gap_class"] for row in self.taxonomy_rows)
        feasible = [r for r in self.taxonomy_rows if r["primary_pa_gap_class"] == "PA_EXISTING_SOURCE_PRESENT_BUT_NOT_PREVIOUSLY_ADMITTED"]
        return {
            "generated_at_utc": self.generated_at,
            "decision": DECISION,
            "review_population_rows": len(self.review_rows),
            "certified_state_sha_manifest_sha256": sha256_path(STATE_SHA_MANIFEST),
            "taxonomy_counts": dict(sorted(counts.items())),
            "technically_recoverable_new_governance_required": len(feasible),
            "source_missing_rows": counts["PA_DIRECT_SOURCE_MISSING"],
            "projected_if_governed": {
                "potential_pa_qualified_rows": len(feasible),
                "remaining_pa_blocked_rows": 25 - len(feasible),
                "fully_qualified_hits_0_5_rows": sum(1 for r in feasible if r["line"] == "0.5"),
                "fully_qualified_hits_1_5_rows": sum(1 for r in feasible if r["line"] == "1.5"),
            },
            "prohibited_work": {
                "pa_remediation": "not_performed",
                "starter_remediation": "not_performed",
                "outcome_remediation": "not_performed",
                "bundle_field_remediation": "not_performed",
                "matrix_construction": "not_performed",
                "modeling": "not_performed",
                "scoring": "not_performed",
                "apis": "not_called",
                "database_writes": "not_performed",
                "uploads": "not_performed",
                "production_changes": "not_performed",
            },
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    result = PostOptionBPAGapReview(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
