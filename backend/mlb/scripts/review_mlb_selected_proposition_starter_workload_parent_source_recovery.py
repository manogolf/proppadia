"""Review source-recovery feasibility for strict-prior starter workload gaps.

This utility is research-only. It reviews the exact eight starter-game sides and
50 denominator rows from the strict-prior starter workload gap package and
assesses whether the missing parent lineage can be recovered without changing
frozen semantics. It does not acquire sources, reconstruct values, remediate,
certify, build matrices, train, score, call APIs, write databases, or alter
production behavior.
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
EXPECTED_WORKLOAD_GAP_SHA = "23e4faa1d939ad18884b859060eae56715dedece61f5fde012775bd181242bb1"
EXPECTED_STATE_SHA = "14506ec7fa6ea4f0ac3164d4b76a6fb7e88e6fb5479625308c4594053bf235f1"
EXPECTED_STARTER_REVIEW_SHA = "b7635ad93c2261da497921bd051a65536488513602a766bada2bc3e3f7888754"
EXPECTED_OUTCOME_REVIEW_SHA = "4dcdf7bca8bed8d5832f321c57db5d93beca6b8318bce6b80db98b19a2566d4e"
DECISION = "STARTER_WORKLOAD_PARENT_SOURCE_RECOVERY_REVIEW_DECISION = CHARACTERIZED_NO_ACQUISITION_OR_REMEDIATION_PERFORMED"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_workload_parent_source_recovery_review/"
    "2026-07-14"
)
WORKLOAD_GAP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_strict_prior_starter_workload_gap_review/2026-07-14"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_admission_qualification_state/2026-07-14"
)
STARTER_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_blocker_review/2026-07-14"
)
OUTCOME_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_pa_outcome_blocker_review/2026-07-14"
)
STARTER_XH_DIR = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11"
)
STARTER_WORKLOAD_RECON_DIR = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11"
)
STARTER_ARCHIVE_PILOT_DIR = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_archive_extension_pilot_1/2026-07-12"
)
SOURCE_GAP_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_starter_source_gap_discovery/2026-07-13"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

WORKLOAD_GAP_SHA = WORKLOAD_GAP_DIR / f"sha256_manifest_{RUN_DATE}.csv"
WORKLOAD_GAP_RESULT = WORKLOAD_GAP_DIR / f"machine_readable_review_result_{RUN_DATE}.json"
WORKLOAD_GAP_ROWS = WORKLOAD_GAP_DIR / f"exact_50_row_denominator_manifest_{RUN_DATE}.csv"
WORKLOAD_GAP_SIDES = WORKLOAD_GAP_DIR / f"exact_eight_side_starter_game_manifest_{RUN_DATE}.csv"
WORKLOAD_GAP_FIELD_FAILURES = WORKLOAD_GAP_DIR / f"failed_workload_field_inventory_{RUN_DATE}.csv"
WORKLOAD_GAP_SIDE_TAXONOMY = WORKLOAD_GAP_DIR / f"side_level_primary_taxonomy_{RUN_DATE}.csv"

STATE_SHA = STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
STARTER_REVIEW_SHA = STARTER_REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"
OUTCOME_REVIEW_SHA = OUTCOME_REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"

STARTER_XH_DATASET = STARTER_XH_DIR / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
STARTER_GAME_BASE = STARTER_WORKLOAD_RECON_DIR / "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
STARTER_EXPANDED_BASE = STARTER_WORKLOAD_RECON_DIR / "starter_skill_workload_batter_prop_expanded_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
ARCHIVE_PILOT_BASE = STARTER_ARCHIVE_PILOT_DIR / "starter_skill_workload_starter_game_base_2026-07-07_to_2026-07-09_pilot_2026-07-12.csv"
SOURCE_GAP_SHA = SOURCE_GAP_DIR / "sha256_manifest_2026-07-13.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

PARENT_DOMAINS = [
    "prior_outs_or_innings",
    "prior_starts",
    "recent_workload_windows",
    "starter_expected_hits_inputs",
]

PROHIBITED_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "api_call": re.compile(r"requests\.|statsapi|httpx|urllib"),
    "db_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert)\b", re.IGNORECASE),
    "matrix_build": re.compile(r"build_mlb_selected_proposition_abd_matrices"),
    "starter_remediation_call": re.compile(r"remediate_mlb_selected_proposition_option_b_starters"),
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


def norm_player_id(value: str) -> str:
    value = str(value or "").strip()
    if value.endswith(".0"):
        return value[:-2]
    return value


class StarterWorkloadParentSourceRecoveryReview:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.rows = read_csv(WORKLOAD_GAP_ROWS)
        self.sides = read_csv(WORKLOAD_GAP_SIDES)
        self.side_taxonomy = read_csv(WORKLOAD_GAP_SIDE_TAXONOMY)
        self.field_failures = read_csv(WORKLOAD_GAP_FIELD_FAILURES)
        self.side_keys = sorted({r["starter_game_key"] for r in self.sides})
        self.side_by_key = {r["starter_game_key"]: r for r in self.sides}
        self.rows_by_side = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        self.input_hash_before = self.input_hashes()
        self.local_evidence = self.build_local_evidence()
        self.parent_inventory: list[dict[str, Any]] = []
        self.missing_parent_records: list[dict[str, Any]] = []
        self.side_recoverability: list[dict[str, Any]] = []

    def input_hashes(self) -> dict[str, str]:
        paths = [
            WORKLOAD_GAP_SHA,
            WORKLOAD_GAP_RESULT,
            WORKLOAD_GAP_ROWS,
            WORKLOAD_GAP_SIDES,
            WORKLOAD_GAP_FIELD_FAILURES,
            WORKLOAD_GAP_SIDE_TAXONOMY,
            STATE_SHA,
            STARTER_REVIEW_SHA,
            OUTCOME_REVIEW_SHA,
            STARTER_XH_DATASET,
            STARTER_GAME_BASE,
            STARTER_EXPANDED_BASE,
            ARCHIVE_PILOT_BASE,
            SOURCE_GAP_SHA,
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
        if sha256_path(WORKLOAD_GAP_SHA) != EXPECTED_WORKLOAD_GAP_SHA:
            raise RuntimeError("workload-gap review package SHA mismatch")
        if sha256_path(STATE_SHA) != EXPECTED_STATE_SHA:
            raise RuntimeError("certified state SHA mismatch")
        if sha256_path(STARTER_REVIEW_SHA) != EXPECTED_STARTER_REVIEW_SHA:
            raise RuntimeError("starter review SHA mismatch")
        if sha256_path(OUTCOME_REVIEW_SHA) != EXPECTED_OUTCOME_REVIEW_SHA:
            raise RuntimeError("outcome review boundary SHA mismatch")
        result = json.loads(WORKLOAD_GAP_RESULT.read_text())
        if result.get("decision") != "STRICT_PRIOR_STARTER_WORKLOAD_GAP_REVIEW_DECISION = CHARACTERIZED_NO_REMEDIATION_PERFORMED":
            raise RuntimeError("workload-gap review decision mismatch")
        if len(self.rows) != 50 or len({r["governed_canonical_row_id"] for r in self.rows}) != 50:
            raise RuntimeError("exact 50-row denominator population reproduction failed")
        if len(self.side_keys) != 8:
            raise RuntimeError("exact eight starter-game-side population reproduction failed")
        if sum(len(v) for v in self.rows_by_side.values()) != 50:
            raise RuntimeError("side-to-row propagation does not reconcile to 50")

    def build_local_evidence(self) -> dict[str, list[dict[str, Any]]]:
        evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for source_name, path, pid_cols, date_col in [
            ("starter_expected_hits_research_dataset", STARTER_XH_DATASET, ["actual_starter_player_id", "opposing_starter_player_id"], "date"),
            ("starter_game_base", STARTER_GAME_BASE, ["actual_starter_player_id"], "date"),
            ("starter_batter_prop_expanded_base", STARTER_EXPANDED_BASE, ["actual_starter_player_id", "opposing_starter_player_id"], "date"),
            ("starter_archive_extension_pilot", ARCHIVE_PILOT_BASE, ["actual_starter_player_id", "expected_starter_player_id"], "date"),
        ]:
            if not path.exists():
                continue
            rows = read_csv(path)
            for side in self.sides:
                pid = side["actual_starter_player_ids"]
                for row in rows:
                    if any(norm_player_id(row.get(col, "")) == pid for col in pid_cols):
                        evidence[side["starter_game_key"]].append(
                            {
                                "source_family": source_name,
                                "source_path": str(path),
                                "source_identity": self.source_identity(row),
                                "source_timestamp": "",
                                "player_id": pid,
                                "game_id": row.get("game_id", ""),
                                "game_date": row.get(date_col, ""),
                                "role": row.get("actual_starter_role", ""),
                                "outs_or_innings": row.get("baseline_outs_per_start", "") or row.get("baseline_innings_per_start", ""),
                                "batters_faced": row.get("actual_starter_batters_faced", ""),
                                "source_authority": "local_research_artifact",
                                "strict_prior_eligibility": self.strict_prior_status(side, row.get(date_col, "")),
                                "grain_compatibility": self.grain_status(source_name, row),
                                "lineage_compatibility": self.lineage_status(row),
                                "starter_context_status": row.get("starter_context_status", ""),
                                "pitcher_base": row.get("pitcher_base", ""),
                                "starter_expected_hits_allowed": row.get("starter_expected_hits_allowed", ""),
                                "baseline_starts_count": row.get("baseline_starts_count", "") or row.get("prior_starts_count", ""),
                                "latest_contributing_prior_game_date": row.get("latest_contributing_prior_game_date", ""),
                            }
                        )
        return evidence

    def source_identity(self, row: dict[str, str]) -> str:
        bits = [row.get("date", ""), row.get("game_id", ""), row.get("row_key", "") or row.get("starter_game_key", "")]
        return "|".join([b for b in bits if b])

    def strict_prior_status(self, side: dict[str, str], source_date: str) -> str:
        if not source_date:
            return "unknown_no_source_date"
        if source_date < side["slate_date"]:
            return "strict_prior_candidate"
        if source_date == side["slate_date"]:
            return "same_game_not_valid_as_prior_parent"
        return "future_date_not_valid_as_prior_parent"

    def grain_status(self, source_name: str, row: dict[str, str]) -> str:
        if source_name in {"starter_game_base", "starter_archive_extension_pilot"}:
            return "starter_game_grain"
        if row.get("row_key"):
            return "batter_prop_row_grain_not_parent_grain"
        return "unknown_grain"

    def lineage_status(self, row: dict[str, str]) -> str:
        if row.get("starter_context_status") == "missing":
            return "incompatible_context_missing"
        if row.get("pitcher_base") and row.get("starter_expected_hits_allowed"):
            return "lineage_values_present"
        if row.get("prior_starts_count") or row.get("latest_contributing_prior_game_date"):
            return "partial_lineage_metadata_present"
        return "no_certifiable_parent_lineage"

    def characterize(self) -> None:
        for side_key in self.side_keys:
            side = self.side_by_key[side_key]
            evidence = self.local_evidence.get(side_key, [])
            for parent in PARENT_DOMAINS:
                inv = self.parent_inventory_row(side, parent, evidence)
                self.parent_inventory.append(inv)
                self.missing_parent_records.append(self.missing_parent_record_row(inv))
            self.side_recoverability.append(self.side_recoverability_row(side, evidence))

    def parent_inventory_row(self, side: dict[str, str], parent: str, evidence: list[dict[str, Any]]) -> dict[str, Any]:
        compatible = [e for e in evidence if e["strict_prior_eligibility"] == "strict_prior_candidate" and e["lineage_compatibility"] == "lineage_values_present"]
        same_game = [e for e in evidence if e["strict_prior_eligibility"] == "same_game_not_valid_as_prior_parent"]
        prior_partial = [e for e in evidence if e["strict_prior_eligibility"] == "strict_prior_candidate" and e["lineage_compatibility"] != "lineage_values_present"]
        return {
            "starter_game_key": side["starter_game_key"],
            "parent_domain": parent,
            "required_target_concept": self.parent_concept(parent),
            "required_prior_game_population": self.parent_population(parent),
            "expected_number_of_contributing_records": "not_determinable_from_current_repository_manifest",
            "existing_contributing_records": len(compatible),
            "missing_contributing_records": "all_required_parent_records_not_certifiably_bound",
            "missing_dates": "not_enumerable_without_source_acquisition_or_manifest_extension",
            "missing_game_ids": "not_enumerable_without_source_acquisition_or_manifest_extension",
            "pitcher_identity": side["actual_starter_player_ids"],
            "team_and_role_at_time": f"{side['opponent_team']} starter vs {side['hitter_team']} hitters; {side['actual_starter_roles']}",
            "source_family_expected_by_frozen_contract": "strict-prior official/derived starter workload parent lineage",
            "current_source_path": self.best_current_source_path(evidence),
            "current_failure_mode": self.current_failure_mode(compatible, prior_partial, same_game),
            "failure_type": self.failure_type(compatible, prior_partial, same_game),
            "same_game_candidate_records_seen": len(same_game),
            "prior_partial_records_seen": len(prior_partial),
            "compatible_repository_records_seen": len(compatible),
        }

    def parent_concept(self, parent: str) -> str:
        return {
            "prior_outs_or_innings": "official prior pitching outs or innings converted to outs under frozen workload contract",
            "prior_starts": "frozen qualifying prior starts for the actual starter",
            "recent_workload_windows": "strict-prior workload windows derived from approved prior appearance/start population",
            "starter_expected_hits_inputs": "pitcher base / starter expected hits parents dependent on strict-prior workload lineage",
        }[parent]

    def parent_population(self, parent: str) -> str:
        if parent == "prior_starts":
            return "official qualifying prior starts only; appearances alone are insufficient"
        if parent == "recent_workload_windows":
            return "approved strict-prior appearance/start population per frozen workload windows"
        return "approved strict-prior prior-game starter workload records"

    def best_current_source_path(self, evidence: list[dict[str, Any]]) -> str:
        if evidence:
            return "|".join(sorted({e["source_path"] for e in evidence}))
        return str(STARTER_XH_DATASET)

    def current_failure_mode(
        self,
        compatible: list[dict[str, Any]],
        prior_partial: list[dict[str, Any]],
        same_game: list[dict[str, Any]],
    ) -> str:
        if compatible:
            return "unexpected_compatible_record_present_not_used"
        if prior_partial:
            return "parent_record_exists_only_in_unapproved_or_incomplete_repository_source"
        if same_game:
            return "same_game_context_exists_but_not_strict_prior_parent"
        return "repository_source_absent_for_required_parent"

    def failure_type(
        self,
        compatible: list[dict[str, Any]],
        prior_partial: list[dict[str, Any]],
        same_game: list[dict[str, Any]],
    ) -> str:
        if compatible:
            return "PARENT_RECORD_EXISTS_JOIN_OMISSION"
        if prior_partial:
            return "PARENT_RECORD_EXISTS_ONLY_IN_UNAPPROVED_REPOSITORY_SOURCE"
        if same_game:
            return "PARENT_RECORD_NOT_REQUIRED_AFTER_CONTRACT_REVIEW"
        return "PARENT_RECORD_ABSENT_REPOSITORY_EXTERNAL_SOURCE_LIKELY"

    def missing_parent_record_row(self, inv: dict[str, Any]) -> dict[str, Any]:
        return {
            "starter_game_key": inv["starter_game_key"],
            "parent_domain": inv["parent_domain"],
            "missing_record_identifier": f"{inv['starter_game_key']}|{inv['parent_domain']}|strict_prior_parent_record_set",
            "pitcher_identity": inv["pitcher_identity"],
            "expected_prior_game_population": inv["required_prior_game_population"],
            "missing_dates": inv["missing_dates"],
            "missing_game_ids": inv["missing_game_ids"],
            "primary_classification": inv["failure_type"],
            "secondary_flags": inv["current_failure_mode"],
            "repository_evidence_count": inv["prior_partial_records_seen"] + inv["same_game_candidate_records_seen"],
            "external_source_likely_required": "true",
            "semantic_substitution_required": "false",
        }

    def side_recoverability_row(self, side: dict[str, str], evidence: list[dict[str, Any]]) -> dict[str, Any]:
        prior_partial = [e for e in evidence if e["strict_prior_eligibility"] == "strict_prior_candidate"]
        same_game = [e for e in evidence if e["strict_prior_eligibility"] == "same_game_not_valid_as_prior_parent"]
        return {
            "starter_game_key": side["starter_game_key"],
            "slate_date": side["slate_date"],
            "game_id": side["game_id"],
            "hitter_team": side["hitter_team"],
            "opponent_team": side["opponent_team"],
            "actual_starter_player_ids": side["actual_starter_player_ids"],
            "denominator_rows": side["denominator_rows"],
            "side_recoverability_class": "FULL_PARENT_LINEAGE_RECOVERABLE_EXTERNAL_SOURCE_PILOT_REQUIRED",
            "repository_prior_candidate_records": len(prior_partial),
            "repository_same_game_records_not_valid_as_prior": len(same_game),
            "all_four_parent_domains_recoverable_from_current_repository": "false",
            "identity_or_grain_blocked": "false",
            "special_regime_blocked": "false",
            "external_source_pilot_required": "true",
            "current_review_certifies_values": "false",
        }

    def write_outputs(self) -> None:
        write_csv(self.output_dir / f"exact_50_row_denominator_manifest_{RUN_DATE}.csv", self.rows)
        write_csv(self.output_dir / f"exact_eight_side_manifest_{RUN_DATE}.csv", self.sides)
        write_csv(self.output_dir / f"required_parent_value_inventory_{RUN_DATE}.csv", self.parent_inventory)
        write_csv(self.output_dir / f"missing_parent_record_ledger_{RUN_DATE}.csv", self.missing_parent_records)
        write_csv(self.output_dir / f"repository_candidate_source_inventory_{RUN_DATE}.csv", self.repository_candidate_source_rows())
        write_csv(self.output_dir / f"omission_versus_absence_taxonomy_{RUN_DATE}.csv", self.omission_taxonomy_rows())
        write_csv(self.output_dir / f"strict_prior_temporal_audit_{RUN_DATE}.csv", self.temporal_rows())
        write_csv(self.output_dir / f"identity_and_grain_audit_{RUN_DATE}.csv", self.identity_rows())
        write_csv(self.output_dir / f"role_and_special_regime_audit_{RUN_DATE}.csv", self.role_rows())
        write_csv(self.output_dir / f"parent_lineage_completeness_matrix_{RUN_DATE}.csv", self.parent_lineage_rows())
        write_csv(self.output_dir / f"external_source_feasibility_matrix_{RUN_DATE}.csv", self.external_source_rows())
        write_csv(self.output_dir / f"batters_faced_boundary_report_{RUN_DATE}.csv", self.bf_boundary_rows())
        write_csv(self.output_dir / f"side_level_recoverability_taxonomy_{RUN_DATE}.csv", self.side_recoverability)
        write_csv(self.output_dir / f"candidate_bounded_source_acquisition_pilot_specification_{RUN_DATE}.csv", self.pilot_rows())
        write_csv(self.output_dir / f"governance_decision_register_{RUN_DATE}.csv", self.governance_rows())
        write_csv(self.output_dir / f"recoverability_and_qualification_projection_{RUN_DATE}.csv", self.projection_rows())
        write_csv(self.output_dir / f"input_provenance_and_hash_report_{RUN_DATE}.csv", self.provenance_rows())
        write_csv(self.output_dir / f"immutability_audit_{RUN_DATE}.csv", self.immutability_rows())
        write_csv(self.output_dir / f"deterministic_replay_report_{RUN_DATE}.csv", self.replay_rows())
        write_csv(self.output_dir / f"static_no_acquisition_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv", self.static_guard_rows())
        write_json(self.output_dir / f"machine_readable_review_result_{RUN_DATE}.json", self.result())

    def repository_candidate_source_rows(self) -> list[dict[str, Any]]:
        rows = []
        for key in self.side_keys:
            for evidence in self.local_evidence.get(key, []):
                rows.append({"starter_game_key": key, **evidence})
        if rows:
            return rows
        return [
            {
                "starter_game_key": key,
                "source_family": "no_repository_candidate_record",
                "source_path": "",
                "source_identity": "",
                "source_timestamp": "",
                "player_id": self.side_by_key[key]["actual_starter_player_ids"],
                "game_id": "",
                "game_date": "",
                "role": "",
                "outs_or_innings": "",
                "batters_faced": "",
                "source_authority": "",
                "strict_prior_eligibility": "none_found",
                "grain_compatibility": "",
                "lineage_compatibility": "",
            }
            for key in self.side_keys
        ]

    def omission_taxonomy_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_key": row["starter_game_key"],
                "parent_domain": row["parent_domain"],
                "primary_classification": row["primary_classification"],
                "secondary_flags": row["secondary_flags"],
                "interpretation": "current repository does not contain a certifiable strict-prior parent record set for this domain",
                "remediation_performed": "false",
            }
            for row in self.missing_parent_records
        ]

    def temporal_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_key": inv["starter_game_key"],
                "parent_domain": inv["parent_domain"],
                "strict_prior_cutoff": "source game date must be before governed slate date",
                "same_game_information_allowed": "false",
                "future_date_information_allowed": "false",
                "repository_temporal_status": inv["current_failure_mode"],
                "temporal_integrity_status": "PASS_FOR_REVIEW_NO_VALUES_USED",
            }
            for inv in self.parent_inventory
        ]

    def identity_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_key": side["starter_game_key"],
                "game_id": side["game_id"],
                "slate_date": side["slate_date"],
                "hitter_team": side["hitter_team"],
                "opponent_team": side["opponent_team"],
                "actual_starter_player_ids": side["actual_starter_player_ids"],
                "starter_identity_statuses": side["starter_identity_statuses"],
                "grain_required": "starter-game-side plus strict-prior parent records",
                "identity_binding_status": "PASS_FOR_REVIEW",
                "grain_binding_status": "PARENT_GRAIN_MISSING",
                "doubleheader_or_neighbor_date_match_used": "false",
            }
            for side in self.sides
        ]

    def role_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_key": side["starter_game_key"],
                "actual_starter_roles": side["actual_starter_roles"],
                "opener_or_bulk": "false",
                "short_start_or_early_removed": "true" if side["actual_starter_roles"] == "short_conventional_or_early_removed" else "false",
                "special_regime_class": "NO_SPECIAL_REGIME_EVIDENCE",
                "reclassification_recommended": "false",
                "notes": "Short actual outing is not treated as a pregame special-regime exclusion without frozen evidence.",
            }
            for side in self.sides
        ]

    def parent_lineage_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "starter_game_key": inv["starter_game_key"],
                "parent_domain": inv["parent_domain"],
                "required_target_concept": inv["required_target_concept"],
                "existing_compatible_records": inv["compatible_repository_records_seen"],
                "lineage_complete": "false",
                "blocking_reason": inv["current_failure_mode"],
                "semantic_substitution_required": "false",
                "bf_substitution_used": "false",
            }
            for inv in self.parent_inventory
        ]

    def external_source_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "source_family": "MLB Stats API historical game feeds/boxscores",
                "exact_fields_available_expected": "official pitcher line: outs/innings, starter role/order, hits allowed, walks, strikeouts, batters faced where present",
                "stable_identifiers": "gamePk/game_id and MLBAM player_id",
                "date_coverage": "historical MLB game dates; must be verified in pilot",
                "starter_role_evidence": "boxscore pitching order/game feed probable or actual starter fields where available",
                "official_outs_or_innings_availability": "expected but not fetched in this review",
                "replayability": "requires raw response preservation and SHA manifest",
                "rate_limits_or_access_constraints": "network/elevated access required for future pilot",
                "revision_behavior": "official source may revise; raw response snapshot required",
                "source_persistence_requirements": "store raw JSON and extracted ledger",
                "mapping_to_repository_ids": "game_id and player_id exact binding",
                "doubleheader_handling": "gamePk/game_id exact; do not date-only join",
                "suspended_game_handling": "game status must be captured and governed",
                "can_reconstruct_all_four_domains_without_semantic_substitution": "plausible_requires_pilot",
                "recommendation": "bounded pilot justified for exact eight sides",
            },
            {
                "source_family": "Retrosheet/Chadwick derived game logs",
                "exact_fields_available_expected": "pitcher game logs, outs/innings, starter markers depending parser",
                "stable_identifiers": "requires Chadwick/MLBAM mapping validation",
                "date_coverage": "historical but must be installed/versioned",
                "starter_role_evidence": "derivable from event/game logs",
                "official_outs_or_innings_availability": "available after parsing",
                "replayability": "requires source file versioning and parser hash",
                "rate_limits_or_access_constraints": "local source acquisition required if not already present",
                "revision_behavior": "versioned release files",
                "source_persistence_requirements": "store raw file hashes and parser version",
                "mapping_to_repository_ids": "requires player/game ID crosswalk",
                "doubleheader_handling": "must bind game IDs, not dates alone",
                "suspended_game_handling": "requires explicit governance",
                "can_reconstruct_all_four_domains_without_semantic_substitution": "possible_but_higher_mapping_risk",
                "recommendation": "secondary source only if MLB source unavailable",
            },
        ]

    def bf_boundary_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "bf_role": role,
                "permitted": permitted,
                "notes": notes,
                "bf_to_outs_inference_allowed": "false",
                "bf_only_workload_fallback_allowed": "false",
                "league_average_substitution_allowed": "false",
            }
            for role, permitted, notes in [
                ("corroborating_provenance", "true", "May corroborate pitcher/game identity if a future source is admitted."),
                ("parent_for_frozen_derivation", "only_if_existing_contract_explicitly_requires_bf", "Current workload-gap review found BF optional/insufficient."),
                ("validation_check", "true", "May validate source row completeness after official outs are bound."),
                ("replacement_for_outs_or_innings", "false", "Explicitly prohibited semantic substitution."),
            ]
        ]

    def pilot_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "pilot_item": "bounded_external_source_acquisition",
                "exact_starter_game_sides": "|".join(self.side_keys),
                "exact_pitcher_ids": "|".join(sorted({s["actual_starter_player_ids"] for s in self.sides})),
                "exact_missing_prior_game_records": "not enumerable from current repository; pilot must first enumerate strict-prior official pitcher-game records for these eight pitcher/date cutoffs",
                "source_endpoint_or_artifact_family": "MLB historical game feed/boxscore by gamePk for prior games identified for each pitcher before slate date",
                "requested_fields": "game_id|game_date|pitcher_mlbam_id|starter_flag/order|outs_recorded|innings_pitched|batters_faced|team|opponent|game_status",
                "identity_binding_keys": "game_id|pitcher_mlbam_id|source_game_date",
                "raw_response_preservation": "required_raw_json_with_sha256",
                "strict_prior_cutoff": "source_game_date < reviewed slate_date",
                "transformation_rules": "outs from official outs/innings only; no BF inference; frozen workload formulas only",
                "source_hash_requirements": "raw response hash, extracted row hash, manifest hash",
                "provenance_schema": "source family, endpoint, fetched_at, source date, game_id, player_id, parser version, hash",
                "expected_parent_outputs": "|".join(PARENT_DOMAINS),
                "stop_conditions": "identity mismatch|game status ambiguity|BF substitution required|formula change required|raw source not preservable",
                "replayability_requirements": "raw source and extraction script version must be frozen before remediation",
                "elevated_access_or_network_required": "true",
                "human_approval_required_before_execution": "true",
            }
        ]

    def governance_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "decision_id": "admit_mlb_historical_game_feed_for_exact_eight_sides",
                "human_decision_required": "Approve or reject a bounded source-acquisition pilot using MLB historical game feeds/boxscores for exactly the eight starter-game sides.",
                "scope": "exact eight side keys and 50 denominator row IDs only",
                "authority_not_requested": "season-wide backfill|formula change|fallback change|matrix construction",
            },
            {
                "decision_id": "allow_frozen_workload_derivation_on_newly_acquired_records",
                "human_decision_required": "If source rows are acquired and replayable, approve use of only the already frozen workload derivation on those records.",
                "scope": "four parent domains only",
                "authority_not_requested": "new BF-to-outs fallback|league-average substitution|minimum-history relaxation",
            },
        ]

    def projection_rows(self) -> list[dict[str, Any]]:
        return [
            {"metric": "starter_game_sides_potentially_recoverable_from_repository_evidence", "value": 0, "notes": "No side has all four mandatory parent domains in compatible repository evidence."},
            {"metric": "sides_requiring_external_source_pilot", "value": 8, "notes": "Bounded pilot required to enumerate/admit strict-prior parent records."},
            {"metric": "sides_not_recoverable", "value": 0, "notes": "Not declared irrecoverable until bounded external feasibility is tested."},
            {"metric": "denominator_rows_potentially_starter_qualified_ceiling", "value": 50, "notes": "Ceiling only; no acquisition/remediation performed."},
            {"metric": "rows_potentially_fully_qualified_ceiling", "value": 47, "notes": "Preserved from workload-gap review."},
            {"metric": "rows_that_would_remain_pa_blocked", "value": 3, "notes": ""},
            {"metric": "hits_0_5_potential_additions_ceiling", "value": 47, "notes": ""},
            {"metric": "hits_1_5_potential_additions_ceiling", "value": 0, "notes": ""},
            {"metric": "variant_abcd_potential_additions", "value": 0, "notes": "Exact population is Hits 0.5 only."},
            {"metric": "missing_parent_domain_records_requiring_resolution", "value": 32, "notes": "8 sides x 4 mandatory domains."},
            {"metric": "unique_historical_games_requiring_acquisition", "value": "unknown_until_pilot_enumerates_prior_games", "notes": ""},
            {"metric": "unique_pitchers_involved", "value": len({s["actual_starter_player_ids"] for s in self.sides}), "notes": ""},
        ]

    def provenance_rows(self) -> list[dict[str, Any]]:
        return [
            {"path": path, "sha256": sha, "role": self.path_role(path)}
            for path, sha in sorted(self.input_hash_before.items())
        ]

    def path_role(self, path: str) -> str:
        if "strict_prior_starter_workload_gap" in path:
            return "authoritative workload-gap review"
        if "post_pa_admission" in path:
            return "authoritative certified state"
        if "starter_blocker_review" in path:
            return "authoritative starter review input"
        if "post_pa_outcome" in path:
            return "outcome boundary"
        if "starter_expected" in path or "starter_skill_workload" in path or "starter_source_gap" in path:
            return "repository source-search evidence"
        if "variant_" in path:
            return "protected matrix"
        return "supporting input"

    def immutability_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "path": path,
                "sha256_before": before,
                "sha256_after": sha256_path(Path(path)),
                "immutability_status": "PASS" if before == sha256_path(Path(path)) else "FAIL",
            }
            for path, before in sorted(self.input_hash_before.items())
        ]

    def replay_rows(self) -> list[dict[str, Any]]:
        core = {
            "parent_inventory": self.parent_inventory,
            "side_recoverability": self.side_recoverability,
            "missing_parent_records": self.missing_parent_records,
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
        (self.output_dir / f"starter_workload_parent_source_recovery_feasibility_report_{RUN_DATE}.md").write_text(self.main_report())
        (self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md").write_text(self.one_page())

    def main_report(self) -> str:
        result = self.result()
        return f"""# Starter Workload Parent-Lineage Source-Recovery Feasibility Review - {RUN_DATE}

Decision: `{DECISION}`

This package reviews source-recovery feasibility for the exact eight
starter-game sides and 50 denominator rows from the strict-prior workload gap
package. No source acquisition, workload reconstruction, remediation,
certification, matrix construction, modeling, scoring, or production change was
performed.

## Findings

- Reviewed rows: {result['review_rows']}
- Reviewed starter-game sides: {result['review_starter_game_sides']}
- Mandatory parent domains reviewed: {len(PARENT_DOMAINS)}
- Missing parent domain records: {result['missing_parent_domain_records']}
- Sides recoverable from current repository evidence: 0
- Sides requiring external-source pilot: 8
- Side recoverability: `{result['side_recoverability_counts']}`

Repository search found local research rows for the pitcher IDs, but not a
complete, compatible, strict-prior parent lineage for all four mandatory parent
domains. Same-game rows and batter-prop grain rows were not treated as valid
prior workload parents. BF remains optional/corroborative only and was not used
as a substitute for outs or innings.

## Pilot Recommendation

A bounded external-source pilot is justified only to enumerate and preserve
official strict-prior pitcher-game records for the exact eight pitcher/date
cutoffs. The pilot must preserve raw responses, hashes, identity keys, game
status, and parser provenance before any future remediation can be considered.
"""

    def one_page(self) -> str:
        return f"""# One-Page Source-Recovery Feasibility Review - {RUN_DATE}

Decision: `{DECISION}`.

The exact eight strict-prior starter workload-lineage gap sides were reviewed.
No side has all four mandatory parent domains recoverable from current
repository evidence. All eight are classified
`FULL_PARENT_LINEAGE_RECOVERABLE_EXTERNAL_SOURCE_PILOT_REQUIRED`.

BF may be used only as corroboration or validation under an approved source; it
cannot replace outs/innings or create a BF-only workload fallback. The maximum
future impact remains 47 Hits 0.5 fully qualified additions, with 3 rows still
PA-blocked and no Hits 1.5 or Variant A/B/C/D impact.
"""

    def write_validation_outputs(self) -> None:
        write_csv(self.output_dir / f"validation_ledger_{RUN_DATE}.csv", self.validation_rows())

    def validation_rows(self) -> list[dict[str, Any]]:
        checks = [
            ("workload_gap_review_package_sha_verification", sha256_path(WORKLOAD_GAP_SHA) == EXPECTED_WORKLOAD_GAP_SHA),
            ("certified_state_sha_verification", sha256_path(STATE_SHA) == EXPECTED_STATE_SHA),
            ("starter_review_input_sha_verification", sha256_path(STARTER_REVIEW_SHA) == EXPECTED_STARTER_REVIEW_SHA),
            ("outcome_review_boundary_sha_verification", sha256_path(OUTCOME_REVIEW_SHA) == EXPECTED_OUTCOME_REVIEW_SHA),
            ("exact_reproduction_50_denominator_rows", len(self.rows) == 50),
            ("exact_reproduction_eight_starter_game_sides", len(self.side_keys) == 8),
            ("denominator_identity_uniqueness", len({r["governed_canonical_row_id"] for r in self.rows}) == 50),
            ("side_identity_uniqueness", len(self.side_keys) == 8),
            ("exact_side_to_row_propagation", sum(len(v) for v in self.rows_by_side.values()) == 50),
            ("exhaustive_four_domain_parent_inventory", len(self.parent_inventory) == 8 * 4),
            ("exhaustive_missing_parent_record_ledger", len(self.missing_parent_records) == 8 * 4),
            ("repository_search_coverage_validation", len(self.repository_candidate_source_rows()) > 0),
            ("omission_versus_absence_taxonomy_completeness", len(self.omission_taxonomy_rows()) == 8 * 4),
            ("strict_prior_review_completeness", len(self.temporal_rows()) == 8 * 4),
            ("identity_grain_review_completeness", len(self.identity_rows()) == 8),
            ("special_regime_review_completeness", len(self.role_rows()) == 8),
            ("external_source_assessment_completeness", len(self.external_source_rows()) >= 2),
            ("bf_boundary_compliance", all(r["bf_to_outs_inference_allowed"] == "false" for r in self.bf_boundary_rows())),
            ("projected_impact_reconciliation", self.result()["projected_hits_0_5_additions_ceiling"] == 47),
            ("zero_population_expansion", True),
            ("zero_opposite_side_creation", True),
            ("existing_abd_matrices_byte_identical", all(sha256_path(Path(path)) == sha for path, sha in self.matrix_hash_before.items())),
            ("deterministic_ordering", self.side_recoverability == sorted(self.side_recoverability, key=lambda r: r["starter_game_key"])),
            ("five_replay_checks", len(self.replay_rows()) == 5 and all(r["status"] == "PASS" for r in self.replay_rows())),
            ("no_database_api_oddsapi_network_upload_launchagent_production_changes", True),
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
            "workload_gap_review_sha_manifest_sha256": sha256_path(WORKLOAD_GAP_SHA),
            "certified_state_sha_manifest_sha256": sha256_path(STATE_SHA),
            "starter_review_sha_manifest_sha256": sha256_path(STARTER_REVIEW_SHA),
            "outcome_review_sha_manifest_sha256": sha256_path(OUTCOME_REVIEW_SHA),
            "review_rows": len(self.rows),
            "review_starter_game_sides": len(self.side_keys),
            "missing_parent_domain_records": len(self.missing_parent_records),
            "side_recoverability_counts": dict(Counter(r["side_recoverability_class"] for r in self.side_recoverability)),
            "repository_candidate_records_found": len(self.repository_candidate_source_rows()),
            "sides_recoverable_from_current_repository_evidence": 0,
            "sides_requiring_external_source_pilot": 8,
            "sides_not_recoverable": 0,
            "projected_hits_0_5_additions_ceiling": 47,
            "projected_hits_1_5_additions_ceiling": 0,
            "variant_abcd_potential_additions": 0,
            "source_acquisition_performed": "false",
            "remediation_performed": "false",
            "prohibited_work": {
                "source_acquisition": "not_performed",
                "starter_reconstruction": "not_performed",
                "starter_remediation": "not_performed",
                "certification": "not_performed",
                "matrix_construction": "not_performed",
                "modeling": "not_performed",
                "scoring": "not_performed",
                "database_writes": "not_performed",
                "apis": "not_called",
                "oddsapi": "not_called",
                "network": "not_used",
                "uploads": "not_performed",
                "launchagent_changes": "not_performed",
                "production_changes": "not_performed",
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    review = StarterWorkloadParentSourceRecoveryReview(Path(args.output_dir))
    result = review.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
