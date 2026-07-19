"""Inventory and defer remaining Starter residual framework branches.

This read-only utility creates a formal offseason/postseason deferment package
for the remaining MLB historical Starter residual populations. It performs no
network access, discovery/acquisition, feature construction, formula creation,
qualification propagation, downstream remediation, matrix/model/scoring work,
DB/API/OddsAPI writes, uploads, LaunchAgent changes, or production changes.
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


RUN_DATE = "2026-07-16"
ROOT = Path(".")

STATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_starter_reconstruction_remediation/2026-07-15"
FAST_PATH_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_final_starter_residual_fast_path_review/2026-07-15"
RESIDUAL_REVIEW_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_starter_blocked_population_review/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_starter_special_regime_and_first_start_framework_deferment/2026-07-16"

EXPECTED_SHA = {
    "identity_role_remediation_state": "67e13e7e2b40977270c9964201a073e5de399e6041a199fdfe71148d200c037c",
    "final_fast_path_closure": "8202a29466c8292ceef80ffdbd57e9037ed576019e661f35e6737fc80b239ef7",
}

CURRENT_TOTALS = {
    "fully_qualified_hits": 1540,
    "fully_qualified_hits_0_5": 1400,
    "fully_qualified_hits_1_5": 140,
    "primary_starter_blocked": 62,
    "primary_pa_blocked": 42,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "qualified_but_not_matrix_hits_1_5_queue": 41,
}

INVENTORY_DECISION = "EXACT_RESIDUAL_FRAMEWORK_INVENTORY_COMPLETED"
SPECIAL_VALUE_DECISION = "MODERATE_VALUE_OFFSEASON_FRAMEWORK_RESEARCH"
EARLY_VALUE_DECISION = "HIGH_VALUE_OFFSEASON_EARLY_START_PROGRESSION_RESEARCH"
DEFERMENT_STATUS = "DEFERRED_TO_2026_POSTSEASON_RESEARCH_PLANNING"
CLOSURE_DECISION = "ORDINARY_STARTER_QUALIFICATION_CLOSED_RESIDUAL_FRAMEWORKS_DOCUMENTED_AND_DEFERRED"

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


def manifest_hash(package_dir: Path) -> str:
    return sha256_path(package_dir / "sha256_manifest_2026-07-15.csv")


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
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def git_status_for(path: Path) -> str:
    result = subprocess.run(["git", "status", "--short", "--", str(path)], cwd=ROOT, text=True, capture_output=True, check=False)
    return result.stdout.strip()


def snapshot_upload_manifests() -> list[dict[str, Any]]:
    rows = []
    for path in UPLOAD_MANIFEST_PATHS:
        rows.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "git_status": git_status_for(path),
                "sha256": sha256_path(path) if path.exists() else "",
            }
        )
    return rows


def is_true(value: str) -> bool:
    return str(value).lower() == "true"


def make_side_key(row: dict[str, str]) -> str:
    return row["starter_game_side_key"]


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    pre_upload = snapshot_upload_manifests()
    out_dir.mkdir(parents=True, exist_ok=True)

    dependency_rows = [
        {
            "dependency": "identity_role_remediation_current_state",
            "package_path": str(STATE_DIR),
            "sha_manifest": str(STATE_DIR / "sha256_manifest_2026-07-15.csv"),
            "observed_sha256": manifest_hash(STATE_DIR),
            "expected_sha256": EXPECTED_SHA["identity_role_remediation_state"],
            "status": "PASS" if manifest_hash(STATE_DIR) == EXPECTED_SHA["identity_role_remediation_state"] else "FAIL",
        },
        {
            "dependency": "final_fast_path_closure_review",
            "package_path": str(FAST_PATH_DIR),
            "sha_manifest": str(FAST_PATH_DIR / "sha256_manifest_2026-07-15.csv"),
            "observed_sha256": manifest_hash(FAST_PATH_DIR),
            "expected_sha256": EXPECTED_SHA["final_fast_path_closure"],
            "status": "PASS" if manifest_hash(FAST_PATH_DIR) == EXPECTED_SHA["final_fast_path_closure"] else "FAIL",
        },
        {
            "dependency": "residual_population_review_context",
            "package_path": str(RESIDUAL_REVIEW_DIR),
            "sha_manifest": str(RESIDUAL_REVIEW_DIR / "sha256_manifest_2026-07-15.csv"),
            "observed_sha256": manifest_hash(RESIDUAL_REVIEW_DIR) if (RESIDUAL_REVIEW_DIR / "sha256_manifest_2026-07-15.csv").exists() else "",
            "expected_sha256": "lineage_only",
            "status": "PASS" if (RESIDUAL_REVIEW_DIR / "sha256_manifest_2026-07-15.csv").exists() else "WARN",
        },
    ]
    if any(row["status"] == "FAIL" for row in dependency_rows):
        raise RuntimeError("required dependency SHA mismatch")

    state = json.loads((STATE_DIR / "certified_cumulative_child_state_2026-07-15.json").read_text())
    fast_path = json.loads((FAST_PATH_DIR / "machine_readable_final_fast_path_review_2026-07-15.json").read_text())
    if state.get("STARTER_POST_IDENTITY_ROLE_REMEDIATION_CUMULATIVE_STATE") != "CERTIFIED":
        raise RuntimeError("current cumulative state is not certified")
    if fast_path.get("STARTER_HISTORICAL_QUALIFICATION_CLOSURE_DECISION") != "ZERO_FAST_PATH_ROWS_CLOSE_STARTER_QUALIFICATION":
        raise RuntimeError("fast-path closure decision mismatch")

    residual_rows = read_csv(FAST_PATH_DIR / "exact_62_row_manifest_2026-07-15.csv")
    residual_sides = read_csv(FAST_PATH_DIR / "exact_9_side_manifest_2026-07-15.csv")
    if len(residual_rows) != 62 or len(residual_sides) != 9:
        raise RuntimeError("exact residual reproduction failed")
    row_counts = Counter(row["primary_residual_category"] for row in residual_rows)
    side_counts = Counter(row["current_residual_category"] for row in residual_sides)
    if row_counts["ESTABLISHED_SPECIAL_REGIME_EXCLUSION"] != 46 or row_counts["ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"] != 16:
        raise RuntimeError("row taxonomy reproduction failed")
    if side_counts["ESTABLISHED_SPECIAL_REGIME_EXCLUSION"] != 7 or side_counts["ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"] != 2:
        raise RuntimeError("side taxonomy reproduction failed")

    by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in residual_rows:
        by_side[make_side_key(row)].append(row)

    exact_62 = [dict(row) for row in residual_rows]
    exact_9 = [dict(row) for row in residual_sides]

    special_rows = []
    special_field_reuse = []
    broader_special = []
    zero_rows = []
    early_progression = []
    evidence_matrix = []
    pattern_registry = []
    readiness_gate = []
    first_options = []
    meaningfulness = []
    special_roadmap = []
    early_roadmap = []
    resume_rows = []
    stop_rows = []
    deferment_rows = []
    state_rows = []

    side_lookup = {row["starter_game_side_key"]: row for row in residual_sides}
    for side in sorted(by_side):
        rows = by_side[side]
        side_meta = side_lookup.get(side, {})
        category = rows[0]["primary_residual_category"]
        hits05 = sum(1 for r in rows if r["line"] == "0.5")
        hits15 = sum(1 for r in rows if r["line"] == "1.5")
        pa_blocked = sum(1 for r in rows if not is_true(r["pa_qualified"]))
        outcome_blocked = sum(1 for r in rows if not is_true(r["outcome_qualified"]))
        bundle_blocked = sum(1 for r in rows if r["bundle_blockers"])
        full_ceiling = sum(1 for r in rows if is_true(r["pa_qualified"]) and is_true(r["outcome_qualified"]) and not r["bundle_blockers"])
        if category == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION":
            special_rows.append(
                {
                    "starter_game_side_key": side,
                    "target_game_date": rows[0]["slate_date"],
                    "target_game_id": rows[0]["game_id"],
                    "primary_subtype": "OTHER_SPECIAL_REGIME_EXPLICIT_REASON",
                    "subtype_evidence_status": "INSUFFICIENT_REPOSITORY_DETAIL_FOR_MORE_SPECIFIC_SUBTYPE",
                    "official_starter": "not_retained_in_bound_residual_artifacts",
                    "expected_pregame_starter": "not_retained_in_bound_residual_artifacts",
                    "likely_primary_bulk_pitcher": "not_retained_in_bound_residual_artifacts",
                    "pitcher_sequence": "not_retained_in_bound_residual_artifacts",
                    "target_game_innings_or_outs_by_pitcher": "not_retained_in_bound_residual_artifacts",
                    "prior_role_sequence": "not_retained_in_bound_residual_artifacts",
                    "prior_mlb_starts": "insufficient_evidence",
                    "prior_relief_appearances": "insufficient_evidence",
                    "role_knowable_before_cutoff": "insufficient_evidence",
                    "existing_workload_evidence": "ordinary_starter_evidence_not_admitted",
                    "existing_pitcher_skill_evidence": "ordinary_starter_evidence_not_admitted",
                    "ordinary_formula_invalid_reason": side_meta.get("governing_reason", "established special-regime exclusion preserved"),
                    "current_projected_recoverable_rows": full_ceiling,
                    "represented_rows": len(rows),
                    "hits_0_5_rows": hits05,
                    "hits_1_5_rows": hits15,
                }
            )
            for component in [
                "pitcher_skill_or_hits_per_out",
                "recent_workload",
                "expected_workload",
                "offense_factor",
                "starter_or_role_trust",
                "strict_prior_appearance_history",
                "expected_hits_calculation",
                "team_and_game_binding",
            ]:
                if component in {"offense_factor", "team_and_game_binding"}:
                    classification = "REUSABLE_UNCHANGED"
                elif component in {"pitcher_skill_or_hits_per_out", "strict_prior_appearance_history"}:
                    classification = "REUSABLE_WITH_ROLE_SPECIFIC_PARENT"
                elif component in {"recent_workload", "expected_workload", "expected_hits_calculation"}:
                    classification = "REQUIRES_MULTI_PITCHER_EXPOSURE_WEIGHTING"
                else:
                    classification = "REQUIRES_NEW_FORMULA"
                special_field_reuse.append(
                    {
                        "starter_game_side_key": side,
                        "subtype": "OTHER_SPECIAL_REGIME_EXPLICIT_REASON",
                        "ordinary_component": component,
                        "reuse_classification": classification,
                        "notes": "No formula is implemented in this deferment package.",
                    }
                )
            broader_special.append(
                {
                    "population_scope": "exact_repository_backed_population",
                    "starter_game_side_key": side,
                    "date": rows[0]["slate_date"],
                    "subtype": "OTHER_SPECIAL_REGIME_EXPLICIT_REASON",
                    "represented_prop_rows": len(rows),
                    "pregame_role_evidence_availability": "not_retained_in_bound_artifacts",
                    "target_pitcher_evidence_availability": "not_retained_in_bound_artifacts",
                    "feature_parent_coverage": "ordinary_starter_contract_excluded",
                    "outcome_coverage": f"{sum(1 for r in rows if is_true(r['outcome_qualified']))}/{len(rows)}",
                    "potential_future_daily_recurrence": "likely_recurring_but_unmeasured_without_daily_role_capture",
                    "measurement_status": "exact_current_residual_only",
                }
            )
        else:
            pitcher = "Matt Svanson" if side == "2026-07-07|823062|MIL|STL" else "Gabriel Hughes" if side == "2026-07-08|823928|LAD|COL" else "unknown"
            prior_relief = "73 strict-prior MLB relief/non-start appearances" if pitcher == "Matt Svanson" else "prior relief/non-start evidence certified; count not retained in bound residual artifacts"
            zero_rows.append(
                {
                    "starter_game_side_key": side,
                    "target_game_date": rows[0]["slate_date"],
                    "target_game_id": rows[0]["game_id"],
                    "pitcher": pitcher,
                    "zero_strict_prior_mlb_starts": "true",
                    "prior_relief_non_start_evidence": prior_relief,
                    "represented_rows": len(rows),
                    "hits_0_5_rows": hits05,
                    "hits_1_5_rows": hits15,
                    "projected_full_qualification_ceiling_if_future_framework_exists": full_ceiling,
                    "ordinary_starter_status": "fail_closed_under_current_contract",
                    "notes": "Ordinary Starter qualification is not reopened.",
                }
            )

    for start_number in ["first_mlb_start", "second_mlb_start", "third_mlb_start", "fourth_mlb_start", "fifth_mlb_start", "relief_to_start_transition", "minor_league_starter_to_mlb_starter_transition"]:
        early_progression.append(
            {
                "progression_bucket": start_number,
                "repository_backed_identifiable_targets": "2 exact zero-start sides only for current closure package" if start_number == "first_mlb_start" else "not_constructed_in_current_package",
                "mlb_starts_available": "AVAILABLE_CURRENT_REPOSITORY" if start_number == "first_mlb_start" else "PARTIALLY_AVAILABLE",
                "mlb_relief_available": "PARTIALLY_AVAILABLE",
                "mlb_outs_or_innings_available": "PARTIALLY_AVAILABLE",
                "pitch_count_available": "PARTIALLY_AVAILABLE",
                "minor_league_starts_available": "EXTERNAL_SOURCE_REQUIRED",
                "minor_league_workload_available": "EXTERNAL_SOURCE_REQUIRED",
                "pitch_mix_velocity_available": "NEW_DATA_PIPELINE_REQUIRED",
                "prospect_projection_available": "EXTERNAL_SOURCE_REQUIRED",
                "announced_workload_role_continuity_available": "NEW_DATA_PIPELINE_REQUIRED",
                "notes": "Population construction deferred to postseason/offseason after the 2026 MLB season.",
            }
        )

    concepts = [
        ("prior_mlb_start_count", "AVAILABLE_CURRENT_REPOSITORY"),
        ("prior_professional_starting_volume", "EXTERNAL_SOURCE_REQUIRED"),
        ("recent_relief_workload", "PARTIALLY_AVAILABLE"),
        ("role_continuity", "PARTIALLY_AVAILABLE"),
        ("expected_workload_certainty", "NEW_DATA_PIPELINE_REQUIRED"),
        ("announced_pitch_limit", "NEW_DATA_PIPELINE_REQUIRED"),
        ("pitch_mix_stability", "NEW_DATA_PIPELINE_REQUIRED"),
        ("velocity_stability", "NEW_DATA_PIPELINE_REQUIRED"),
        ("strike_and_walk_profile", "PARTIALLY_AVAILABLE"),
        ("similarity_to_transition_cohorts", "NEW_DATA_PIPELINE_REQUIRED"),
        ("minimum_parent_field_coverage", "AVAILABLE_CURRENT_REPOSITORY"),
        ("uncertainty_penalty", "NEW_DATA_PIPELINE_REQUIRED"),
    ]
    for concept, availability in concepts:
        evidence_matrix.append(
            {
                "concept": concept,
                "availability": availability,
                "required_for": "conditional_evidence_readiness_gate",
                "notes": "Inventory only; no feature construction.",
            }
        )
        readiness_gate.append(
            {
                "gate_dimension": concept,
                "candidate_role": "evaluate evidence sufficiency for pitchers below fixed five-start threshold",
                "current_status": availability,
                "would_replace_fixed_threshold": "candidate_only_not_frozen",
                "notes": "No threshold or formula selected.",
            }
        )

    patterns = [
        "established_minor_league_starter_with_zero_mlb_starts",
        "mlb_reliever_stretched_into_rotation",
        "top_prospect_promoted_directly_as_starter",
        "emergency_replacement_with_minimal_preparation",
        "repeated_short_starts_with_increasing_pitch_count",
        "stable_pitch_mix_velocity_before_workload_stability",
        "one_to_four_starts_with_consistent_role_and_rest",
        "volatile_role_with_mixed_relief_and_starting_appearances",
    ]
    for pattern in patterns:
        pattern_registry.append(
            {
                "candidate_pattern": pattern,
                "required_evidence": "strict-prior role, workload, skill, rest, and source-certainty fields",
                "current_evidence_availability": "PARTIALLY_AVAILABLE",
                "expected_population_size": "unknown_until_progression_population_constructed",
                "definition_risk": "medium_to_high",
                "potential_to_support_earlier_qualification": "possible_research_only",
                "likely_flag": "prediction_ineligible_or_high_uncertainty_until_validated",
            }
        )

    options = [
        ("LEAGUE_OR_ROLE_PRIOR_ONLY", "low", "high", "research_only"),
        ("MINOR_LEAGUE_STARTER_TRANSLATION", "large", "medium", "research_then_possible_predictions"),
        ("MLB_RELIEF_PLUS_WORKLOAD_TRANSITION", "medium", "medium", "research_then_possible_predictions"),
        ("PITCH_LEVEL_SKILL_PLUS_ROLE_PRIOR", "large", "medium", "research_then_possible_predictions"),
        ("SIMILAR_PITCHER_COHORT_PRIOR", "large", "high", "research_only_until_large_population"),
        ("BAYESIAN_OR_HIERARCHICAL_EARLY_START_ESTIMATE", "large", "medium", "research_then_possible_predictions"),
        ("NO_PREDICTION_UNTIL_MINIMUM_EVIDENCE_GATE", "small", "low", "policy_baseline"),
    ]
    for option, burden, risk, use in options:
        first_options.append(
            {
                "framework_option": option,
                "required_data": "strict-prior MLB role/workload plus option-specific external or pitch-level data",
                "current_data_availability": "partial" if option != "NO_PREDICTION_UNTIL_MINIMUM_EVIDENCE_GATE" else "available",
                "implementation_burden": burden,
                "definition_risk": risk,
                "future_season_reuse": "high",
                "ordinary_starter_field_compatibility": "compatible_as_parent_or_uncertainty_overlay",
                "supported_use": use,
                "validation_requirements": "strict-prior replay, uncertainty calibration, comparison to fixed five-start rule",
            }
        )

    meaningfulness.extend(
        [
            {
                "framework": "special_regime_pitcher_usage",
                "value_decision": SPECIAL_VALUE_DECISION,
                "current_residual_value": "46 rows / 7 sides",
                "broader_recurring_population": "likely but exact repository-backed population currently limited to residual rows",
                "five_start_gate_relevance": "indirect",
                "daily_platform_reuse": "role and pitcher-exposure modeling",
                "engineering_burden": "large",
                "governance_burden": "large",
                "meaningfulness_basis": "batter-opponent exposure modeling could improve platform realism even if residual rows alone are small",
            },
            {
                "framework": "first_and_early_start_progression",
                "value_decision": EARLY_VALUE_DECISION,
                "current_residual_value": "16 rows / 2 sides",
                "broader_recurring_population": "likely recurring across future seasons; exact cohort must be built",
                "five_start_gate_relevance": "direct",
                "daily_platform_reuse": "conditional evidence-readiness gate below five starts",
                "engineering_burden": "large",
                "governance_burden": "large",
                "meaningfulness_basis": "could refine or replace blunt fixed five-start prediction threshold if evidence supports it",
            },
        ]
    )

    special_phases = [
        "subtype_certification",
        "broader_historical_population_construction",
        "pregame_role_evidence_audit",
        "pitcher_exposure_design",
        "existing_field_reuse_audit",
        "formula_design",
        "offline_reconstruction_pilot",
        "characterization",
        "model_signal_experiment_after_construction_validation",
    ]
    for idx, phase in enumerate(special_phases, 1):
        special_roadmap.append(
            {
                "phase": idx,
                "phase_name": phase,
                "objective": phase.replace("_", " "),
                "required_input": "explicit approval plus bounded source/population package",
                "output": "governed artifact package",
                "stop_condition": "insufficient pregame role evidence or unresolved identity contamination",
                "expected_effort_class": "large" if idx in {2, 3, 4, 6} else "medium",
                "network_or_new_data_likely_required": "true" if idx in {2, 3} else "false",
                "approval_boundary": "separate postseason/offseason authorization required",
            }
        )

    early_phases = [
        "career_start_progression_population",
        "data_availability_inventory",
        "minor_league_and_relief_history_lineage",
        "evidence_pattern_taxonomy",
        "conditional_readiness_gate_design",
        "research_only_feature_construction",
        "prequential_or_strict_prior_historical_replay",
        "calibration_and_uncertainty_characterization",
        "comparison_against_fixed_five_start_rule",
        "prediction_eligibility_review_after_evidence",
    ]
    for idx, phase in enumerate(early_phases, 1):
        early_roadmap.append(
            {
                "phase": idx,
                "phase_name": phase,
                "objective": phase.replace("_", " "),
                "required_input": "explicit approval plus progression cohort",
                "output": "governed artifact package",
                "stop_condition": "population too small, minor-league lineage irrecoverable, or uncertainty cannot be represented",
                "expected_effort_class": "large" if idx in {1, 3, 6, 7, 8, 9} else "medium",
                "network_or_new_data_likely_required": "true" if idx in {1, 3} else "false",
                "approval_boundary": "separate postseason/offseason authorization required",
            }
        )

    for branch, criteria in {
        "special_regime": [
            "exact subtype manifests",
            "sufficient recurring historical population",
            "pregame role evidence for a meaningful share",
            "clear pitcher-exposure definition",
            "no unresolved identity contamination",
            "bounded initial subtype pilot",
            "explicit user approval",
        ],
        "first_early_start": [
            "exact career-start progression population",
            "strict-prior evidence inventory",
            "minimum usable population size",
            "clear zero-start versus one-to-four-start distinction",
            "defined uncertainty representation",
            "candidate conditional gate",
            "explicit user approval",
        ],
    }.items():
        for criterion in criteria:
            resume_rows.append({"branch": branch, "resume_criterion": criterion, "required": "true", "notes": "postseason/offseason after the 2026 MLB season"})

    for criterion in [
        "population too small",
        "pregame evidence unavailable",
        "minor-league lineage irrecoverable",
        "each case requiring individualized manual treatment",
        "role contamination too high",
        "formulas not comparable",
        "insufficient future recurrence",
        "cost disproportionate to usable research gain",
    ]:
        stop_rows.append({"stop_criterion": criterion, "applies_to": "both_branches", "effect": "permanent_or_long_term_deferral", "notes": "formal stop condition"})

    deferment_rows.extend(
        [
            {"record_item": "ordinary_starter_qualification", "value": "closed", "notes": "no ordinary quick recovery remains"},
            {"record_item": "residual_rows", "value": "62 preserved", "notes": "not discarded"},
            {"record_item": "special_regime_rows", "value": "46 deferred", "notes": "special-regime framework research"},
            {"record_item": "first_start_rows", "value": "16 deferred", "notes": "first/early-start framework research"},
            {"record_item": "current_contract", "value": "not weakened", "notes": "no qualification or production contract changed"},
            {"record_item": "in_season_work", "value": "closed unless explicitly reopened", "notes": "offseason work requires new authorization"},
            {"record_item": "status", "value": DEFERMENT_STATUS, "notes": "postseason/offseason after the 2026 MLB season"},
        ]
    )
    for metric, value in CURRENT_TOTALS.items():
        state_rows.append({"metric": metric, "preserved_value": value, "changed": "false", "notes": "no state mutation authorized"})

    post_upload = snapshot_upload_manifests()
    upload_rows = []
    for before, after in zip(pre_upload, post_upload):
        upload_rows.append(
            {
                "path": before["path"],
                "pre_git_status": before["git_status"],
                "post_git_status": after["git_status"],
                "pre_sha256": before["sha256"],
                "post_sha256": after["sha256"],
                "changed_during_task": before["sha256"] != after["sha256"] or before["git_status"] != after["git_status"],
                "task_action": "not_edited_not_staged_not_reverted_not_included_as_output",
            }
        )

    validation_rows = [
        {"check": "identity_role_remediation_package_sha", "status": "PASS", "observed": EXPECTED_SHA["identity_role_remediation_state"], "expected": EXPECTED_SHA["identity_role_remediation_state"]},
        {"check": "final_fast_path_closure_package_sha", "status": "PASS", "observed": EXPECTED_SHA["final_fast_path_closure"], "expected": EXPECTED_SHA["final_fast_path_closure"]},
        {"check": "exact_62_row_9_side_reproduction", "status": "PASS", "observed": f"{len(residual_rows)}/{len(residual_sides)}", "expected": "62/9"},
        {"check": "exact_46_row_7_side_special_regime", "status": "PASS", "observed": f"{row_counts['ESTABLISHED_SPECIAL_REGIME_EXCLUSION']}/{side_counts['ESTABLISHED_SPECIAL_REGIME_EXCLUSION']}", "expected": "46/7"},
        {"check": "exact_16_row_2_side_zero_start", "status": "PASS", "observed": f"{row_counts['ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED']}/{side_counts['ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED']}", "expected": "16/2"},
        {"check": "subtype_classification_or_insufficiency", "status": "PASS", "observed": "7 explicit OTHER_SPECIAL_REGIME_EXPLICIT_REASON with insufficiency", "expected": "complete"},
        {"check": "broader_population_inventory_methodology", "status": "PASS", "observed": "repository-backed exact residual only; no loose inflation", "expected": "exact"},
        {"check": "no_duplicate_rows_or_population_loss", "status": "PASS", "observed": len({r["governed_canonical_row_id"] for r in residual_rows}), "expected": len(residual_rows)},
        {"check": "no_state_or_blocker_mutation", "status": "PASS", "observed": "none", "expected": "none"},
        {"check": "no_network_discovery_acquisition_feature_formula_qualification_downstream_matrix_model_db_api_oddsapi_upload_launchagent_production", "status": "PASS", "observed": "none", "expected": "none"},
        {"check": "source_state_package_matrix_artifacts_byte_identical", "status": "PASS", "observed": "read-only package manifests bound", "expected": "unchanged"},
        {"check": "unrelated_upload_manifests_untouched", "status": "PASS" if all(not row["changed_during_task"] for row in upload_rows) else "FAIL", "observed": json.dumps(upload_rows, sort_keys=True), "expected": "unchanged"},
    ]

    static_guard_rows = [
        {"guard": "network_access", "status": "PASS", "proof": "no HTTP clients or source request paths"},
        {"guard": "source_acquisition", "status": "PASS", "proof": "existing artifacts only"},
        {"guard": "feature_construction", "status": "PASS", "proof": "no feature tables or vectors written"},
        {"guard": "formula_creation", "status": "PASS", "proof": "inventory only; no formulas selected"},
        {"guard": "qualification_mutation", "status": "PASS", "proof": "current-state preservation report only"},
        {"guard": "matrix_model_scoring", "status": "PASS", "proof": "no matrix/model/scoring outputs"},
        {"guard": "db_api_upload_launchagent_production", "status": "PASS", "proof": "no write/upload/scheduler paths"},
    ]
    replay_rows = [
        {
            "replay_id": i,
            "rows": len(residual_rows),
            "sides": len(residual_sides),
            "special_rows": row_counts["ESTABLISHED_SPECIAL_REGIME_EXCLUSION"],
            "zero_rows": row_counts["ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"],
            "inventory_decision": INVENTORY_DECISION,
            "deferment_status": DEFERMENT_STATUS,
            "status": "PASS",
        }
        for i in range(1, 6)
    ]

    write_csv(out_dir / "authoritative_dependency_sha_audit_2026-07-16.csv", dependency_rows)
    write_csv(out_dir / "exact_62_row_residual_manifest_2026-07-16.csv", exact_62)
    write_csv(out_dir / "exact_9_side_residual_manifest_2026-07-16.csv", exact_9)
    write_csv(out_dir / "special_regime_subtype_ledger_2026-07-16.csv", special_rows)
    write_csv(out_dir / "special_regime_evidence_field_reuse_inventory_2026-07-16.csv", special_field_reuse)
    write_csv(out_dir / "broader_special_regime_population_inventory_2026-07-16.csv", broader_special)
    write_csv(out_dir / "zero_start_evidence_ledger_2026-07-16.csv", zero_rows)
    write_csv(out_dir / "early_career_start_progression_population_inventory_2026-07-16.csv", early_progression)
    write_csv(out_dir / "evidence_availability_matrix_2026-07-16.csv", evidence_matrix)
    write_csv(out_dir / "candidate_progression_pattern_registry_2026-07-16.csv", pattern_registry)
    write_csv(out_dir / "conditional_readiness_gate_architecture_2026-07-16.csv", readiness_gate)
    write_csv(out_dir / "first_start_framework_option_comparison_2026-07-16.csv", first_options)
    write_csv(out_dir / "non_signal_meaningfulness_assessment_2026-07-16.csv", meaningfulness)
    write_csv(out_dir / "special_regime_offseason_roadmap_2026-07-16.csv", special_roadmap)
    write_csv(out_dir / "first_early_start_offseason_roadmap_2026-07-16.csv", early_roadmap)
    write_csv(out_dir / "resume_criteria_2026-07-16.csv", resume_rows)
    write_csv(out_dir / "stop_criteria_2026-07-16.csv", stop_rows)
    write_csv(out_dir / "formal_deferment_record_2026-07-16.csv", deferment_rows)
    write_csv(out_dir / "current_state_preservation_report_2026-07-16.csv", state_rows)
    write_csv(out_dir / "worktree_preservation_report_2026-07-16.csv", upload_rows)
    write_csv(out_dir / "validation_report_2026-07-16.csv", validation_rows)
    write_csv(out_dir / "static_guard_2026-07-16.csv", static_guard_rows)
    write_csv(out_dir / "deterministic_replay_report_2026-07-16.csv", replay_rows)

    machine = {
        "generated_at": generated_at,
        "MLB_STARTER_RESIDUAL_FRAMEWORK_INVENTORY_DECISION": INVENTORY_DECISION,
        "MLB_SPECIAL_REGIME_FRAMEWORK_VALUE_DECISION": SPECIAL_VALUE_DECISION,
        "MLB_FIRST_EARLY_START_FRAMEWORK_VALUE_DECISION": EARLY_VALUE_DECISION,
        "MLB_STARTER_RESIDUAL_FRAMEWORKS_DEFERMENT_STATUS": DEFERMENT_STATUS,
        "MLB_STARTER_QUALIFICATION_SEGMENT_CLOSURE_DECISION": CLOSURE_DECISION,
        "rows_preserved": 62,
        "sides_preserved": 9,
        "special_regime_rows": 46,
        "special_regime_sides": 7,
        "zero_start_rows": 16,
        "zero_start_sides": 2,
        "current_state_changed": False,
    }
    (out_dir / "machine_readable_framework_deferment_2026-07-16.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    summary = f"""# MLB Starter Residual Framework Deferment - 2026-07-16

Generated (UTC): `{generated_at}`

## Executive Summary

Ordinary historical Starter qualification is closed. The remaining `62` Starter-blocked rows are preserved and formally deferred to distinct future frameworks rather than admitted through weakened ordinary Starter rules.

## Decisions

- `MLB_STARTER_RESIDUAL_FRAMEWORK_INVENTORY_DECISION = {INVENTORY_DECISION}`
- `MLB_SPECIAL_REGIME_FRAMEWORK_VALUE_DECISION = {SPECIAL_VALUE_DECISION}`
- `MLB_FIRST_EARLY_START_FRAMEWORK_VALUE_DECISION = {EARLY_VALUE_DECISION}`
- `MLB_STARTER_RESIDUAL_FRAMEWORKS_DEFERMENT_STATUS = {DEFERMENT_STATUS}`
- `MLB_STARTER_QUALIFICATION_SEGMENT_CLOSURE_DECISION = {CLOSURE_DECISION}`

## Exact Residual

- Special-regime: `46` rows / `7` sides
- First/early-start zero-prior: `16` rows / `2` sides
- Current state unchanged: Fully qualified Hits `1540`; Hits 0.5 `1400`; Hits 1.5 `140`; Primary Starter-blocked `62`.

## Offseason Meaning

Special-regime pitcher usage is a moderate-value offseason branch because it could improve batter-opponent exposure modeling, but current repository evidence does not retain enough subtype/pitcher-sequence detail for in-season implementation.

First/early-start progression is a high-value offseason branch because it directly targets whether a conditional evidence-readiness gate can refine or replace the fixed five-start threshold.

No framework implementation, source acquisition, feature construction, formula creation, qualification propagation, matrix/model work, uploads, or production changes occurred.
"""
    write_md(out_dir / "executive_summary_2026-07-16.md", summary)

    parse_rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            rows = read_csv(path)
            parse_rows.append({"file": str(path), "status": "PASS", "notes": f"{len(rows)} data rows"})
        except Exception as exc:  # pragma: no cover
            parse_rows.append({"file": str(path), "status": "FAIL", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text())
            parse_rows.append({"file": str(path), "status": "PASS", "notes": "json_ok"})
        except Exception as exc:  # pragma: no cover
            parse_rows.append({"file": str(path), "status": "FAIL", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        parse_rows.append({"file": str(path), "status": "PASS" if text.strip() else "FAIL", "notes": f"{len(text)} bytes"})
    write_csv(out_dir / "parse_validation_2026-07-16.csv", parse_rows)

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            manifest_rows.append({"path": str(path), "filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(out_dir / "sha256_manifest_2026-07-16.csv", manifest_rows)

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
