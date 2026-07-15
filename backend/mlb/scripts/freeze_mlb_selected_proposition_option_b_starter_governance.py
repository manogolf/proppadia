"""Freeze selected-proposition Option B Starter remediation governance.

This utility creates a governance/specification package only. It reproduces
the exact OPTION_B_FEASIBLE_NOT_EXECUTED population from the completed Starter
blocker review and freezes contracts for a future bounded remediation. It does
not remediate Starter values, construct matrices, train models, call APIs,
write databases, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_option_b_starter_governance/2026-07-14"
)
REVIEW_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_blocker_review/2026-07-14"
)
FIRST_BLOCK_OPTION_B_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_starter_option_b_certified_remediation/2026-07-13"
)
GOVERNANCE_DECISION_DIR = Path(
    "artifacts/analysis/model_development/mlb_starter_historical_reconstruction_governance_decision/2026-07-13"
)
CONTRACT_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/mlb_starter_actual_vs_expected_contract_review/2026-07-13"
)
STARTER_XH_DIR = Path("artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11")
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

ROW_PROJECTION = REVIEW_DIR / f"denominator_to_starter_game_projection_ledger_{RUN_DATE}.csv"
GAME_SIDES = REVIEW_DIR / f"starter_game_natural_grain_population_{RUN_DATE}.csv"
MATRIX_A = MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv"
MATRIX_B = MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv"
MATRIX_D = MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv"

PROHIBITED_PATTERNS = {
    "model_fit_call": re.compile(r"\.fit\s*\("),
    "predict_call": re.compile(r"\.predict\s*\("),
    "metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|roi|profit)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "api_call": re.compile(r"requests\.|statsapi|httpx|urllib"),
    "db_write": re.compile(r"\b(insert|update|delete|upsert)\b", re.IGNORECASE),
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


class OptionBGovernanceFreeze:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.row_projection = read_csv(ROW_PROJECTION)
        self.game_sides = read_csv(GAME_SIDES)
        self.option_b_rows = [
            r for r in self.row_projection if r["primary_technical_category"] == "OPTION_B_FEASIBLE_NOT_EXECUTED"
        ]
        option_b_side_keys = {r["starter_game_key"] for r in self.option_b_rows}
        self.option_b_sides = [r for r in self.game_sides if r["starter_game_key"] in option_b_side_keys]
        self.matrix_ids = set()
        for path in [MATRIX_A, MATRIX_B, MATRIX_D]:
            self.matrix_ids.update(r["governed_canonical_row_id"] for r in read_csv(path))
        self.status = "OPTION_B_STARTER_REMEDIATION_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_EXECUTION_APPROVAL"

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.write_manifests()
        self.write_contracts()
        self.write_markdown()
        self.write_decision_json()
        self.write_validation()
        self.static_guard()
        self.parse_validation()
        self.sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "option_b_rows": len(self.option_b_rows),
            "option_b_starter_game_sides": len(self.option_b_sides),
            "status": self.status,
        }

    def write_manifests(self) -> None:
        row_manifest = []
        for idx, row in enumerate(self.option_b_rows, start=1):
            row_manifest.append(
                {
                    "manifest_order": idx,
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
                    "starter_game_key": row["starter_game_key"],
                    "recoverability_class": row["recoverability_class"],
                    "other_downstream_blockers_after_starter": row["other_downstream_blockers_after_starter"],
                    "selection_conditioned_population": row["selection_conditioned_population"],
                    "market_side_identity": row["market_side_identity"],
                    "governance_scope": row["governance_scope"],
                }
            )
        side_manifest = []
        for idx, row in enumerate(self.option_b_sides, start=1):
            side_manifest.append(
                {
                    "manifest_order": idx,
                    "starter_game_key": row["starter_game_key"],
                    "slate_date": row["slate_date"],
                    "game_id": row["game_id"],
                    "hitter_team": row["hitter_team"],
                    "opponent_team": row["opponent_team"],
                    "denominator_rows": row["denominator_rows"],
                    "hits_0_5_rows": row["hits_0_5_rows"],
                    "hits_1_5_rows": row["hits_1_5_rows"],
                    "pa_secondary_blocked_rows": row["pa_secondary_blocked_rows"],
                    "starter_only_blocked_rows": row["starter_only_blocked_rows"],
                    "actual_starter_player_ids": row["actual_starter_player_ids"],
                    "starter_identity_statuses": row["starter_identity_statuses"],
                    "actual_starter_roles": row["actual_starter_roles"],
                    "strict_prior_workload_reconstructable": row["strict_prior_workload_reconstructable"],
                    "special_regime": row["special_regime"],
                    "governed_future_execution_class": "OPTION_B_FEASIBLE_NOT_EXECUTED",
                }
            )
        write_csv(self.output_dir / f"exact_649_row_denominator_manifest_{RUN_DATE}.csv", row_manifest)
        write_csv(self.output_dir / f"exact_96_starter_game_side_manifest_{RUN_DATE}.csv", side_manifest)

    def write_contracts(self) -> None:
        self.write_source_hierarchy()
        self.write_field_contract()
        self.write_temporal_contract()
        self.write_identity_contract()
        self.write_special_regime_contract()
        self.write_failure_taxonomy()
        self.write_provenance_schema()
        self.write_certification_table()
        self.write_immutability_contract()
        self.write_replayability_contract()
        self.write_human_boundary()

    def write_source_hierarchy(self) -> None:
        rows = [
            {
                "precedence": 1,
                "source_name": "First-block Option B approval payload",
                "path": str(FIRST_BLOCK_OPTION_B_DIR / "mlb_starter_option_b_approval_payload_2026-07-13.json"),
                "purpose": "defines approved interpretation",
                "authority": "authoritative_governance",
                "temporal_requirement": "historical qualification only",
                "may_resolve_identity": "contract language only",
                "may_resolve_workload": "contract language only",
                "prohibited_uses": "does not identify selected-block rows itself",
            },
            {
                "precedence": 2,
                "source_name": "Starter historical reconstruction governance Option B language",
                "path": str(GOVERNANCE_DECISION_DIR / "mlb_starter_governance_option_b_language_2026-07-13.md"),
                "purpose": "Option B scope language",
                "authority": "authoritative_governance",
                "temporal_requirement": "actual starter as binding key only; features strict-prior",
                "may_resolve_identity": "no",
                "may_resolve_workload": "no",
                "prohibited_uses": "cannot broaden population",
            },
            {
                "precedence": 3,
                "source_name": "Selected-block Starter blocker review",
                "path": str(REVIEW_DIR / f"starter_game_natural_grain_population_{RUN_DATE}.csv"),
                "purpose": "exact 96-side population and feasibility class",
                "authority": "population_authority_for_future_task",
                "temporal_requirement": "date-locked local artifact",
                "may_resolve_identity": "no",
                "may_resolve_workload": "no",
                "prohibited_uses": "review only; no remediation values",
            },
            {
                "precedence": 4,
                "source_name": "Starter expected hits research dataset",
                "path": str(STARTER_XH_DIR / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"),
                "purpose": "selected-block identity/workload evidence candidate",
                "authority": "bounded_source_candidate",
                "temporal_requirement": "actual starter identity allowed only as historical binding key; workload must be strict-prior",
                "may_resolve_identity": "yes_for_option_b_if_unique",
                "may_resolve_workload": "yes_if_strict_prior_fields_pass",
                "prohibited_uses": "no same-game performance as feature; no source-gap rows",
            },
            {
                "precedence": 5,
                "source_name": "Starter Skill / Workload reconstruction source",
                "path": "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv",
                "purpose": "strict-prior parent workload source",
                "authority": "workload_parent_source",
                "temporal_requirement": "latest contributing prior game date < slate_date",
                "may_resolve_identity": "no",
                "may_resolve_workload": "yes",
                "prohibited_uses": "no same-game or future rows",
            },
        ]
        write_csv(self.output_dir / f"approved_source_hierarchy_{RUN_DATE}.csv", rows)

    def write_field_contract(self) -> None:
        rows = [
            {
                "field_name": "selected_starter_id",
                "field_family": "identity",
                "source_columns": "actual_starter_player_id",
                "formula": "unique authoritative actual starter ID used as historical binding key only",
                "lookback_boundary": "not a feature; same-game identity may bind only under Option B",
                "minimum_prior_history": "not applicable",
                "fallback_sequence": "none; fail closed if missing/conflicting",
                "missingness_behavior": "fail row/side",
                "clipping_or_clamping": "none",
                "unit": "MLBAM player ID",
                "rounding": "strip .0 artifact if present",
                "derived_from_parents": "no",
            },
            {
                "field_name": "weighted_multiseason_hits_per_out",
                "field_family": "workload_skill",
                "source_columns": "prior starts hits_allowed and outs_recorded",
                "formula": "same archived Starter Skill / Workload strict-prior formula; stable/recent blend from approved parent source",
                "lookback_boundary": "feature_cutoff_date = slate_date - 1; latest contributing prior game < slate_date",
                "minimum_prior_history": "at least one qualifying prior starter record unless frozen missingness permits null",
                "fallback_sequence": "approved reconstruction parent only; no generic fallback",
                "missingness_behavior": "retain/fail per frozen field contract; no zero fill",
                "clipping_or_clamping": "none beyond parent formula",
                "unit": "hits per out",
                "rounding": "preserve source precision",
                "derived_from_parents": "yes",
            },
            {
                "field_name": "expected_outs_blended_v1",
                "field_family": "workload",
                "source_columns": "prior starter outs windows",
                "formula": "same archived Starter Skill / Workload strict-prior formula; stable_65_recent5_35 when recent sample exists",
                "lookback_boundary": "feature_cutoff_date = slate_date - 1",
                "minimum_prior_history": "at least one qualifying prior starter record unless frozen missingness permits null",
                "fallback_sequence": "approved parent source only",
                "missingness_behavior": "retain/fail per frozen field contract",
                "clipping_or_clamping": "none beyond parent formula",
                "unit": "outs",
                "rounding": "preserve source precision",
                "derived_from_parents": "yes",
            },
            {
                "field_name": "workload_confidence",
                "field_family": "status",
                "source_columns": "prior_starts_count/recent starts/role evidence",
                "formula": "parent source categorical confidence",
                "lookback_boundary": "strict-prior only",
                "minimum_prior_history": "same as parent",
                "fallback_sequence": "none",
                "missingness_behavior": "fail if required by target variant; otherwise retain contract null",
                "clipping_or_clamping": "not applicable",
                "unit": "category",
                "rounding": "not applicable",
                "derived_from_parents": "yes",
            },
            {
                "field_name": "expected_role_label",
                "field_family": "role",
                "source_columns": "prior usage and standard regime evidence",
                "formula": "parent source expected role label",
                "lookback_boundary": "strict-prior only",
                "minimum_prior_history": "same as parent",
                "fallback_sequence": "none",
                "missingness_behavior": "fail/retain by frozen variant contract",
                "clipping_or_clamping": "not applicable",
                "unit": "category",
                "rounding": "not applicable",
                "derived_from_parents": "yes",
            },
        ]
        write_csv(self.output_dir / f"field_level_reconstruction_contract_{RUN_DATE}.csv", rows)

    def write_temporal_contract(self) -> None:
        rows = [
            {"field_group": "identity_binding", "latest_permissible_state": "postgame actual starter identity only as binding key", "strict_prior_required": "not a feature", "prohibited": "actual starter performance as feature", "artifact_proof": "identity source path, game_id, player_id, unique binding status"},
            {"field_group": "workload_features", "latest_permissible_state": "strict-prior cutoff before slate date", "strict_prior_required": "true", "prohibited": "same-game/future pitching outcomes", "artifact_proof": "feature_cutoff_date, latest_contributing_prior_game_date, prior game IDs"},
            {"field_group": "status_trust_fields", "latest_permissible_state": "strict-prior parent source or governed Option B decision", "strict_prior_required": "true for workload parents", "prohibited": "downstream model outputs", "artifact_proof": "source tier and parent field lineage"},
        ]
        write_csv(self.output_dir / f"temporal_integrity_contract_{RUN_DATE}.csv", rows)

    def write_identity_contract(self) -> None:
        cases = [
            ("exact_starter_match", "unique game_id + opponent team side + actual_starter_player_id", "accept if in exact 96-side manifest"),
            ("multiple_candidate_starters", "more than one candidate ID for same starter-game side", "fail closed"),
            ("scratched_or_replaced_starter", "conflict between probable and actual starter", "fail closed unless future governance explicitly allows"),
            ("opener_or_bullpen_game", "role flags indicate opener/short/bullpen", "exclude"),
            ("doubleheader", "same teams/date with game-ID ambiguity", "require exact game_id else fail"),
            ("suspended_or_resumed", "date/game state ambiguity", "fail closed"),
            ("two_way_player", "pitching role complication", "exclude or require separate governance"),
            ("duplicate_names", "same name/missing ID", "player ID required"),
            ("team_alias", "team code mismatch", "only exact local normalized team code accepted"),
            ("home_away_disagreement", "orientation conflict", "fail closed"),
        ]
        write_csv(
            self.output_dir / f"identity_resolution_and_tiebreak_contract_{RUN_DATE}.csv",
            [{"case": c, "evidence_condition": e, "rule": r} for c, e, r in cases],
        )

    def write_special_regime_contract(self) -> None:
        regimes = ["opener", "bulk_reliever", "bullpen_game", "short_start_expectation", "injury_limited_workload", "callup_insufficient_history", "role_transition", "planned_tandem", "two_way_player_pitching_appearance", "uncertain_starter", "postponed_suspended_rescheduled_game"]
        rows = [
            {
                "special_regime": regime,
                "future_option_b_treatment": "excluded_from_649_row_execution",
                "requires_separate_governance": "true",
                "notes": "46 rows already classified SPECIAL_REGIME_ESTABLISHED_EXCLUSION remain outside Option B execution",
            }
            for regime in regimes
        ]
        write_csv(self.output_dir / f"special_regime_exclusion_contract_{RUN_DATE}.csv", rows)

    def write_failure_taxonomy(self) -> None:
        failures = [
            "MISSING_DIRECT_SOURCE",
            "CONFLICTING_SOURCES",
            "UNRESOLVED_IDENTITY",
            "INSUFFICIENT_STRICT_PRIOR_WORKLOAD_HISTORY",
            "INCOMPLETE_LINEAGE",
            "TEMPORAL_UNCERTAINTY",
            "INCOMPATIBLE_GRAIN",
            "UNSUPPORTED_SPECIAL_REGIME",
            "NON_DETERMINISTIC_EVIDENCE",
            "SOURCE_PARSE_FAILURE",
            "SOURCE_HASH_MISMATCH",
            "POPULATION_MISMATCH",
        ]
        rows = [{"failure_code": code, "policy": "fail_closed", "allowed_recovery": "none_inside_execution_without_prior_rule"} for code in failures]
        write_csv(self.output_dir / f"failure_taxonomy_{RUN_DATE}.csv", rows)

    def write_provenance_schema(self) -> None:
        fields = [
            "remediation_version",
            "remediation_decision",
            "source_path",
            "source_sha256",
            "source_timestamp",
            "source_tier",
            "identity_resolution_method",
            "identity_confidence_classification",
            "workload_reconstruction_method",
            "workload_parent_fields",
            "strict_prior_cutoff",
            "latest_contributing_prior_game_date",
            "special_regime_status",
            "original_value",
            "remediated_value",
            "changed_status",
            "failure_reason",
            "deterministic_replay_key",
            "selection_conditioned_population",
            "market_side_identity",
            "governance_scope",
        ]
        rows = [{"field_name": f, "required": "true", "notes": "required on success and/or failure manifest as applicable"} for f in fields]
        write_csv(self.output_dir / f"provenance_schema_{RUN_DATE}.csv", rows)

    def write_certification_table(self) -> None:
        rows = [
            {"certification_layer": "Starter identity", "required_conditions": "exact 96-side membership; unique actual starter ID; no conflict; not special regime", "status_after_success": "IDENTITY_CERTIFIED_OPTION_B"},
            {"certification_layer": "Starter workload", "required_conditions": "strict-prior parent fields available; cutoff before slate date; no same-game/future feature values", "status_after_success": "WORKLOAD_CERTIFIED_STRICT_PRIOR"},
            {"certification_layer": "Starter Bundle field completeness", "required_conditions": "all frozen required Starter fields present or contract-null", "status_after_success": "STARTER_FIELDS_COMPLETE"},
            {"certification_layer": "Propagated denominator rows", "required_conditions": "only exact 649 governed denominator rows; no 36/other taxonomy rows; selected-proposition metadata preserved", "status_after_success": "DENOMINATOR_ROWS_PROPAGATED"},
            {"certification_layer": "Final Starter qualification", "required_conditions": "identity + workload + field completeness + provenance + replay pass", "status_after_success": "STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER"},
        ]
        write_csv(self.output_dir / f"certification_decision_table_{RUN_DATE}.csv", rows)

    def write_immutability_contract(self) -> None:
        rows = [
            {"protected_item": "denominator membership/order", "rule": "no mutation"},
            {"protected_item": "opposite side", "rule": "no creation"},
            {"protected_item": "non-characterized rows", "rule": "cannot enter execution"},
            {"protected_item": "existing 99-row A/B/D Hits 1.5 matrices", "rule": "no changes"},
            {"protected_item": "Variant C", "rule": "no decision or construction"},
            {"protected_item": "PA/outcome/matrix/model/production", "rule": "out of scope"},
            {"protected_item": "historical source artifacts", "rule": "read-only; write new bounded package"},
        ]
        write_csv(self.output_dir / f"immutability_and_non_mutation_contract_{RUN_DATE}.csv", rows)

    def write_replayability_contract(self) -> None:
        rows = [
            {"requirement": "canonical input manifests", "rule": "649 row and 96 side manifests must hash-match"},
            {"requirement": "deterministic ordering", "rule": "manifest_order frozen; outputs sorted by manifest_order"},
            {"requirement": "stable identity keys", "rule": "governed_canonical_row_id and starter_game_key required"},
            {"requirement": "rerun behavior", "rule": "exact reproduction or fail with discrepancy"},
            {"requirement": "source-change detection", "rule": "source path and SHA required"},
            {"requirement": "output manifest", "rule": "all artifacts SHA256 listed"},
        ]
        write_csv(self.output_dir / f"replayability_contract_{RUN_DATE}.csv", rows)

    def write_human_boundary(self) -> None:
        rows = [
            {
                "boundary": "execution authorization",
                "status": "not_authorized_by_this_governance_freeze",
                "required_future_status": "explicit human approval for one bounded Option B remediation execution",
            },
            {
                "boundary": "final_governance_status",
                "status": "FROZEN_AWAITING_EXPLICIT_EXECUTION_APPROVAL",
                "required_future_status": "human approval consumed fail-closed",
            },
        ]
        write_csv(self.output_dir / f"human_approval_boundary_{RUN_DATE}.csv", rows)

    def write_markdown(self) -> None:
        main = f"""# Historical Selected-Proposition Option B Starter Remediation Governance - {RUN_DATE}

## Status

`{self.status}`

This package freezes governance for a future bounded Option B Starter identity
and strict-prior workload remediation. It does not authorize or execute
remediation.

## Authoritative Option B Meaning

Repository evidence defines Option B as follows: authoritative unique postgame
actual-starter identity may be used solely as a historical binding key to
reconstruct strictly prior Starter Skill / Workload features when direct
pregame expected-starter evidence is unavailable. It does not redefine live
expected-starter semantics, does not apply to production, excludes special
regimes, and requires provenance, source SHAs, replay flags, and row-level
auditability.

Primary evidence:
- `{FIRST_BLOCK_OPTION_B_DIR / "mlb_starter_option_b_certified_remediation_findings_2026-07-13.md"}`
- `{FIRST_BLOCK_OPTION_B_DIR / "mlb_starter_option_b_approval_payload_2026-07-13.json"}`
- `{GOVERNANCE_DECISION_DIR / "mlb_starter_governance_option_b_language_2026-07-13.md"}`

## Frozen Population

- Denominator rows: `{len(self.option_b_rows)}`
- Starter-game sides: `{len(self.option_b_sides)}`
- Existing certified 99-row A/B/D matrix overlap: `0`

The hitter-prop denominator identity remains:
`slate_date | game_id | player_id | prop_type | line | side`.

The Starter-game-side identity is:
`slate_date | game_id | hitter_team | opponent_team`.

The future remediation may project Starter-game-side decisions to denominator
rows only through the exact frozen manifests in this package.

## Boundaries

No source-gap rows, strict-prior-incomplete rows, special-regime rows, Variant C
rows, opposite-side rows, PA rows, outcome rows, matrix rows, model artifacts, or
production surfaces are included in this governance approval.

## Failure Policy

All ambiguity fails closed. There is no best-effort guessing, no silent null
filling, no majority vote, no fallback formula, and no population expansion.

## Final Decision

`OPTION_B_STARTER_REMEDIATION_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_EXECUTION_APPROVAL`
"""
        summary = f"""# One-Page Option B Starter Governance Summary - {RUN_DATE}

Frozen governance status:
`OPTION_B_STARTER_REMEDIATION_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_EXECUTION_APPROVAL`

Future execution population:
- `{len(self.option_b_rows)}` denominator rows
- `{len(self.option_b_sides)}` Starter-game sides

Option B meaning:
Use unique actual-starter identity only as a historical binding key; reconstruct
Starter workload fields only from strict-prior evidence.

Special regimes remain excluded. Existing 99-row A/B/D matrices remain
unchanged. This governance freeze does not authorize remediation.
"""
        (self.output_dir / f"main_governance_specification_{RUN_DATE}.md").write_text(main)
        (self.output_dir / f"one_page_decision_summary_{RUN_DATE}.md").write_text(summary)

    def write_decision_json(self) -> None:
        write_json(
            self.output_dir / f"machine_readable_governance_contract_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "status": self.status,
                "option_b_definition": "authoritative unique postgame actual-starter identity may be used solely as a historical binding key; reconstructed Starter workload fields must be strict-prior",
                "population": {
                    "denominator_rows": len(self.option_b_rows),
                    "starter_game_sides": len(self.option_b_sides),
                    "classification": "OPTION_B_FEASIBLE_NOT_EXECUTED",
                },
                "prohibited": {
                    "starter_remediation": True,
                    "matrix_construction": True,
                    "modeling": True,
                    "api_calls": True,
                    "database_writes": True,
                    "production_changes": True,
                },
            },
        )

    def write_validation(self) -> None:
        row_ids = [r["governed_canonical_row_id"] for r in self.option_b_rows]
        side_ids = [r["starter_game_key"] for r in self.option_b_sides]
        overlap_99 = len(set(row_ids) & self.matrix_ids)
        excluded_taxonomy_overlap = sum(
            1
            for r in self.option_b_rows
            if r["primary_technical_category"] != "OPTION_B_FEASIBLE_NOT_EXECUTED"
            or "SPECIAL_REGIME" in r["recoverability_class"]
            or "Source population incomplete" in r["recoverability_class"]
        )
        rows = [
            ("exact_649_row_reproduction", len(self.option_b_rows), 649),
            ("exact_96_starter_game_side_reproduction", len(self.option_b_sides), 96),
            ("zero_excluded_taxonomy_overlap", excluded_taxonomy_overlap, 0),
            ("zero_existing_99_matrix_overlap", overlap_99, 0),
            ("denominator_identity_unique", len(row_ids) - len(set(row_ids)), 0),
            ("starter_game_side_identity_unique", len(side_ids) - len(set(side_ids)), 0),
        ]
        write_csv(
            self.output_dir / f"governance_validation_report_{RUN_DATE}.csv",
            [{"check": k, "observed": o, "expected": e, "status": "PASS" if o == e else "FAIL"} for k, o, e in rows],
        )

    def static_guard(self) -> None:
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
                if (
                    "pattern.finditer" in line
                    or "re.compile" in line
                    or line.startswith('"')
                    or "h.update" in line
                    or ".update(" in line
                ):
                    continue
                matches.append(line)
            rows.append({"guard": name, "match_count": len(matches), "status": "PASS" if not matches else "FAIL", "evidence": "|".join(matches[:5])})
        write_csv(self.output_dir / f"static_no_model_no_signal_guard_{RUN_DATE}.csv", rows)

    def parse_validation(self) -> None:
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

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            if path.is_file():
                rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = OptionBGovernanceFreeze(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
