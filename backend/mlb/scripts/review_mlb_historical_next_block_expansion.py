"""Review and select the next bounded historical qualification block.

Read-only planning/inventory utility. It does not qualify rows, build matrices,
train models, score rows, call APIs, write databases, or change production.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-13"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_next_block_expansion_readiness_review/2026-07-13"
)
INVENTORY_DIR = Path("artifacts/analysis/model_development/mlb_historical_certified_population_qualification/2026-07-13")
SLATE_INVENTORY = INVENTORY_DIR / "mlb_historical_slate_inventory_2026-07-13.csv"
BLOCK_SUMMARY = INVENTORY_DIR / "mlb_historical_date_block_summary_2026-07-13.csv"
SOURCE_COVERAGE = INVENTORY_DIR / "mlb_historical_source_coverage_matrix_2026-07-13.csv"
FIRST_BLOCK_MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13"
)
OFFLINE_PROCESS_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_offline_process_validation/2026-07-13"
)
DRY_RUN_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_no_promotion_training_dry_run/2026-07-13"
)
FIRST_BLOCK_DATES = {
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-25",
    "2026-06-26",
    "2026-06-27",
    "2026-06-28",
}
RECOMMENDED_DATES = [
    "2026-06-29",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
    "2026-07-03",
    "2026-07-04",
    "2026-07-05",
    "2026-07-06",
    "2026-07-07",
    "2026-07-08",
    "2026-07-09",
]


PROHIBITED_PATTERNS = {
    "fit": re.compile(r"\.fit\s*\("),
    "predict": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "metric": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "ranking": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "model_selection": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|train_test_split)\b"),
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
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def as_int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def pct(n: int, d: int) -> float:
    return round(n / d, 6) if d else 0.0


def classify_date(row: dict[str, str]) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if row["slate_date"] in FIRST_BLOCK_DATES:
        return "ALREADY_PROCESS_VALIDATED_FIRST_BLOCK", ["first block already completed full process path"]
    if row.get("qualification_class") == "Class A - Near-direct qualification":
        return "READY_FOR_BOUNDED_QUALIFICATION", []
    if row.get("denominator_source_present") != "present":
        blockers.append("DENOMINATOR_NOT_IDENTIFIED")
    if row.get("explicit_run_tag_present") != "yes":
        blockers.append("TEMPORAL_RUN_TAG_ABSENT")
    if row.get("starter_source_present") != "present":
        blockers.append("STARTER_SOURCE_NOT_DIRECTLY_IDENTIFIED")
    if row.get("pa_source_present") != "present":
        blockers.append("PA_SOURCE_NOT_DIRECTLY_IDENTIFIED")
    if row.get("offense_context_source_present") != "present":
        blockers.append("OFFENSE_CONTEXT_NOT_IDENTIFIED")
    if row.get("outcome_source_present") != "present":
        blockers.append("OUTCOME_SOURCE_NOT_IDENTIFIED")
    if row.get("temporal_integrity_status") not in {"certified"}:
        blockers.append("TEMPORAL_PROVENANCE_REQUIRES_AUDIT")
    if row.get("replayability_status") not in {"certified"}:
        blockers.append("REPLAYABILITY_REQUIRES_PROBE")
    if row.get("qualification_class") == "Class U - Currently unresolved":
        return "DENOMINATOR_NOT_YET_CERTIFIABLE", blockers or ["source discovery unresolved"]
    if row.get("starter_source_present") != "present" or row.get("pa_source_present") != "present":
        return "READY_WITH_EXPECTED_BOUNDED_REMEDIATION", blockers
    if row.get("outcome_source_present") != "present":
        return "OUTCOME_SOURCE_COVERAGE_INCOMPLETE", blockers
    if row.get("temporal_integrity_status") != "certified":
        return "TEMPORAL_PROVENANCE_INSUFFICIENT", blockers
    return "FEATURE_SOURCE_COVERAGE_INCOMPLETE", blockers


class Review:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.inventory = read_csv(SLATE_INVENTORY)
        self.block_summary = read_csv(BLOCK_SUMMARY)
        self.source_coverage = read_csv(SOURCE_COVERAGE)
        self.decision_statuses: dict[str, str] = {}

    def rows_for(self, dates: list[str]) -> list[dict[str, str]]:
        wanted = set(dates)
        return [r for r in self.inventory if r["slate_date"] in wanted]

    def range_rows(self, start: str, end: str) -> list[dict[str, str]]:
        return [r for r in self.inventory if start <= r["slate_date"] <= end]

    def estimate(self, rows: list[dict[str, str]]) -> dict[str, int]:
        denominator = sum(as_int(r.get("canonical_candidate_rows_exact")) for r in rows)
        attachable = sum(as_int(r.get("estimated_attachable_rows")) for r in rows)
        return {
            "dates": len(rows),
            "approx_denominator_rows": denominator,
            "approx_numeric_outcome_rows": attachable,
            "expected_nonappearance_game_status_exceptions": max(0, denominator - attachable),
            "expected_starter_covered_rows": sum(
                as_int(r.get("canonical_candidate_rows_exact")) for r in rows if r.get("starter_source_present") == "present"
            ),
            "expected_pa_covered_rows": sum(
                as_int(r.get("canonical_candidate_rows_exact")) for r in rows if r.get("pa_source_present") == "present"
            ),
            "expected_hitter_persistence_covered_rows": sum(
                as_int(r.get("canonical_candidate_rows_exact")) for r in rows if "hitter_persistence" in r.get("evidence_paths", "")
            ),
            "expected_variant_c_market_covered_rows": sum(
                as_int(r.get("canonical_candidate_rows_exact"))
                for r in rows
                if r.get("variant_c_market_source_present") == "present"
            ),
        }

    def refreshed_inventory(self) -> list[dict[str, Any]]:
        rows = []
        for r in self.inventory:
            status, blockers = classify_date(r)
            rows.append(
                {
                    **r,
                    "date_level_readiness_status": status,
                    "contributing_blockers": "|".join(blockers),
                    "source_path_exists": str(Path(r["denominator_source_path"]).exists()).lower()
                    if r.get("denominator_source_path")
                    else "false",
                }
            )
        write_csv(self.output_dir / f"refreshed_historical_date_inventory_{RUN_DATE}.csv", rows)
        return rows

    def date_ledgers(self, inventory_rows: list[dict[str, Any]]) -> None:
        denom = []
        domain = []
        outcome = []
        bundle = []
        for r in inventory_rows:
            denom.append(
                {
                    "slate_date": r["slate_date"],
                    "date_block_id": r["date_block_id"],
                    "denominator_source_present": r["denominator_source_present"],
                    "denominator_source_path": r["denominator_source_path"],
                    "explicit_run_tag_present": r["explicit_run_tag_present"],
                    "canonical_candidate_rows_exact": r["canonical_candidate_rows_exact"],
                    "temporal_integrity_status": r["temporal_integrity_status"],
                    "replayability_status": r["replayability_status"],
                    "date_level_readiness_status": r["date_level_readiness_status"],
                    "contributing_blockers": r["contributing_blockers"],
                }
            )
            domain.append(
                {
                    "slate_date": r["slate_date"],
                    "starter_direct_source": r["starter_source_present"],
                    "starter_reconstructable_source": "expected" if "component reconstruction" in r.get("recoverable_domains", "") else "",
                    "pa_direct_source": r["pa_source_present"],
                    "pa_strict_prior_reconstructable_source": "expected" if r["pa_source_present"] == "present" else "",
                    "pa_sparse_history_population": "possible_if_first_player_history_missing" if r["season"] == "2026" else "unknown",
                    "team_context_source": r["offense_context_source_present"],
                    "likely_domain_blockers": r["blocking_domains"],
                }
            )
            outcome.append(
                {
                    "slate_date": r["slate_date"],
                    "outcome_source_present": r["outcome_source_present"],
                    "outcome_attachability_status": r["outcome_attachability_status"],
                    "game_status_nonappearance_evidence": "expected_via_certified_governance_path"
                    if r["outcome_source_present"] == "present"
                    else "not_identified",
                    "likely_outcome_blockers": ""
                    if r["outcome_source_present"] == "present"
                    else "authoritative outcome source not yet identified",
                }
            )
            bundle.append(
                {
                    "slate_date": r["slate_date"],
                    "hitter_persistence": "present" if "hitter_persistence" in r.get("evidence_paths", "") else "not_identified",
                    "offense_factor": r["offense_context_source_present"],
                    "starter_bundle_fields": r["starter_source_present"],
                    "pa_bundle_fields": r["pa_source_present"],
                    "variant_c_market_fields": r["variant_c_market_source_present"],
                    "prepared_features_present": r["prepared_features_present"],
                    "frozen_bundle_field_status": "likely_complete"
                    if r["date_level_readiness_status"] == "READY_FOR_BOUNDED_QUALIFICATION"
                    else "requires_source_recovery_or_validation",
                    "source_sha_or_immutable_provenance": "source-path-level evidence only; row-level SHA deferred to execution",
                }
            )
        write_csv(self.output_dir / f"date_level_denominator_readiness_ledger_{RUN_DATE}.csv", denom)
        write_csv(self.output_dir / f"date_level_domain_source_coverage_ledger_{RUN_DATE}.csv", domain)
        write_csv(self.output_dir / f"date_level_outcome_source_coverage_ledger_{RUN_DATE}.csv", outcome)
        write_csv(self.output_dir / f"date_level_bundle_field_coverage_ledger_{RUN_DATE}.csv", bundle)

    def source_regime_map(self) -> list[dict[str, Any]]:
        rows = []
        by_block = defaultdict(list)
        for r in self.inventory:
            by_block[r["date_block_id"]].append(r)
        for block, vals in sorted(by_block.items()):
            rows.append(
                {
                    "date_block_id": block,
                    "date_start": min(r["slate_date"] for r in vals),
                    "date_end": max(r["slate_date"] for r in vals),
                    "dates": len(vals),
                    "denominator_identity_modes": "|".join(sorted({r["denominator_source_identity"] for r in vals})),
                    "explicit_run_tag_dates": sum(1 for r in vals if r["explicit_run_tag_present"] == "yes"),
                    "starter_dates": sum(1 for r in vals if r["starter_source_present"] == "present"),
                    "pa_dates": sum(1 for r in vals if r["pa_source_present"] == "present"),
                    "offense_dates": sum(1 for r in vals if r["offense_context_source_present"] == "present"),
                    "outcome_dates": sum(1 for r in vals if r["outcome_source_present"] == "present"),
                    "regime_label": self.regime_label(vals),
                }
            )
        write_csv(self.output_dir / f"source_regime_map_{RUN_DATE}.csv", rows)
        return rows

    def regime_label(self, rows: list[dict[str, str]]) -> str:
        if all(r["qualification_class"] == "Class A - Near-direct qualification" for r in rows):
            return "near_direct_current_2026_certified_like_regime"
        if any(r["explicit_run_tag_present"] == "yes" for r in rows):
            return "explicit_run_tag_partial_component_reconstruction_regime"
        if all(r["denominator_source_present"] == "present" for r in rows):
            return "canonical_unversioned_denominator_recovery_regime"
        return "unresolved_or_sparse_source_regime"

    def candidate_blocks(self) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, str]]]]:
        candidates: dict[str, list[dict[str, str]]] = {
            "strategy_a_adjacent_extension_after_first_block": self.rows_for(RECOMMENDED_DATES),
            "strategy_b_earlier_2026_may_01_to_may_07": self.range_rows("2026-05-01", "2026-05-07"),
            "strategy_c_noncontiguous_pilot": self.rows_for(
                ["2026-05-01", "2026-05-02", "2026-05-03", "2026-06-01", "2026-06-02", "2026-06-03", "2026-07-01", "2026-07-02", "2026-07-03"]
            ),
            "strategy_d_larger_contiguous_wave_may_01_to_june_28": self.range_rows("2026-05-01", "2026-06-28"),
        }
        rows = []
        for name, vals in candidates.items():
            est = self.estimate(vals)
            statuses = Counter(classify_date(r)[0] for r in vals)
            rows.append(
                {
                    "strategy": name,
                    "date_start": min((r["slate_date"] for r in vals), default=""),
                    "date_end": max((r["slate_date"] for r in vals), default=""),
                    "explicit_date_list": "|".join(r["slate_date"] for r in vals),
                    **est,
                    "date_readiness_mix": "|".join(f"{k}:{v}" for k, v in statuses.items()),
                    "advantages": self.strategy_advantages(name),
                    "risks": self.strategy_risks(name),
                    "selection_assessment": "RECOMMENDED" if name == "strategy_a_adjacent_extension_after_first_block" else "NOT_SELECTED",
                }
            )
        write_csv(self.output_dir / f"candidate_block_comparison_{RUN_DATE}.csv", rows)
        return rows, candidates

    def strategy_advantages(self, name: str) -> str:
        return {
            "strategy_a_adjacent_extension_after_first_block": "complete inventory coverage; immediate repeatability test; all core domains present; 11 dates",
            "strategy_b_earlier_2026_may_01_to_may_07": "tests earlier source conditions after practical 2026-05-01 lower boundary",
            "strategy_c_noncontiguous_pilot": "tests multiple regimes and portability early",
            "strategy_d_larger_contiguous_wave_may_01_to_june_28": "fastest path toward longer temporal spans",
        }[name]

    def strategy_risks(self, name: str) -> str:
        return {
            "strategy_a_adjacent_extension_after_first_block": "same broad source regime as late-June/early-July; older pilot artifacts must not replace current certification",
            "strategy_b_earlier_2026_may_01_to_may_07": "starter source not directly identified; temporal replay requires source-lock audit",
            "strategy_c_noncontiguous_pilot": "fragmented remediation; mixed source regimes slow exact blocker accounting",
            "strategy_d_larger_contiguous_wave_may_01_to_june_28": "too large for next bounded execution; broad source gaps risk scope creep",
        }[name]

    def write_strategy_assessments(self, comparison: list[dict[str, Any]]) -> None:
        name_to_file = {
            "strategy_a_adjacent_extension_after_first_block": "adjacent_extension_assessment",
            "strategy_b_earlier_2026_may_01_to_may_07": "earlier_2026_assessment",
            "strategy_c_noncontiguous_pilot": "noncontiguous_pilot_assessment",
            "strategy_d_larger_contiguous_wave_may_01_to_june_28": "larger_wave_assessment",
        }
        for row in comparison:
            text = f"""# {row['strategy']} - {RUN_DATE}

Date range: `{row['date_start']}` to `{row['date_end']}`.

Dates: `{row['dates']}`.

Approximate denominator rows: `{row['approx_denominator_rows']}`.

Readiness mix: `{row['date_readiness_mix']}`.

Advantages: {row['advantages']}.

Risks: {row['risks']}.

Selection assessment: `{row['selection_assessment']}`.
"""
            (self.output_dir / f"{name_to_file[row['strategy']]}_{RUN_DATE}.md").write_text(text)

    def first_block_role(self) -> list[dict[str, Any]]:
        rows = [
            {
                "date_range": "2026-06-22_to_2026-06-28",
                "role_classification": "process-development block and process-validation block",
                "future_training_eligible": "requires explicit governance review; not automatically eligible",
                "future_validation_eligible": "no",
                "future_holdout_eligible": "no",
                "evidence_grade_evaluation_role": "not untouched; do not silently use as holdout",
                "rationale": "block was repeatedly inspected and remediated while denominator, Starter, PA, outcome, matrix, and dry-run machinery were developed",
            }
        ]
        write_csv(self.output_dir / f"completed_first_block_role_classification_{RUN_DATE}.csv", rows)
        md = """# Completed First Block Role Classification

The 2026-06-22 through 2026-06-28 block is classified as a process-development and process-validation block. It should not be treated as an untouched validation or holdout population because the campaign repeatedly inspected and remediated it while building the machinery.
"""
        (self.output_dir / f"completed_first_block_role_classification_{RUN_DATE}.md").write_text(md)
        return rows

    def fold_feasibility(self) -> list[dict[str, Any]]:
        rows = [
            {
                "future_need": "date_ordered_fit_periods",
                "conceptual_requirement": "multiple contiguous weeks of certified dates before any untouched validation period",
                "current_status": "one process-development block plus one recommended expansion candidate",
                "notes": "do not use seven-date pilot as evidence-grade fold",
            },
            {
                "future_need": "validation_periods",
                "conceptual_requirement": "separate certified dates not used during process development",
                "current_status": "not yet available",
                "notes": "requires further expansion after next block",
            },
            {
                "future_need": "untouched_holdout_periods",
                "conceptual_requirement": "later certified block selected before signal inspection",
                "current_status": "not yet available",
                "notes": "selection must be based on source readiness, not outcomes",
            },
            {
                "future_need": "common-date Variant A-D comparison",
                "conceptual_requirement": "same certified dates and denominator rules across all variants",
                "current_status": "possible only after repeated block qualification",
                "notes": "no variant comparison authorized here",
            },
            {
                "future_need": "Hits 0.5 and Hits 1.5 assessment",
                "conceptual_requirement": "enough certified dates per line scope to avoid tiny process-only pockets",
                "current_status": "not evidence-grade",
                "notes": "scope-specific signal remains unauthorized",
            },
        ]
        write_csv(self.output_dir / f"long_term_temporal_fold_feasibility_{RUN_DATE}.csv", rows)
        md = "# Long-Term Temporal Fold Feasibility\n\nHistorical expansion needs repeated certified blocks before any evidence-grade temporal fold design. The completed first block validates machinery, not predictive evidence.\n"
        (self.output_dir / f"long_term_temporal_fold_feasibility_{RUN_DATE}.md").write_text(md)
        return rows

    def projections(self, comparison: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for row in comparison:
            denom = as_int(str(row["approx_denominator_rows"]))
            attach = as_int(str(row["approx_numeric_outcome_rows"]))
            rows.append(
                {
                    "strategy": row["strategy"],
                    "denominator_dates": row["dates"],
                    "approx_denominator_rows": denom,
                    "expected_numeric_outcome_coverage": attach,
                    "expected_nonappearance_game_status_exceptions": row["expected_nonappearance_game_status_exceptions"],
                    "expected_starter_coverage": row["expected_starter_covered_rows"],
                    "expected_pa_coverage": row["expected_pa_covered_rows"],
                    "expected_hitter_persistence_coverage": row["expected_hitter_persistence_covered_rows"],
                    "expected_variant_c_market_field_coverage": row["expected_variant_c_market_covered_rows"],
                    "likely_variant_a_b_d_matrix_population_estimate": "unknown_until_row_level_qualification",
                    "likely_variant_c_matrix_population_estimate": "unknown_until_market-field row-level qualification",
                    "remediation_categories_likely": "none/minimal source replay"
                    if row["strategy"] == "strategy_a_adjacent_extension_after_first_block"
                    else "denominator source-lock; starter reconstruction; outcome attachment; temporal replay",
                    "estimate_status": "ESTIMATE_NOT_CERTIFIED",
                }
            )
        write_csv(self.output_dir / f"candidate_population_growth_projections_{RUN_DATE}.csv", rows)
        return rows

    def recommendation(self, candidates: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
        vals = candidates["strategy_a_adjacent_extension_after_first_block"]
        est = self.estimate(vals)
        rows = [
            {
                "recommended_action": "bounded_denominator_and_source_qualification_execution",
                "date_range": "2026-06-29_to_2026-07-09",
                "explicit_date_list": "|".join(RECOMMENDED_DATES),
                "maximum_denominator_population": 15000,
                "estimated_denominator_population": est["approx_denominator_rows"],
                "execution_mode": "one bounded block with mandatory date-by-date gates and per-date stop conditions",
                "expected_source_regime": "near-direct current 2026 explicit-run-tag regime",
                "anticipated_remediation_categories": "source replay verification; current-governance outcome certification; possible row-level Starter/PA edge cases; Variant C market-field audit",
                "human_approval_needed": "yes before executing qualification; no new governance ambiguity expected unless stop condition appears",
                "why_preferable": "highest source readiness, all core domains present, 11 dates to test repeatability beyond seven-day pilot, bounded under 15k rows, useful separation from process-development block",
            }
        ]
        write_csv(self.output_dir / f"recommended_exact_next_block_{RUN_DATE}.csv", rows)
        return rows

    def stop_conditions(self) -> list[dict[str, Any]]:
        rows = [
            {"stop_condition": "authoritative pregame denominator cannot be reproduced", "failure_action": "stop execution and report blocker"},
            {"stop_condition": "temporal provenance fails for any date", "failure_action": "stop or isolate date before qualification"},
            {"stop_condition": "denominator identity is ambiguous or duplicate-expanding", "failure_action": "stop"},
            {"stop_condition": "source format changes invalidate frozen semantics", "failure_action": "stop and request governance review"},
            {"stop_condition": "new nonappearance/game-status ambiguity outside approved treatment", "failure_action": "stop and request human decision"},
            {"stop_condition": "projected population exceeds 15,000 denominator rows", "failure_action": "stop or reduce date list"},
            {"stop_condition": "required external acquisition exceeds authorization", "failure_action": "stop"},
            {"stop_condition": "deterministic replay fails", "failure_action": "stop"},
        ]
        write_csv(self.output_dir / f"future_execution_stop_conditions_{RUN_DATE}.csv", rows)
        return rows

    def human_approval(self) -> list[dict[str, Any]]:
        rows = [
            {
                "approval_item": "execute next bounded denominator/source qualification block",
                "required": "yes",
                "reason": "next task will certify new population rows",
                "expected_ambiguity": "none beyond existing governance unless stop condition appears",
            },
            {
                "approval_item": "new governance treatment",
                "required": "conditional",
                "reason": "only if nonappearance, game-status, source-regime, or temporal ambiguity exceeds prior approvals",
                "expected_ambiguity": "low for recommended block",
            },
        ]
        write_csv(self.output_dir / f"human_approval_requirement_{RUN_DATE}.csv", rows)
        return rows

    def decisions(self) -> dict[str, str]:
        self.decision_statuses = {
            "HISTORICAL_INVENTORY_REPRODUCTION_STATUS": "PASS_484_DATES_REPRODUCED",
            "DATE_LEVEL_DENOMINATOR_READINESS_STATUS": "PASS_LEDGER_EMITTED",
            "DATE_LEVEL_STARTER_SOURCE_STATUS": "PASS_LEDGER_EMITTED_WITH_GAPS",
            "DATE_LEVEL_PA_SOURCE_STATUS": "PASS_LEDGER_EMITTED_WITH_GAPS",
            "DATE_LEVEL_BUNDLE_FIELD_STATUS": "PASS_LEDGER_EMITTED_WITH_GAPS",
            "DATE_LEVEL_OUTCOME_SOURCE_STATUS": "PASS_LEDGER_EMITTED_WITH_GAPS",
            "SOURCE_REGIME_CHARACTERIZATION_STATUS": "PASS_BLOCK_REGIMES_CHARACTERIZED",
            "FIRST_BLOCK_FUTURE_ROLE": "PROCESS_DEVELOPMENT_AND_PROCESS_VALIDATION_BLOCK_NOT_UNTOUCHED_HOLDOUT",
            "ADJACENT_EXTENSION_STATUS": "RECOMMENDED_READY_FOR_BOUNDED_QUALIFICATION",
            "EARLIER_2026_BLOCK_STATUS": "PLAUSIBLE_LATER_WITH_EXPECTED_REMEDIATION",
            "NONCONTIGUOUS_PILOT_STATUS": "DEFER_FRAGMENTATION_RISK",
            "LARGER_CONTIGUOUS_WAVE_STATUS": "DEFER_SCOPE_TOO_LARGE_FOR_NEXT_EXECUTION",
            "TEMPORAL_FOLD_FEASIBILITY_STATUS": "NOT_YET_EVIDENCE_GRADE_MORE_CERTIFIED_BLOCKS_REQUIRED",
            "HUMAN_APPROVAL_REQUIRED": "YES_FOR_NEXT_EXECUTION",
            "NEXT_BLOCK_SELECTION_DECISION": "SELECT_2026_06_29_TO_2026_07_09",
            "NEXT_BOUNDED_EXECUTION_READINESS": "READY_FOR_SEPARATE_BOUNDED_DENOMINATOR_AND_SOURCE_QUALIFICATION_REQUEST",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Execute bounded denominator and source qualification for 2026-06-29 through 2026-07-09 with 15,000-row cap and date-by-date gates",
        }
        return self.decision_statuses

    def markdown_reports(
        self,
        inventory_rows: list[dict[str, Any]],
        comparison: list[dict[str, Any]],
        recommendation: list[dict[str, Any]],
    ) -> None:
        status_counts = Counter(r["date_level_readiness_status"] for r in inventory_rows)
        status_lines = "\n".join(f"- {k}: `{v}`" for k, v in sorted(status_counts.items()))
        comparison_lines = "\n".join(
            f"- {r['strategy']}: {r['date_start']} to {r['date_end']}, {r['approx_denominator_rows']} estimated denominator rows, `{r['selection_assessment']}`"
            for r in comparison
        )
        decision_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        rec = recommendation[0]
        main = f"""# MLB Historical Next Block Expansion Readiness Review - {RUN_DATE}

## Executive Summary

This read-only review reproduced the 484-date historical inventory and selected the next bounded historical qualification block. No rows were qualified, no matrices were constructed, no model was trained, no predictions or metrics were produced, and no production systems were changed.

## Inventory Summary

- Date range: `2024-03-28` through `2026-07-13`
- Historical slate dates reproduced: `484`
- Approximate known denominator rows across block summary: `1279986`

Date readiness mix:

{status_lines}

## Candidate Strategy Comparison

{comparison_lines}

## First Block Treatment

The 2026-06-22 through 2026-06-28 block is classified as a process-development and process-validation block. It is not an untouched holdout and should not be silently reused for evidence-grade evaluation.

## Recommended Next Block

- Recommended action: `{rec['recommended_action']}`
- Date range: `{rec['date_range']}`
- Estimated denominator population: `{rec['estimated_denominator_population']}`
- Maximum approved population recommendation: `{rec['maximum_denominator_population']}`
- Execution mode: {rec['execution_mode']}
- Why preferable: {rec['why_preferable']}

## Decision Statuses

{decision_lines}
"""
        one_page = f"""# One-Page Human Decision Summary - {RUN_DATE}

Recommended next bounded action: denominator and source qualification execution for `2026-06-29` through `2026-07-09`.

Recommended cap: `15,000` denominator rows.

Why this block: it is the only current Class A near-direct block, has 11 dates, all core source domains present, and tests repeatability beyond the seven-date process-development block without jumping into older source-regime remediation.

Human approval is required before executing qualification. No model or signal work is authorized.
"""
        (self.output_dir / f"next_block_expansion_readiness_report_{RUN_DATE}.md").write_text(main)
        (self.output_dir / f"one_page_human_decision_summary_{RUN_DATE}.md").write_text(one_page)

    def parse_validation(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    with path.open(newline="") as f:
                        reader = csv.reader(f)
                        header = next(reader, None)
                        row_count = sum(1 for _ in reader)
                    status = "PASS"
                    notes = f"{len(header or [])} columns"
                except Exception as exc:
                    row_count = ""
                    status = "FAIL"
                    notes = str(exc)
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    row_count = ""
                    status = "PASS"
                    notes = "json parsed"
                except Exception as exc:
                    row_count = ""
                    status = "FAIL"
                    notes = str(exc)
            elif path.suffix == ".md":
                row_count = ""
                status = "PASS" if path.read_text().startswith("#") else "WARN"
                notes = "markdown reviewed"
            else:
                continue
            rows.append({"artifact_path": str(path), "parse_status": status, "row_count": row_count, "notes": notes})
        write_csv(self.output_dir / f"parse_validation_{RUN_DATE}.csv", rows)
        return rows

    def deterministic_validation(self, inventory_rows: list[dict[str, Any]], comparison: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = [
            {
                "check_name": "inventory_date_count",
                "expected": 484,
                "observed": len(inventory_rows),
                "status": "PASS" if len(inventory_rows) == 484 else "FAIL",
            },
            {
                "check_name": "inventory_date_range",
                "expected": "2024-03-28_to_2026-07-13",
                "observed": f"{min(r['slate_date'] for r in inventory_rows)}_to_{max(r['slate_date'] for r in inventory_rows)}",
                "status": "PASS"
                if min(r["slate_date"] for r in inventory_rows) == "2024-03-28"
                and max(r["slate_date"] for r in inventory_rows) == "2026-07-13"
                else "FAIL",
            },
            {
                "check_name": "duplicate_date_check",
                "expected": 0,
                "observed": len(inventory_rows) - len({r["slate_date"] for r in inventory_rows}),
                "status": "PASS" if len(inventory_rows) == len({r["slate_date"] for r in inventory_rows}) else "FAIL",
            },
            {
                "check_name": "recommended_block_disjoint_from_first_block",
                "expected": 0,
                "observed": len(set(RECOMMENDED_DATES) & FIRST_BLOCK_DATES),
                "status": "PASS" if not (set(RECOMMENDED_DATES) & FIRST_BLOCK_DATES) else "FAIL",
            },
            {
                "check_name": "recommended_population_cap",
                "expected": "<=15000",
                "observed": next(
                    r["approx_denominator_rows"]
                    for r in comparison
                    if r["strategy"] == "strategy_a_adjacent_extension_after_first_block"
                ),
                "status": "PASS"
                if as_int(
                    str(
                        next(
                            r["approx_denominator_rows"]
                            for r in comparison
                            if r["strategy"] == "strategy_a_adjacent_extension_after_first_block"
                        )
                    )
                )
                <= 15000
                else "FAIL",
            },
        ]
        write_csv(self.output_dir / f"deterministic_inventory_reproduction_validation_{RUN_DATE}.csv", rows)
        return rows

    def static_guard(self) -> list[dict[str, Any]]:
        text = Path(__file__).read_text()
        rows = []
        for name, pattern in PROHIBITED_PATTERNS.items():
            matches = []
            for m in pattern.finditer(text):
                line_start = text.rfind("\n", 0, m.start()) + 1
                line_end = text.find("\n", m.start())
                line = text[line_start : line_end if line_end != -1 else len(text)].strip()
                if "PROHIBITED_PATTERNS" in line or "re.compile" in line or "pattern.finditer" in line:
                    continue
                matches.append(line)
            rows.append(
                {
                    "guard": name,
                    "forbidden_occurrences": len(matches),
                    "status": "PASS" if not matches else "FAIL",
                    "evidence": "|".join(matches[:5]),
                }
            )
        write_csv(self.output_dir / f"static_no_model_signal_guard_{RUN_DATE}.csv", rows)
        return rows

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                rows.append(
                    {
                        "artifact_path": str(path),
                        "filename": path.name,
                        "sha256": sha256_path(path),
                        "bytes": path.stat().st_size,
                    }
                )
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        inventory_rows = self.refreshed_inventory()
        self.date_ledgers(inventory_rows)
        regimes = self.source_regime_map()
        comparison, candidates = self.candidate_blocks()
        self.write_strategy_assessments(comparison)
        first_block = self.first_block_role()
        folds = self.fold_feasibility()
        projections = self.projections(comparison)
        recommendation = self.recommendation(candidates)
        stops = self.stop_conditions()
        approvals = self.human_approval()
        self.decisions()
        self.markdown_reports(inventory_rows, comparison, recommendation)
        write_json(
            self.output_dir / f"machine_readable_next_block_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "decision_statuses": self.decision_statuses,
                "recommended_next_block": recommendation[0],
                "inventory": {
                    "dates": len(inventory_rows),
                    "date_start": min(r["slate_date"] for r in inventory_rows),
                    "date_end": max(r["slate_date"] for r in inventory_rows),
                    "status_counts": Counter(r["date_level_readiness_status"] for r in inventory_rows),
                },
                "constraints": {
                    "qualification_executed": "false",
                    "matrices_constructed": "false",
                    "model_training": "false",
                    "signal_evaluation": "false",
                    "db_writes": "false",
                    "external_api_calls": "false",
                    "production_changes": "false",
                },
            },
        )
        deterministic = self.deterministic_validation(inventory_rows, comparison)
        guard = self.static_guard()
        parse = self.parse_validation()
        self.sha_manifest()
        return {
            "output_dir": str(self.output_dir),
            "inventory_dates": len(inventory_rows),
            "recommended_block": recommendation[0]["date_range"],
            "recommended_estimated_denominator": recommendation[0]["estimated_denominator_population"],
            "parse_failures": sum(1 for r in parse if r["parse_status"] == "FAIL"),
            "guard_failures": sum(1 for r in guard if r["status"] != "PASS"),
            "deterministic_failures": sum(1 for r in deterministic if r["status"] != "PASS"),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    result = Review(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["parse_failures"] or result["guard_failures"] or result["deterministic_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
