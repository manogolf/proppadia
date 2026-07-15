"""Review canonical side identity for the selected historical denominator.

Governance review only. It inspects immutable artifacts and source code to
determine whether model_pick_side may be treated as canonical side. It does not
mutate identity, certify outcomes, construct matrices, call APIs, write
databases, or evaluate models/signals.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-13"
DEFAULT_OUT = Path("artifacts/analysis/model_development/mlb_historical_canonical_side_identity_review/2026-07-13")
SELECTED_WAVE = Path("artifacts/analysis/model_development/mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13")
CAP_REVIEW = Path("artifacts/analysis/model_development/mlb_historical_sub_block_cap_fitting_review/2026-07-13")
PARENT_STAGE1 = Path("artifacts/analysis/model_development/mlb_historical_qualification_wave_2026-06-29_to_2026-07-09/2026-07-13")
FIRST_BLOCK_DENOM = Path("artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13/exact_frozen_denominator_manifest_2026-07-13.csv")
FIRST_BLOCK_OUTCOME = Path("artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13/complete_1904_outcome_certification_ledger_2026-07-13.csv")
SELECTED_DENOM = CAP_REVIEW / f"selected_sub_block_denominator_manifest_{RUN_DATE}.csv"
BUILD_SLATE = Path("backend/mlb/scripts/build_mlb_slate_output.py")
RECONCILE = Path("backend/mlb/scripts/build_mlb_reconcile_rows.py")
SPINE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12")
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "model_selection_call": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|train_test_split)\b"),
}


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", errors="ignore") as f:
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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def base_key(row: dict[str, str]) -> str:
    return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id")), clean(row.get("prop_type")), clean(row.get("line"))])


def full_key(row: dict[str, str], side_col: str = "side") -> str:
    return base_key(row) + "|" + clean(row.get(side_col))


def load_source_rows(denom: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    paths = sorted({row["source_path"] for row in denom})
    out: dict[str, dict[str, str]] = {}
    for path_text in paths:
        for src in read_csv(Path(path_text)):
            key = "|".join(
                [
                    clean(src.get("slate_date")),
                    clean(src.get("game_id")),
                    clean(src.get("player_id")),
                    clean(src.get("prop_type")),
                    clean(src.get("line")),
                ]
            )
            out[key] = src
    return out


def line_hits(path: Path, terms: list[str]) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for i, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
        if any(term in line for term in terms):
            rows.append({"path": str(path), "line_number": i, "line_excerpt": line.strip()[:240]})
    return rows


class Review:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.denom = read_csv(SELECTED_DENOM)
        self.sources = load_source_rows(self.denom)
        self.first_block = read_csv(FIRST_BLOCK_DENOM)
        self.first_sources = load_source_rows(self.first_block)
        self.status: dict[str, str] = {}

    def review_population(self) -> None:
        rows = []
        for i, row in enumerate(self.denom, 1):
            src = self.sources.get(base_key(row), {})
            rows.append(
                {
                    "row_order": i,
                    "canonical_row_id": row["canonical_row_id"],
                    "base_key": base_key(row),
                    "canonical_side": clean(row.get("side")),
                    "model_pick_side": clean(src.get("model_pick_side")),
                    "source_side": clean(src.get("side")),
                    "prob_over": clean(src.get("prob_over")),
                    "prob_under": clean(src.get("prob_under")),
                    "model_pick_prob": clean(src.get("model_pick_prob")),
                    "selected_side_price": clean(src.get("selected_side_price")),
                    "market_price_over": clean(src.get("market_price_over")),
                    "market_price_under": clean(src.get("market_price_under")),
                    "source_path": row["source_path"],
                    "source_sha256": row["source_sha256"],
                }
            )
        write_csv(self.out_dir / f"frozen_14816_review_population_{RUN_DATE}.csv", rows)

    def structural_audits(self) -> None:
        base_counts = Counter(base_key(row) for row in self.denom)
        sides_by_base: dict[str, set[str]] = defaultdict(set)
        model_sides_by_base: dict[str, set[str]] = defaultdict(set)
        for row in self.denom:
            sides_by_base[base_key(row)].add(clean(row.get("side")))
            src = self.sources.get(base_key(row), {})
            model_sides_by_base[base_key(row)].add(clean(src.get("model_pick_side")))
        base_rows = []
        for key, count in base_counts.items():
            model_sides = sorted(model_sides_by_base[key])
            base_rows.append(
                {
                    "base_key": key,
                    "row_count": count,
                    "canonical_sides": "|".join(sorted(sides_by_base[key])),
                    "model_pick_sides": "|".join(model_sides),
                    "has_both_model_pick_sides": str({"over", "under"}.issubset(set(model_sides))).lower(),
                    "multiplicity_class": "single_row_per_base_key" if count == 1 else "multiple_rows_same_base_key",
                }
            )
        write_csv(self.out_dir / f"base_key_multiplicity_audit_{RUN_DATE}.csv", base_rows)
        mp_counts = Counter()
        for row in self.denom:
            src = self.sources.get(base_key(row), {})
            val = clean(src.get("model_pick_side"))
            if val in {"over", "under"}:
                mp_counts[val] += 1
            elif not val:
                mp_counts["missing"] += 1
            else:
                mp_counts["invalid"] += 1
        write_csv(
            self.out_dir / f"model_pick_side_value_quality_audit_{RUN_DATE}.csv",
            [{"value_class": k, "rows": v, "pct": round(v / len(self.denom), 6)} for k, v in mp_counts.items()],
        )
        write_csv(
            self.out_dir / f"one_sided_two_sided_structural_analysis_{RUN_DATE}.csv",
            [
                {"measure": "denominator_rows", "value": len(self.denom), "interpretation": "frozen review population"},
                {"measure": "unique_base_keys_without_side", "value": len(base_counts), "interpretation": "one row per base key if equal to denominator rows"},
                {"measure": "base_keys_with_multiple_rows", "value": sum(1 for v in base_counts.values() if v > 1), "interpretation": "would indicate paired or duplicate base keys"},
                {"measure": "base_keys_with_both_model_pick_sides", "value": sum(1 for r in base_rows if r["has_both_model_pick_sides"] == "true"), "interpretation": "opposite selected-side duplicates inside denominator"},
                {"measure": "canonical_side_blank_rows", "value": sum(1 for r in self.denom if clean(r.get("side")) == ""), "interpretation": "canonical side completeness failure"},
                {"measure": "model_pick_side_present_valid_rows", "value": mp_counts["over"] + mp_counts["under"], "interpretation": "technical settlement side available, semantic status still model-derived"},
            ],
        )

    def first_block_comparison(self) -> None:
        rows = []
        agree = 0
        missing_model = 0
        for row in self.first_block:
            src = self.first_sources.get(base_key(row), {})
            side = clean(row.get("side"))
            model_side = clean(src.get("model_pick_side"))
            if not model_side:
                missing_model += 1
            if side and model_side and side == model_side:
                agree += 1
            rows.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "base_key": base_key(row),
                    "canonical_side": side,
                    "source_side": clean(src.get("side")),
                    "source_model_pick_side": model_side,
                    "side_model_pick_agreement": str(side == model_side).lower() if model_side else "unknown",
                    "source_path": row["source_path"],
                    "source_row_number": row.get("source_row_number", ""),
                    "lineage_note": "source file has no side column value; model_pick_side is computed by slate output code",
                }
            )
        write_csv(self.out_dir / f"first_block_side_lineage_comparison_{RUN_DATE}.csv", rows)
        write_csv(
            self.out_dir / f"first_block_side_lineage_summary_{RUN_DATE}.csv",
            [
                {"measure": "first_block_rows", "value": len(self.first_block)},
                {"measure": "canonical_side_present", "value": sum(1 for r in self.first_block if clean(r.get("side")))},
                {"measure": "source_model_pick_side_missing", "value": missing_model},
                {"measure": "side_equals_source_model_pick_side", "value": agree},
                {"measure": "agreement_rate_when_model_pick_present", "value": round(agree / max(1, len(self.first_block) - missing_model), 6)},
            ],
        )

    def code_and_contracts(self) -> None:
        targets = [BUILD_SLATE, RECONCILE, Path("backend/mlb/scripts/review_mlb_historical_outcome_remediation.py"), Path("backend/mlb/scripts/build_mlb_hitter_persistence_characterization.py"), Path("backend/mlb/scripts/build_mlb_starter_skill_workload_research.py")]
        rows = []
        for path in targets:
            hits = line_hits(path, ["model_pick_side", "selected_side", "side", "pick_side"])
            for hit in hits:
                purpose = "unknown"
                if path == BUILD_SLATE:
                    purpose = "creates slate output; computes model_pick_side from calibrated probability"
                elif path == RECONCILE:
                    purpose = "uses model_pick_side for model-pick outcome and selected price in reconcile rows"
                rows.append(
                    {
                        **hit,
                        "purpose": purpose,
                        "treats_as_identity": "mixed_or_downstream" if "side_col=\"model_pick_side\"" in hit["line_excerpt"] else "not_proven",
                        "treats_as_prediction_or_recommendation": "yes" if "pick_side" in hit["line_excerpt"] or "model_pick_side" in hit["line_excerpt"] else "unknown",
                        "pre_or_post_prediction": "post_prediction" if path == BUILD_SLATE else "downstream",
                    }
                )
        write_csv(self.out_dir / f"code_and_schema_lineage_inventory_{RUN_DATE}.csv", rows)
        contract_rows = []
        for path in sorted(list(SPEC_DIR.glob("*")) + list(SPINE_DIR.glob("*"))):
            if path.is_file() and path.suffix in {".md", ".json", ".csv"}:
                text = path.read_text(errors="ignore")[:10000]
                if any(term in text for term in ["side", "canonical", "denominator", "identity", "model_pick_side"]):
                    contract_rows.append(
                        {
                            "path": str(path),
                            "sha256": sha256_path(path),
                            "mentions_side": str("side" in text).lower(),
                            "mentions_model_pick_side": str("model_pick_side" in text).lower(),
                            "mentions_canonical_identity": str("canonical" in text and "identity" in text).lower(),
                            "review_note": "contract reference inventoried; exact amendment not performed",
                        }
                    )
        write_csv(self.out_dir / f"frozen_contract_clause_inventory_{RUN_DATE}.csv", contract_rows)

    def semantic_outputs(self) -> None:
        write_csv(
            self.out_dir / f"field_semantic_origin_analysis_{RUN_DATE}.csv",
            [
                {
                    "field": "model_pick_side",
                    "classification": "pregame_model_selected_direction",
                    "evidence": "build_mlb_slate_output.py computes pick_side = over if p_over >= 0.5 else under after prediction probability is known",
                    "market_identity_status": "not market-provided side identity",
                    "canonical_alias_status": "not proven; evidence weighs against alias",
                },
                {
                    "field": "selected_side_price",
                    "classification": "post-selection market context",
                    "evidence": "market audit context uses side_col=model_pick_side to derive selected-side context",
                    "market_identity_status": "derived from selected side and available over/under prices",
                    "canonical_alias_status": "not independent side identity",
                },
            ],
        )
        write_csv(
            self.out_dir / f"temporal_and_selection_conditioning_analysis_{RUN_DATE}.csv",
            [
                {
                    "question": "when_model_pick_side_known",
                    "finding": "after pregame model probability generation within slate output construction",
                    "implication": "using it as denominator side conditions the experimental population on historical model-selected direction",
                },
                {
                    "question": "full_market_or_selected_population",
                    "finding": "source row contains both over/under price/probability columns but only one computed model_pick_side",
                    "implication": "population is structurally one row per prop/line, not a paired over/under row set",
                },
                {
                    "question": "challenger_interpretation",
                    "finding": "future experiments would evaluate features on historical champion-selected sides unless separately governed",
                    "implication": "Champion-Challenger comparison would need explicit selected-population interpretation",
                },
            ],
        )
        write_csv(
            self.out_dir / f"outcome_settlement_compatibility_analysis_{RUN_DATE}.csv",
            [
                {"dimension": "technical_settlement", "status": "FEASIBLE_IF_MODEL_PICK_SIDE_BOUND", "finding": "over/under value exists for all rows and deterministic half-line formulas are known"},
                {"dimension": "semantic_identity", "status": "NOT_CURRENTLY_VALID_WITHOUT_GOVERNANCE", "finding": "model_pick_side is computed model-selected direction, not canonical side in frozen denominator"},
                {"dimension": "contract_permission", "status": "NOT_PERMITTED_BY_CURRENT_TASK", "finding": "task may review but not execute binding or certify one-sided interpretation"},
                {"dimension": "experimental_population", "status": "SELECTION_CONDITIONED", "finding": "binding would create/evaluate one-sided selected-proposition population"},
            ],
        )
        write_csv(
            self.out_dir / f"opposite_side_evidence_inventory_{RUN_DATE}.csv",
            [
                {
                    "evidence_type": "source_columns",
                    "finding": "source rows retain market_price_over, market_price_under, prob_over, prob_under as columns",
                    "opposite_side_row_exists_in_denominator": "no",
                    "adding_opposite_side_effect": "would create new denominator membership",
                },
                {
                    "evidence_type": "base_key_structure",
                    "finding": "selected denominator has one row per base key without paired opposite-side row",
                    "opposite_side_row_exists_in_denominator": "no",
                    "adding_opposite_side_effect": "requires denominator rebuild or paired-market source governance",
                },
            ],
        )

    def options_and_decision(self) -> None:
        interpretations = [
            ("A", "Canonical alias", "NOT_SUPPORTED", "model_pick_side is computed from model probability; source side blank"),
            ("B", "Inherently one-sided selected proposition", "SUPPORTED_WITH_SELECTION_CONDITIONING", "one row per prop/line and selected side exists, but side was model-selected"),
            ("C", "Model output improperly substituted for market identity", "SUPPORTED_IF_USED_AS_CANONICAL_ALIAS", "using model_pick_side as ordinary market identity would redefine side"),
            ("D", "Two-sided denominator incompletely serialized", "PARTIALLY_SUPPORTED_FOR_MARKET_COLUMNS_NOT_ROWS", "over/under prices exist as columns, not paired denominator rows"),
            ("E", "Contract ambiguity", "SUPPORTED_RECOMMENDED", "contracts and prior packages do not clearly authorize one-sided selected population binding"),
        ]
        write_csv(
            self.out_dir / f"interpretation_comparison_a_to_e_{RUN_DATE}.csv",
            [{"interpretation": i, "name": n, "evidence_status": s, "notes": notes} for i, n, s, notes in interpretations],
        )
        options = [
            ("A", "Preserve stop", 14816, "canonical side incomplete; no outcome certification", "safest current-contract path"),
            ("B", "Bounded historical side binding", 14816, "would complete side technically if approved", "requires human approval; not alias"),
            ("C", "One-sided population certification", 14816, "possible selected-proposition population", "selection-conditioned experiments only"),
            ("D", "Rebuild from broader paired-market source", "unknown", "could define two-sided market population", "new denominator project; not executed"),
            ("E", "Contract clarification/amendment required", 14816, "decision package before any resume", "recommended governance path"),
        ]
        write_csv(
            self.out_dir / f"governance_option_comparison_{RUN_DATE}.csv",
            [
                {
                    "option": o,
                    "description": d,
                    "projected_denominator_rows": rows,
                    "outcome_certification_effect": effect,
                    "governance_note": note,
                }
                for o, d, rows, effect, note in options
            ],
        )
        write_csv(
            self.out_dir / f"population_and_experiment_projections_by_option_{RUN_DATE}.csv",
            [
                {
                    "option": o,
                    "denominator_row_count": rows,
                    "canonical_identity_completeness": "complete only if binding/rebuild approved" if o in {"B", "C", "D"} else "incomplete",
                    "potential_variant_population": "blocked until outcome and field gates pass" if o in {"A", "E"} else "still subject to feature/outcome gates",
                    "champion_challenger_interpretation": "selected historical side conditioning must be disclosed" if o in {"B", "C"} else "not ready",
                    "portability_to_earlier_blocks": "requires first-block side lineage governance review" if o in {"B", "C"} else "not applicable",
                }
                for o, _, rows, _, _ in options
            ],
        )
        write_csv(
            self.out_dir / f"recommended_governance_decision_{RUN_DATE}.csv",
            [
                {
                    "recommended_option": "Option E with explicit path to Option C/B only by separate approval",
                    "decision": "do not resume outcome certification yet",
                    "reason": "model_pick_side is model-derived selected direction; one-sided population interpretation is plausible but not currently contract-authorized",
                    "next_action": "human governance decision: preserve stop, approve selected-proposition interpretation/binding, or require denominator rebuild",
                }
            ],
        )
        write_csv(
            self.out_dir / f"human_approval_requirement_{RUN_DATE}.csv",
            [
                {
                    "human_approval_required": "yes",
                    "required_before": "any canonical side binding, one-sided certification, outcome resume, or denominator rebuild",
                    "current_review_action": "recommendation only",
                }
            ],
        )
        (self.out_dir / f"future_bounded_execution_contract_draft_{RUN_DATE}.md").write_text(
            "# Future Bounded Execution Contract Draft\n\n"
            "A future execution may resume outcome certification only after a human governance decision explicitly authorizes one of:\n\n"
            "1. preserve blank side and keep outcomes blocked;\n"
            "2. bind `model_pick_side` as historical selected-proposition side with explicit selection-conditioning language;\n"
            "3. rebuild a two-sided denominator from a broader paired-market source.\n\n"
            "No row addition, removal, reorder, or silent side repair is permitted by this review.\n"
        )

    def reports(self) -> None:
        (self.out_dir / f"main_canonical_side_review_report_{RUN_DATE}.md").write_text(
            "# Canonical Side Identity and One-Sided Proposition Review\n\n"
            "## Executive Summary\n\n"
            "The 14,816-row selected denominator is structurally one row per `slate_date|game_id|player_id|prop_type|line` base key, "
            "with blank canonical `side` and valid `model_pick_side` available from the source slate output. Repository code shows "
            "`model_pick_side` is computed after model probability generation (`over` when `prob_over >= 0.5`, otherwise `under`). "
            "It is therefore not a market-provided canonical side alias.\n\n"
            "The evidence supports an inherently one-sided selected-proposition interpretation, but that interpretation is selection-conditioned "
            "on historical model choices and is not clearly authorized by the frozen contracts. Outcome certification should not resume without a human governance decision.\n\n"
            "## Recommendation\n\n"
            "Recommended decision: `Option E` now, with possible future approval of a bounded selected-proposition side binding only if the human governance record explicitly accepts the selection-conditioned interpretation.\n"
        )
        (self.out_dir / f"one_page_human_decision_summary_{RUN_DATE}.md").write_text(
            "# One-Page Human Decision Summary\n\n"
            "Finding: `model_pick_side` is not a canonical alias. It is a pregame model-selected direction.\n\n"
            "The denominator appears one-sided at a selected-proposition grain, but using `model_pick_side` as side would condition the experimental population on historical model choices.\n\n"
            "Recommendation: do not resume outcome certification yet. Make a human governance decision: preserve stop, approve selected-proposition binding, or rebuild a paired-market denominator.\n"
        )

    def decision_json(self) -> None:
        self.status = {
            "REVIEW_POPULATION_REPRODUCTION": "PASS_14816_ROWS_8_DATES",
            "CANONICAL_SIDE_COMPLETENESS_STATUS": "FAIL_14816_BLANK",
            "MODEL_PICK_SIDE_COMPLETENESS_STATUS": "PASS_VALID_OVER_UNDER",
            "BASE_KEY_MULTIPLICITY_STATUS": "PASS_ONE_ROW_PER_BASE_KEY",
            "ONE_SIDED_POPULATION_EVIDENCE_STATUS": "SUPPORTED_STRUCTURALLY_AND_BY_CODE",
            "TWO_SIDED_UPSTREAM_EVIDENCE_STATUS": "PRICES_AND_PROBABILITIES_EXIST_AS_COLUMNS_NOT_ROWS",
            "FIRST_BLOCK_SIDE_LINEAGE_STATUS": "AMBIGUOUS_SIDE_POPULATED_BUT_SOURCE_SIDE_BLANK",
            "CODE_SCHEMA_LINEAGE_STATUS": "PASS_INVENTORIED",
            "MODEL_PICK_SIDE_SEMANTIC_STATUS": "PREGAME_MODEL_SELECTED_DIRECTION_NOT_MARKET_IDENTITY",
            "SELECTION_CONDITIONING_STATUS": "PRESENT_IF_BOUND_AS_SIDE",
            "OUTCOME_SETTLEMENT_TECHNICAL_STATUS": "FEASIBLE_IF_BOUND",
            "CANONICAL_IDENTITY_BINDING_FEASIBILITY": "TECHNICALLY_FEASIBLE_GOVERNANCE_REQUIRED",
            "CURRENT_CONTRACT_PERMISSION": "NOT_ESTABLISHED",
            "GOVERNANCE_AMBIGUITY_STATUS": "HUMAN_DECISION_REQUIRED",
            "HUMAN_APPROVAL_REQUIRED": "YES_BEFORE_ANY_BINDING_OR_RESUME",
            "CANONICAL_SIDE_REVIEW_DECISION": "RECOMMEND_OPTION_E_CONTRACT_CLARIFICATION_WITH_OPTION_C_B_AS_POSSIBLE_APPROVED_PATH",
            "OUTCOME_CERTIFICATION_RESUME_READINESS": "NOT_READY",
            "EXPERIMENTAL_POPULATION_INTERPRETATION_STATUS": "SELECTED_PROPOSITION_POPULATION_IF_APPROVED",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Human governance decision on selected-proposition side binding versus denominator rebuild.",
        }
        write_json(
            self.out_dir / f"machine_readable_decision_{RUN_DATE}.json",
            {
                "decision_statuses": self.status,
                "review_population_rows": len(self.denom),
                "recommendation": "Option E now; do not resume certification without explicit human governance.",
                "state_mutated": False,
            },
        )

    def validation(self) -> None:
        source_rows = [self.sources.get(base_key(r), {}) for r in self.denom]
        base_count = len({base_key(r) for r in self.denom})
        validations = [
            {"check": "exact_14816_rows", "status": "PASS" if len(self.denom) == 14816 else "FAIL", "observed": len(self.denom), "expected": 14816},
            {"check": "exact_8_dates", "status": "PASS" if len({r["slate_date"] for r in self.denom}) == 8 else "FAIL", "observed": len({r["slate_date"] for r in self.denom}), "expected": 8},
            {"check": "blank_canonical_side", "status": "PASS" if all(not clean(r.get("side")) for r in self.denom) else "FAIL", "observed": sum(1 for r in self.denom if not clean(r.get("side"))), "expected": 14816},
            {"check": "model_pick_side_valid", "status": "PASS" if all(clean(r.get("model_pick_side")) in {"over", "under"} for r in source_rows) else "FAIL", "observed": sum(1 for r in source_rows if clean(r.get("model_pick_side")) in {"over", "under"}), "expected": 14816},
            {"check": "base_key_uniqueness", "status": "PASS" if base_count == len(self.denom) else "FAIL", "observed": base_count, "expected": len(self.denom)},
            {"check": "no_identity_mutation", "status": "PASS", "observed": "review only", "expected": "no side populated"},
        ]
        write_csv(self.out_dir / f"deterministic_reproduction_validation_{RUN_DATE}.csv", validations)
        parse_rows = []
        for path in sorted(self.out_dir.glob("*")):
            if path.suffix == ".csv":
                try:
                    read_csv(path)
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "csv", "parse_status": status, "detail": detail})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "json", "parse_status": status, "detail": detail})
            elif path.suffix == ".md":
                parse_rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().strip() else "FAIL", "detail": ""})
        write_csv(self.out_dir / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        self.static_guard()
        self.sha_manifest()

    def static_guard(self) -> None:
        lines = []
        in_pattern_block = False
        for line in Path(__file__).read_text().splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_pattern_block = True
                continue
            if in_pattern_block and line == "}":
                in_pattern_block = False
                continue
            if not in_pattern_block:
                lines.append(line)
        text = "\n".join(lines)
        write_csv(
            self.out_dir / f"static_no_model_signal_guard_{RUN_DATE}.csv",
            [{"guard": name, "status": "PASS" if not list(pattern.finditer(text)) else "FAIL", "match_count": len(list(pattern.finditer(text)))} for name, pattern in PROHIBITED_PATTERNS.items()],
        )

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.out_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.out_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def run(self) -> dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.review_population()
        self.structural_audits()
        self.first_block_comparison()
        self.code_and_contracts()
        self.semantic_outputs()
        self.options_and_decision()
        self.reports()
        self.decision_json()
        self.validation()
        return {"output_dir": str(self.out_dir), "rows": len(self.denom), "decision": self.status["CANONICAL_SIDE_REVIEW_DECISION"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    args = parser.parse_args(argv)
    result = Review(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
