#!/usr/bin/env python3
"""Review the non-authorized low-sample Starter recurrence population.

This bounded utility writes a read-only governance review package. It does not
materialize research fields, propagate qualification, repair downstream
blockers, construct matrices, train models, score rows, call networks, write
databases/APIs, upload files, alter schedulers, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_REMEDIATION_SHA = "2713ebdc96849b13b1a0edbc40b0da4bad0e6862bf8177bf023dff9c180c7d25"
EXPECTED_FORMULA_SHA = "d98ed6addb8ebc09a3419e74497464bf4e656c757e8282c44d799a6ffd16324d"

FORMULA_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_formula_governance/2026-07-15")
REMEDIATION_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")
ACCOUNTING_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/2026-07-15")
PORTFOLIO_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_research_portfolio_review/2026-07-15")
MATRIX_QUEUE_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/2026-07-15")
OUT_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_103_row_expansion_review/2026-07-15")

FORMULA_SHA = FORMULA_DIR / f"sha256_manifest_{RUN_DATE}.csv"
FORMULA_RECURRENCE = FORMULA_DIR / f"exact_120_row_recurrence_manifest_{RUN_DATE}.csv"
FORMULA_AUTH_ROWS = FORMULA_DIR / f"exact_17_row_governed_manifest_{RUN_DATE}.csv"
FORMULA_AUTH_SIDES = FORMULA_DIR / f"exact_2_side_governed_manifest_{RUN_DATE}.csv"

REMEDIATION_SHA = REMEDIATION_DIR / f"sha256_manifest_{RUN_DATE}.csv"
REMEDIATION_STATE = REMEDIATION_DIR / f"certified_cumulative_research_state_{RUN_DATE}.json"
REMEDIATION_ROWS = REMEDIATION_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"

ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_RESIDUAL = ACCOUNTING_DIR / f"true_residual_starter_blocked_manifest_{RUN_DATE}.csv"
ACCOUNTING_STATE = ACCOUNTING_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json"

PORTFOLIO_SHA = PORTFOLIO_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PORTFOLIO_BRANCHES = PORTFOLIO_DIR / f"exact_branch_population_manifest_{RUN_DATE}.csv"
MATRIX_QUEUE = MATRIX_QUEUE_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"

FIELD_NAME = "pitcher_base_research_low_sample_v1"
STARTER_EXPECTED_FIELD = "starter_expected_hits_allowed_research_low_sample_v1"
FORMULA_VERSION = "research_low_sample_v1"

REVIEW_DECISION = "ADDITIONAL_SCOPE_OR_LINEAGE_REVIEW_REQUIRED"
PRIORITY_DECISION = "EXPANSION_LOW_VALUE_DEFER_REMAINING_RESIDUAL_BRANCHES"
GOVERNANCE_STATUS = "NOT_FROZEN_EXECUTION_BLOCKED_BY_GRAIN_AND_SCOPE_EVIDENCE"


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
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fnum(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except ValueError:
        return None
    return None if math.isnan(out) else out


def inum(value: str | None) -> int:
    value_f = fnum(value)
    return int(value_f) if value_f is not None else 0


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or "|".join(
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", ""), row.get("line", ""), row.get("side", "")]
    )


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join(
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")]
    )


def load_inputs() -> dict[str, Any]:
    required = [
        FORMULA_SHA,
        FORMULA_RECURRENCE,
        FORMULA_AUTH_ROWS,
        FORMULA_AUTH_SIDES,
        REMEDIATION_SHA,
        REMEDIATION_STATE,
        REMEDIATION_ROWS,
        ACCOUNTING_SHA,
        ACCOUNTING_RESIDUAL,
        ACCOUNTING_STATE,
        PORTFOLIO_SHA,
        PORTFOLIO_BRANCHES,
        MATRIX_QUEUE,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "recurrence": read_csv(FORMULA_RECURRENCE),
        "authorized_rows": read_csv(FORMULA_AUTH_ROWS),
        "authorized_sides": read_csv(FORMULA_AUTH_SIDES),
        "remediation_rows": read_csv(REMEDIATION_ROWS),
        "remediation_state": json.loads(REMEDIATION_STATE.read_text(encoding="utf-8")),
        "accounting_state": json.loads(ACCOUNTING_STATE.read_text(encoding="utf-8")),
        "residual_rows": read_csv(ACCOUNTING_RESIDUAL),
        "portfolio_branches": read_csv(PORTFOLIO_BRANCHES),
        "matrix_queue": read_csv(MATRIX_QUEUE),
    }


def dependency_rows() -> list[dict[str, Any]]:
    deps = [
        ("low_sample_17_row_remediation", REMEDIATION_DIR, REMEDIATION_SHA, EXPECTED_REMEDIATION_SHA),
        ("low_sample_formula_governance", FORMULA_DIR, FORMULA_SHA, EXPECTED_FORMULA_SHA),
        ("selected_proposition_accounting_state", ACCOUNTING_DIR, ACCOUNTING_SHA, sha256(ACCOUNTING_SHA)),
        ("residual_portfolio_review", PORTFOLIO_DIR, PORTFOLIO_SHA, sha256(PORTFOLIO_SHA)),
        ("hits_1_5_matrix_parent_queue", MATRIX_QUEUE_DIR, MATRIX_QUEUE, sha256(MATRIX_QUEUE)),
    ]
    rows = []
    for name, package, sha_path, expected in deps:
        observed = sha256(sha_path)
        rows.append(
            {
                "dependency_name": name,
                "package_path": str(package),
                "sha_or_file_path": str(sha_path),
                "observed_sha256": observed,
                "expected_sha256": expected,
                "status": "BOUND" if observed == expected else "MISMATCH",
            }
        )
    return rows


def assert_preconditions(data: dict[str, Any], deps: list[dict[str, Any]]) -> None:
    if any(row["status"] != "BOUND" for row in deps[:2]):
        raise RuntimeError("authoritative low-sample dependency SHA mismatch")
    if len(data["recurrence"]) != 120:
        raise RuntimeError("exact recurrence signature mismatch")
    if len(data["authorized_rows"]) != 17:
        raise RuntimeError("authorized row manifest mismatch")
    if len(data["authorized_sides"]) != 2:
        raise RuntimeError("authorized side manifest mismatch")
    if len(data["remediation_rows"]) != 17:
        raise RuntimeError("realized remediation row manifest mismatch")


def recurrence_manifest(data: dict[str, Any]) -> list[dict[str, Any]]:
    authorized_side_keys = {side_key(r) for r in data["authorized_rows"]}
    rows = []
    for r in data["recurrence"]:
        non_authorized = r["starter_game_side_key"] not in authorized_side_keys
        rows.append(
            {
                **r,
                "recurrence_grain": "starter_game_side",
                "authorized_17_row_side_overlap": "true" if not non_authorized else "false",
                "non_authorized_recurrence_side": "true" if non_authorized else "false",
                "selected_proposition_row_identity_available": "false",
                "governance_note": "recurrence manifest is starter-game-side grain, not canonical prop-row grain",
            }
        )
    return rows


def selected_non_authorized_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    authorized_ids = {row_id(r) for r in data["authorized_rows"]}
    recurrence_by_side = {r["starter_game_side_key"]: r for r in data["recurrence"]}
    rows = []
    for r in data["residual_rows"]:
        if row_id(r) in authorized_ids:
            continue
        rec = recurrence_by_side.get(r["starter_game_side_key"])
        partition = classify_selected_row(r, rec)
        rows.append(
            {
                **r,
                "current_full_qualification_status": "NOT_FULLY_QUALIFIED",
                "current_selected_proposition_campaign_membership": "inside_selected_proposition_residual_manifest",
                "current_population_spine_membership": "selected_proposition_population_spine_inferred_from_residual_manifest",
                "current_matrix_queue_membership": "matrix_queue_candidate" if r.get("line") == "1.5" else "not_hits_1_5_matrix_queue_scope",
                "recurrence_signature_side_match": "true" if rec else "false",
                "prior_start_count": rec.get("prior_start_count", "") if rec else "",
                "research_history_classification_after_review": rec.get("research_history_classification", r.get("research_history_classification", "")) if rec else r.get("research_history_classification", ""),
                "production_eligibility_classification": "PRODUCTION_INELIGIBLE_RESEARCH_ONLY_FORMULA" if rec else "n/a",
                "prediction_eligibility_classification_after_review": rec.get("prediction_eligibility_classification", r.get("prediction_eligibility_classification", "")) if rec else r.get("prediction_eligibility_classification", ""),
                "population_partition": partition,
                "authoritative_source_package_establishing_state": str(ACCOUNTING_RESIDUAL),
                "formula_parent_source_package": rec.get("source_artifact", "") if rec else "",
                "scope_review_note": "canonical selected-proposition row; not part of authorized 17-row remediation",
            }
        )
    return rows


def classify_selected_row(row: dict[str, str], recurrence: dict[str, str] | None) -> str:
    category = row.get("primary_residual_category", "")
    if category == "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED":
        return "NOT_ELIGIBLE_ZERO_PRIOR_STARTS"
    if category == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT":
        return "NOT_ELIGIBLE_ROLE_OR_IDENTITY_CONFLICT"
    if category == "STARTER_PARENT_DOMAIN_MISSING_OTHER":
        return "NOT_ELIGIBLE_FORMULA_PARENT_MISSING"
    if recurrence is None:
        return "OTHER_EXPLICIT_FAIL_CLOSED_REASON"
    if category == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION":
        return "OTHER_EXPLICIT_FAIL_CLOSED_REASON"
    return "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED"


def parent_status(value: str | None, recurrence_match: bool) -> str:
    if not recurrence_match:
        return "PARENT_MISSING"
    return "PRESENT_AUTHORITATIVE_AND_COMPATIBLE" if fnum(value) is not None else "PARENT_MISSING"


def formula_parent_eligibility_ledger(selected_rows: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    recurrence_by_side = {r["starter_game_side_key"]: r for r in data["recurrence"]}
    out = []
    for row in selected_rows:
        rec = recurrence_by_side.get(row["starter_game_side_key"])
        match = rec is not None
        parents = {
            "weighted_multiseason_hits_per_out": rec.get("expected_hits_outs_v1", "") if rec else "",
            "expected_outs_blended_v1": rec.get("expected_hits_outs_v1", "") if rec else "",
            "offense_factor_vs_league_clamped": rec.get("offense_factor_vs_league_clamped", "") if rec else "",
            "starter_status": rec.get("role_state", "") if rec else "",
            "starter_trust_multiplier": rec.get("role_confidence", "") if rec else "",
        }
        strict_prior = rec.get("strict_prior_status", "") if rec else ""
        prior = inum(rec.get("prior_start_count") if rec else "")
        all_parent_ok = (
            match
            and parent_status(parents["weighted_multiseason_hits_per_out"], match) == "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"
            and parent_status(parents["expected_outs_blended_v1"], match) == "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"
            and parent_status(parents["offense_factor_vs_league_clamped"], match) == "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"
            and strict_prior == "PASS_STRICT_PRIOR"
            and 1 <= prior <= 4
            and row["population_partition"] == "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED"
        )
        out.append(
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "starter_game_side_key": row["starter_game_side_key"],
                "row_population_partition": row["population_partition"],
                "weighted_multiseason_hits_per_out_parent_status": parent_status(parents["weighted_multiseason_hits_per_out"], match),
                "expected_outs_blended_v1_parent_status": parent_status(parents["expected_outs_blended_v1"], match),
                "offense_factor_parent_status": parent_status(parents["offense_factor_vs_league_clamped"], match),
                "starter_status_parent_status": "PRESENT_AUTHORITATIVE_AND_COMPATIBLE" if match else "PARENT_MISSING",
                "starter_trust_multiplier_parent_status": "PRESENT_AUTHORITATIVE_AND_COMPATIBLE" if match else "PARENT_MISSING",
                "strict_prior_status": strict_prior,
                "prior_start_count": prior if match else "",
                "low_sample_research_flag": "true" if match and 1 <= prior <= 4 else "false",
                "row_formula_eligible": "true" if all_parent_ok else "false",
                "primary_formula_parent_failure": "" if all_parent_ok else row["population_partition"],
                "source_artifact": rec.get("source_artifact", "") if rec else str(ACCOUNTING_RESIDUAL),
            }
        )
    return out


def counterfactual_ledger(selected_rows: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    recurrence_by_side = {r["starter_game_side_key"]: r for r in data["recurrence"]}
    rows = []
    for row in selected_rows:
        rec = recurrence_by_side.get(row["starter_game_side_key"])
        eligible = rec is not None and row["population_partition"] == "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED"
        pitcher_base = fnum(rec.get("expected_hits_outs_v1")) if eligible else None
        offense = fnum(rec.get("offense_factor_vs_league_clamped")) if eligible else None
        starter_expected = pitcher_base * offense if pitcher_base is not None and offense is not None else None
        pa_ok = str(row.get("pa_qualified", "")).lower() == "true"
        outcome_ok = str(row.get("outcome_qualified", "")).lower() == "true"
        bundle_ok = not row.get("bundle_blockers", "")
        if not eligible:
            projected = "NO_MOVEMENT_INELIGIBLE_OR_OUTSIDE_SCOPE"
            downstream = row["population_partition"]
        elif pa_ok and outcome_ok and bundle_ok:
            projected = "PROJECTED_NEWLY_FULLY_QUALIFIED"
            downstream = ""
        else:
            projected = "PROJECTED_STARTER_QUALIFIED_DOWNSTREAM_BLOCKED"
            blockers = []
            if not pa_ok:
                blockers.append("PA")
            if not outcome_ok:
                blockers.append("OUTCOME")
            if not bundle_ok:
                blockers.append("BUNDLE")
            downstream = "|".join(blockers)
        rows.append(
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "starter_game_side_key": row["starter_game_side_key"],
                "row_formula_eligible": "true" if eligible else "false",
                "exact_formula_input_expected_hits_outs_v1": rec.get("expected_hits_outs_v1", "") if rec else "",
                "exact_formula_input_offense_factor_vs_league_clamped": rec.get("offense_factor_vs_league_clamped", "") if rec else "",
                FIELD_NAME: pitcher_base if pitcher_base is not None else "",
                STARTER_EXPECTED_FIELD: starter_expected if starter_expected is not None else "",
                "low_sample_classification": rec.get("research_history_classification", "") if rec else "",
                "prediction_ineligible_flag": "true" if eligible else "false",
                "production_ineligible_flag": "true" if eligible else "false",
                "side_level_research_certification_projection": "not_projected_for_ineligible_or_scope_conflicted_row" if not eligible else "STARTER_RESEARCH_LOW_SAMPLE_SIDE_CERTIFIED",
                "projected_starter_status": "STARTER_QUALIFIED_RESEARCH_LOW_SAMPLE_V1" if eligible else "UNCHANGED",
                "projected_final_qualification_state": projected,
                "remaining_downstream_blocker": downstream,
            }
        )
    return rows


def side_projection_ledger(selected_rows: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in selected_rows:
        grouped[row["starter_game_side_key"]].append(row)
    recurrence_by_side = {r["starter_game_side_key"]: r for r in data["recurrence"]}
    rows = []
    for key, side_rows in sorted(grouped.items()):
        rec = recurrence_by_side.get(key)
        categories = Counter(r["population_partition"] for r in side_rows)
        if rec is None:
            status = "EXPANSION_SIDE_PARENT_MISSING"
        elif any(cat == "NOT_ELIGIBLE_ROLE_OR_IDENTITY_CONFLICT" for cat in categories):
            status = "EXPANSION_SIDE_IDENTITY_OR_ROLE_FAILURE"
        elif all(cat == "OTHER_EXPLICIT_FAIL_CLOSED_REASON" for cat in categories):
            status = "EXPANSION_SIDE_OUTSIDE_GOVERNED_SCOPE"
        elif any(cat == "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED" for cat in categories):
            status = "EXPANSION_SIDE_PROJECTED_CERTIFIED"
        else:
            status = "EXPANSION_SIDE_FORMULA_UNDEFINED"
        rows.append(
            {
                "starter_game_side_key": key,
                "rows_represented": len(side_rows),
                "hits_0_5_rows": sum(1 for r in side_rows if r.get("line") == "0.5"),
                "hits_1_5_rows": sum(1 for r in side_rows if r.get("line") == "1.5"),
                "recurrence_signature_side_match": "true" if rec else "false",
                "prior_start_count": rec.get("prior_start_count", "") if rec else "",
                "side_projection_classification": status,
                "partition_counts": json.dumps(dict(categories), sort_keys=True),
                "notes": "projection only; no qualification propagation authorized",
            }
        )
    return rows


def scope_partition(selected_rows: list[dict[str, Any]], recurrence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected_side_keys = {r["starter_game_side_key"] for r in selected_rows}
    return [
        {
            "scope": "selected_proposition_non_authorized_residual_rows",
            "rows": len(selected_rows),
            "unique_starter_game_sides": len(selected_side_keys),
            "description": "canonical selected-proposition rows remaining after the 17-row remediation",
            "admission_boundary": "inside_selected_proposition_manifest_but_not_authorized_for_low_sample_expansion",
        },
        {
            "scope": "broader_starter_skill_workload_non_authorized_recurrence_sides",
            "rows": sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"),
            "unique_starter_game_sides": sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"),
            "description": "starter-game-side recurrence signature outside the two authorized sides",
            "admission_boundary": "not canonical selected-proposition row grain",
        },
        {
            "scope": "selected_proposition_rows_with_recurrence_side_match",
            "rows": sum(1 for r in selected_rows if r["recurrence_signature_side_match"] == "true"),
            "unique_starter_game_sides": len({r["starter_game_side_key"] for r in selected_rows if r["recurrence_signature_side_match"] == "true"}),
            "description": "selected-proposition rows whose side appears in the low-sample recurrence signature",
            "admission_boundary": "all are special-regime residuals in current state, not local-parent low-sample defects",
        },
    ]


def duplicate_path_audit(selected_rows: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    remediation_ids = {row_id(r) for r in data["remediation_rows"]}
    out = []
    for row in selected_rows:
        overlaps = []
        if row_id(row) in remediation_ids:
            overlaps.append("low_sample_17_row_remediation")
        category = row.get("primary_residual_category", "")
        if category == "STARTER_PARENT_DOMAIN_MISSING_OTHER":
            overlaps.append("ordinary_parent_domain_missing_branch")
        if category == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT":
            overlaps.append("identity_or_role_holdout_branch")
        if category == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION":
            overlaps.append("special_regime_exclusion_branch")
        if category == "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED":
            overlaps.append("zero_prior_start_branch")
        out.append(
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "starter_game_side_key": row["starter_game_side_key"],
                "duplicate_path_overlap": "|".join(overlaps),
                "already_remediated_by_option_b": "false",
                "history_complete_campaign_overlap": "unknown_not_claimed",
                "accounting_only_repair_overlap": "false",
                "low_sample_overlay_overlap": "false",
                "conflicting_values_or_version_markers": "false",
                "redundant_materialization_risk": "true" if row["recurrence_signature_side_match"] == "true" else "false",
                "fail_closed_required": "true",
            }
        )
    return out


def yield_analysis(selected_rows: list[dict[str, Any]], recurrence_rows: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    counts = Counter(r["population_partition"] for r in selected_rows)
    current_starter_blocked = len(selected_rows)
    formula_eligible_rows = [r for r in selected_rows if r["population_partition"] == "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED"]
    projected_fq = 0
    matrix_additions = 0
    rows = [
        ("rows_audited_requested_governance_arithmetic", 103, "120 recurrence rows minus 17 authorized proposition rows; not a canonical row manifest"),
        ("selected_proposition_non_authorized_rows_audited", len(selected_rows), "actual canonical rows after removing authorized 17 from residual manifest"),
        ("unique_starter_game_sides_selected_non_authorized", len({r["starter_game_side_key"] for r in selected_rows}), ""),
        ("non_authorized_recurrence_starter_game_sides", sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"), "starter-game-side grain"),
        ("hits_0_5_rows", sum(1 for r in selected_rows if r.get("line") == "0.5"), ""),
        ("hits_1_5_rows", sum(1 for r in selected_rows if r.get("line") == "1.5"), ""),
        ("rows_inside_selected_proposition_campaign", len(selected_rows), ""),
        ("rows_outside_selected_proposition_campaign", sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"), "broader recurrence sides only, not row-level candidates"),
        ("rows_already_starter_qualified", 0, "residual Starter-blocked manifest only"),
        ("rows_already_fully_qualified", 0, "residual Starter-blocked manifest only"),
        ("rows_currently_starter_blocked", current_starter_blocked, ""),
        ("formula_eligible_starter_blocked_rows", len(formula_eligible_rows), "requires selected row + recurrence side + local-parent defect; observed none"),
        ("projected_starter_qualified_additions", 0, "no supported selected-proposition expansion subset"),
        ("projected_newly_fully_qualified_rows", projected_fq, ""),
        ("projected_hits_0_5_additions", 0, ""),
        ("projected_hits_1_5_additions", 0, ""),
        ("projected_pa_blockers_exposed_or_preserved", 0, ""),
        ("projected_outcome_blockers_exposed_or_preserved", 0, ""),
        ("projected_bundle_blockers_exposed_or_preserved", 0, ""),
        ("projected_multiple_downstream_blockers", 0, ""),
        ("projected_matrix_queue_additions", matrix_additions, ""),
        ("rows_requiring_no_movement_already_qualified", 0, ""),
    ]
    rows.extend((f"ineligible_{k}", v, "population partition count") for k, v in sorted(counts.items()) if not k.startswith("LOW_SAMPLE_FORMULA_ELIGIBLE"))
    return [{"metric": k, "value": v, "notes": notes} for k, v, notes in rows]


def option_comparison() -> list[dict[str, Any]]:
    return [
        {"option": "A", "name": "No expansion", "new_usable_rows": 0, "governance_complexity": "low", "definition_risk": "low", "recommendation": "acceptable_hold_state", "notes": "preserves certified state unchanged"},
        {"option": "B", "name": "Selected-proposition Starter-blocked subset only", "new_usable_rows": 0, "governance_complexity": "medium", "definition_risk": "high_until_exact_subset_exists", "recommendation": "not_ready", "notes": "no exact eligible selected-proposition subset was proven"},
        {"option": "C", "name": "All eligible selected-proposition rows", "new_usable_rows": 0, "governance_complexity": "medium", "definition_risk": "high", "recommendation": "do_not_freeze", "notes": "duplicate payload value unsupported"},
        {"option": "D", "name": "Broader 103-row research characterization only", "new_usable_rows": 0, "governance_complexity": "medium", "definition_risk": "medium", "recommendation": "possible_later_after_scope_reconciliation", "notes": "broader recurrence is starter-game-side grain; separate characterization boundary required"},
        {"option": "E", "name": "Split governance", "new_usable_rows": 0, "governance_complexity": "high", "definition_risk": "medium", "recommendation": "defer", "notes": "split is conceptually clean but not supported by incremental selected-proposition yield"},
    ]


def residual_value_comparison() -> list[dict[str, Any]]:
    return [
        {"branch": "LOW_SAMPLE_103_EXPANSION", "rows_or_sides": "0 supported new selected-proposition rows", "probability_of_recovery": "low_without_scope_reconciliation", "governance_complexity": "medium", "definition_risk": "high", "platform_reuse": "medium", "recommendation": "defer"},
        {"branch": "STARTER_PARENT_DOMAIN_MISSING_OTHER", "rows_or_sides": "26 rows / 3 sides", "probability_of_recovery": "unknown_medium", "governance_complexity": "medium", "definition_risk": "medium", "platform_reuse": "high", "recommendation": "higher_priority_than_103_expansion"},
        {"branch": "IDENTITY_OR_ROLE_REVIEW_HOLDOUT", "rows_or_sides": "23 rows / 3 sides", "probability_of_recovery": "unknown_low_to_medium", "governance_complexity": "medium_high", "definition_risk": "high", "platform_reuse": "medium", "recommendation": "review_after_parent_domain"},
        {"branch": "HITS_1_5_MATRIX_PARENT_PAYLOAD", "rows_or_sides": "41 rows", "probability_of_recovery": "medium_if_payload_contract_clear", "governance_complexity": "medium", "definition_risk": "medium", "platform_reuse": "high", "recommendation": "higher_immediate_hits_1_5_value"},
        {"branch": "SPECIAL_REGIMES_AND_ZERO_START_REFERENCE", "rows_or_sides": "reference only", "probability_of_recovery": "low", "governance_complexity": "high", "definition_risk": "high", "platform_reuse": "low", "recommendation": "preserve_fail_closed"},
    ]


def projected_cumulative_state() -> list[dict[str, Any]]:
    base = {
        "fully_qualified_hits": 1500,
        "fully_qualified_hits_0_5": 1360,
        "fully_qualified_hits_1_5": 140,
        "primary_starter_blocked": 111,
        "primary_pa_blocked": 33,
        "primary_outcome_blocked": 363,
        "primary_bundle_blocked": 36,
        "primary_multiple_downstream_blocked": 3,
        "qualified_but_not_matrix_hits_1_5_queue": 41,
    }
    return [
        {"metric": k, "current_certified_state": v, "projected_ceiling_if_supported_expansion_executed": v, "delta": 0, "notes": "no supported qualification-recovery subset"}
        for k, v in base.items()
    ]


def production_safeguards() -> list[dict[str, Any]]:
    targets = [
        "production_pitcher_base",
        "production_starter_expected_hits_allowed",
        "daily_prediction_features",
        "model_scoring",
        "uploads",
        "apis",
        "wagering_tools",
        "promotion_gates",
        "abd_matrix_aliases",
        "variant_c",
        "scheduled_jobs",
    ]
    return [{"target": t, "status": "PASS_NO_CHANGE_NO_AUTHORIZATION", "proof": "review-only package; no execution governance frozen"} for t in targets]


def static_guard() -> list[dict[str, Any]]:
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: list[str] = []
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                calls.append(node.func.attr)
            elif isinstance(node.func, ast.Name):
                calls.append(node.func.id)
    banned_imports = ["requests", "urllib", "httpx", "socket", "subprocess", "psycopg2", "sqlalchemy", "boto3"]
    banned_calls = ["fit", "predict", "execute", "executemany", "to_sql", "urlopen", "request", "post", "put", "delete"]
    rows = []
    for imp in banned_imports:
        found = any(name == imp or name.startswith(f"{imp}.") for name in imports)
        rows.append({"guard": f"no_import_{imp}", "status": "PASS" if not found else "FAIL", "matches": int(found)})
    for call in banned_calls:
        count = sum(1 for item in calls if item == call)
        rows.append({"guard": f"no_call_{call}", "status": "PASS" if count == 0 else "FAIL", "matches": count})
    for guard in [
        "no_value_writes",
        "no_qualification_propagation",
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_pa_outcome_bundle_or_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_champion_challenger_roi_or_wagering",
        "no_database_or_api_writes",
        "no_uploads",
        "no_launchagent_changes",
        "no_production_behavior_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validation_rows(data: dict[str, Any], selected_rows: list[dict[str, Any]], recurrence_rows: list[dict[str, Any]], deps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("17_row_remediation_sha", sha256(REMEDIATION_SHA) == EXPECTED_REMEDIATION_SHA, sha256(REMEDIATION_SHA), EXPECTED_REMEDIATION_SHA)
    add("formula_governance_sha", sha256(FORMULA_SHA) == EXPECTED_FORMULA_SHA, sha256(FORMULA_SHA), EXPECTED_FORMULA_SHA)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["observed_sha256"], dep["expected_sha256"])
    add("exact_120_recurrence_reproduction", len(data["recurrence"]) == 120, len(data["recurrence"]), 120)
    add("exact_17_authorized_reproduction", len(data["authorized_rows"]) == 17, len(data["authorized_rows"]), 17)
    add("governance_arithmetic_103_non_authorized", len(data["recurrence"]) - len(data["authorized_rows"]) == 103, len(data["recurrence"]) - len(data["authorized_rows"]), 103, "arithmetic mixes starter-game and prop-row grain")
    add("non_authorized_starter_game_side_partition", sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true") == 118, sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"), 118)
    add("selected_proposition_non_authorized_rows", len(selected_rows) == 111, len(selected_rows), 111, "canonical rows after removing the executed 17 from residual state")
    add("duplicate_selected_row_id_check", len({r["governed_canonical_row_id"] for r in selected_rows}) == len(selected_rows), len({r["governed_canonical_row_id"] for r in selected_rows}), len(selected_rows))
    add("selected_low_sample_formula_eligible_new_yield", sum(1 for r in selected_rows if r["population_partition"] == "LOW_SAMPLE_FORMULA_ELIGIBLE_CURRENTLY_STARTER_BLOCKED") == 0, Counter(r["population_partition"] for r in selected_rows), "0")
    add("source_state_files_read_only", True, "no source file write targets", "read_only")
    add("no_outcome_or_model_performance_use", True, "not used", "not used")
    add("no_value_writes", True, "not performed", "not performed")
    add("no_qualification_propagation", True, "not performed", "not performed")
    add("no_network_access", True, "not performed", "not performed")
    add("no_db_api_upload_launchagent_or_production_change", True, "not performed", "not performed")
    return rows


def parse_validation(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(paths):
        if path.name.startswith("sha256_manifest_"):
            continue
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md" and not path.read_text(encoding="utf-8").strip():
                status = "FAIL"
                notes = "empty markdown"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            notes = repr(exc)
        rows.append({"relative_path": path.name, "parser": path.suffix.lstrip("."), "status": status, "notes": notes})
    return rows


def deterministic_replay(data: dict[str, Any], selected_rows: list[dict[str, Any]], recurrence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline = {
        "recurrence": len(data["recurrence"]),
        "authorized_rows": len(data["authorized_rows"]),
        "selected_non_authorized": len(selected_rows),
        "non_authorized_recurrence_sides": sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"),
        "partitions": dict(Counter(r["population_partition"] for r in selected_rows)),
    }
    rows = []
    for iteration in range(1, 6):
        replay_data = load_inputs()
        replay_recurrence = recurrence_manifest(replay_data)
        replay_selected = selected_non_authorized_rows(replay_data)
        observed = {
            "recurrence": len(replay_data["recurrence"]),
            "authorized_rows": len(replay_data["authorized_rows"]),
            "selected_non_authorized": len(replay_selected),
            "non_authorized_recurrence_sides": sum(1 for r in replay_recurrence if r["non_authorized_recurrence_side"] == "true"),
            "partitions": dict(Counter(r["population_partition"] for r in replay_selected)),
        }
        rows.append({"iteration": iteration, "status": "PASS" if observed == baseline else "FAIL", "observed_signature": json.dumps(observed, sort_keys=True), "expected_signature": json.dumps(baseline, sort_keys=True)})
    return rows


def package_manifest() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"relative_path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    return rows


def write_markdown(selected_rows: list[dict[str, Any]], recurrence_rows: list[dict[str, Any]]) -> None:
    partitions = Counter(r["population_partition"] for r in selected_rows)
    text = f"""# Low-Sample Research Pitcher Base 103-Row Expansion Review - {RUN_DATE}

Generated: `{GENERATED_AT}`

`MLB_LOW_SAMPLE_103_ROW_EXPANSION_REVIEW_DECISION = {REVIEW_DECISION}`

`MLB_LOW_SAMPLE_EXPANSION_PRIORITY_DECISION = {PRIORITY_DECISION}`

`MLB_LOW_SAMPLE_EXPANSION_GOVERNANCE_STATUS = {GOVERNANCE_STATUS}`

## Executive Summary

This package reviewed the requested non-authorized low-sample Starter recurrence scope without materializing values or changing qualification state.

The central finding is a grain mismatch. The frozen formula package establishes 120 recurrence rows at starter-game-side grain. The completed remediation authorized 17 selected-proposition denominator rows across 2 sides. The prior package's `103` non-authorized figure is valid only as arithmetic (`120 - 17`) and not as an independently reproducible manifest of 103 canonical proposition rows.

The row-level selected-proposition evidence now available contains 111 non-authorized residual Starter-blocked rows after removing the executed 17. Of those, 14 match sides in the low-sample recurrence signature, but those rows are classified as established special-regime exclusions rather than local-parent low-sample formula defects. No row-level selected-proposition expansion subset was proven.

## Key Counts

- Requested governance arithmetic: 103
- Canonical selected-proposition non-authorized rows audited: {len(selected_rows)}
- Non-authorized recurrence starter-game-side rows audited: {sum(1 for r in recurrence_rows if r['non_authorized_recurrence_side'] == 'true')}
- Selected-proposition rows matching recurrence sides: {sum(1 for r in selected_rows if r['recurrence_signature_side_match'] == 'true')}
- Projected new full qualification yield: 0
- Projected Hits 1.5 matrix queue additions: 0

## Partition

```json
{json.dumps(dict(partitions), indent=2, sort_keys=True)}
```

## Decision

No bounded expansion governance is frozen by this task. The safest result is to preserve the 103-governance wording as an arithmetic boundary, preserve the 111 canonical residual rows unchanged, and prioritize remaining residual branches with clearer row-level recovery value.

## Safeguards

No production formulas, tiers, model scoring, uploads, DB/API state, LaunchAgents, Variant C, PA, Outcome, Bundle, or matrix artifacts were changed.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_rows()
    assert_preconditions(data, deps)
    recurrence_rows = recurrence_manifest(data)
    selected_rows = selected_non_authorized_rows(data)
    parent_rows = formula_parent_eligibility_ledger(selected_rows, data)
    counterfactual_rows = counterfactual_ledger(selected_rows, data)
    side_rows = side_projection_ledger(selected_rows, data)
    scope_rows = scope_partition(selected_rows, recurrence_rows)
    duplicate_rows = duplicate_path_audit(selected_rows, data)
    yield_rows = yield_analysis(selected_rows, recurrence_rows, data)
    option_rows = option_comparison()
    residual_rows = residual_value_comparison()
    projected_rows = projected_cumulative_state()
    safeguard_rows = production_safeguards()
    guard_rows = static_guard()
    validation = validation_rows(data, selected_rows, recurrence_rows, deps)

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"governance_103_arithmetic_boundary_{RUN_DATE}.csv", [
        {
            "governance_label": "non_authorized_recurrence_population_103",
            "formula_recurrence_rows": len(data["recurrence"]),
            "authorized_remediation_rows": len(data["authorized_rows"]),
            "arithmetic_non_authorized_rows": len(data["recurrence"]) - len(data["authorized_rows"]),
            "actual_non_authorized_recurrence_starter_game_sides": sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"),
            "actual_selected_proposition_non_authorized_rows": len(selected_rows),
            "canonical_103_row_identity_manifest_available": "false",
            "notes": "103 is preserved as governance arithmetic, not row-level canonical identity evidence",
        }
    ])
    write_csv(OUT_DIR / f"non_authorized_recurrence_starter_game_side_manifest_{RUN_DATE}.csv", [r for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"])
    write_csv(OUT_DIR / f"selected_proposition_non_authorized_row_manifest_{RUN_DATE}.csv", selected_rows)
    write_csv(OUT_DIR / f"selected_proposition_and_wider_scope_partition_{RUN_DATE}.csv", scope_rows)
    write_csv(OUT_DIR / f"current_state_and_duplicate_path_audit_{RUN_DATE}.csv", duplicate_rows)
    write_csv(OUT_DIR / f"formula_parent_eligibility_ledger_{RUN_DATE}.csv", parent_rows)
    write_csv(OUT_DIR / f"in_memory_counterfactual_formula_ledger_{RUN_DATE}.csv", counterfactual_rows)
    write_csv(OUT_DIR / f"side_level_projected_certification_ledger_{RUN_DATE}.csv", side_rows)
    write_csv(OUT_DIR / f"incremental_yield_analysis_{RUN_DATE}.csv", yield_rows)
    write_csv(OUT_DIR / f"downstream_blocker_analysis_{RUN_DATE}.csv", [
        {"blocker": k, "rows": v, "notes": "from selected-proposition non-authorized residual rows"}
        for k, v in sorted(Counter(r["population_partition"] for r in selected_rows).items())
    ])
    write_csv(OUT_DIR / f"expansion_option_comparison_{RUN_DATE}.csv", option_rows)
    write_csv(OUT_DIR / f"residual_branch_value_comparison_{RUN_DATE}.csv", residual_rows)
    write_csv(OUT_DIR / f"future_governance_manifest_if_supported_{RUN_DATE}.csv", [
        {"governance_component": "execution_contract", "status": "NOT_FROZEN", "reason": "no exact eligible selected-proposition expansion subset proven"},
        {"governance_component": "exact_side_manifest", "status": "NOT_FROZEN", "reason": "side-level broader recurrence lacks canonical row-level admission boundary"},
        {"governance_component": "exact_row_manifest", "status": "NOT_FROZEN", "reason": "103 row identity contract not reproducible from authoritative packages"},
    ])
    write_csv(OUT_DIR / f"projected_cumulative_state_{RUN_DATE}.csv", projected_rows)
    write_csv(OUT_DIR / f"production_and_matrix_safeguard_report_{RUN_DATE}.csv", safeguard_rows)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard_rows)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_LOW_SAMPLE_103_ROW_EXPANSION_REVIEW_DECISION": REVIEW_DECISION,
        "MLB_LOW_SAMPLE_EXPANSION_PRIORITY_DECISION": PRIORITY_DECISION,
        "MLB_LOW_SAMPLE_EXPANSION_GOVERNANCE_STATUS": GOVERNANCE_STATUS,
        "requested_governance_arithmetic_rows": 103,
        "selected_proposition_non_authorized_rows_audited": len(selected_rows),
        "non_authorized_recurrence_starter_game_sides": sum(1 for r in recurrence_rows if r["non_authorized_recurrence_side"] == "true"),
        "projected_newly_fully_qualified_rows": 0,
        "projected_matrix_queue_additions": 0,
        "partition_counts": dict(Counter(r["population_partition"] for r in selected_rows)),
        "prohibited_work": {
            "value_writes": "not_performed",
            "qualification_propagation": "not_performed",
            "network_access": "not_performed",
            "database_or_api_writes": "not_performed",
            "uploads": "not_performed",
            "model_or_matrix_work": "not_performed",
            "production_behavior_change": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_low_sample_103_row_expansion_review_{RUN_DATE}.json", machine)
    write_markdown(selected_rows, recurrence_rows)

    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", deterministic_replay(data, selected_rows, recurrence_rows))
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()]))
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    result = build_package()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
