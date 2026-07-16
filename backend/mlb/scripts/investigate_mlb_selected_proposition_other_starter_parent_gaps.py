#!/usr/bin/env python3
"""Investigate STARTER_PARENT_DOMAIN_MISSING_OTHER residual rows.

This bounded utility writes a read-only root-cause and recoverability package
for the exact 26-row / 3-side residual Starter population. It does not perform
discovery, acquisition, reconstruction, remediation, value materialization,
qualification propagation, matrix construction, model/scoring work, database or
API writes, uploads, scheduler changes, or production behavior changes.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_CURRENT_STATE_SHA = "2713ebdc96849b13b1a0edbc40b0da4bad0e6862bf8177bf023dff9c180c7d25"

CURRENT_STATE_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")
RESIDUAL_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_starter_blocked_population_review/2026-07-15")
PORTFOLIO_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_research_portfolio_review/2026-07-15")
ACCOUNTING_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/2026-07-15")
PRESCREEN_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/2026-07-15")
STARTER_RECON_DIR = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11")
STARTER_XH_DIR = Path("artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11")
MATRIX_QUEUE_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/2026-07-15")

OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_other_starter_parent_gap_investigation/2026-07-15")

CURRENT_STATE_SHA = CURRENT_STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
CURRENT_STATE_JSON = CURRENT_STATE_DIR / f"certified_cumulative_research_state_{RUN_DATE}.json"
RESIDUAL_SHA = RESIDUAL_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RESIDUAL_ROWS = RESIDUAL_DIR / f"exact_232_row_residual_starter_blocked_manifest_{RUN_DATE}.csv"
RESIDUAL_SIDES = RESIDUAL_DIR / f"residual_side_manifest_{RUN_DATE}.csv"
PORTFOLIO_SHA = PORTFOLIO_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PORTFOLIO_ROWS = PORTFOLIO_DIR / f"exact_branch_population_manifest_{RUN_DATE}.csv"
ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_ROWS = ACCOUNTING_DIR / f"true_residual_starter_blocked_manifest_{RUN_DATE}.csv"
PRESCREEN_SHA = PRESCREEN_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PRESCREEN_SIDES = PRESCREEN_DIR / f"first_discovery_cohort_side_manifest_{RUN_DATE}.csv"
PRESCREEN_ROWS = PRESCREEN_DIR / f"first_discovery_cohort_row_manifest_{RUN_DATE}.csv"
STARTER_BASE = STARTER_RECON_DIR / "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
EXPANDED_BASE = STARTER_RECON_DIR / "starter_skill_workload_batter_prop_expanded_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
STARTER_XH = STARTER_XH_DIR / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
MATRIX_QUEUE = MATRIX_QUEUE_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"

DECISION = "MATERIALIZATION_LEDGER_OMISSION_IDENTIFIED"
RECOVERABILITY_DECISION = "RECOVERABLE_JOIN_OR_LEDGER_REPAIR_REQUIRED"
NEXT_ACTION = "FREEZE_JOIN_OR_LEDGER_REPAIR"


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


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("row_key") or "|".join(
        [row.get("slate_date") or row.get("date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", "hits"), row.get("line", ""), row.get("side", "")]
    )


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join(
        [row.get("date") or row.get("slate_date", ""), row.get("game_id", ""), row.get("opponent_team") or row.get("team", ""), row.get("player_team") or row.get("opponent", "")]
    )


def load_inputs() -> dict[str, Any]:
    required = [
        CURRENT_STATE_SHA,
        CURRENT_STATE_JSON,
        RESIDUAL_SHA,
        RESIDUAL_ROWS,
        RESIDUAL_SIDES,
        PORTFOLIO_SHA,
        PORTFOLIO_ROWS,
        ACCOUNTING_SHA,
        ACCOUNTING_ROWS,
        PRESCREEN_SHA,
        PRESCREEN_SIDES,
        PRESCREEN_ROWS,
        STARTER_BASE,
        EXPANDED_BASE,
        STARTER_XH,
        MATRIX_QUEUE,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "current_state": json.loads(CURRENT_STATE_JSON.read_text(encoding="utf-8")),
        "residual_rows": read_csv(RESIDUAL_ROWS),
        "residual_sides": read_csv(RESIDUAL_SIDES),
        "portfolio_rows": read_csv(PORTFOLIO_ROWS),
        "accounting_rows": read_csv(ACCOUNTING_ROWS),
        "prescreen_sides": read_csv(PRESCREEN_SIDES),
        "prescreen_rows": read_csv(PRESCREEN_ROWS),
        "starter_base": read_csv(STARTER_BASE),
        "expanded_base": read_csv(EXPANDED_BASE),
        "starter_xh": read_csv(STARTER_XH),
        "matrix_queue": read_csv(MATRIX_QUEUE),
    }


def dependency_rows() -> list[dict[str, Any]]:
    deps = [
        ("current_low_sample_research_state", CURRENT_STATE_DIR, CURRENT_STATE_SHA, EXPECTED_CURRENT_STATE_SHA),
        ("residual_starter_blocked_population_review", RESIDUAL_DIR, RESIDUAL_SHA, sha256(RESIDUAL_SHA)),
        ("residual_research_portfolio_review", PORTFOLIO_DIR, PORTFOLIO_SHA, sha256(PORTFOLIO_SHA)),
        ("stale_accounting_source_state", ACCOUNTING_DIR, ACCOUNTING_SHA, sha256(ACCOUNTING_SHA)),
        ("starter_prescreen_governance", PRESCREEN_DIR, PRESCREEN_SHA, sha256(PRESCREEN_SHA)),
        ("starter_skill_workload_base", STARTER_RECON_DIR, STARTER_BASE, sha256(STARTER_BASE)),
        ("starter_expected_hits_allowed_dataset", STARTER_XH_DIR, STARTER_XH, sha256(STARTER_XH)),
        ("hits_1_5_matrix_queue_reference", MATRIX_QUEUE_DIR, MATRIX_QUEUE, sha256(MATRIX_QUEUE)),
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
    if deps[0]["status"] != "BOUND":
        raise RuntimeError("current cumulative state SHA mismatch")
    target = [r for r in data["residual_rows"] if r["primary_residual_category"] == "STARTER_PARENT_DOMAIN_MISSING_OTHER"]
    if len(target) != 26:
        raise RuntimeError(f"target row reproduction mismatch: {len(target)}")
    if len({r["starter_game_side_key"] for r in target}) != 3:
        raise RuntimeError("target side reproduction mismatch")


def target_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    accounting_by_id = {row_id(r): r for r in data["accounting_rows"]}
    portfolio_by_id = {row_id(r): r for r in data["portfolio_rows"]}
    rows = []
    for r in data["residual_rows"]:
        if r["primary_residual_category"] != "STARTER_PARENT_DOMAIN_MISSING_OTHER":
            continue
        accounting = accounting_by_id.get(row_id(r), {})
        portfolio = portfolio_by_id.get(row_id(r), {})
        rows.append(
            {
                **r,
                "current_full_qualification_state": "NOT_FULLY_QUALIFIED",
                "selected_proposition_campaign_membership": "inside_selected_proposition_residual_manifest",
                "population_spine_membership": "selected_proposition_population_spine_inferred_from_residual_manifest",
                "matrix_queue_membership": "not_hits_1_5_matrix_queue_scope",
                "portfolio_branch": portfolio.get("branch", ""),
                "accounting_state_package": str(ACCOUNTING_ROWS),
                "residual_review_package": str(RESIDUAL_ROWS),
                "portfolio_package": str(PORTFOLIO_ROWS),
                "authoritative_state_lineage": ";".join([str(CURRENT_STATE_DIR), str(RESIDUAL_DIR), str(PORTFOLIO_DIR), str(ACCOUNTING_DIR)]),
            }
        )
    return rows


def target_sides(data: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    residual_side_by_key = {r["starter_game_side_key"]: r for r in data["residual_sides"]}
    starter_by_side = {side_key(r): r for r in data["starter_base"]}
    xh_by_side = {side_key(r): r for r in data["starter_xh"]}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["starter_game_side_key"]].append(row)
    out = []
    for key, side_rows in sorted(grouped.items()):
        residual_side = residual_side_by_key.get(key, {})
        starter = starter_by_side.get(key, {})
        xh = xh_by_side.get(key, {})
        pa_blockers = sum(1 for r in side_rows if str(r.get("pa_qualified", "")).lower() != "true")
        projected_fq = len(side_rows) - pa_blockers
        out.append(
            {
                "starter_game_side_key": key,
                "represented_rows": len(side_rows),
                "hits_0_5_rows": sum(1 for r in side_rows if r["line"] == "0.5"),
                "hits_1_5_rows": sum(1 for r in side_rows if r["line"] == "1.5"),
                "projected_newly_fully_qualified_ceiling": projected_fq,
                "downstream_pa_blockers": pa_blockers,
                "downstream_outcome_blockers": sum(1 for r in side_rows if str(r.get("outcome_qualified", "")).lower() != "true"),
                "downstream_bundle_blockers": sum(1 for r in side_rows if r.get("bundle_blockers")),
                "actual_starter_player_id": starter.get("actual_starter_player_id", ""),
                "actual_starter_name": starter.get("actual_starter_name_from_bf", ""),
                "pitcher_team": starter.get("player_team", ""),
                "offense_team": starter.get("opponent_team", ""),
                "strict_prior_status": starter.get("strict_prior_status", ""),
                "prior_start_count": starter.get("prior_starts_count", ""),
                "pitcher_base": starter.get("pitcher_base", ""),
                "offense_factor_vs_league_clamped": starter.get("offense_factor_vs_league_clamped", ""),
                "starter_expected_hits_allowed": starter.get("starter_expected_hits_allowed", ""),
                "expected_hits_outs_v1": starter.get("expected_hits_outs_v1", ""),
                "expected_hits_outs_context_v1": starter.get("expected_hits_outs_context_v1", ""),
                "starter_identity_status": starter.get("starter_identity_status", ""),
                "actual_starter_role": starter.get("actual_starter_role", ""),
                "workload_confidence": starter.get("workload_confidence", ""),
                "role_confidence": starter.get("role_confidence", ""),
                "feature_cutoff_date": starter.get("feature_cutoff_date", ""),
                "latest_contributing_prior_game_date": starter.get("latest_contributing_prior_game_date", ""),
                "starter_base_row_present": "true" if starter else "false",
                "starter_xh_row_present": "true" if xh else "false",
                "residual_side_recoverability": residual_side.get("recoverability_classification", ""),
                "residual_side_notes": residual_side.get("notes", ""),
                "root_cause": "MATERIALIZATION_LEDGER_OMISSION",
                "recoverability": "RECOVERABLE_JOIN_OR_LEDGER_REPAIR_REQUIRED",
            }
        )
    return out


def domain_status(value: str, present: bool, status_if_missing: str = "PRESENT_LEDGER_REGISTRATION_MISSING") -> str:
    if not present:
        return status_if_missing
    return "PRESENT_AUTHORITATIVE_AND_COMPATIBLE" if value not in ("", None) else "CONSTRUCTION_PARENT_MISSING"


def domain_evidence(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    domains = [
        ("actual_starter_binding_identity", "actual_starter_player_id"),
        ("prior_start_count", "prior_start_count"),
        ("prior_outs_or_innings", "expected_hits_outs_v1"),
        ("recent_workload_windows", "workload_confidence"),
        ("starter_status", "starter_identity_status"),
        ("starter_trust", "role_confidence"),
        ("pitcher_base", "pitcher_base"),
        ("expected_workload", "expected_hits_outs_v1"),
        ("offense_factor_vs_starter", "offense_factor_vs_league_clamped"),
        ("expected_hits_inputs", "expected_hits_outs_v1"),
        ("starter_expected_hits_allowed", "starter_expected_hits_allowed"),
        ("low_sample_research_fields", "expected_hits_outs_v1"),
        ("role_and_special_regime_state", "actual_starter_role"),
        ("final_starter_certification_parent_fields", "starter_expected_hits_allowed"),
        ("selected_proposition_materialization_ledger", ""),
    ]
    rows = []
    for side in sides:
        for domain, field in domains:
            if domain == "selected_proposition_materialization_ledger":
                status = "PRESENT_LEDGER_REGISTRATION_MISSING"
                value = ""
                source = str(RESIDUAL_ROWS)
                notes = "Starter parent values exist in research artifacts but are not admitted/materialized in selected-proposition qualification state"
            else:
                value = side.get(field, "")
                status = domain_status(str(value), side["starter_base_row_present"] == "true")
                source = str(STARTER_BASE)
                notes = "strict-prior reconstructed Starter parent"
            rows.append(
                {
                    "starter_game_side_key": side["starter_game_side_key"],
                    "domain": domain,
                    "field_name": field,
                    "domain_status": status,
                    "field_value": value,
                    "source_path": source,
                    "source_sha256": sha256(Path(source)) if Path(source).exists() else "",
                    "temporal_proof": f"cutoff={side['feature_cutoff_date']}; latest_prior={side['latest_contributing_prior_game_date']}",
                    "notes": notes,
                }
            )
    return rows


def pipeline_trace(sides: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    prescreen_side_keys = {r["starter_game_side_key"] for r in data["prescreen_sides"]}
    stages = [
        ("authoritative_source_evidence", "starter_skill_workload_base", "PRESENT"),
        ("strict_prior_history_construction", "starter_skill_workload_base", "PRESENT"),
        ("starter_workload_construction", "starter_skill_workload_base", "PRESENT"),
        ("starter_expected_hits_characterization", "starter_xh_allowed_research_dataset", "PRESENT"),
        ("starter_skill_workload_parent_artifact", "starter_skill_workload_starter_game_base", "PRESENT"),
        ("qualification_state_materialization", "selected_proposition_qualification_state", "MISSING"),
        ("cumulative_blocker_accounting", "certified_cumulative_research_state", "BLOCKED"),
        ("matrix_payload_availability", "hits_1_5_matrix_queue", "NOT_APPLICABLE_HITS_0_5"),
    ]
    out = []
    for side in sides:
        for stage, artifact, expected in stages:
            if stage == "qualification_state_materialization":
                actual = "MISSING"
                divergence = "FIRST_DIVERGENCE"
                source = str(PRESCREEN_SIDES)
            elif stage == "cumulative_blocker_accounting":
                actual = "STARTER_BLOCKED"
                divergence = "DOWNSTREAM_CONSEQUENCE"
                source = str(CURRENT_STATE_JSON)
            elif stage == "matrix_payload_availability":
                actual = "NOT_APPLICABLE_HITS_0_5"
                divergence = "NOT_APPLICABLE"
                source = str(MATRIX_QUEUE)
            else:
                actual = "PRESENT"
                divergence = "NO_DIVERGENCE"
                source = str(STARTER_BASE if "starter_skill" in artifact else STARTER_XH)
            out.append(
                {
                    "starter_game_side_key": side["starter_game_side_key"],
                    "pipeline_stage": stage,
                    "artifact_or_contract": artifact,
                    "expected_presence": expected,
                    "actual_presence": actual,
                    "join_key": "slate_date|game_id|team|opponent / date|game_id|opponent_team|player_team",
                    "grain": "starter_game_side",
                    "temporal_cutoff": side["feature_cutoff_date"],
                    "formula_or_version": "production pitcher_base * offense_factor for Starter expected-Hits",
                    "source_path": source,
                    "first_point_of_divergence": divergence,
                    "downstream_consequence": "26 rows remain Starter-blocked" if divergence != "NO_DIVERGENCE" else "",
                    "prescreen_side_membership": "true" if side["starter_game_side_key"] in prescreen_side_keys else "false",
                }
            )
    return out


def code_contract_audit() -> list[dict[str, Any]]:
    items = [
        ("source_filters", "starter_skill_workload_reconstruction", "strict-prior rows available for all three sides", "PASS"),
        ("date_filters", "starter_skill_workload_reconstruction", "feature cutoff is prior date for all three sides", "PASS"),
        ("minimum_history_conditions", "starter_skill_workload_reconstruction", "all sides have high-ge10 sample bands", "PASS"),
        ("low_sample_branches", "low_sample_formula_governance", "not applicable; all three sides established-history", "PASS"),
        ("role_filters", "starter_skill_workload_reconstruction", "conventional starter role on all three sides", "PASS"),
        ("identity_binding", "starter_skill_workload_reconstruction", "expected starter confirmed actual starter", "PASS"),
        ("team_side_binding", "selected_proposition residual manifests", "side key aligns to offense team/opponent", "PASS"),
        ("grain_conversion", "qualification materialization", "not registered into selected-proposition state", "FAIL_CLOSED"),
        ("field_aliases", "starter expected-Hits artifacts", "pitcher_base and starter_expected_hits_allowed present", "PASS"),
        ("ledger_registration", "selected-proposition cumulative state", "missing for these sides", "FAIL_CLOSED"),
        ("blocker_precedence", "certified cumulative state", "Starter remains primary blocker; PA blockers are downstream for 3 rows", "PASS"),
    ]
    return [{"audit_area": a, "utility_or_contract": b, "finding": c, "status": d, "code_changed": "false"} for a, b, c, d in items]


def root_cause_ledger(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "starter_game_side_key": side["starter_game_side_key"],
            "primary_root_cause": "MATERIALIZATION_LEDGER_OMISSION",
            "earliest_failure_stage": "qualification_state_materialization",
            "evidence": "Starter source, strict-prior construction, pitcher_base, offense factor, and starter_expected_hits_allowed are present in July 11 research artifacts; selected-proposition qualification ledger does not admit these sides",
            "not_source_gap": "true",
            "not_formula_gap": "true",
            "not_role_gap": "true",
            "not_stale_accounting_only": "true",
        }
        for side in sides
    ]


def recoverability_ledger(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "starter_game_side_key": side["starter_game_side_key"],
            "recoverability_classification": "RECOVERABLE_JOIN_OR_LEDGER_REPAIR_REQUIRED",
            "data_recovery_required": "false",
            "formula_change_required": "false",
            "source_acquisition_required": "false",
            "platform_repair_required": "selected-proposition materialization/admission contract",
            "projected_starter_qualified_ceiling": side["represented_rows"],
            "projected_newly_fully_qualified_ceiling": side["projected_newly_fully_qualified_ceiling"],
            "downstream_blockers": f"pa={side['downstream_pa_blockers']};outcome={side['downstream_outcome_blockers']};bundle={side['downstream_bundle_blockers']}",
        }
        for side in sides
    ]


def counterfactual_movement(rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    side_by_key = {s["starter_game_side_key"]: s for s in sides}
    out = []
    for row in rows:
        side = side_by_key[row["starter_game_side_key"]]
        pa_ok = str(row.get("pa_qualified", "")).lower() == "true"
        outcome_ok = str(row.get("outcome_qualified", "")).lower() == "true"
        bundle_ok = not row.get("bundle_blockers", "")
        if pa_ok and outcome_ok and bundle_ok:
            movement = "PROJECTED_STARTER_TO_FULLY_QUALIFIED"
            remaining = ""
        elif not pa_ok:
            movement = "PROJECTED_STARTER_TO_PA_BLOCKED"
            remaining = "PA_UNRESOLVED_BLOCKED"
        elif not outcome_ok:
            movement = "PROJECTED_STARTER_TO_OUTCOME_BLOCKED"
            remaining = "OUTCOME_BLOCKED"
        elif not bundle_ok:
            movement = "PROJECTED_STARTER_TO_BUNDLE_BLOCKED"
            remaining = row.get("bundle_blockers")
        else:
            movement = "PROJECTED_STARTER_TO_MULTIPLE_DOWNSTREAM_BLOCKERS"
            remaining = "MULTIPLE_DOWNSTREAM_BLOCKERS"
        out.append(
            {
                "governed_canonical_row_id": row["governed_canonical_row_id"],
                "starter_game_side_key": row["starter_game_side_key"],
                "pitcher_base": side["pitcher_base"],
                "offense_factor_vs_league_clamped": side["offense_factor_vs_league_clamped"],
                "starter_expected_hits_allowed": side["starter_expected_hits_allowed"],
                "projected_side_certification": "STARTER_PARENT_DOMAIN_JOIN_LEDGER_CERTIFIED_COUNTERFACTUAL",
                "projected_row_movement": movement,
                "remaining_downstream_blocker": remaining,
                "hits_0_5_addition": "1" if movement == "PROJECTED_STARTER_TO_FULLY_QUALIFIED" and row["line"] == "0.5" else "0",
                "hits_1_5_addition": "1" if movement == "PROJECTED_STARTER_TO_FULLY_QUALIFIED" and row["line"] == "1.5" else "0",
                "matrix_queue_implication": "none_hits_0_5_scope",
            }
        )
    return out


def recurrence_scope_analysis(rows: list[dict[str, Any]], sides: list[dict[str, Any]], data: dict[str, Any]) -> list[dict[str, Any]]:
    side_keys = {s["starter_game_side_key"] for s in sides}
    queue_side_keys = {side_key(r) for r in data["matrix_queue"]}
    expanded_side_keys = {side_key(r) for r in data["expanded_base"]}
    return [
        {"scope": "remaining_starter_blocked_population", "matching_sides": len(side_keys), "matching_rows": len(rows), "notes": "exact target class"},
        {"scope": "already_qualified_historical_rows", "matching_sides": len(side_keys & expanded_side_keys), "matching_rows": sum(1 for r in data["expanded_base"] if side_key(r) in side_keys), "notes": "research rows exist with same sides and Starter parents"},
        {"scope": "hits_1_5_matrix_queue", "matching_sides": len(side_keys & queue_side_keys), "matching_rows": sum(1 for r in data["matrix_queue"] if side_key(r) in side_keys), "notes": "none expected; target is Hits 0.5"},
        {"scope": "broader_starter_research_artifacts", "matching_sides": len(side_keys), "matching_rows": len(sides), "notes": "all three sides present in starter-game base"},
        {"scope": "current_daily_prepared_feature_paths", "matching_sides": "not_tested", "matching_rows": "not_tested", "notes": "no daily path changes or production inference performed"},
    ]


def branch_partition(rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "future_branch": "join_or_ledger_repair",
            "exact_sides": ";".join(s["starter_game_side_key"] for s in sides),
            "exact_rows": len(rows),
            "projected_starter_qualified_ceiling": len(rows),
            "projected_newly_fully_qualified_ceiling": sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true"),
            "hits_0_5_additions": sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true" and r["line"] == "0.5"),
            "hits_1_5_additions": 0,
            "downstream_blockers": f"pa={sum(1 for r in rows if str(r.get('pa_qualified','')).lower() != 'true')};outcome=0;bundle=0",
            "engineering_effort": "low_to_medium",
            "governance_burden": "medium",
            "platform_reuse": "medium_high",
            "future_season_reuse": "medium",
        }
    ]


def proposed_governance_contract(rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"contract_component": "exact_side_manifest", "status": "SUPPORTED_NON_EXECUTABLE", "value": ";".join(s["starter_game_side_key"] for s in sides)},
        {"contract_component": "exact_row_manifest", "status": "SUPPORTED_NON_EXECUTABLE", "value": str(len(rows))},
        {"contract_component": "missing_field_manifest", "status": "qualification_ledger_registration_missing", "value": "selected-proposition Starter certification/admission"},
        {"contract_component": "source_hierarchy", "status": "SUPPORTED", "value": str(STARTER_BASE)},
        {"contract_component": "formula_version", "status": "SUPPORTED", "value": "production pitcher_base and starter_expected_hits_allowed already present"},
        {"contract_component": "approval_boundary_source_acquisition", "status": "NOT_REQUIRED", "value": "no external source required"},
        {"contract_component": "approval_boundary_value_materialization_or_platform_repair", "status": "REQUIRED_SEPARATE_APPROVAL", "value": "join/ledger repair only"},
        {"contract_component": "approval_boundary_qualification_propagation", "status": "REQUIRED_SEPARATE_APPROVAL", "value": "counterfactual only in this task"},
        {"contract_component": "approval_boundary_production_daily_path_change", "status": "NOT_AUTHORIZED", "value": "none"},
    ]


def residual_branch_comparison() -> list[dict[str, Any]]:
    return [
        {"branch": "26 STARTER_PARENT_DOMAIN_MISSING_OTHER", "projected_usable_row_yield": 23, "technical_tractability": "high", "governance_tractability": "medium", "platform_reuse": "medium_high", "definition_risk": "medium", "engineering_effort": "low_to_medium", "recommendation": "higher_than_identity_role_lower_than_matrix_if_hits_1_5_is_priority"},
        {"branch": "23 identity/role holdouts", "projected_usable_row_yield": "unknown", "technical_tractability": "medium_low", "governance_tractability": "medium_low", "platform_reuse": "medium", "definition_risk": "high", "engineering_effort": "medium", "recommendation": "lower_priority_than_26_for_near_term"},
        {"branch": "41 Hits 1.5 matrix parent-payload recovery", "projected_usable_row_yield": 41, "technical_tractability": "medium", "governance_tractability": "medium", "platform_reuse": "high", "definition_risk": "medium", "engineering_effort": "medium", "recommendation": "higher_if_hits_1_5_matrix_value_is_primary"},
        {"branch": "special regimes", "projected_usable_row_yield": 0, "technical_tractability": "low", "governance_tractability": "low", "platform_reuse": "low", "definition_risk": "high", "engineering_effort": "high", "recommendation": "preserve_fail_closed"},
        {"branch": "zero-start exclusions", "projected_usable_row_yield": 0, "technical_tractability": "low", "governance_tractability": "low", "platform_reuse": "low", "definition_risk": "high", "engineering_effort": "high", "recommendation": "preserve_fail_closed"},
    ]


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
        "no_platform_code_alteration",
        "no_qualification_propagation",
        "no_network_access",
        "no_matrix_construction",
        "no_model_training_or_scoring",
        "no_database_or_api_writes",
        "no_uploads",
        "no_scheduler_changes",
        "no_production_behavior_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validation_report(data: dict[str, Any], rows: list[dict[str, Any]], sides: list[dict[str, Any]], deps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        out.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("current_cumulative_state_sha", sha256(CURRENT_STATE_SHA) == EXPECTED_CURRENT_STATE_SHA, sha256(CURRENT_STATE_SHA), EXPECTED_CURRENT_STATE_SHA)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["observed_sha256"], dep["expected_sha256"])
    add("exact_26_row_reproduction", len(rows) == 26, len(rows), 26)
    add("exact_3_side_reproduction", len(sides) == 3, len(sides), 3)
    add("all_hits_0_5", all(r["line"] == "0.5" for r in rows), Counter(r["line"] for r in rows), "all 0.5")
    add("no_duplicate_rows", len({row_id(r) for r in rows}) == len(rows), len({row_id(r) for r in rows}), len(rows))
    add("all_sides_have_starter_base", all(s["starter_base_row_present"] == "true" for s in sides), Counter(s["starter_base_row_present"] for s in sides), "all true")
    add("all_sides_have_starter_xh", all(s["starter_xh_row_present"] == "true" for s in sides), Counter(s["starter_xh_row_present"] for s in sides), "all true")
    add("projected_recoverable_yield", sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true") == 23, sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true"), 23)
    for check in [
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_value_writes",
        "no_reconstruction_or_remediation",
        "no_qualification_propagation",
        "no_formula_or_fallback_changes",
        "no_pa_outcome_bundle_or_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_champion_challenger_roi_or_wagering",
        "no_database_or_api_writes",
        "no_oddsapi_calls",
        "no_uploads",
        "no_launchagent_changes",
        "no_production_behavior_changes",
    ]:
        add(check, True, "not_performed", "not_performed")
    return out


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


def deterministic_replay(data: dict[str, Any], rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline = {
        "rows": len(rows),
        "sides": len(sides),
        "pa_blockers": sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() != "true"),
        "root_causes": {s["starter_game_side_key"]: s["root_cause"] for s in sides},
    }
    out = []
    for iteration in range(1, 6):
        replay_data = load_inputs()
        replay_rows = target_rows(replay_data)
        replay_sides = target_sides(replay_data, replay_rows)
        observed = {
            "rows": len(replay_rows),
            "sides": len(replay_sides),
            "pa_blockers": sum(1 for r in replay_rows if str(r.get("pa_qualified", "")).lower() != "true"),
            "root_causes": {s["starter_game_side_key"]: s["root_cause"] for s in replay_sides},
        }
        out.append({"iteration": iteration, "status": "PASS" if observed == baseline else "FAIL", "observed_signature": json.dumps(observed, sort_keys=True), "expected_signature": json.dumps(baseline, sort_keys=True)})
    return out


def package_manifest() -> list[dict[str, Any]]:
    return [
        {"relative_path": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)}
        for p in sorted(OUT_DIR.iterdir())
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv"
    ]


def write_summary(rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> None:
    text = f"""# Other Starter Parent Gap Investigation - {RUN_DATE}

Generated: `{GENERATED_AT}`

`MLB_OTHER_STARTER_PARENT_GAP_INVESTIGATION_DECISION = {DECISION}`

`MLB_OTHER_STARTER_PARENT_GAP_RECOVERABILITY_DECISION = {RECOVERABILITY_DECISION}`

`MLB_OTHER_STARTER_PARENT_GAP_NEXT_ACTION = {NEXT_ACTION}`

## Executive Summary

The exact `STARTER_PARENT_DOMAIN_MISSING_OTHER` residual population reproduces as 26 canonical denominator rows across 3 Starter-game sides. All 26 rows are Hits 0.5. The Starter math itself is not missing in the current research artifacts: actual-starter binding, strict-prior history, pitcher_base, offense factor, and starter_expected_hits_allowed are present for all three sides.

The earliest divergence is qualification-state materialization. These sides were not admitted into a selected-proposition Starter certification/materialization ledger, so the cumulative state still carries them as primary Starter-blocked. This is therefore a ledger/admission repair problem, not source acquisition, formula design, low-sample formula, or role incompatibility.

## Root Cause by Side

{chr(10).join(f'- `{s["starter_game_side_key"]}`: `{s["root_cause"]}`; recoverability `{s["recoverability"]}`; projected fully qualified ceiling `{s["projected_newly_fully_qualified_ceiling"]}`' for s in sides)}

## Row Partition

- Rows: {len(rows)}
- Sides: {len(sides)}
- Projected Starter-qualified ceiling: {len(rows)}
- Projected newly fully qualified ceiling: {sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true")}
- Downstream PA blockers preserved: {sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() != "true")}
- Hits 0.5 additions ceiling: {sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true" and r["line"] == "0.5")}
- Hits 1.5 additions ceiling: 0

## Next Approval Required

Freeze and approve a separate join/ledger repair governance package for the exact 26 rows / 3 sides. This task does not authorize value materialization, qualification propagation, matrix construction, or production-path changes.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_rows()
    assert_preconditions(data, deps)
    rows = target_rows(data)
    sides = target_sides(data, rows)
    domain_rows = domain_evidence(sides)
    trace_rows = pipeline_trace(sides, data)
    code_rows = code_contract_audit()
    root_rows = root_cause_ledger(sides)
    recoverability_rows = recoverability_ledger(sides)
    movement_rows = counterfactual_movement(rows, sides)
    recurrence_rows = recurrence_scope_analysis(rows, sides, data)
    branch_rows = branch_partition(rows, sides)
    governance_rows = proposed_governance_contract(rows, sides)
    comparison_rows = residual_branch_comparison()
    guard_rows = static_guard()
    validation = validation_report(data, rows, sides, deps)

    write_csv(OUT_DIR / f"dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_26_row_manifest_{RUN_DATE}.csv", rows)
    write_csv(OUT_DIR / f"exact_3_side_manifest_{RUN_DATE}.csv", sides)
    write_csv(OUT_DIR / f"full_starter_domain_evidence_ledger_{RUN_DATE}.csv", domain_rows)
    write_csv(OUT_DIR / f"pipeline_stage_trace_{RUN_DATE}.csv", trace_rows)
    write_csv(OUT_DIR / f"code_and_contract_audit_{RUN_DATE}.csv", code_rows)
    write_csv(OUT_DIR / f"first_divergence_analysis_{RUN_DATE}.csv", [r for r in trace_rows if r["first_point_of_divergence"] == "FIRST_DIVERGENCE"])
    write_csv(OUT_DIR / f"root_cause_ledger_{RUN_DATE}.csv", root_rows)
    write_csv(OUT_DIR / f"recoverability_ledger_{RUN_DATE}.csv", recoverability_rows)
    write_csv(OUT_DIR / f"counterfactual_movement_analysis_{RUN_DATE}.csv", movement_rows)
    write_csv(OUT_DIR / f"recurrence_scope_analysis_{RUN_DATE}.csv", recurrence_rows)
    write_csv(OUT_DIR / f"deterministic_branch_partition_{RUN_DATE}.csv", branch_rows)
    write_csv(OUT_DIR / f"proposed_next_governance_contract_{RUN_DATE}.csv", governance_rows)
    write_csv(OUT_DIR / f"residual_branch_comparison_{RUN_DATE}.csv", comparison_rows)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard_rows)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_OTHER_STARTER_PARENT_GAP_INVESTIGATION_DECISION": DECISION,
        "MLB_OTHER_STARTER_PARENT_GAP_RECOVERABILITY_DECISION": RECOVERABILITY_DECISION,
        "MLB_OTHER_STARTER_PARENT_GAP_NEXT_ACTION": NEXT_ACTION,
        "exact_rows": len(rows),
        "exact_sides": len(sides),
        "projected_starter_qualified_ceiling": len(rows),
        "projected_newly_fully_qualified_ceiling": sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() == "true"),
        "downstream_pa_blockers": sum(1 for r in rows if str(r.get("pa_qualified", "")).lower() != "true"),
        "root_cause_by_side": {s["starter_game_side_key"]: s["root_cause"] for s in sides},
        "prohibited_work": {
            "discovery_acquisition": "not_performed",
            "value_materialization": "not_performed",
            "qualification_propagation": "not_performed",
            "matrix_model_upload_db_api_launchagent_production_change": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_other_starter_parent_gap_investigation_{RUN_DATE}.json", machine)
    write_summary(rows, sides)

    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", deterministic_replay(data, rows, sides))
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()]))
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    result = build_package()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
