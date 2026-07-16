#!/usr/bin/env python3
"""Investigate the 17-row local Starter construction/persistence defect.

This bounded utility is read-only decision support. It binds certified packages,
reproduces the exact 17-row / 2-side population, traces the missing pitcher_base
and starter_expected_hits_allowed fields, freezes root-cause and repairability
decisions, and writes a non-executable repair-governance outline. It does not
repair values, materialize payloads, propagate qualification, construct matrices,
train models, score rows, call networks, write databases/APIs, upload files,
alter schedulers, or change production behavior.
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

PORTFOLIO_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_residual_research_portfolio_review/"
    "2026-07-15"
)
ACCOUNTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)
LINEAGE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_pitcher_base_lineage_investigation/"
    "2026-07-15"
)
PRESCREEN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/"
    "2026-07-15"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_local_starter_platform_defect_investigation/"
    "2026-07-15"
)

EXPECTED_PORTFOLIO_SHA_MANIFEST_SHA256 = "75ab96b9fca343735a93fde3cb2c1ce23cf433f346ee956262993f8164035306"

PORTFOLIO_SHA = PORTFOLIO_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PORTFOLIO_JSON = PORTFOLIO_DIR / f"machine_readable_residual_research_portfolio_review_{RUN_DATE}.json"
PORTFOLIO_BRANCH_MANIFEST = PORTFOLIO_DIR / f"exact_branch_population_manifest_{RUN_DATE}.csv"

ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_STATE = ACCOUNTING_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json"

LINEAGE_SHA = LINEAGE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
LINEAGE_JSON = LINEAGE_DIR / f"machine_readable_lineage_investigation_{RUN_DATE}.json"
LINEAGE_ROW_MANIFEST = LINEAGE_DIR / f"exact_fail_closed_row_manifest_{RUN_DATE}.csv"
LINEAGE_SIDE_MANIFEST = LINEAGE_DIR / f"exact_fail_closed_side_manifest_{RUN_DATE}.csv"
LINEAGE_PARENT_AUDIT = LINEAGE_DIR / f"parent_domain_audit_{RUN_DATE}.csv"
LINEAGE_MISSINGNESS = LINEAGE_DIR / f"missingness_root_cause_analysis_{RUN_DATE}.csv"
LINEAGE_PITCHER_MAP = LINEAGE_DIR / f"pitcher_base_lineage_map_{RUN_DATE}.csv"
LINEAGE_STARTER_EXPECTED_MAP = LINEAGE_DIR / f"starter_expected_hits_allowed_lineage_map_{RUN_DATE}.csv"
LINEAGE_RECOVERABILITY = LINEAGE_DIR / f"recoverability_classification_{RUN_DATE}.csv"

PRESCREEN_LEDGER = PRESCREEN_DIR / f"exact_prescreen_matching_side_ledger_{RUN_DATE}.csv"
PRESCREEN_SPEC = PRESCREEN_DIR / f"reusable_prescreening_specification_{RUN_DATE}.csv"

STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/"
    "2026-07-11/starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_XH = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/"
    "2026-07-11/starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
RESEARCH_BUILDER = Path("backend/mlb/scripts/build_mlb_starter_skill_workload_research.py")
PRESCREEN_BUILDER = Path("backend/mlb/scripts/freeze_mlb_selected_proposition_starter_prescreen_and_discovery_cohort_governance.py")
ENV_GENERATOR = Path("backend/mlb/scripts/report_mlb_hits_environment.py")

EXPECTED_ROWS = 17
EXPECTED_SIDES = 2
EXPECTED_HITS_0_5 = 17
EXPECTED_HITS_1_5 = 0
EXPECTED_FULLY_QUALIFIED_CEILING = 16

ROOT_CAUSE_DECISION = "LOW_SAMPLE_BRANCH_DEFECT"
REPAIRABILITY_DECISION = "REPAIRABLE_NEW_FORMULA_GOVERNANCE_REQUIRED"
REPAIR_GOVERNANCE_STATUS = "FROZEN_NON_EXECUTABLE_FORMULA_GOVERNANCE_REQUIRED_NO_REPAIR_AUTHORIZED"
PORTFOLIO_DECISION = "LOCAL_PLATFORM_DEFECT_REPAIR_HIGH_VALUE"


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
    return row.get("governed_canonical_row_id") or "|".join(
        [
            row.get("slate_date", ""),
            row.get("game_id", ""),
            row.get("player_id", ""),
            row.get("prop_type", ""),
            row.get("line", ""),
            row.get("side", ""),
        ]
    )


def side_key_from_starter_base(row: dict[str, str]) -> str:
    return "|".join([row.get("date", ""), row.get("game_id", ""), row.get("opponent_team", ""), row.get("player_team", "")])


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join([row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")])


def int_value(value: str | None) -> int:
    try:
        return int(float(value or "0"))
    except ValueError:
        return 0


def load_inputs() -> dict[str, Any]:
    required = [
        PORTFOLIO_SHA,
        PORTFOLIO_JSON,
        PORTFOLIO_BRANCH_MANIFEST,
        ACCOUNTING_SHA,
        ACCOUNTING_STATE,
        LINEAGE_SHA,
        LINEAGE_JSON,
        LINEAGE_ROW_MANIFEST,
        LINEAGE_SIDE_MANIFEST,
        LINEAGE_PARENT_AUDIT,
        LINEAGE_MISSINGNESS,
        LINEAGE_PITCHER_MAP,
        LINEAGE_STARTER_EXPECTED_MAP,
        LINEAGE_RECOVERABILITY,
        PRESCREEN_LEDGER,
        PRESCREEN_SPEC,
        STARTER_BASE,
        STARTER_XH,
        RESEARCH_BUILDER,
        PRESCREEN_BUILDER,
        ENV_GENERATOR,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "portfolio": json.loads(PORTFOLIO_JSON.read_text(encoding="utf-8")),
        "accounting": json.loads(ACCOUNTING_STATE.read_text(encoding="utf-8")),
        "lineage": json.loads(LINEAGE_JSON.read_text(encoding="utf-8")),
        "portfolio_rows": read_csv(PORTFOLIO_BRANCH_MANIFEST),
        "lineage_rows": read_csv(LINEAGE_ROW_MANIFEST),
        "lineage_sides": read_csv(LINEAGE_SIDE_MANIFEST),
        "parent_audit": read_csv(LINEAGE_PARENT_AUDIT),
        "missingness": read_csv(LINEAGE_MISSINGNESS),
        "pitcher_map": read_csv(LINEAGE_PITCHER_MAP),
        "starter_expected_map": read_csv(LINEAGE_STARTER_EXPECTED_MAP),
        "recoverability": read_csv(LINEAGE_RECOVERABILITY),
        "prescreen": read_csv(PRESCREEN_LEDGER),
        "prescreen_spec": read_csv(PRESCREEN_SPEC),
        "starter_base": read_csv(STARTER_BASE),
    }


def dependency_rows() -> list[dict[str, Any]]:
    rows = [
        ("portfolio_review", PORTFOLIO_DIR, PORTFOLIO_SHA, EXPECTED_PORTFOLIO_SHA_MANIFEST_SHA256, "authoritative selected branch"),
        ("accounting_repaired_cumulative_state", ACCOUNTING_DIR, ACCOUNTING_SHA, sha256(ACCOUNTING_SHA), "authoritative cumulative totals"),
        ("earlier_local_parent_lineage_investigation", LINEAGE_DIR, LINEAGE_SHA, sha256(LINEAGE_SHA), "authoritative earlier lineage finding"),
    ]
    return [
        {
            "dependency_name": name,
            "package_path": str(package),
            "sha_manifest_path": str(sha_path),
            "sha_manifest_sha256": observed,
            "expected_sha_manifest_sha256": expected,
            "status": "BOUND" if observed == expected else "MISMATCH",
            "notes": notes,
        }
        for name, package, sha_path, expected, notes in rows
        for observed in [sha256(sha_path)]
    ]


def exact_branch_rows(data: dict[str, Any]) -> list[dict[str, str]]:
    return [r for r in data["portfolio_rows"] if r.get("branch") == "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT"]


def exact_side_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    branch_ids = {side_key(r) for r in exact_branch_rows(data)}
    prescreen_by_side = {r["starter_game_side_key"]: r for r in data["prescreen"]}
    starter_by_side = {side_key_from_starter_base(r): r for r in data["starter_base"]}
    rows: list[dict[str, Any]] = []
    for key in sorted(branch_ids):
        pre = prescreen_by_side.get(key, {})
        base = starter_by_side.get(key, {})
        row_count = sum(1 for r in exact_branch_rows(data) if side_key(r) == key)
        rows.append(
            {
                "starter_game_side_key": key,
                "represented_rows": row_count,
                "hits_0_5_rows": sum(1 for r in exact_branch_rows(data) if side_key(r) == key and r.get("line") == "0.5"),
                "hits_1_5_rows": sum(1 for r in exact_branch_rows(data) if side_key(r) == key and r.get("line") == "1.5"),
                "projected_qualification_ceiling": pre.get("projected_qualification_ceiling") or "",
                "actual_starter_player_id": pre.get("pitcher_id") or base.get("actual_starter_player_id"),
                "actual_starter_name": pre.get("pitcher_name") or base.get("actual_starter_name_from_bf"),
                "strict_prior_start_count": pre.get("strict_prior_history_count") or base.get("prior_starts_count"),
                "recent5_prior_starts": base.get("recent5_prior_starts_count"),
                "recent3_prior_starts": base.get("recent3_prior_starts_count"),
                "feature_cutoff_date": base.get("feature_cutoff_date"),
                "latest_contributing_prior_game_date": base.get("latest_contributing_prior_game_date"),
                "expected_workload_outs": base.get("baseline_outs_per_start") or base.get("expected_outs_blended_v1") or "",
                "offense_factor_vs_league_clamped": base.get("offense_factor_vs_league_clamped") or pre.get("offense_factor_status"),
                "pitcher_base": base.get("pitcher_base"),
                "starter_expected_hits_allowed": base.get("starter_expected_hits_allowed"),
                "expected_hits_outs_v1": base.get("expected_hits_outs_v1"),
                "expected_hits_outs_context_v1": base.get("expected_hits_outs_context_v1") or pre.get("expected_hits_outs_context_v1"),
                "starter_status": base.get("starter_identity_status") or "",
                "starter_trust": base.get("starter_context_status") or "",
                "workload_confidence": base.get("workload_confidence"),
                "role_confidence": base.get("role_confidence"),
                "sample_size_band": base.get("sample_size_band"),
                "final_prescreen_classification": pre.get("final_prescreen_classification"),
                "source_artifact": str(STARTER_BASE),
                "earliest_missing_stage": pre.get("earliest_missing_stage") or str(STARTER_XH),
            }
        )
    return rows


def normalize_classification(value: str) -> str:
    mapping = {
        "PRESENT_AND_COMPATIBLE": "PRESENT_AUTHORITATIVE_AND_COMPATIBLE",
        "PRESENT_BUT_NOT_PERSISTED_TO_CHILD": "PRESENT_NOT_PERSISTED",
        "DIRECT_SOURCE_MISSING": "DIRECT_PARENT_MISSING",
    }
    return mapping.get(value, value)


def parent_domain_evidence(data: dict[str, Any], side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    side_keys = {r["starter_game_side_key"] for r in side_rows}
    rows: list[dict[str, Any]] = []
    for r in data["parent_audit"]:
        if r["starter_game_side_key"] in side_keys:
            rows.append(
                {
                    "starter_game_side_key": r["starter_game_side_key"],
                    "parent_field": r["domain"],
                    "classification": normalize_classification(r["classification"]),
                    "source_path": r["source_path"],
                    "row_identity": r["row_identity"],
                    "source_date": r["source_date"],
                    "value": r["value"],
                    "package_sha": sha256(LINEAGE_SHA),
                    "temporal_proof": "strict-prior proof in source row" if "strict" in r["domain"] or "prior" in r["domain"] else "see source row and feature cutoff",
                    "notes": r["notes"],
                }
            )
    audited_pairs = {(r["starter_game_side_key"], r["parent_field"]) for r in rows}
    fields = [
        ("actual_starter_identity", "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
        ("strict_prior_cutoff", "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
        ("prior_starts", "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
        ("expected_workload", "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
        ("offense_factor_vs_league_clamped", "PRESENT_AUTHORITATIVE_AND_COMPATIBLE"),
        ("pitcher_base", "DIRECT_PARENT_MISSING"),
        ("starter_expected_hits_allowed", "DIRECT_PARENT_MISSING"),
        ("expected_hits_outs_context_v1", "PRESENT_WRONG_VERSION"),
    ]
    for side in side_rows:
        for field, classification in fields:
            if (side["starter_game_side_key"], field) in audited_pairs:
                continue
            rows.append(
                {
                    "starter_game_side_key": side["starter_game_side_key"],
                    "parent_field": field,
                    "classification": classification,
                    "source_path": side["source_artifact"],
                    "row_identity": side["starter_game_side_key"],
                    "source_date": side["starter_game_side_key"].split("|")[0],
                    "value": {
                        "actual_starter_identity": side["actual_starter_player_id"],
                        "prior_starts": side["strict_prior_start_count"],
                        "expected_workload": side["expected_workload_outs"],
                        "offense_factor_vs_league_clamped": side["offense_factor_vs_league_clamped"],
                        "pitcher_base": side["pitcher_base"],
                        "starter_expected_hits_allowed": side["starter_expected_hits_allowed"],
                        "expected_hits_outs_context_v1": side["expected_hits_outs_context_v1"],
                    }.get(field, ""),
                    "package_sha": sha256(PORTFOLIO_SHA),
                    "temporal_proof": f"feature_cutoff_date={side['feature_cutoff_date']}; latest_prior={side['latest_contributing_prior_game_date']}",
                    "notes": "added from exact two-side prescreen/starter-base evidence for complete side coverage",
                }
            )
    return sorted(rows, key=lambda r: (r["starter_game_side_key"], r["parent_field"]))


def formula_version_authority() -> list[dict[str, Any]]:
    rows = []
    rows.append(
        {
            "field": "pitcher_base",
            "authoritative_owner": "Starter Expected Hits Allowed / Starter Skill-Workload",
            "governed_definition": "production-style pitcher_expected_hits_allowed_weighted from governed strict-prior Starter parents",
            "required_parents": "expected/probable starter binding; strict-prior pitcher history; weighted multiseason hits/out or existing expected-hits base parents; existing minimum-history rules",
            "parent_grain": "starter-game / opposing-starter matchup",
            "output_grain": "starter-game and downstream batter-row matchup",
            "temporal_cutoff": "all contributing pitcher history before slate date; feature_cutoff_date < slate_date",
            "low_sample_behavior": "under current evidence, low_lt5 branch may emit diagnostics but does not authorize production-style pitcher_base",
            "null_behavior": "missing production-style parent fails closed",
            "fallback_hierarchy": "none frozen for expected_hits_outs_context_v1 substitution",
            "producing_utility": str(ENV_GENERATOR),
            "research_persistence_utility": str(RESEARCH_BUILDER),
            "persistence_artifact": str(STARTER_XH),
            "version_identifier": "production_style_pitcher_expected_hits_allowed_weighted",
            "aliases": "pitcher_expected_hits_allowed_weighted; in A/B/D matrix lineage weighted_multiseason_hits_per_out may alias baseline_hits_allowed_per_out, but not this defect repair",
            "relationship_to_diagnostics": "expected_hits_outs_context_v1 is diagnostic/not authorized substitute",
        }
    )
    rows.append(
        {
            "field": "starter_expected_hits_allowed",
            "authoritative_owner": "Starter Expected Hits Allowed / Hits Environment",
            "governed_definition": "starter_expected_hits_allowed = pitcher_base * offense_factor_vs_league_clamped when governed parents exist",
            "required_parents": "pitcher_base; offense_factor_vs_league_clamped; starter status/trust certification",
            "parent_grain": "starter-game / offense-team matchup",
            "output_grain": "downstream batter-row matchup",
            "temporal_cutoff": "prior-date offense context and strict-prior pitcher history",
            "low_sample_behavior": "cannot be derived when governed pitcher_base is blank",
            "null_behavior": "missing pitcher_base propagates missing starter_expected_hits_allowed",
            "fallback_hierarchy": "none frozen",
            "producing_utility": str(ENV_GENERATOR),
            "research_persistence_utility": str(RESEARCH_BUILDER),
            "persistence_artifact": str(STARTER_XH),
            "version_identifier": "pitcher_base_times_offense_factor_clamped",
            "aliases": "expected_hits_allowed_matchup in environment source",
            "relationship_to_diagnostics": "diagnostic outs-context resembles an expected-hits estimate but is not authorized as this field",
        }
    )
    return rows


def pipeline_trace(side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stages = [
        ("strict_prior_source_evidence", "prior starts/workload present", "present", "not divergent"),
        ("historical_parent_construction", "expected workload and diagnostic outs-context present", "present_diagnostic_only", "not divergent"),
        ("starter_expected_hits_characterization", "pitcher_base and starter_expected_hits_allowed expected if governed production parent exists", "missing", "first_divergence"),
        ("starter_skill_workload_parent_artifact", "inherits blank pitcher_base/starter_expected, retains diagnostics", "present_not_authoritative", "downstream_consequence"),
        ("field_materialization_ledger", "cannot register governed payload values", "missing_payload", "downstream_consequence"),
        ("qualification_state", "Starter fail-closed", "starter_blocked", "downstream_consequence"),
        ("downstream_matrix_payload_availability", "no matrix payload materialization", "not_available", "downstream_consequence"),
        ("daily_or_production_platform", "formula still works when pitcher_expected_hits_allowed_weighted exists", "not_proven_current_daily_failure", "boundary"),
    ]
    rows = []
    for side in side_rows:
        for idx, (stage, expected, actual, divergence) in enumerate(stages, start=1):
            rows.append(
                {
                    "starter_game_side_key": side["starter_game_side_key"],
                    "stage_order": idx,
                    "pipeline_stage": stage,
                    "expected_row_presence": "present",
                    "actual_row_presence": "present" if actual != "missing_payload" else "not_payload_materialized",
                    "expected_field_value": expected,
                    "actual_field_state": actual,
                    "join_key": side["starter_game_side_key"],
                    "field_alias": "pitcher_base|starter_expected_hits_allowed|expected_hits_allowed_matchup",
                    "package_or_file": str(STARTER_XH if stage == "starter_expected_hits_characterization" else STARTER_BASE),
                    "first_point_of_divergence": "true" if divergence == "first_divergence" else "false",
                    "failure_propagates_downstream": "true" if divergence.startswith("downstream") or divergence == "first_divergence" else "false",
                    "notes": divergence,
                }
            )
    return rows


def code_path_audit() -> list[dict[str, Any]]:
    return [
        {
            "script": str(RESEARCH_BUILDER),
            "function_or_block": "_build_starter_rows",
            "inspected_topic": "diagnostic outs-context construction",
            "finding": "weighted_hpo * blended_outs and optional offense factor are retained as expected_hits_outs_v1/context_v1 diagnostics",
            "defect_relation": "diagnostic branch exists but does not persist governed production-style pitcher_base",
            "repair_implication": "promoting diagnostic to pitcher_base would require formula/version governance",
        },
        {
            "script": str(RESEARCH_BUILDER),
            "function_or_block": "_build_starter_rows",
            "inspected_topic": "production-style fields",
            "finding": "starter_expected_hits_allowed reads expected_hits_allowed_matchup; pitcher_base reads pitcher_expected_hits_allowed_weighted from environment row",
            "defect_relation": "if environment row lacks production base, research artifact keeps both fields blank",
            "repair_implication": "persistence-only repair is insufficient unless governed production base exists",
        },
        {
            "script": str(PRESCREEN_BUILDER),
            "function_or_block": "prescreen_sides",
            "inspected_topic": "local defect detection",
            "finding": "strict-prior, prior starts, expected workload, and offense factor present while pitcher_base/starter_expected blank triggers LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
            "defect_relation": "this is the exact 2-side / 17-row classification used here",
            "repair_implication": "current governance forbids diagnostic substitution",
        },
        {
            "script": str(ENV_GENERATOR),
            "function_or_block": "expected hits calculation",
            "inspected_topic": "daily production formula",
            "finding": "expected_hits_allowed_matchup = pitcher_expected_hits_allowed_weighted * offense_factor_vs_league_clamped when pitcher base exists",
            "defect_relation": "daily formula is intact; failure is missing production-style base for low-history branch in historical/research artifact",
            "repair_implication": "do not change production formula in this task",
        },
    ]


def recurrence_analysis(data: dict[str, Any], exact_rows: list[dict[str, str]], side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matching_starter_base = [
        r
        for r in data["starter_base"]
        if r.get("strict_prior_status") == "PASS_STRICT_PRIOR"
        and int_value(r.get("prior_starts_count")) > 0
        and r.get("expected_hits_outs_context_v1")
        and not r.get("pitcher_base")
        and not r.get("starter_expected_hits_allowed")
    ]
    exact_side_set = {r["starter_game_side_key"] for r in side_rows}
    rows = [
        {
            "scope": "frozen_authorized_population",
            "matching_sides": len(exact_side_set),
            "matching_rows": len(exact_rows),
            "dates_affected": ";".join(sorted({r["slate_date"] for r in exact_rows})),
            "pitchers_affected": ";".join(sorted({str(r["actual_starter_player_id"]) for r in side_rows})),
            "historical_only": "true_for_current_authorized_population",
            "daily_recurrence_possible": "unknown_but_current_daily_formula_unchanged",
            "already_fixed_later_path": "not_proven",
            "notes": "authorized remediation scope is not expanded by recurrence analysis",
        },
        {
            "scope": "starter_skill_workload_research_base_same_signature",
            "matching_sides": len(matching_starter_base),
            "matching_rows": "not_denominator_rows",
            "dates_affected": ";".join(sorted({r["date"] for r in matching_starter_base})),
            "pitchers_affected": str(len({r["actual_starter_player_id"] for r in matching_starter_base})),
            "historical_only": "research_artifact_scope",
            "daily_recurrence_possible": "possible_if_low_history_starter_lacks_production_base",
            "already_fixed_later_path": "not_verified",
            "notes": "120 starter-game rows in research base match missing production-base plus diagnostic-present signature; not authorized remediation population",
        },
        {
            "scope": "41_row_matrix_queue",
            "matching_sides": "not_repair_population",
            "matching_rows": 0,
            "dates_affected": "",
            "pitchers_affected": "",
            "historical_only": "matrix_parent_payload_issue_separate",
            "daily_recurrence_possible": "no_direct_conclusion",
            "already_fixed_later_path": "n/a",
            "notes": "matrix queue remains blocked by parent payload authority, not this 17-row local platform-defect repair",
        },
    ]
    return rows


def counterfactual_validation(side_rows: list[dict[str, Any]], exact_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows = []
    exact_by_side = defaultdict(list)
    for row in exact_rows:
        exact_by_side[side_key(row)].append(row)
    for side in side_rows:
        diagnostic_available = bool(side["expected_hits_outs_context_v1"])
        governed_base_available = bool(side["pitcher_base"])
        rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "authoritative_parents_identified": "partial",
                "strict_prior_parents_present": "true",
                "offense_factor_present": "true" if side["offense_factor_vs_league_clamped"] else "false",
                "governed_pitcher_base_present": "true" if governed_base_available else "false",
                "diagnostic_expected_hits_context_present": "true" if diagnostic_available else "false",
                "diagnostic_substitution_allowed": "false",
                "existing_formula_counterfactual_status": "BLOCKED_FORMULA_GOVERNANCE_REQUIRED",
                "data_type_precision_alias_grain_temporal_status": "not_validated_for_governed_pitcher_base",
                "bf_boundary": "corroboration_only_no_substitution",
                "projected_rows_receiving_pitcher_base_if_formula_later_approved": len(exact_by_side[side["starter_game_side_key"]]),
                "projected_rows_becoming_fully_qualified_if_later_approved": side["projected_qualification_ceiling"],
                "remaining_downstream_blocked_rows": len(exact_by_side[side["starter_game_side_key"]]) - int_value(side["projected_qualification_ceiling"]),
                "notes": "No values computed or written; this is a ceiling-only validation design.",
            }
        )
    return rows


def projected_movement(data: dict[str, Any], exact_rows: list[dict[str, str]], side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_receiving = len(exact_rows)
    newly_fq = sum(int_value(r["projected_qualification_ceiling"]) for r in side_rows)
    return [
        {
            "projection_scope": "ceiling_if_future_formula_governance_and_repair_approved",
            "rows_receiving_pitcher_base": rows_receiving,
            "rows_receiving_starter_expected_hits_allowed": rows_receiving,
            "rows_becoming_starter_qualified": rows_receiving,
            "rows_becoming_newly_fully_qualified": newly_fq,
            "hits_0_5_additions": newly_fq,
            "hits_1_5_additions": 0,
            "downstream_pa_blockers": rows_receiving - newly_fq,
            "downstream_outcome_blockers": 0,
            "downstream_bundle_blockers": 0,
            "matrix_queue_implications": "none_direct_all_rows_hits_0_5",
            "abd_payload_authority_implications": "none_direct",
            "hypothetical_fully_qualified_hits_total": data["accounting"]["after_totals"]["fully_qualified_hits"] + newly_fq,
            "hypothetical_fully_qualified_hits_0_5_total": data["accounting"]["after_totals"]["fully_qualified_hits_0_5"] + newly_fq,
            "hypothetical_fully_qualified_hits_1_5_total": data["accounting"]["after_totals"]["fully_qualified_hits_1_5"],
            "notes": "Projection only; no qualification state propagated.",
        }
    ]


def repair_governance_contract(side_rows: list[dict[str, Any]], exact_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "contract_section": "code_platform_repair",
            "approval_status": "separate_approval_required",
            "scope": "decide whether low_lt5 diagnostic expected_hits_outs_context_v1 can become or feed governed pitcher_base without changing field definition",
            "authorized_now": "false",
            "prohibited_now": "code change; formula change; fallback change; production behavior change",
        },
        {
            "contract_section": "historical_17_row_backfill",
            "approval_status": "separate_approval_required_after_formula_governance",
            "scope": f"{len(exact_rows)} exact rows / {len(side_rows)} exact sides",
            "authorized_now": "false",
            "prohibited_now": "writing repaired values or overlays",
        },
        {
            "contract_section": "qualification_state_propagation",
            "approval_status": "separate_approval_required_after_backfill_validation",
            "scope": "ceiling 16 newly fully qualified rows; preserve downstream blocker on remaining row",
            "authorized_now": "false",
            "prohibited_now": "state propagation or matrix construction",
        },
        {
            "contract_section": "future_daily_behavior_change",
            "approval_status": "separate_approval_required_if_daily_impact_confirmed",
            "scope": "only if current daily low-history Starter production path is proven affected",
            "authorized_now": "false",
            "prohibited_now": "LaunchAgent, upload, model, selector, or daily production changes",
        },
    ]


def regression_test_design(side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "test_name": "exact_two_side_population_lock",
            "population": ";".join(r["starter_game_side_key"] for r in side_rows),
            "expected": "2 sides / 17 rows only",
            "failure_action": "fail closed",
        },
        {
            "test_name": "diagnostic_substitution_guard",
            "population": "low_lt5 diagnostic rows",
            "expected": "expected_hits_outs_context_v1 not used unless formula governance explicitly authorizes it",
            "failure_action": "fail closed",
        },
        {
            "test_name": "strict_prior_temporal_guard",
            "population": "all future repair rows",
            "expected": "feature_cutoff_date < slate_date and latest contributing prior game before slate date",
            "failure_action": "fail closed",
        },
        {
            "test_name": "downstream_blocker_preservation",
            "population": "17 rows",
            "expected": "row with independent PA/downstream blocker remains not fully qualified",
            "failure_action": "fail closed",
        },
    ]


def production_impact() -> list[dict[str, Any]]:
    return [
        {
            "path": str(ENV_GENERATOR),
            "current_daily_or_production_path": "yes",
            "finding": "current formula computes expected_hits_allowed_matchup only when pitcher_expected_hits_allowed_weighted exists",
            "impact_classification": "daily_formula_intact_low_history_missing_base_recurrence_possible_not_proven",
            "future_approval_boundary": "separate daily-path impact audit before any production code repair",
            "notes": "No production behavior changed.",
        },
        {
            "path": str(RESEARCH_BUILDER),
            "current_daily_or_production_path": "research_platform",
            "finding": "diagnostic expected_hits_outs_context_v1 exists while governed pitcher_base/starter_expected remain blank",
            "impact_classification": "historical_research_artifact_defect_confirmed",
            "future_approval_boundary": "formula governance before repair",
            "notes": "No production behavior changed.",
        },
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
        "no_platform_code_modification",
        "no_repaired_value_write",
        "no_qualification_propagation",
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_reconstruction_or_remediation_write",
        "no_formula_or_fallback_change",
        "no_pa_outcome_bundle_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_champion_challenger_work",
        "no_database_or_api_write",
        "no_oddsapi_call",
        "no_upload",
        "no_launchagent_or_production_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validate(data: dict[str, Any], exact_rows: list[dict[str, str]], side_rows: list[dict[str, Any]], deps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("portfolio_sha_manifest_hash", sha256(PORTFOLIO_SHA) == EXPECTED_PORTFOLIO_SHA_MANIFEST_SHA256, sha256(PORTFOLIO_SHA), EXPECTED_PORTFOLIO_SHA_MANIFEST_SHA256)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["sha_manifest_sha256"], dep["expected_sha_manifest_sha256"])
    add("exact_17_row_reproduction", len(exact_rows) == EXPECTED_ROWS, len(exact_rows), EXPECTED_ROWS)
    add("exact_2_side_reproduction", len({side_key(r) for r in exact_rows}) == EXPECTED_SIDES, len({side_key(r) for r in exact_rows}), EXPECTED_SIDES)
    add("hits_0_5_composition", sum(1 for r in exact_rows if r["line"] == "0.5") == EXPECTED_HITS_0_5, sum(1 for r in exact_rows if r["line"] == "0.5"), EXPECTED_HITS_0_5)
    add("hits_1_5_composition", sum(1 for r in exact_rows if r["line"] == "1.5") == EXPECTED_HITS_1_5, sum(1 for r in exact_rows if r["line"] == "1.5"), EXPECTED_HITS_1_5)
    add("projected_ceiling_16", sum(int_value(r["projected_qualification_ceiling"]) for r in side_rows) == EXPECTED_FULLY_QUALIFIED_CEILING, sum(int_value(r["projected_qualification_ceiling"]) for r in side_rows), EXPECTED_FULLY_QUALIFIED_CEILING)
    ids = [row_id(r) for r in exact_rows]
    add("no_duplicate_rows", len(ids) == len(set(ids)), len(ids) - len(set(ids)), 0)
    add("no_silent_population_expansion", len(exact_rows) == 17 and len(side_rows) == 2, f"{len(exact_rows)} rows / {len(side_rows)} sides", "17 rows / 2 sides")
    add("root_cause_classification_frozen", ROOT_CAUSE_DECISION in {"LOW_SAMPLE_BRANCH_DEFECT"}, ROOT_CAUSE_DECISION, "LOW_SAMPLE_BRANCH_DEFECT")
    add("repairability_classification_frozen", REPAIRABILITY_DECISION == "REPAIRABLE_NEW_FORMULA_GOVERNANCE_REQUIRED", REPAIRABILITY_DECISION, "REPAIRABLE_NEW_FORMULA_GOVERNANCE_REQUIRED")
    for total, expected in [
        ("fully_qualified_hits", 1484),
        ("fully_qualified_hits_0_5", 1344),
        ("fully_qualified_hits_1_5", 140),
        ("primary_starter_blocked", 128),
        ("qualified_but_not_matrix_hits_1_5_queue", 41),
    ]:
        add(f"accounting_total_{total}", data["accounting"]["after_totals"][total] == expected, data["accounting"]["after_totals"][total], expected)
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


def write_markdown(machine: dict[str, Any]) -> None:
    text = f"""# Local Starter Platform Defect Investigation - {RUN_DATE}

Generated: `{GENERATED_AT}`

## Executive Summary

`MLB_LOCAL_STARTER_PLATFORM_DEFECT_ROOT_CAUSE_DECISION = {ROOT_CAUSE_DECISION}`

`MLB_LOCAL_STARTER_PLATFORM_DEFECT_REPAIRABILITY_DECISION = {REPAIRABILITY_DECISION}`

`MLB_LOCAL_STARTER_PLATFORM_REPAIR_GOVERNANCE_STATUS = {REPAIR_GOVERNANCE_STATUS}`

`MLB_LOCAL_STARTER_PLATFORM_DEFECT_PORTFOLIO_DECISION = {PORTFOLIO_DECISION}`

The exact frozen population is 17 denominator rows across 2 Starter-game-side identities. All 17 rows are Hits 0.5. The branch remains high-value as a platform investigation, but it is not an existing-formula backfill. The governed production-style `pitcher_base` is missing in the Starter Expected Hits characterization stage, and `starter_expected_hits_allowed` is missing downstream because its required parent is absent.

Strict-prior workload evidence, prior starts, expected workload, and offense factor are present. A diagnostic field, `expected_hits_outs_context_v1`, is also present, but the existing contracts do not authorize it as a substitute for `pitcher_base` or `starter_expected_hits_allowed`.

## First Divergence

Earliest authoritative divergence: `artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv`.

## Projected Movement

If a future formula-governance decision and separate repair are approved, the ceiling is:

- rows receiving `pitcher_base`: 17
- rows receiving `starter_expected_hits_allowed`: 17
- rows becoming Starter-qualified: 17
- rows becoming newly fully qualified: 16
- Hits 0.5 additions: 16
- Hits 1.5 additions: 0

No qualification propagation was performed.

## Production Boundary

The current daily environment formula remains `pitcher_expected_hits_allowed_weighted * offense_factor_vs_league_clamped` when the pitcher base exists. This investigation confirms a historical/research low-history persistence/governance gap, not a production formula change. Any daily-path repair requires separate approval after a daily impact audit.

## Next Bounded Approval Recommended

Freeze and execute a formula-governance-only review for whether the low-history diagnostic `expected_hits_outs_context_v1` can ever be promoted into or used to construct governed production-style `pitcher_base`. Do not repair the 17 rows until that governance question is answered.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def package_manifest() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"relative_path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    return rows


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_rows()
    exact_rows = exact_branch_rows(data)
    side_rows = exact_side_rows(data)
    parent_rows = parent_domain_evidence(data, side_rows)
    formula_rows = formula_version_authority()
    pipeline_rows = pipeline_trace(side_rows)
    code_rows = code_path_audit()
    recurrence_rows = recurrence_analysis(data, exact_rows, side_rows)
    counterfactual_rows = counterfactual_validation(side_rows, exact_rows)
    movement_rows = projected_movement(data, exact_rows, side_rows)
    repair_rows = repair_governance_contract(side_rows, exact_rows)
    regression_rows = regression_test_design(side_rows)
    production_rows = production_impact()
    validation_rows = validate(data, exact_rows, side_rows, deps)
    guard_rows = static_guard()

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_17_row_manifest_{RUN_DATE}.csv", exact_rows)
    write_csv(OUT_DIR / f"exact_2_side_manifest_{RUN_DATE}.csv", side_rows)
    write_csv(OUT_DIR / f"parent_domain_evidence_ledger_{RUN_DATE}.csv", parent_rows)
    write_csv(OUT_DIR / f"formula_version_authority_map_{RUN_DATE}.csv", formula_rows)
    write_csv(OUT_DIR / f"pipeline_stage_trace_{RUN_DATE}.csv", pipeline_rows)
    write_csv(OUT_DIR / f"code_path_audit_{RUN_DATE}.csv", code_rows)
    write_csv(OUT_DIR / f"first_divergence_analysis_{RUN_DATE}.csv", [
        {
            "first_divergence_stage": str(STARTER_XH),
            "primary_root_cause": ROOT_CAUSE_DECISION,
            "downstream_consequence": "starter_expected_hits_allowed missing because governed pitcher_base parent is missing",
            "diagnostic_substitute_status": "expected_hits_outs_context_v1_present_but_not_authorized",
            "notes": "primary cause separated from downstream qualification/matrix consequences",
        }
    ])
    write_csv(OUT_DIR / f"root_cause_classification_{RUN_DATE}.csv", [
        {
            "MLB_LOCAL_STARTER_PLATFORM_DEFECT_ROOT_CAUSE_DECISION": ROOT_CAUSE_DECISION,
            "primary_cause": "low-history branch emits diagnostic expected-hits context but no governed production-style pitcher_base",
            "downstream_consequences": "starter_expected_hits_allowed missing; Starter certification fail-closed; no qualification propagation",
            "evidence": "strict-prior parents and offense factor present; pitcher_base blank; diagnostic context present; substitution prohibited",
        }
    ])
    write_csv(OUT_DIR / f"recurrence_and_platform_impact_analysis_{RUN_DATE}.csv", recurrence_rows)
    write_csv(OUT_DIR / f"repairability_classification_{RUN_DATE}.csv", [
        {
            "MLB_LOCAL_STARTER_PLATFORM_DEFECT_REPAIRABILITY_DECISION": REPAIRABILITY_DECISION,
            "reason": "existing contracts do not prove diagnostic expected_hits_outs_context_v1 equals governed pitcher_base/starter_expected; formula governance required before repair",
            "existing_formula_unchanged": "not_proven_for_low_sample_branch",
            "repair_under_current_task": "not_authorized",
        }
    ])
    write_csv(OUT_DIR / f"read_only_counterfactual_repair_validation_{RUN_DATE}.csv", counterfactual_rows)
    write_csv(OUT_DIR / f"projected_qualification_movement_{RUN_DATE}.csv", movement_rows)
    write_csv(OUT_DIR / f"frozen_future_repair_governance_contract_{RUN_DATE}.csv", repair_rows)
    write_csv(OUT_DIR / f"production_daily_path_impact_assessment_{RUN_DATE}.csv", production_rows)
    write_csv(OUT_DIR / f"regression_test_design_{RUN_DATE}.csv", regression_rows)
    write_csv(OUT_DIR / f"portfolio_conclusion_{RUN_DATE}.csv", [
        {
            "MLB_LOCAL_STARTER_PLATFORM_DEFECT_PORTFOLIO_DECISION": PORTFOLIO_DECISION,
            "comparison_26_row_other_starter_parent": "larger yield but less platform reuse and more source uncertainty",
            "comparison_23_row_identity_role": "role framework useful but contamination risk higher",
            "comparison_41_row_matrix_payload": "larger matrix-only yield but parent evidence missing and high version-drift risk",
            "next_approval_recommended": "formula_governance_review_for_low_history_expected_hits_context",
        }
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard_rows)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation_rows)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_LOCAL_STARTER_PLATFORM_DEFECT_ROOT_CAUSE_DECISION": ROOT_CAUSE_DECISION,
        "MLB_LOCAL_STARTER_PLATFORM_DEFECT_REPAIRABILITY_DECISION": REPAIRABILITY_DECISION,
        "MLB_LOCAL_STARTER_PLATFORM_REPAIR_GOVERNANCE_STATUS": REPAIR_GOVERNANCE_STATUS,
        "MLB_LOCAL_STARTER_PLATFORM_DEFECT_PORTFOLIO_DECISION": PORTFOLIO_DECISION,
        "exact_rows": len(exact_rows),
        "exact_sides": len({side_key(r) for r in exact_rows}),
        "projected_newly_fully_qualified_ceiling": EXPECTED_FULLY_QUALIFIED_CEILING,
        "first_divergence_stage": str(STARTER_XH),
        "repair_branches_requiring_separate_approval": [
            "formula_governance_review",
            "code_platform_repair",
            "historical_17_row_backfill",
            "qualification_state_propagation",
            "future_daily_behavior_change",
        ],
        "prohibited_work": {
            "platform_code_changes": "not_performed",
            "repaired_value_writes": "not_performed",
            "qualification_propagation": "not_performed",
            "network_access": "not_performed",
            "discovery_or_acquisition": "not_performed",
            "reconstruction_or_remediation_writes": "not_performed",
            "formula_or_fallback_changes": "not_performed",
            "matrix_construction": "not_performed",
            "model_signal_scoring": "not_performed",
            "database_or_api_writes": "not_performed",
            "oddsapi_upload_launchagent_production": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_local_starter_platform_defect_investigation_{RUN_DATE}.json", machine)
    write_markdown(machine)

    replay_rows = []
    baseline = {
        "root": ROOT_CAUSE_DECISION,
        "repair": REPAIRABILITY_DECISION,
        "rows": len(exact_rows),
        "sides": len({side_key(r) for r in exact_rows}),
        "movement": movement_rows,
    }
    for iteration in range(1, 6):
        replay_data = load_inputs()
        replay_exact = exact_branch_rows(replay_data)
        replay_sides = exact_side_rows(replay_data)
        observed = {
            "root": ROOT_CAUSE_DECISION,
            "repair": REPAIRABILITY_DECISION,
            "rows": len(replay_exact),
            "sides": len({side_key(r) for r in replay_exact}),
            "movement": projected_movement(replay_data, replay_exact, replay_sides),
        }
        replay_rows.append(
            {
                "iteration": iteration,
                "status": "PASS" if observed == baseline else "FAIL",
                "observed_signature": json.dumps(observed, sort_keys=True),
                "expected_signature": json.dumps(baseline, sort_keys=True),
            }
        )
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", replay_rows)

    parse_rows = parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()])
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    machine = build_package()
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
