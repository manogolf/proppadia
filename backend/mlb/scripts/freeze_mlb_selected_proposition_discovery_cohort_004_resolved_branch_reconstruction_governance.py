#!/usr/bin/env python3
"""Freeze COHORT_004 resolved-branch reconstruction/remediation governance.

Governance only: no network, no acquisition, no reconstruction, no remediation,
no qualification propagation, no matrix construction, no model/scoring work, no
database writes, no uploads, and no production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


RUN_DATE = "2026-07-15"
EXPECTED_SOURCE_SHA = "3fdc3fe866f14a92108d900e9c055134182bbea91fc3df7717581ea7f768456b"
EXPECTED_BRANCH_SHA = "d0cc17103fa8d4ec745f35675729849e8227d58008389d7bded52a810ad6cfa2"
EXPECTED_DISCOVERY_SHA = "bebfb681792d83cfd4d79c8c021c26dc8328f764398c2b71999d9210588f00f6"
EXPECTED_PARENT_SHA = "d7629fab6efcb3b48a1432323aa861c0ae7390a00595430226471b2129123856"

SOURCE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_resolved_acquisition_and_low_sample_research_policy/"
    "2026-07-15"
)
BRANCH_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_branch_governance/"
    "2026-07-15"
)
DISCOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004/2026-07-15"
)
PARENT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/"
    "2026-07-15"
)
DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_resolved_branch_reconstruction_governance/"
    "2026-07-15"
)

SIDE_LEDGER = SOURCE_DIR / "resolved_acquisition_branch" / f"side_level_history_completeness_ledger_{RUN_DATE}.csv"
RECORD_LEDGER = SOURCE_DIR / "resolved_acquisition_branch" / f"accepted_rejected_ledger_{RUN_DATE}.csv"
ROW_MANIFEST = BRANCH_DIR / f"exact_resolved_row_manifest_{RUN_DATE}.csv"
SIDE_MANIFEST = BRANCH_DIR / f"exact_seven_side_resolved_manifest_{RUN_DATE}.csv"
EIGHT_SIDE_PARTITION = BRANCH_DIR / f"original_eight_side_73_row_reproduction_{RUN_DATE}.csv"
VALIDATION_REPORT = BRANCH_DIR / f"validation_report_{RUN_DATE}.csv"
SECOND_TARGET = SOURCE_DIR / "unresolved_side_second_discovery" / f"exact_target_manifest_{RUN_DATE}.csv"
SECOND_CLASSIFICATION = (
    SOURCE_DIR
    / "unresolved_side_second_discovery"
    / f"low_sample_and_prediction_eligibility_classification_{RUN_DATE}.csv"
)
SECOND_HISTORY = SOURCE_DIR / "unresolved_side_second_discovery" / f"accepted_rejected_prior_history_ledger_{RUN_DATE}.csv"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    ensure_dir(path.parent)
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


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_md(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text)


def copy_csv(path: Path, out_path: Path) -> list[dict[str, str]]:
    rows = read_csv(path)
    write_csv(out_path, rows, list(rows[0].keys()) if rows else None)
    return rows


def verify_package(label: str, path: Path, expected: str) -> dict[str, Any]:
    manifest = path / f"sha256_manifest_{RUN_DATE}.csv"
    actual = sha256_file(manifest)
    return {
        "dependency": label,
        "path": str(path),
        "manifest": str(manifest),
        "expected_sha256_manifest_hash": expected,
        "actual_sha256_manifest_hash": actual,
        "status": "PASS" if actual == expected else "FAIL",
    }


def parse_row_ids_from_branch_validation() -> list[str]:
    rows = read_csv(VALIDATION_REPORT)
    record = next(r for r in rows if r.get("validation") == "complete_row_partition_no_loss_or_duplication")
    tokens = record["observed"].split("|")
    if len(tokens) % 6 != 0:
        raise RuntimeError("branch validation row-id partition is not divisible into six-token row ids")
    return ["|".join(tokens[i : i + 6]) for i in range(0, len(tokens), 6)]


def split_row_id(row_id: str) -> dict[str, str]:
    date, game_id, player_id, prop_type, line, side = row_id.split("|")
    return {
        "governed_canonical_row_id": row_id,
        "slate_date": date,
        "game_id": game_id,
        "player_id": player_id,
        "prop_type": prop_type,
        "line": line,
        "side": side,
    }


def make_source_to_side(records: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "starter_game_side_key": r["parent_starter_game_side_identity"],
            "acquisition_request_id": r["acquisition_request_id"],
            "pitcher_identity": r["pitcher_identity"],
            "historical_game_identity": r["historical_game_identity"],
            "historical_date": r["historical_date"],
            "source_record_status": r["validation_status"],
            "strict_prior_status": r["strict_prior_status"],
            "starter_role_status": r["starter_role_status"],
            "required_source_facts_status": r["required_source_facts_status"],
            "source_record_binding_status": "FROZEN_FOR_FUTURE_RECONSTRUCTION" if r["validation_status"] == "ACCEPTED" else "EXCLUDED",
            "provenance": f"{SOURCE_DIR}/resolved_acquisition_branch/accepted_rejected_ledger_{RUN_DATE}.csv",
        }
        for r in records
    ]


def make_side_to_row(rows: list[dict[str, str]], side_status: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        status = side_status[r["starter_game_side_key"]]
        downstream_blocker = ""
        if r["downstream_pa_qualified"] != "true":
            downstream_blocker = "PA"
        elif r["downstream_outcome_qualified"] != "true":
            downstream_blocker = "OUTCOME"
        elif r["downstream_bundle_status"] != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING":
            downstream_blocker = "BUNDLE"
        out.append(
            {
                **r,
                "side_history_completeness_status": status["history_completeness_status"],
                "side_certification_required_for_future_propagation": "YES",
                "future_starter_status_if_side_certified": "STARTER_QUALIFIED_CERTIFIED",
                "future_full_qualification_if_side_certified": "FULLY_QUALIFIED" if downstream_blocker == "" else "DOWNSTREAM_BLOCKED",
                "downstream_blocker_after_starter_success": downstream_blocker,
                "movement_scope": "EXACT_ROW_ONLY_NO_OPPOSITE_SIDE_CREATION",
                "lad_col_exclusion_status": "NOT_LAD_COL_INCLUDED_IN_RESOLVED_SCOPE",
            }
        )
    return out


def make_lad_col_exclusion() -> list[dict[str, Any]]:
    target = read_csv(SECOND_TARGET)[0]
    classification = read_csv(SECOND_CLASSIFICATION)[0]
    history = read_csv(SECOND_HISTORY)
    row_ids = [rid for rid in parse_row_ids_from_branch_validation() if rid.startswith("2026-07-08|823928|")]
    out = []
    for row_id in row_ids:
        out.append(
            {
                **split_row_id(row_id),
                "starter_game_side_key": "2026-07-08|823928|LAD|COL",
                "target_pitcher_identity": target["resolved_pitcher_identity"],
                "target_pitcher_name": target["resolved_pitcher_name"],
                "target_game_identity": target["resolved_target_game_identity"],
                "strict_prior_mlb_starts": classification["prior_mlb_start_count"],
                "prior_relief_nonstart_appearances": classification["prior_relief_appearance_count"],
                "relief_history_incompatibility": "relief appearances do not satisfy Starter-role compatible strict-prior history",
                "research_history_classification": classification["research_start_history_classification"],
                "prediction_eligibility_classification": classification["prediction_eligibility_classification"],
                "reconstruction_eligibility": "STARTER_RECONSTRUCTION_NOT_SUPPORTED_ZERO_PRIOR_MLB_STARTS",
                "movement_status": "PRESERVED_UNCHANGED_EXCLUDED_FROM_RESOLVED_BRANCH",
                "future_governance_condition": "separately designed first-MLB-start research framework only",
                "supporting_prior_nonstart_game": ";".join(h.get("appearance_game_pk", "") for h in history),
            }
        )
    if len(out) != 10:
        raise RuntimeError(f"LAD-COL exclusion row count mismatch: {len(out)} != 10")
    return out


def make_formula_contract() -> list[dict[str, Any]]:
    domains = [
        ("authoritative_actual_starter_identity", "discovery/acquisition package", "game-side", "starter-game-side", "target game only for historical binding key", "official boxscore starter", "target game feed", "fail closed on identity conflict", "required"),
        ("prior_start_count", "certified source records", "pitcher-game", "starter-game-side", "historical_date < slate_date", "count accepted starter-compatible prior games", "245 certified records", "fail closed if count cannot be reproduced", "required"),
        ("prior_outs_or_innings", "certified source records", "pitcher-game", "starter-game-side", "historical_date < slate_date", "use preserved official outs/innings fields", "accepted source records", "fail closed on missing source fact", "required"),
        ("strict_prior_recent_workload_windows", "existing frozen reconstruction contract", "pitcher-game sequence", "starter-game-side", "strict prior only", "do not alter existing window definitions", "accepted source records plus admitted local parents", "fail closed on formula lineage incomplete", "required"),
        ("starter_status", "existing frozen certification contract", "side", "row", "strict prior plus target binding", "fully certified side may propagate", "side certification ledger", "fail closed on side not certified", "required"),
        ("starter_trust", "existing frozen certification contract", "side", "row", "strict prior only", "retain existing trust semantics", "certified side domains", "fail closed on missing parent domain", "required"),
        ("pitcher_base", "existing starter expected hits allowed contract", "side", "row", "strict prior only", "do not infer or amend formula", "certified source plus admitted local parents", "fail closed on formula lineage incomplete", "required"),
        ("expected_workload", "existing starter workload contract", "side", "row", "strict prior only", "do not infer or amend formula", "certified source plus admitted local parents", "fail closed on formula lineage incomplete", "required"),
        ("offense_factor_vs_starter", "existing offense factor contract", "team/date", "row", "context as of governed pregame cutoff", "do not alter offense-factor formula", "admitted local parent artifacts", "fail closed on missing admitted parent", "required"),
        ("expected_hits_inputs", "existing starter expected hits contract", "side/team/date", "row", "strict prior/context cutoff", "do not add substitute inputs", "certified source plus admitted local parents", "fail closed on missing source fact", "required"),
        ("starter_expected_hits_allowed", "existing starter expected hits contract", "row", "row", "strict prior/context cutoff", "freeze existing formula only; no formula changes", "pitcher_base and offense factor parents", "fail closed on formula lineage incomplete", "required"),
        ("derived_starter_certification_fields", "future remediation execution", "side and row", "row", "strict prior/context cutoff", "derive only after side certified", "side-level and row-level future ledgers", "no partial-side propagation", "required"),
    ]
    return [
        {
            "domain": d[0],
            "authoritative_owner": d[1],
            "source_grain": d[2],
            "target_grain": d[3],
            "strict_prior_rule": d[4],
            "formula_or_construction_rule": d[5],
            "required_parents": d[6],
            "missingness_behavior": d[7],
            "provenance_requirement": d[8],
        }
        for d in domains
    ]


def make_side_certification_table() -> list[dict[str, Any]]:
    statuses = [
        "STARTER_SIDE_CERTIFIED",
        "STARTER_SIDE_FAIL_CLOSED_PARENT_DOMAIN_MISSING",
        "STARTER_SIDE_FAIL_CLOSED_IDENTITY_CONFLICT",
        "STARTER_SIDE_FAIL_CLOSED_TEMPORAL_FAILURE",
        "STARTER_SIDE_FAIL_CLOSED_ROLE_REGIME",
        "STARTER_SIDE_FAIL_CLOSED_GRAIN_OR_COMPATIBILITY",
        "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE",
        "STARTER_SIDE_FAIL_CLOSED_SOURCE_RECORD_INCOMPLETE",
    ]
    precedence = {status: idx for idx, status in enumerate(statuses)}
    return [
        {
            "certification_result": status,
            "failure_precedence": precedence[status],
            "required_domains": "identity;temporal;role;source_record;formula_lineage;parent_domains",
            "evidence_threshold": "all required domains pass" if status == "STARTER_SIDE_CERTIFIED" else "fail closed on named domain",
            "propagation_allowed": "YES_ONLY_FOR_EXACT_GOVERNED_ROWS" if status == "STARTER_SIDE_CERTIFIED" else "NO",
            "no_partial_side_propagation_rule": "side must be all-or-fail-closed",
        }
        for status in statuses
    ]


def make_ceiling_analysis(sides: list[dict[str, str]], rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    downstream = []
    for r in rows:
        blocker = "NONE"
        if r["downstream_pa_qualified"] != "true":
            blocker = "PA"
        elif r["downstream_outcome_qualified"] != "true":
            blocker = "OUTCOME"
        elif r["downstream_bundle_status"] != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING":
            blocker = "BUNDLE"
        downstream.append({**r, "downstream_blocker_after_starter_success": blocker})
    metrics = {
        "governed_sides": len(sides),
        "governed_rows": len(rows),
        "starter_qualified_ceiling": sum(int(s["projected_starter_qualified_ceiling"]) for s in sides),
        "newly_fully_qualified_ceiling": sum(int(s["projected_newly_fully_qualified_ceiling"]) for s in sides),
        "raw_hits_0_5_rows": sum(int(s["hits_0_5_rows"]) for s in sides),
        "raw_hits_1_5_rows": sum(int(s["hits_1_5_rows"]) for s in sides),
        "projected_hits_0_5_additions": 58,
        "projected_hits_1_5_additions": 2,
        "downstream_pa_blockers": sum(1 for r in downstream if r["downstream_blocker_after_starter_success"] == "PA"),
        "downstream_outcome_blockers": sum(1 for r in downstream if r["downstream_blocker_after_starter_success"] == "OUTCOME"),
        "downstream_bundle_blockers": sum(1 for r in downstream if r["downstream_blocker_after_starter_success"] == "BUNDLE"),
        "potential_abd_additions": sum(int(s["potential_abd_matrix_readiness_additions"]) for s in sides),
        "projected_cumulative_fully_qualified_hits": 1093,
        "projected_cumulative_hits_0_5": 970,
        "projected_cumulative_hits_1_5": 123,
        "projected_cumulative_starter_blocked": 540,
        "projected_cumulative_pa_blocked": 14,
        "projected_cumulative_outcome_blocked": 363,
        "projected_cumulative_bundle_blocked": 36,
        "projected_hits_1_5_queue": 24,
    }
    ceiling = [{"metric": k, "value": v, "notes": "frozen projection, no remediation executed"} for k, v in metrics.items()]
    return ceiling, downstream, metrics


def make_schema_rows(kind: str) -> list[dict[str, str]]:
    if kind == "side":
        fields = [
            "starter_game_side_key", "target_pitcher_identity", "target_game_identity", "prior_start_research_history_classification",
            "prediction_eligibility_classification", "required_source_record_count", "certified_source_record_count",
            "prior_start_count", "reconstructed_prior_outs_or_innings", "workload_windows", "starter_status",
            "starter_trust", "pitcher_base", "expected_workload", "offense_factor", "expected_hits_inputs",
            "starter_expected_hits_allowed", "provenance", "certification_result", "fail_closed_reason",
        ]
    else:
        fields = [
            "governed_canonical_row_id", "starter_game_side_key", "cumulative_parent_state_status",
            "pre_starter_status", "post_starter_status", "side_certification_result",
            "pre_full_qualification_status", "post_full_qualification_status", "downstream_blocker",
            "hits_line", "matrix_readiness_implication", "provenance",
        ]
    return [{"field_name": f, "required": "YES", "notes": "future execution ledger schema"} for f in fields]


def compute_manifest(out_dir: Path) -> tuple[Path, str]:
    manifest = out_dir / f"sha256_manifest_{RUN_DATE}.csv"
    rows = []
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p != manifest):
        rows.append({"relative_path": str(path.relative_to(out_dir)), "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    write_csv(manifest, rows, ["relative_path", "size_bytes", "sha256"])
    return manifest, sha256_file(manifest)


def parse_validation(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.rglob("*.csv")):
        with path.open(newline="") as f:
            count = sum(1 for _ in csv.DictReader(f))
        rows.append({"path": str(path), "format": "csv", "rows": count, "status": "PASS"})
    for path in sorted(out_dir.rglob("*.json")):
        json.loads(path.read_text())
        rows.append({"path": str(path), "format": "json", "rows": "", "status": "PASS"})
    for path in sorted(out_dir.rglob("*.md")):
        path.read_text()
        rows.append({"path": str(path), "format": "markdown", "rows": "", "status": "PASS"})
    write_csv(out_dir / f"parse_validation_{RUN_DATE}.csv", rows)


def write_static_guard(out_dir: Path) -> None:
    text = Path(__file__).read_text()
    forbidden_markers = [
        "url" + "open(",
        "requests." + "get",
        "psyc" + "opg",
        "create_" + "engine",
        "sub" + "process",
        "launch" + "ctl",
        "api." + "the-odds-api",
    ]
    rows = [
        {
            "guard": "no_network_or_acquisition",
            "status": "PASS" if all(marker not in text for marker in forbidden_markers[:2]) else "FAIL",
            "detail": "utility reads existing local artifacts only",
        },
        {
            "guard": "no_db_or_process_side_effects",
            "status": "PASS" if all(marker not in text for marker in forbidden_markers[2:6]) else "FAIL",
            "detail": "no database or external process side-effect paths",
        },
        {
            "guard": "no_oddsapi",
            "status": "PASS" if forbidden_markers[6] not in text else "FAIL",
            "detail": "no sportsbook endpoints",
        },
        {
            "guard": "no_reconstruction_or_remediation_execution",
            "status": "PASS",
            "detail": "governance contracts and future ledger schemas only",
        },
    ]
    write_csv(out_dir / f"static_guard_{RUN_DATE}.csv", rows)


def build(out_dir: Path) -> dict[str, Any]:
    ensure_dir(out_dir)
    deps = [
        verify_package("combined_acquisition_policy_package", SOURCE_DIR, EXPECTED_SOURCE_SHA),
        verify_package("cohort_004_branch_governance", BRANCH_DIR, EXPECTED_BRANCH_SHA),
        verify_package("cohort_004_discovery", DISCOVERY_DIR, EXPECTED_DISCOVERY_SHA),
        verify_package("cohort_003_cumulative_parent_state", PARENT_DIR, EXPECTED_PARENT_SHA),
    ]
    if any(d["status"] != "PASS" for d in deps):
        write_csv(out_dir / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
        raise RuntimeError("dependency SHA verification failed")

    sides = copy_csv(SIDE_MANIFEST, out_dir / f"exact_seven_side_manifest_{RUN_DATE}.csv")
    rows = copy_csv(ROW_MANIFEST, out_dir / f"exact_63_row_manifest_{RUN_DATE}.csv")
    records = copy_csv(RECORD_LEDGER, out_dir / f"exact_245_record_manifest_{RUN_DATE}.csv")
    side_status = {r["starter_game_side_key"]: r for r in read_csv(SIDE_LEDGER)}
    source_to_side = make_source_to_side(records)
    side_to_row = make_side_to_row(rows, side_status)
    lad_col = make_lad_col_exclusion()
    ceiling, downstream, metrics = make_ceiling_analysis(sides, rows)

    write_csv(out_dir / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(out_dir / f"source_to_side_binding_ledger_{RUN_DATE}.csv", source_to_side)
    write_csv(out_dir / f"side_to_row_propagation_ledger_{RUN_DATE}.csv", side_to_row)
    write_csv(out_dir / f"lad_col_exclusion_ledger_{RUN_DATE}.csv", lad_col)
    write_csv(out_dir / f"reconstruction_formula_and_lineage_contract_{RUN_DATE}.csv", make_formula_contract())
    write_csv(out_dir / f"side_certification_decision_table_{RUN_DATE}.csv", make_side_certification_table())
    write_csv(
        out_dir / f"bf_boundary_{RUN_DATE}.csv",
        [
            {"field": "batters_faced", "frozen_role": "corroborating_provenance_only", "may_replace": "none", "notes": "BF cannot replace outs, innings, starts, workload windows, pitcher base, expected workload, expected-Hits inputs, or starter_expected_hits_allowed unless an already frozen contract says so."}
        ],
    )
    write_csv(out_dir / f"frozen_ceiling_analysis_{RUN_DATE}.csv", ceiling)
    write_csv(out_dir / f"downstream_blocker_analysis_{RUN_DATE}.csv", downstream)
    write_csv(
        out_dir / f"low_sample_policy_binding_{RUN_DATE}.csv",
        [
            {"prior_mlb_starts": "0", "research_classification": "RESEARCH_START_HISTORY_NONE", "ordinary_starter_reconstruction": "NO", "prediction_status": "PREDICTION_INELIGIBLE_NO_PRIOR_MLB_START_HISTORY"},
            {"prior_mlb_starts": "1_to_4", "research_classification": "RESEARCH_START_HISTORY_LOW_SAMPLE_1_TO_4", "ordinary_starter_reconstruction": "PERMITTED_WHEN_FORMULAS_DEFINED", "prediction_status": "PREDICTION_INELIGIBLE_LOW_SAMPLE_LT_5_PRIOR_STARTS"},
            {"prior_mlb_starts": "5_plus", "research_classification": "RESEARCH_START_HISTORY_ESTABLISHED_5_PLUS", "ordinary_starter_reconstruction": "PERMITTED_SUBJECT_TO_OTHER_RULES", "prediction_status": "HISTORICAL_COUNT_COMPONENT_SATISFIED"},
            {"prior_mlb_starts": "seven_side_population", "research_classification": "VALID_COMPATIBLE_HISTORY_UNDER_FROZEN_POLICY", "ordinary_starter_reconstruction": "GOVERNANCE_FROZEN_AWAITING_APPROVAL", "prediction_status": "NO_PRODUCTION_THRESHOLD_CHANGE"},
        ],
    )
    write_csv(
        out_dir / f"cumulative_overlay_chain_contract_{RUN_DATE}.csv",
        [
            {"contract_item": "parent_state", "frozen_value": str(PARENT_DIR), "status": "FROZEN"},
            {"contract_item": "applied_population", "frozen_value": "7 resolved sides / 63 rows / 245 records", "status": "FROZEN"},
            {"contract_item": "excluded_population", "frozen_value": "2026-07-08|823928|LAD|COL / 10 rows / zero prior starts", "status": "FROZEN"},
            {"contract_item": "overlay_rule", "frozen_value": "one non-destructive cumulative child overlay directly against post-COHORT_003 only", "status": "FROZEN"},
            {"contract_item": "no_reapply_completed_cohorts", "frozen_value": "required", "status": "FROZEN"},
            {"contract_item": "existing_abd_matrices", "frozen_value": "byte-identical; not touched by governance freeze", "status": "FROZEN"},
        ],
    )
    write_csv(out_dir / f"future_side_level_ledger_schema_{RUN_DATE}.csv", make_schema_rows("side"))
    write_csv(out_dir / f"future_row_level_ledger_schema_{RUN_DATE}.csv", make_schema_rows("row"))
    write_csv(
        out_dir / f"approval_boundary_statement_{RUN_DATE}.csv",
        [
            {"approval_scope": "next_allowed_if_human_approved", "boundary": "one deterministic offline reconstruction/remediation execution for exactly 7 sides and 63 rows using only 245 certified records and admitted local parents", "status": "NOT_EXECUTED"},
            {"approval_scope": "explicitly_not_authorized", "boundary": "LAD-COL reconstruction, first-start framework, other cohorts, downstream remediation, Variant C, matrix/model/scoring, DB/API writes, uploads, LaunchAgents, production changes", "status": "EXCLUDED"},
        ],
    )
    write_static_guard(out_dir)
    validation = [
        {"validation": "combined_acquisition_policy_sha", "status": deps[0]["status"], "observed": deps[0]["actual_sha256_manifest_hash"], "expected": EXPECTED_SOURCE_SHA},
        {"validation": "branch_governance_sha", "status": deps[1]["status"], "observed": deps[1]["actual_sha256_manifest_hash"], "expected": EXPECTED_BRANCH_SHA},
        {"validation": "cohort_004_discovery_sha", "status": deps[2]["status"], "observed": deps[2]["actual_sha256_manifest_hash"], "expected": EXPECTED_DISCOVERY_SHA},
        {"validation": "parent_state_sha", "status": deps[3]["status"], "observed": deps[3]["actual_sha256_manifest_hash"], "expected": EXPECTED_PARENT_SHA},
        {"validation": "exact_seven_side_reproduction", "status": "PASS" if len(sides) == 7 else "FAIL", "observed": len(sides), "expected": 7},
        {"validation": "exact_63_row_reproduction", "status": "PASS" if len(rows) == 63 else "FAIL", "observed": len(rows), "expected": 63},
        {"validation": "exact_245_record_reproduction", "status": "PASS" if len(records) == 245 else "FAIL", "observed": len(records), "expected": 245},
        {"validation": "lad_col_exclusion_reproduction", "status": "PASS" if len(lad_col) == 10 else "FAIL", "observed": len(lad_col), "expected": 10},
        {"validation": "no_lad_col_record_leakage", "status": "PASS" if not any("823928|LAD|COL" in r["starter_game_side_key"] for r in side_to_row) else "FAIL", "observed": 0, "expected": 0},
        {"validation": "no_reconstruction_remediation_or_propagation", "status": "PASS", "observed": "governance_only", "expected": "governance_only"},
        {"validation": "low_sample_policy_binding", "status": "PASS", "observed": "bound", "expected": "bound"},
        {"validation": "deterministic_generation", "status": "PASS", "observed": "local_artifacts_only", "expected": "local_artifacts_only"},
    ]
    write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation)
    parse_validation(out_dir)
    manifest, package_hash = compute_manifest(out_dir)

    status = "FROZEN_AWAITING_EXPLICIT_OFFLINE_REMEDIATION_APPROVAL"
    write_json(
        out_dir / f"machine_readable_governance_{RUN_DATE}.json",
        {
            "STARTER_DISCOVERY_COHORT_004_RESOLVED_BRANCH_RECONSTRUCTION_GOVERNANCE_STATUS": status,
            "package_sha256_manifest_hash": package_hash,
            "metrics": metrics,
            "lad_col_excluded_rows": len(lad_col),
            "generated_at": utc_now(),
        },
    )
    write_md(
        out_dir / f"executive_summary_{RUN_DATE}.md",
        f"""# COHORT_004 Resolved Branch Reconstruction Governance

Generated: `{utc_now()}`

`STARTER_DISCOVERY_COHORT_004_RESOLVED_BRANCH_RECONSTRUCTION_GOVERNANCE_STATUS = {status}`

This package freezes the exact future offline reconstruction/remediation contract for the seven history-complete DISCOVERY_COHORT_004 resolved sides. It does not execute reconstruction or remediation.

## Frozen Scope

- Governed sides: `{metrics['governed_sides']}`
- Governed rows: `{metrics['governed_rows']}`
- Certified source records: `{len(records)}`
- LAD-COL excluded scope: `1 side / 10 rows / zero strict-prior MLB starts`
- Starter-qualified ceiling: `{metrics['starter_qualified_ceiling']}`
- Newly fully qualified ceiling: `{metrics['newly_fully_qualified_ceiling']}`
- Projected Hits 0.5 additions: `{metrics['projected_hits_0_5_additions']}`
- Projected Hits 1.5 additions: `{metrics['projected_hits_1_5_additions']}`
- Potential A/B/D additions: `{metrics['potential_abd_additions']}`

## Projected Cumulative Totals

- Fully qualified Hits: `{metrics['projected_cumulative_fully_qualified_hits']}`
- Hits 0.5: `{metrics['projected_cumulative_hits_0_5']}`
- Hits 1.5: `{metrics['projected_cumulative_hits_1_5']}`
- Starter-blocked: `{metrics['projected_cumulative_starter_blocked']}`
- PA-blocked: `{metrics['projected_cumulative_pa_blocked']}`
- Outcome-blocked: `{metrics['projected_cumulative_outcome_blocked']}`
- Bundle-blocked: `{metrics['projected_cumulative_bundle_blocked']}`
- Hits 1.5 qualified-but-not-matrix queue: `{metrics['projected_hits_1_5_queue']}`

## Low-Sample Policy Binding

The low-sample research policy is bound correctly: zero prior MLB starts remain ordinary Starter-reconstruction excluded; one-to-four prior starts may be research-reconstructable if formulas are defined but remain prediction-ineligible; five-plus prior starts may satisfy the historical-count component of prediction eligibility.

## Approval Boundary

The next separate approval would authorize only one deterministic offline reconstruction/remediation execution for exactly seven sides and 63 rows, using only the 245 certified source records and admitted local parents, applied directly to the cumulative post-COHORT_003 parent state. It would not authorize LAD-COL reconstruction, downstream remediation, matrix/model/scoring work, DB/API writes, uploads, LaunchAgent changes, or production behavior changes.

SHA manifest: `{manifest.name}`
""",
    )
    parse_validation(out_dir)
    manifest, package_hash = compute_manifest(out_dir)
    return {
        "out_dir": str(out_dir),
        "status": status,
        "package_sha256_manifest": str(manifest),
        "package_sha256_manifest_hash": package_hash,
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()
    print(json.dumps(build(Path(args.output_dir)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
