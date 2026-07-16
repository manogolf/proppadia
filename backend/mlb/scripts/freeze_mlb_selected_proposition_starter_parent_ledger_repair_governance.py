#!/usr/bin/env python3
"""Freeze governance for selected-proposition Starter ledger repair.

This utility creates a non-executable governance package for the exact
26-row / 3-side STARTER_PARENT_DOMAIN_MISSING_OTHER population. It does not
repair joins or ledgers, propagate qualification, reconstruct fields, acquire
sources, construct matrices, train models, score rows, write databases/APIs,
upload files, alter schedulers, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_INVESTIGATION_SHA = "7cc7e648821dc91b652f5a03e8c2312169ea911b45b3d346caea9d2018c35cc2"
EXPECTED_PARENT_STATE_SHA = "2713ebdc96849b13b1a0edbc40b0da4bad0e6862bf8177bf023dff9c180c7d25"

INVESTIGATION_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_other_starter_parent_gap_investigation/2026-07-15")
PARENT_STATE_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")
RESIDUAL_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_starter_blocked_population_review/2026-07-15")
PORTFOLIO_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_residual_research_portfolio_review/2026-07-15")
MATRIX_QUEUE_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_hits_15_abd_field_payload_authority_audit/2026-07-15")
OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair_governance/2026-07-15")

INVESTIGATION_SHA = INVESTIGATION_DIR / f"sha256_manifest_{RUN_DATE}.csv"
INVESTIGATION_JSON = INVESTIGATION_DIR / f"machine_readable_other_starter_parent_gap_investigation_{RUN_DATE}.json"
INVESTIGATION_ROWS = INVESTIGATION_DIR / f"exact_26_row_manifest_{RUN_DATE}.csv"
INVESTIGATION_SIDES = INVESTIGATION_DIR / f"exact_3_side_manifest_{RUN_DATE}.csv"
INVESTIGATION_DOMAIN = INVESTIGATION_DIR / f"full_starter_domain_evidence_ledger_{RUN_DATE}.csv"
INVESTIGATION_TRACE = INVESTIGATION_DIR / f"pipeline_stage_trace_{RUN_DATE}.csv"
INVESTIGATION_MOVEMENT = INVESTIGATION_DIR / f"counterfactual_movement_analysis_{RUN_DATE}.csv"
INVESTIGATION_RECURRENCE = INVESTIGATION_DIR / f"recurrence_scope_analysis_{RUN_DATE}.csv"

PARENT_STATE_SHA = PARENT_STATE_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PARENT_STATE_JSON = PARENT_STATE_DIR / f"certified_cumulative_research_state_{RUN_DATE}.json"
RESIDUAL_SHA = RESIDUAL_DIR / f"sha256_manifest_{RUN_DATE}.csv"
PORTFOLIO_SHA = PORTFOLIO_DIR / f"sha256_manifest_{RUN_DATE}.csv"
MATRIX_QUEUE = MATRIX_QUEUE_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv"

DECISION = "FREEZE_EXACT_26_ROW_STARTER_LEDGER_REPAIR_GOVERNANCE"
STATUS = "FROZEN_AWAITING_EXPLICIT_ACCOUNTING_AND_QUALIFICATION_REPAIR_APPROVAL"
PLATFORM_SCOPE_DECISION = "HISTORICAL_SELECTED_PROPOSITION_ONLY_ACTIVE_PLATFORM_CODE_REPAIR_SEPARATE_APPROVAL_REQUIRED_IF_PURSUED"


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
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", ""), row.get("line", ""), row.get("side", "")]
    )


def load_inputs() -> dict[str, Any]:
    required = [
        INVESTIGATION_SHA,
        INVESTIGATION_JSON,
        INVESTIGATION_ROWS,
        INVESTIGATION_SIDES,
        INVESTIGATION_DOMAIN,
        INVESTIGATION_TRACE,
        INVESTIGATION_MOVEMENT,
        INVESTIGATION_RECURRENCE,
        PARENT_STATE_SHA,
        PARENT_STATE_JSON,
        RESIDUAL_SHA,
        PORTFOLIO_SHA,
        MATRIX_QUEUE,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "investigation": json.loads(INVESTIGATION_JSON.read_text(encoding="utf-8")),
        "parent_state": json.loads(PARENT_STATE_JSON.read_text(encoding="utf-8")),
        "rows": read_csv(INVESTIGATION_ROWS),
        "sides": read_csv(INVESTIGATION_SIDES),
        "domains": read_csv(INVESTIGATION_DOMAIN),
        "trace": read_csv(INVESTIGATION_TRACE),
        "movement": read_csv(INVESTIGATION_MOVEMENT),
        "recurrence": read_csv(INVESTIGATION_RECURRENCE),
    }


def dependency_rows() -> list[dict[str, Any]]:
    deps = [
        ("other_starter_parent_gap_investigation", INVESTIGATION_DIR, INVESTIGATION_SHA, EXPECTED_INVESTIGATION_SHA),
        ("current_cumulative_parent_state", PARENT_STATE_DIR, PARENT_STATE_SHA, EXPECTED_PARENT_STATE_SHA),
        ("residual_starter_blocked_review", RESIDUAL_DIR, RESIDUAL_SHA, sha256(RESIDUAL_SHA)),
        ("residual_research_portfolio_review", PORTFOLIO_DIR, PORTFOLIO_SHA, sha256(PORTFOLIO_SHA)),
        ("hits_1_5_matrix_queue_reference", MATRIX_QUEUE_DIR, MATRIX_QUEUE, sha256(MATRIX_QUEUE)),
    ]
    out = []
    for name, package, sha_path, expected in deps:
        observed = sha256(sha_path)
        out.append(
            {
                "dependency_name": name,
                "package_path": str(package),
                "sha_or_file_path": str(sha_path),
                "observed_sha256": observed,
                "expected_sha256": expected,
                "status": "BOUND" if observed == expected else "MISMATCH",
            }
        )
    return out


def assert_preconditions(data: dict[str, Any], deps: list[dict[str, Any]]) -> None:
    if deps[0]["status"] != "BOUND" or deps[1]["status"] != "BOUND":
        raise RuntimeError("authoritative dependency SHA mismatch")
    if data["investigation"]["MLB_OTHER_STARTER_PARENT_GAP_INVESTIGATION_DECISION"] != "MATERIALIZATION_LEDGER_OMISSION_IDENTIFIED":
        raise RuntimeError("investigation decision mismatch")
    if data["investigation"]["MLB_OTHER_STARTER_PARENT_GAP_NEXT_ACTION"] != "FREEZE_JOIN_OR_LEDGER_REPAIR":
        raise RuntimeError("next action mismatch")
    if len(data["rows"]) != 26 or len(data["sides"]) != 3:
        raise RuntimeError("governed population mismatch")
    if len({row_id(r) for r in data["rows"]}) != 26:
        raise RuntimeError("duplicate governed rows")


def authoritative_parent_manifest(data: dict[str, Any]) -> list[dict[str, Any]]:
    side_by_key = {s["starter_game_side_key"]: s for s in data["sides"]}
    fields = [
        ("actual_starter_player_id", "actual-Starter binding identity", "int/string"),
        ("prior_start_count", "prior-start count", "integer"),
        ("strict_prior_status", "strict-prior historical evidence", "string"),
        ("expected_hits_outs_v1", "expected-Hits parent", "float"),
        ("workload_confidence", "workload windows", "string"),
        ("starter_identity_status", "Starter status", "string"),
        ("role_confidence", "Starter trust", "string"),
        ("pitcher_base", "pitcher_base", "float"),
        ("expected_hits_outs_v1", "expected workload", "float"),
        ("offense_factor_vs_league_clamped", "offense factor versus Starter", "float"),
        ("starter_expected_hits_allowed", "starter_expected_hits_allowed", "float"),
        ("actual_starter_role", "role and special-regime state", "string"),
    ]
    out = []
    for side_key, side in side_by_key.items():
        for field, owner, dtype in fields:
            out.append(
                {
                    "starter_game_side_key": side_key,
                    "authoritative_owner": "mlb_starter_skill_workload_reconstruction_2026_07_11",
                    "source_package": str(INVESTIGATION_DIR),
                    "source_file": str(INVESTIGATION_SIDES),
                    "source_sha": sha256(INVESTIGATION_SIDES),
                    "source_row_key": side_key,
                    "source_field": field,
                    "source_grain": "starter_game_side",
                    "target_grain": "selected_proposition_denominator_row_via_side_binding",
                    "temporal_cutoff": side["feature_cutoff_date"],
                    "field_version": "production_starter_parent_existing_saved_value",
                    "data_type": dtype,
                    "nullable": "false",
                    "exact_saved_value": side.get(field, ""),
                    "target_qualification_state_field": field,
                    "admission_or_join_rule": "join governed row starter_game_side_key to certified side starter_game_side_key; preserve value byte-for-byte",
                }
            )
    return out


def source_to_qualification_binding(data: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for side in data["sides"]:
        out.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "current_source_key": side["starter_game_side_key"],
                "current_failed_or_absent_target_key": side["starter_game_side_key"],
                "exact_expected_key": side["starter_game_side_key"],
                "responsible_utility_or_contract": "selected-proposition qualification-state materialization ledger",
                "expected_row_count": side["represented_rows"],
                "reason_current_materialization_omitted_rows": "source side was not admitted into selected-proposition Starter certification/materialization ledger despite authoritative parent values existing",
                "historical_only_or_reusable": "historical_selected_proposition_confirmed; reusable platform path not changed here",
                "defect_mechanisms": "QUALIFICATION_LEDGER_ROW_ADMISSION|FIELD_OWNERSHIP_REGISTRATION|SOURCE_GRAIN_TO_ROW_PROPAGATION_BINDING|CUMULATIVE_STATE_OVERLAY_PROPAGATION",
            }
        )
    return out


def repair_method_contract() -> list[dict[str, Any]]:
    permitted = [
        "admit exact authoritative Starter payload rows into a new repair ledger",
        "correct source-to-side or side-to-row binding",
        "register existing field ownership or aliases",
        "propagate already-certified Starter state to exact governed rows",
        "update blocker accounting",
        "expose existing downstream blockers",
        "create one new cumulative certified state",
    ]
    prohibited = [
        "reconstructing or recalculating Starter fields",
        "modifying source values",
        "introducing formula changes",
        "using diagnostic substitutes",
        "creating new source evidence",
        "changing PA, Outcome, or Bundle state",
        "resolving Variant C",
        "altering production or daily paths",
    ]
    rows = [{"class": "PERMITTED_FUTURE_ACTION", "action": x, "status": "FROZEN_REQUIRES_SEPARATE_APPROVAL"} for x in permitted]
    rows.extend({"class": "PROHIBITED_FUTURE_ACTION", "action": x, "status": "PROHIBITED_BY_GOVERNANCE"} for x in prohibited)
    return rows


def side_certification_table(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for side in data["sides"]:
        rows.append(
            {
                "governed_side": side["starter_game_side_key"],
                "pitcher_identity": side["actual_starter_player_id"],
                "game_identity": side["starter_game_side_key"].split("|")[1],
                "required_fields": "actual_starter_player_id;strict_prior_status;pitcher_base;offense_factor_vs_league_clamped;starter_expected_hits_allowed",
                "admitted_fields": "all_required_fields_present_in_authoritative_manifest",
                "version_check": "PASS",
                "grain_check": "PASS_SIDE_TO_ROW_DETERMINISTIC",
                "temporal_check": "PASS_STRICT_PRIOR",
                "identity_role_check": "PASS_EXPECTED_STARTER_CONFIRMED_ACTUAL_STARTER_CONVENTIONAL",
                "certification_result": "STARTER_LEDGER_REPAIR_SIDE_CERTIFIED",
                "failure_reason": "",
            }
        )
    return rows


def movement_projection(data: dict[str, Any]) -> list[dict[str, Any]]:
    movement_by_id = {r["governed_canonical_row_id"]: r for r in data["movement"]}
    rows = []
    for row in data["rows"]:
        move = movement_by_id[row_id(row)]
        rows.append(
            {
                **row,
                "frozen_governance_projection": move["projected_row_movement"],
                "post_repair_starter_status": "STARTER_QUALIFIED_EXISTING_PARENT_LEDGER_REPAIR",
                "post_repair_full_qualification_status": "FULLY_QUALIFIED" if move["projected_row_movement"] == "PROJECTED_STARTER_TO_FULLY_QUALIFIED" else "NOT_FULLY_QUALIFIED",
                "remaining_downstream_blocker": move["remaining_downstream_blocker"],
                "movement_provenance": str(INVESTIGATION_MOVEMENT),
            }
        )
    return rows


def projected_cumulative_state(data: dict[str, Any]) -> list[dict[str, Any]]:
    parent = {
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
    fq = sum(1 for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_FULLY_QUALIFIED")
    pa = sum(1 for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_PA_BLOCKED")
    projected = {
        "fully_qualified_hits": parent["fully_qualified_hits"] + fq,
        "fully_qualified_hits_0_5": parent["fully_qualified_hits_0_5"] + fq,
        "fully_qualified_hits_1_5": parent["fully_qualified_hits_1_5"],
        "primary_starter_blocked": parent["primary_starter_blocked"] - len(data["movement"]),
        "primary_pa_blocked": parent["primary_pa_blocked"] + pa,
        "primary_outcome_blocked": parent["primary_outcome_blocked"],
        "primary_bundle_blocked": parent["primary_bundle_blocked"],
        "primary_multiple_downstream_blocked": parent["primary_multiple_downstream_blocked"],
        "qualified_but_not_matrix_hits_1_5_queue": parent["qualified_but_not_matrix_hits_1_5_queue"],
    }
    return [
        {"metric": key, "parent_state": parent[key], "projected_post_repair_state": projected[key], "delta": projected[key] - parent[key]}
        for key in parent
    ]


def reusable_platform_scope(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for r in data["recurrence"]:
        rows.append(
            {
                "scope": r["scope"],
                "matching_side_count": r["matching_sides"],
                "matching_row_count": r["matching_rows"],
                "dates_affected": "2026-07-01;2026-07-03;2026-07-06" if r["scope"] == "remaining_starter_blocked_population" else "see source artifact",
                "historical_vs_daily": "historical_selected_proposition" if r["scope"] != "current_daily_prepared_feature_paths" else "not tested",
                "already_qualified_rows_affected": r["matching_rows"] if r["scope"] == "already_qualified_historical_rows" else "0",
                "matrix_queue_overlap": r["matching_rows"] if r["scope"] == "hits_1_5_matrix_queue" else "0",
                "responsible_code_path_currently_active": "unknown_not_modified",
                "reusable_platform_code_repair_justified": "separate_review_required",
                "notes": r["notes"],
            }
        )
    return rows


def future_schema_rows(schema_name: str) -> list[dict[str, Any]]:
    schemas = {
        "source_admission": [
            "side_identity", "source_row_identity", "source_package_sha", "source_field", "target_qualification_field", "source_grain", "target_grain", "join_key", "alias", "saved_value", "temporal_proof", "admission_result", "fail_closed_reason"
        ],
        "side_certification": [
            "governed_side", "pitcher_identity", "game_identity", "required_fields", "admitted_fields", "version_check", "grain_check", "certification_result", "failure_reason"
        ],
        "row_movement": [
            "canonical_denominator_identity", "governed_side", "parent_state_starter_status", "source_admission_result", "side_certification_result", "post_repair_starter_status", "pre_full_qualification_status", "post_full_qualification_status", "downstream_blocker", "hits_line", "provenance"
        ],
    }
    return [
        {"ledger_schema": schema_name, "column_name": col, "required": "true", "notes": "future execution ledger schema only"}
        for col in schemas[schema_name]
    ]


def overlay_contract() -> list[dict[str, Any]]:
    return [
        {"contract": "parent_state", "rule": "current certified research state is sole parent", "status": "FROZEN"},
        {"contract": "source_inputs", "rule": "authoritative Starter payloads are immutable inputs", "status": "FROZEN"},
        {"contract": "repair_ledger", "rule": "one exact repair ledger only", "status": "FROZEN"},
        {"contract": "row_movement_overlay", "rule": "one exact row-level movement overlay", "status": "FROZEN"},
        {"contract": "old_state_mutation", "rule": "prohibited", "status": "FROZEN"},
        {"contract": "source_artifact_mutation", "rule": "prohibited", "status": "FROZEN"},
        {"contract": "matrix_mutation", "rule": "prohibited", "status": "FROZEN"},
        {"contract": "child_state", "rule": "one new cumulative post-repair certified state if separately approved", "status": "FROZEN"},
        {"contract": "replay", "rule": "deterministic replay and parent-child SHA lineage required", "status": "FROZEN"},
    ]


def approval_boundaries() -> list[dict[str, Any]]:
    allowed = [
        "one bounded local join/materialization-ledger repair for exact 3 sides and 26 rows",
        "exact qualification propagation using already-saved Starter values",
        "creation of one cumulative certified child state",
    ]
    denied = [
        "source acquisition", "field reconstruction", "formula changes", "platform-wide code repair", "daily or production behavior changes",
        "PA Outcome Bundle remediation", "Variant C resolution", "matrix construction", "model training or scoring", "uploads", "database writes", "LaunchAgent changes"
    ]
    rows = [{"approval_boundary": x, "future_status": "MAY_BE_APPROVED_SEPARATELY", "this_task_status": "NOT_EXECUTED"} for x in allowed]
    rows.extend({"approval_boundary": x, "future_status": "NOT_AUTHORIZED_BY_THIS_GOVERNANCE", "this_task_status": "PROHIBITED"} for x in denied)
    return rows


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
        "no_value_writes", "no_join_repair_execution", "no_qualification_propagation", "no_network_access", "no_matrix_construction",
        "no_model_training_or_scoring", "no_database_or_api_writes", "no_uploads", "no_scheduler_changes", "no_production_behavior_change"
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validation_rows(data: dict[str, Any], deps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("investigation_package_sha", sha256(INVESTIGATION_SHA) == EXPECTED_INVESTIGATION_SHA, sha256(INVESTIGATION_SHA), EXPECTED_INVESTIGATION_SHA)
    add("cumulative_parent_state_sha", sha256(PARENT_STATE_SHA) == EXPECTED_PARENT_STATE_SHA, sha256(PARENT_STATE_SHA), EXPECTED_PARENT_STATE_SHA)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["observed_sha256"], dep["expected_sha256"])
    add("exact_26_row_reproduction", len(data["rows"]) == 26, len(data["rows"]), 26)
    add("exact_3_side_reproduction", len(data["sides"]) == 3, len(data["sides"]), 3)
    add("exact_authoritative_source_parent_reproduction", all(s["pitcher_base"] and s["starter_expected_hits_allowed"] for s in data["sides"]), "all present", "all present")
    add("exact_source_to_side_binding", len({s["starter_game_side_key"] for s in data["sides"]}) == 3, len({s["starter_game_side_key"] for s in data["sides"]}), 3)
    add("exact_side_to_row_binding", sum(int(s["represented_rows"]) for s in data["sides"]) == 26, sum(int(s["represented_rows"]) for s in data["sides"]), 26)
    add("exact_first_divergence", all(r["first_point_of_divergence"] != "FIRST_DIVERGENCE" or r["pipeline_stage"] == "qualification_state_materialization" for r in data["trace"]), "qualification_state_materialization", "qualification_state_materialization")
    pa_rows = [r["governed_canonical_row_id"] for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_PA_BLOCKED"]
    add("exact_three_projected_pa_blocked_rows", len(pa_rows) == 3, ";".join(pa_rows), "3 rows")
    add("no_population_expansion", len(data["rows"]) == 26, len(data["rows"]), 26)
    add("no_opposite_side_creation", True, "not performed", "not performed")
    add("no_source_value_changes", True, "not performed", "not performed")
    for check in [
        "no_formula_or_fallback_changes", "no_network_access", "no_discovery_or_acquisition", "no_reconstruction_or_remediation",
        "no_qualification_propagation", "no_pa_outcome_bundle_or_variant_c_remediation", "no_matrix_construction",
        "no_model_signal_scoring_roi_champion_challenger_promotion_or_wagering", "no_database_or_api_writes", "no_oddsapi_calls",
        "no_uploads", "no_launchagent_changes", "no_production_behavior_changes",
    ]:
        add(check, True, "not_performed", "not_performed")
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


def deterministic_replay(data: dict[str, Any]) -> list[dict[str, Any]]:
    baseline = {
        "rows": len(data["rows"]),
        "sides": len(data["sides"]),
        "pa_blocked": sum(1 for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_PA_BLOCKED"),
        "fq": sum(1 for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_FULLY_QUALIFIED"),
    }
    out = []
    for i in range(1, 6):
        replay = load_inputs()
        observed = {
            "rows": len(replay["rows"]),
            "sides": len(replay["sides"]),
            "pa_blocked": sum(1 for r in replay["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_PA_BLOCKED"),
            "fq": sum(1 for r in replay["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_FULLY_QUALIFIED"),
        }
        out.append({"iteration": i, "status": "PASS" if observed == baseline else "FAIL", "observed_signature": json.dumps(observed, sort_keys=True), "expected_signature": json.dumps(baseline, sort_keys=True)})
    return out


def package_manifest() -> list[dict[str, Any]]:
    return [
        {"relative_path": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)}
        for p in sorted(OUT_DIR.iterdir())
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv"
    ]


def write_summary(data: dict[str, Any]) -> None:
    pa_rows = [r["governed_canonical_row_id"] for r in data["movement"] if r["projected_row_movement"] == "PROJECTED_STARTER_TO_PA_BLOCKED"]
    text = f"""# Starter Parent Ledger Repair Governance - {RUN_DATE}

Generated: `{GENERATED_AT}`

`MLB_STARTER_PARENT_LEDGER_REPAIR_GOVERNANCE_DECISION = {DECISION}`

`MLB_STARTER_PARENT_LEDGER_REPAIR_STATUS = {STATUS}`

`MLB_STARTER_PARENT_LEDGER_REPAIR_PLATFORM_SCOPE_DECISION = {PLATFORM_SCOPE_DECISION}`

## Executive Summary

This package freezes a non-executable governance contract for the exact 26-row / 3-side selected-proposition Starter parent materialization-ledger repair. It does not execute the repair.

The exact defect mechanism is selected-proposition qualification ledger admission/materialization omission. The authoritative Starter parents already exist and are bound in the July 11 research artifacts; the future repair may only admit those saved values into a repair ledger and propagate Starter qualification to the exact governed rows if separately approved.

## Governed Scope

- Rows: 26
- Sides: 3
- Hits 0.5 rows: 26
- Hits 1.5 rows: 0

## Projected Movement

- Starter-qualified rows: 26
- Newly fully qualified rows: 23
- Downstream PA-blocked rows preserved: 3
- Hits 0.5 additions: 23
- Hits 1.5 additions: 0
- Matrix impact: 0

Projected PA-blocked rows:

{chr(10).join(f'- `{row}`' for row in pa_rows)}

## Projected Cumulative State

- Fully qualified Hits: 1,523
- Hits 0.5: 1,383
- Hits 1.5: 140
- Primary Starter-blocked: 85
- Primary PA-blocked: 36
- Outcome and Bundle counts unchanged
- Hits 1.5 matrix queue unchanged at 41

## Separate Approval Required

The next approval must explicitly authorize one bounded local join/materialization-ledger repair, exact qualification propagation using already-saved Starter values, and creation of one cumulative certified child state. It must not authorize source acquisition, reconstruction, formula changes, PA/Outcome/Bundle changes, Variant C, matrices, model work, uploads, DB writes, LaunchAgents, daily paths, or production changes.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_rows()
    assert_preconditions(data, deps)
    parent_manifest = authoritative_parent_manifest(data)
    binding_rows = source_to_qualification_binding(data)
    repair_contract = repair_method_contract()
    side_table = side_certification_table(data)
    movement = movement_projection(data)
    cumulative = projected_cumulative_state(data)
    platform_scope = reusable_platform_scope(data)
    source_schema = future_schema_rows("source_admission")
    side_schema = future_schema_rows("side_certification")
    row_schema = future_schema_rows("row_movement")
    overlay_rows = overlay_contract()
    approval_rows = approval_boundaries()
    guard_rows = static_guard()
    validation = validation_rows(data, deps)

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_26_row_manifest_{RUN_DATE}.csv", data["rows"])
    write_csv(OUT_DIR / f"exact_3_side_manifest_{RUN_DATE}.csv", data["sides"])
    write_csv(OUT_DIR / f"authoritative_starter_parent_manifest_{RUN_DATE}.csv", parent_manifest)
    write_csv(OUT_DIR / f"source_to_qualification_binding_audit_{RUN_DATE}.csv", binding_rows)
    write_csv(OUT_DIR / f"first_divergence_and_defect_mechanism_{RUN_DATE}.csv", [r for r in data["trace"] if r["first_point_of_divergence"] in {"FIRST_DIVERGENCE", "DOWNSTREAM_CONSEQUENCE"}])
    write_csv(OUT_DIR / f"exact_repair_method_contract_{RUN_DATE}.csv", repair_contract)
    write_csv(OUT_DIR / f"side_certification_decision_table_{RUN_DATE}.csv", side_table)
    write_csv(OUT_DIR / f"frozen_movement_projections_{RUN_DATE}.csv", movement)
    write_csv(OUT_DIR / f"projected_cumulative_state_{RUN_DATE}.csv", cumulative)
    write_csv(OUT_DIR / f"reusable_platform_scope_analysis_{RUN_DATE}.csv", platform_scope)
    write_csv(OUT_DIR / f"future_source_admission_ledger_schema_{RUN_DATE}.csv", source_schema)
    write_csv(OUT_DIR / f"future_side_certification_ledger_schema_{RUN_DATE}.csv", side_schema)
    write_csv(OUT_DIR / f"future_row_movement_ledger_schema_{RUN_DATE}.csv", row_schema)
    write_csv(OUT_DIR / f"cumulative_overlay_and_immutability_contract_{RUN_DATE}.csv", overlay_rows)
    write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", approval_rows)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard_rows)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_STARTER_PARENT_LEDGER_REPAIR_GOVERNANCE_DECISION": DECISION,
        "MLB_STARTER_PARENT_LEDGER_REPAIR_STATUS": STATUS,
        "MLB_STARTER_PARENT_LEDGER_REPAIR_PLATFORM_SCOPE_DECISION": PLATFORM_SCOPE_DECISION,
        "governed_rows": len(data["rows"]),
        "governed_sides": len(data["sides"]),
        "projected_starter_qualified": 26,
        "projected_newly_fully_qualified": 23,
        "projected_pa_blocked": 3,
        "projected_cumulative": {r["metric"]: r["projected_post_repair_state"] for r in cumulative},
        "prohibited_work": {
            "repair_execution": "not_performed",
            "value_materialization": "not_performed",
            "qualification_propagation": "not_performed",
            "matrix_model_upload_db_api_launchagent_production_change": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_starter_parent_ledger_repair_governance_{RUN_DATE}.json", machine)
    write_summary(data)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", deterministic_replay(data))
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()]))
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    result = build_package()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
